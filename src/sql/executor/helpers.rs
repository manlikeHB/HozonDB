use crate::{
    benchmark::metrics::QueryMetrics,
    catalog::index::IndexEntry,
    constants::PageId,
    index::{key::IndexKey, node::leaf::RowLocation},
    sql::database::Database,
};
use std::io::{self, Error, ErrorKind};

use crate::{
    catalog::{
        row::{Row, Value},
        schema::{Column, DataType},
    },
    storage::page::{PAGE_SIZE, PageManager},
};

pub fn read_row_at_location(pm: &PageManager, location: RowLocation) -> io::Result<Row> {
    let page_data = pm.read_page(location.page_id())?;
    let (row_offset, row_length) = PageManager::read_slot(&page_data, location.slot());
    let (row, _) =
        Row::from_bytes(&page_data[row_offset as usize..(row_offset + row_length) as usize])?;
    Ok(row)
}

/// Read all rows from a page
fn read_rows_from_page(
    page_data: &[u8; PAGE_SIZE],
    slot_count: u16,
) -> io::Result<Vec<(Row, u16)>> {
    let mut rows_and_slots = Vec::new();

    for idx in 0..slot_count {
        let (row_offset, row_length) = PageManager::read_slot(page_data, idx);

        if row_length == 0 {
            continue; // skip deleted row
        }

        let (row, _) =
            Row::from_bytes(&page_data[row_offset as usize..(row_offset + row_length) as usize])?;

        rows_and_slots.push((row, idx));
    }

    Ok(rows_and_slots)
}

// Read all rows from a table
// which possible spans across multiple pages
pub fn read_all_table_rows(
    pm: &PageManager,
    first_page: u32,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut cur_page = first_page;

    loop {
        // Read page data
        let page_data = pm.read_page(cur_page)?;

        // track page reads
        if let Some(m) = metrics.as_mut() {
            m.pages_read += 1;
        }

        let page_meta = PageManager::read_metadata_from_buffer(&page_data);

        // Parse all rows from the page
        let rows_and_slot = read_rows_from_page(&page_data, page_meta.slot_count)?;

        // track rows scanned
        if let Some(m) = metrics.as_mut() {
            m.rows_scanned += rows.len();
        }
        let mut new_rows = rows_and_slot.into_iter().map(|(row, _)| row).collect();
        rows.append(&mut new_rows);

        if let Some(next_page) = page_meta.next_page {
            cur_page = next_page;
        } else {
            break;
        }
    }

    Ok(rows)
}

pub fn scan_table_with_locations(
    pm: &PageManager,
    first_page: u32,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<Vec<(Row, RowLocation)>> {
    let mut rows_and_location = Vec::new();
    let mut cur_page = first_page;

    loop {
        // Read page data
        let page_data = pm.read_page(cur_page)?;

        // track page reads
        if let Some(m) = metrics.as_mut() {
            m.pages_read += 1;
        }

        let page_meta = PageManager::read_metadata_from_buffer(&page_data);

        // Parse all rows from the page
        let rows_and_slots = read_rows_from_page(&page_data, page_meta.slot_count)?;

        for (row, slot) in rows_and_slots {
            // track rows scanned
            if let Some(m) = metrics.as_mut() {
                m.rows_scanned += 1;
            }

            let row_location = RowLocation::new(cur_page, slot);
            rows_and_location.push((row, row_location));
        }

        if let Some(next_page) = page_meta.next_page {
            cur_page = next_page;
        } else {
            break;
        }
    }

    Ok(rows_and_location)
}

pub fn get_table_first_page_and_cols<'a>(
    db: &'a Database,
    table_name: &str,
) -> io::Result<(u32, &'a Vec<Column>)> {
    let (first_page, columns) = match db.get_table(&table_name) {
        Some(meta) => (meta.first_page(), meta.schema().columns()),
        None => {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Table '{}' does not exist", table_name),
            ));
        }
    };

    Ok((first_page, columns))
}

pub fn insert_row_into_page(
    db: &mut Database,
    table_name: &str,
    last_page: PageId,
    values: &[Value],
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<(PageId, u16)> {
    let mut last_page_data = db.read_page(last_page)?;

    // Track page read
    if let Some(m) = metrics.as_mut() {
        m.pages_read += 1;
    }

    let mut last_page_meta = PageManager::read_metadata_from_buffer(&last_page_data);
    let row_bytes = Row::to_bytes_from_values(values);

    let space_needed = row_bytes.len() + 4;
    let available = last_page_meta.free_space_end - last_page_meta.free_space_start;

    // try to write new row to last page
    let (row_page_id, slot) = if space_needed <= available as usize {
        // get row offset to insert new row
        let row_offset = last_page_meta.free_space_end as usize - row_bytes.len();
        // write row to page data
        last_page_data[row_offset..row_offset + row_bytes.len()].copy_from_slice(&row_bytes);
        // write slot to page data
        PageManager::write_slot(
            &mut last_page_data,
            last_page_meta.slot_count,
            row_offset as u16,
            row_bytes.len() as u16,
        );

        // update metadata
        last_page_meta.slot_count += 1;
        last_page_meta.free_space_start += 4;
        last_page_meta.free_space_end -= row_bytes.len() as u16;

        // update metadata and write page to disk
        PageManager::update_metadata_in_buffer(&mut last_page_data, &last_page_meta);
        db.write_page(last_page, &last_page_data)?;

        // Track page write
        if let Some(m) = metrics.as_mut() {
            m.pages_written += 1;
        }

        (last_page, last_page_meta.slot_count - 1)
    } else {
        // Create a new page
        let new_page = db.allocate_page()?;
        let mut new_page_data = db.read_page(new_page)?;
        let mut new_page_meta = PageManager::read_metadata_from_buffer(&new_page_data);
        // get row offset
        let row_offset = new_page_meta.free_space_end as usize - row_bytes.len();
        // write row to page data
        new_page_data[row_offset..row_offset + row_bytes.len()].copy_from_slice(&row_bytes);
        // write slot
        PageManager::write_slot(
            &mut new_page_data,
            new_page_meta.slot_count,
            row_offset as u16,
            row_bytes.len() as u16,
        );

        // update metadata
        new_page_meta.slot_count += 1;
        new_page_meta.free_space_start += 4;
        new_page_meta.free_space_end -= row_bytes.len() as u16;

        PageManager::update_metadata_in_buffer(&mut new_page_data, &new_page_meta);
        db.write_page(new_page, &new_page_data)?;

        // Track new page write
        if let Some(m) = metrics.as_mut() {
            m.pages_written += 1;
        }

        // Update the previous page's metadata to point to the new page
        last_page_meta.next_page = Some(new_page);
        db.update_page_metadata(last_page, &last_page_meta)?;

        if let Some(m) = metrics.as_mut() {
            m.pages_written += 1;
        }

        // update table's last page
        db.update_table_last_page(table_name, new_page)?;

        (new_page, new_page_meta.slot_count - 1)
    };

    Ok((row_page_id, slot))
}

pub fn index_new_row(
    db: &mut Database,
    index_entries: &[IndexEntry],
    value_and_col_pairs: &[(&Value, &Column)],
    row_location: RowLocation,
) -> io::Result<()> {
    for entry in index_entries {
        let (val, _) = value_and_col_pairs
            .iter()
            .find(|(_, c)| c.name() == entry.column_name())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "indexed column '{}' not found in schema",
                        entry.column_name()
                    ),
                )
            })?;

        db.insert_into_index(
            entry.index_name(),
            IndexKey::try_from((*val).clone())?,
            row_location,
        )?;
    }

    Ok(())
}

pub fn delete_indexes(
    db: &mut Database,
    index_entries: &[IndexEntry],
    value_and_col_pairs: &[(&Value, &Column)],
) -> io::Result<()> {
    for entry in index_entries {
        let (val, _) = value_and_col_pairs
            .iter()
            .find(|(_, c)| c.name() == entry.column_name())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "indexed column '{}' not found in schema",
                        entry.column_name()
                    ),
                )
            })?;

        db.delete_from_index(entry.index_name(), &IndexKey::try_from((*val).clone())?)?;
    }

    Ok(())
}

pub fn collect_page_chain(pm: &PageManager, first_page: PageId) -> io::Result<Vec<PageId>> {
    let mut chain = Vec::new();
    let mut current = Some(first_page);

    while let Some(page_id) = current {
        chain.push(page_id);
        let metadata = pm.read_page_metadata(page_id)?;
        current = metadata.next_page;
    }

    Ok(chain)
}

/// Validate that a value matches the expected data type
pub fn validate_value_type(value: &Value, data_type: &DataType) -> bool {
    match (value, data_type) {
        (Value::Integer(_), DataType::Integer) => true,
        (Value::Text(_), DataType::Text) => true,
        (Value::Boolean(_), DataType::Boolean) => true,
        (Value::Null, _) => true, // NULL can go in any column
        _ => false,
    }
}
