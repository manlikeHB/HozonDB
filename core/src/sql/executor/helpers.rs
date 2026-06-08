use crate::{
    benchmark::metrics::QueryMetrics,
    catalog::index::IndexEntry,
    constants::{self, PageId},
    index::{key::IndexKey, node::leaf::RowLocation},
    sql::{
        database::Database,
        executor::evaluator::evaluate_expr,
        parser::{BinaryOperator, Expr},
    },
    storage::{buffer_pool::BufferPool, page::PageType},
    wal::record_type::WalRecordType,
};
use std::io::{self, Error, ErrorKind};

use crate::{
    catalog::{
        row::{Row, Value},
        schema::{Column, DataType},
    },
    storage::page::{PAGE_SIZE, PageManager},
};

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

fn scan_table_with_locations(
    db: &mut Database,
    first_page: u32,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<Vec<(Row, RowLocation)>> {
    let mut rows_and_location = Vec::new();
    let mut cur_page = first_page;

    loop {
        // Read page data
        let page_data = db.read_page(cur_page)?;

        // track page reads
        if let Some(m) = metrics.as_mut() {
            m.pages_read += 1;
        }

        let page_meta = PageManager::read_metadata_from_buffer(&page_data, PageType::Slotted);

        // Parse all rows from the page
        let rows_and_slots = read_rows_from_page(&page_data, page_meta.slot_count()?)?;

        for (row, slot) in rows_and_slots {
            // track rows scanned
            if let Some(m) = metrics.as_mut() {
                m.rows_scanned += 1;
            }

            let row_location = RowLocation::new(cur_page, slot);
            rows_and_location.push((row, row_location));
        }

        if let Some(next_page) = page_meta.next_page()? {
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
    let (wal_writer, buffer_pool) = db.get_wal_and_buffer_pool();
    // let mut last_page_data = db.read_page(last_page)?;
    let mut last_page_data = buffer_pool.get_page_mut(last_page)?;

    // Track page read
    if let Some(m) = metrics.as_mut() {
        m.pages_read += 1;
    }

    let mut last_page_meta =
        PageManager::read_metadata_from_buffer(&last_page_data, PageType::Slotted);
    let row_bytes = Row::to_bytes_from_values(values);

    let space_needed = row_bytes.len() + 4;
    let available = last_page_meta.free_space_end()? - last_page_meta.free_space_start()?;

    // try to write new row to last page
    let (row_page_id, slot) = if space_needed <= available as usize {
        // get row offset to insert new row
        let row_offset = last_page_meta.free_space_end()? as usize - row_bytes.len();
        // append record to WAL log
        let lsn = wal_writer.append_slotted(
            WalRecordType::Insert,
            table_name,
            last_page,
            last_page_meta.slot_count()?,
            &row_bytes,
            &vec![],
        )?;
        // write row to page data
        last_page_data[row_offset..row_offset + row_bytes.len()].copy_from_slice(&row_bytes);
        // write slot to page data
        PageManager::write_slot(
            &mut last_page_data,
            last_page_meta.slot_count()?,
            row_offset as u16,
            row_bytes.len() as u16,
        );

        // update metadata
        last_page_meta.update_slot_count();
        last_page_meta.update_free_space_start();
        last_page_meta.update_free_space_end(row_bytes.len());
        last_page_meta.set_lsn(lsn);

        // update metadata and write page to disk
        PageManager::update_metadata_in_buffer(&mut last_page_data, &last_page_meta);

        // mark page dirty
        db.mark_dirty(last_page, lsn)?;

        // Track page write
        if let Some(m) = metrics.as_mut() {
            m.pages_written += 1;
        }

        (last_page, last_page_meta.slot_count()? - 1)
    } else {
        // Create a new page
        let new_page = buffer_pool.allocate_slotted_page()?;
        let mut new_page_data = buffer_pool.get_page_mut(new_page)?;
        let mut new_page_meta =
            PageManager::read_metadata_from_buffer(&new_page_data, PageType::Slotted);
        // get row offset
        let row_offset = new_page_meta.free_space_end()? as usize - row_bytes.len();
        // append record to WAL log
        let lsn = wal_writer.append_slotted(
            WalRecordType::Insert,
            table_name,
            new_page,
            new_page_meta.slot_count()?,
            &row_bytes,
            &vec![],
        )?;
        // write row to page data
        new_page_data[row_offset..row_offset + row_bytes.len()].copy_from_slice(&row_bytes);
        // write slot
        PageManager::write_slot(
            &mut new_page_data,
            new_page_meta.slot_count()?,
            row_offset as u16,
            row_bytes.len() as u16,
        );

        // update metadata
        new_page_meta.update_slot_count();
        new_page_meta.update_free_space_start();
        new_page_meta.update_free_space_end(row_bytes.len());
        new_page_meta.set_lsn(lsn);

        PageManager::update_metadata_in_buffer(&mut new_page_data, &new_page_meta);
        // mark page dirty
        buffer_pool.mark_dirty(new_page, lsn)?;

        // Track new page write
        if let Some(m) = metrics.as_mut() {
            m.pages_written += 1;
        }

        // Update the previous page's metadata to point to the new page
        last_page_meta.set_next_page(new_page);
        buffer_pool.update_page_metadata(last_page, &last_page_meta)?;

        if let Some(m) = metrics.as_mut() {
            m.pages_written += 1;
        }

        // update table's last page
        db.update_table_last_page(table_name, new_page)?;

        (new_page, new_page_meta.slot_count()? - 1)
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

// TODO: add helper for slotted and raw pages
pub fn collect_page_chain(
    buffer_pool: &mut BufferPool,
    first_page: PageId,
) -> io::Result<Vec<PageId>> {
    let mut chain = Vec::new();
    let mut current = Some(first_page);

    while let Some(page_id) = current {
        chain.push(page_id);
        let metadata = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
        current = metadata.next_page()?;
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

pub fn resolve_rows(
    db: &mut Database,
    table_name: &str,
    first_page: u32,
    where_clause: &Option<Expr>,
    column_names: &[String],
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<Vec<(Row, RowLocation)>> {
    let mut filtered_rows_and_loc = Vec::new();

    // try index seek first
    let index_used = if let Some(Expr::BinaryOp { left, op, right }) = &where_clause {
        if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
            if let Some(entry) = db
                .get_indexes_for_table(table_name)
                .and_then(|entries| entries.iter().find(|e| e.column_name() == col))
                .cloned()
            {
                let key = IndexKey::try_from(val.clone())?;

                match op {
                    BinaryOperator::Equals => {
                        // point lookup
                        match db.search_index(entry.index_name(), &key)? {
                            Some(loc) => {
                                let row = read_row(db.read_page(loc.page_id())?, loc)?;
                                filtered_rows_and_loc.push((row, loc));
                                if let Some(m) = metrics.as_mut() {
                                    m.pages_read += 1;
                                    m.rows_scanned += 1;
                                }
                            }
                            None => {}
                        }
                        true
                    }
                    BinaryOperator::LessThan | BinaryOperator::LessOrEqual => {
                        // range scan — end bound only
                        let locations =
                            db.range_index_scan(entry.index_name(), None, Some(&key), op)?;
                        for loc in locations {
                            let row = read_row(db.read_page(loc.page_id())?, loc)?;
                            if let Some(m) = metrics.as_mut() {
                                m.pages_read += 1; // TODO: track pages read for metrics
                                m.rows_scanned += 1;
                            }
                            filtered_rows_and_loc.push((row, loc));
                        }
                        true
                    }
                    BinaryOperator::GreaterThan | BinaryOperator::GreaterOrEqual => {
                        // range scan — start bound only
                        let locations =
                            db.range_index_scan(entry.index_name(), Some(&key), None, op)?;
                        for loc in locations {
                            let row = read_row(db.read_page(loc.page_id())?, loc)?;
                            if let Some(m) = metrics.as_mut() {
                                m.pages_read += 1; // TODO: track pages read for metrics
                                m.rows_scanned += 1;
                            }
                            filtered_rows_and_loc.push((row, loc));
                        }
                        true
                    }
                    _ => false, // AND, OR, NotEquals — fall through to full scan
                }
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    // fall back to full scan if index not used
    if !index_used {
        let rows_and_loc = scan_table_with_locations(db, first_page, metrics)?;
        for (row, loc) in rows_and_loc {
            let matches = if let Some(expr) = where_clause {
                match evaluate_expr(expr, &row, column_names) {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!("Warning: Error evaluating WHERE clause: {}", e);
                        false
                    }
                }
            } else {
                true // no WHERE = include all
            };
            if matches {
                filtered_rows_and_loc.push((row, loc));
            }
        }
    }

    Ok(filtered_rows_and_loc)
}

pub fn read_row(page_data: &[u8; PAGE_SIZE], loc: RowLocation) -> io::Result<Row> {
    let (row_offset, row_length) = PageManager::read_slot(page_data, loc.slot());
    let (row, _) =
        Row::from_bytes(&page_data[row_offset as usize..(row_offset + row_length) as usize])?;

    Ok(row)
}

pub fn validate_index_key_length(
    value: &Value,
    column: &Column,
    index_entries: &[IndexEntry],
) -> io::Result<()> {
    if let Value::Text(s) = value {
        let is_indexed = index_entries
            .iter()
            .any(|e| e.column_name() == column.name());

        if is_indexed && s.len() > constants::MAX_TEXT_INDEX_KEY_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "text value for indexed column '{}' exceeds {} byte limit",
                    column.name(),
                    constants::MAX_TEXT_INDEX_KEY_BYTES
                ),
            ));
        }
    }
    Ok(())
}
