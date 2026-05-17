use crate::{
    benchmark::metrics::QueryMetrics,
    catalog::index::IndexEntry,
    constants::PageId,
    index::{key::IndexKey, node::leaf::RowLocation},
    sql::{database::Database, evaluator::evaluate_expr, parser::BinaryOperator},
};
use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{
    catalog::{
        row::{Row, Value},
        schema::{Column, DataType, Schema},
    },
    sql::parser::{Expr, SelectColumns, Statement},
    storage::page::{PAGE_SIZE, PageManager},
};

pub struct Executor {
    database: Database,
}

#[derive(Debug)]
pub enum ExecutionResult {
    Success {
        message: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
}

impl Executor {
    pub fn new(database: Database) -> Self {
        Executor { database }
    }

    fn read_row_at_location(&self, location: RowLocation) -> io::Result<Row> {
        let page_data = self.database.read_page(location.page_id())?;
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

            let (row, _) = Row::from_bytes(
                &page_data[row_offset as usize..(row_offset + row_length) as usize],
            )?;

            rows_and_slots.push((row, idx));
        }

        Ok(rows_and_slots)
    }

    // Read all rows from a table
    // which possible spans across multiple pages
    fn read_all_table_rows(
        &self,
        first_page: u32,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<(Vec<Row>, Vec<PageId>)> {
        let mut rows = Vec::new();
        let mut cur_page = first_page;
        let mut old_chain = Vec::new();

        loop {
            // Read page data
            let page_data = self.database.read_page(cur_page)?;

            // track page reads
            if let Some(m) = metrics.as_mut() {
                m.pages_read += 1;
            }

            let page_meta = PageManager::read_metadata_from_buffer(&page_data);

            // Parse all rows from the page
            let rows_and_slot = Self::read_rows_from_page(&page_data, page_meta.slot_count)?;

            // track rows scanned
            if let Some(m) = metrics.as_mut() {
                m.rows_scanned += rows.len();
            }
            let mut new_rows = rows_and_slot.into_iter().map(|(row, _)| row).collect();
            rows.append(&mut new_rows);

            // collect old chain pages
            old_chain.push(cur_page);

            if let Some(next_page) = page_meta.next_page {
                cur_page = next_page;
            } else {
                break;
            }
        }

        Ok((rows, old_chain))
    }

    fn scan_table_with_locations(
        &self,
        first_page: u32,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<Vec<(Row, RowLocation)>> {
        let mut rows_and_location = Vec::new();
        let mut cur_page = first_page;

        loop {
            // Read page data
            let page_data = self.database.read_page(cur_page)?;

            // track page reads
            if let Some(m) = metrics.as_mut() {
                m.pages_read += 1;
            }

            let page_meta = PageManager::read_metadata_from_buffer(&page_data);

            // Parse all rows from the page
            let rows_and_slots = Self::read_rows_from_page(&page_data, page_meta.slot_count)?;

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

    pub fn execute(
        &mut self,
        statement: Statement,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<ExecutionResult> {
        let start = std::time::Instant::now();

        let result = match statement {
            Statement::CreateTable { name, columns } => self.execute_create(name, columns),
            Statement::Insert { table_name, values } => {
                self.execute_insert(table_name, values, metrics)
            }
            Statement::Select {
                table_name,
                columns,
                where_clause,
            } => self.execute_select(table_name, columns, where_clause, metrics),
            Statement::DropTable { name } => self.execute_drop_table(name),
            Statement::Delete {
                table_name,
                where_clause,
            } => self.execute_delete(table_name, where_clause, metrics),
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => self.execute_update(table_name, assignments, where_clause, metrics),
        };

        if let Some(m) = metrics {
            m.duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        }

        result
    }

    fn get_table_first_page_and_cols(&self, table_name: &str) -> io::Result<(u32, &Vec<Column>)> {
        let (first_page, columns) = match self.database.get_table(&table_name) {
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

    fn execute_create(
        &mut self,
        table_name: String,
        columns: Vec<Column>,
    ) -> io::Result<ExecutionResult> {
        let schema = Schema::new(&table_name, columns)?;
        self.database.create_table(schema)?;

        Ok(ExecutionResult::Success {
            message: format!("Table '{}' created.", table_name),
        })
    }

    fn execute_insert(
        &mut self,
        table_name: String,
        values: Vec<Value>,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<ExecutionResult> {
        let (_, columns) = self.get_table_first_page_and_cols(&table_name)?;
        let columns = columns.to_vec();

        // Validate value count
        if values.len() != columns.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Expected {} values, got {}", columns.len(), values.len()),
            ));
        }

        let value_and_col_pairs: Vec<(&Value, &Column)> =
            values.iter().zip(columns.iter()).collect();

        // Validate data types
        for (value, column) in &value_and_col_pairs {
            if !validate_value_type(value, column.data_type()) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "Type mismatch for column '{}': expected {:?}, got {:?}",
                        column.name(),
                        column.data_type(),
                        value
                    ),
                ));
            }
        }

        // get table indexes
        let index_entries = self
            .database
            .get_indexes_for_table(&table_name)
            .map(|entries| entries.to_vec())
            .unwrap_or_default();

        // check for duplicate primary key
        for entry in &index_entries {
            if entry.is_primary() {
                let (val, _) = value_and_col_pairs
                    .iter()
                    .find(|(_, col)| entry.column_name() == col.name())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "indexed column '{}' not found in schema",
                                entry.column_name()
                            ),
                        )
                    })?;

                if let Some(_) = self
                    .database
                    .search_index(entry.index_name(), &IndexKey::try_from((*val).clone())?)?
                {
                    return Err(Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "duplicate primary key value for column '{}'",
                            entry.column_name()
                        ),
                    ));
                }
            }
        }

        // get last page
        let last_page = self
            .database
            .get_table_last_page(&table_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("Last page for {} not found", &table_name),
                )
            })?;

        // insert row
        let (row_page_id, slot) =
            self.insert_row_into_page(&table_name, last_page, &values, metrics)?;
        // Index new row if table was indexed
        let row_location = RowLocation::new(row_page_id, slot);
        self.index_new_row(&index_entries, &value_and_col_pairs, row_location)?;

        if let Some(m) = metrics.as_mut() {
            m.rows_modified += 1;
        }

        Ok(ExecutionResult::Success {
            message: "1 row inserted.".to_string(),
        })
    }

    fn insert_row_into_page(
        &mut self,
        table_name: &str,
        last_page: PageId,
        values: &[Value],
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<(PageId, u16)> {
        let mut last_page_data = self.database.read_page(last_page)?;

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
            self.database.write_page(last_page, &last_page_data)?;

            // Track page write
            if let Some(m) = metrics.as_mut() {
                m.pages_written += 1;
            }

            (last_page, last_page_meta.slot_count - 1)
        } else {
            // Create a new page
            let new_page = self.database.allocate_page()?;
            let mut new_page_data = self.database.read_page(new_page)?;
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
            self.database.write_page(new_page, &new_page_data)?;

            // Track new page write
            if let Some(m) = metrics.as_mut() {
                m.pages_written += 1;
            }

            // Update the previous page's metadata to point to the new page
            last_page_meta.next_page = Some(new_page);
            self.database
                .update_page_metadata(last_page, &last_page_meta)?;

            if let Some(m) = metrics.as_mut() {
                m.pages_written += 1;
            }

            // update table's last page
            self.database.update_table_last_page(table_name, new_page)?;

            (new_page, new_page_meta.slot_count - 1)
        };

        Ok((row_page_id, slot))
    }

    fn index_new_row(
        &mut self,
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

            self.database.insert_into_index(
                entry.index_name(),
                IndexKey::try_from((*val).clone())?,
                row_location,
            )?;
        }

        Ok(())
    }

    fn delete_indexes(
        &mut self,
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

            self.database
                .delete_from_index(entry.index_name(), &IndexKey::try_from((*val).clone())?)?;
        }

        Ok(())
    }

    fn execute_select(
        &mut self,
        table_name: String,
        select_columns: SelectColumns,
        where_clause: Option<Expr>,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<ExecutionResult> {
        let (first_page, columns) = self.get_table_first_page_and_cols(&table_name)?;

        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        let mut index_used = false;
        let mut filtered_rows = Vec::new();

        // check for index-eligible WHERE clause

        // TODO: range scan support (WHERE col > x, WHERE col BETWEEN x AND y)
        // Currently only equality predicates use the index.
        // Non-equality WHERE clauses on indexed columns fall back to full scan.
        // Implementing range scans requires walking the leaf linked list
        // from the first matching leaf — a natural extension of the current B+ tree.
        if let Some(Expr::BinaryOp {
            left,
            op: BinaryOperator::Equals,
            right,
        }) = &where_clause
        {
            if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                // check if column is indexed
                if let Some(entry) = self
                    .database
                    .get_indexes_for_table(&table_name)
                    .and_then(|entries| entries.iter().find(|entry| entry.column_name() == col))
                    .cloned()
                {
                    let key = IndexKey::try_from(val.clone())?;
                    let result = self.database.search_index(entry.index_name(), &key)?;

                    match result {
                        Some(row_location) => {
                            let row = self.read_row_at_location(row_location)?;
                            filtered_rows.push(row);

                            if let Some(m) = metrics.as_mut() {
                                m.pages_read += 1; // one data page read
                                m.rows_scanned += 1;
                            }

                            index_used = true;
                        }
                        None => {}
                    }
                }
            }
        }

        if !index_used {
            // full scan
            let (rows, _) = self.read_all_table_rows(first_page, metrics)?;

            // check if there are any rows in this table
            if rows.len() == 0 {
                return Ok(ExecutionResult::Rows {
                    columns: all_column_names,
                    rows: Vec::<Row>::new(),
                });
            }

            // filter rows based on the where clause
            for row in rows {
                if let Some(ref expr) = where_clause {
                    match evaluate_expr(expr, &row, &all_column_names) {
                        Ok(true) => filtered_rows.push(row),
                        Ok(false) => (),
                        Err(e) => {
                            eprintln!("Warning: Error evaluating WHERE clause: {}", e);
                        }
                    }
                } else {
                    filtered_rows.push(row);
                }
            }
        }

        // Handle column selection
        match select_columns {
            SelectColumns::All => Ok(ExecutionResult::Rows {
                columns: all_column_names,
                rows: filtered_rows,
            }),
            SelectColumns::Specific(requested_cols) => {
                // Find indices of requested columns
                let mut column_indices = Vec::new();
                let mut result_column_names = Vec::new();

                for req_col in &requested_cols {
                    match all_column_names.iter().position(|c| c == req_col) {
                        Some(idx) => {
                            column_indices.push(idx);
                            result_column_names.push(req_col.clone());
                        }
                        None => {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                format!(
                                    "Column '{}' does not exist in table '{}'",
                                    req_col, table_name
                                ),
                            ));
                        }
                    }
                }

                // Project rows to only include selected columns
                let projected_rows: Vec<Row> = filtered_rows
                    .iter()
                    .map(|row| {
                        let values: Vec<Value> = column_indices
                            .iter()
                            .filter_map(|&idx| row.get_value(idx).cloned())
                            .collect();
                        Row::new(values)
                    })
                    .collect();

                Ok(ExecutionResult::Rows {
                    columns: result_column_names,
                    rows: projected_rows,
                })
            }
        }
    }

    fn execute_drop_table(&mut self, table_name: String) -> io::Result<ExecutionResult> {
        let (first_page, _) = self.get_table_first_page_and_cols(&table_name)?;

        let page_chain = self.collect_page_chain(first_page)?;

        self.database.drop_table(&table_name)?;

        for page in page_chain {
            self.database.free_page(page)?;
        }

        // TODO: drop_table does not free B+ tree index pages — only the index
        // catalog entry is removed. Index pages are orphaned on disk until
        // compaction. Fixing this requires BPlusTree to expose a full page
        // traversal method.
        self.database.drop_table_indexes(&table_name)?;

        Ok(ExecutionResult::Success {
            message: format!("{} table successfully dropped", table_name),
        })
    }

    // TODO: dead slots are never reclaimed within a page. A compaction pass is
    // needed to pack live rows together, reset free_space_start/free_space_end,
    // and return fully-dead pages to the free list. Without this, repeated
    // deletes gradually waste page space permanently.
    // TODO: update tables last page when table is compacted
    fn execute_delete(
        &mut self,
        table_name: String,
        where_clause: Option<Expr>,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<ExecutionResult> {
        let (first_page, columns) = self.get_table_first_page_and_cols(&table_name)?;
        let columns = columns.to_vec();

        // get all rows with row location
        let rows_and_loc = self.scan_table_with_locations(first_page, metrics)?;
        let rows_len = rows_and_loc.len();

        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        // check if there are any rows in this table
        if rows_len == 0 {
            return Ok(ExecutionResult::Success {
                message: "0 rows deleted".to_string(),
            });
        }

        // filter rows based on the where clause
        let mut deleted_rows = Vec::new();
        let mut dirty_pages: HashMap<PageId, [u8; PAGE_SIZE]> = HashMap::new();

        for (row, loc) in rows_and_loc {
            let should_delete = if let Some(ref expr) = where_clause {
                match evaluate_expr(expr, &row, &all_column_names) {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!("Warning: Error evaluating WHERE clause: {}", e);
                        false
                    }
                }
            } else {
                true
            }; // no WHERE = delete all

            if should_delete {
                if !dirty_pages.contains_key(&loc.page_id()) {
                    let page_data = self.database.read_page(loc.page_id())?;
                    dirty_pages.insert(loc.page_id(), page_data);
                }

                if let Some(page_data) = dirty_pages.get_mut(&loc.page_id()) {
                    PageManager::mark_slot_dead(page_data, loc.slot());
                }
                deleted_rows.push(row);
            }
        }

        // TODO: index deletion happens after pages are written to disk.
        // If index deletion fails mid-way, rows are dead on disk but index entries
        // still exist — leaving the database in an inconsistent state.
        // WAL (Write-Ahead Logging) solves this by logging intent before any write,
        // enabling recovery to a consistent state on crash.
        for (page_id, page_data) in &dirty_pages {
            self.database.write_page(*page_id, page_data)?;
        }

        // delete indexed keys
        let index_entries = self
            .database
            .get_indexes_for_table(&table_name)
            .map(|e| e.to_vec())
            .unwrap_or_default();

        for row in &deleted_rows {
            let value_and_col_pairs: Vec<(&Value, &Column)> =
                row.values().iter().zip(columns.iter()).collect();
            self.delete_indexes(&index_entries, &value_and_col_pairs)?;
        }

        let num_rows = deleted_rows.len();

        if let Some(m) = metrics {
            m.rows_modified = num_rows;
        }
        Ok(ExecutionResult::Success {
            message: format!(
                "{} {} deleted.",
                num_rows,
                if num_rows > 1 { "rows" } else { "row" },
            ),
        })
    }

    fn execute_update(
        &mut self,
        table_name: String,
        assignments: Vec<(String, Value)>,
        where_clause: Option<Expr>,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<ExecutionResult> {
        let (first_page, columns) = self.get_table_first_page_and_cols(&table_name)?;

        let columns = columns.to_vec();

        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        // get last page
        let last_page = self
            .database
            .get_table_last_page(&table_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("Last page for {} not found", &table_name),
                )
            })?;

        // Validate assignments (column exists + type matches)
        for (col_name, value) in &assignments {
            let col_index = all_column_names
                .iter()
                .position(|c| c == col_name)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "Column '{}' does not exist in table '{}'",
                            col_name, table_name
                        ),
                    )
                })?;

            let column = &columns[col_index];

            if !validate_value_type(value, column.data_type()) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "Type mismatch for column '{}': expected {:?}, got {:?}",
                        col_name,
                        column.data_type(),
                        value
                    ),
                ));
            }
        }

        let rows_and_locs = self.scan_table_with_locations(first_page, metrics)?;

        // Check if table is empty
        if rows_and_locs.len() == 0 {
            return Ok(ExecutionResult::Success {
                message: "0 rows updated.".to_string(),
            });
        }

        let index_entries = self
            .database
            .get_indexes_for_table(&table_name)
            .map(|e| e.to_vec())
            .unwrap_or_default();

        // TODO: if WHERE clause is on an indexed column, use index seek instead of
        // full table scan. Look up RowLocation directly from the B+ tree and jump
        // to the exact page/slot — avoids scanning the entire table.

        // Update rows based on WHERE clause
        let mut updated_count = 0;
        let mut dirty_pages: HashMap<PageId, [u8; PAGE_SIZE]> = HashMap::new();
        let mut deleted_rows = Vec::new();
        for (row, loc) in rows_and_locs {
            let should_update = if let Some(ref expr) = where_clause {
                match evaluate_expr(expr, &row, &all_column_names) {
                    Ok(true) => true,
                    Ok(false) => false,
                    Err(e) => {
                        eprintln!("Warning: Error evaluating WHERE clause: {}", e);
                        false
                    }
                }
            } else {
                true // No WHERE = update all
            };

            if should_update {
                let mut updated_values = row.values().clone();

                // Apply assignments
                for (col_name, val) in &assignments {
                    if let Some(index) = all_column_names.iter().position(|c| c == col_name) {
                        updated_values[index] = val.clone();
                    }
                }

                updated_count += 1;
                let updated_row = Row::new(updated_values);

                let old_value_and_col_pairs: Vec<(&Value, &Column)> =
                    row.values().iter().zip(columns.iter()).collect();
                let new_value_and_col_pairs: Vec<(&Value, &Column)> =
                    updated_row.values().iter().zip(columns.iter()).collect();

                // if updated row fit previous space, write into old location else re-insert
                if updated_row.to_bytes().len() <= row.to_bytes().len() {
                    if !dirty_pages.contains_key(&loc.page_id()) {
                        let page_data = self.database.read_page(loc.page_id())?;

                        dirty_pages.insert(loc.page_id(), page_data);
                    }

                    if let Some(page_data) = dirty_pages.get_mut(&loc.page_id()) {
                        let (row_offset, _) = PageManager::read_slot(page_data, loc.slot());

                        let new_bytes = updated_row.to_bytes();
                        // write new row
                        page_data[row_offset as usize..row_offset as usize + new_bytes.len()]
                            .copy_from_slice(&new_bytes);

                        // update slot to reflect new row length
                        PageManager::write_slot(
                            page_data,
                            loc.slot(),
                            row_offset,
                            new_bytes.len() as u16,
                        );
                    }

                    // delete old row index
                    self.delete_indexes(&index_entries, &old_value_and_col_pairs)?;
                    // index row
                    self.index_new_row(&index_entries, &new_value_and_col_pairs, loc)?;
                } else {
                    // insert updated row as new row
                    let (row_page_id, slot) = self.insert_row_into_page(
                        &table_name,
                        last_page,
                        &updated_row.values(),
                        metrics,
                    )?;
                    // index new row
                    let row_location = RowLocation::new(row_page_id, slot);
                    self.index_new_row(&index_entries, &new_value_and_col_pairs, row_location)?;

                    // delete old row
                    if !dirty_pages.contains_key(&loc.page_id()) {
                        let page_data = self.database.read_page(loc.page_id())?;

                        dirty_pages.insert(loc.page_id(), page_data);
                    }

                    if let Some(page_data) = dirty_pages.get_mut(&loc.page_id()) {
                        PageManager::mark_slot_dead(page_data, loc.slot());
                    }
                    // collect old row to be freed if indexed
                    deleted_rows.push(row);
                }
            };
        }

        // write update pages to disk
        for (page_id, page_data) in &dirty_pages {
            self.database.write_page(*page_id, page_data)?;
        }

        // delete indexed keys for deleted rows
        for row in &deleted_rows {
            let value_and_col_pairs: Vec<(&Value, &Column)> =
                row.values().iter().zip(columns.iter()).collect();
            self.delete_indexes(&index_entries, &value_and_col_pairs)?;
        }

        if let Some(m) = metrics {
            m.rows_modified = updated_count;
        }

        Ok(ExecutionResult::Success {
            message: format!(
                "{} {} updated.",
                updated_count,
                if updated_count == 1 { "row" } else { "rows" }
            ),
        })
    }

    // #[cfg(test)]
    fn collect_page_chain(&self, first_page: PageId) -> io::Result<Vec<PageId>> {
        let mut chain = Vec::new();
        let mut current = Some(first_page);

        while let Some(page_id) = current {
            chain.push(page_id);
            let metadata = self.database.read_page_metadata(page_id)?;
            current = metadata.next_page;
        }

        Ok(chain)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::{Column, DataType};
    use crate::sql::parser::BinaryOperator;
    use crate::storage::page::PageManager;
    use crate::storage::page::SLOT_DIRECTORY_START;
    use std::fs;

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

    fn create_test_executor(db_name: &str) -> Executor {
        let pm = PageManager::new(&format!("{}.hdb", db_name)).unwrap();
        let db = Database::new(pm).unwrap();
        Executor::new(db)
    }

    #[test]
    fn test_execute_create_table() {
        cleanup("test_exec_create");

        let mut executor = create_test_executor("test_exec_create");

        let columns = vec![
            Column::new("id", DataType::Integer, true),
            Column::new("name", DataType::Text, false),
        ];

        let statement = Statement::CreateTable {
            name: "users".to_string(),
            columns,
        };

        let result = executor.execute(statement, &mut None).unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("users"));
                assert!(message.contains("created"));
            }
            _ => panic!("Expected Success result"),
        }

        cleanup("test_exec_create");
    }

    #[test]
    fn test_execute_insert_single_row() {
        cleanup("test_exec_insert");

        let mut executor = create_test_executor("test_exec_insert");

        // Create table
        let columns = vec![
            Column::new("id", DataType::Integer, true),
            Column::new("name", DataType::Text, false),
        ];
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns,
                },
                &mut None,
            )
            .unwrap();

        // Insert row
        let values = vec![Value::Integer(1), Value::Text("Alice".to_string())];
        let result = executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        cleanup("test_exec_insert");
    }

    #[test]
    fn test_execute_insert_multiple_rows() {
        cleanup("test_exec_multi_insert");

        let mut executor = create_test_executor("test_exec_multi_insert");

        // Create table
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert multiple rows
        for i in 1..=5 {
            let values = vec![Value::Integer(i), Value::Text(format!("User{}", i))];
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values,
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify with SELECT
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 5);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_exec_multi_insert");
    }

    #[test]
    fn test_execute_insert_wrong_column_count() {
        cleanup("test_exec_wrong_count");

        let mut executor = create_test_executor("test_exec_wrong_count");

        // Create table with 2 columns
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Try to insert 3 values
        let values = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
        ];
        let result = executor.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_exec_wrong_count");
    }

    #[test]
    fn test_execute_insert_wrong_type() {
        cleanup("test_exec_wrong_type");

        let mut executor = create_test_executor("test_exec_wrong_type");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Try to insert text where integer expected
        let values = vec![
            Value::Text("not a number".to_string()),
            Value::Text("Alice".to_string()),
        ];
        let result = executor.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_exec_wrong_type");
    }

    #[test]
    fn test_execute_insert_nonexistent_table() {
        cleanup("test_exec_no_table");

        let mut executor = create_test_executor("test_exec_no_table");

        let values = vec![Value::Integer(1)];
        let result = executor.execute(
            Statement::Insert {
                table_name: "nonexistent".to_string(),
                values,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_exec_no_table");
    }

    #[test]
    fn test_execute_select_all_columns() {
        cleanup("test_exec_select_all");

        let mut executor = create_test_executor("test_exec_select_all");

        // Setup
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                        Column::new("active", DataType::Boolean, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(1),
                        Value::Text("Alice".to_string()),
                        Value::Boolean(true),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Test SELECT *
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0], "id");
                assert_eq!(columns[1], "name");
                assert_eq!(columns[2], "active");
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_exec_select_all");
    }

    #[test]
    fn test_execute_select_specific_columns() {
        cleanup("test_exec_select_specific");

        let mut executor = create_test_executor("test_exec_select_specific");

        // Setup
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                        Column::new("email", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(1),
                        Value::Text("Alice".to_string()),
                        Value::Text("alice@example.com".to_string()),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Test SELECT specific columns
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::Specific(vec!["name".to_string(), "id".to_string()]),
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0], "name");
                assert_eq!(columns[1], "id");
                assert_eq!(rows.len(), 1);

                // Verify values are in correct order
                let row = &rows[0];
                match (&row.values()[0], &row.values()[1]) {
                    (Value::Text(name), Value::Integer(id)) => {
                        assert_eq!(name, "Alice");
                        assert_eq!(*id, 1);
                    }
                    _ => panic!("Unexpected value types"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_exec_select_specific");
    }

    #[test]
    fn test_execute_select_nonexistent_column() {
        cleanup("test_exec_select_bad_col");

        let mut executor = create_test_executor("test_exec_select_bad_col");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        let result = executor.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::Specific(vec!["nonexistent".to_string()]),
                where_clause: None,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_exec_select_bad_col");
    }

    #[test]
    fn test_execute_select_empty_table() {
        cleanup("test_exec_select_empty");

        let mut executor = create_test_executor("test_exec_select_empty");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_exec_select_empty");
    }

    #[test]
    fn test_all_data_types() {
        cleanup("test_exec_all_types");

        let mut executor = create_test_executor("test_exec_all_types");

        // Create table with all types
        executor
            .execute(
                Statement::CreateTable {
                    name: "test".to_string(),
                    columns: vec![
                        Column::new("int_col", DataType::Integer, true),
                        Column::new("text_col", DataType::Text, false),
                        Column::new("bool_col", DataType::Boolean, false),
                        Column::new("null_col", DataType::Null, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert row with all types
        executor
            .execute(
                Statement::Insert {
                    table_name: "test".to_string(),
                    values: vec![
                        Value::Integer(42),
                        Value::Text("hello".to_string()),
                        Value::Boolean(true),
                        Value::Null,
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Select and verify
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "test".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                let values = rows[0].values();
                assert_eq!(values.len(), 4);

                match (&values[0], &values[1], &values[2], &values[3]) {
                    (Value::Integer(i), Value::Text(t), Value::Boolean(b), Value::Null) => {
                        assert_eq!(*i, 42);
                        assert_eq!(t, "hello");
                        assert_eq!(*b, true);
                    }
                    _ => panic!("Unexpected value types"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_exec_all_types");
    }

    #[test]
    fn test_metadata_updates_correctly() {
        cleanup("test_exec_metadata");

        let mut executor = create_test_executor("test_exec_metadata");

        // Create table
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        // Get table's first page
        let first_page = executor.database.get_table("users").unwrap().first_page();

        // Check initial metadata
        let metadata = executor.database.read_page_metadata(first_page).unwrap();
        assert_eq!(metadata.slot_count, 0);
        assert_eq!(metadata.free_space_start as usize, SLOT_DIRECTORY_START);
        assert_eq!(metadata.free_space_end as usize, PAGE_SIZE);

        // Insert row
        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        // Check metadata updated
        let metadata = executor.database.read_page_metadata(first_page).unwrap();
        assert_eq!(metadata.slot_count, 1);
        assert_ne!(metadata.free_space_start as usize, SLOT_DIRECTORY_START);
        assert_ne!(metadata.free_space_end as usize, PAGE_SIZE);

        cleanup("test_exec_metadata");
    }

    #[test]
    fn test_null_values_in_any_column() {
        cleanup("test_exec_nulls");

        let mut executor = create_test_executor("test_exec_nulls");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, false),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // NULL can go in any column type
        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Null, Value::Null],
                },
                &mut None,
            )
            .unwrap();

        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(matches!(rows[0].values()[0], Value::Null));
                assert!(matches!(rows[0].values()[1], Value::Null));
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_exec_nulls");
    }

    #[test]
    fn test_where_equals_integer() {
        cleanup("test_where_eq_int");
        let mut executor = create_test_executor("test_where_eq_int");

        // Setup
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Test WHERE id = 2
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(2))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[1] {
                    Value::Text(name) => assert_eq!(name, "Bob"),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_eq_int");
    }

    #[test]
    fn test_where_equals_text() {
        cleanup("test_where_eq_text");
        let mut executor = create_test_executor("test_where_eq_text");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Test WHERE name = 'Alice'
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("name".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Text("Alice".to_string()))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[0] {
                    Value::Integer(id) => assert_eq!(*id, 1),
                    _ => panic!("Expected Integer"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_eq_text");
    }

    #[test]
    fn test_where_greater_than() {
        cleanup("test_where_gt");
        let mut executor = create_test_executor("test_where_gt");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Integer(20 + i)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Test WHERE age > 23
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("age".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Literal(Value::Integer(23))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // ages 24 and 25
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_gt");
    }

    #[test]
    fn test_where_less_than() {
        cleanup("test_where_lt");
        let mut executor = create_test_executor("test_where_lt");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, false)],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i * 10)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Test WHERE id < 30
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::LessThan,
                        right: Box::new(Expr::Literal(Value::Integer(30))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // 10 and 20
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_lt");
    }

    #[test]
    fn test_where_and_simple() {
        cleanup("test_where_and");
        let mut executor = create_test_executor("test_where_and");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Integer(25)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(2), Value::Integer(30)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(3), Value::Integer(35)],
                },
                &mut None,
            )
            .unwrap();

        // Test WHERE id > 1 AND age < 35
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(Expr::Literal(Value::Integer(1))),
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("age".to_string())),
                            op: BinaryOperator::LessThan,
                            right: Box::new(Expr::Literal(Value::Integer(35))),
                        }),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1); // Only id=2, age=30
                match &rows[0].values()[0] {
                    Value::Integer(id) => assert_eq!(*id, 2),
                    _ => panic!("Expected Integer"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_and");
    }

    #[test]
    fn test_where_or_simple() {
        cleanup("test_where_or");
        let mut executor = create_test_executor("test_where_or");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Test WHERE id = 1 OR id = 5
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Integer(1))),
                        }),
                        op: BinaryOperator::Or,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Integer(5))),
                        }),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // id=1 and id=5
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_or");
    }

    #[test]
    fn test_where_no_matches() {
        cleanup("test_where_none");
        let mut executor = create_test_executor("test_where_none");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        // Test WHERE id = 999 (doesn't exist)
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(999))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0); // No matches
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_none");
    }

    #[test]
    fn test_where_with_specific_columns() {
        cleanup("test_where_cols");
        let mut executor = create_test_executor("test_where_cols");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(1),
                        Value::Text("Alice".to_string()),
                        Value::Integer(25),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Test SELECT name FROM users WHERE id = 1
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::Specific(vec!["name".to_string()]),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0], "name");
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[0] {
                    Value::Text(name) => assert_eq!(name, "Alice"),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_cols");
    }

    #[test]
    fn test_where_boolean() {
        cleanup("test_where_bool");
        let mut executor = create_test_executor("test_where_bool");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("name", DataType::Text, true),
                        Column::new("active", DataType::Boolean, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Text("Alice".to_string()), Value::Boolean(true)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Text("Bob".to_string()), Value::Boolean(false)],
                },
                &mut None,
            )
            .unwrap();

        // Test WHERE active = true
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("active".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Boolean(true))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[0] {
                    Value::Text(name) => assert_eq!(name, "Alice"),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_where_bool");
    }

    #[test]
    fn test_drop_table_success() {
        cleanup("test_drop_success");
        let mut executor = create_test_executor("test_drop_success");

        // Create table
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        // Verify table exists
        let result = executor.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );
        assert!(result.is_ok());

        // Drop table
        let result = executor.execute(
            Statement::DropTable {
                name: "users".to_string(),
            },
            &mut None,
        );

        assert!(result.is_ok());
        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("users"));
                assert!(message.contains("dropped") || message.contains("Dropped"));
            }
            _ => panic!("Expected Success result"),
        }

        cleanup("test_drop_success");
    }

    #[test]
    fn test_drop_nonexistent_table() {
        cleanup("test_drop_nonexist");
        let mut executor = create_test_executor("test_drop_nonexist");

        // Try to drop table that doesn't exist
        let result = executor.execute(
            Statement::DropTable {
                name: "nonexistent".to_string(),
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_drop_nonexist");
    }

    #[test]
    fn test_drop_table_then_select_fails() {
        cleanup("test_drop_select");
        let mut executor = create_test_executor("test_drop_select");

        // Create and drop table
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::DropTable {
                    name: "users".to_string(),
                },
                &mut None,
            )
            .unwrap();

        // Try to select from dropped table
        let result = executor.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_drop_select");
    }

    #[test]
    fn test_drop_table_then_recreate() {
        cleanup("test_drop_recreate");
        let mut executor = create_test_executor("test_drop_recreate");

        // Create table
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        // Insert data
        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        // Drop table
        executor
            .execute(
                Statement::DropTable {
                    name: "users".to_string(),
                },
                &mut None,
            )
            .unwrap();

        // Recreate with different schema
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Should be empty
        let result = executor.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );

        if result.is_err() {
            eprintln!("result: {:?}", result);
        }

        match result.unwrap() {
            ExecutionResult::Rows { rows, columns } => {
                assert_eq!(rows.len(), 0); // New table should be empty
                assert_eq!(columns.len(), 2); // New schema has 2 columns
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_drop_recreate");
    }

    #[test]
    fn test_drop_table_persistence() {
        cleanup("test_drop_persist");

        // Create and drop in first session
        {
            let mut executor = create_test_executor("test_drop_persist");

            executor
                .execute(
                    Statement::CreateTable {
                        name: "users".to_string(),
                        columns: vec![Column::new("id", DataType::Integer, true)],
                    },
                    &mut None,
                )
                .unwrap();

            executor
                .execute(
                    Statement::CreateTable {
                        name: "orders".to_string(),
                        columns: vec![Column::new("id", DataType::Integer, false)],
                    },
                    &mut None,
                )
                .unwrap();

            executor
                .execute(
                    Statement::DropTable {
                        name: "users".to_string(),
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify drop persisted in second session
        {
            let mut executor = create_test_executor("test_drop_persist");

            // users should not exist
            let result = executor.execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            );
            assert!(result.is_err());

            // orders should still exist
            let result = executor.execute(
                Statement::Select {
                    table_name: "orders".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            );
            assert!(result.is_ok());
        }

        cleanup("test_drop_persist");
    }

    #[test]
    fn test_delete_single_row() {
        cleanup("test_delete_single");
        let mut executor = create_test_executor("test_delete_single");

        // Setup
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(3), Value::Text("Charlie".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Delete WHERE id = 2
        let result = executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(2))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify only 2 rows remain
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                // Verify Bob is gone
                for row in rows {
                    match &row.values()[1] {
                        Value::Text(name) => assert_ne!(name, "Bob"),
                        _ => panic!("Expected Text"),
                    }
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_single");
    }

    #[test]
    fn test_delete_multiple_rows() {
        cleanup("test_delete_multiple");
        let mut executor = create_test_executor("test_delete_multiple");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Integer(20 + i)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Delete WHERE age > 23
        let result = executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("age".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Literal(Value::Integer(23))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 row")); // Should delete 2 rows (age 24, 25)
            }
            _ => panic!("Expected Success result"),
        }

        // Verify 3 rows remain
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3); // ages 21, 22, 23 remain
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_multiple");
    }

    #[test]
    fn test_delete_all_rows() {
        cleanup("test_delete_all");
        let mut executor = create_test_executor("test_delete_all");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=3 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // DELETE without WHERE clause
        let result = executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("3 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify table is empty
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_all");
    }

    #[test]
    fn test_delete_no_matches() {
        cleanup("test_delete_none");
        let mut executor = create_test_executor("test_delete_none");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        // Delete WHERE id = 999 (doesn't exist)
        let result = executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(999))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("0 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify row still exists
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_none");
    }

    #[test]
    fn test_delete_with_and_condition() {
        cleanup("test_delete_and");
        let mut executor = create_test_executor("test_delete_and");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                        Column::new("active", DataType::Boolean, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        let test_data = vec![(1, 25, true), (2, 30, true), (3, 35, false), (4, 28, true)];

        for (id, age, active) in test_data {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![
                            Value::Integer(id),
                            Value::Integer(age),
                            Value::Boolean(active),
                        ],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Delete WHERE age > 27 AND active = true
        let result = executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("age".to_string())),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(Expr::Literal(Value::Integer(27))),
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("active".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Boolean(true))),
                        }),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 row")); // Should delete Bob (30, true) and Diana (28, true)
            }
            _ => panic!("Expected Success result"),
        }

        // Verify correct rows remain
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // Alice (25, true) and Charlie (35, false)
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_and");
    }

    #[test]
    fn test_delete_from_nonexistent_table() {
        cleanup("test_delete_no_table");
        let mut executor = create_test_executor("test_delete_no_table");

        let result = executor.execute(
            Statement::Delete {
                table_name: "nonexistent".to_string(),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(1))),
                }),
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_delete_no_table");
    }

    #[test]
    fn test_delete_persistence() {
        cleanup("test_delete_persist");

        // Delete in first session
        {
            let mut executor = create_test_executor("test_delete_persist");

            executor
                .execute(
                    Statement::CreateTable {
                        name: "users".to_string(),
                        columns: vec![Column::new("id", DataType::Integer, true)],
                    },
                    &mut None,
                )
                .unwrap();

            for i in 1..=5 {
                executor
                    .execute(
                        Statement::Insert {
                            table_name: "users".to_string(),
                            values: vec![Value::Integer(i)],
                        },
                        &mut None,
                    )
                    .unwrap();
            }

            // Delete id > 3
            executor
                .execute(
                    Statement::Delete {
                        table_name: "users".to_string(),
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(Expr::Literal(Value::Integer(3))),
                        }),
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify deletion persisted
        {
            let mut executor = create_test_executor("test_delete_persist");

            let result = executor
                .execute(
                    Statement::Select {
                        table_name: "users".to_string(),
                        columns: SelectColumns::All,
                        where_clause: None,
                    },
                    &mut None,
                )
                .unwrap();

            match result {
                ExecutionResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 3); // Only 1, 2, 3 remain
                    for row in rows {
                        match &row.values()[0] {
                            Value::Integer(id) => assert!(*id <= 3),
                            _ => panic!("Expected Integer"),
                        }
                    }
                }
                _ => panic!("Expected Rows result"),
            }
        }

        cleanup("test_delete_persist");
    }

    #[test]
    fn test_delete_then_insert() {
        cleanup("test_delete_insert");
        let mut executor = create_test_executor("test_delete_insert");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        // Insert rows
        for i in 1..=3 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Delete all
        executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        // Insert new rows
        for i in 10..=12 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify only new rows exist
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                for row in rows {
                    match &row.values()[0] {
                        Value::Integer(id) => assert!(*id >= 10),
                        _ => panic!("Expected Integer"),
                    }
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_insert");
    }

    #[test]
    fn test_update_single_column() {
        cleanup("test_update_single");
        let mut executor = create_test_executor("test_update_single");

        // Setup
        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(1),
                        Value::Text("Alice".to_string()),
                        Value::Integer(25),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(2),
                        Value::Text("Bob".to_string()),
                        Value::Integer(30),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Update WHERE id = 1
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("age".to_string(), Value::Integer(26))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify update
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[2] {
                    Value::Integer(age) => assert_eq!(*age, 26),
                    _ => panic!("Expected Integer"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_single");
    }

    #[test]
    fn test_update_multiple_columns() {
        cleanup("test_update_multi");
        let mut executor = create_test_executor("test_update_multi");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(1),
                        Value::Text("Alice".to_string()),
                        Value::Integer(25),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Update multiple columns
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![
                        ("name".to_string(), Value::Text("Alicia".to_string())),
                        ("age".to_string(), Value::Integer(26)),
                    ],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify both columns updated
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match (&rows[0].values()[1], &rows[0].values()[2]) {
                    (Value::Text(name), Value::Integer(age)) => {
                        assert_eq!(name, "Alicia");
                        assert_eq!(*age, 26);
                    }
                    _ => panic!("Expected Text and Integer"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_multi");
    }

    #[test]
    fn test_update_multiple_rows() {
        cleanup("test_update_multi_rows");
        let mut executor = create_test_executor("test_update_multi_rows");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, false),
                        Column::new("age", DataType::Integer, false),
                        Column::new("active", DataType::Boolean, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![
                            Value::Integer(i),
                            Value::Integer(20 + i),
                            Value::Boolean(true),
                        ],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Update WHERE age > 23
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("active".to_string(), Value::Boolean(false))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("age".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Literal(Value::Integer(23))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 rows")); // ages 24 and 25
            }
            _ => panic!("Expected Success result"),
        }

        // Verify correct rows updated
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("active".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Boolean(false))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_multi_rows");
    }

    #[test]
    fn test_update_all_rows() {
        cleanup("test_update_all");
        let mut executor = create_test_executor("test_update_all");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("active", DataType::Boolean, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=3 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Boolean(true)],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // UPDATE without WHERE clause
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("active".to_string(), Value::Boolean(false))],
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("3 rows"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify all rows updated
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                for row in rows {
                    match &row.values()[1] {
                        Value::Boolean(active) => assert_eq!(*active, false),
                        _ => panic!("Expected Boolean"),
                    }
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_all");
    }

    #[test]
    fn test_update_no_matches() {
        cleanup("test_update_none");
        let mut executor = create_test_executor("test_update_none");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Integer(25)],
                },
                &mut None,
            )
            .unwrap();

        // Update WHERE id = 999 (doesn't exist)
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("age".to_string(), Value::Integer(30))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(999))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("0 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify row unchanged
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[1] {
                    Value::Integer(age) => assert_eq!(*age, 25),
                    _ => panic!("Expected Integer"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_none");
    }

    #[test]
    fn test_update_nonexistent_table() {
        cleanup("test_update_no_table");
        let mut executor = create_test_executor("test_update_no_table");

        let result = executor.execute(
            Statement::Update {
                table_name: "nonexistent".to_string(),
                assignments: vec![("age".to_string(), Value::Integer(30))],
                where_clause: None,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_update_no_table");
    }

    #[test]
    fn test_update_nonexistent_column() {
        cleanup("test_update_bad_col");
        let mut executor = create_test_executor("test_update_bad_col");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, false)],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        // Try to update non-existent column
        let result = executor.execute(
            Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("nonexistent".to_string(), Value::Integer(30))],
                where_clause: None,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_update_bad_col");
    }

    #[test]
    fn test_update_type_mismatch() {
        cleanup("test_update_type_err");
        let mut executor = create_test_executor("test_update_type_err");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Integer(25)],
                },
                &mut None,
            )
            .unwrap();

        // Try to set integer column to text
        let result = executor.execute(
            Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("age".to_string(), Value::Text("not a number".to_string()))],
                where_clause: None,
            },
            &mut None,
        );

        assert!(result.is_err());

        cleanup("test_update_type_err");
    }

    #[test]
    fn test_update_with_complex_where() {
        cleanup("test_update_complex");
        let mut executor = create_test_executor("test_update_complex");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("age", DataType::Integer, false),
                        Column::new("active", DataType::Boolean, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        let test_data = vec![(1, 25, true), (2, 30, true), (3, 35, false), (4, 28, true)];

        for (id, age, active) in test_data {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![
                            Value::Integer(id),
                            Value::Integer(age),
                            Value::Boolean(active),
                        ],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Update WHERE age > 27 AND active = true
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("active".to_string(), Value::Boolean(false))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("age".to_string())),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(Expr::Literal(Value::Integer(27))),
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("active".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Boolean(true))),
                        }),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 rows")); // Bob (30, true) and Diana (28, true)
            }
            _ => panic!("Expected Success result"),
        }

        cleanup("test_update_complex");
    }

    #[test]
    fn test_update_persistence() {
        cleanup("test_update_persist");

        // Update in first session
        {
            let mut executor = create_test_executor("test_update_persist");

            executor
                .execute(
                    Statement::CreateTable {
                        name: "users".to_string(),
                        columns: vec![
                            Column::new("id", DataType::Integer, true),
                            Column::new("age", DataType::Integer, false),
                        ],
                    },
                    &mut None,
                )
                .unwrap();

            for i in 1..=3 {
                executor
                    .execute(
                        Statement::Insert {
                            table_name: "users".to_string(),
                            values: vec![Value::Integer(i), Value::Integer(20 + i)],
                        },
                        &mut None,
                    )
                    .unwrap();
            }

            // Update id = 2
            executor
                .execute(
                    Statement::Update {
                        table_name: "users".to_string(),
                        assignments: vec![("age".to_string(), Value::Integer(99))],
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Integer(2))),
                        }),
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify update persisted
        {
            let mut executor = create_test_executor("test_update_persist");

            let result = executor
                .execute(
                    Statement::Select {
                        table_name: "users".to_string(),
                        columns: SelectColumns::All,
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Integer(2))),
                        }),
                    },
                    &mut None,
                )
                .unwrap();

            match result {
                ExecutionResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 1);
                    match &rows[0].values()[1] {
                        Value::Integer(age) => assert_eq!(*age, 99),
                        _ => panic!("Expected Integer"),
                    }
                }
                _ => panic!("Expected Rows result"),
            }
        }

        cleanup("test_update_persist");
    }

    #[test]
    fn test_update_text_column() {
        cleanup("test_update_text");
        let mut executor = create_test_executor("test_update_text");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Update text column
        let result = executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("name".to_string(), Value::Text("Alicia".to_string()))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify text updated
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[1] {
                    Value::Text(name) => assert_eq!(name, "Alicia"),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_text");
    }

    #[test]
    fn test_insert_beyond_one_page() {
        cleanup("test_multi_insert");
        let mut executor = create_test_executor("test_multi_insert");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert 100 rows with large text (~90 bytes per row)
        // This should span multiple pages (4KB page / 90 bytes ≈ 45 rows per page)
        for i in 0..100 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![
                            Value::Integer(i),
                            Value::Text("x".repeat(80)), // 80 character string
                        ],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify all rows are readable
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 100);

                // Verify first row
                match (&rows[0].values()[0], &rows[0].values()[1]) {
                    (Value::Integer(id), Value::Text(data)) => {
                        assert_eq!(*id, 0);
                        assert_eq!(data.len(), 80);
                    }
                    _ => panic!("Unexpected value types"),
                }

                // Verify last row
                match (&rows[99].values()[0], &rows[99].values()[1]) {
                    (Value::Integer(id), Value::Text(data)) => {
                        assert_eq!(*id, 99);
                        assert_eq!(data.len(), 80);
                    }
                    _ => panic!("Unexpected value types"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_multi_insert");
    }

    #[test]
    fn test_page_chain_metadata() {
        cleanup("test_page_chain");
        let mut executor = create_test_executor("test_page_chain");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert enough data for multiple pages
        for i in 0..100 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Manually verify page chain
        let first_page = executor.database.get_table("users").unwrap().first_page();

        let mut current_page = first_page;
        let mut page_count = 0;
        let mut visited = std::collections::HashSet::new();

        loop {
            // Prevent infinite loops
            assert!(visited.insert(current_page), "Circular page chain detected");

            page_count += 1;

            let page_data = executor.database.read_page(current_page).unwrap();
            let page_meta = PageManager::read_metadata_from_buffer(&page_data);

            // Each page should have rows
            assert!(page_meta.slot_count > 0, "Page {} has 0 rows", current_page);

            match page_meta.next_page {
                Some(next) => {
                    current_page = next;
                }
                None => {
                    // Last page
                    break;
                }
            }
        }

        // Should have multiple pages
        assert!(
            page_count >= 2,
            "Expected multiple pages, got {}",
            page_count
        );

        cleanup("test_page_chain");
    }

    #[test]
    fn test_select_from_multi_page_table() {
        cleanup("test_select_multi");
        let mut executor = create_test_executor("test_select_multi");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert 150 rows
        for i in 0..150 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text("test".to_string())],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Test: SELECT * returns all rows
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 150);
            }
            _ => panic!("Expected Rows result"),
        }

        // Test: SELECT with WHERE spanning pages
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(Expr::Literal(Value::Integer(50))),
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::LessThan,
                            right: Box::new(Expr::Literal(Value::Integer(100))),
                        }),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 49); // 51-99 inclusive = 49 rows

                // Verify all rows in correct range
                for row in rows {
                    match &row.values()[0] {
                        Value::Integer(id) => {
                            assert!(*id > 50 && *id < 100);
                        }
                        _ => panic!("Expected integer"),
                    }
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_select_multi");
    }

    #[test]
    fn test_delete_from_multi_page_table() {
        cleanup("test_delete_multi");
        let mut executor = create_test_executor("test_delete_multi");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert 150 rows
        for i in 0..150 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Delete rows from middle: WHERE id >= 50 AND id < 100
        executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::GreaterOrEqual,
                            right: Box::new(Expr::Literal(Value::Integer(50))),
                        }),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::LessThan,
                            right: Box::new(Expr::Literal(Value::Integer(100))),
                        }),
                    }),
                },
                &mut None,
            )
            .unwrap();

        // Verify correct rows remain: 0-49 and 100-149 = 100 rows
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 100);

                // Verify no rows in deleted range
                for row in &rows {
                    match &row.values()[0] {
                        Value::Integer(id) => {
                            assert!(*id < 50 || *id >= 100, "Found deleted row with id {}", id);
                        }
                        _ => panic!("Expected integer"),
                    }
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_multi");
    }

    #[test]
    fn test_update_in_multi_page_table() {
        cleanup("test_update_multi");
        let mut executor = create_test_executor("test_update_multi");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert 100 rows
        for i in 0..100 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text("original".to_string())],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Update subset: SET data = 'UPDATED' WHERE id < 30
        executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("data".to_string(), Value::Text("UPDATED".to_string()))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::LessThan,
                        right: Box::new(Expr::Literal(Value::Integer(30))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        // Verify only first 30 rows are updated
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 100);

                for row in rows {
                    match (&row.values()[0], &row.values()[1]) {
                        (Value::Integer(id), Value::Text(data)) => {
                            if *id < 30 {
                                assert_eq!(data, "UPDATED", "Row {} should be updated", id);
                            } else {
                                assert_eq!(data, "original", "Row {} should not be updated", id);
                            }
                        }
                        _ => panic!("Unexpected value types"),
                    }
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_multi");
    }

    #[test]
    fn test_multi_page_persistence() {
        cleanup("test_multi_persist");

        // Session 1: Create multi-page table
        {
            let mut executor = create_test_executor("test_multi_persist");

            executor
                .execute(
                    Statement::CreateTable {
                        name: "users".to_string(),
                        columns: vec![
                            Column::new("id", DataType::Integer, true),
                            Column::new("data", DataType::Text, false),
                        ],
                    },
                    &mut None,
                )
                .unwrap();

            for i in 0..100 {
                executor
                    .execute(
                        Statement::Insert {
                            table_name: "users".to_string(),
                            values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                        },
                        &mut None,
                    )
                    .unwrap();
            }
        } // Drop executor, close database

        // Session 2: Reopen and verify all rows present
        {
            let mut executor = create_test_executor("test_multi_persist");

            let result = executor
                .execute(
                    Statement::Select {
                        table_name: "users".to_string(),
                        columns: SelectColumns::All,
                        where_clause: None,
                    },
                    &mut None,
                )
                .unwrap();

            match result {
                ExecutionResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 100);

                    // Verify data integrity
                    for (i, row) in rows.iter().enumerate() {
                        match &row.values()[0] {
                            Value::Integer(id) => {
                                assert_eq!(*id, i as i32);
                            }
                            _ => panic!("Expected integer"),
                        }
                    }
                }
                _ => panic!("Expected Rows result"),
            }
        }

        cleanup("test_multi_persist");
    }

    #[test]
    fn test_multiple_tables_multi_page() {
        cleanup("test_multi_tables_mp");
        let mut executor = create_test_executor("test_multi_tables_mp");

        // Create table1
        executor
            .execute(
                Statement::CreateTable {
                    name: "table1".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Create table2
        executor
            .execute(
                Statement::CreateTable {
                    name: "table2".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("info", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert 80 rows into table1
        for i in 0..80 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "table1".to_string(),
                        values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Insert 120 rows into table2
        for i in 0..120 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "table2".to_string(),
                        values: vec![Value::Integer(i), Value::Text("y".repeat(80))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Verify table1
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "table1".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 80);
            }
            _ => panic!("Expected Rows result"),
        }

        // Verify table2
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "table2".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 120);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_multi_tables_mp");
    }

    #[test]
    fn test_delete_all_from_multi_page() {
        cleanup("test_delete_all_mp");
        let mut executor = create_test_executor("test_delete_all_mp");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // Insert 100 rows (multi-page)
        for i in 0..100 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // Delete all (no WHERE clause)
        executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        // Verify table is empty
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_all_mp");
    }

    #[test]
    fn test_insert_updates_index() {
        cleanup("test_insert_updates_index");
        let mut executor = create_test_executor("test_insert_updates_index");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // insert some rows
        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text(format!("user_{}", i))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // verify index exists in catalog
        let index_entries = executor
            .database
            .get_indexes_for_table("users")
            .expect("index should exist for primary key table");
        assert!(!index_entries.is_empty());

        // verify index has the primary key column
        let pk_index = index_entries.iter().find(|e| e.is_primary());
        assert!(pk_index.is_some());

        cleanup("test_insert_updates_index");
    }

    #[test]
    fn test_create_table_with_primary_key_creates_index() {
        cleanup("test_create_table_pk_index");
        let mut executor = create_test_executor("test_create_table_pk_index");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // index should exist in catalog
        let indexes = executor.database.get_indexes_for_table("users");
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].is_primary());
        assert_eq!(indexes[0].column_name(), "id");

        // index tree should be loaded in memory
        assert!(!executor.database.indexes().is_empty());

        cleanup("test_create_table_pk_index");
    }

    #[test]
    fn test_create_table_without_primary_key_no_index() {
        cleanup("test_create_table_no_pk");
        let mut executor = create_test_executor("test_create_table_no_pk");

        executor
            .execute(
                Statement::CreateTable {
                    name: "logs".to_string(),
                    columns: vec![
                        Column::new("message", DataType::Text, false),
                        Column::new("level", DataType::Integer, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // no index should be created
        let indexes = executor.database.get_indexes_for_table("logs");
        assert!(indexes.is_none());

        cleanup("test_create_table_no_pk");
    }

    #[test]
    fn test_select_uses_index_for_primary_key_equality() {
        cleanup("test_select_index_eq");
        let mut executor = create_test_executor("test_select_index_eq");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=10 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text(format!("user_{}", i))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // select by primary key
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(5))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get_value(0), Some(&Value::Integer(5)));
                assert_eq!(
                    rows[0].get_value(1),
                    Some(&Value::Text("user_5".to_string()))
                );
            }
            _ => panic!("expected rows"),
        }

        cleanup("test_select_index_eq");
    }

    #[test]
    fn test_select_index_key_not_found_returns_empty() {
        cleanup("test_select_index_not_found");
        let mut executor = create_test_executor("test_select_index_not_found");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(999))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected rows"),
        }

        cleanup("test_select_index_not_found");
    }

    #[test]
    fn test_select_index_vs_full_scan_page_reads() {
        cleanup("test_select_index_metrics");
        let mut executor = create_test_executor("test_select_index_metrics");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=250 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text(format!("user_{}", i))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // full scan metrics
        let mut full_scan_metrics = Some(QueryMetrics::new());
        executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("name".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Text("user_250".to_string()))),
                    }),
                },
                &mut full_scan_metrics,
            )
            .unwrap();

        // index scan metrics — same query on indexed column
        let mut index_metrics = Some(QueryMetrics::new());
        executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(250))),
                    }),
                },
                &mut index_metrics,
            )
            .unwrap();

        let full_scan_pages = full_scan_metrics.unwrap().pages_read;
        let index_pages = index_metrics.unwrap().pages_read;

        // index should read significantly fewer pages
        assert!(
            index_pages < full_scan_pages,
            "index read {} pages, full scan read {} pages — index should be faster",
            index_pages,
            full_scan_pages
        );

        cleanup("test_select_index_metrics");
    }

    #[test]
    fn test_select_non_indexed_column_falls_back_to_full_scan() {
        cleanup("test_select_no_index_fallback");
        let mut executor = create_test_executor("test_select_no_index_fallback");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text(format!("user_{}", i))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // WHERE on non-indexed column — should fall back to full scan and still work
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("name".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Text("user_3".to_string()))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].get_value(1),
                    Some(&Value::Text("user_3".to_string()))
                );
            }
            _ => panic!("expected rows"),
        }

        cleanup("test_select_no_index_fallback");
    }

    #[test]
    fn test_insert_duplicate_primary_key_rejected() {
        cleanup("test_duplicate_pk");
        let mut executor = create_test_executor("test_duplicate_pk");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // first insert should succeed
        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // duplicate primary key should fail
        let result = executor.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("Bob".to_string())],
            },
            &mut None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);

        // verify only one row exists — duplicate was not inserted
        let select = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match select {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].get_value(1),
                    Some(&Value::Text("Alice".to_string()))
                );
            }
            _ => panic!("expected rows"),
        }

        cleanup("test_duplicate_pk");
    }

    #[test]
    fn test_insert_duplicate_allowed_on_non_pk_column() {
        cleanup("test_duplicate_non_pk");
        let mut executor = create_test_executor("test_duplicate_non_pk");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // two rows with same name but different pk — should both succeed
        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(2), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        let select = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match select {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected rows"),
        }

        cleanup("test_duplicate_non_pk");
    }

    #[test]
    fn test_delete_updates_index() {
        cleanup("test_delete_updates_index");
        let mut executor = create_test_executor("test_delete_updates_index");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text(format!("user_{}", i))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // delete row with id = 3
        executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(3))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        // index lookup for deleted key should return nothing
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(3))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected rows"),
        }

        // other keys should still be findable via index
        for i in [1, 2, 4, 5] {
            let result = executor
                .execute(
                    Statement::Select {
                        table_name: "users".to_string(),
                        columns: SelectColumns::All,
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Integer(i))),
                        }),
                    },
                    &mut None,
                )
                .unwrap();

            match result {
                ExecutionResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 1, "key {} should still exist", i);
                }
                _ => panic!("expected rows"),
            }
        }

        cleanup("test_delete_updates_index");
    }

    #[test]
    fn test_delete_all_rows_clears_index() {
        cleanup("test_delete_all_clears_index");
        let mut executor = create_test_executor("test_delete_all_clears_index");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        for i in 1..=3 {
            executor
                .execute(
                    Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text(format!("user_{}", i))],
                    },
                    &mut None,
                )
                .unwrap();
        }

        // delete all rows
        executor
            .execute(
                Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        // all index lookups should return empty
        for i in 1..=3 {
            let result = executor
                .execute(
                    Statement::Select {
                        table_name: "users".to_string(),
                        columns: SelectColumns::All,
                        where_clause: Some(Expr::BinaryOp {
                            left: Box::new(Expr::Column("id".to_string())),
                            op: BinaryOperator::Equals,
                            right: Box::new(Expr::Literal(Value::Integer(i))),
                        }),
                    },
                    &mut None,
                )
                .unwrap();

            match result {
                ExecutionResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 0, "key {} should be gone", i);
                }
                _ => panic!("expected rows"),
            }
        }

        cleanup("test_delete_all_clears_index");
    }

    #[test]
    fn test_update_larger_row_creates_new_slot() {
        cleanup("test_update_new_slot");
        let mut executor = create_test_executor("test_update_new_slot");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("short".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Update to a much larger value — forces new slot
        executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("data".to_string(), Value::Text("x".repeat(200)))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1); // old dead slot not returned
                match &rows[0].values()[1] {
                    Value::Text(data) => assert_eq!(data, &"x".repeat(200)),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_new_slot");
    }

    #[test]
    fn test_update_index_consistency_inplace() {
        cleanup("test_update_index_inplace");
        let mut executor = create_test_executor("test_update_index_inplace");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Update non-indexed column — in-place since same or smaller size
        executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("name".to_string(), Value::Text("Bob".to_string()))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        // index lookup by id should still find the row
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[1] {
                    Value::Text(name) => assert_eq!(name, "Bob"),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_index_inplace");
    }

    #[test]
    fn test_update_index_consistency_new_slot() {
        cleanup("test_update_index_new_slot");
        let mut executor = create_test_executor("test_update_index_new_slot");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("data", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1), Value::Text("short".to_string())],
                },
                &mut None,
            )
            .unwrap();

        // Update to larger value — forces new slot
        executor
            .execute(
                Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("data".to_string(), Value::Text("x".repeat(200)))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        // index lookup should find row at new location
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(1))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0].values()[1] {
                    Value::Text(data) => assert_eq!(data, &"x".repeat(200)),
                    _ => panic!("Expected Text"),
                }
            }
            _ => panic!("Expected Rows result"),
        }

        // old slot should not appear as a duplicate
        let result = executor
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                },
                &mut None,
            )
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_update_index_new_slot");
    }

    #[test]
    fn test_table_last_page() {}
}
