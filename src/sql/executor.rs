use crate::{sql::evaluator::evaluate_expr, storage::page::PageId};
use std::io::{self, Error, ErrorKind};

use crate::{
    catalog::{
        row::{Row, Value},
        schema::{Column, DataType, Schema},
        table::TableCatalog,
    },
    sql::parser::{Expr, SelectColumns, Statement},
    storage::page::{PAGE_DATA_START, PAGE_SIZE, PageManager, PageMetadata},
};

pub struct Executor {
    catalog: TableCatalog,
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
    pub fn new(catalog: TableCatalog) -> Self {
        Executor { catalog }
    }

    /// Read all rows from a page
    fn read_rows_from_page(page_data: &[u8], num_rows: usize) -> io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut offset = PAGE_DATA_START;

        for _ in 0..num_rows {
            let (row, bytes_consumed) = Row::from_bytes(&page_data[offset..])?;
            rows.push(row);
            offset += bytes_consumed;
        }

        Ok(rows)
    }

    // Read all rows from a table
    // which possible spans across multiple pages
    fn read_all_table_rows(&self, first_page: u32) -> io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut cur_page = first_page;

        loop {
            // Read page data
            let page_data = self.catalog.read_page(cur_page)?;
            let page_meta = PageManager::read_metadata_from_buffer(&page_data);

            // Parse all rows from the page
            let mut new_rows = Self::read_rows_from_page(&page_data, page_meta.num_rows)?;
            rows.append(&mut new_rows);

            if let Some(next_page) = page_meta.next_page {
                cur_page = next_page;
            } else {
                break;
            }
        }

        Ok(rows)
    }

    /// Write rows to a single page buffer
    /// Returns the number of rows written and final offset when page is full
    fn write_rows_to_page(
        page_data: &mut [u8; 4096],
        rows: &[Row],
        mut offset: usize,
    ) -> io::Result<(usize, usize)> {
        let mut rows_written = 0;

        // Serialize rows
        for row in rows {
            let row_bytes = row.to_bytes();

            // Check if row fits
            if offset + row_bytes.len() > PAGE_SIZE {
                // stop here and return rows written and last offset
                break;
            }

            // write row
            page_data[offset..offset + row_bytes.len()].copy_from_slice(&row_bytes);
            offset += row_bytes.len();
            rows_written += 1;
        }

        Ok((rows_written, offset))
    }

    /// writes rows of a table to as many pages as is required
    fn write_all_table_rows(&mut self, first_page: PageId, rows: &[Row]) -> io::Result<()> {
        let mut current_page_id = first_page;
        let mut remaining_rows = rows; // shrinks as we write

        loop {
            let mut page_data = [0u8; PAGE_SIZE];

            // Write as many rows as fit
            let (rows_written, final_offset) =
                Self::write_rows_to_page(&mut page_data, remaining_rows, PAGE_DATA_START)?;

            // Update page metadata
            let has_more_rows = rows_written < remaining_rows.len();
            let next_page = if has_more_rows {
                // Need another page - allocate it
                let new_page = self.catalog.allocate_page()?;
                Some(new_page)
            } else {
                None // Last page
            };

            let metadata = PageMetadata {
                is_full: has_more_rows,
                last_offset: final_offset,
                num_rows: rows_written,
                next_page,
            };
            PageManager::update_metadata_in_buffer(&mut page_data, &metadata);

            // Write page to disk
            self.catalog.write_page(current_page_id, &page_data)?;

            // Move to remaining rows
            remaining_rows = &remaining_rows[rows_written..];

            // Move to next page if needed
            if let Some(next) = next_page {
                current_page_id = next;
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn execute(&mut self, statement: Statement) -> io::Result<ExecutionResult> {
        match statement {
            Statement::CreateTable { name, columns } => self.execute_create(name, columns),
            Statement::Insert { table_name, values } => self.execute_insert(table_name, values),
            Statement::Select {
                table_name,
                columns,
                where_clause,
            } => self.execute_select(table_name, columns, where_clause),
            Statement::DropTable { name } => self.execute_drop_table(name),
            Statement::Delete {
                table_name,
                where_clause,
            } => self.execute_delete(table_name, where_clause),
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => self.execute_update(table_name, assignments, where_clause),
        }
    }

    fn execute_create(
        &mut self,
        table_name: String,
        columns: Vec<Column>,
    ) -> io::Result<ExecutionResult> {
        let schema = Schema::new(&table_name, columns);
        self.catalog.create_table(schema)?;
        Ok(ExecutionResult::Success {
            message: format!("Table '{}' created.", table_name),
        })
    }

    fn execute_insert(
        &mut self,
        table_name: String,
        values: Vec<Value>,
    ) -> io::Result<ExecutionResult> {
        // Get table metadata
        let (first_page, columns) = match self.catalog.get_table(&table_name) {
            Some(meta) => (meta.first_page(), meta.schema().columns()),
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                ));
            }
        };

        // Validate value count
        if values.len() != columns.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Expected {} values, got {}", columns.len(), values.len()),
            ));
        }

        // Validate data types
        for (value, column) in values.iter().zip(columns.iter()) {
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

        // Read all existing rows
        let mut all_rows = self.read_all_table_rows(first_page)?;

        // Add new row
        all_rows.push(Row::new(values));

        // Rewrite everything // TODO: append new row instead of rewriting everything
        self.write_all_table_rows(first_page, &all_rows)?;

        Ok(ExecutionResult::Success {
            message: "1 row inserted.".to_string(),
        })
    }

    fn execute_select(
        &mut self,
        table_name: String,
        select_columns: SelectColumns,
        where_clause: Option<Expr>,
    ) -> io::Result<ExecutionResult> {
        // Get table metadata
        let (first_page, columns) = match self.catalog.get_table(&table_name) {
            Some(meta) => (meta.first_page(), meta.schema().columns()),
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                ));
            }
        };

        let rows = self.read_all_table_rows(first_page)?;

        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        // check if there are any rows in this table
        if rows.len() == 0 {
            return Ok(ExecutionResult::Rows {
                columns: all_column_names,
                rows: Vec::<Row>::new(),
            });
        }

        // filter rows based on the where clause
        let filtered_rows: Vec<Row> = rows
            .into_iter()
            .filter_map(|row| {
                if let Some(ref expr) = where_clause {
                    match evaluate_expr(expr, &row, &all_column_names) {
                        Ok(true) => Some(row),
                        Ok(false) => None,
                        Err(e) => {
                            eprintln!("Warning: Error evaluating WHERE clause: {}", e);
                            None
                        }
                    }
                } else {
                    Some(row)
                }
            })
            .collect();

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
        self.catalog.drop_table(&table_name)?;

        Ok(ExecutionResult::Success {
            message: format!("{} table successfully dropped", table_name),
        })
    }

    fn execute_delete(
        &mut self,
        table_name: String,
        where_clause: Option<Expr>,
    ) -> io::Result<ExecutionResult> {
        // Get table metadata
        let (first_page, columns) = match self.catalog.get_table(&table_name) {
            Some(meta) => (meta.first_page(), meta.schema().columns()),
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                ));
            }
        };

        // get all rows
        let rows = self.read_all_table_rows(first_page)?;
        let rows_len = rows.len();

        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        // check if there are any rows in this table
        if rows_len == 0 {
            return Ok(ExecutionResult::Success {
                message: "0 rows deleted".to_string(),
            });
        }

        // filter rows based on the where clause
        let filtered_rows: Vec<Row> = rows
            .into_iter()
            .filter_map(|row| {
                if let Some(ref expr) = where_clause {
                    match evaluate_expr(expr, &row, &all_column_names) {
                        Ok(true) => None, // if row matches filter out
                        Ok(false) => Some(row),
                        Err(e) => {
                            eprintln!("Warning: Error evaluating WHERE clause: {}", e);
                            None
                        }
                    }
                } else {
                    None // if no where clause then delete all rows
                }
            })
            .collect();

        self.write_all_table_rows(first_page, &filtered_rows)?;

        let num_rows = rows_len - filtered_rows.len();
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
    ) -> io::Result<ExecutionResult> {
        // Get table metadata
        let (first_page, columns) = match self.catalog.get_table(&table_name) {
            Some(meta) => (meta.first_page(), meta.schema().columns()),
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                ));
            }
        };

        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

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

        let rows = self.read_all_table_rows(first_page)?;

        // Check if table is empty
        if rows.len() == 0 {
            return Ok(ExecutionResult::Success {
                message: "0 rows updated.".to_string(),
            });
        }

        // Update rows based on WHERE clause
        let mut updated_count = 0;
        let updated_rows: Vec<Row> = rows
            .into_iter()
            .map(|row| {
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
                    Row::new(updated_values)
                } else {
                    row
                }
            })
            .collect();

        // write updated rows to page
        self.write_all_table_rows(first_page, &updated_rows)?;

        Ok(ExecutionResult::Success {
            message: format!(
                "{} {} updated.",
                updated_count,
                if updated_count == 1 { "row" } else { "rows" }
            ),
        })
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
    use std::fs;

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

    fn create_test_executor(db_name: &str) -> Executor {
        let pm = PageManager::new(&format!("{}.hdb", db_name)).unwrap();
        let catalog = TableCatalog::new(pm).unwrap();
        Executor::new(catalog)
    }

    #[test]
    fn test_execute_create_table() {
        cleanup("test_exec_create");

        let mut executor = create_test_executor("test_exec_create");

        let columns = vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text),
        ];

        let statement = Statement::CreateTable {
            name: "users".to_string(),
            columns,
        };

        let result = executor.execute(statement).unwrap();

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
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text),
        ];
        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns,
            })
            .unwrap();

        // Insert row
        let values = vec![Value::Integer(1), Value::Text("Alice".to_string())];
        let result = executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        // Insert multiple rows
        for i in 1..=5 {
            let values = vec![Value::Integer(i), Value::Text(format!("User{}", i))];
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values,
                })
                .unwrap();
        }

        // Verify with SELECT
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        // Try to insert 3 values
        let values = vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Boolean(true),
        ];
        let result = executor.execute(Statement::Insert {
            table_name: "users".to_string(),
            values,
        });

        assert!(result.is_err());

        cleanup("test_exec_wrong_count");
    }

    #[test]
    fn test_execute_insert_wrong_type() {
        cleanup("test_exec_wrong_type");

        let mut executor = create_test_executor("test_exec_wrong_type");

        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        // Try to insert text where integer expected
        let values = vec![
            Value::Text("not a number".to_string()),
            Value::Text("Alice".to_string()),
        ];
        let result = executor.execute(Statement::Insert {
            table_name: "users".to_string(),
            values,
        });

        assert!(result.is_err());

        cleanup("test_exec_wrong_type");
    }

    #[test]
    fn test_execute_insert_nonexistent_table() {
        cleanup("test_exec_no_table");

        let mut executor = create_test_executor("test_exec_no_table");

        let values = vec![Value::Integer(1)];
        let result = executor.execute(Statement::Insert {
            table_name: "nonexistent".to_string(),
            values,
        });

        assert!(result.is_err());

        cleanup("test_exec_no_table");
    }

    #[test]
    fn test_execute_select_all_columns() {
        cleanup("test_exec_select_all");

        let mut executor = create_test_executor("test_exec_select_all");

        // Setup
        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                    Column::new("active", DataType::Boolean),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Boolean(true),
                ],
            })
            .unwrap();

        // Test SELECT *
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                    Column::new("email", DataType::Text),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Text("alice@example.com".to_string()),
                ],
            })
            .unwrap();

        // Test SELECT specific columns
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::Specific(vec!["name".to_string(), "id".to_string()]),
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            })
            .unwrap();

        let result = executor.execute(Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::Specific(vec!["nonexistent".to_string()]),
            where_clause: None,
        });

        assert!(result.is_err());

        cleanup("test_exec_select_bad_col");
    }

    #[test]
    fn test_execute_select_empty_table() {
        cleanup("test_exec_select_empty");

        let mut executor = create_test_executor("test_exec_select_empty");

        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "test".to_string(),
                columns: vec![
                    Column::new("int_col", DataType::Integer),
                    Column::new("text_col", DataType::Text),
                    Column::new("bool_col", DataType::Boolean),
                    Column::new("null_col", DataType::Null),
                ],
            })
            .unwrap();

        // Insert row with all types
        executor
            .execute(Statement::Insert {
                table_name: "test".to_string(),
                values: vec![
                    Value::Integer(42),
                    Value::Text("hello".to_string()),
                    Value::Boolean(true),
                    Value::Null,
                ],
            })
            .unwrap();

        // Select and verify
        let result = executor
            .execute(Statement::Select {
                table_name: "test".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        // Get table's first page
        let first_page = executor.catalog.get_table("users").unwrap().first_page();

        // Check initial metadata
        let metadata = executor.catalog.read_page_metadata(first_page).unwrap();
        assert_eq!(metadata.num_rows, 0);
        assert_eq!(metadata.last_offset, PAGE_DATA_START);

        // Insert row
        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            })
            .unwrap();

        // Check metadata updated
        let metadata = executor.catalog.read_page_metadata(first_page).unwrap();
        assert_eq!(metadata.num_rows, 1);
        assert!(metadata.last_offset > PAGE_DATA_START);

        cleanup("test_exec_metadata");
    }

    #[test]
    fn test_null_values_in_any_column() {
        cleanup("test_exec_nulls");

        let mut executor = create_test_executor("test_exec_nulls");

        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        // NULL can go in any column type
        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Null, Value::Null],
            })
            .unwrap();

        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            })
            .unwrap();

        // Test WHERE id = 2
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(2))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            })
            .unwrap();

        // Test WHERE name = 'Alice'
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("name".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Text("Alice".to_string()))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Integer(20 + i)],
                })
                .unwrap();
        }

        // Test WHERE age > 23
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(Value::Integer(23))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i * 10)],
                })
                .unwrap();
        }

        // Test WHERE id < 30
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::LessThan,
                    right: Box::new(Expr::Literal(Value::Integer(30))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Integer(25)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(2), Value::Integer(30)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(3), Value::Integer(35)],
            })
            .unwrap();

        // Test WHERE id > 1 AND age < 35
        let result = executor
            .execute(Statement::Select {
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
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                })
                .unwrap();
        }

        // Test WHERE id = 1 OR id = 5
        let result = executor
            .execute(Statement::Select {
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
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            })
            .unwrap();

        // Test WHERE id = 999 (doesn't exist)
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(999))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Integer(25),
                ],
            })
            .unwrap();

        // Test SELECT name FROM users WHERE id = 1
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::Specific(vec!["name".to_string()]),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(1))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("name", DataType::Text),
                    Column::new("active", DataType::Boolean),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Text("Alice".to_string()), Value::Boolean(true)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Text("Bob".to_string()), Value::Boolean(false)],
            })
            .unwrap();

        // Test WHERE active = true
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("active".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Boolean(true))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        // Verify table exists
        let result = executor.execute(Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            where_clause: None,
        });
        assert!(result.is_ok());

        // Drop table
        let result = executor.execute(Statement::DropTable {
            name: "users".to_string(),
        });

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
        let result = executor.execute(Statement::DropTable {
            name: "nonexistent".to_string(),
        });

        assert!(result.is_err());

        cleanup("test_drop_nonexist");
    }

    #[test]
    fn test_drop_table_then_select_fails() {
        cleanup("test_drop_select");
        let mut executor = create_test_executor("test_drop_select");

        // Create and drop table
        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        executor
            .execute(Statement::DropTable {
                name: "users".to_string(),
            })
            .unwrap();

        // Try to select from dropped table
        let result = executor.execute(Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            where_clause: None,
        });

        assert!(result.is_err());

        cleanup("test_drop_select");
    }

    #[test]
    fn test_drop_table_then_recreate() {
        cleanup("test_drop_recreate");
        let mut executor = create_test_executor("test_drop_recreate");

        // Create table
        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        // Insert data
        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            })
            .unwrap();

        // Drop table
        executor
            .execute(Statement::DropTable {
                name: "users".to_string(),
            })
            .unwrap();

        // Recreate with different schema
        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        // Should be empty
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
            .unwrap();

        match result {
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
                .execute(Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer)],
                })
                .unwrap();

            executor
                .execute(Statement::CreateTable {
                    name: "orders".to_string(),
                    columns: vec![Column::new("id", DataType::Integer)],
                })
                .unwrap();

            executor
                .execute(Statement::DropTable {
                    name: "users".to_string(),
                })
                .unwrap();
        }

        // Verify drop persisted in second session
        {
            let mut executor = create_test_executor("test_drop_persist");

            // users should not exist
            let result = executor.execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            });
            assert!(result.is_err());

            // orders should still exist
            let result = executor.execute(Statement::Select {
                table_name: "orders".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            });
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(2), Value::Text("Bob".to_string())],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(3), Value::Text("Charlie".to_string())],
            })
            .unwrap();

        // Delete WHERE id = 2
        let result = executor
            .execute(Statement::Delete {
                table_name: "users".to_string(),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(2))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify only 2 rows remain
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Integer(20 + i)],
                })
                .unwrap();
        }

        // Delete WHERE age > 23
        let result = executor
            .execute(Statement::Delete {
                table_name: "users".to_string(),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(Value::Integer(23))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 row")); // Should delete 2 rows (age 24, 25)
            }
            _ => panic!("Expected Success result"),
        }

        // Verify 3 rows remain
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        for i in 1..=3 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                })
                .unwrap();
        }

        // DELETE without WHERE clause
        let result = executor
            .execute(Statement::Delete {
                table_name: "users".to_string(),
                where_clause: None,
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("3 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify table is empty
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            })
            .unwrap();

        // Delete WHERE id = 999 (doesn't exist)
        let result = executor
            .execute(Statement::Delete {
                table_name: "users".to_string(),
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(999))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("0 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify row still exists
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                    Column::new("active", DataType::Boolean),
                ],
            })
            .unwrap();

        let test_data = vec![(1, 25, true), (2, 30, true), (3, 35, false), (4, 28, true)];

        for (id, age, active) in test_data {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(id),
                        Value::Integer(age),
                        Value::Boolean(active),
                    ],
                })
                .unwrap();
        }

        // Delete WHERE age > 27 AND active = true
        let result = executor
            .execute(Statement::Delete {
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
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 row")); // Should delete Bob (30, true) and Diana (28, true)
            }
            _ => panic!("Expected Success result"),
        }

        // Verify correct rows remain
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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

        let result = executor.execute(Statement::Delete {
            table_name: "nonexistent".to_string(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(1))),
            }),
        });

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
                .execute(Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer)],
                })
                .unwrap();

            for i in 1..=5 {
                executor
                    .execute(Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i)],
                    })
                    .unwrap();
            }

            // Delete id > 3
            executor
                .execute(Statement::Delete {
                    table_name: "users".to_string(),
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Literal(Value::Integer(3))),
                    }),
                })
                .unwrap();
        }

        // Verify deletion persisted
        {
            let mut executor = create_test_executor("test_delete_persist");

            let result = executor
                .execute(Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        // Insert rows
        for i in 1..=3 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                })
                .unwrap();
        }

        // Delete all
        executor
            .execute(Statement::Delete {
                table_name: "users".to_string(),
                where_clause: None,
            })
            .unwrap();

        // Insert new rows
        for i in 10..=12 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                })
                .unwrap();
        }

        // Verify only new rows exist
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Integer(25),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(2),
                    Value::Text("Bob".to_string()),
                    Value::Integer(30),
                ],
            })
            .unwrap();

        // Update WHERE id = 1
        let result = executor
            .execute(Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("age".to_string(), Value::Integer(26))],
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(1))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify update
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(1))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Integer(25),
                ],
            })
            .unwrap();

        // Update multiple columns
        let result = executor
            .execute(Statement::Update {
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
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify both columns updated
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                    Column::new("active", DataType::Boolean),
                ],
            })
            .unwrap();

        for i in 1..=5 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(i),
                        Value::Integer(20 + i),
                        Value::Boolean(true),
                    ],
                })
                .unwrap();
        }

        // Update WHERE age > 23
        let result = executor
            .execute(Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("active".to_string(), Value::Boolean(false))],
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(Expr::Literal(Value::Integer(23))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("2 rows")); // ages 24 and 25
            }
            _ => panic!("Expected Success result"),
        }

        // Verify correct rows updated
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("active".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Boolean(false))),
                }),
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("active", DataType::Boolean),
                ],
            })
            .unwrap();

        for i in 1..=3 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Boolean(true)],
                })
                .unwrap();
        }

        // UPDATE without WHERE clause
        let result = executor
            .execute(Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("active".to_string(), Value::Boolean(false))],
                where_clause: None,
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("3 rows"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify all rows updated
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Integer(25)],
            })
            .unwrap();

        // Update WHERE id = 999 (doesn't exist)
        let result = executor
            .execute(Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("age".to_string(), Value::Integer(30))],
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(999))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("0 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify row unchanged
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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

        let result = executor.execute(Statement::Update {
            table_name: "nonexistent".to_string(),
            assignments: vec![("age".to_string(), Value::Integer(30))],
            where_clause: None,
        });

        assert!(result.is_err());

        cleanup("test_update_no_table");
    }

    #[test]
    fn test_update_nonexistent_column() {
        cleanup("test_update_bad_col");
        let mut executor = create_test_executor("test_update_bad_col");

        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer)],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            })
            .unwrap();

        // Try to update non-existent column
        let result = executor.execute(Statement::Update {
            table_name: "users".to_string(),
            assignments: vec![("nonexistent".to_string(), Value::Integer(30))],
            where_clause: None,
        });

        assert!(result.is_err());

        cleanup("test_update_bad_col");
    }

    #[test]
    fn test_update_type_mismatch() {
        cleanup("test_update_type_err");
        let mut executor = create_test_executor("test_update_type_err");

        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Integer(25)],
            })
            .unwrap();

        // Try to set integer column to text
        let result = executor.execute(Statement::Update {
            table_name: "users".to_string(),
            assignments: vec![("age".to_string(), Value::Text("not a number".to_string()))],
            where_clause: None,
        });

        assert!(result.is_err());

        cleanup("test_update_type_err");
    }

    #[test]
    fn test_update_with_complex_where() {
        cleanup("test_update_complex");
        let mut executor = create_test_executor("test_update_complex");

        executor
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("age", DataType::Integer),
                    Column::new("active", DataType::Boolean),
                ],
            })
            .unwrap();

        let test_data = vec![(1, 25, true), (2, 30, true), (3, 35, false), (4, 28, true)];

        for (id, age, active) in test_data {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(id),
                        Value::Integer(age),
                        Value::Boolean(active),
                    ],
                })
                .unwrap();
        }

        // Update WHERE age > 27 AND active = true
        let result = executor
            .execute(Statement::Update {
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
            })
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
                .execute(Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer),
                        Column::new("age", DataType::Integer),
                    ],
                })
                .unwrap();

            for i in 1..=3 {
                executor
                    .execute(Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Integer(20 + i)],
                    })
                    .unwrap();
            }

            // Update id = 2
            executor
                .execute(Statement::Update {
                    table_name: "users".to_string(),
                    assignments: vec![("age".to_string(), Value::Integer(99))],
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(2))),
                    }),
                })
                .unwrap();
        }

        // Verify update persisted
        {
            let mut executor = create_test_executor("test_update_persist");

            let result = executor
                .execute(Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Column("id".to_string())),
                        op: BinaryOperator::Equals,
                        right: Box::new(Expr::Literal(Value::Integer(2))),
                    }),
                })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            })
            .unwrap();

        executor
            .execute(Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            })
            .unwrap();

        // Update text column
        let result = executor
            .execute(Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("name".to_string(), Value::Text("Alicia".to_string()))],
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(1))),
                }),
            })
            .unwrap();

        match result {
            ExecutionResult::Success { message } => {
                assert!(message.contains("1 row"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify text updated
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Insert 100 rows with large text (~90 bytes per row)
        // This should span multiple pages (4KB page / 90 bytes ≈ 45 rows per page)
        for i in 0..100 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Integer(i),
                        Value::Text("x".repeat(80)), // 80 character string
                    ],
                })
                .unwrap();
        }

        // Verify all rows are readable
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Insert enough data for multiple pages
        for i in 0..100 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                })
                .unwrap();
        }

        // Manually verify page chain
        let first_page = executor.catalog.get_table("users").unwrap().first_page();

        let mut current_page = first_page;
        let mut page_count = 0;
        let mut visited = std::collections::HashSet::new();

        loop {
            // Prevent infinite loops
            assert!(visited.insert(current_page), "Circular page chain detected");

            page_count += 1;

            let page_data = executor.catalog.read_page(current_page).unwrap();
            let page_meta = PageManager::read_metadata_from_buffer(&page_data);

            // Each page should have rows
            assert!(page_meta.num_rows > 0, "Page {} has 0 rows", current_page);

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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Insert 150 rows
        for i in 0..150 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Text("test".to_string())],
                })
                .unwrap();
        }

        // Test: SELECT * returns all rows
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 150);
            }
            _ => panic!("Expected Rows result"),
        }

        // Test: SELECT with WHERE spanning pages
        let result = executor
            .execute(Statement::Select {
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
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Insert 150 rows
        for i in 0..150 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                })
                .unwrap();
        }

        // Delete rows from middle: WHERE id >= 50 AND id < 100
        executor
            .execute(Statement::Delete {
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
            })
            .unwrap();

        // Verify correct rows remain: 0-49 and 100-149 = 100 rows
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Insert 100 rows
        for i in 0..100 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Text("original".to_string())],
                })
                .unwrap();
        }

        // Update subset: SET data = 'UPDATED' WHERE id < 30
        executor
            .execute(Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("data".to_string(), Value::Text("UPDATED".to_string()))],
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::LessThan,
                    right: Box::new(Expr::Literal(Value::Integer(30))),
                }),
            })
            .unwrap();

        // Verify only first 30 rows are updated
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
                .execute(Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("id", DataType::Integer),
                        Column::new("data", DataType::Text),
                    ],
                })
                .unwrap();

            for i in 0..100 {
                executor
                    .execute(Statement::Insert {
                        table_name: "users".to_string(),
                        values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                    })
                    .unwrap();
            }
        } // Drop executor, close database

        // Session 2: Reopen and verify all rows present
        {
            let mut executor = create_test_executor("test_multi_persist");

            let result = executor
                .execute(Statement::Select {
                    table_name: "users".to_string(),
                    columns: SelectColumns::All,
                    where_clause: None,
                })
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
            .execute(Statement::CreateTable {
                name: "table1".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Create table2
        executor
            .execute(Statement::CreateTable {
                name: "table2".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("info", DataType::Text),
                ],
            })
            .unwrap();

        // Insert 80 rows into table1
        for i in 0..80 {
            executor
                .execute(Statement::Insert {
                    table_name: "table1".to_string(),
                    values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                })
                .unwrap();
        }

        // Insert 120 rows into table2
        for i in 0..120 {
            executor
                .execute(Statement::Insert {
                    table_name: "table2".to_string(),
                    values: vec![Value::Integer(i), Value::Text("y".repeat(80))],
                })
                .unwrap();
        }

        // Verify table1
        let result = executor
            .execute(Statement::Select {
                table_name: "table1".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 80);
            }
            _ => panic!("Expected Rows result"),
        }

        // Verify table2
        let result = executor
            .execute(Statement::Select {
                table_name: "table2".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
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
            .execute(Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer),
                    Column::new("data", DataType::Text),
                ],
            })
            .unwrap();

        // Insert 100 rows (multi-page)
        for i in 0..100 {
            executor
                .execute(Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Text("x".repeat(80))],
                })
                .unwrap();
        }

        // Delete all (no WHERE clause)
        executor
            .execute(Statement::Delete {
                table_name: "users".to_string(),
                where_clause: None,
            })
            .unwrap();

        // Verify table is empty
        let result = executor
            .execute(Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            })
            .unwrap();

        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Rows result"),
        }

        cleanup("test_delete_all_mp");
    }
}
