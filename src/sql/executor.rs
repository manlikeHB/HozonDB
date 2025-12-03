use std::io::{self, Error, ErrorKind};

use crate::{
    catalog::{
        row::{Row, Value},
        schema::{Column, Schema},
        table::TableCatalog,
    },
    sql::parser::{SelectColumns, Statement},
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

    pub fn execute(&mut self, statement: Statement) -> io::Result<ExecutionResult> {
        match statement {
            Statement::CreateTable { name, columns } => self.execute_create(name, columns),
            Statement::Insert { table_name, values } => self.execute_insert(table_name, values),
            Statement::Select {
                table_name,
                columns,
            } => self.execute_select(table_name, columns),
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
            let valid = match (value, column.data_type()) {
                (Value::Integer(_), crate::catalog::schema::DataType::Integer) => true,
                (Value::Text(_), crate::catalog::schema::DataType::Text) => true,
                (Value::Boolean(_), crate::catalog::schema::DataType::Boolean) => true,
                (Value::Null, crate::catalog::schema::DataType::Null) => true,
                (Value::Null, _) => true, // NULL can go in any column
                _ => false,
            };

            if !valid {
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

        // Read existing page data
        let mut page_data = self.catalog.read_page(first_page)?;

        let page_meta = PageManager::read_metadata_from_buffer(&page_data);
        let offset = page_meta.last_offset;

        // Serialize new row
        let row_bytes = Row::new(values).to_bytes();

        // Check if it fits
        // TODO: multiple page support
        if offset + row_bytes.len() > PAGE_SIZE {
            return Err(Error::new(
                ErrorKind::OutOfMemory,
                "Page full - multiple page support not yet implemented",
            ));
        }

        // Write row bytes to page
        // TODO: update is_full based on when page is actually full
        page_data[offset..offset + row_bytes.len()].copy_from_slice(&row_bytes);
        let metadata = PageMetadata {
            is_full: page_meta.is_full,
            last_offset: offset + row_bytes.len(),
            num_rows: page_meta.num_rows + 1,
        };

        // update page metadata
        PageManager::update_metadata_in_buffer(&mut page_data, &metadata);

        // Write page back
        self.catalog.write_page(first_page, &page_data)?;

        Ok(ExecutionResult::Success {
            message: "1 row inserted.".to_string(),
        })
    }

    fn execute_select(
        &mut self,
        table_name: String,
        select_columns: SelectColumns,
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

        // Read page data
        let page_data = self.catalog.read_page(first_page)?;
        let page_meta = PageManager::read_metadata_from_buffer(&page_data);
        // Extract column names
        let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        // check if there are any rows in this table
        if page_meta.num_rows == 0 {
            return Ok(ExecutionResult::Rows {
                columns: all_column_names,
                rows: Vec::<Row>::new(),
            });
        }

        // Parse all rows from the page
        let mut rows = Vec::new();
        let mut offset = PAGE_DATA_START;

        for _ in 0..page_meta.num_rows {
            let (row, byte_consumed) = Row::from_bytes(&page_data[offset..])?;
            rows.push(row);
            offset += byte_consumed;
        }

        // Handle column selection
        match select_columns {
            SelectColumns::All => Ok(ExecutionResult::Rows {
                columns: all_column_names,
                rows,
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
                let projected_rows: Vec<Row> = rows
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::{Column, DataType};
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
}
