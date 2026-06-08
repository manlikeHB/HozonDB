use crate::{
    benchmark::metrics::QueryMetrics,
    index::{key::IndexKey, node::leaf::RowLocation},
    sql::{
        database::Database,
        executor::{ExecutionResult, helpers},
    },
};
use std::io::{self, Error, ErrorKind};

use crate::catalog::{row::Value, schema::Column};

pub fn execute_insert(
    db: &mut Database,
    table_name: String,
    values: Vec<Value>,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<ExecutionResult> {
    let (_, columns) = helpers::get_table_first_page_and_cols(db, &table_name)?;
    let columns = columns.to_vec();

    // Validate value count
    if values.len() != columns.len() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("Expected {} values, got {}", columns.len(), values.len()),
        ));
    }

    let value_and_col_pairs: Vec<(&Value, &Column)> = values.iter().zip(columns.iter()).collect();

    // get table indexes
    let index_entries = db
        .get_indexes_for_table(&table_name)
        .map(|entries| entries.to_vec())
        .unwrap_or_default();

    // Validate data types
    for (value, column) in &value_and_col_pairs {
        if !helpers::validate_value_type(value, column.data_type()) {
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

        helpers::validate_index_key_length(value, column, &index_entries)?;
    }

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

            if let Some(_) =
                db.search_index(entry.index_name(), &IndexKey::try_from((*val).clone())?)?
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
    let last_page = db.get_table_last_page(&table_name).ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            format!("Last page for {} not found", &table_name),
        )
    })?;

    // insert row
    let (row_page_id, slot) =
        helpers::insert_row_into_page(db, &table_name, last_page, &values, metrics)?;

    // Index new row if table was indexed
    let row_location = RowLocation::new(row_page_id, slot);
    helpers::index_new_row(db, &index_entries, &value_and_col_pairs, row_location)?;

    if let Some(m) = metrics.as_mut() {
        m.rows_modified += 1;
    }

    Ok(ExecutionResult::Success {
        message: "1 row inserted.".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::schema::DataType,
        constants,
        sql::{
            executor::test_helpers::*,
            parser::{SelectColumns, Statement},
        },
        storage::page::{PageManager, PageType},
    };

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
            let page_meta = PageManager::read_metadata_from_buffer(&page_data, PageType::Slotted);

            // Each page should have rows
            assert!(
                page_meta.slot_count().unwrap() > 0,
                "Page {} has 0 rows",
                current_page
            );

            match page_meta.next_page().unwrap() {
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

            executor.database.checkpoint().unwrap();
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
    fn test_insert_text_index_key_exceeds_max_bytes_rejected() {
        cleanup("test_insert_text_key_too_long");
        let mut executor = create_test_executor("test_insert_text_key_too_long");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![
                        Column::new("email", DataType::Text, true),
                        Column::new("name", DataType::Text, false),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // exactly at the limit — should succeed
        let valid_email = "a".repeat(constants::MAX_TEXT_INDEX_KEY_BYTES);
        let result = executor.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Text(valid_email.clone()),
                    Value::Text("Alice".to_string()),
                ],
            },
            &mut None,
        );
        assert!(
            result.is_ok(),
            "insert at exactly MAX_TEXT_INDEX_KEY_BYTES should succeed"
        );

        // one byte over the limit — should fail
        let invalid_email = "a".repeat(constants::MAX_TEXT_INDEX_KEY_BYTES + 1);
        let result = executor.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Text(invalid_email), Value::Text("Bob".to_string())],
            },
            &mut None,
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);

        // verify only the valid row was inserted
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
                assert_eq!(rows[0].get_value(0), Some(&Value::Text(valid_email)));
            }
            _ => panic!("expected rows"),
        }

        cleanup("test_insert_text_key_too_long");
    }
}
