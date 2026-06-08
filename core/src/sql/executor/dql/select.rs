use crate::{
    benchmark::metrics::QueryMetrics,
    sql::{
        database::Database,
        executor::{ExecutionResult, helpers},
    },
};
use std::io::{self, Error, ErrorKind};

use crate::{
    catalog::row::{Row, Value},
    sql::parser::{Expr, SelectColumns},
};

// TODO: Stream rows incrementally instead of collecting into Vec<Row>.
// Currently all rows are buffered in memory before returning, which means
// server-side gRPC streaming sends everything at once under the hood.
// Fix: change ExecutionResult::Rows to yield rows page-by-page, likely
// via a channel (tokio::sync::mpsc) or an async iterator, so the server
// can stream rows to the client as pages are read from disk.
pub fn execute_select(
    db: &mut Database,
    table_name: String,
    select_columns: SelectColumns,
    where_clause: Option<Expr>,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<ExecutionResult> {
    let (first_page, columns) = helpers::get_table_first_page_and_cols(db, &table_name)?;

    // Extract column names
    let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

    let filtered_rows_and_loc = helpers::resolve_rows(
        db,
        &table_name,
        first_page,
        &where_clause,
        &all_column_names,
        metrics,
    )?;

    // check if there are any rows in this table
    if filtered_rows_and_loc.len() == 0 {
        return Ok(ExecutionResult::Rows {
            columns: all_column_names,
            rows: Vec::<Row>::new(),
        });
    }

    // Handle column selection
    match select_columns {
        SelectColumns::All => Ok(ExecutionResult::Rows {
            columns: all_column_names,
            rows: filtered_rows_and_loc
                .into_iter()
                .map(|(row, _)| row)
                .collect(),
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
            let projected_rows: Vec<Row> = filtered_rows_and_loc
                .iter()
                .map(|(row, _)| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::schema::{Column, DataType},
        sql::{
            executor::test_helpers::*,
            parser::{BinaryOperator, SelectColumns, Statement},
        },
        storage::page::{PAGE_SIZE, PageType, SLOT_DIRECTORY_START},
    };

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
        let metadata = executor
            .database
            .read_page_metadata(first_page, PageType::Slotted)
            .unwrap();
        assert_eq!(metadata.slot_count().unwrap(), 0);
        assert_eq!(
            metadata.free_space_start().unwrap() as usize,
            SLOT_DIRECTORY_START
        );
        assert_eq!(metadata.free_space_end().unwrap() as usize, PAGE_SIZE);

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
        let metadata = executor
            .database
            .read_page_metadata(first_page, PageType::Slotted)
            .unwrap();
        assert_eq!(metadata.slot_count().unwrap(), 1);
        assert_ne!(
            metadata.free_space_start().unwrap() as usize,
            SLOT_DIRECTORY_START
        );
        assert_ne!(metadata.free_space_end().unwrap() as usize, PAGE_SIZE);

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
}
