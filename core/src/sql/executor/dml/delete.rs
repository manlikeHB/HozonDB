use crate::{
    benchmark::metrics::QueryMetrics,
    sql::{
        database::Database,
        executor::{ExecutionResult, helpers},
    },
    storage::page::PageType,
    wal::record_type::WalRecordType,
};
use std::io;

use crate::{
    catalog::{row::Value, schema::Column},
    sql::parser::Expr,
    storage::page::PageManager,
};

// TODO: dead slots are never reclaimed within a page. A compaction pass is
// needed to pack live rows together, reset free_space_start/free_space_end,
// and return fully-dead pages to the free list. Without this, repeated
// deletes gradually waste page space permanently.
// TODO: update tables last page when table is compacted
pub fn execute_delete(
    db: &mut Database,
    table_name: String,
    where_clause: Option<Expr>,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<ExecutionResult> {
    let (first_page, columns) = helpers::get_table_first_page_and_cols(db, &table_name)?;
    let columns = columns.to_vec();

    // Extract column names
    let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

    // get all affected rows with row location
    let rows_and_loc = helpers::resolve_rows(
        db,
        &table_name,
        first_page,
        &where_clause,
        &all_column_names,
        metrics,
    )?;

    // check if there are any rows in this table
    if rows_and_loc.len() == 0 {
        return Ok(ExecutionResult::Success {
            message: "0 rows deleted".to_string(),
        });
    }

    // filter rows based on the where clause
    let mut deleted_rows = Vec::new();
    // let (wal_writer, buffer_pool) = db.get_wal_and_buffer_pool();

    for (row, loc) in rows_and_loc {
        // log delete record
        let lsn = db.wal_append_slotted(
            WalRecordType::Delete,
            &table_name,
            loc.page_id(),
            loc.slot(),
            &[],
            &row.to_bytes(), // old_data = the row being deleted
        )?;

        // mark slot dead
        let page_data = db.get_page_mut(loc.page_id())?;
        PageManager::mark_slot_dead(page_data, loc.slot());

        // update page meta lsn
        let mut page_meta = PageManager::read_metadata_from_buffer(page_data, PageType::Slotted);
        page_meta.set_lsn(lsn);
        PageManager::update_metadata_in_buffer(page_data, &page_meta);

        // mark page dirty
        db.mark_dirty(loc.page_id(), lsn);

        deleted_rows.push(row);
    }

    // TODO: track number of pages that were written to for metrics

    // delete indexed keys
    let index_entries = db
        .get_indexes_for_table(&table_name)
        .map(|e| e.to_vec())
        .unwrap_or_default();

    if index_entries.len() > 0 {
        for row in &deleted_rows {
            let value_and_col_pairs: Vec<(&Value, &Column)> =
                row.values().iter().zip(columns.iter()).collect();
            helpers::delete_indexes(db, &index_entries, &value_and_col_pairs)?;
        }
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        catalog::schema::DataType,
        sql::{
            executor::test_helpers::*,
            parser::{BinaryOperator, SelectColumns, Statement},
        },
    };

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

            executor.database.checkpoint().unwrap();
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
}
