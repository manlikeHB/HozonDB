use crate::{
    benchmark::metrics::QueryMetrics,
    index::node::leaf::RowLocation,
    sql::{
        database::Database,
        executor::{ExecutionResult, helpers},
    },
    storage::page::PageType,
    wal::record_type::WalRecordType,
};
use std::io::{self, Error, ErrorKind};

use crate::{
    catalog::{
        row::{Row, Value},
        schema::Column,
    },
    sql::parser::Expr,
    storage::page::PageManager,
};

pub fn execute_update(
    db: &mut Database,
    table_name: String,
    assignments: Vec<(String, Value)>,
    where_clause: Option<Expr>,
    metrics: &mut Option<QueryMetrics>,
) -> io::Result<ExecutionResult> {
    let (first_page, columns) = helpers::get_table_first_page_and_cols(db, &table_name)?;

    let columns = columns.to_vec();

    // Extract column names
    let all_column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

    // get last page
    let last_page = db.get_table_last_page(&table_name).ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            format!("Last page for {} not found", &table_name),
        )
    })?;

    let index_entries = db
        .get_indexes_for_table(&table_name)
        .map(|e| e.to_vec())
        .unwrap_or_default();

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

        if !helpers::validate_value_type(value, column.data_type()) {
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

        helpers::validate_index_key_length(value, column, &index_entries)?;
    }

    let rows_and_locs = helpers::resolve_rows(
        db,
        &table_name,
        first_page,
        &where_clause,
        &all_column_names,
        metrics,
    )?;

    // Check if table is empty
    if rows_and_locs.len() == 0 {
        return Ok(ExecutionResult::Success {
            message: "0 rows updated.".to_string(),
        });
    }

    // Update rows based on WHERE clause
    let mut updated_count = 0;
    let mut deleted_rows = Vec::new();
    for (row, loc) in rows_and_locs {
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

        let old_bytes = row.to_bytes();
        let new_bytes = updated_row.to_bytes();

        // if updated row fit previous space, write into old location else re-insert
        if new_bytes.len() <= old_bytes.len() {
            let lsn = db.wal_append_slotted(
                WalRecordType::Update,
                &table_name,
                loc.page_id(),
                loc.slot(),
                &new_bytes,
                &old_bytes,
            )?;

            let page_data = db.get_page_mut(loc.page_id())?;
            let (row_offset, _) = PageManager::read_slot(page_data, loc.slot());

            // write new row
            page_data[row_offset as usize..row_offset as usize + new_bytes.len()]
                .copy_from_slice(&new_bytes);
            // update slot
            PageManager::write_slot(page_data, loc.slot(), row_offset, new_bytes.len() as u16);

            // update page meta lsn
            let mut page_meta =
                PageManager::read_metadata_from_buffer(page_data, PageType::Slotted);
            page_meta.set_lsn(lsn);
            PageManager::update_metadata_in_buffer(page_data, &page_meta);

            // mark page dirty
            db.mark_dirty(loc.page_id(), lsn)?;
            if let Some(m) = metrics.as_mut() {
                m.pages_dirtied += 1;
            }

            // delete old row index
            helpers::delete_indexes(db, &index_entries, &old_value_and_col_pairs)?;
            // index row
            helpers::index_new_row(db, &index_entries, &new_value_and_col_pairs, loc)?;
        } else {
            // insert updated row as new row
            let (row_page_id, slot) = helpers::insert_row_into_page(
                db,
                &table_name,
                last_page,
                &updated_row.values(),
                metrics,
            )?;

            // TODO: should indexing newly inserted updated row come before deleting old row?
            // index new row
            let row_location = RowLocation::new(row_page_id, slot);
            helpers::index_new_row(db, &index_entries, &new_value_and_col_pairs, row_location)?;

            // delete old row
            // log delete WAL record
            let lsn = db.wal_append_slotted(
                WalRecordType::Delete,
                &table_name,
                loc.page_id(),
                loc.slot(),
                &vec![],
                &old_bytes,
            )?;

            // mark slot dead
            let page_data = db.get_page_mut(loc.page_id())?;
            PageManager::mark_slot_dead(page_data, loc.slot());

            // update page meta lsn
            let mut page_meta =
                PageManager::read_metadata_from_buffer(page_data, PageType::Slotted);
            page_meta.set_lsn(lsn);
            PageManager::update_metadata_in_buffer(page_data, &page_meta);

            // mark page dirty
            db.mark_dirty(loc.page_id(), lsn)?;
            if let Some(m) = metrics.as_mut() {
                m.pages_dirtied += 1;
            }

            // collect old row to be freed if indexed
            deleted_rows.push(row);
        }
    }

    // delete indexed keys for deleted rows
    if index_entries.len() > 0 {
        for row in &deleted_rows {
            let value_and_col_pairs: Vec<(&Value, &Column)> =
                row.values().iter().zip(columns.iter()).collect();
            helpers::delete_indexes(db, &index_entries, &value_and_col_pairs)?;
        }
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        catalog::schema::DataType,
        constants,
        sql::parser::{BinaryOperator, SelectColumns, Statement},
        test_helpers::*,
    };

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

            executor.database.checkpoint().unwrap();
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
    fn test_update_text_index_key_exceeds_max_bytes_rejected() {
        cleanup("test_update_text_key_too_long");
        let mut executor = create_test_executor("test_update_text_key_too_long");

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

        // insert a valid row
        let valid_email = "alice@example.com".to_string();
        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![
                        Value::Text(valid_email.clone()),
                        Value::Text("Alice".to_string()),
                    ],
                },
                &mut None,
            )
            .unwrap();

        // update with a value that exceeds the limit — should fail
        let invalid_email = "a".repeat(constants::MAX_TEXT_INDEX_KEY_BYTES + 1);
        let result = executor.execute(
            Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("email".to_string(), Value::Text(invalid_email))],
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("email".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Text(valid_email.clone()))),
                }),
            },
            &mut None,
        );

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);

        // verify original row is unchanged
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

        cleanup("test_update_text_key_too_long");
    }
}
