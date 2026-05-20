use crate::sql::{
    database::Database,
    executor::{
        ExecutionResult,
        helpers::{collect_page_chain, get_table_first_page_and_cols},
    },
};
use std::io;

pub fn execute_drop_table(db: &mut Database, table_name: String) -> io::Result<ExecutionResult> {
    let (first_page, _) = get_table_first_page_and_cols(db, &table_name)?;

    let page_chain = collect_page_chain(db.pm(), first_page)?;

    db.drop_table(&table_name)?;

    for page in page_chain {
        db.free_page(page)?;
    }

    // TODO: drop_table does not free B+ tree index pages — only the index
    // catalog entry is removed. Index pages are orphaned on disk until
    // compaction. Fixing this requires BPlusTree to expose a full page
    // traversal method.
    db.drop_table_indexes(&table_name)?;

    Ok(ExecutionResult::Success {
        message: format!("{} table successfully dropped", table_name),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        catalog::{
            row::Value,
            schema::{Column, DataType},
        },
        sql::{
            executor::test_helpers::*,
            parser::{SelectColumns, Statement},
        },
    };

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
}
