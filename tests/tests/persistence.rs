mod common;

use common::{cleanup, create_executor, row_count, row_values, select_all};
use hozondb_core::catalog::row::Value;
use hozondb_core::catalog::schema::{Column, DataType};
use hozondb_core::sql::parser::Statement;

#[test]
fn test_clean_shutdown_data_survives() {
    cleanup("persist_clean_shutdown");

    {
        let mut ex = create_executor("persist_clean_shutdown");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(42)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Checkpoint, &mut None).unwrap();
    }

    {
        let mut ex = create_executor("persist_clean_shutdown");
        let values = row_values(select_all(&mut ex, "users"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(42));
    }

    cleanup("persist_clean_shutdown");
}

#[test]
fn test_checkpoint_then_crash_recovers_remaining() {
    cleanup("persist_checkpoint_crash");

    {
        let mut ex = create_executor("persist_checkpoint_crash");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Checkpoint, &mut None).unwrap(); // row 1 safe on disk

        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(2)],
            },
            &mut None,
        )
        .unwrap();
        // crash — row 2 only in WAL
    }

    {
        let mut ex = create_executor("persist_checkpoint_crash");
        assert_eq!(row_count(select_all(&mut ex, "users")), 2);
    }

    cleanup("persist_checkpoint_crash");
}

#[test]
fn test_multiple_checkpoints_data_survives() {
    cleanup("persist_multi_checkpoint");

    {
        let mut ex = create_executor("persist_multi_checkpoint");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        for i in 1..=3 {
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                },
                &mut None,
            )
            .unwrap();
            ex.execute(Statement::Checkpoint, &mut None).unwrap(); // checkpoint after each insert
        }
    }

    {
        let mut ex = create_executor("persist_multi_checkpoint");
        assert_eq!(row_count(select_all(&mut ex, "users")), 3);
    }

    cleanup("persist_multi_checkpoint");
}

#[test]
fn test_table_schema_persists_across_restart() {
    cleanup("persist_schema");

    {
        let mut ex = create_executor("persist_schema");
        ex.execute(
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

        ex.execute(Statement::Checkpoint, &mut None).unwrap();
    }

    {
        let mut ex = create_executor("persist_schema");

        // table should exist with correct schema
        let result = ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Boolean(true),
                ],
            },
            &mut None,
        );

        assert!(result.is_ok());
    }

    cleanup("persist_schema");
}

#[test]
fn test_index_persists_across_restart() {
    cleanup("persist_index");

    {
        let mut ex = create_executor("persist_index");
        ex.execute(
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
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i), Value::Text(format!("user{}", i))],
                },
                &mut None,
            )
            .unwrap();
        }

        ex.execute(Statement::Checkpoint, &mut None).unwrap();
    }

    {
        let mut ex = create_executor("persist_index");

        // point lookup via index
        let result = ex
            .execute(
                Statement::Select {
                    table_name: "users".to_string(),
                    columns: hozondb_core::sql::parser::SelectColumns::All,
                    where_clause: Some(hozondb_core::sql::parser::Expr::BinaryOp {
                        left: Box::new(hozondb_core::sql::parser::Expr::Column("id".to_string())),
                        op: hozondb_core::sql::parser::BinaryOperator::Equals,
                        right: Box::new(hozondb_core::sql::parser::Expr::Literal(Value::Integer(
                            4,
                        ))),
                    }),
                },
                &mut None,
            )
            .unwrap();

        let values = row_values(result);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(4));
    }

    cleanup("persist_index");
}

#[test]
fn test_empty_table_persists() {
    cleanup("persist_empty_table");

    {
        let mut ex = create_executor("persist_empty_table");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Checkpoint, &mut None).unwrap();
    }

    {
        let mut ex = create_executor("persist_empty_table");
        assert_eq!(row_count(select_all(&mut ex, "users")), 0);
    }

    cleanup("persist_empty_table");
}
