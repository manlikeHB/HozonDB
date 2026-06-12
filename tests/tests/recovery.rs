mod common;

use common::{cleanup, create_executor, row_count, row_values, select_all};
use hozondb_core::catalog::row::Value;
use hozondb_core::catalog::schema::{Column, DataType};
use hozondb_core::sql::parser::{BinaryOperator, Expr, SelectColumns, Statement};

#[test]
fn test_recovery_single_insert() {
    cleanup("rec_single_insert");

    {
        let mut ex = create_executor("rec_single_insert");
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
        // no checkpoint — simulates crash
    }

    {
        let mut ex = create_executor("rec_single_insert");
        assert_eq!(row_count(select_all(&mut ex, "users")), 1);
    }

    cleanup("rec_single_insert");
}

#[test]
fn test_recovery_multiple_inserts() {
    cleanup("rec_multi_insert");

    {
        let mut ex = create_executor("rec_multi_insert");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        for i in 1..=5 {
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                },
                &mut None,
            )
            .unwrap();
        }
        // no checkpoint
    }

    {
        let mut ex = create_executor("rec_multi_insert");
        assert_eq!(row_count(select_all(&mut ex, "users")), 5);
    }

    cleanup("rec_multi_insert");
}

#[test]
fn test_recovery_delete() {
    cleanup("rec_delete");

    {
        let mut ex = create_executor("rec_delete");
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
        }

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        ex.execute(
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
        // crash
    }

    {
        let mut ex = create_executor("rec_delete");
        let values = row_values(select_all(&mut ex, "users"));
        assert_eq!(values.len(), 2);
        let ids: Vec<i32> = values
            .iter()
            .map(|r| match r[0] {
                Value::Integer(i) => i,
                _ => panic!("expected integer"),
            })
            .collect();
        assert!(!ids.contains(&2));
    }

    cleanup("rec_delete");
}

#[test]
fn test_recovery_update() {
    cleanup("rec_update");

    {
        let mut ex = create_executor("rec_update");
        ex.execute(
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

        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        ex.execute(
            Statement::Update {
                table_name: "users".to_string(),
                assignments: vec![("name".to_string(), Value::Text("Bob".to_string()))],
                where_clause: None,
            },
            &mut None,
        )
        .unwrap();
        // crash
    }

    {
        let mut ex = create_executor("rec_update");
        let values = row_values(select_all(&mut ex, "users"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][1], Value::Text("Bob".to_string()));
    }

    cleanup("rec_update");
}

#[test]
fn test_recovery_multi_page_table() {
    cleanup("rec_multi_page");

    {
        let mut ex = create_executor("rec_multi_page");
        ex.execute(
            Statement::CreateTable {
                name: "logs".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer, false),
                    Column::new("msg", DataType::Text, false),
                ],
            },
            &mut None,
        )
        .unwrap();

        for i in 0..200 {
            ex.execute(
                Statement::Insert {
                    table_name: "logs".to_string(),
                    values: vec![
                        Value::Integer(i),
                        Value::Text(format!("log message number {}", i)),
                    ],
                },
                &mut None,
            )
            .unwrap();
        }
        // no checkpoint
    }

    {
        let mut ex = create_executor("rec_multi_page");
        assert_eq!(row_count(select_all(&mut ex, "logs")), 200);
    }

    cleanup("rec_multi_page");
}

#[test]
fn test_recovery_index_consistent_after_crash() {
    cleanup("rec_index");

    {
        let mut ex = create_executor("rec_index");
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
        // no checkpoint
    }

    {
        let mut ex = create_executor("rec_index");
        let result = ex
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

        let values = row_values(result);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(3));
    }

    cleanup("rec_index");
}

#[test]
fn test_recovery_is_idempotent() {
    cleanup("rec_idempotent");

    {
        let mut ex = create_executor("rec_idempotent");
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
        }
        // no checkpoint
        ex.execute(Statement::Checkpoint, &mut None).unwrap();
    }

    // recover twice — result should be identical
    {
        let mut ex = create_executor("rec_idempotent");
        assert_eq!(row_count(select_all(&mut ex, "users")), 3);
    }

    {
        let mut ex = create_executor("rec_idempotent");
        assert_eq!(row_count(select_all(&mut ex, "users")), 3);
    }

    cleanup("rec_idempotent");
}

#[test]
fn test_recovery_drop_table() {
    cleanup("rec_drop_table");

    {
        let mut ex = create_executor("rec_drop_table");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::CreateTable {
                name: "orders".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        ex.execute(
            Statement::DropTable {
                name: "users".to_string(),
            },
            &mut None,
        )
        .unwrap();
        // crash
    }

    {
        let mut ex = create_executor("rec_drop_table");

        // users should not exist
        let result = ex.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );
        assert!(result.is_err());

        // orders should still exist
        assert_eq!(row_count(select_all(&mut ex, "orders")), 0);
    }

    cleanup("rec_drop_table");
}

#[test]
fn test_recovery_multi_page_link_preserved() {
    cleanup("rec_page_link");

    {
        let mut ex = create_executor("rec_page_link");
        ex.execute(
            Statement::CreateTable {
                name: "logs".to_string(),
                columns: vec![
                    Column::new("id", DataType::Integer, false),
                    Column::new("data", DataType::Text, false),
                ],
            },
            &mut None,
        )
        .unwrap();

        // insert enough to force a second page
        for i in 0..100 {
            ex.execute(
                Statement::Insert {
                    table_name: "logs".to_string(),
                    values: vec![Value::Integer(i), Value::Text("a".repeat(40))],
                },
                &mut None,
            )
            .unwrap();
        }

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        // insert more rows after checkpoint — tests page link recovery
        for i in 100..150 {
            ex.execute(
                Statement::Insert {
                    table_name: "logs".to_string(),
                    values: vec![Value::Integer(i), Value::Text("a".repeat(40))],
                },
                &mut None,
            )
            .unwrap();
        }
        // crash
    }

    {
        let mut ex = create_executor("rec_page_link");
        assert_eq!(row_count(select_all(&mut ex, "logs")), 150);
    }

    cleanup("rec_page_link");
}

#[test]
fn test_recovery_free_page_reuse_indexed() {
    cleanup("rec_free_page_indexed");

    {
        let mut ex = create_executor("rec_free_page_indexed");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, true)],
            },
            &mut None,
        )
        .unwrap();

        for i in 1..=50 {
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                },
                &mut None,
            )
            .unwrap();
        }

        // drop table — frees pages
        ex.execute(
            Statement::DropTable {
                name: "users".to_string(),
            },
            &mut None,
        )
        .unwrap();

        // create new table — should reuse freed pages
        ex.execute(
            Statement::CreateTable {
                name: "orders".to_string(),
                columns: vec![Column::new("id", DataType::Integer, true)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "orders".to_string(),
                values: vec![Value::Integer(99)],
            },
            &mut None,
        )
        .unwrap();
        // crash
    }

    {
        let mut ex = create_executor("rec_free_page_indexed");

        // users gone, orders with correct data
        let result = ex.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );
        assert!(result.is_err());

        let values = row_values(select_all(&mut ex, "orders"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(99));
    }

    cleanup("rec_free_page_indexed");
}

#[test]
fn test_recovery_free_page_reuse_non_indexed() {
    cleanup("rec_free_page_non_indexed");

    {
        let mut ex = create_executor("rec_free_page_non_indexed");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        for i in 1..=50 {
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                },
                &mut None,
            )
            .unwrap();
        }

        // drop table — frees pages
        ex.execute(
            Statement::DropTable {
                name: "users".to_string(),
            },
            &mut None,
        )
        .unwrap();

        // create new table — should reuse freed pages
        ex.execute(
            Statement::CreateTable {
                name: "orders".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "orders".to_string(),
                values: vec![Value::Integer(99)],
            },
            &mut None,
        )
        .unwrap();
        // crash
    }

    {
        let mut ex = create_executor("rec_free_page_non_indexed");

        // users gone, orders with correct data
        let result = ex.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );
        assert!(result.is_err());

        let values = row_values(select_all(&mut ex, "orders"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(99));
    }

    cleanup("rec_free_page_non_indexed");
}

#[test]
fn test_recovery_free_page_reuse_wal_ordering() {
    // Verifies that when pages are freed and reallocated from the free list
    // without a checkpoint, recovery correctly restores the reallocated page
    // with the right data — not stale free page content.
    // This exercises the WAL ordering around free list allocation:
    // the AllocatePage record must be logged before the free list head
    // is updated, otherwise recovery cannot reinitialize the page.
    cleanup("rec_alloc_wal_order");

    {
        let mut ex = create_executor("rec_alloc_wal_order");

        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        for i in 1..=50 {
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                },
                &mut None,
            )
            .unwrap();
        }

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        // free pages by dropping the table
        ex.execute(
            Statement::DropTable {
                name: "users".to_string(),
            },
            &mut None,
        )
        .unwrap();

        // reallocate from free list — no checkpoint before crash
        ex.execute(
            Statement::CreateTable {
                name: "orders".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "orders".to_string(),
                values: vec![Value::Integer(42)],
            },
            &mut None,
        )
        .unwrap();
        // crash — free list reallocation only in WAL
    }

    {
        let mut ex = create_executor("rec_alloc_wal_order");

        // users dropped — should not exist
        let result = ex.execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );
        assert!(result.is_err());

        // orders should exist with correct data — not stale free page content
        let values = row_values(select_all(&mut ex, "orders"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(42));
    }

    cleanup("rec_alloc_wal_order");
}

#[test]
fn test_recovery_free_list_head_not_stale_after_alloc() {
    // Verifies that after recovery, a page that was allocated from the
    // free list before the crash is not still listed as the free list head.
    // recover_allocate_page must update PageManager::first_free_page
    // to reflect the allocation — otherwise the next allocate_page call
    // would hand out a page already in use.
    cleanup("rec_free_list_stale");

    {
        let mut ex = create_executor("rec_free_list_stale");

        // create and populate a table
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        for i in 1..=20 {
            ex.execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(i)],
                },
                &mut None,
            )
            .unwrap();
        }

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        // free pages — free list now has entries on disk
        ex.execute(
            Statement::DropTable {
                name: "users".to_string(),
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Checkpoint, &mut None).unwrap();

        // allocate from free list — no checkpoint before crash
        ex.execute(
            Statement::CreateTable {
                name: "orders".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "orders".to_string(),
                values: vec![Value::Integer(1)],
            },
            &mut None,
        )
        .unwrap();

        // crash — AllocatePage logged but free list header not updated on disk
    }

    {
        let mut ex = create_executor("rec_free_list_stale");

        // orders should be intact
        let values = row_values(select_all(&mut ex, "orders"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(1));

        // now allocate again — if free list head is stale, this hands out
        // a page already used by orders, corrupting it
        ex.execute(
            Statement::CreateTable {
                name: "logs".to_string(),
                columns: vec![Column::new("id", DataType::Integer, false)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(
            Statement::Insert {
                table_name: "logs".to_string(),
                values: vec![Value::Integer(99)],
            },
            &mut None,
        )
        .unwrap();

        // orders must still have correct data — not corrupted by stale free page reuse
        let values = row_values(select_all(&mut ex, "orders"));
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], Value::Integer(1));
    }

    cleanup("rec_free_list_stale");
}
