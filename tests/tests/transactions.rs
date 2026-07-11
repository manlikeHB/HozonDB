mod common;

use common::{cleanup, create_executor, row_count, row_values, select_all};
use hozondb_core::catalog::row::Value;
use hozondb_core::catalog::schema::{Column, DataType};
use hozondb_core::sql::parser::{BinaryOperator, Expr, SelectColumns, Statement};

#[test]
fn test_rollback_insert_not_visible_in_same_session() {
    cleanup("txn_rollback_insert");

    let mut ex = create_executor("txn_rollback_insert");
    ex.execute(
        Statement::CreateTable {
            name: "users".to_string(),
            columns: vec![Column::new("id", DataType::Integer, true)],
        },
        &mut None,
    )
    .unwrap();

    ex.execute(Statement::Begin, &mut None).unwrap();
    ex.execute(
        Statement::Insert {
            table_name: "users".to_string(),
            values: vec![Value::Integer(1)],
        },
        &mut None,
    )
    .unwrap();
    ex.execute(Statement::RollBack, &mut None).unwrap();

    assert_eq!(row_count(select_all(&mut ex, "users")), 0);

    cleanup("txn_rollback_insert");
}

#[test]
fn test_rollback_update_restores_value_in_same_session() {
    cleanup("txn_rollback_update");

    let mut ex = create_executor("txn_rollback_update");
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

    ex.execute(
        Statement::Insert {
            table_name: "users".to_string(),
            values: vec![Value::Integer(1), Value::Text("Alice".to_string())],
        },
        &mut None,
    )
    .unwrap();

    ex.execute(Statement::Begin, &mut None).unwrap();
    ex.execute(
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
    ex.execute(Statement::RollBack, &mut None).unwrap();

    let values = row_values(select_all(&mut ex, "users"));
    assert_eq!(values.len(), 1);
    assert_eq!(values[0][1], Value::Text("Alice".to_string()));

    cleanup("txn_rollback_update");
}

#[test]
fn test_rollback_delete_restores_row_in_same_session() {
    cleanup("txn_rollback_delete");

    let mut ex = create_executor("txn_rollback_delete");
    ex.execute(
        Statement::CreateTable {
            name: "users".to_string(),
            columns: vec![Column::new("id", DataType::Integer, true)],
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

    ex.execute(Statement::Begin, &mut None).unwrap();
    ex.execute(
        Statement::Delete {
            table_name: "users".to_string(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(1))),
            }),
        },
        &mut None,
    )
    .unwrap();
    ex.execute(Statement::RollBack, &mut None).unwrap();

    assert_eq!(row_count(select_all(&mut ex, "users")), 1);

    cleanup("txn_rollback_delete");
}

#[test]
fn test_explicit_commit_persists_across_restart() {
    cleanup("txn_commit_restart");

    {
        let mut ex = create_executor("txn_commit_restart");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, true)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Begin, &mut None).unwrap();
        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            },
            &mut None,
        )
        .unwrap();
        ex.execute(Statement::Commit, &mut None).unwrap();
        // crash — no checkpoint, relies on WAL redo of the committed txn
    }

    {
        let mut ex = create_executor("txn_commit_restart");
        assert_eq!(row_count(select_all(&mut ex, "users")), 1);
    }

    cleanup("txn_commit_restart");
}

#[test]
fn test_rollback_then_restart_data_not_resurrected() {
    cleanup("txn_rollback_restart");

    {
        let mut ex = create_executor("txn_rollback_restart");
        ex.execute(
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![Column::new("id", DataType::Integer, true)],
            },
            &mut None,
        )
        .unwrap();

        ex.execute(Statement::Begin, &mut None).unwrap();
        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![Value::Integer(1)],
            },
            &mut None,
        )
        .unwrap();
        ex.execute(Statement::RollBack, &mut None).unwrap();
        // crash — the Abort record (not just the in-memory rollback) is what
        // must keep this insert from coming back during WAL redo
    }

    {
        let mut ex = create_executor("txn_rollback_restart");
        assert_eq!(row_count(select_all(&mut ex, "users")), 0);
    }

    cleanup("txn_rollback_restart");
}

#[test]
fn test_rollback_multi_page_insert_removes_all_rows() {
    cleanup("txn_rollback_multi_page");

    let mut ex = create_executor("txn_rollback_multi_page");
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

    ex.execute(Statement::Begin, &mut None).unwrap();
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
    ex.execute(Statement::RollBack, &mut None).unwrap();

    assert_eq!(row_count(select_all(&mut ex, "logs")), 0);

    cleanup("txn_rollback_multi_page");
}

#[test]
#[ignore]
fn test_rollback_after_index_split_leaves_index_consistent() {
    cleanup("txn_rollback_index_split");

    let mut ex = create_executor("txn_rollback_index_split");
    ex.execute(
        Statement::CreateTable {
            name: "users".to_string(),
            // Text primary key -> small btree order (15), splits after a
            // handful of inserts instead of needing hundreds
            columns: vec![
                Column::new("id", DataType::Text, true),
                Column::new("name", DataType::Text, false),
            ],
        },
        &mut None,
    )
    .unwrap();

    ex.execute(Statement::Begin, &mut None).unwrap();
    for i in 0..40 {
        ex.execute(
            Statement::Insert {
                table_name: "users".to_string(),
                values: vec![
                    Value::Text(format!("user-{:03}", i)),
                    Value::Text(format!("Name {}", i)),
                ],
            },
            &mut None,
        )
        .unwrap();
    }
    ex.execute(Statement::RollBack, &mut None).unwrap();

    assert_eq!(row_count(select_all(&mut ex, "users")), 0);

    // point lookup via the rolled-back index should find nothing
    let result = ex
        .execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Text("user-010".to_string()))),
                }),
            },
            &mut None,
        )
        .unwrap();
    assert_eq!(row_count(result), 0);

    // re-inserting a rolled-back key must succeed — a stale index entry
    // left behind by an incomplete split-undo would surface here
    ex.execute(
        Statement::Insert {
            table_name: "users".to_string(),
            values: vec![
                Value::Text("user-010".to_string()),
                Value::Text("fresh insert".to_string()),
            ],
        },
        &mut None,
    )
    .unwrap();
    assert_eq!(row_count(select_all(&mut ex, "users")), 1);

    cleanup("txn_rollback_index_split");
}

#[test]
fn test_explicit_multi_page_commit_persists_across_restart() {
    cleanup("txn_commit_multi_page_restart");

    {
        let mut ex = create_executor("txn_commit_multi_page_restart");
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

        ex.execute(Statement::Begin, &mut None).unwrap();
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
        ex.execute(Statement::Commit, &mut None).unwrap();
        // crash — no checkpoint, relies on WAL redo of the whole multi-page txn
    }

    {
        let mut ex = create_executor("txn_commit_multi_page_restart");
        assert_eq!(row_count(select_all(&mut ex, "logs")), 100);
    }

    cleanup("txn_commit_multi_page_restart");
}

#[test]
#[ignore = "known gap: rolling back CREATE TABLE doesn't revert the in-memory TableCatalog/indexes"]
fn test_rollback_create_table_removes_table() {
    cleanup("txn_rollback_create_table");

    let mut ex = create_executor("txn_rollback_create_table");

    ex.execute(Statement::Begin, &mut None).unwrap();
    ex.execute(
        Statement::CreateTable {
            name: "users".to_string(),
            columns: vec![Column::new("id", DataType::Integer, true)],
        },
        &mut None,
    )
    .unwrap();
    ex.execute(Statement::RollBack, &mut None).unwrap();

    let result = ex.execute(
        Statement::Select {
            table_name: "users".to_string(),
            columns: SelectColumns::All,
            where_clause: None,
        },
        &mut None,
    );
    assert!(
        result.is_err(),
        "CREATE TABLE inside a rolled-back transaction should not leave the table visible"
    );

    cleanup("txn_rollback_create_table");
}

#[test]
#[ignore = "known gap: TableCatalog.last_page isn't reverted by rollback, corrupting the next insert after a multi-page rollback"]
fn test_insert_after_multi_page_rollback_does_not_corrupt_table() {
    cleanup("txn_rollback_multi_page_reuse");

    let mut ex = create_executor("txn_rollback_multi_page_reuse");
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

    ex.execute(Statement::Begin, &mut None).unwrap();
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
    ex.execute(Statement::RollBack, &mut None).unwrap();

    ex.execute(
        Statement::Insert {
            table_name: "logs".to_string(),
            values: vec![Value::Integer(1), Value::Text("still works".to_string())],
        },
        &mut None,
    )
    .unwrap();
    assert_eq!(row_count(select_all(&mut ex, "logs")), 1);

    cleanup("txn_rollback_multi_page_reuse");
}
