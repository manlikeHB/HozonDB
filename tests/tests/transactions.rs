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

#[test]
fn test_failed_create_table_does_not_persist_across_restart() {
    cleanup("txn_failed_create_atomicity");

    {
        let mut ex = create_executor("txn_failed_create_atomicity");

        // Boolean PK fails at the index-creation step, which runs *after*
        // the table catalog entry has already been written and WAL-logged.
        let result = ex.execute(
            Statement::CreateTable {
                name: "bad_table".to_string(),
                columns: vec![Column::new("flag", DataType::Boolean, true)],
            },
            &mut None,
        );

        assert!(
            result.is_err(),
            "expected CREATE TABLE to fail for a Boolean PK"
        );
        // crash — no explicit ROLLBACK was ever called, since the executor
        // doesn't roll back on error; ~test currently fails here already~
    }

    {
        let mut ex = create_executor("txn_failed_create_atomicity");

        // If the implicit txn was correctly rolled back, "bad_table" should
        // not exist. Currently it does, because the executor commits the
        // implicit transaction unconditionally, even on error.
        let result = ex.execute(
            Statement::Select {
                table_name: "bad_table".to_string(),
                columns: SelectColumns::All,
                where_clause: None,
            },
            &mut None,
        );

        assert!(
            result.is_err(),
            "bad_table should not exist — the failed CREATE TABLE should have rolled back, not committed"
        );
    }

    cleanup("txn_failed_create_atomicity");
}

#[test]
fn test_failed_create_table_not_visible_in_same_session() {
    cleanup("txn_failed_create_in_memory");

    let mut ex = create_executor("txn_failed_create_in_memory");

    let result = ex.execute(
        Statement::CreateTable {
            name: "bad_table".to_string(),
            columns: vec![Column::new("flag", DataType::Boolean, true)],
        },
        &mut None,
    );
    assert!(
        result.is_err(),
        "expected CREATE TABLE to fail for a Boolean PK"
    );

    // No restart — this checks the in-memory table_catalog directly.
    // A genuinely nonexistent table produces a clean NotFound with
    // "does not exist" (see helpers::get_table_first_page_and_cols).
    // If rollback only undid the WAL/page and not the in-memory catalog,
    // this will instead fail with some other error (or not fail at all)
    // because the catalog still thinks bad_table exists.
    let select_result = ex.execute(
        Statement::Select {
            table_name: "bad_table".to_string(),
            columns: SelectColumns::All,
            where_clause: None,
        },
        &mut None,
    );

    match select_result {
        Err(e) => assert_eq!(
            e.kind(),
            std::io::ErrorKind::NotFound,
            "bad_table should be reported as not existing (in-memory catalog should \
             have been rolled back too), got a different error instead: {e}"
        ),
        Ok(_) => {
            panic!("bad_table should not be queryable — in-memory catalog was not rolled back")
        }
    }

    cleanup("txn_failed_create_in_memory");
}

#[test]
fn test_rollback_leaf_insert_without_root_change_invalidates_cache() {
    cleanup("txn_rollback_leaf_cache");

    let mut ex = create_executor("txn_rollback_leaf_cache");
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

    // committed rows — order for an Integer PK is 371, so this single
    // leaf never splits and the root page never changes
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

    ex.execute(Statement::Begin, &mut None).unwrap();
    ex.execute(
        Statement::Insert {
            table_name: "users".to_string(),
            values: vec![Value::Integer(100), Value::Text("rolled back".to_string())],
        },
        &mut None,
    )
    .unwrap();
    ex.execute(Statement::RollBack, &mut None).unwrap();

    // this insert only touched the existing leaf page — no root change,
    // no index catalog page touched. If the tree's node cache wasn't
    // invalidated on rollback, this lookup would still find the row
    // straight out of the stale in-memory node, even though the on-disk
    // page was correctly reverted.
    let result = ex
        .execute(
            Statement::Select {
                table_name: "users".to_string(),
                columns: SelectColumns::All,
                where_clause: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Column("id".to_string())),
                    op: BinaryOperator::Equals,
                    right: Box::new(Expr::Literal(Value::Integer(100))),
                }),
            },
            &mut None,
        )
        .unwrap();
    assert_eq!(
        row_count(result),
        0,
        "rolled-back key still visible via index — stale cache?"
    );

    // sanity check: the earlier committed rows are untouched
    assert_eq!(row_count(select_all(&mut ex, "users")), 5);

    // re-inserting the rolled-back key should succeed cleanly
    ex.execute(
        Statement::Insert {
            table_name: "users".to_string(),
            values: vec![Value::Integer(100), Value::Text("fresh insert".to_string())],
        },
        &mut None,
    )
    .unwrap();
    assert_eq!(row_count(select_all(&mut ex, "users")), 6);

    cleanup("txn_rollback_leaf_cache");
}
