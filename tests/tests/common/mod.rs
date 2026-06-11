#[allow(dead_code)]
use hozondb_core::catalog::row::Value;
use hozondb_core::sql::parser::{SelectColumns, Statement};
use hozondb_core::sql::{
    database::Database,
    executor::{ExecutionResult, Executor},
};
use std::fs;

pub fn cleanup(name: &str) {
    let _ = fs::remove_file(format!("{}.hdb", name));
    let _ = fs::remove_file(format!("{}.hdb.lock", name));
    let _ = fs::remove_file(format!("{}.wal", name));
}

#[allow(dead_code)]
pub fn create_executor(name: &str) -> Executor {
    Executor::new(Database::new(name).unwrap())
}

#[allow(dead_code)]
pub fn select_all(ex: &mut Executor, table: &str) -> ExecutionResult {
    ex.execute(
        Statement::Select {
            table_name: table.to_string(),
            columns: SelectColumns::All,
            where_clause: None,
        },
        &mut None,
    )
    .unwrap()
}

#[allow(dead_code)]
pub fn row_count(result: ExecutionResult) -> usize {
    match result {
        ExecutionResult::Rows { rows, .. } => rows.len(),
        _ => panic!("expected rows"),
    }
}

#[allow(dead_code)]
pub fn row_values(result: ExecutionResult) -> Vec<Vec<Value>> {
    match result {
        ExecutionResult::Rows { rows, .. } => {
            rows.into_iter().map(|r| r.values().to_vec()).collect()
        }
        _ => panic!("expected rows"),
    }
}
