use crate::sql::{database::Database, executor::Executor};

use std::fs;

pub fn cleanup(basename: &str) {
    let _ = fs::remove_file(format!("{}.hdb", basename));
    let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    let _ = fs::remove_file(format!("{}.wal", basename));
}

pub fn create_test_executor(db_name: &str) -> Executor {
    let db = Database::new(db_name).unwrap();
    Executor::new(db)
}
