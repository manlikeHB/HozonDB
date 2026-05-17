use crate::{
    sql::{database::Database, executor::Executor},
    storage::page::PageManager,
};

use std::fs;

pub fn cleanup(basename: &str) {
    let _ = fs::remove_file(format!("{}.hdb", basename));
    let _ = fs::remove_file(format!("{}.hdb.lock", basename));
}

pub fn create_test_executor(db_name: &str) -> Executor {
    let pm = PageManager::new(&format!("{}.hdb", db_name)).unwrap();
    let db = Database::new(pm).unwrap();
    Executor::new(db)
}
