pub mod ddl; // Data Definition
pub mod dml; // Data Modification
pub mod dql; // Data Query
pub mod evaluator;
pub mod helpers;

pub use ddl::{create_table::execute_create, drop_table::execute_drop_table};
pub use dml::{delete::execute_delete, insert::execute_insert, update::execute_update};
pub use dql::select::execute_select;

use crate::{
    benchmark::metrics::QueryMetrics,
    catalog::row::Row,
    sql::{database::Database, parser::Statement},
};
use std::io::{self, ErrorKind};

pub struct Executor {
    database: Database,
}

#[derive(Debug)]
pub enum ExecutionResult {
    Success {
        message: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
}

impl Executor {
    pub fn new(database: Database) -> Self {
        Executor { database }
    }

    pub fn execute(
        &mut self,
        statement: Statement,
        metrics: &mut Option<QueryMetrics>,
    ) -> io::Result<ExecutionResult> {
        let start = std::time::Instant::now();

        let result = match statement {
            Statement::CreateTable { name, columns } => {
                // begin an implicit transaction if no active transaction
                self.database.begin_implicit_txn()?;
                let res = execute_create(self, name, columns);
                // Commit an implicit transaction
                if self.database.is_txn_implicit()? {
                    self.database.commit_txn()?;
                }
                res
            }
            Statement::Insert { table_name, values } => {
                // begin an implicit transaction if no active transaction
                self.database.begin_implicit_txn()?;
                let res = execute_insert(&mut self.database, table_name, values, metrics);
                // Commit an implicit transaction
                if self.database.is_txn_implicit()? {
                    self.database.commit_txn()?;
                }
                res
            }
            Statement::Select {
                table_name,
                columns,
                where_clause,
            } => execute_select(
                &mut self.database,
                table_name,
                columns,
                where_clause,
                metrics,
            ),
            Statement::DropTable { name } => {
                // begin an implicit transaction if no active transaction
                self.database.begin_implicit_txn()?;

                let res = execute_drop_table(&mut self.database, name);
                // Commit an implicit transaction
                if self.database.is_txn_implicit()? {
                    self.database.commit_txn()?;
                }
                res
            }
            Statement::Delete {
                table_name,
                where_clause,
            } => {
                // begin an implicit transaction if no active transaction
                self.database.begin_implicit_txn()?;
                let res = execute_delete(&mut self.database, table_name, where_clause, metrics);
                // Commit an implicit transaction
                if self.database.is_txn_implicit()? {
                    self.database.commit_txn()?;
                }
                res
            }
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => {
                // begin an implicit transaction if no active transaction
                self.database.begin_implicit_txn()?;

                let res = execute_update(
                    &mut self.database,
                    table_name,
                    assignments,
                    where_clause,
                    metrics,
                );
                // Commit an implicit transaction
                if self.database.is_txn_implicit()? {
                    self.database.commit_txn()?;
                }
                res
            }
            Statement::Checkpoint => {
                if self.database.txn_is_active() {
                    return Err(io::Error::new(
                        ErrorKind::Other,
                        "cannot checkpoint during an active transaction",
                    ));
                }
                self.database.checkpoint()?;
                Ok(ExecutionResult::Success {
                    message: "Checkpoint complete.".to_string(),
                })
            }
            Statement::Begin => {
                let txn_id = self.database.begin_explicit_txn()?;
                Ok(ExecutionResult::Success {
                    message: format!("Transaction {} started", txn_id),
                })
            }
            Statement::Commit => {
                self.database.commit_txn()?;
                Ok(ExecutionResult::Success {
                    message: "Transaction committed".to_string(),
                })
            }
            Statement::RollBack => {
                // TODO: roll back txn
                Ok(ExecutionResult::Success {
                    message: "Transaction rolled back".to_string(),
                })
            }
        };

        if let Some(m) = metrics {
            m.duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog::row::Value,
        catalog::schema::{Column, DataType},
        sql::parser::Statement,
        test_helpers::*,
    };

    #[test]
    fn test_explicit_txn_commit_clears_txn() {
        cleanup("test_exec_explicit_commit");
        let mut executor = create_test_executor("test_exec_explicit_commit");

        executor.execute(Statement::Begin, &mut None).unwrap();
        assert!(executor.database.txn_is_active());

        executor.execute(Statement::Commit, &mut None).unwrap();
        assert!(!executor.database.txn_is_active());

        cleanup("test_exec_explicit_commit");
    }

    #[test]
    fn test_implicit_txn_cleared_after_statement() {
        cleanup("test_exec_implicit_cleared");
        let mut executor = create_test_executor("test_exec_implicit_cleared");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        // implicit txn should be committed and cleared
        assert!(!executor.database.txn_is_active());

        cleanup("test_exec_implicit_cleared");
    }

    #[test]
    fn test_double_begin_errors() {
        cleanup("test_exec_double_begin");
        let mut executor = create_test_executor("test_exec_double_begin");

        executor.execute(Statement::Begin, &mut None).unwrap();
        let result = executor.execute(Statement::Begin, &mut None);
        assert!(result.is_err());

        cleanup("test_exec_double_begin");
    }

    #[test]
    fn test_commit_no_txn_errors() {
        cleanup("test_exec_commit_no_txn");
        let mut executor = create_test_executor("test_exec_commit_no_txn");

        let result = executor.execute(Statement::Commit, &mut None);
        assert!(result.is_err());

        cleanup("test_exec_commit_no_txn");
    }

    #[test]
    fn test_lsns_collected_on_insert() {
        cleanup("test_exec_lsns_insert");
        let mut executor = create_test_executor("test_exec_lsns_insert");

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        executor.execute(Statement::Begin, &mut None).unwrap();

        executor
            .execute(
                Statement::Insert {
                    table_name: "users".to_string(),
                    values: vec![Value::Integer(1)],
                },
                &mut None,
            )
            .unwrap();

        // txn should have collected lsns before commit
        let lsn_count = executor.database.txn_lsns().unwrap().len();
        assert!(lsn_count > 0, "expected LSNs to be tracked on active txn");

        executor.execute(Statement::Commit, &mut None).unwrap();

        cleanup("test_exec_lsns_insert");
    }

    #[test]
    fn test_lsns_collected_on_create_table() {
        cleanup("test_exec_lsns_create");
        let mut executor = create_test_executor("test_exec_lsns_create");

        executor.execute(Statement::Begin, &mut None).unwrap();

        executor
            .execute(
                Statement::CreateTable {
                    name: "users".to_string(),
                    columns: vec![Column::new("id", DataType::Integer, true)],
                },
                &mut None,
            )
            .unwrap();

        // create table generates at least 2 lsns: AllocatePage + CreateTable WAL record
        let lsn_count = executor.database.txn_lsns().unwrap().len();
        assert!(
            lsn_count >= 2,
            "expected at least 2 LSNs for CREATE TABLE, got {}",
            lsn_count
        );

        executor.execute(Statement::Commit, &mut None).unwrap();

        cleanup("test_exec_lsns_create");
    }
}
