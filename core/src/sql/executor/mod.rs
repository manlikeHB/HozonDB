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
use std::io;

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
