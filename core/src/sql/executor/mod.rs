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
            Statement::CreateTable { name, columns } => execute_create(self, name, columns),
            Statement::Insert { table_name, values } => {
                execute_insert(&mut self.database, table_name, values, metrics)
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
            Statement::DropTable { name } => execute_drop_table(&mut self.database, name),
            Statement::Delete {
                table_name,
                where_clause,
            } => execute_delete(&mut self.database, table_name, where_clause, metrics),
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => execute_update(
                &mut self.database,
                table_name,
                assignments,
                where_clause,
                metrics,
            ),
        };

        if let Some(m) = metrics {
            m.duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        }

        result
    }
}
