use crate::benchmark::metrics::QueryMetrics;
use crate::catalog::row::Value;
use crate::catalog::schema::{Column, DataType};
use crate::sql::executor::{ExecutionResult, Executor};
use crate::sql::parser::{BinaryOperator, Expr, SelectColumns, Statement};
use std::io;

pub struct BenchmarkRunner<'a> {
    executor: &'a mut Executor,
    temp_table_name: String,
    num_rows: usize,
}

pub struct BenchmarkResult {
    pub operation: String,
    pub metrics: QueryMetrics,
    pub rows_affected: usize,
}

impl<'a> BenchmarkRunner<'a> {
    // Creates a new benchmark runner with a temporary table
    pub fn new(executor: &'a mut Executor, num_rows: usize) -> Self {
        let temp_table_name = format!(
            "benchmark_temp_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_secs()
        );

        BenchmarkRunner {
            executor,
            temp_table_name,
            num_rows,
        }
    }

    // Sets up the temporary table and inserts test data
    pub fn setup(&mut self) -> io::Result<()> {
        // Create table with schema: (id INTEGER, name TEXT, age INTEGER)
        let columns = vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::Text),
            Column::new("age", DataType::Integer),
        ];
        let create_stm = Statement::CreateTable {
            name: self.temp_table_name.clone(),
            columns,
        };
        self.executor.execute(create_stm, &mut None)?;

        // Insert num_rows rows with test data
        for i in 1..=self.num_rows {
            let values = vec![
                Value::Integer(i as i32),
                Value::Text(format!("Name{}", i)),
                Value::Integer((20 + (i % 30)) as i32),
            ];
            let insert_stm = Statement::Insert {
                table_name: self.temp_table_name.clone(),
                values,
            };
            self.executor.execute(insert_stm, &mut None)?;

            if i % 10000 == 0 {
                println!("Inserted {} rows...", i);
            }
        }

        Ok(())
    }

    // Cleans up by dropping the temporary table
    pub fn cleanup(&mut self) -> io::Result<()> {
        let stm = Statement::DropTable {
            name: self.temp_table_name.clone(),
        };

        self.executor.execute(stm, &mut None)?;
        Ok(())
    }

    // Runs all benchmarks and returns results
    pub fn run_all_benchmarks(&mut self) -> io::Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        // READ benchmarks
        results.push(self.benchmark_select_full_scan()?);
        results.push(self.benchmark_select_first()?);
        results.push(self.benchmark_select_middle()?);
        results.push(self.benchmark_select_last()?);
        results.push(self.benchmark_select_nonexistent()?);

        // WRITE benchmarks
        results.push(self.benchmark_insert_single()?);
        results.push(self.benchmark_update_single()?);
        results.push(self.benchmark_update_bulk()?);
        // Bulk delete benchmark coming before single delete to avoid conflicts,
        // while 10% is deleted from the start, a single row is deleted from the end
        results.push(self.benchmark_delete_bulk()?);
        results.push(self.benchmark_delete_single()?);

        Ok(results)
    }

    fn benchmark_select(
        &mut self,
        metrics: &mut Option<QueryMetrics>,
        id_value: usize,
    ) -> io::Result<usize> {
        // SELECT * FROM temp_table WHERE id = id_value;

        let stm = Statement::Select {
            table_name: self.temp_table_name.clone(),
            columns: SelectColumns::All,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(id_value as i32))),
            }),
        };

        let result = self.executor.execute(stm, metrics)?;

        let rows_affected = match result {
            ExecutionResult::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        Ok(rows_affected)
    }

    fn benchmark_select_first(&mut self) -> io::Result<BenchmarkResult> {
        // SELECT * FROM temp_table WHERE id = 1;

        let mut metrics = Some(QueryMetrics::new());
        let rows_affected = self.benchmark_select(&mut metrics, 1)?;

        Ok(BenchmarkResult {
            operation: "SELECT (first row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_select_middle(&mut self) -> io::Result<BenchmarkResult> {
        // SELECT * FROM temp_table WHERE id = (num_rows / 2);
        let mut metrics = Some(QueryMetrics::new());
        let rows_affected = self.benchmark_select(&mut metrics, self.num_rows / 2)?;

        Ok(BenchmarkResult {
            operation: "SELECT (middle row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_select_last(&mut self) -> io::Result<BenchmarkResult> {
        // SELECT * FROM temp_table WHERE id = num_rows;
        let mut metrics = Some(QueryMetrics::new());
        let rows_affected = self.benchmark_select(&mut metrics, self.num_rows)?;

        Ok(BenchmarkResult {
            operation: "SELECT (last row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_select_nonexistent(&mut self) -> io::Result<BenchmarkResult> {
        // SELECT * FROM temp_table WHERE id = (num_rows + 1);
        let mut metrics = Some(QueryMetrics::new());
        let rows_affected = self.benchmark_select(&mut metrics, self.num_rows + 1)?;

        Ok(BenchmarkResult {
            operation: "SELECT (non-existent row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_select_full_scan(&mut self) -> io::Result<BenchmarkResult> {
        // SELECT * FROM temp_table;

        let stm = Statement::Select {
            table_name: self.temp_table_name.clone(),
            columns: SelectColumns::All,
            where_clause: None,
        };

        let mut metrics = Some(QueryMetrics::new());
        let result = self.executor.execute(stm, &mut metrics)?;

        let rows_affected = match result {
            ExecutionResult::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        Ok(BenchmarkResult {
            operation: "SELECT (full table scan)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_update_single(&mut self) -> io::Result<BenchmarkResult> {
        // UPDATE temp_table SET age = age + 1 WHERE id = 1;
        let mut metrics = Some(QueryMetrics::new());

        let stm = Statement::Update {
            table_name: self.temp_table_name.clone(),
            assignments: vec![("age".to_string(), Value::Integer(99))],
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(1))),
            }),
        };

        let _ = self.executor.execute(stm, &mut metrics)?;

        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "UPDATE (single row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_update_bulk(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let bulk_val = self.num_rows / 10; // Update 10% of rows

        let stm = Statement::Update {
            table_name: self.temp_table_name.clone(),
            assignments: vec![("age".to_string(), Value::Integer(99))],
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::LessOrEqual,
                right: Box::new(Expr::Literal(Value::Integer(bulk_val as i32))),
            }),
        };

        let _ = self.executor.execute(stm, &mut metrics)?;

        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "UPDATE (bulk rows - 10%)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_delete_single(&mut self) -> io::Result<BenchmarkResult> {
        // DELETE FROM temp_table WHERE id = 1;
        let mut metrics = Some(QueryMetrics::new());

        let stm = Statement::Delete {
            table_name: self.temp_table_name.clone(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer((self.num_rows - 1) as i32))),
            }),
        };

        let _ = self.executor.execute(stm, &mut metrics)?;

        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "DELETE (single row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_delete_bulk(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let bulk_val = self.num_rows / 10; // Delete 10% of rows

        let stm = Statement::Delete {
            table_name: self.temp_table_name.clone(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::LessOrEqual,
                right: Box::new(Expr::Literal(Value::Integer(bulk_val as i32))),
            }),
        };

        let _ = self.executor.execute(stm, &mut metrics)?;

        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "DELETE (bulk rows - 10%)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_insert_single(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let stmt = Statement::Insert {
            table_name: self.temp_table_name.clone(),
            values: vec![
                Value::Integer(self.num_rows as i32 + 1), // New ID
                Value::Text("NewUser".to_string()),
                Value::Integer(25),
            ],
        };

        let _ = self.executor.execute(stmt, &mut metrics)?;

        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "INSERT (1 row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }
}
