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

    pub fn setup(&mut self) -> io::Result<()> {
        let columns = vec![
            Column::new("id", DataType::Integer, true),
            Column::new("name", DataType::Text, false),
            Column::new("age", DataType::Integer, false),
        ];
        self.executor.execute(
            Statement::CreateTable {
                name: self.temp_table_name.clone(),
                columns,
            },
            &mut None,
        )?;

        for i in 1..=self.num_rows {
            let values = vec![
                Value::Integer(i as i32),
                Value::Text(format!("Name{}", i)),
                Value::Integer((20 + (i % 30)) as i32),
            ];
            self.executor.execute(
                Statement::Insert {
                    table_name: self.temp_table_name.clone(),
                    values,
                },
                &mut None,
            )?;

            if i % 10000 == 0 {
                println!("Inserted {} rows...", i);
            }
        }

        Ok(())
    }

    pub fn cleanup(&mut self) -> io::Result<()> {
        self.executor.execute(
            Statement::DropTable {
                name: self.temp_table_name.clone(),
            },
            &mut None,
        )?;
        Ok(())
    }

    pub fn run_all_benchmarks(&mut self) -> io::Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        // READ benchmarks — no WHERE clause
        results.push(self.benchmark_select_full_scan()?);

        // READ benchmarks — indexed (WHERE on primary key)
        results.push(self.benchmark_select_by_id("SELECT idx seek (first)", 1)?);
        results.push(self.benchmark_select_by_id("SELECT idx seek (middle)", self.num_rows / 2)?);
        results.push(self.benchmark_select_by_id("SELECT idx seek (last)", self.num_rows)?);
        results.push(self.benchmark_select_by_id("SELECT idx seek (missing)", self.num_rows + 1)?);

        // READ benchmarks — non-indexed (WHERE on age, full scan)
        results.push(self.benchmark_select_by_age("SELECT scan by age=25", 25)?);
        results.push(self.benchmark_select_by_age("SELECT scan by age=49", 49)?);

        // WRITE benchmarks
        results.push(self.benchmark_insert_single()?);
        results.push(self.benchmark_update_inplace()?);
        results.push(self.benchmark_update_new_slot()?);
        results.push(self.benchmark_update_bulk()?);
        // bulk delete before single to avoid id conflicts
        results.push(self.benchmark_delete_bulk()?);
        results.push(self.benchmark_delete_single()?);

        // WRITE benchmarks — explicit transaction vs no transaction
        // (isolates the group-commit win: N fsyncs vs 1)
        let txn_batch_size = self.num_rows / 10;
        results.push(self.benchmark_insert_n_no_txn(txn_batch_size)?);
        results.push(self.benchmark_insert_n_explicit_txn(txn_batch_size)?);

        Ok(results)
    }

    // SELECT WHERE id = value (uses index seek)
    fn benchmark_select_by_id(
        &mut self,
        label: &str,
        id_value: usize,
    ) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let stm = Statement::Select {
            table_name: self.temp_table_name.clone(),
            columns: SelectColumns::All,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(id_value as i32))),
            }),
        };

        let result = self.executor.execute(stm, &mut metrics)?;
        let rows_affected = match result {
            ExecutionResult::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        Ok(BenchmarkResult {
            operation: label.to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    // SELECT WHERE age = value (non-indexed, forces full scan)
    fn benchmark_select_by_age(
        &mut self,
        label: &str,
        age_value: i32,
    ) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let stm = Statement::Select {
            table_name: self.temp_table_name.clone(),
            columns: SelectColumns::All,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(age_value))),
            }),
        };

        let result = self.executor.execute(stm, &mut metrics)?;
        let rows_affected = match result {
            ExecutionResult::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        Ok(BenchmarkResult {
            operation: label.to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_select_full_scan(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let stm = Statement::Select {
            table_name: self.temp_table_name.clone(),
            columns: SelectColumns::All,
            where_clause: None,
        };

        let result = self.executor.execute(stm, &mut metrics)?;
        let rows_affected = match result {
            ExecutionResult::Rows { rows, .. } => rows.len(),
            _ => 0,
        };

        Ok(BenchmarkResult {
            operation: "SELECT full scan".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    // UPDATE in-place — new value same size as old (age column, integer)
    fn benchmark_update_inplace(&mut self) -> io::Result<BenchmarkResult> {
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
            operation: "UPDATE (fits slot)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    // UPDATE new slot — new value larger than old (name column, short → long text)
    fn benchmark_update_new_slot(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        // "Name2" is ~5 chars, replacing with 200-char string forces new slot
        let long_name = "X".repeat(200);
        let stm = Statement::Update {
            table_name: self.temp_table_name.clone(),
            assignments: vec![("name".to_string(), Value::Text(long_name))],
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Literal(Value::Integer(2))),
            }),
        };

        let _ = self.executor.execute(stm, &mut metrics)?;
        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "UPDATE (exceeds slot)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_update_bulk(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());
        let bulk_val = self.num_rows / 10;

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
            operation: "UPDATE bulk in-place 10%".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_delete_single(&mut self) -> io::Result<BenchmarkResult> {
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
        let bulk_val = self.num_rows / 10;

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
            operation: "DELETE bulk 10%".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    fn benchmark_insert_single(&mut self) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());

        let stmt = Statement::Insert {
            table_name: self.temp_table_name.clone(),
            values: vec![
                Value::Integer(self.num_rows as i32 + 1),
                Value::Text("NewUser".to_string()),
                Value::Integer(25),
            ],
        };

        let _ = self.executor.execute(stmt, &mut metrics)?;
        let rows_affected = metrics.as_ref().unwrap().rows_modified;

        Ok(BenchmarkResult {
            operation: "INSERT (single row)".to_string(),
            metrics: metrics.unwrap(),
            rows_affected,
        })
    }

    // N separate INSERT statements, each auto-committed on its own —
    // today's baseline: one fsync per statement.
    fn benchmark_insert_n_no_txn(&mut self, n: usize) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());
        // offset well clear of every other benchmark's id range
        let start_id = self.num_rows + 1_000;

        let start = std::time::Instant::now();
        for i in 0..n {
            let stmt = Statement::Insert {
                table_name: self.temp_table_name.clone(),
                values: vec![
                    Value::Integer((start_id + i) as i32),
                    Value::Text("NoTxnUser".to_string()),
                    Value::Integer(25),
                ],
            };
            let _ = self.executor.execute(stmt, &mut metrics)?;
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        let mut metrics = metrics.unwrap();
        metrics.duration_ms = elapsed_ms;
        let rows_affected = metrics.rows_modified;

        Ok(BenchmarkResult {
            operation: format!("INSERT x{} (no txn)", n),
            metrics,
            rows_affected,
        })
    }

    // Same N inserts, wrapped in one explicit BEGIN/COMMIT — a single
    // fsync for the whole batch instead of N.
    fn benchmark_insert_n_explicit_txn(&mut self, n: usize) -> io::Result<BenchmarkResult> {
        let mut metrics = Some(QueryMetrics::new());
        // offset well clear of the no-txn batch above and every other benchmark
        let start_id = self.num_rows + 2_000 + n;

        let start = std::time::Instant::now();
        self.executor.execute(Statement::Begin, &mut None)?;
        for i in 0..n {
            let stmt = Statement::Insert {
                table_name: self.temp_table_name.clone(),
                values: vec![
                    Value::Integer((start_id + i) as i32),
                    Value::Text("TxnUser".to_string()),
                    Value::Integer(25),
                ],
            };
            let _ = self.executor.execute(stmt, &mut metrics)?;
        }
        self.executor.execute(Statement::Commit, &mut None)?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        let mut metrics = metrics.unwrap();
        metrics.duration_ms = elapsed_ms;
        let rows_affected = metrics.rows_modified;

        Ok(BenchmarkResult {
            operation: format!("INSERT x{} (explicit txn)", n),
            metrics,
            rows_affected,
        })
    }
}
