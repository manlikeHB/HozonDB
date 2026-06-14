use comfy_table::Table;
use hozondb_core::benchmark::{BenchmarkResult, BenchmarkRunner};
use hozondb_core::sql::{database::Database, executor::Executor};

fn main() -> std::io::Result<()> {
    let num_rows: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    let db_name = format!(
        "benchmark_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    );

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║              HozonDB Benchmark Starting...                   ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
    println!("\nConfiguration:");
    println!("  Rows: {}", num_rows);
    println!("\nSetting up test data...");

    let db = Database::new(&db_name)?;
    let mut executor = Executor::new(db);
    let mut runner = BenchmarkRunner::new(&mut executor, num_rows);

    runner.setup()?;
    println!("Running benchmarks...");
    let results = runner.run_all_benchmarks()?;
    println!("Cleaning up...");
    runner.cleanup()?; // drops the temp table

    // drop executor to release the database lock
    drop(executor);

    // clean up database files
    let _ = std::fs::remove_file(format!("{}.hdb", db_name));
    let _ = std::fs::remove_file(format!("{}.hdb.lock", db_name));
    let _ = std::fs::remove_file(format!("{}.wal", db_name));

    print_results(&results);
    Ok(())
}

fn print_results(results: &[BenchmarkResult]) {
    println!("\nBENCHMARK RESULTS\n");

    // READ operations
    println!("── READ OPERATIONS ──");
    let mut read_table = Table::new();
    read_table.set_header(vec![
        "Query",
        "Duration (ms)",
        "Disk Reads",
        "BP Hits",
        "Rows Scanned",
        "Rows Returned",
    ]);

    let read_ops = [
        "SELECT full scan",
        "SELECT idx seek (first)",
        "SELECT idx seek (middle)",
        "SELECT idx seek (last)",
        "SELECT idx seek (missing)",
        "SELECT scan by age=25",
        "SELECT scan by age=49",
    ];

    for r in results
        .iter()
        .filter(|r| read_ops.contains(&r.operation.as_str()))
    {
        read_table.add_row(vec![
            r.operation.clone(),
            format!("{:.2}", r.metrics.duration_ms),
            r.metrics.disk_reads.to_string(),
            r.metrics.buffer_pool_hits.to_string(),
            r.metrics.rows_scanned.to_string(),
            r.rows_affected.to_string(),
        ]);
    }
    println!("{read_table}\n");

    // WRITE operations
    println!("── WRITE OPERATIONS ──");
    let mut write_table = Table::new();
    write_table.set_header(vec![
        "Operation",
        "Duration (ms)",
        "BP Hits",
        "Pages Dirtied",
        "Rows Modified",
    ]);

    for r in results
        .iter()
        .filter(|r| !read_ops.contains(&r.operation.as_str()))
    {
        write_table.add_row(vec![
            r.operation.clone(),
            format!("{:.2}", r.metrics.duration_ms),
            r.metrics.buffer_pool_hits.to_string(),
            r.metrics.pages_dirtied.len().to_string(),
            r.metrics.rows_modified.to_string(),
        ]);
    }
    println!("{write_table}");
}
