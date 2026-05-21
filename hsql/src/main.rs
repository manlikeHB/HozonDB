use comfy_table::Table;
use hozondb_client::HozonDBClient;
use hozondb_core::proto::{execute_response, value};
use rustyline::error::ReadlineError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .ok_or("Usage: hsql -- <server address>")?;
    let mut client = HozonDBClient::connect(&addr).await?;
    let mut rl = rustyline::DefaultEditor::new()?;

    loop {
        match rl.readline("hozondb> ") {
            Ok(line) => {
                rl.add_history_entry(&line)?;

                let sql = line.trim();

                if sql.is_empty() {
                    continue;
                }

                if sql == ".exit" {
                    println!("Exiting HozonDB. Goodbye!");
                    break;
                }

                match client.execute(sql).await {
                    Ok(response) => {
                        if let Some(kind) = response.kind {
                            match kind {
                                execute_response::Kind::Message(m) => {
                                    println!("{m}");
                                }
                                execute_response::Kind::Rows(result_set) => {
                                    let mut table = Table::new();

                                    table.set_header(result_set.columns);

                                    for row in &result_set.rows {
                                        let cells: Vec<String> = row
                                            .values
                                            .iter()
                                            .map(|v| match &v.kind {
                                                Some(value::Kind::Integer(i)) => i.to_string(),
                                                Some(value::Kind::Text(t)) => t.clone(),
                                                Some(value::Kind::Boolean(b)) => b.to_string(),
                                                Some(value::Kind::IsNull(_)) => "NULL".to_string(),
                                                None => "NULL".to_string(),
                                            })
                                            .collect();
                                        table.add_row(cells);
                                    }

                                    println!("{table}");
                                }
                            }
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e.message()),
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
