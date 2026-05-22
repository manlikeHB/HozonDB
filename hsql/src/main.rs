//! hsql — interactive CLI for HozonDB.
//!
//! Connects to a running HozonDB server over gRPC.
//! Usage: hsql <server_address>
//! Example: hsql http://[::1]:50051

use comfy_table::Table;
use hozondb_client::HozonDBClient;
use hozondb_core::proto::{execute_response, query_response::Payload, value};
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

                let sql_lower = sql.to_lowercase();
                if sql_lower.starts_with("select") {
                    match client.query(sql).await {
                        Ok(mut stream) => {
                            let mut table = Table::new();

                            loop {
                                match stream.message().await? {
                                    Some(res) => match res.payload {
                                        Some(p) => build_table(p, &mut table),
                                        None => continue,
                                    },
                                    None => break,
                                }
                            }

                            println!("{table}");
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e.message())
                        }
                    }
                } else {
                    match client.execute(sql).await {
                        Ok(response) => {
                            if let Some(kind) = response.kind {
                                match kind {
                                    execute_response::Kind::Message(m) => {
                                        println!("{m}");
                                    }
                                    _ => eprintln!("Error: Expected a message response"),
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e.message())
                        }
                    }
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

fn build_table(payload: Payload, table: &mut Table) {
    match payload {
        Payload::Headers(h) => {
            table.set_header(h.columns);
        }
        Payload::Row(r) => {
            let cells: Vec<String> = r
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
    }
}
