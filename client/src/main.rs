use comfy_table::Table;
use hozondb_core::proto::{
    ExecuteRequest, execute_response, hozon_db_service_client::HozonDbServiceClient, value,
};
use rustyline::error::ReadlineError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = HozonDbServiceClient::connect("http://[::1]:50051").await?;

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
                    break;
                }

                let request = tonic::Request::new(ExecuteRequest {
                    sql: sql.to_string(),
                });

                match client.execute(request).await {
                    Ok(response) => {
                        let res = response.into_inner();

                        if let Some(kind) = res.kind {
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
