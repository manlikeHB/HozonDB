use std::{net::SocketAddr, sync::Arc};

use hozondb_core::{
    proto::{
        ExecuteRequest, ExecuteResponse, ResultSet, execute_response,
        hozon_db_service_server::{HozonDbService, HozonDbServiceServer},
    },
    sql::{
        database::Database,
        executor::{ExecutionResult, Executor},
        parser,
        tokenizer::{self},
    },
    storage::page::PageManager,
};

use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Server};

pub struct HozonDbServer {
    executor: Arc<Mutex<Executor>>,
}

impl HozonDbServer {
    pub fn new(executor: Arc<Mutex<Executor>>) -> Self {
        HozonDbServer { executor }
    }
}

#[tonic::async_trait]
impl HozonDbService for HozonDbServer {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let sql = request.into_inner().sql;

        let tokens = tokenizer::tokenize(&sql)?;
        let statement = parser::Parser::new(tokens).parse()?;

        let res = match self.executor.lock().await.execute(statement, &mut None)? {
            ExecutionResult::Success { message } => ExecuteResponse {
                kind: Some(execute_response::Kind::Message(message)),
            },
            ExecutionResult::Rows { columns, rows } => ExecuteResponse {
                kind: Some(execute_response::Kind::Rows(ResultSet {
                    rows: rows.into_iter().map(|r| From::from(r)).collect(),
                    columns,
                })),
            },
        };

        Ok(Response::new(res))
    }
}

pub async fn start_server(
    addr: SocketAddr,
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let page_manager = PageManager::new(&format!("{db_name}.hdb"))?;
    let db = Database::new(page_manager)?;
    let executor = Executor::new(db);
    let service = HozonDbServer::new(Arc::new(Mutex::new(executor)));

    println!("HozonDB server listening on {}", addr);

    tokio::select! {
        result = Server::builder()
            .add_service(HozonDbServiceServer::new(service))
            .serve(addr) => {
                result?;
            }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down server...");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hozondb_core::{
        proto::execute_response, sql::database::Database, storage::page::PageManager,
    };
    use std::fs;

    fn cleanup(name: &str) {
        let _ = fs::remove_file(format!("{}.hdb", name));
        let _ = fs::remove_file(format!("{}.hdb.lock", name));
    }

    fn create_test_service(name: &str) -> HozonDbServer {
        let pm = PageManager::new(&format!("{}.hdb", name)).unwrap();
        let db = Database::new(pm).unwrap();
        let executor = Executor::new(db);
        HozonDbServer::new(Arc::new(Mutex::new(executor)))
    }

    #[tokio::test]
    async fn test_execute_create_table() {
        cleanup("test_grpc_create");
        let service = create_test_service("test_grpc_create");

        let request = Request::new(ExecuteRequest {
            sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);".to_string(),
        });

        let response = service.execute(request).await.unwrap().into_inner();

        assert!(matches!(
            response.kind,
            Some(execute_response::Kind::Message(_))
        ));

        cleanup("test_grpc_create");
    }

    #[tokio::test]
    async fn test_execute_insert_and_select() {
        cleanup("test_grpc_insert_select");
        let service = create_test_service("test_grpc_insert_select");

        service
            .execute(Request::new(ExecuteRequest {
                sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);".to_string(),
            }))
            .await
            .unwrap();

        service
            .execute(Request::new(ExecuteRequest {
                sql: "INSERT INTO users VALUES (1, 'Alice');".to_string(),
            }))
            .await
            .unwrap();

        let response = service
            .execute(Request::new(ExecuteRequest {
                sql: "SELECT * FROM users;".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        match response.kind {
            Some(execute_response::Kind::Rows(result_set)) => {
                assert_eq!(result_set.rows.len(), 1);
                assert_eq!(result_set.columns, vec!["id", "name"]);
            }
            _ => panic!("Expected Rows"),
        }

        cleanup("test_grpc_insert_select");
    }

    #[tokio::test]
    async fn test_execute_invalid_sql_returns_error() {
        cleanup("test_grpc_invalid");
        let service = create_test_service("test_grpc_invalid");

        let request = Request::new(ExecuteRequest {
            sql: "SELECT FROM;".to_string(),
        });

        let result = service.execute(request).await;
        assert!(result.is_err());

        cleanup("test_grpc_invalid");
    }
}
