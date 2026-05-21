use std::sync::Arc;

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

struct HozonDbServer {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Usage: hozondb-server <database name>")?;
    let addr = "[::1]:50051".parse()?;
    let page_manager = PageManager::new(&format!("{path}.hdb"))?;
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
