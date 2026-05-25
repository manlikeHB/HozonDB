use hozondb_core::proto::query_response::Payload;
use std::io;
use std::pin::Pin;
use std::{net::SocketAddr, sync::Arc};
use tokio_stream::Stream;

use hozondb_core::proto::{Headers, QueryRequest, QueryResponse};
use hozondb_core::{
    proto::{
        ExecuteRequest, ExecuteResponse, execute_response,
        hozon_db_service_server::{HozonDbService, HozonDbServiceServer},
    },
    sql::{
        database::Database,
        executor::{ExecutionResult, Executor},
        parser,
        tokenizer::{self},
    },
};

use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Server};

/// gRPC service implementation for HozonDB.
///
/// Wraps the query executor behind a mutex for safe concurrent access.
/// All queries are serialized — one executes at a time.
pub struct HozonDbServer {
    executor: Arc<Mutex<Executor>>,
}

impl HozonDbServer {
    pub fn new(executor: Arc<Mutex<Executor>>) -> Self {
        HozonDbServer { executor }
    }

    async fn execute_sql(&self, sql: &str) -> Result<ExecutionResult, io::Error> {
        let tokens = tokenizer::tokenize(&sql)?;
        let statement = parser::Parser::new(tokens).parse()?;

        self.executor.lock().await.execute(statement, &mut None)
    }
}

#[tonic::async_trait]
impl HozonDbService for HozonDbServer {
    type QueryStream = Pin<Box<dyn Stream<Item = Result<QueryResponse, Status>> + Send>>;

    #[tracing::instrument(skip(self, request), fields(sql = %request.get_ref().sql))]
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let start = std::time::Instant::now();
        let sql = request.into_inner().sql;
        tracing::info!("Executing statement");

        let res = match self.execute_sql(&sql).await {
            Ok(ExecutionResult::Success { message }) => ExecuteResponse {
                kind: Some(execute_response::Kind::Message(message)),
            },
            Err(e) => {
                tracing::error!(error = %e, "Statement execution failed");
                return Err(Status::from(e));
            }
            _ => {
                tracing::warn!("Query command sent to execute RPC");
                return Err(Status::invalid_argument(
                    "Query commands should be handled with `query`",
                ));
            }
        };

        tracing::info!(duration_ms = %start.elapsed().as_millis(), "Statement completed");
        Ok(Response::new(res))
    }

    #[tracing::instrument(skip(self, request), fields(sql = %request.get_ref().sql))]
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let start = std::time::Instant::now();
        let sql = request.into_inner().sql;
        tracing::info!("Executing query");

        let (columns, rows) = match self.execute_sql(&sql).await {
            Ok(ExecutionResult::Rows { columns, rows }) => (columns, rows),
            Err(e) => {
                tracing::error!(error = %e, "Query execution failed");
                return Err(Status::from(e));
            }
            _ => {
                tracing::warn!("Modification command sent to query RPC");
                return Err(Status::invalid_argument(
                    "Modification commands should be handled with `execute`",
                ));
            }
        };

        // TODO: This buffers all rows into memory before streaming.
        // True streaming requires the executor to yield rows incrementally (page by page).
        // See the TODO in execute_select for the required executor changes.
        let mut query_response = Vec::new();

        let headers = QueryResponse {
            payload: Some(Payload::Headers(Headers { columns })),
        };

        query_response.push(Ok(headers));
        let rows_len = rows.len();

        for row in rows {
            let row_res = QueryResponse {
                payload: Some(Payload::Row(row.into())),
            };
            query_response.push(Ok(row_res));
        }

        tracing::info!(duration_ms = %start.elapsed().as_millis(), rows = rows_len, "Query completed");
        Ok(Response::new(Box::pin(tokio_stream::iter(query_response))))
    }
}

/// Starts the HozonDB gRPC server on the given address.
///
/// Opens the database file at `{db_name}.hdb`, initializes the executor,
/// and listens for incoming connections. Shuts down gracefully on Ctrl+C,
/// ensuring the database lock file is released.
pub async fn start_server(
    addr: SocketAddr,
    db_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new(db_name)?;
    let executor = Executor::new(db);
    let service = HozonDbServer::new(Arc::new(Mutex::new(executor)));

    tracing::info!("HozonDB server listening on {}", addr);

    tokio::select! {
        result = Server::builder()
            .add_service(HozonDbServiceServer::new(service))
            .serve(addr) => {
                result?;
            }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down server...");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hozondb_core::{proto::execute_response, sql::database::Database};
    use std::fs;
    use tokio_stream::StreamExt;

    fn cleanup(name: &str) {
        let _ = fs::remove_file(format!("{}.hdb", name));
        let _ = fs::remove_file(format!("{}.hdb.lock", name));
    }

    fn create_test_service(name: &str) -> HozonDbServer {
        let db = Database::new(name).unwrap();
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

        let mut stream = service
            .query(Request::new(QueryRequest {
                sql: "SELECT * FROM users;".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        // first message should be headers
        let first = stream.next().await.unwrap().unwrap();
        match first.payload {
            Some(Payload::Headers(h)) => {
                assert_eq!(h.columns, vec!["id", "name"]);
            }
            _ => panic!("Expected Headers"),
        }

        // second message should be the row
        let second = stream.next().await.unwrap().unwrap();
        match second.payload {
            Some(Payload::Row(r)) => {
                assert_eq!(r.values.len(), 2);
            }
            _ => panic!("Expected Row"),
        }

        cleanup("test_grpc_insert_select");
    }

    #[tokio::test]
    async fn test_execute_invalid_sql_returns_error() {
        cleanup("test_grpc_invalid");
        let service = create_test_service("test_grpc_invalid");

        let request = Request::new(QueryRequest {
            sql: "SELECT FROM;".to_string(),
        });

        let result = service.query(request).await;
        assert!(result.is_err());

        cleanup("test_grpc_invalid");
    }
}
