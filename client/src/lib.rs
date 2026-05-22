use hozondb_core::proto::{
    ExecuteRequest, ExecuteResponse, QueryRequest, QueryResponse,
    hozon_db_service_client::HozonDbServiceClient,
};
use tonic::{
    Status, Streaming,
    transport::{self, Channel},
};

/// gRPC client for HozonDB.
///
/// Use `connect` to establish a connection to a running HozonDB server,
/// then `execute` for DDL/DML statements and `query` for SELECT queries.
pub struct HozonDBClient {
    client: HozonDbServiceClient<Channel>,
}

impl HozonDBClient {
    /// Connects to a HozonDB server at the given address.
    /// Example: `HozonDBClient::connect("http://[::1]:50051").await?`
    pub async fn connect(addr: &str) -> Result<Self, transport::Error> {
        let client = HozonDbServiceClient::connect(addr.to_string()).await?;

        Ok(HozonDBClient { client })
    }

    /// Executes a DDL or DML statement (CREATE, INSERT, UPDATE, DELETE, DROP).
    /// For SELECT queries use `query` instead.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResponse, Status> {
        let request = tonic::Request::new(ExecuteRequest {
            sql: sql.to_string(),
        });

        Ok(self.client.execute(request).await?.into_inner())
    }

    /// Executes a SELECT query and returns a stream of responses.
    /// First message contains column headers, subsequent messages contain rows.
    pub async fn query(&mut self, sql: &str) -> Result<Streaming<QueryResponse>, Status> {
        let request = tonic::Request::new(QueryRequest {
            sql: sql.to_string(),
        });

        Ok(self.client.query(request).await?.into_inner())
    }
}
