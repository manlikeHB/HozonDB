use hozondb_core::proto::{
    ExecuteRequest, ExecuteResponse, QueryRequest, QueryResponse,
    hozon_db_service_client::HozonDbServiceClient,
};
use tonic::{
    Status, Streaming,
    transport::{self, Channel},
};

pub struct HozonDBClient {
    client: HozonDbServiceClient<Channel>,
}

impl HozonDBClient {
    pub async fn connect(addr: &str) -> Result<Self, transport::Error> {
        let client = HozonDbServiceClient::connect(addr.to_string()).await?;

        Ok(HozonDBClient { client })
    }

    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResponse, Status> {
        let request = tonic::Request::new(ExecuteRequest {
            sql: sql.to_string(),
        });

        Ok(self.client.execute(request).await?.into_inner())
    }

    pub async fn query(&mut self, sql: &str) -> Result<Streaming<QueryResponse>, Status> {
        let request = tonic::Request::new(QueryRequest {
            sql: sql.to_string(),
        });

        Ok(self.client.query(request).await?.into_inner())
    }
}
