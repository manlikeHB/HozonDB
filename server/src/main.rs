use hozondb_core::proto::{
    ExecuteRequest, ExecuteResponse,
    hozon_db_service_server::{HozonDbService, HozonDbServiceServer},
};

use tonic::{Request, Response, Status, transport::Server};

struct HozonDB;

#[tonic::async_trait]
impl HozonDbService for HozonDB {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = HozonDB;

    println!("HozonDB server listening on {}", addr);

    Server::builder()
        .add_service(HozonDbServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
