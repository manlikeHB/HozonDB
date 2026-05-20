use hozondb_core::proto::{ExecuteRequest, hozon_db_service_client::HozonDbServiceClient};

// use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = HozonDbServiceClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(ExecuteRequest {
        sql: "SELECT * FROM users".to_string(),
    });

    let response = client.execute(request).await?;

    println!("Reponse: {:?}", response);

    Ok(())
}
