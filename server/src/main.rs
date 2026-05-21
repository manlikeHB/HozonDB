use hozondb_server::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Usage: hozondb-server -- <database name>")?;

    let addr = "[::1]:50051".parse()?;
    start_server(addr, &path).await
}
