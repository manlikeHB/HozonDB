use hozondb_server::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();

    let db_name = args
        .get(1)
        .ok_or("Usage: hozondb-server <database name> [--addr <address>]")?;

    let addr = args
        .windows(2)
        .find(|w| w[0] == "--addr")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or_else(|| "[::]:50051".parse().unwrap());

    start_server(addr, &db_name).await
}
