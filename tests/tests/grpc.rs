mod common;

use hozondb_client::HozonDBClient;
use hozondb_core::proto::{execute_response, query_response};
use hozondb_server::start_server;
use std::net::SocketAddr;
use tokio_stream::StreamExt;

use common::cleanup;

async fn spawn_test_server(db_name: &str) -> SocketAddr {
    let addr: SocketAddr = "[::1]:0".parse().unwrap();

    let server_addr = {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();
        drop(listener);
        bound_addr
    };

    let db = db_name.to_string();
    tokio::spawn(async move {
        start_server(server_addr, &db).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    server_addr
}

#[tokio::test]
async fn test_full_round_trip() {
    cleanup("test_grpc_round_trip");
    let addr = spawn_test_server("test_grpc_round_trip").await;
    let url = format!("http://{}", addr);

    let mut client = HozonDBClient::connect(&url).await.unwrap();

    let res = client
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        .await
        .unwrap();
    assert!(matches!(res.kind, Some(execute_response::Kind::Message(_))));

    client
        .execute("INSERT INTO users VALUES (1, 'Alice');")
        .await
        .unwrap();
    client
        .execute("INSERT INTO users VALUES (2, 'Bob');")
        .await
        .unwrap();

    let mut stream = client.query("SELECT * FROM users;").await.unwrap();

    let first = stream.next().await.unwrap().unwrap();
    match first.payload {
        Some(query_response::Payload::Headers(h)) => {
            assert_eq!(h.columns, vec!["id", "name"]);
        }
        _ => panic!("expected headers"),
    }

    let mut row_count = 0;
    while let Some(Ok(_)) = stream.next().await {
        row_count += 1;
    }
    assert_eq!(row_count, 2);

    cleanup("test_grpc_round_trip");
}

#[tokio::test]
async fn test_invalid_sql_returns_error() {
    cleanup("test_grpc_invalid_sql");
    let addr = spawn_test_server("test_grpc_invalid_sql").await;
    let url = format!("http://{}", addr);

    let mut client = HozonDBClient::connect(&url).await.unwrap();
    let result = client.execute("SELECT FROM;").await;
    assert!(result.is_err());

    cleanup("test_grpc_invalid_sql");
}
