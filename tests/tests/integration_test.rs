use hozondb_client::HozonDBClient;
use hozondb_core::proto::{execute_response, query_response};
use hozondb_server::start_server;
use std::{fs, net::SocketAddr};
use tokio_stream::StreamExt;

fn cleanup(name: &str) {
    let _ = fs::remove_file(format!("{}.hdb", name));
    let _ = fs::remove_file(format!("{}.hdb.lock", name));
    let _ = fs::remove_file(format!("{}.wal", name));
}

async fn spawn_test_server(db_name: &str) -> SocketAddr {
    let addr: SocketAddr = "[::1]:0".parse().unwrap(); // port 0 = OS assigns a free port

    let server_addr = {
        // we need the actual bound address, not port 0
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();
        drop(listener);
        bound_addr
    };

    let db = db_name.to_string();
    tokio::spawn(async move {
        start_server(server_addr, &db).await.unwrap();
    });

    // give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    server_addr
}

#[tokio::test]
async fn test_full_round_trip() {
    cleanup("test_integration");
    let addr = spawn_test_server("test_integration").await;
    let url = format!("http://{}", addr);

    let mut client = HozonDBClient::connect(&url).await.unwrap();

    // CREATE
    let res = client
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        .await
        .unwrap();
    assert!(matches!(res.kind, Some(execute_response::Kind::Message(_))));

    // INSERT
    client
        .execute("INSERT INTO users VALUES (1, 'Alice');")
        .await
        .unwrap();
    client
        .execute("INSERT INTO users VALUES (2, 'Bob');")
        .await
        .unwrap();

    // SELECT - first message is headers
    let mut stream = client.query("SELECT * FROM users;").await.unwrap();

    let first = stream.next().await.unwrap().unwrap();
    match first.payload {
        Some(query_response::Payload::Headers(h)) => {
            assert_eq!(h.columns, vec!["id", "name"]);
        }
        _ => panic!("Expected Headers"),
    }

    // collect remaining rows
    let mut row_count = 0;
    while let Some(Ok(_)) = stream.next().await {
        row_count += 1;
    }
    assert_eq!(row_count, 2);

    cleanup("test_integration");
}

#[tokio::test]
async fn test_invalid_sql_returns_error() {
    cleanup("test_integration_invalid");
    let addr = spawn_test_server("test_integration_invalid").await;
    let url = format!("http://{}", addr);

    let mut client = HozonDBClient::connect(&url).await.unwrap();
    let result = client.execute("SELECT FROM;").await;
    assert!(result.is_err());

    cleanup("test_integration_invalid");
}
