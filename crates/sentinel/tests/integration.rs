//! Integration tests for the Sentinel server.
//!
//! These tests start a Sentinel instance in-process and verify
//! command dispatch over TCP.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

use valkey_sentinel::config::SentinelConfig;
use valkey_sentinel::server::SentinelServer;
use valkey_sentinel::state::{MasterInfo, SentinelState};

/// Helper: send a RESP command and read the reply.
async fn send_cmd(addr: &str, cmd: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(cmd.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

/// Helper: send a RESP array command.
async fn send_resp_cmd(addr: &str, args: &[&str]) -> String {
    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    send_cmd(addr, &cmd).await
}

#[tokio::test]
async fn test_sentinel_ping() {
    let port = 26390;
    let addr = format!("127.0.0.1:{}", port);

    let state = Arc::new(SentinelState::new(port));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    // Run server in background.
    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    // Wait for server to start.
    sleep(Duration::from_millis(200)).await;

    let reply = send_cmd(&addr, "PING\r\n").await;
    assert!(reply.contains("PONG"), "expected PONG, got: {:?}", reply);
}

#[tokio::test]
async fn test_sentinel_masters_empty() {
    let port = 26391;
    let addr = format!("127.0.0.1:{}", port);

    let state = Arc::new(SentinelState::new(port));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "MASTERS"]).await;
    // Should return an empty array.
    assert!(
        reply.starts_with("*0") || reply.starts_with("*-\r\n"),
        "expected empty array, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_master_not_found() {
    let port = 26392;
    let addr = format!("127.0.0.1:{}", port);

    let state = Arc::new(SentinelState::new(port));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "MASTER", "nonexistent"]).await;
    assert!(
        reply.starts_with('-'),
        "expected error, got: {:?}",
        reply
    );
    assert!(reply.contains("not found"), "got: {:?}", reply);
}

#[tokio::test]
async fn test_sentinel_myid() {
    let port = 26393;
    let addr = format!("127.0.0.1:{}", port);

    let state = Arc::new(SentinelState::new(port));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "MYID"]).await;
    // Should return a bulk string with a 40-char hex ID.
    assert!(
        reply.starts_with('$'),
        "expected bulk string, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_masters_with_monitor() {
    let port = 26394;
    let addr = format!("127.0.0.1:{}", port);

    let mut masters = HashMap::new();
    let master_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    masters.insert(
        "mymaster".to_string(),
        MasterInfo::new("mymaster".to_string(), master_addr, 2, 5000),
    );

    let state = Arc::new(SentinelState::with_masters(port, masters));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "MASTERS"]).await;
    // Should return an array with one element.
    assert!(
        reply.starts_with("*1"),
        "expected array of 1, got: {:?}",
        reply
    );
    assert!(
        reply.contains("mymaster"),
        "expected 'mymaster' in reply, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_get_master_addr() {
    let port = 26395;
    let addr = format!("127.0.0.1:{}", port);

    let mut masters = HashMap::new();
    let master_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    masters.insert(
        "mymaster".to_string(),
        MasterInfo::new("mymaster".to_string(), master_addr, 2, 5000),
    );

    let state = Arc::new(SentinelState::with_masters(port, masters));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"]).await;
    // Should return [127.0.0.1, 6379]
    assert!(
        reply.starts_with("*2"),
        "expected array of 2, got: {:?}",
        reply
    );
    assert!(
        reply.contains("127.0.0.1"),
        "expected IP in reply, got: {:?}",
        reply
    );
    assert!(
        reply.contains("6379"),
        "expected port in reply, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_info() {
    let port = 26396;
    let addr = format!("127.0.0.1:{}", port);

    let state = Arc::new(SentinelState::new(port));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["INFO"]).await;
    assert!(
        reply.contains("# Sentinel"),
        "expected # Sentinel section, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_ckquorum() {
    let port = 26397;
    let addr = format!("127.0.0.1:{}", port);

    let mut masters = HashMap::new();
    let master_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    masters.insert(
        "mymaster".to_string(),
        MasterInfo::new("mymaster".to_string(), master_addr, 1, 5000),
    );

    let state = Arc::new(SentinelState::with_masters(port, masters));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "CKQUORUM", "mymaster"]).await;
    // With quorum=1 and only us, should be OK.
    assert!(
        reply.starts_with('+') || reply.starts_with('$'),
        "expected simple/bulk string, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_is_master_down() {
    let port = 26398;
    let addr = format!("127.0.0.1:{}", port);

    let mut masters = HashMap::new();
    let master_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    masters.insert(
        "mymaster".to_string(),
        MasterInfo::new("mymaster".to_string(), master_addr, 2, 5000),
    );

    let state = Arc::new(SentinelState::with_masters(port, masters));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(
        &addr,
        &["SENTINEL", "IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*"],
    )
    .await;
    // Should return [0, *, 0] — not down.
    assert!(
        reply.starts_with("*3"),
        "expected array of 3, got: {:?}",
        reply
    );
}

#[tokio::test]
async fn test_sentinel_replicas_empty() {
    let port = 26399;
    let addr = format!("127.0.0.1:{}", port);

    let mut masters = HashMap::new();
    let master_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    masters.insert(
        "mymaster".to_string(),
        MasterInfo::new("mymaster".to_string(), master_addr, 2, 5000),
    );

    let state = Arc::new(SentinelState::with_masters(port, masters));
    let server = SentinelServer::new(state);

    let config = SentinelConfig {
        port,
        bind_addr: "127.0.0.1".to_string(),
        monitors: vec![],
    };

    tokio::spawn(async move {
        server.run(config).await.unwrap();
    });

    sleep(Duration::from_millis(200)).await;

    let reply = send_resp_cmd(&addr, &["SENTINEL", "REPLICAS", "mymaster"]).await;
    // Should return an empty array.
    assert!(
        reply.starts_with("*0"),
        "expected empty array, got: {:?}",
        reply
    );
}
