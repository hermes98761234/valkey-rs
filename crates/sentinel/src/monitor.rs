use crate::state::{MasterState, ReplicaInfo, SharedState};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Connect to a Valkey/Redis instance and send a PING command.
/// Returns true if we get a PONG reply within the timeout.
async fn ping_node(addr: SocketAddr, timeout_ms: u64) -> bool {
    let result =
        tokio::time::timeout(Duration::from_millis(timeout_ms), TcpStream::connect(addr)).await;

    let mut stream = match result {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            debug!("PING connect failed for {}: {}", addr, e);
            return false;
        }
        Err(_) => {
            debug!("PING connect timed out for {}", addr);
            return false;
        }
    };

    // Send PING as a simple RESP command.
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
    if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut stream, ping_cmd).await {
        debug!("PING write failed for {}: {}", addr, e);
        return false;
    }

    // Read reply.
    let mut buf = [0u8; 256];
    let result = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        tokio::io::AsyncReadExt::read(&mut stream, &mut buf),
    )
    .await;

    match result {
        Ok(Ok(n)) if n > 0 => {
            let reply = String::from_utf8_lossy(&buf[..n]);
            debug!("PING reply from {}: {:?}", addr, reply.trim());
            reply.contains("+PONG") || reply.contains("PONG")
        }
        _ => {
            debug!("PING read failed/timed out for {}", addr);
            false
        }
    }
}

/// Send INFO replication to a node and parse replica information.
async fn fetch_replicas(addr: SocketAddr, timeout_ms: u64) -> Vec<ReplicaInfo> {
    let result =
        tokio::time::timeout(Duration::from_millis(timeout_ms), TcpStream::connect(addr)).await;

    let mut stream = match result {
        Ok(Ok(s)) => s,
        _ => return Vec::new(),
    };

    let cmd = b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    if tokio::io::AsyncWriteExt::write_all(&mut stream, cmd)
        .await
        .is_err()
    {
        return Vec::new();
    }

    let mut buf = vec![0u8; 8192];
    let result = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        tokio::io::AsyncReadExt::read(&mut stream, &mut buf),
    )
    .await;

    let n = match result {
        Ok(Ok(n)) if n > 0 => n,
        _ => return Vec::new(),
    };

    let text = String::from_utf8_lossy(&buf[..n]);
    parse_replicas(&text)
}

/// Parse the replication section of INFO output for replica entries.
fn parse_replicas(info: &str) -> Vec<ReplicaInfo> {
    let mut replicas = Vec::new();
    let mut in_replication = false;

    for line in info.lines() {
        let line = line.trim();
        if line.starts_with("# Replication") {
            in_replication = true;
            continue;
        }
        if in_replication {
            if line.starts_with('#') || line.is_empty() {
                if !line.starts_with("# Replication") {
                    // End of replication section.
                    break;
                }
                continue;
            }
            if line.starts_with("slave") {
                // Parse: slave0:ip=127.0.0.1,port=6380,state=online,offset=123,lag=1
                // The key part may have a prefix like "slave0:ip" — take the part after ':'.
                let mut ip = String::new();
                let mut port = 0u16;
                let mut offset = 0u64;
                let priority = 100i64;

                for part in line.split(',') {
                    if let Some((k, v)) = part.split_once('=') {
                        // Strip prefix like "slave0:" from key.
                        let key = k.trim().rsplit(':').next().unwrap_or(k.trim());
                        match key {
                            "ip" => ip = v.to_string(),
                            "port" => port = v.parse().unwrap_or(0),
                            "offset" => offset = v.parse().unwrap_or(0),
                            _ => {}
                        }
                    }
                }

                if !ip.is_empty() && port > 0 {
                    if let Ok(addr) = format!("{}:{}", ip, port).parse::<SocketAddr>() {
                        replicas.push(ReplicaInfo {
                            addr,
                            replication_offset: offset,
                            priority,
                            last_seen: Instant::now(),
                        });
                    }
                }
            }
        }
    }

    replicas
}

/// Start the health-check monitoring loop.
/// Periodically pings all monitored masters and replicas, updates state,
/// and triggers failover when quorum is reached.
pub async fn start_monitor(state: SharedState, check_interval_ms: u64) {
    let mut ticker = interval(Duration::from_millis(check_interval_ms));

    info!(
        "Monitor loop started (interval={}ms, masters={})",
        check_interval_ms,
        state.masters.read().await.len()
    );

    loop {
        ticker.tick().await;

        let mut masters = state.masters.write().await;
        for (name, master) in masters.iter_mut() {
            let addr = master.current_primary;

            // Ping the current primary.
            let alive = ping_node(addr, master.down_after_ms).await;

            if alive {
                master.last_ping_reply = Some(Instant::now());
                if master.state == MasterState::Sdown {
                    info!("Master {} recovered from SDOWN", name);
                    master.state = MasterState::Ok;
                    master.sdown_since = None;
                }
            } else {
                if master.state == MasterState::Ok {
                    warn!("Master {}: no PING reply, marking SDOWN", name);
                    master.state = MasterState::Sdown;
                    master.sdown_since = Some(Instant::now());
                }
            }

            // Fetch replica info from the primary.
            if alive {
                let replicas = fetch_replicas(addr, 2000).await;
                if !replicas.is_empty() {
                    master.replicas = replicas;
                    debug!("Master {}: found {} replicas", name, master.replicas.len());
                }
            }

            // Check if we should transition to ODOWN (quorum).
            if master.state == MasterState::Sdown {
                // In a real implementation, we'd ask other sentinels.
                // For now, if only 1 sentinel is configured (quorum=1),
                // we go straight to ODOWN.
                if master.quorum <= 1 {
                    warn!("Master {}: quorum reached (quorum=1), marking ODOWN", name);
                    master.state = MasterState::Odown;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_replicas() {
        let info = r#"# Replication
role:master
connected_slaves:2
slave0:ip=127.0.0.1,port=6380,state=online,offset=42,lag=0
slave1:ip=127.0.0.1,port=6381,state=online,offset=40,lag=1
master_failover_state:no-failover
"#;
        let replicas = parse_replicas(info);
        assert_eq!(replicas.len(), 2);
        assert_eq!(replicas[0].addr.port(), 6380);
        assert_eq!(replicas[0].replication_offset, 42);
        assert_eq!(replicas[1].addr.port(), 6381);
    }

    #[test]
    fn test_parse_no_replicas() {
        let info = r#"# Replication
role:master
connected_slaves:0
"#;
        let replicas = parse_replicas(info);
        assert!(replicas.is_empty());
    }
}
