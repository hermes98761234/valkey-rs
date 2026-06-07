use crate::state::{MasterState, SharedState};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

/// Send a command to a Valkey/Redis node and read the reply.
async fn send_command(addr: SocketAddr, cmd: &[u8], timeout_ms: u64) -> Option<Vec<u8>> {
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        TcpStream::connect(addr),
    )
    .await;

    let mut stream = match result {
        Ok(Ok(s)) => s,
        _ => return None,
    };

    if stream.write_all(cmd).await.is_err() {
        return None;
    }

    let mut buf = vec![0u8; 4096];
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        stream.read(&mut buf),
    )
    .await;

    match result {
        Ok(Ok(n)) if n > 0 => Some(buf[..n].to_vec()),
        _ => None,
    }
}

/// Promote a replica to primary by sending REPLICAOF NO ONE.
async fn promote_replica(addr: SocketAddr) -> bool {
    let cmd = b"*3\r\n$8\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n";
    match send_command(addr, cmd, 5000).await {
        Some(reply) => {
            let text = String::from_utf8_lossy(&reply);
            let ok = text.contains("+OK");
            if ok {
                info!("Promoted {} to primary", addr);
            } else {
                warn!("REPLICAOF NO ONE reply from {}: {:?}", addr, text.trim());
            }
            ok
        }
        None => {
            warn!("Failed to send REPLICAOF NO ONE to {}", addr);
            false
        }
    }
}

/// Reconfigure a replica to replicate from a new primary.
async fn reconfigure_replica(replica_addr: SocketAddr, new_primary: SocketAddr) -> bool {
    let host = new_primary.ip().to_string();
    let port = new_primary.port().to_string();
    // Build: *3\r\n$8\r\nREPLICAOF\r\n$<hostlen>\r\n<host>\r\n$<portlen>\r\n<port>\r\n
    let cmd = format!(
        "*3\r\n$8\r\nREPLICAOF\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        host.len(),
        host,
        port.len(),
        port
    );
    match send_command(replica_addr, cmd.as_bytes(), 5000).await {
        Some(reply) => {
            let text = String::from_utf8_lossy(&reply);
            let ok = text.contains("+OK");
            if ok {
                info!(
                    "Reconfigured {} to replicate from {}",
                    replica_addr, new_primary
                );
            } else {
                warn!("REPLICAOF reply from {}: {:?}", replica_addr, text.trim());
            }
            ok
        }
        None => {
            warn!("Failed to reconfigure replica {}", replica_addr);
            false
        }
    }
}

/// Pick the best replica for promotion.
/// Criteria: lowest priority value, then highest replication offset.
fn pick_best_replica(replicas: &[crate::state::ReplicaInfo]) -> Option<&crate::state::ReplicaInfo> {
    replicas
        .iter()
        .min_by_key(|r| (r.priority, std::cmp::Reverse(r.replication_offset)))
}

/// Execute failover for a master that is in ODOWN state.
/// 1. Pick the best replica.
/// 2. Promote it to primary.
/// 3. Reconfigure other replicas to point to the new primary.
/// 4. Update master state.
pub async fn execute_failover(state: SharedState, master_name: &str) -> bool {
    let mut masters = state.masters.write().await;
    let master = match masters.get_mut(master_name) {
        Some(m) => m,
        None => {
            warn!("Failover: master '{}' not found", master_name);
            return false;
        }
    };

    if master.state != MasterState::Odown {
        warn!(
            "Failover: master '{}' is not in ODOWN state (current: {:?})",
            master_name, master.state
        );
        return false;
    }

    master.state = MasterState::Failover;
    master.failover_epoch += 1;
    let epoch = master.failover_epoch;

    info!(
        "Starting failover for '{}' (epoch={}), replicas={}",
        master_name,
        epoch,
        master.replicas.len()
    );

    if master.replicas.is_empty() {
        warn!("Failover: no replicas available for '{}'", master_name);
        master.state = MasterState::Odown;
        return false;
    }

    // Pick best replica.
    let best = pick_best_replica(&master.replicas).unwrap().clone();
    info!("Selected replica {} for promotion", best.addr);

    // Promote.
    if !promote_replica(best.addr).await {
        warn!("Failed to promote replica {}", best.addr);
        master.state = MasterState::Odown;
        return false;
    }

    let new_primary = best.addr;
    master.current_primary = new_primary;

    // Reconfigure other replicas.
    let other_replicas: Vec<_> = master
        .replicas
        .iter()
        .filter(|r| r.addr != new_primary)
        .cloned()
        .collect();

    for replica in &other_replicas {
        reconfigure_replica(replica.addr, new_primary).await;
    }

    // Remove the promoted replica from the replica list.
    master.replicas.retain(|r| r.addr != new_primary);

    // Add old primary as a replica (it will be reconfigured when it comes back).
    master.replicas.push(crate::state::ReplicaInfo {
        addr: master.addr,
        replication_offset: 0,
        priority: 100,
        last_seen: std::time::Instant::now(),
    });

    master.state = MasterState::Ok;
    master.last_ping_reply = Some(std::time::Instant::now());
    master.sdown_since = None;

    info!(
        "Failover complete for '{}': new primary is {} (epoch={})",
        master_name, new_primary, epoch
    );

    // Publish +switch-master event (in a real impl, this goes to PubSub channels).
    info!(
        "+switch-master {} {} {} {}",
        master_name, master.addr, new_primary, epoch
    );

    true
}

/// Force a failover (SENTINEL failover command).
pub async fn force_failover(state: SharedState, master_name: &str) -> bool {
    info!("Force failover requested for '{}'", master_name);

    // Set to ODOWN so execute_failover will proceed.
    {
        let mut masters = state.masters.write().await;
        if let Some(master) = masters.get_mut(master_name) {
            master.state = MasterState::Odown;
        } else {
            return false;
        }
    }

    execute_failover(state, master_name).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ReplicaInfo;

    #[test]
    fn test_pick_best_replica() {
        let replicas = vec![
            ReplicaInfo {
                addr: "127.0.0.1:6380".parse().unwrap(),
                replication_offset: 100,
                priority: 100,
                last_seen: std::time::Instant::now(),
            },
            ReplicaInfo {
                addr: "127.0.0.1:6381".parse().unwrap(),
                replication_offset: 200,
                priority: 50,
                last_seen: std::time::Instant::now(),
            },
            ReplicaInfo {
                addr: "127.0.0.1:6382".parse().unwrap(),
                replication_offset: 150,
                priority: 50,
                last_seen: std::time::Instant::now(),
            },
        ];

        let best = pick_best_replica(&replicas).unwrap();
        // Should pick the one with lowest priority (50) and highest offset (200).
        assert_eq!(best.addr.port(), 6381);
    }
}
