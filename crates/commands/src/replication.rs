use bytes::Bytes;
use std::sync::{Arc, OnceLock};
use valkey_proto::RespValue;
use valkey_replication::ReplicaState;
use valkey_storage::Store;

// ---------------------------------------------------------------------------
// Global replication state (initialized from server main)
// ---------------------------------------------------------------------------

static REPL_MGR: OnceLock<Option<Arc<valkey_replication::ReplicationManager>>> = OnceLock::new();
static REPLICA_STATE: OnceLock<Arc<ReplicaState>> = OnceLock::new();

/// Initialize the global replication state. Called once from server main.
pub fn init_replication(
    mgr: Option<Arc<valkey_replication::ReplicationManager>>,
    replica_state: Arc<ReplicaState>,
) {
    let _ = REPL_MGR.set(mgr);
    let _ = REPLICA_STATE.set(replica_state);
}

/// Get the replication manager if available.
pub fn replication_manager() -> Option<Arc<valkey_replication::ReplicationManager>> {
    REPL_MGR.get().and_then(|o| o.clone())
}

/// Get the replica state.
pub fn replica_state() -> Option<Arc<ReplicaState>> {
    REPLICA_STATE.get().cloned()
}

// ---------------------------------------------------------------------------
// REPLCONF command
// ---------------------------------------------------------------------------

/// Handle REPLCONF commands from replicas.
pub async fn handle_replconf(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'replconf' command".into());
    }

    let subcmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };

    match subcmd.as_str() {
        "LISTENING-PORT" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".into());
            }
            RespValue::ok()
        }
        "CAPA" => RespValue::ok(),
        "ACK" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".into());
            }
            if let Ok(offset_str) = std::str::from_utf8(&args[1]) {
                if let Ok(offset) = offset_str.parse::<u64>() {
                    if let Some(mgr) = replication_manager() {
                        mgr.record_anonymous_ack(offset);
                    }
                    return RespValue::ok();
                }
            }
            RespValue::Error("ERR invalid ACK offset".into())
        }
        "GETACK" => {
            if let Some(mgr) = replication_manager() {
                let offset = mgr.master_repl_offset();
                RespValue::Array(Some(vec![
                    RespValue::bulk(Bytes::from("REPLCONF")),
                    RespValue::bulk(Bytes::from("ACK")),
                    RespValue::bulk(Bytes::from(offset.to_string())),
                ]))
            } else {
                RespValue::Array(Some(vec![
                    RespValue::bulk(Bytes::from("REPLCONF")),
                    RespValue::bulk(Bytes::from("ACK")),
                    RespValue::bulk(Bytes::from("0")),
                ]))
            }
        }
        _ => RespValue::Error(format!("ERR unknown REPLCONF subcommand `{}`", subcmd).into()),
    }
}

// ---------------------------------------------------------------------------
// REPLICAOF command
// ---------------------------------------------------------------------------

/// Handle REPLICAOF command.
pub async fn cmd_replicaof(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    let state = match replica_state() {
        Some(s) => s,
        None => return RespValue::Error("ERR replication not initialized".into()),
    };

    match valkey_replication::replica::cmd_replicaof(args, store, state).await {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::Error(e),
    }
}

// ---------------------------------------------------------------------------
// WAIT command
// ---------------------------------------------------------------------------

/// Handle WAIT command.
///
/// WAIT numreplicas timeout
/// Blocks until `numreplicas` replicas have acknowledged all writes sent before
/// the WAIT command, or until `timeout` milliseconds have elapsed.
/// Returns the number of replicas that acknowledged (integer).
/// WAIT 0 0 returns immediately with the current ack count (non-blocking).
pub async fn cmd_wait(args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'wait' command".into());
    }

    let num_replicas: usize = match std::str::from_utf8(&args[0]) {
        Ok(s) => match s.parse() {
            Ok(n) => n,
            Err(_) => return RespValue::Error("ERR invalid numreplicas".into()),
        },
        Err(_) => return RespValue::Error("ERR invalid numreplicas".into()),
    };

    let timeout_ms: u64 = match std::str::from_utf8(&args[1]) {
        Ok(s) => match s.parse() {
            Ok(n) => n,
            Err(_) => return RespValue::Error("ERR invalid timeout".into()),
        },
        Err(_) => return RespValue::Error("ERR invalid timeout".into()),
    };

    let mgr = match replication_manager() {
        Some(m) => m,
        None => return RespValue::Integer(0),
    };

    // WAIT 0 0 = return immediately with current ack count
    if num_replicas == 0 && timeout_ms == 0 {
        let current_offset = mgr.master_repl_offset();
        let acked = mgr.count_acked_replicas(current_offset);
        return RespValue::Integer(acked as i64);
    }

    // Record the replication offset at the time of the WAIT call.
    // We need to wait until at least num_replicas replicas have acked
    // up to this offset.
    let target_offset = mgr.master_repl_offset();

    // Fast path: check if we already have enough acks
    let acked = mgr.count_acked_replicas(target_offset);
    if acked >= num_replicas {
        return RespValue::Integer(acked as i64);
    }

    // If timeout is 0, wait indefinitely
    if timeout_ms == 0 {
        let mut notify = mgr.ack_notify_channel();
        loop {
            notify.changed().await.ok();
            let acked = mgr.count_acked_replicas(target_offset);
            if acked >= num_replicas {
                return RespValue::Integer(acked as i64);
            }
        }
    }

    // Bounded wait with timeout
    let result = tokio::time::timeout(tokio::time::Duration::from_millis(timeout_ms), async {
        let mut notify = mgr.ack_notify_channel();
        loop {
            notify.changed().await.ok();
            let acked = mgr.count_acked_replicas(target_offset);
            if acked >= num_replicas {
                return acked;
            }
        }
    })
    .await;

    match result {
        Ok(acked) => RespValue::Integer(acked as i64),
        Err(_) => {
            // Timeout expired — return current ack count
            let acked = mgr.count_acked_replicas(target_offset);
            RespValue::Integer(acked as i64)
        }
    }
}

// ---------------------------------------------------------------------------
// PSYNC command
// ---------------------------------------------------------------------------

/// PSYNC is an internal replication command, not for client use.
pub async fn cmd_psync(_args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    RespValue::Error("ERR PSYNC is a replication internal command".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_wait_0_0_returns_immediately() {
        // WAIT 0 0 with no replication manager should return 0
        init_replication(None, valkey_replication::ReplicaState::new());
        let result = cmd_wait(&[Bytes::from("0"), Bytes::from("0")]).await;
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_wait_invalid_args() {
        init_replication(None, valkey_replication::ReplicaState::new());

        // Too few arguments
        let result = cmd_wait(&[Bytes::from("1")]).await;
        assert!(matches!(result, RespValue::Error(_)));

        // Too many arguments
        let result = cmd_wait(&[Bytes::from("1"), Bytes::from("100"), Bytes::from("extra")]).await;
        assert!(matches!(result, RespValue::Error(_)));

        // Invalid numreplicas
        let result = cmd_wait(&[Bytes::from("abc"), Bytes::from("100")]).await;
        assert!(matches!(result, RespValue::Error(_)));

        // Invalid timeout
        let result = cmd_wait(&[Bytes::from("1"), Bytes::from("abc")]).await;
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_wait_with_timeout_returns_within_timeout() {
        // WAIT 1 500 with no replicas should timeout after 500ms and return 0
        let store = valkey_storage::Store::new();
        let repl_mgr = valkey_replication::ReplicationManager::new(store);
        init_replication(Some(repl_mgr), valkey_replication::ReplicaState::new());

        let start = tokio::time::Instant::now();
        let result = cmd_wait(&[Bytes::from("1"), Bytes::from("500")]).await;
        let elapsed = start.elapsed();

        // Should return 0 (no replicas acked)
        assert_eq!(result, RespValue::Integer(0));
        // Should complete within the timeout (with some margin)
        assert!(elapsed < tokio::time::Duration::from_millis(1000));
    }
}
