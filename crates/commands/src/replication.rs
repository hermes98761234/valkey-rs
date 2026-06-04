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
        "CAPA" => {
            RespValue::ok()
        }
        "ACK" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".into());
            }
            if let Ok(offset_str) = std::str::from_utf8(&args[1]) {
                if offset_str.parse::<u64>().is_ok() {
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

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);
    let mut wait_duration = tokio::time::Duration::from_millis(1);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        let connected = mgr.replica_count();
        if connected >= num_replicas {
            return RespValue::Integer(connected as i64);
        }

        let remaining = deadline - tokio::time::Instant::now();
        let sleep_time = wait_duration.min(remaining);
        tokio::time::sleep(sleep_time).await;
        wait_duration = (wait_duration * 2).min(tokio::time::Duration::from_millis(50));
    }

    RespValue::Integer(mgr.replica_count() as i64)
}

// ---------------------------------------------------------------------------
// PSYNC command
// ---------------------------------------------------------------------------

/// PSYNC is an internal replication command, not for client use.
pub async fn cmd_psync(_args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    RespValue::Error("ERR PSYNC is a replication internal command".into())
}

