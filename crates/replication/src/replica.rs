use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{info, warn, error};

use valkey_persistence::rdb;
use valkey_storage::Store;

// ---------------------------------------------------------------------------
// Replica state
// ---------------------------------------------------------------------------

/// Tracks the state of this server when acting as a replica.
pub struct ReplicaState {
    /// Whether this server is currently a replica (read-only mode).
    pub is_replica: AtomicBool,
    /// The host of the leader we're replicating from.
    pub master_host: std::sync::Mutex<Option<String>>,
    /// The port of the leader.
    pub master_port: std::sync::Mutex<Option<u16>>,
    /// Our current replication offset (how much we've applied).
    pub repl_offset: AtomicU64,
    /// The leader's repl_id.
    pub master_repl_id: std::sync::Mutex<Option<String>>,
    /// Whether replica-read-only is enabled (default: true).
    pub replica_read_only: AtomicBool,
}

impl ReplicaState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            is_replica: AtomicBool::new(false),
            master_host: std::sync::Mutex::new(None),
            master_port: std::sync::Mutex::new(None),
            repl_offset: AtomicU64::new(0),
            master_repl_id: std::sync::Mutex::new(None),
            replica_read_only: AtomicBool::new(true),
        })
    }

    /// Check if this server is currently acting as a replica.
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(Ordering::Relaxed)
    }

    /// Check if write commands should be rejected (replica-read-only).
    pub fn is_read_only(&self) -> bool {
        self.is_replica() && self.replica_read_only.load(Ordering::Relaxed)
    }

    /// Get the current replication offset.
    pub fn offset(&self) -> u64 {
        self.repl_offset.load(Ordering::Relaxed)
    }

    /// Update the replication offset.
    pub fn set_offset(&self, offset: u64) {
        self.repl_offset.store(offset, Ordering::Relaxed);
    }

    /// Get the master repl_id for PSYNC.
    pub fn master_repl_id(&self) -> Option<String> {
        self.master_repl_id.lock().unwrap().clone()
    }
}

// ---------------------------------------------------------------------------
// REPLICAOF command handler
// ---------------------------------------------------------------------------

/// Handle the REPLICAOF command.
///
/// `REPLICAOF host port` — start replicating from the given leader.
/// `REPLICAOF NO ONE` — stop replication and become a master.
pub async fn cmd_replicaof(
    args: &[Bytes],
    store: &Arc<Store>,
    repl_state: Arc<ReplicaState>,
) -> Result<(), String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'replicaof' command".into());
    }

    let host = std::str::from_utf8(&args[0])
        .map_err(|_| "ERR invalid host")?
        .to_string();
    let port_str = std::str::from_utf8(&args[1])
        .map_err(|_| "ERR invalid port")?;

    // Handle NO ONE
    if host.eq_ignore_ascii_case("NO") && port_str.eq_ignore_ascii_case("ONE") {
        info!("REPLICAOF NO ONE: stopping replication");
        repl_state.is_replica.store(false, Ordering::Relaxed);
        *repl_state.master_host.lock().unwrap() = None;
        *repl_state.master_port.lock().unwrap() = None;
        return Ok(());
    }

    let port: u16 = port_str.parse().map_err(|_| "ERR invalid port")?;

    info!("REPLICAOF {} {}: starting replication", host, port);

    // Store master info
    *repl_state.master_host.lock().unwrap() = Some(host.clone());
    *repl_state.master_port.lock().unwrap() = Some(port);

    // Connect to leader and perform sync
    let addr = format!("{}:{}", host, port);
    match TcpStream::connect(&addr).await {
        Ok(stream) => {
            if let Err(e) = perform_sync(stream, store, &repl_state).await {
                error!("Replication sync failed: {}", e);
                repl_state.is_replica.store(false, Ordering::Relaxed);
                return Err(format!("ERR replication sync failed: {}", e));
            }
        }
        Err(e) => {
            error!("Failed to connect to leader at {}: {}", addr, e);
            repl_state.is_replica.store(false, Ordering::Relaxed);
            return Err(format!("ERR could not connect to master: {}", e));
        }
    }

    Ok(())
}

/// Perform the PSYNC handshake and initial sync with the leader.
async fn perform_sync(
    mut stream: TcpStream,
    store: &Arc<Store>,
    repl_state: &Arc<ReplicaState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (read_half, mut write_half) = stream.split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // Step 1: Send PING
    write_all_resp(&mut write_half, b"*1\r\n$4\r\nPING\r\n").await?;
    line.clear();
    reader.read_line(&mut line).await?;
    if !line.starts_with("+PONG") {
        return Err(format!("Unexpected PONG response: {}", line.trim()).into());
    }
    info!("Replica: received PONG from leader");

    // Step 2: Send REPLCONF listening-port
    // Use port 0 for now (we don't know our listening port in this context)
    let replconf_listening = b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
    write_all_resp(&mut write_half, replconf_listening).await?;
    line.clear();
    reader.read_line(&mut line).await?;
    if !line.starts_with("+OK") {
        warn!("Unexpected REPLCONF response: {}", line.trim());
    }

    // Step 3: Send REPLCONF capa eof psync2
    let replconf_capa = b"*4\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$7\r\npsync2\r\n";
    write_all_resp(&mut write_half, replconf_capa).await?;
    line.clear();
    reader.read_line(&mut line).await?;
    if !line.starts_with("+OK") {
        warn!("Unexpected REPLCONF capa response: {}", line.trim());
    }

    // Step 4: Send PSYNC
    let (repl_id, offset) = if let Some(id) = repl_state.master_repl_id() {
        let off = repl_state.offset();
        (id, off as i64)
    } else {
        ("?".to_string(), -1i64)
    };

    let psync_cmd = format!("*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        repl_id.len(), repl_id,
        offset.to_string().len(), offset);
    write_all_resp(&mut write_half, psync_cmd.as_bytes()).await?;

    // Step 5: Read the response (+FULLRESYNC or +CONTINUE)
    line.clear();
    reader.read_line(&mut line).await?;

    if line.starts_with("+FULLRESYNC") {
        // Parse: +FULLRESYNC <repl_id> <offset>\r\n
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() >= 3 {
            let master_id = parts[1].to_string();
            let master_offset: u64 = parts[2].parse().unwrap_or(0);
            info!("Replica: FULLRESYNC from repl_id={} offset={}", master_id, master_offset);
            *repl_state.master_repl_id.lock().unwrap() = Some(master_id);
            repl_state.set_offset(master_offset);
        }

        // Read RDB bulk string
        // The next line should be $<len>\r\n
        line.clear();
        reader.read_line(&mut line).await?;
        if !line.starts_with('$') {
            return Err(format!("Expected RDB bulk string, got: {}", line.trim()).into());
        }
        let rdb_len: usize = line[1..].trim().parse()
            .map_err(|e| format!("Invalid RDB length: {}", e))?;

        // Read RDB data + trailing \r\n
        let mut rdb_data = vec![0u8; rdb_len + 2];
        reader.read_exact(&mut rdb_data).await?;
        // Strip trailing \r\n
        let rdb_data = &rdb_data[..rdb_len];

        info!("Replica: received RDB data ({} bytes)", rdb_len);

        // Load RDB into store
        let rdb_path = std::env::temp_dir().join("valkey-repl-replica.rdb");
        tokio::fs::write(&rdb_path, rdb_data).await?;
        rdb::load(store, &rdb_path).await
            .map_err(|e| format!("RDB load error: {}", e))?;
        let _ = tokio::fs::remove_file(&rdb_path).await;

        info!("Replica: RDB loaded successfully");
    } else if line.starts_with("+CONTINUE") {
        info!("Replica: partial resync (CONTINUE)");
        // Backlog data will follow in the replication stream
    } else {
        return Err(format!("Unexpected PSYNC response: {}", line.trim()).into());
    }

    // Mark as active replica
    repl_state.is_replica.store(true, Ordering::Relaxed);

    // Step 6: Enter replication stream loop
    info!("Replica: entering replication stream loop");
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                warn!("Replica: leader closed connection");
                repl_state.is_replica.store(false, Ordering::Relaxed);
                break;
            }
            Ok(_) => {
                // Parse the replication stream commands and apply them
                // For now, we just update the offset
                repl_state.set_offset(repl_state.offset() + line.len() as u64);
            }
            Err(e) => {
                error!("Replica: read error: {}", e);
                repl_state.is_replica.store(false, Ordering::Relaxed);
                break;
            }
        }
    }

    Ok(())
}

/// Write raw bytes to an AsyncWrite.
async fn write_all_resp<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_state_default() {
        let state = ReplicaState::new();
        assert!(!state.is_replica());
        assert!(!state.is_read_only());
        assert_eq!(state.offset(), 0);
    }

    #[test]
    fn test_replica_read_only() {
        let state = ReplicaState::new();
        state.is_replica.store(true, Ordering::Relaxed);
        assert!(state.is_read_only());

        state.replica_read_only.store(false, Ordering::Relaxed);
        assert!(!state.is_read_only());
    }
}
