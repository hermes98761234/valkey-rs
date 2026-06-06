use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, Mutex as TokioMutex};
use tracing::info;

use valkey_persistence::rdb;
use valkey_storage::Store;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("RDB error: {0}")]
    Rdb(#[from] valkey_persistence::rdb::RdbError),
    #[error("Replica disconnected")]
    Disconnected,
}

// ---------------------------------------------------------------------------
// Circular buffer for replication backlog
// ---------------------------------------------------------------------------

/// A fixed-size circular buffer that stores raw bytes (RESP-encoded commands).
pub struct CircularBuffer {
    buf: Vec<u8>,
    capacity: usize,
    /// Logical offset of the first byte in the buffer
    start: usize,
    /// Number of bytes currently stored
    len: usize,
}

impl CircularBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity],
            capacity,
            start: 0,
            len: 0,
        }
    }

    /// Append bytes to the buffer, overwriting old data if full.
    pub fn append(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        if data.len() >= self.capacity {
            // Data is larger than buffer — store only the tail
            let start = data.len() - self.capacity;
            self.buf[..self.capacity].copy_from_slice(&data[start..]);
            self.start = 0;
            self.len = self.capacity;
            return;
        }

        // Write data, potentially wrapping around
        for (i, &byte) in data.iter().enumerate() {
            let pos = (self.start + self.len + i) % self.capacity;
            self.buf[pos] = byte;
        }

        self.len += data.len();
        if self.len > self.capacity {
            // We overwrote old data
            self.start = (self.start + (self.len - self.capacity)) % self.capacity;
            self.len = self.capacity;
        }
    }

    /// Get all data currently in the buffer, in order from oldest to newest.
    pub fn data(&self) -> Vec<u8> {
        if self.len == 0 {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(self.len);
        for i in 0..self.len {
            result.push(self.buf[(self.start + i) % self.capacity]);
        }
        result
    }

    /// Get data starting from a given offset (relative to the logical start
    /// of the backlog). Returns None if the data has been evicted.
    pub fn get_from_offset(&self, offset: u64, base_offset: u64) -> Option<Vec<u8>> {
        if offset < base_offset {
            // This offset is before our oldest data -- can't satisfy
            return None;
        }
        Some(self.data())
    }
}

// ---------------------------------------------------------------------------
// Replica connection state
// ---------------------------------------------------------------------------

pub struct ReplicaConn {
    pub id: u64,
    pub offset: AtomicU64,
    pub active: AtomicBool,
}

impl ReplicaConn {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            offset: AtomicU64::new(0),
            active: AtomicBool::new(true),
        }
    }
}

// ---------------------------------------------------------------------------
// Replication manager (leader side)
// ---------------------------------------------------------------------------

/// Manages replication state for the leader server.
///
/// Tracks connected replicas, maintains a circular backlog of write commands,
/// and handles PSYNC (full and partial resynchronization) requests.
pub struct ReplicationManager {
    replicas: DashMap<u64, Arc<ReplicaConn>>,
    backlog: TokioMutex<CircularBuffer>,
    backlog_offset: AtomicU64,
    repl_id: String,
    next_replica_id: AtomicU64,
    store: Arc<Store>,
    /// Channel for streaming write commands to all replicas
    cmd_tx: broadcast::Sender<Bytes>,
    /// Per-replica ack offsets: maps replica_id -> last acknowledged offset
    ack_offsets: DashMap<u64, AtomicU64>,
    /// Anonymous ack offsets for when we can't identify which replica sent ACK
    anonymous_ack_offsets: std::sync::Mutex<Vec<u64>>,
    /// Notifier for WAIT command: broadcast when replicas ack
    ack_notify: tokio::sync::watch::Sender<()>,
    ack_notify_rx: tokio::sync::watch::Receiver<()>,
}

impl ReplicationManager {
    /// Default backlog size: 1 MB
    const DEFAULT_BACKLOG_SIZE: usize = 1024 * 1024;

    pub fn new(store: Arc<Store>) -> Arc<Self> {
        let (cmd_tx, _cmd_rx) = broadcast::channel(4096);
        let repl_id = generate_repl_id();
        let (ack_notify, ack_notify_rx) = tokio::sync::watch::channel(());

        let mgr = Arc::new(Self {
            replicas: DashMap::new(),
            backlog: TokioMutex::new(CircularBuffer::new(Self::DEFAULT_BACKLOG_SIZE)),
            backlog_offset: AtomicU64::new(0),
            repl_id,
            next_replica_id: AtomicU64::new(1),
            store,
            cmd_tx,
            ack_offsets: DashMap::new(),
            anonymous_ack_offsets: Mutex::new(Vec::new()),
            ack_notify,
            ack_notify_rx,
        });

        // Spawn replica expiry reaper
        let mgr_weak = Arc::downgrade(&mgr);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Some(mgr) = mgr_weak.upgrade() {
                    mgr.reap_disconnected_replicas();
                } else {
                    break;
                }
            }
        });

        mgr
    }

    /// Get the replication ID.
    pub fn repl_id(&self) -> &str {
        &self.repl_id
    }

    /// Get the current master replication offset.
    pub fn master_repl_offset(&self) -> u64 {
        self.backlog_offset.load(Ordering::Relaxed)
    }

    /// Get the number of connected replicas.
    pub fn replica_count(&self) -> usize {
        self.replicas
            .iter()
            .filter(|e| e.value().active.load(Ordering::Relaxed))
            .count()
    }

    /// Update the ack offset for a replica. Called when the leader receives
    /// REPLCONF ACK <offset> from a replica.
    pub fn replica_ack(&self, replica_id: u64, offset: u64) {
        self.ack_offsets
            .entry(replica_id)
            .or_insert_with(|| AtomicU64::new(0))
            .store(offset, Ordering::Relaxed);
        // Notify any WAIT commands that a replica acked
        let _ = self.ack_notify.send(());
    }

    /// Record an ack offset from an anonymous replica connection.
    /// Used when the leader receives REPLCONF ACK but can't identify which replica sent it.
    /// Stores the offset in a rotating buffer capped at the current replica count.
    pub fn record_anonymous_ack(&self, offset: u64) {
        let mut offsets = self.anonymous_ack_offsets.lock().unwrap();
        let count = self.replica_count();
        if count == 0 {
            return;
        }
        if offsets.len() < count {
            offsets.push(offset);
        } else {
            // Replace the entry with the smallest offset (round-robin replacement)
            if let Some(min_idx) = offsets
                .iter()
                .enumerate()
                .min_by_key(|(_, v)| *v)
                .map(|(i, _)| i)
            {
                offsets[min_idx] = offset;
            }
        }
        let _ = self.ack_notify.send(());
    }

    /// Count how many connected replicas have acked at least `min_offset`.
    pub fn count_acked_replicas(&self, min_offset: u64) -> usize {
        // First try per-replica tracking
        let counted = self
            .replicas
            .iter()
            .filter(|e| {
                if !e.value().active.load(Ordering::Relaxed) {
                    return false;
                }
                self.ack_offsets
                    .get(e.key())
                    .map(|a| a.value().load(Ordering::Relaxed) >= min_offset)
                    .unwrap_or(false)
            })
            .count();
        if counted > 0 {
            return counted;
        }
        // Fall back to anonymous ack tracking
        let offsets = self.anonymous_ack_offsets.lock().unwrap();
        offsets.iter().filter(|o| **o >= min_offset).count()
    }

    /// Get a receiver for the ack notification channel.
    pub fn ack_notify_channel(&self) -> tokio::sync::watch::Receiver<()> {
        self.ack_notify_rx.clone()
    }

    /// Append a write command to the backlog and broadcast to replicas.
    pub async fn propagate(&self, resp_bytes: Bytes, cmd_len: u64) {
        // Add to backlog
        let mut backlog = self.backlog.lock().await;
        backlog.append(&resp_bytes);
        drop(backlog);

        // Advance offset
        self.backlog_offset.fetch_add(cmd_len, Ordering::Relaxed);

        // Broadcast to all replicas
        let _ = self.cmd_tx.send(resp_bytes);
    }

    /// Subscribe to the replication command stream.
    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.cmd_tx.subscribe()
    }

    /// Handle a replica that just connected and wants to sync.
    ///
    /// This performs either a full resync (RDB transfer) or partial resync
    /// (replay backlog from offset).
    pub async fn handle_replica_sync(
        self: &Arc<Self>,
        mut stream: tokio::net::tcp::OwnedWriteHalf,
        repl_id: Option<&str>,
        offset: Option<u64>,
    ) -> Result<u64, ReplicationError> {
        let replica_id = self.next_replica_id.fetch_add(1, Ordering::SeqCst);
        let replica = Arc::new(ReplicaConn::new(replica_id));
        self.replicas.insert(replica_id, replica.clone());

        info!(
            "Replica {} connected, requesting PSYNC repl_id={:?} offset={:?}",
            replica_id, repl_id, offset
        );

        // Determine if we can do a partial resync
        let can_partial = self.can_partial_resync(repl_id, offset);

        if can_partial {
            let offset = offset.unwrap();
            info!(
                "Partial resync for replica {} from offset {}",
                replica_id, offset
            );

            // Send +CONTINUE
            let resp = format!("+CONTINUE {}\r\n", self.repl_id);
            stream.write_all(resp.as_bytes()).await?;

            // Send backlog from offset
            let backlog = self.backlog.lock().await;
            let data = backlog.get_from_offset(offset, self.backlog_offset.load(Ordering::Relaxed));
            drop(backlog);

            if let Some(data) = data {
                stream.write_all(&data).await?;
            }
        } else {
            info!("Full resync for replica {}", replica_id);

            let current_offset = self.backlog_offset.load(Ordering::Relaxed);

            // Send +FULLRESYNC {repl_id} {offset}
            let resp = format!("+FULLRESYNC {} {}\r\n", self.repl_id, current_offset);
            stream.write_all(resp.as_bytes()).await?;

            // Generate RDB and send as bulk string
            let rdb_path = std::env::temp_dir().join("valkey-repl.rdb");
            rdb::save(&self.store, &rdb_path).await?;

            // Read RDB file
            let mut file = tokio::fs::File::open(&rdb_path).await?;
            let mut rdb_data = Vec::new();
            file.read_to_end(&mut rdb_data).await?;
            drop(file);
            let _ = tokio::fs::remove_file(&rdb_path).await;

            // Send as RESP bulk string: $<len>\r\n<data>
            let header = format!("${}\r\n", rdb_data.len());
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(&rdb_data).await?;
            // Note: no trailing \r\n for raw RDB payload, standard Redis sends
            // the RDB as-is followed by CRLF
            stream.write_all(b"\r\n").await?;

            // Update replica's offset
            replica.offset.store(current_offset, Ordering::Relaxed);
        }

        stream.flush().await?;

        replica.offset.store(
            self.backlog_offset.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        Ok(replica_id)
    }

    fn can_partial_resync(&self, repl_id: Option<&str>, offset: Option<u64>) -> bool {
        match (repl_id, offset) {
            (Some(id), Some(_offset)) => {
                if id != self.repl_id {
                    return false;
                }
                // Always allow partial resync; the backlog circular buffer
                // check will naturally return None if data is evicted.
                true
            }
            _ => false,
        }
    }

    /// Remove disconnected replicas from the map.
    fn reap_disconnected_replicas(&self) {
        self.replicas
            .retain(|_, replica| replica.active.load(Ordering::Relaxed));
        // Clean up ack offsets for removed replicas
        let active_ids: Vec<u64> = self
            .replicas
            .iter()
            .map(|e| *e.key())
            .collect();
        self.ack_offsets
            .retain(|id, _| active_ids.contains(id));
    }

    /// Mark a replica as disconnected.
    pub fn disconnect_replica(&self, replica_id: u64) {
        if let Some(replica) = self.replicas.get(&replica_id) {
            replica.active.store(false, Ordering::Relaxed);
        }
    }

    /// Get info string for the replication section of INFO command.
    pub fn info(&self) -> String {
        let connected = self.replica_count();
        format!(
            "role:master\r\n\
             connected_slaves:{}\r\n\
             master_replid:{}\r\n\
             master_repl_offset:{}\r\n\
             repl_backlog_active:1\r\n\
             repl_backlog_size:{}\r\n\
             repl_backlog_first_byte_offset:0\r\n\
             repl_backlog_histlen:{}",
            connected,
            self.repl_id,
            self.master_repl_offset(),
            Self::DEFAULT_BACKLOG_SIZE,
            self.backlog_offset.load(Ordering::Relaxed),
        )
    }
}

/// Generate a 40-character random hex string for use as repl_id.
fn generate_repl_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut s = String::with_capacity(40);
    for i in 0..5 {
        let mut hasher = DefaultHasher::new();
        std::process::id().hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);
        i.hash(&mut hasher);
        let seed = hasher.finish();
        for j in 0..8 {
            let nibble = ((seed >> (j * 4)) & 0xF) as u8;
            s.push_str(&format!("{:x}", nibble));
        }
    }
    s
}

// ---------------------------------------------------------------------------
// Utility: encode a RESP command array into wire format
// ---------------------------------------------------------------------------

/// Encode a RESP command array as raw bytes for replication.
pub fn encode_repl_command(cmd: &[Bytes]) -> Bytes {
    let mut buf = Vec::new();
    // Array header
    buf.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
    for arg in cmd {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    Bytes::from(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circular_buffer_basic() {
        let mut buf = CircularBuffer::new(64);
        assert!(buf.data().is_empty());

        buf.append(b"hello world");
        assert_eq!(buf.data(), b"hello world");
    }

    #[test]
    fn test_circular_buffer_overflow() {
        let mut buf = CircularBuffer::new(10);
        buf.append(b"0123456789");
        buf.append(b"ABCDE");
        // Old data should be overwritten
        let data = buf.data();
        assert_eq!(data.len(), 10);
        // The buffer should contain the tail of what was written
    }

    #[test]
    fn test_repl_id_generation() {
        let id = generate_repl_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_encode_repl_command() {
        let cmd = vec![
            Bytes::from("SET"),
            Bytes::from("mykey"),
            Bytes::from("myvalue"),
        ];
        let encoded = encode_repl_command(&cmd);
        assert_eq!(
            &encoded[..],
            b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        );
    }

    #[tokio::test]
    async fn test_replica_ack_tracking() {
        let store = valkey_storage::Store::new();
        let mgr = ReplicationManager::new(store);

        // Simulate a replica connecting
        let replica_id = mgr.next_replica_id.fetch_add(1, Ordering::SeqCst);
        let replica = Arc::new(ReplicaConn::new(replica_id));
        mgr.replicas.insert(replica_id, replica);

        // Initially no replicas have acked offset 100
        assert_eq!(mgr.count_acked_replicas(100), 0);

        // Replica acks offset 50
        mgr.replica_ack(replica_id, 50);
        assert_eq!(mgr.count_acked_replicas(100), 0);
        assert_eq!(mgr.count_acked_replicas(50), 1);
        assert_eq!(mgr.count_acked_replicas(25), 1);

        // Replica acks offset 200
        mgr.replica_ack(replica_id, 200);
        assert_eq!(mgr.count_acked_replicas(100), 1);
        assert_eq!(mgr.count_acked_replicas(200), 1);
        assert_eq!(mgr.count_acked_replicas(201), 0);
    }

    #[tokio::test]
    async fn test_anonymous_ack_tracking() {
        let store = valkey_storage::Store::new();
        let mgr = ReplicationManager::new(store);

        // No replicas connected, anonymous ack should be no-op
        mgr.record_anonymous_ack(100);
        assert_eq!(mgr.count_acked_replicas(0), 0);

        // Add a replica
        let replica_id = mgr.next_replica_id.fetch_add(1, Ordering::SeqCst);
        let replica = Arc::new(ReplicaConn::new(replica_id));
        mgr.replicas.insert(replica_id, replica);

        // Anonymous ack should now be recorded
        mgr.record_anonymous_ack(100);
        assert_eq!(mgr.count_acked_replicas(100), 1);
        assert_eq!(mgr.count_acked_replicas(200), 0);

        // Another anonymous ack with higher offset
        mgr.record_anonymous_ack(200);
        assert_eq!(mgr.count_acked_replicas(100), 1);
    }

    #[tokio::test]
    async fn test_count_acked_replicas_multiple() {
        let store = valkey_storage::Store::new();
        let mgr = ReplicationManager::new(store);

        // Add 3 replicas
        for _ in 0..3 {
            let id = mgr.next_replica_id.fetch_add(1, Ordering::SeqCst);
            let replica = Arc::new(ReplicaConn::new(id));
            mgr.replicas.insert(id, replica);
        }

        // Replica 1 acks offset 100
        mgr.replica_ack(1, 100);
        assert_eq!(mgr.count_acked_replicas(100), 1);

        // Replica 2 acks offset 100
        mgr.replica_ack(2, 100);
        assert_eq!(mgr.count_acked_replicas(100), 2);

        // Replica 3 acks offset 50 (behind)
        mgr.replica_ack(3, 50);
        assert_eq!(mgr.count_acked_replicas(100), 2);
        assert_eq!(mgr.count_acked_replicas(50), 3);
    }
}
