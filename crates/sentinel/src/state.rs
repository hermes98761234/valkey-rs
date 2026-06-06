use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// State of a monitored master from this sentinel's perspective.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MasterState {
    /// Master is responding to PING.
    Ok,
    /// This sentinel thinks the master is down (no PING reply within down-after-ms).
    Sdown,
    /// Quorum of sentinels agree the master is down.
    Odown,
    /// Failover is in progress.
    Failover,
}

/// Information about a single replica.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub addr: SocketAddr,
    /// Replication offset (approximate, from INFO replication).
    pub replication_offset: u64,
    /// Priority (lower = preferred for promotion).
    pub priority: i64,
    /// Last time we heard from this replica.
    pub last_seen: Instant,
}

/// Information about another sentinel monitoring the same master.
#[derive(Debug, Clone)]
pub struct SentinelPeer {
    pub addr: SocketAddr,
    /// Sentinel's unique ID (if known).
    pub run_id: Option<String>,
    pub last_seen: Instant,
}

/// Full state for one monitored master.
#[derive(Debug, Clone)]
pub struct MasterInfo {
    pub name: String,
    pub addr: SocketAddr,
    /// How many sentinels must agree for ODOWN.
    pub quorum: u32,
    /// Time before marking SDOWN.
    pub down_after_ms: u64,
    /// Failover timeout in ms.
    pub failover_timeout: u64,
    /// Current state.
    pub state: MasterState,
    /// Known replicas.
    pub replicas: Vec<ReplicaInfo>,
    /// Other sentinels monitoring this master.
    pub sentinels: Vec<SentinelPeer>,
    /// When we last got a PING reply.
    pub last_ping_reply: Option<Instant>,
    /// When SDOWN was detected.
    pub sdown_since: Option<Instant>,
    /// Failover epoch (incremented each failover attempt).
    pub failover_epoch: u64,
    /// Current primary address (may differ from addr after failover).
    pub current_primary: SocketAddr,
}

impl MasterInfo {
    pub fn new(name: String, addr: SocketAddr, quorum: u32, down_after_ms: u64) -> Self {
        Self {
            name,
            addr,
            quorum,
            down_after_ms,
            failover_timeout: 60_000,
            state: MasterState::Ok,
            replicas: Vec::new(),
            sentinels: Vec::new(),
            last_ping_reply: None,
            sdown_since: None,
            failover_epoch: 0,
            current_primary: addr,
        }
    }

    /// Check if the master should transition to SDOWN.
    pub fn check_sdown(&self) -> bool {
        if self.state != MasterState::Ok {
            return false;
        }
        match self.last_ping_reply {
            None => true, // never got a reply
            Some(t) => t.elapsed() > Duration::from_millis(self.down_after_ms),
        }
    }
}

/// Global sentinel state, shared across all tasks.
pub struct SentinelState {
    /// This sentinel's unique ID (random hex string).
    pub myid: String,
    /// All monitored masters, keyed by name.
    pub masters: RwLock<HashMap<String, MasterInfo>>,
    /// Port this sentinel listens on.
    pub port: u16,
}

impl SentinelState {
    pub fn new(port: u16) -> Self {
        let myid = format!("{:040x}", rand::random::<u128>());
        Self {
            myid,
            masters: RwLock::new(HashMap::new()),
            port,
        }
    }

    pub fn with_masters(port: u16, masters: HashMap<String, MasterInfo>) -> Self {
        let myid = format!("{:040x}", rand::random::<u128>());
        Self {
            myid,
            masters: RwLock::new(masters),
            port,
        }
    }
}

pub type SharedState = std::sync::Arc<SentinelState>;
