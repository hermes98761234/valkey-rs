use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Number of hash slots in Redis Cluster.
pub const NUM_SLOTS: usize = 16384;

/// Role of a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Master,
    Slave,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Master => write!(f, "master"),
            NodeRole::Slave => write!(f, "slave"),
        }
    }
}

/// Flags for a cluster node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeFlags {
    pub myself: bool,
    pub master: bool,
    pub slave: bool,
    pub pfail: bool,
    pub fail: bool,
    pub noaddr: bool,
    pub handshake: bool,
    pub noflags: bool,
}

impl NodeFlags {
    pub fn to_str(&self) -> String {
        let mut flags = Vec::new();
        if self.myself {
            flags.push("myself");
        }
        if self.master {
            flags.push("master");
        }
        if self.slave {
            flags.push("slave");
        }
        if self.pfail {
            flags.push("pfail");
        }
        if self.fail {
            flags.push("fail");
        }
        if self.noaddr {
            flags.push("noaddr");
        }
        if self.handshake {
            flags.push("handshake");
        }
        if self.noflags {
            flags.push("noflags");
        }
        flags.join(",")
    }
}

/// A range of hash slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

impl SlotRange {
    pub fn new(start: u16, end: u16) -> Self {
        Self { start, end }
    }

    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }
}

/// Information about a single cluster node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: String,
    pub addr: String,
    pub role: NodeRole,
    pub epoch: u64,
    pub flags: NodeFlags,
    pub slots: Vec<SlotRange>,
}

impl NodeInfo {
    pub fn new(id: String, addr: String, role: NodeRole) -> Self {
        Self {
            id,
            addr,
            role,
            epoch: 0,
            flags: NodeFlags::default(),
            slots: Vec::new(),
        }
    }

    /// Format a single node as a CLUSTER NODES line.
    pub fn to_nodes_line(&self) -> String {
        let slots_str = if self.slots.is_empty() {
            String::new()
        } else {
            self.slots
                .iter()
                .map(|r| {
                    if r.start == r.end {
                        format!("{}", r.start)
                    } else {
                        format!("{}-{}", r.start, r.end)
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        };

        let _role_str = match self.role {
            NodeRole::Master => "master",
            NodeRole::Slave => "slave",
        };

        let master_id = if self.role == NodeRole::Slave {
            " -"
        } else {
            ""
        };

        format!(
            "{} {} {} {} {} {} {} {}",
            self.id,
            self.addr,
            self.flags.to_str(),
            master_id,
            self.epoch,
            self.epoch,
            self.epoch,
            slots_str
        )
    }
}

/// The full cluster topology state, shared across tasks.
pub struct ClusterState {
    /// This node's own info (mutable).
    pub myself: RwLock<NodeInfo>,
    /// All known nodes, keyed by node_id.
    pub nodes: DashMap<String, NodeInfo>,
    /// Slot-to-node mapping: slots[slot] = Some(node_id).
    pub slots: RwLock<Vec<Option<String>>>,
    /// Slots being migrated: slot -> target node_id.
    pub migrating: RwLock<HashMap<u16, String>>,
    /// Slots being imported: slot -> source node_id.
    pub importing: RwLock<HashMap<u16, String>>,
    /// Current config epoch.
    pub current_epoch: RwLock<u64>,
}

impl ClusterState {
    pub fn new(myself: NodeInfo) -> Arc<Self> {
        let mut slots_vec = Vec::with_capacity(NUM_SLOTS);
        slots_vec.resize_with(NUM_SLOTS, || None);

        let myself_id = myself.id.clone();
        let nodes = DashMap::new();
        nodes.insert(myself_id, myself.clone());

        Arc::new(Self {
            myself: RwLock::new(myself),
            nodes,
            slots: RwLock::new(slots_vec),
            migrating: RwLock::new(HashMap::new()),
            importing: RwLock::new(HashMap::new()),
            current_epoch: RwLock::new(0),
        })
    }

    /// Get the node_id that owns a given slot.
    pub fn slot_owner(&self, slot: u16) -> Option<String> {
        if slot as usize >= NUM_SLOTS {
            return None;
        }
        let slots = self.slots.read().unwrap();
        slots[slot as usize].clone()
    }

    /// Check if a slot is being migrated.
    pub fn is_migrating(&self, slot: u16) -> Option<String> {
        let migrating = self.migrating.read().unwrap();
        migrating.get(&slot).cloned()
    }

    /// Check if a slot is being imported.
    pub fn is_importing(&self, slot: u16) -> Option<String> {
        let importing = self.importing.read().unwrap();
        importing.get(&slot).cloned()
    }

    /// Assign a range of slots to a node.
    pub fn assign_slots(&self, node_id: &str, ranges: &[SlotRange]) {
        let mut slots = self.slots.write().unwrap();
        for range in ranges {
            for slot in range.start..=range.end {
                if (slot as usize) < NUM_SLOTS {
                    slots[slot as usize] = Some(node_id.to_string());
                }
            }
        }
        drop(slots);
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            node.slots = ranges.to_vec();
        }
    }

    /// Add or update a node from gossip.
    pub fn merge_node(&self, info: NodeInfo) {
        let id = info.id.clone();
        if let Some(existing) = self.nodes.get(&id) {
            if info.epoch < existing.epoch {
                return;
            }
        }
        self.nodes.insert(id, info);
    }

    /// Set self role and flags.
    pub fn set_self_role(&self, role: NodeRole) {
        let mut myself = self.myself.write().unwrap();
        myself.role = role;
        match role {
            NodeRole::Master => {
                myself.flags.master = true;
                myself.flags.slave = false;
            }
            NodeRole::Slave => {
                myself.flags.slave = true;
                myself.flags.master = false;
            }
        }
        drop(myself);
        let myself_read = self.myself.read().unwrap();
        self.nodes
            .insert(myself_read.id.clone(), myself_read.clone());
    }

    /// Set self epoch.
    pub fn set_self_epoch(&self, epoch: u64) {
        let mut myself = self.myself.write().unwrap();
        myself.epoch = epoch;
        drop(myself);
        let myself_read = self.myself.read().unwrap();
        self.nodes
            .insert(myself_read.id.clone(), myself_read.clone());
    }

    /// Add a new node from handshake.
    pub fn add_node(&self, node_id: String, addr: String, role: NodeRole, flags: NodeFlags) {
        let mut info = NodeInfo::new(node_id.clone(), addr, role);
        info.flags = flags;
        self.nodes.insert(node_id, info);
    }

    /// Generate CLUSTER INFO output.
    pub fn cluster_info(&self) -> String {
        let nodes_count = self.nodes.len();
        let masters_count = self
            .nodes
            .iter()
            .filter(|n| n.role == NodeRole::Master)
            .count();
        let assigned = self
            .slots
            .read()
            .unwrap()
            .iter()
            .filter(|s| s.is_some())
            .count();
        let current_epoch = *self.current_epoch.read().unwrap();
        let my_epoch = self.myself.read().unwrap().epoch;

        let lines = vec![
            format!("cluster_state:{}", if assigned > 0 { "ok" } else { "fail" }),
            format!("cluster_slots_assigned:{}", assigned),
            format!("cluster_slots_ok:{}", assigned),
            format!("cluster_slots_pfail:0"),
            format!("cluster_slots_fail:0"),
            format!("cluster_known_nodes:{}", nodes_count),
            format!("cluster_size:{}", masters_count),
            format!("cluster_current_epoch:{}", current_epoch),
            format!("cluster_my_epoch:{}", my_epoch),
            format!("cluster_stats_messages_sent:0"),
            format!("cluster_stats_messages_received:0"),
        ];
        lines.join("\r\n") + "\r\n"
    }

    /// Generate CLUSTER NODES output.
    pub fn cluster_nodes(&self) -> String {
        let mut lines = Vec::new();
        for entry in self.nodes.iter() {
            lines.push(entry.value().to_nodes_line());
        }
        lines.join("\r\n") + "\r\n"
    }
}

/// Generate a random 40-character hex node ID.
pub fn generate_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut result = String::with_capacity(40);
    let mut state = seed;
    for _ in 0..40 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let nibble = ((state >> 32) & 0xF) as u8;
        result.push_str(&format!("{:x}", nibble));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_node_id_format() {
        let id = generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn node_flags_format() {
        let mut flags = NodeFlags::default();
        flags.myself = true;
        flags.master = true;
        assert_eq!(flags.to_str(), "myself,master");
    }

    #[test]
    fn slot_range_contains() {
        let range = SlotRange::new(0, 100);
        assert!(range.contains(0));
        assert!(range.contains(50));
        assert!(range.contains(100));
        assert!(!range.contains(101));
    }

    #[test]
    fn cluster_state_new() {
        let node = NodeInfo::new(
            generate_node_id(),
            "127.0.0.1:6379".into(),
            NodeRole::Master,
        );
        let state = ClusterState::new(node);
        assert_eq!(state.slots.read().unwrap().len(), NUM_SLOTS);
        assert!(state.slots.read().unwrap().iter().all(|s| s.is_none()));
    }

    #[test]
    fn assign_slots() {
        let node = NodeInfo::new(
            generate_node_id(),
            "127.0.0.1:6379".into(),
            NodeRole::Master,
        );
        let node_id = node.id.clone();
        let state = ClusterState::new(node);
        state.assign_slots(&node_id, &[SlotRange::new(0, 5460)]);
        assert_eq!(state.slot_owner(0), Some(node_id.clone()));
        assert_eq!(state.slot_owner(5460), Some(node_id));
        assert_eq!(state.slot_owner(5461), None);
    }

    #[test]
    fn cluster_info_format() {
        let node = NodeInfo::new(
            generate_node_id(),
            "127.0.0.1:6379".into(),
            NodeRole::Master,
        );
        let state = ClusterState::new(node);
        let info = state.cluster_info();
        assert!(info.contains("cluster_state:"));
        assert!(info.contains("cluster_known_nodes:1"));
    }
}
