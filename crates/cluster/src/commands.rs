use crate::slots::key_hash_slot;
use crate::state::{ClusterState, NodeRole};
use bytes::Bytes;
use std::fs;
use std::sync::Arc;
use tracing::{info, warn};

/// Result of checking whether a command can be executed locally.
#[derive(Debug)]
pub enum RouteAction {
    /// Execute the command locally.
    Local,
    /// Redirect with MOVED error.
    Moved { slot: u16, addr: String },
    /// Redirect with ASK error.
    Asking { slot: u16, addr: String },
}

/// Cluster command router — determines where a command should be executed.
pub struct ClusterRouter {
    state: Arc<ClusterState>,
    /// Tracks whether the next command should bypass slot checks (ASKING).
    asking: std::sync::atomic::AtomicBool,
}

impl ClusterRouter {
    pub fn new(state: Arc<ClusterState>) -> Self {
        Self {
            state,
            asking: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Set the ASK flag (after receiving an ASK redirect).
    pub fn set_asking(&self, val: bool) {
        self.asking.store(val, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check if ASK flag is set.
    pub fn is_asking(&self) -> bool {
        self.asking.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Route a command based on its key.
    pub fn route(&self, key: &[u8]) -> RouteAction {
        if self.is_asking() {
            self.asking
                .store(false, std::sync::atomic::Ordering::SeqCst);
            return RouteAction::Local;
        }

        let slot = key_hash_slot(key);

        // Check if this slot is being imported (ASK redirect)
        if let Some(source_id) = self.state.is_importing(slot) {
            if let Some(source_node) = self.state.nodes.get(&source_id) {
                return RouteAction::Asking {
                    slot,
                    addr: source_node.addr.clone(),
                };
            }
        }

        // Check slot ownership
        match self.state.slot_owner(slot) {
            Some(node_id) => {
                let myself = self.state.myself.read().unwrap();
                if node_id == myself.id {
                    RouteAction::Local
                } else {
                    drop(myself);
                    if let Some(node) = self.state.nodes.get(&node_id) {
                        RouteAction::Moved {
                            slot,
                            addr: node.addr.clone(),
                        }
                    } else {
                        RouteAction::Local
                    }
                }
            }
            None => RouteAction::Local,
        }
    }
}

/// Handler for CLUSTER subcommands.
pub struct ClusterCommandHandler {
    state: Arc<ClusterState>,
    nodes_conf_path: String,
}

impl ClusterCommandHandler {
    pub fn new(state: Arc<ClusterState>, nodes_conf_path: String) -> Self {
        Self {
            state,
            nodes_conf_path,
        }
    }

    /// Handle a CLUSTER subcommand.
    /// Returns a RESP-formatted response string.
    pub fn handle(&self, subcmd: &str, args: &[Bytes]) -> String {
        match subcmd.to_ascii_uppercase().as_str() {
            "INFO" => self.cluster_info(),
            "NODES" => self.cluster_nodes(),
            "MEET" => self.cluster_meet(args),
            "REPLICATE" => self.cluster_replicate(args),
            "FAILOVER" => self.cluster_failover(args),
            "RESET" => self.cluster_reset(args),
            "KEYSLOT" => self.cluster_keyslot(args),
            "GETKEYSINSLOT" => self.cluster_getkeysinslot(args),
            "COUNTKEYSINSLOT" => self.cluster_countkeysinslot(args),
            "ADDSLOTS" => self.cluster_addslots(args),
            "DELSLOTS" => self.cluster_delslots(args),
            "SETSLOT" => self.cluster_setslot(args),
            "MYID" => self.cluster_myid(),
            "SLOTS" => self.cluster_slots(),
            "SAVECONFIG" => self.cluster_saveconfig(),
            _ => format!("-ERR Unknown CLUSTER subcommand '{}'\r\n", subcmd),
        }
    }

    fn cluster_info(&self) -> String {
        self.state.cluster_info()
    }

    fn cluster_nodes(&self) -> String {
        self.state.cluster_nodes()
    }

    fn cluster_meet(&self, args: &[Bytes]) -> String {
        if args.len() < 2 {
            return "-ERR wrong number of arguments for 'CLUSTER MEET'\r\n".to_string();
        }
        let ip = String::from_utf8_lossy(&args[0]);
        let port = String::from_utf8_lossy(&args[1]);
        let addr = format!("{}:{}", ip, port);

        info!("CLUSTER MEET from {}", addr);

        let peer_id = crate::state::generate_node_id();
        self.state.add_node(
            peer_id,
            addr,
            NodeRole::Master,
            crate::state::NodeFlags {
                handshake: true,
                ..Default::default()
            },
        );

        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_replicate(&self, args: &[Bytes]) -> String {
        if args.is_empty() {
            return "-ERR wrong number of arguments for 'CLUSTER REPLICATE'\r\n".to_string();
        }
        let target_id = String::from_utf8_lossy(&args[0]).to_string();

        if !self.state.nodes.contains_key(&target_id) {
            return format!("-ERR Unknown node {}\r\n", target_id);
        }

        self.state.set_self_role(NodeRole::Slave);
        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_failover(&self, args: &[Bytes]) -> String {
        let _force = args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase() == "FORCE")
            .unwrap_or(false);
        let _takeover = args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase() == "TAKEOVER")
            .unwrap_or(false);

        let myself = self.state.myself.read().unwrap();
        if myself.role != NodeRole::Slave {
            return "-ERR FAILOVER only works on slave nodes\r\n".to_string();
        }
        let new_epoch = myself.epoch + 1;
        drop(myself);

        self.state.set_self_epoch(new_epoch);
        self.state.set_self_role(NodeRole::Master);
        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_reset(&self, args: &[Bytes]) -> String {
        let hard = args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase() == "HARD")
            .unwrap_or(false);

        info!("CLUSTER RESET (hard={})", hard);

        if hard {
            let new_id = crate::state::generate_node_id();
            let mut myself = self.state.myself.write().unwrap();
            myself.id = new_id;
            myself.epoch = 0;
            myself.flags = crate::state::NodeFlags::default();
            myself.flags.myself = true;
            myself.flags.master = true;
            myself.slots.clear();
            drop(myself);
            self.state.nodes.clear();
            let myself_read = self.state.myself.read().unwrap();
            self.state
                .nodes
                .insert(myself_read.id.clone(), myself_read.clone());
        } else {
            self.state.set_self_epoch(0);
        }

        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_keyslot(&self, args: &[Bytes]) -> String {
        if args.is_empty() {
            return "-ERR wrong number of arguments for 'CLUSTER KEYSLOT'\r\n".to_string();
        }
        let key = &args[0];
        let slot = key_hash_slot(key);
        format!(":{}\r\n", slot)
    }

    fn cluster_getkeysinslot(&self, args: &[Bytes]) -> String {
        if args.len() < 2 {
            return "-ERR wrong number of arguments for 'CLUSTER GETKEYSINSLOT'\r\n".to_string();
        }
        let _slot: u16 = match String::from_utf8_lossy(&args[0]).parse() {
            Ok(s) => s,
            Err(_) => return "-ERR invalid slot\r\n".to_string(),
        };
        let _count: usize = match String::from_utf8_lossy(&args[1]).parse() {
            Ok(c) => c,
            Err(_) => return "-ERR invalid count\r\n".to_string(),
        };
        // In a real implementation, scan the keyspace for keys in this slot
        "*0\r\n".to_string()
    }

    fn cluster_countkeysinslot(&self, args: &[Bytes]) -> String {
        if args.is_empty() {
            return "-ERR wrong number of arguments for 'CLUSTER COUNTKEYSINSLOT'\r\n".to_string();
        }
        let _slot: u16 = match String::from_utf8_lossy(&args[0]).parse() {
            Ok(s) => s,
            Err(_) => return "-ERR invalid slot\r\n".to_string(),
        };
        ":0\r\n".to_string()
    }

    fn cluster_addslots(&self, args: &[Bytes]) -> String {
        if args.is_empty() {
            return "-ERR wrong number of arguments for 'CLUSTER ADDSLOTS'\r\n".to_string();
        }

        let myself = self.state.myself.read().unwrap();
        let my_id = myself.id.clone();
        drop(myself);

        for arg in args {
            let slot: u16 = match String::from_utf8_lossy(arg).parse() {
                Ok(s) => s,
                Err(_) => {
                    return format!("-ERR Invalid slot: {}\r\n", String::from_utf8_lossy(arg));
                }
            };
            let mut slots = self.state.slots.write().unwrap();
            if (slot as usize) < crate::state::NUM_SLOTS {
                slots[slot as usize] = Some(my_id.clone());
            }
        }

        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_delslots(&self, args: &[Bytes]) -> String {
        if args.is_empty() {
            return "-ERR wrong number of arguments for 'CLUSTER DELSLOTS'\r\n".to_string();
        }

        for arg in args {
            let slot: u16 = match String::from_utf8_lossy(arg).parse() {
                Ok(s) => s,
                Err(_) => {
                    return format!("-ERR Invalid slot: {}\r\n", String::from_utf8_lossy(arg));
                }
            };
            let mut slots = self.state.slots.write().unwrap();
            if (slot as usize) < crate::state::NUM_SLOTS {
                slots[slot as usize] = None;
            }
        }

        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_setslot(&self, args: &[Bytes]) -> String {
        if args.len() < 2 {
            return "-ERR wrong number of arguments for 'CLUSTER SETSLOT'\r\n".to_string();
        }

        let slot: u16 = match String::from_utf8_lossy(&args[0]).parse() {
            Ok(s) => s,
            Err(_) => return "-ERR invalid slot\r\n".to_string(),
        };

        let subcmd = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();

        match subcmd.as_str() {
            "IMPORTING" => {
                if args.len() < 3 {
                    return "-ERR CLUSTER SETSLOT IMPORTING requires node_id\r\n".to_string();
                }
                let source_id = String::from_utf8_lossy(&args[2]).to_string();
                let mut importing = self.state.importing.write().unwrap();
                importing.insert(slot, source_id);
            }
            "MIGRATING" => {
                if args.len() < 3 {
                    return "-ERR CLUSTER SETSLOT MIGRATING requires node_id\r\n".to_string();
                }
                let target_id = String::from_utf8_lossy(&args[2]).to_string();
                let mut migrating = self.state.migrating.write().unwrap();
                migrating.insert(slot, target_id);
            }
            "STABLE" => {
                let mut importing = self.state.importing.write().unwrap();
                importing.remove(&slot);
                let mut migrating = self.state.migrating.write().unwrap();
                migrating.remove(&slot);
            }
            "NODE" => {
                if args.len() < 3 {
                    return "-ERR CLUSTER SETSLOT NODE requires node_id\r\n".to_string();
                }
                let node_id = String::from_utf8_lossy(&args[2]).to_string();
                let mut slots = self.state.slots.write().unwrap();
                if (slot as usize) < crate::state::NUM_SLOTS {
                    slots[slot as usize] = Some(node_id);
                }
                let mut importing = self.state.importing.write().unwrap();
                importing.remove(&slot);
                let mut migrating = self.state.migrating.write().unwrap();
                migrating.remove(&slot);
            }
            _ => {
                return format!("-ERR Unknown SETSLOT subcommand '{}'\r\n", subcmd);
            }
        }

        self.save_config();
        "+OK\r\n".to_string()
    }

    fn cluster_myid(&self) -> String {
        let myself = self.state.myself.read().unwrap();
        let id = &myself.id;
        format!("${}\r\n{}\r\n", id.len(), id)
    }

    fn cluster_slots(&self) -> String {
        let myself = self.state.myself.read().unwrap();
        let my_id = myself.id.clone();
        let my_addr = myself.addr.clone();
        drop(myself);

        let parts: Vec<&str> = my_addr.rsplitn(2, ':').collect();
        if parts.len() == 2 {
            let port = parts[0];
            let host = parts[1];
            format!(
                "*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n${}\r\n{}\r\n:{}\r\n${}\r\n{}\r\n",
                host.len(),
                host,
                port,
                my_id.len(),
                my_id
            )
        } else {
            String::new()
        }
    }

    fn cluster_saveconfig(&self) -> String {
        self.save_config();
        "+OK\r\n".to_string()
    }

    fn save_config(&self) {
        let mut content = String::new();
        for entry in self.state.nodes.iter() {
            let node = entry.value();
            let flags = node.flags.to_str();
            let role = match node.role {
                NodeRole::Master => "master",
                NodeRole::Slave => "slave",
            };
            let slots_str = node
                .slots
                .iter()
                .map(|r| {
                    if r.start == r.end {
                        format!("{}", r.start)
                    } else {
                        format!("{}-{}", r.start, r.end)
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");

            content.push_str(&format!(
                "{} {} {} {} {} {} {} {}\n",
                node.id, node.addr, flags, role, node.epoch, 0, 0, slots_str
            ));
        }

        if let Err(e) = fs::write(&self.nodes_conf_path, content) {
            warn!("Failed to save nodes.conf: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{NodeInfo, NodeRole};

    fn test_state() -> Arc<ClusterState> {
        let mut node = NodeInfo::new(
            crate::state::generate_node_id(),
            "127.0.0.1:6379".into(),
            NodeRole::Master,
        );
        node.flags.myself = true;
        node.flags.master = true;
        ClusterState::new(node)
    }

    #[test]
    fn route_local() {
        let state = test_state();
        let router = ClusterRouter::new(state);
        match router.route(b"foo") {
            RouteAction::Local => {}
            _ => panic!("Expected Local"),
        }
    }

    #[test]
    fn route_asking_bypasses_check() {
        let state = test_state();
        let router = ClusterRouter::new(state);
        router.set_asking(true);
        match router.route(b"foo") {
            RouteAction::Local => {}
            _ => panic!("Expected Local with ASK flag"),
        }
        assert!(!router.is_asking());
    }

    #[test]
    fn cluster_keyslot() {
        let state = test_state();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let args = vec![Bytes::from("foo")];
        let resp = handler.handle("KEYSLOT", &args);
        assert!(resp.starts_with(':'));
    }

    #[test]
    fn cluster_myid() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let resp = handler.handle("MYID", &[]);
        let myself = state2.myself.read().unwrap();
        assert!(resp.contains(&myself.id));
    }

    #[test]
    fn cluster_meet() {
        let state = test_state();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let args = vec![Bytes::from("127.0.0.1"), Bytes::from("6380")];
        let resp = handler.handle("MEET", &args);
        assert_eq!(resp, "+OK\r\n");
    }

    #[test]
    fn cluster_info() {
        let state = test_state();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let resp = handler.handle("INFO", &[]);
        assert!(resp.contains("cluster_state:"));
        assert!(resp.contains("cluster_known_nodes:1"));
    }

    #[test]
    fn cluster_nodes() {
        let state = test_state();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let resp = handler.handle("NODES", &[]);
        assert!(resp.contains("myself"));
    }

    #[test]
    fn cluster_replicate() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        // First add a target node
        let target_id = crate::state::generate_node_id();
        state2.add_node(
            target_id.clone(),
            "127.0.0.1:6380".into(),
            NodeRole::Master,
            crate::state::NodeFlags::default(),
        );
        let args = vec![Bytes::from(target_id.clone())];
        let resp = handler.handle("REPLICATE", &args);
        assert_eq!(resp, "+OK\r\n");
        let myself = state2.myself.read().unwrap();
        assert_eq!(myself.role, NodeRole::Slave);
    }

    #[test]
    fn cluster_addslots() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let args = vec![Bytes::from("0"), Bytes::from("1"), Bytes::from("2")];
        let resp = handler.handle("ADDSLOTS", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(state2.slot_owner(0).is_some());
        assert!(state2.slot_owner(1).is_some());
        assert!(state2.slot_owner(2).is_some());
    }

    #[test]
    fn cluster_delslots() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        // First add slots
        let add_args = vec![Bytes::from("0"), Bytes::from("1")];
        handler.handle("ADDSLOTS", &add_args);
        // Then delete
        let del_args = vec![Bytes::from("0")];
        let resp = handler.handle("DELSLOTS", &del_args);
        assert_eq!(resp, "+OK\r\n");
        assert!(state2.slot_owner(0).is_none());
        assert!(state2.slot_owner(1).is_some());
    }

    #[test]
    fn cluster_setslot_importing() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let args = vec![
            Bytes::from("0"),
            Bytes::from("IMPORTING"),
            Bytes::from("abc123"),
        ];
        let resp = handler.handle("SETSLOT", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(state2.is_importing(0).is_some());
    }

    #[test]
    fn cluster_setslot_migrating() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        let args = vec![
            Bytes::from("0"),
            Bytes::from("MIGRATING"),
            Bytes::from("abc123"),
        ];
        let resp = handler.handle("SETSLOT", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(state2.is_migrating(0).is_some());
    }

    #[test]
    fn cluster_setslot_stable() {
        let state = test_state();
        let state2 = state.clone();
        let handler = ClusterCommandHandler::new(state, "/tmp/test_nodes.conf".into());
        // First set importing
        let args = vec![
            Bytes::from("0"),
            Bytes::from("IMPORTING"),
            Bytes::from("abc123"),
        ];
        handler.handle("SETSLOT", &args);
        // Then set stable
        let args = vec![Bytes::from("0"), Bytes::from("STABLE")];
        let resp = handler.handle("SETSLOT", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(state2.is_importing(0).is_none());
    }
}
