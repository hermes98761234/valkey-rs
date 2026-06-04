pub mod commands;
pub mod gossip;
pub mod slots;
pub mod state;

pub use commands::{ClusterCommandHandler, ClusterRouter, RouteAction};
pub use gossip::{GossipConfig, GossipManager, GossipMessage, GossipNodeEntry};
pub use slots::key_hash_slot;
pub use state::{
    generate_node_id, ClusterState, NodeFlags, NodeInfo, NodeRole, SlotRange, NUM_SLOTS,
};

use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Configuration for cluster mode.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Whether cluster mode is enabled.
    pub enabled: bool,
    /// Path to the nodes.conf file for node ID persistence.
    pub config_file: String,
    /// Port for cluster bus (0 = use client_port + 10000).
    pub bus_port: u16,
    /// Whether to announce the cluster bus address.
    pub announce_bus_addr: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            config_file: "nodes.conf".into(),
            bus_port: 0,
            announce_bus_addr: false,
        }
    }
}

/// Initialize cluster mode.
/// Loads or generates a node ID, reads nodes.conf if present,
/// and returns a ClusterState handle.
pub fn init_cluster(config: &ClusterConfig, bind_addr: &str) -> Option<Arc<ClusterState>> {
    if !config.enabled {
        return None;
    }

    info!(
        "Initializing cluster mode (config_file={})",
        config.config_file
    );

    // Load or generate node ID
    let node_id = load_or_generate_node_id(&config.config_file);

    info!("Cluster node ID: {}", node_id);

    // Create node info
    let mut myself = NodeInfo::new(node_id, bind_addr.into(), NodeRole::Master);
    myself.flags.myself = true;
    myself.flags.master = true;

    // Create cluster state
    let state = ClusterState::new(myself);

    // Load existing cluster topology from nodes.conf
    load_nodes_conf(&state, &config.config_file);

    Some(state)
}

/// Load node ID from nodes.conf, or generate a new one.
fn load_or_generate_node_id(config_file: &str) -> String {
    if let Ok(content) = std::fs::read_to_string(config_file) {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if !parts.is_empty() && parts[0].len() == 40 {
                // First field is the node ID
                return parts[0].to_string();
            }
        }
    }
    generate_node_id()
}

/// Load cluster topology from nodes.conf.
fn load_nodes_conf(state: &Arc<ClusterState>, config_file: &str) {
    if !Path::new(config_file).exists() {
        return;
    }

    if let Ok(content) = std::fs::read_to_string(config_file) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 8 {
                continue;
            }

            let id = parts[0].to_string();
            let addr = parts[1].to_string();
            let role = if parts[3] == "slave" {
                NodeRole::Slave
            } else {
                NodeRole::Master
            };

            let mut info = NodeInfo::new(id.clone(), addr, role);
            info.epoch = parts[4].parse().unwrap_or(0);

            // Parse flags
            for flag in parts[2].split(',') {
                match flag {
                    "myself" => info.flags.myself = true,
                    "master" => info.flags.master = true,
                    "slave" => info.flags.slave = true,
                    "pfail" => info.flags.pfail = true,
                    "fail" => info.flags.fail = true,
                    "noaddr" => info.flags.noaddr = true,
                    "handshake" => info.flags.handshake = true,
                    _ => {}
                }
            }

            // Parse slot ranges (from field 7 onwards)
            for slot_field in &parts[7..] {
                if slot_field.contains('-') {
                    let range_parts: Vec<&str> = slot_field.split('-').collect();
                    if range_parts.len() == 2 {
                        if let (Ok(start), Ok(end)) =
                            (range_parts[0].parse::<u16>(), range_parts[1].parse::<u16>())
                        {
                            info.slots.push(SlotRange::new(start, end));
                        }
                    }
                } else if let Ok(slot) = slot_field.parse::<u16>() {
                    info.slots.push(SlotRange::new(slot, slot));
                }
            }

            state.nodes.insert(id, info);
        }
    }

    info!("Loaded {} nodes from {}", state.nodes.len(), config_file);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_cluster_disabled() {
        let config = ClusterConfig {
            enabled: false,
            ..Default::default()
        };
        let state = init_cluster(&config, "127.0.0.1:6379");
        assert!(state.is_none());
    }

    #[test]
    fn init_cluster_enabled() {
        let config = ClusterConfig {
            enabled: true,
            config_file: "/tmp/test_cluster_init.conf".into(),
            ..Default::default()
        };
        let state = init_cluster(&config, "127.0.0.1:6379");
        assert!(state.is_some());
        let state = state.unwrap();
        let myself = state.myself.read().unwrap();
        assert!(myself.flags.myself);
        assert!(myself.flags.master);
        assert_eq!(myself.role, NodeRole::Master);
    }

    #[test]
    fn load_or_generate_node_id_new() {
        let id = load_or_generate_node_id("/tmp/nonexistent_nodes.conf");
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
