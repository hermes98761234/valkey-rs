use crate::state::{ClusterState, NodeInfo, NodeFlags};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

/// Configuration for the gossip protocol.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// How often to send gossip messages.
    pub interval: Duration,
    /// Number of nodes to gossip to each interval.
    pub fanout: usize,
    /// Port for cluster bus communication.
    pub bus_port: u16,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(100),
            fanout: 3,
            bus_port: 16379,
        }
    }
}

/// A gossip message containing a subset of the sender's node table.
#[derive(Debug, Clone)]
pub struct GossipMessage {
    pub sender_id: String,
    pub sender_addr: String,
    pub nodes: Vec<GossipNodeEntry>,
    pub current_epoch: u64,
}

/// A single node entry in a gossip message.
#[derive(Debug, Clone)]
pub struct GossipNodeEntry {
    pub id: String,
    pub addr: String,
    pub flags: NodeFlags,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub epoch: u64,
}

impl GossipNodeEntry {
    pub fn from_node_info(info: &NodeInfo) -> Self {
        Self {
            id: info.id.clone(),
            addr: info.addr.clone(),
            flags: info.flags,
            ping_sent: 0,
            pong_recv: 0,
            epoch: info.epoch,
        }
    }
}

/// The gossip protocol manager.
pub struct GossipManager {
    state: Arc<ClusterState>,
    config: GossipConfig,
}

impl GossipManager {
    pub fn new(state: Arc<ClusterState>, config: GossipConfig) -> Self {
        Self { state, config }
    }

    /// Start the gossip protocol loop.
    pub fn spawn(self) {
        let state = self.state.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.interval);
            loop {
                interval.tick().await;
                Self::gossip_tick(&state, &config).await;
            }
        });
    }

    async fn gossip_tick(state: &Arc<ClusterState>, config: &GossipConfig) {
        let known_nodes: Vec<NodeInfo> = state
            .nodes
            .iter()
            .filter(|n| {
                let myself = state.myself.read().unwrap();
                n.id != myself.id
            })
            .map(|n| n.value().clone())
            .collect();

        if known_nodes.is_empty() {
            return;
        }

        let fanout = config.fanout.min(known_nodes.len());
        let selected: Vec<_> = known_nodes.into_iter().take(fanout).collect();

        for node in &selected {
            debug!("Gossiping to node {} at {}", node.id, node.addr);
            // In a real implementation, send PING/MEET over cluster bus TCP here
        }
    }

    /// Handle an incoming gossip message and merge node info.
    pub fn handle_gossip(state: &Arc<ClusterState>, msg: GossipMessage) {
        debug!(
            "Received gossip from {} (epoch {})",
            msg.sender_id, msg.current_epoch
        );

        // Merge each node entry
        for entry in &msg.nodes {
            let myself = state.myself.read().unwrap();
            if entry.id == myself.id {
                continue;
            }
            drop(myself);

            let role = if entry.flags.slave {
                crate::state::NodeRole::Slave
            } else {
                crate::state::NodeRole::Master
            };
            let mut new_info = NodeInfo::new(entry.id.clone(), entry.addr.clone(), role);
            new_info.flags = entry.flags;
            new_info.epoch = entry.epoch;

            state.merge_node(new_info);
        }

        // Also add/update the sender
        let myself = state.myself.read().unwrap();
        if msg.sender_id != myself.id {
            drop(myself);
            let mut sender_info = NodeInfo::new(
                msg.sender_id.clone(),
                msg.sender_addr.clone(),
                crate::state::NodeRole::Master,
            );
            sender_info.epoch = msg.current_epoch;
            state.merge_node(sender_info);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{generate_node_id, NodeRole};

    #[test]
    fn gossip_merge_node() {
        let node = NodeInfo::new(
            generate_node_id(),
            "127.0.0.1:6379".into(),
            NodeRole::Master,
        );
        let state = ClusterState::new(node);

        let remote_id = generate_node_id();
        let msg = GossipMessage {
            sender_id: remote_id.clone(),
            sender_addr: "127.0.0.1:6380".into(),
            nodes: vec![],
            current_epoch: 1,
        };

        GossipManager::handle_gossip(&state, msg);
        assert!(state.nodes.contains_key(&remote_id));
    }

    #[test]
    fn gossip_skips_self() {
        let node = NodeInfo::new(
            generate_node_id(),
            "127.0.0.1:6379".into(),
            NodeRole::Master,
        );
        let state = ClusterState::new(node);

        let myself = state.myself.read().unwrap();
        let self_id = myself.id.clone();
        drop(myself);

        let msg = GossipMessage {
            sender_id: self_id.clone(),
            sender_addr: "127.0.0.1:6379".into(),
            nodes: vec![],
            current_epoch: 0,
        };

        GossipManager::handle_gossip(&state, msg);
        assert_eq!(state.nodes.len(), 1);
    }
}
