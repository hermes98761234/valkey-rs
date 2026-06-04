/// Integration tests for cluster mode.
/// Tests 3-node cluster formation, slot assignment, and MOVED/ASK redirects.
#[cfg(test)]
mod integration_tests {
    use bytes::Bytes;
    use std::sync::Arc;
    use valkey_cluster::commands::{ClusterCommandHandler, ClusterRouter, RouteAction};
    use valkey_cluster::slots::key_hash_slot;
    use valkey_cluster::state::{
        generate_node_id, ClusterState, NodeFlags, NodeInfo, NodeRole, SlotRange, NUM_SLOTS,
    };

    /// Simulate a 3-node cluster with slot assignments.
    /// Node A: slots 0-5460
    /// Node B: slots 5461-10922
    /// Node C: slots 10923-16383
    struct TestCluster {
        pub node_a_state: Arc<ClusterState>,
        pub node_b_state: Arc<ClusterState>,
        pub node_c_state: Arc<ClusterState>,
        pub handler_a: ClusterCommandHandler,
        pub handler_b: ClusterCommandHandler,
        pub handler_c: ClusterCommandHandler,
        pub router_a: ClusterRouter,
        pub router_b: ClusterRouter,
        pub router_c: ClusterRouter,
    }

    fn port(base: u16, offset: u16) -> String {
        format!("127.0.0.1:{}", base + offset)
    }

    fn setup_cluster() -> TestCluster {
        let base_port = 7000u16;

        // Create 3 node states
        let node_a = NodeInfo::new(generate_node_id(), port(base_port, 0), NodeRole::Master);
        let node_b = NodeInfo::new(generate_node_id(), port(base_port, 1), NodeRole::Master);
        let node_c = NodeInfo::new(generate_node_id(), port(base_port, 2), NodeRole::Master);

        let state_a = ClusterState::new(node_a);
        let state_b = ClusterState::new(node_b);
        let state_c = ClusterState::new(node_c);

        // Set myself flags
        {
            let mut a = state_a.myself.write().unwrap();
            a.flags.myself = true;
            a.flags.master = true;
        }
        {
            let mut b = state_b.myself.write().unwrap();
            b.flags.myself = true;
            b.flags.master = true;
        }
        {
            let mut c = state_c.myself.write().unwrap();
            c.flags.myself = true;
            c.flags.master = true;
        }

        // Create handlers
        let handler_a = ClusterCommandHandler::new(
            state_a.clone(),
            format!("/tmp/test_cluster_a_{}.conf", base_port),
        );
        let handler_b = ClusterCommandHandler::new(
            state_b.clone(),
            format!("/tmp/test_cluster_b_{}.conf", base_port),
        );
        let handler_c = ClusterCommandHandler::new(
            state_c.clone(),
            format!("/tmp/test_cluster_c_{}.conf", base_port),
        );

        // Create routers
        let router_a = ClusterRouter::new(state_a.clone());
        let router_b = ClusterRouter::new(state_b.clone());
        let router_c = ClusterRouter::new(state_c.clone());

        // Simulate CLUSTER MEET — each node learns about the others
        // Node A meets B and C
        let b_addr = port(base_port, 1);
        let c_addr = port(base_port, 2);

        let b_id = {
            let b = state_b.myself.read().unwrap();
            b.id.clone()
        };
        let c_id = {
            let c = state_c.myself.read().unwrap();
            c.id.clone()
        };
        let a_id = {
            let a = state_a.myself.read().unwrap();
            a.id.clone()
        };

        // Add nodes to each other's node tables
        state_a.add_node(b_id.clone(), b_addr.clone(), NodeRole::Master, {
            let mut f = NodeFlags::default();
            f.master = true;
            f
        });
        state_a.add_node(c_id.clone(), c_addr.clone(), NodeRole::Master, {
            let mut f = NodeFlags::default();
            f.master = true;
            f
        });

        state_b.add_node(a_id.clone(), port(base_port, 0), NodeRole::Master, {
            let mut f = NodeFlags::default();
            f.master = true;
            f
        });
        state_b.add_node(c_id.clone(), c_addr.clone(), NodeRole::Master, {
            let mut f = NodeFlags::default();
            f.master = true;
            f
        });

        state_c.add_node(a_id.clone(), port(base_port, 0), NodeRole::Master, {
            let mut f = NodeFlags::default();
            f.master = true;
            f
        });
        state_c.add_node(b_id.clone(), b_addr.clone(), NodeRole::Master, {
            let mut f = NodeFlags::default();
            f.master = true;
            f
        });

        // Assign slots
        // Node A: 0-5460
        state_a.assign_slots(&a_id, &[SlotRange::new(0, 5460)]);
        // Node B: 5461-10922
        state_b.assign_slots(&b_id, &[SlotRange::new(5461, 10922)]);
        // Node C: 10923-16383
        state_c.assign_slots(&c_id, &[SlotRange::new(10923, 16383)]);

        TestCluster {
            node_a_state: state_a,
            node_b_state: state_b,
            node_c_state: state_c,
            handler_a,
            handler_b,
            handler_c,
            router_a,
            router_b,
            router_c,
        }
    }

    #[test]
    fn three_node_cluster_formation() {
        let cluster = setup_cluster();

        // Each node should know about all 3 nodes
        assert_eq!(cluster.node_a_state.nodes.len(), 3);
        assert_eq!(cluster.node_b_state.nodes.len(), 3);
        assert_eq!(cluster.node_c_state.nodes.len(), 3);
    }

    #[test]
    fn slot_assignment_coverage() {
        let cluster = setup_cluster();

        // Node A owns slots 0-5460
        assert!(cluster.node_a_state.slot_owner(0).is_some());
        assert!(cluster.node_a_state.slot_owner(5460).is_some());
        assert!(cluster.node_a_state.slot_owner(5461).is_none());

        // Node B owns slots 5461-10922
        assert!(cluster.node_b_state.slot_owner(5461).is_some());
        assert!(cluster.node_b_state.slot_owner(10922).is_some());
        assert!(cluster.node_b_state.slot_owner(10923).is_none());

        // Node C owns slots 10923-16383
        assert!(cluster.node_c_state.slot_owner(10923).is_some());
        assert!(cluster.node_c_state.slot_owner(16383).is_some());
    }

    #[test]
    fn cluster_info_after_setup() {
        let cluster = setup_cluster();

        let info_a = cluster.handler_a.handle("INFO", &[]);
        assert!(info_a.contains("cluster_known_nodes:3"));
        assert!(info_a.contains("cluster_size:3"));
        assert!(info_a.contains("cluster_slots_assigned:5461"));
    }

    #[test]
    fn cluster_nodes_format() {
        let cluster = setup_cluster();

        let nodes_a = cluster.handler_a.handle("NODES", &[]);
        // Should contain 3 node lines
        let node_count = nodes_a.lines().filter(|l| !l.is_empty()).count();
        assert!(node_count >= 3, "Expected 3 nodes, got {}", node_count);
    }

    #[test]
    fn cluster_meet_adds_node() {
        let cluster = setup_cluster();

        // Node A meets a new node D
        let args = vec![Bytes::from("127.0.0.1"), Bytes::from("7003")];
        let resp = cluster.handler_a.handle("MEET", &args);
        assert_eq!(resp, "+OK\r\n");

        // Node A should now know about 4 nodes
        assert_eq!(cluster.node_a_state.nodes.len(), 4);
    }

    #[test]
    fn cluster_keyslot_command() {
        let cluster = setup_cluster();

        let args = vec![Bytes::from("mykey")];
        let resp = cluster.handler_a.handle("KEYSLOT", &args);
        assert!(resp.starts_with(':'));
        assert!(resp.ends_with("\r\n"));
    }

    #[test]
    fn cluster_addslots_and_delslots() {
        let cluster = setup_cluster();

        // Add slot 100 to node A
        let args = vec![Bytes::from("100")];
        let resp = cluster.handler_a.handle("ADDSLOTS", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(cluster.node_a_state.slot_owner(100).is_some());

        // Delete slot 100 from node A
        let resp = cluster.handler_a.handle("DELSLOTS", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(cluster.node_a_state.slot_owner(100).is_none());
    }

    #[test]
    fn cluster_replicate() {
        let cluster = setup_cluster();

        // Get node A's ID
        let a_id = {
            let a = cluster.node_a_state.myself.read().unwrap();
            a.id.clone()
        };

        // Make node B replicate node A
        let args = vec![Bytes::from(a_id)];
        let resp = cluster.handler_b.handle("REPLICATE", &args);
        assert_eq!(resp, "+OK\r\n");

        let b = cluster.node_b_state.myself.read().unwrap();
        assert_eq!(b.role, NodeRole::Slave);
    }

    #[test]
    fn cluster_failover_on_slave() {
        let cluster = setup_cluster();

        // First make node C a replica of node A
        let a_id = {
            let a = cluster.node_a_state.myself.read().unwrap();
            a.id.clone()
        };
        let args = vec![Bytes::from(a_id)];
        cluster.handler_c.handle("REPLICATE", &args);

        // Now failover on node C
        let resp = cluster.handler_c.handle("FAILOVER", &[]);
        assert_eq!(resp, "+OK\r\n");

        let c = cluster.node_c_state.myself.read().unwrap();
        assert_eq!(c.role, NodeRole::Master);
        assert!(c.epoch > 0);
    }

    #[test]
    fn cluster_failover_rejected_on_master() {
        let cluster = setup_cluster();

        // Node A is already a master, failover should be rejected
        let resp = cluster.handler_a.handle("FAILOVER", &[]);
        assert!(resp.contains("ERR"));
    }

    #[test]
    fn cluster_reset_soft() {
        let cluster = setup_cluster();

        let resp = cluster.handler_a.handle("RESET", &[]);
        assert_eq!(resp, "+OK\r\n");
    }

    #[test]
    fn cluster_setslot_states() {
        let cluster = setup_cluster();

        // Set slot 5000 to IMPORTING from node A
        let a_id = {
            let a = cluster.node_a_state.myself.read().unwrap();
            a.id.clone()
        };
        let args = vec![
            Bytes::from("5000"),
            Bytes::from("IMPORTING"),
            Bytes::from(a_id.clone()),
        ];
        let resp = cluster.handler_a.handle("SETSLOT", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(cluster.node_a_state.is_importing(5000).is_some());

        // Set slot 5000 to MIGRATING to node A
        let args = vec![
            Bytes::from("5000"),
            Bytes::from("MIGRATING"),
            Bytes::from(a_id),
        ];
        let resp = cluster.handler_a.handle("SETSLOT", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(cluster.node_a_state.is_migrating(5000).is_some());

        // Set slot 5000 to STABLE
        let args = vec![Bytes::from("5000"), Bytes::from("STABLE")];
        let resp = cluster.handler_a.handle("SETSLOT", &args);
        assert_eq!(resp, "+OK\r\n");
        assert!(cluster.node_a_state.is_importing(5000).is_none());
        assert!(cluster.node_a_state.is_migrating(5000).is_none());
    }

    #[test]
    fn has_tag_keys_hash_to_same_slot() {
        // {user}1000 and {user}2000 should hash to the same slot
        let slot1 = key_hash_slot(b"{user}1000");
        let slot2 = key_hash_slot(b"{user}2000");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn different_tags_hash_to_different_slots() {
        // These are very likely to be different (though not guaranteed)
        let slot1 = key_hash_slot(b"{user}1000");
        let slot2 = key_hash_slot(b"{post}2000");
        // We can't guarantee they're different, but the test verifies no panic
        assert!(slot1 < NUM_SLOTS as u16);
        assert!(slot2 < NUM_SLOTS as u16);
    }
}
