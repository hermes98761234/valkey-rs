use bytes::Bytes;
use valkey_proto::RespValue;

/// Handle CLUSTER subcommands.
/// Since full cluster mode requires a ClusterState handle (initialized at startup),
/// these stubs handle the case where cluster mode is not enabled.
/// When cluster mode IS enabled, the server routes CLUSTER commands through
/// the cluster crate's ClusterCommandHandler instead.
pub async fn handle(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'cluster' command".into());
    }
    let subcmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    let sub_args = &args[1..];
    match subcmd.as_str() {
        "INFO" => cluster_info(),
        "NODES" => cluster_nodes(),
        "MEET" => cluster_meet(sub_args),
        "RESET" => cluster_reset(sub_args),
        "KEYSLOT" => cluster_keyslot(sub_args),
        "MYID" => cluster_myid(),
        "SLOTS" => cluster_slots(),
        "SAVECONFIG" => cluster_saveconfig(),
        "GETKEYSINSLOT" => cluster_getkeysinslot(sub_args),
        "COUNTKEYSINSLOT" => cluster_countkeysinslot(sub_args),
        "ADDSLOTS" => cluster_addslots(sub_args),
        "DELSLOTS" => cluster_delslots(sub_args),
        "SETSLOT" => cluster_setslot(sub_args),
        "REPLICATE" => cluster_replicate(sub_args),
        "FAILOVER" => cluster_failover(sub_args),
        "BUMPEPOCH" => cluster_bumpepoch(),
        _ => RespValue::Error(format!("ERR unknown CLUSTER subcommand '{}'", subcmd).into()),
    }
}

fn cluster_info() -> RespValue {
    let info = "cluster_state:fail\r\n\
                cluster_slots_assigned:0\r\n\
                cluster_slots_ok:0\r\n\
                cluster_slots_pfail:0\r\n\
                cluster_slots_fail:0\r\n\
                cluster_known_nodes:0\r\n\
                cluster_size:0\r\n\
                cluster_current_epoch:0\r\n\
                cluster_my_epoch:0\r\n\
                cluster_stats_messages_ping_sent:0\r\n\
                cluster_stats_messages_pong_sent:0\r\n\
                cluster_stats_messages_sent:0\r\n\
                cluster_stats_messages_ping_received:0\r\n\
                cluster_stats_messages_pong_received:0\r\n\
                cluster_stats_messages_meet_received:0\r\n\
                cluster_stats_messages_received:0\r\n";
    RespValue::BulkString(Some(Bytes::from(info)))
}

fn cluster_nodes() -> RespValue {
    RespValue::BulkString(None)
}

fn cluster_meet(args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'CLUSTER MEET' command".into(),
        );
    }
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_reset(args: &[Bytes]) -> RespValue {
    let _hard = args
        .first()
        .map(|a| {
            String::from_utf8_lossy(a).to_ascii_uppercase() == "HARD"
        })
        .unwrap_or(false);
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_keyslot(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'CLUSTER KEYSLOT' command".into(),
        );
    }
    // CRC16 mod 16384 — compute the hash slot for the key
    let key = &args[0];
    let slot = crc16_xmodem(key) % 16384;
    RespValue::Integer(slot as i64)
}

fn cluster_myid() -> RespValue {
    // Return empty/null bulk string since there's no node ID
    RespValue::BulkString(None)
}

fn cluster_slots() -> RespValue {
    RespValue::Array(Some(vec![]))
}

fn cluster_saveconfig() -> RespValue {
    RespValue::SimpleString("OK".into())
}

fn cluster_getkeysinslot(args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'CLUSTER GETKEYSINSLOT' command".into(),
        );
    }
    RespValue::Array(Some(vec![]))
}

fn cluster_countkeysinslot(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'CLUSTER COUNTKEYSINSLOT' command".into(),
        );
    }
    RespValue::Integer(0)
}

fn cluster_addslots(_args: &[Bytes]) -> RespValue {
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_delslots(_args: &[Bytes]) -> RespValue {
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_setslot(args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'CLUSTER SETSLOT' command".into(),
        );
    }
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_replicate(_args: &[Bytes]) -> RespValue {
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_failover(_args: &[Bytes]) -> RespValue {
    RespValue::Error(
        "ERR This instance has cluster support disabled".into(),
    )
}

fn cluster_bumpepoch() -> RespValue {
    // In stub mode (cluster disabled), epoch is always 0
    // When cluster mode is enabled, the ClusterCommandHandler handles this
    RespValue::Integer(0)
}

/// CRC16-XMODEM for computing Redis cluster hash slots.
fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_info() {
        let r = cluster_info();
        match r {
            RespValue::BulkString(Some(s)) => {
                let text = String::from_utf8_lossy(&s);
                assert!(text.contains("cluster_state:fail"));
                assert!(text.contains("cluster_slots_assigned:0"));
                assert!(text.contains("cluster_known_nodes:0"));
            }
            _ => panic!("expected bulk string"),
        }
    }

    #[tokio::test]
    async fn test_cluster_nodes() {
        let r = cluster_nodes();
        assert_eq!(r, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_cluster_keyslot() {
        let r = cluster_keyslot(&[Bytes::from("foo")]);
        match r {
            RespValue::Integer(_) => {}
            _ => panic!("expected integer"),
        }
    }

    #[tokio::test]
    async fn test_cluster_keyslot_no_args() {
        let r = cluster_keyslot(&[Bytes::new(); 0]); // empty
        match r {
            RespValue::Error(_) => {}
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_myid() {
        let r = cluster_myid();
        assert_eq!(r, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_cluster_meet() {
        let r = cluster_meet(&[Bytes::from("127.0.0.1"), Bytes::from("6380")]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_meet_no_args() {
        let r = cluster_meet(&[Bytes::new(); 0]);
        match r {
            RespValue::Error(_) => {}
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_reset() {
        let r = cluster_reset(&[]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_slots() {
        let r = cluster_slots();
        match r {
            RespValue::Array(Some(arr)) => assert!(arr.is_empty()),
            _ => panic!("expected empty array"),
        }
    }

    #[tokio::test]
    async fn test_cluster_saveconfig() {
        assert_eq!(cluster_saveconfig(), RespValue::SimpleString("OK".into()));
    }

    #[tokio::test]
    async fn test_cluster_unknown_subcommand() {
        let r = handle(&[Bytes::from("BOGUS")]).await;
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("unknown CLUSTER subcommand"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_addslots() {
        let r = cluster_addslots(&[Bytes::from("0")]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_delslots() {
        let r = cluster_delslots(&[Bytes::from("0")]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_setslot() {
        let r = cluster_setslot(&[Bytes::from("0"), Bytes::from("STABLE")]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_replicate() {
        let r = cluster_replicate(&[Bytes::from("abc123")]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_failover() {
        let r = cluster_failover(&[]);
        match r {
            RespValue::Error(s) => {
                assert!(s.contains("cluster support disabled"));
            }
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_no_args() {
        let r = handle(&[]).await;
        match r {
            RespValue::Error(_) => {}
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_cluster_bumpepoch() {
        let r = cluster_bumpepoch();
        // In stub mode (cluster disabled), epoch is 0
        assert_eq!(r, RespValue::Integer(0));
    }

    // Verify that CRC16-based keyslot returns deterministic values
    #[tokio::test]
    async fn test_crc16_known_values() {
        // CRC16 should be deterministic
        let slot_foo = crc16_xmodem(b"foo") % 16384;
        let slot_foo2 = crc16_xmodem(b"foo") % 16384;
        assert_eq!(slot_foo, slot_foo2);

        // Different keys should (almost certainly) give different slots
        let slot_bar = crc16_xmodem(b"bar") % 16384;
        assert_ne!(slot_foo, slot_bar);
    }
}
