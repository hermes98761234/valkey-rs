use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;

use crate::server::ClientCtx;
use valkey_pubsub::PubSubHub;

/// Context for pubsub commands, holding references to hub and client state.
/// Note: Store is not needed for most pubsub commands (only PUBLISH returns a count).
pub struct PubSubCtx {
    pub hub: Arc<PubSubHub>,
    pub client: Arc<std::sync::RwLock<ClientCtx>>,
}

impl PubSubCtx {
    pub fn new(hub: Arc<PubSubHub>) -> Self {
        Self {
            hub,
            client: Arc::new(std::sync::RwLock::new(ClientCtx::new())),
        }
    }
}

/// Handle SUBSCRIBE command.
pub fn cmd_subscribe(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'subscribe' command".into());
    }
    let mut results = Vec::new();
    for channel in args {
        results.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from("subscribe")),
            RespValue::bulk(channel.clone()),
            RespValue::int(1),
        ]));
    }
    if results.len() == 1 {
        results.into_iter().next().unwrap()
    } else {
        RespValue::Array(Some(results))
    }
}

/// Handle UNSUBSCRIBE command.
pub fn cmd_unsubscribe(args: &[Bytes], _ctx: &PubSubCtx) -> RespValue {
    let mut results = Vec::new();
    if args.is_empty() {
        results.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from("unsubscribe")),
            RespValue::null_bulk(),
            RespValue::int(0),
        ]));
    } else {
        for channel in args {
            results.push(RespValue::array(vec![
                RespValue::bulk(Bytes::from("unsubscribe")),
                RespValue::bulk(channel.clone()),
                RespValue::int(0),
            ]));
        }
    }
    if results.len() == 1 {
        results.into_iter().next().unwrap()
    } else {
        RespValue::Array(Some(results))
    }
}

/// Handle PSUBSCRIBE command.
pub fn cmd_psubscribe(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'psubscribe' command".into());
    }
    let mut results = Vec::new();
    for pattern in args {
        results.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from("psubscribe")),
            RespValue::bulk(pattern.clone()),
            RespValue::int(1),
        ]));
    }
    if results.len() == 1 {
        results.into_iter().next().unwrap()
    } else {
        RespValue::Array(Some(results))
    }
}

/// Handle PUNSUBSCRIBE command.
pub fn cmd_punsubscribe(args: &[Bytes], _ctx: &PubSubCtx) -> RespValue {
    let mut results = Vec::new();
    if args.is_empty() {
        results.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from("punsubscribe")),
            RespValue::null_bulk(),
            RespValue::int(0),
        ]));
    } else {
        for pattern in args {
            results.push(RespValue::array(vec![
                RespValue::bulk(Bytes::from("punsubscribe")),
                RespValue::bulk(pattern.clone()),
                RespValue::int(0),
            ]));
        }
    }
    if results.len() == 1 {
        results.into_iter().next().unwrap()
    } else {
        RespValue::Array(Some(results))
    }
}

/// Handle PUBLISH command.
pub fn cmd_publish(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'publish' command".into());
    }
    let channel = &args[0];
    let message = args[1].clone();
    let count = ctx.hub.publish(channel, message);
    RespValue::int(count)
}

/// Handle PUBSUB command with subcommands.
pub fn cmd_pubsub(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'pubsub' command".into());
    }
    let subcmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match subcmd.as_str() {
        "CHANNELS" => cmd_pubsub_channels(&args[1..], ctx),
        "NUMSUB" => cmd_pubsub_numsub(&args[1..], ctx),
        "NUMPAT" => cmd_pubsub_numpat(&args[1..], ctx),
        "SHARDCHANNELS" => cmd_pubsub_shardchannels(&args[1..], ctx),
        "SHARDNUMSUB" => cmd_pubsub_shardnumsub(&args[1..], ctx),
        "HELP" => cmd_pubsub_help(),
        _ => RespValue::Error(format!("ERR unknown PUBSUB subcommand '{}'", subcmd).into()),
    }
}

fn cmd_pubsub_channels(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    let pattern = if args.is_empty() {
        None
    } else {
        Some(args[0].clone())
    };
    let channels = ctx.hub.channels(pattern.as_ref());
    RespValue::array(channels.into_iter().map(|ch| RespValue::bulk(ch)).collect())
}

fn cmd_pubsub_numsub(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.is_empty() {
        return RespValue::array(vec![]);
    }
    let counts = ctx.hub.numsub(args);
    let mut result = Vec::new();
    for (channel, count) in counts {
        result.push(RespValue::bulk(channel));
        result.push(RespValue::int(count));
    }
    RespValue::array(result)
}

fn cmd_pubsub_numpat(_args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    RespValue::int(ctx.hub.numpat() as i64)
}

fn cmd_pubsub_shardchannels(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    let pattern = if args.is_empty() {
        None
    } else {
        Some(args[0].clone())
    };
    let channels = ctx.hub.shard_channels(pattern.as_ref());
    RespValue::array(channels.into_iter().map(|ch| RespValue::bulk(ch)).collect())
}

fn cmd_pubsub_shardnumsub(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.is_empty() {
        return RespValue::array(vec![]);
    }
    let counts = ctx.hub.shard_numsub(args);
    let mut result = Vec::new();
    for (channel, count) in counts {
        result.push(RespValue::bulk(channel));
        result.push(RespValue::int(count));
    }
    RespValue::array(result)
}

fn cmd_pubsub_help() -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(
            "PUBSUB <subcommand> [argument [argument ...]]>. Subcommands are:",
        )),
        RespValue::bulk(Bytes::from(
            "CHANNELS [pattern] -- Return the currently active channels.",
        )),
        RespValue::bulk(Bytes::from(
            "NUMSUB [channel ...] -- Returns the number of subscribers per channel.",
        )),
        RespValue::bulk(Bytes::from(
            "NUMPAT -- Returns the number of pattern subscriptions.",
        )),
        RespValue::bulk(Bytes::from("SHARDCHANNELS -- Stub for cluster mode.")),
        RespValue::bulk(Bytes::from("SHARDNUMSUB -- Stub for cluster mode.")),
    ])
}

/// Handle SPUBLISH (Shard Publish) command.
pub fn cmd_spublish(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'spublish' command".into());
    }
    let channel = &args[0];
    let message = args[1].clone();
    let count = ctx.hub.spublish(channel, message);
    RespValue::int(count)
}

/// Handle SSUBSCRIBE (Shard Subscribe) command.
pub fn cmd_ssubscribe(args: &[Bytes], ctx: &PubSubCtx) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'ssubscribe' command".into());
    }
    let mut results = Vec::new();
    for channel in args {
        results.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from("ssubscribe")),
            RespValue::bulk(channel.clone()),
            RespValue::int(1),
        ]));
    }
    if results.len() == 1 {
        results.into_iter().next().unwrap()
    } else {
        RespValue::Array(Some(results))
    }
}

/// Handle SUNSUBSCRIBE (Shard Unsubscribe) command.
pub fn cmd_sunsubscribe(args: &[Bytes], _ctx: &PubSubCtx) -> RespValue {
    let mut results = Vec::new();
    if args.is_empty() {
        results.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from("sunsubscribe")),
            RespValue::null_bulk(),
            RespValue::int(0),
        ]));
    } else {
        for channel in args {
            results.push(RespValue::array(vec![
                RespValue::bulk(Bytes::from("sunsubscribe")),
                RespValue::bulk(channel.clone()),
                RespValue::int(0),
            ]));
        }
    }
    if results.len() == 1 {
        results.into_iter().next().unwrap()
    } else {
        RespValue::Array(Some(results))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_hub() -> Arc<PubSubHub> {
        PubSubHub::new()
    }

    fn test_ctx(hub: Arc<PubSubHub>) -> PubSubCtx {
        PubSubCtx::new(hub)
    }

    #[tokio::test]
    async fn test_publish_no_subscribers() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_publish(&[Bytes::from("ch1"), Bytes::from("msg")], &ctx);
        assert_eq!(r, RespValue::int(0));
    }

    #[tokio::test]
    async fn test_publish_with_subscriber() {
        let hub = test_hub();
        let mut rx = hub.subscribe(Bytes::from("ch1"));
        let ctx = test_ctx(hub);
        let r = cmd_publish(&[Bytes::from("ch1"), Bytes::from("hello")], &ctx);
        assert_eq!(r, RespValue::int(1));
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_publish_with_pattern_subscriber() {
        let hub = test_hub();
        let mut rx = hub.psubscribe(Bytes::from("chan*"));
        let ctx = test_ctx(hub);
        let r = cmd_publish(&[Bytes::from("chan1"), Bytes::from("data")], &ctx);
        assert!(r == RespValue::int(1) || r == RespValue::int(2));
        let (ch, msg) = rx.try_recv().unwrap();
        assert_eq!(ch, Bytes::from("chan1"));
        assert_eq!(msg, Bytes::from("data"));
    }

    #[tokio::test]
    async fn test_subscribe_args() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_subscribe(&[Bytes::from("ch1")], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], RespValue::bulk(Bytes::from("subscribe")));
                assert_eq!(arr[1], RespValue::bulk(Bytes::from("ch1")));
            }
            _ => panic!("expected array response"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_no_args() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_subscribe(&[], &ctx);
        assert_eq!(
            r,
            RespValue::Error("ERR wrong number of arguments for 'subscribe' command".into())
        );
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_unsubscribe(&[Bytes::from("ch1")], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr[0], RespValue::bulk(Bytes::from("unsubscribe")));
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_psubscribe() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_psubscribe(&[Bytes::from("chan*")], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr[0], RespValue::bulk(Bytes::from("psubscribe")));
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_punsubscribe() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_punsubscribe(&[Bytes::from("chan*")], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr[0], RespValue::bulk(Bytes::from("punsubscribe")));
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_channels() {
        let hub = test_hub();
        let _rx1 = hub.subscribe(Bytes::from("ch1"));
        let _rx2 = hub.subscribe(Bytes::from("ch2"));
        let ctx = test_ctx(hub);
        let r = cmd_pubsub_channels(&[], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_channels_with_pattern() {
        let hub = test_hub();
        let _rx1 = hub.subscribe(Bytes::from("ch1"));
        let _rx2 = hub.subscribe(Bytes::from("other"));
        let ctx = test_ctx(hub);
        let r = cmd_pubsub_channels(&[Bytes::from("ch*")], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1);
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_numsub() {
        let hub = test_hub();
        let _rx = hub.subscribe(Bytes::from("ch1"));
        let ctx = test_ctx(hub);
        let r = cmd_pubsub_numsub(&[Bytes::from("ch1"), Bytes::from("ch2")], &ctx);
        match r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[1], RespValue::int(1));
                assert_eq!(arr[3], RespValue::int(0));
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_numpat() {
        let hub = test_hub();
        assert_eq!(hub.numpat(), 0);
        let _rx = hub.psubscribe(Bytes::from("chan*"));
        assert_eq!(hub.numpat(), 1);
    }

    #[tokio::test]
    async fn test_pubsub_shardchannels() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_pubsub_shardchannels(&[], &ctx);
        assert_eq!(r, RespValue::array(vec![]));
    }

    #[tokio::test]
    async fn test_pubsub_shardnumsub() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_pubsub_shardnumsub(&[], &ctx);
        assert_eq!(r, RespValue::array(vec![]));
    }

    #[tokio::test]
    async fn test_publish_wrong_args() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_publish(&[Bytes::from("ch1")], &ctx);
        assert_eq!(
            r,
            RespValue::Error("ERR wrong number of arguments for 'publish' command".into())
        );
    }

    #[tokio::test]
    async fn test_pubsub_unknown_subcommand() {
        let hub = test_hub();
        let ctx = test_ctx(hub);
        let r = cmd_pubsub(&[Bytes::from("INVALID")], &ctx);
        match r {
            RespValue::Error(_) => {}
            _ => panic!("expected error for unknown subcommand"),
        }
    }
}
