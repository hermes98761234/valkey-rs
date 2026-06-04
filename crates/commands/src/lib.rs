use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::Store;

pub mod zset;

pub type Db = Arc<Store>;

pub async fn dispatch(cmd: Vec<Bytes>, store: Db) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR empty command".into());
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    let args = &cmd[1..];
    let r = match name.as_str() {
        "PING" => return RespValue::SimpleString("PONG".into()),
        "QUIT" => return RespValue::SimpleString("OK".into()),
        "ZADD" => zset::zadd(&store, args),
        "ZSCORE" => zset::zscore(&store, args),
        "ZMSCORE" => zset::zmscore(&store, args),
        "ZRANK" => zset::zrank(&store, args),
        "ZREVRANK" => zset::zrevrank(&store, args),
        "ZRANGE" => zset::zrange(&store, args),
        "ZREVRANGE" => zset::zrevrange(&store, args),
        "ZRANGEBYSCORE" => zset::zrangebyscore(&store, args),
        "ZREVRANGEBYSCORE" => zset::zrevrangebyscore(&store, args),
        "ZRANGEBYLEX" => zset::zrangebylex(&store, args),
        "ZCOUNT" => zset::zcount(&store, args),
        "ZLEXCOUNT" => zset::zlexcount(&store, args),
        "ZREM" => zset::zrem(&store, args),
        "ZREMRANGEBYRANK" => zset::zremrangebyrank(&store, args),
        "ZREMRANGEBYSCORE" => zset::zremrangebyscore(&store, args),
        "ZREMRANGEBYLEX" => zset::zremrangebylex(&store, args),
        "ZCARD" => zset::zcard(&store, args),
        "ZINCRBY" => zset::zincrby(&store, args),
        "ZPOPMIN" => zset::zpopmin(&store, args),
        "ZPOPMAX" => zset::zpopmax(&store, args),
        "ZUNIONSTORE" => zset::zunionstore(&store, args),
        "ZINTERSTORE" => zset::zinterstore(&store, args),
        "ZDIFFSTORE" => zset::zdiffstore(&store, args),
        "ZSCAN" => zset::zscan(&store, args),
        "ZRANDMEMBER" => zset::zrandmember(&store, args),
        "ZRANGESTORE" => zset::zrangestore(&store, args),
        _ => return RespValue::Error(format!("ERR unknown command `{}`", name).into()),
    };
    match r {
        Ok(v) => v,
        Err(e) => RespValue::Error(e),
    }
}
