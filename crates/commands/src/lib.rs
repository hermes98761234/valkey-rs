use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::Store;

pub mod set;

pub async fn dispatch(cmd: Vec<Bytes>, store: Arc<Store>) -> RespValue {
    let name = match cmd.first() {
        Some(b) => b.to_ascii_uppercase(),
        None => {
            return RespValue::Error("ERR empty command".into());
        }
    };

    let args = &cmd[1..];

    match name.as_slice() {
        b"PING" => RespValue::SimpleString("PONG".into()),
        b"QUIT" => RespValue::SimpleString("OK".into()),
        b"SADD" => set::sadd(&store, args),
        b"SMEMBERS" => set::smembers(&store, args),
        b"SISMEMBER" => set::sismember(&store, args),
        b"SMISMEMBER" => set::smismember(&store, args),
        b"SCARD" => set::scard(&store, args),
        b"SREM" => set::srem(&store, args),
        b"SPOP" => set::spop(&store, args),
        b"SRANDMEMBER" => set::srandmember(&store, args),
        b"SMOVE" => set::smove(&store, args),
        b"SUNION" => set::sunion(&store, args),
        b"SINTER" => set::sinter(&store, args),
        b"SDIFF" => set::sdiff(&store, args),
        b"SUNIONSTORE" => set::sunionstore(&store, args),
        b"SINTERSTORE" => set::sinterstore(&store, args),
        b"SDIFFSTORE" => set::sdiffstore(&store, args),
        b"SSCAN" => set::sscan(&store, args),
        _ => RespValue::Error(
            format!("ERR unknown command `{}`", String::from_utf8_lossy(&name)).into(),
        ),
    }
}
