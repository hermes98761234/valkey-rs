use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::Store;

pub mod list;
pub mod string;

pub async fn dispatch(cmd: Vec<Bytes>, store: Arc<Store>) -> RespValue {
    if cmd.is_empty() { return RespValue::Error("ERR empty command".into()); }
    let name = match std::str::from_utf8(&cmd[0]) { Ok(s) => s.to_ascii_uppercase(), Err(_) => return RespValue::Error("ERR invalid command name".into()) };
    match name.as_str() {
        "PING" => RespValue::SimpleString("PONG".into()),
        "QUIT" => RespValue::SimpleString("OK".into()),
        "GET"|"SET"|"DEL"|"GETSET"|"MGET"|"MSET"|"MSETNX"|"INCR"|"DECR"|"INCRBY"|"DECRBY"|"INCRBYFLOAT"|"APPEND"|"STRLEN"|"GETRANGE"|"SETRANGE"|"SETNX"|"SETEX"|"PSETEX"|"GETEX"|"GETDEL" => string::handle(&cmd[1..], &store).await,
        "LPUSH"|"RPUSH"|"LPOP"|"RPOP"|"LRANGE"|"LLEN"|"LINDEX"|"LSET"|"LINSERT"|"LREM"|"LTRIM"|"LMOVE"|"BLPOP"|"BRPOP" => list::handle(&cmd[1..], &store).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", name).into()),
    }
}
