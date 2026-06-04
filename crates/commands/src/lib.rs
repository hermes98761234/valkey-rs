use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::Store;

pub mod list;

pub async fn dispatch(cmd: Vec<Bytes>, store: Arc<Store>) -> RespValue {
    let name = match cmd.first() {
        Some(b) => b.to_ascii_uppercase(),
        None => {
            return RespValue::Error("ERR empty command".into());
        }
    };

    match name.as_slice() {
        b"PING" => RespValue::SimpleString("PONG".into()),
        b"QUIT" => RespValue::SimpleString("OK".into()),
        b"LPUSH" | b"RPUSH" | b"LPOP" | b"RPOP" | b"LRANGE" | b"LLEN" | b"LINDEX"
        | b"LSET" | b"LINSERT" | b"LREM" | b"LTRIM" | b"LMOVE" | b"BLPOP" | b"BRPOP" => {
            list::handle(&cmd[1..], &store).await
        }
        _ => RespValue::Error(
            format!("ERR unknown command `{}`", String::from_utf8_lossy(&name)).into(),
        ),
    }
}
