use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::Store;

pub async fn dispatch(cmd: Vec<Bytes>, _store: Arc<Store>) -> RespValue {
    let name = match cmd.first() {
        Some(b) => b.to_ascii_uppercase(),
        None => {
            return RespValue::Error("ERR empty command".into());
        }
    };

    match name.as_slice() {
        b"PING" => RespValue::SimpleString("PONG".into()),
        b"QUIT" => RespValue::SimpleString("OK".into()),
        _ => RespValue::Error(
            format!("ERR unknown command `{}`", String::from_utf8_lossy(&name)).into(),
        ),
    }
}
