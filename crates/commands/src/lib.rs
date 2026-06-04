use bytes::Bytes;
use std::sync::Arc;

use valkey_proto::RespValue;
use valkey_storage::Store;

mod keys;

pub async fn dispatch(cmd: Vec<Bytes>, store: Arc<Store>) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR empty command".into());
    }

    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };

    match name.as_str() {
        "PING" => RespValue::SimpleString("PONG".into()),
        "QUIT" => RespValue::SimpleString("OK".into()),
        _ => keys::handle(&cmd, &store).await,
    }
}
