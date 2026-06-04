use bytes::Bytes;
use valkey_proto::RespValue;
use valkey_storage::Store;
use std::sync::Arc;

pub fn handle_eval(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    RespValue::Error("ERR EVAL not yet implemented".into())
}

pub fn handle_evalsha(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    RespValue::Error("ERR EVALSHA not yet implemented".into())
}

pub fn handle_script(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    RespValue::Error("ERR SCRIPT not yet implemented".into())
}
