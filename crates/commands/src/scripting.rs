use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_scripting::{
    handle_eval, handle_eval_ro, handle_evalsha, handle_evalsha_ro, handle_fcall, handle_fcall_ro,
    handle_function, handle_script,
};
use valkey_storage::Store;

/// Handle EVAL command.
pub async fn handle_eval_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_eval(args, store)
}

/// Handle EVALSHA command.
pub async fn handle_evalsha_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_evalsha(args, store)
}

/// Handle EVALRO command (read-only).
pub async fn handle_evalro_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_eval_ro(args, store)
}

/// Handle EVALSHARO command (read-only).
pub async fn handle_evalsharo_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_evalsha_ro(args, store)
}

/// Handle SCRIPT subcommands.
pub async fn handle_script_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_script(args, store)
}

/// Handle FCALL command.
pub async fn handle_fcall_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_fcall(args, store)
}

/// Handle FCALL_RO command (read-only).
pub async fn handle_fcall_ro_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_fcall_ro(args, store)
}

/// Handle FUNCTION subcommands.
pub async fn handle_function_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    handle_function(args, store)
}
