use bytes::Bytes;
use valkey_proto::RespValue;

/// Handle the ASKING command.
///
/// ASKING indicates that the next command should be directed to a slot
/// that is being imported. The server sets the ASKING flag on the client
/// context so that the cluster router will allow access to importing slots.
pub async fn handle(_args: &[Bytes]) -> RespValue {
    // The actual ASKING flag is set in the per-connection CommandCtx
    // at the connection handler level. This handler just acknowledges the command.
    // Full implementation would set a per-client flag in the connection context.
    RespValue::SimpleString("OK".into())
}
