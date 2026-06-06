use bytes::Bytes;
use std::sync::{Arc, RwLock};
use valkey_proto::RespValue;

use crate::server::ClientCtx;
use crate::Db;

// MULTI — start a transaction block
pub async fn cmd_multi(client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let mut ctx = client.write().unwrap();
    if ctx.multi {
        return RespValue::Error("ERR MULTI calls can not be nested".into());
    }
    ctx.multi = true;
    ctx.queue.clear();
    ctx.dirty = false;
    RespValue::ok()
}

// DISCARD — abort transaction
pub async fn cmd_discard(client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let mut ctx = client.write().unwrap();
    if !ctx.multi {
        return RespValue::Error("ERR DISCARD without MULTI".into());
    }
    ctx.multi = false;
    ctx.queue.clear();
    ctx.watched.clear();
    ctx.dirty = false;
    RespValue::ok()
}

// EXEC — execute queued commands
pub async fn cmd_exec(client: Arc<RwLock<ClientCtx>>, store: Db) -> RespValue {
    let (queue, dirty) = {
        let mut ctx = client.write().unwrap();
        if !ctx.multi {
            return RespValue::Error("ERR EXEC without MULTI".into());
        }
        let q = ctx.queue.clone();
        let d = ctx.dirty;
        ctx.multi = false;
        ctx.queue.clear();
        ctx.watched.clear();
        ctx.dirty = false;
        (q, d)
    };

    if dirty {
        return RespValue::Array(None);
    }

    let cmd_ctx = crate::CommandCtx {
        client: Arc::clone(&client),
        config: Arc::new(RwLock::new(crate::server::ServerConfig::default())),
    };
    let mut results = Vec::with_capacity(queue.len());
    for cmd in &queue {
        let response = Box::pin(crate::dispatch_ctx(
            cmd.clone(),
            Arc::clone(&store),
            &cmd_ctx,
        ))
        .await;
        results.push(response);
    }

    RespValue::Array(Some(results))
}

// WATCH — watch keys for optimistic locking
pub async fn cmd_watch(args: &[Bytes], client: Arc<RwLock<ClientCtx>>, store: &Db) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'watch' command".into());
    }
    let mut ctx = client.write().unwrap();
    if ctx.multi {
        return RespValue::Error("ERR WATCH inside MULTI is not allowed".into());
    }
    ctx.watched = args.to_vec();
    ctx.dirty = false;
    // Register watchers with the store for each key
    for key in args {
        let client_clone = Arc::clone(&client);
        let rx = store.watch(key);
        tokio::task::spawn_blocking(move || {
            // Wait for notification (blocking recv moved off async thread)
            if rx.recv().is_ok() {
                let mut c = client_clone.write().unwrap();
                c.dirty = true;
            }
        });
    }
    RespValue::ok()
}

// UNWATCH — clear all watched keys
pub async fn cmd_unwatch(client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let mut ctx = client.write().unwrap();
    ctx.watched.clear();
    ctx.dirty = false;
    RespValue::ok()
}

// Handle a transaction command, or return None if not a transaction command
pub async fn handle(
    cmd: &[Bytes],
    store: &Db,
    client: Arc<RwLock<ClientCtx>>,
) -> Option<RespValue> {
    if cmd.is_empty() {
        return None;
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return None,
    };
    match name.as_str() {
        "MULTI" => Some(cmd_multi(client).await),
        "DISCARD" => Some(cmd_discard(client).await),
        "EXEC" => Some(cmd_exec(client, Arc::clone(store)).await),
        "WATCH" => Some(cmd_watch(&cmd[1..], client, store).await),
        "UNWATCH" => Some(cmd_unwatch(client).await),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dispatch_ctx, CommandCtx};
    use bytes::Bytes;
    use std::sync::Arc;

    fn test_cmd(name: &str, args: &[&str]) -> Vec<Bytes> {
        let mut cmd = vec![Bytes::from(name.to_string())];
        for a in args {
            cmd.push(Bytes::from(a.to_string()));
        }
        cmd
    }

    fn test_ctx() -> &'static CommandCtx {
        use std::sync::OnceLock;
        static CTX: OnceLock<CommandCtx> = OnceLock::new();
        CTX.get_or_init(CommandCtx::new)
    }

    fn fresh_ctx() -> CommandCtx {
        CommandCtx::new()
    }

    #[tokio::test]
    async fn test_multi_exec_happy_path() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("MULTI", &[]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());

        let r = dispatch_ctx(test_cmd("SET", &["a", "1"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::SimpleString("QUEUED".into()));

        let r = dispatch_ctx(test_cmd("SET", &["b", "2"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::SimpleString("QUEUED".into()));

        let r = dispatch_ctx(test_cmd("GET", &["a"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::SimpleString("QUEUED".into()));

        let r = dispatch_ctx(test_cmd("EXEC", &[]), store.clone(), &ctx).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], RespValue::ok());
                assert_eq!(arr[1], RespValue::ok());
                assert_eq!(arr[2], RespValue::bulk(Bytes::from("1")));
            }
            _ => panic!("expected array, got {:?}", r),
        }

        let r = dispatch_ctx(test_cmd("GET", &["a"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("1")));

        let r = dispatch_ctx(test_cmd("GET", &["b"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("2")));
    }

    #[tokio::test]
    async fn test_multi_nested_error() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("MULTI", &[]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());

        let r = dispatch_ctx(test_cmd("MULTI", &[]), store.clone(), &ctx).await;
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_discard_clears_queue() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("MULTI", &[]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());

        let r = dispatch_ctx(test_cmd("SET", &["x", "42"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::SimpleString("QUEUED".into()));

        let r = dispatch_ctx(test_cmd("DISCARD", &[]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());

        let r = dispatch_ctx(test_cmd("GET", &["x"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::null_bulk());
    }

    #[tokio::test]
    async fn test_discard_without_multi_error() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("DISCARD", &[]), store.clone(), &ctx).await;
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_exec_without_multi_error() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("EXEC", &[]), store.clone(), &ctx).await;
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_unwatch() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("UNWATCH", &[]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_watch_without_multi() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("WATCH", &["w"]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_watch_inside_multi_error() {
        let store = valkey_storage::Store::new();
        let ctx = fresh_ctx();

        let r = dispatch_ctx(test_cmd("MULTI", &[]), store.clone(), &ctx).await;
        assert_eq!(r, RespValue::ok());

        let r = dispatch_ctx(test_cmd("WATCH", &["w"]), store.clone(), &ctx).await;
        assert!(matches!(r, RespValue::Error(_)));
    }
}
