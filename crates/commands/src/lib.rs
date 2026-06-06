#![allow(
    clippy::len_without_is_empty,
    clippy::len_zero,
    clippy::manual_is_multiple_of,
    clippy::unwrap_or_default,
    clippy::redundant_closure,
    clippy::single_match,
    clippy::trim_split_whitespace,
    clippy::unnecessary_to_owned,
    clippy::useless_conversion,
    clippy::needless_return,
    clippy::match_single_binding,
    clippy::needless_borrow,
    clippy::field_reassign_with_default,
    clippy::new_without_default,
    clippy::should_implement_trait,
    clippy::useless_format,
    clippy::map_clone,
    clippy::vec_init_then_push,
    clippy::manual_strip,
    clippy::manual_ignore_case_cmp,
    clippy::needless_range_loop,
    clippy::manual_unwrap_or,
    clippy::for_kv_map,
    dead_code,
    unused_imports,
    unused_mut,
    unused_variables
)]
use bytes::Bytes;
use std::sync::{Arc, OnceLock, RwLock};
use valkey_proto::RespValue;
use valkey_storage::Store;

pub mod acl;
pub mod asking;
pub mod cluster;
pub mod hash;
pub mod keys;
pub mod list;
pub mod migrate;
pub mod pubsub;
pub mod replication;
pub mod scripting;
pub mod server;
pub mod set;
pub mod string;
pub mod transaction;
pub mod zset;

pub type Db = Arc<Store>;

pub struct CommandCtx {
    pub client: Arc<RwLock<server::ClientCtx>>,
    pub config: Arc<RwLock<server::ServerConfig>>,
}

impl CommandCtx {
    pub fn new() -> Self {
        Self {
            client: Arc::new(RwLock::new(server::ClientCtx::new())),
            config: Arc::new(RwLock::new(server::ServerConfig::default())),
        }
    }
}

fn global_ctx() -> &'static CommandCtx {
    static CTX: OnceLock<CommandCtx> = OnceLock::new();
    CTX.get_or_init(CommandCtx::new)
}

pub async fn dispatch(cmd: Vec<Bytes>, store: Db) -> RespValue {
    dispatch_ctx(cmd, store, global_ctx()).await
}

pub async fn dispatch_ctx(cmd: Vec<Bytes>, store: Db, ctx: &CommandCtx) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR empty command".into());
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    let args = &cmd[1..];

    // Transaction commands are always handled, even inside MULTI
    let is_transaction_cmd = matches!(
        name.as_str(),
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH"
    );

    // If in MULTI mode and not a transaction command, queue it
    if !is_transaction_cmd {
        let is_multi = ctx.client.read().unwrap().multi;
        if is_multi {
            let mut client = ctx.client.write().unwrap();
            client.queue.push(cmd);
            return RespValue::SimpleString("QUEUED".into());
        }
    }

    // ACL permission check (skip for ACL/AUTH/transaction commands to avoid deadlock)
    let is_acl_or_auth = matches!(name.as_str(), "ACL" | "AUTH");
    if !is_transaction_cmd && !is_acl_or_auth {
        let client_guard = ctx.client.read().unwrap();
        let keys: Vec<Bytes> = args.to_vec();
        if let Err(reason) = acl::check_permission(&client_guard, &name, &keys) {
            return RespValue::Error(reason.into());
        }
    }

    match name.as_str() {
        "PING" | "ECHO" | "SELECT" | "DBSIZE" | "FLUSHDB" | "FLUSHALL" | "INFO" | "COMMAND"
        | "CONFIG" | "SAVE" | "BGSAVE" | "BGREWRITEAOF" | "LASTSAVE" | "TIME" | "LATENCY"
        | "SLOWLOG" | "MEMORY" | "CLIENT" | "DEBUG" | "OBJECT" | "RESET" | "REPLCONF"
        | "REPLICAOF" | "SLAVEOF" => {
            server::handle(&cmd, &store, ctx.client.clone(), ctx.config.clone()).await
        }
        "QUIT" => return RespValue::SimpleString("OK".into()),
        "GET" | "SET" | "DEL" | "GETSET" | "MGET" | "MSET" | "MSETNX" | "INCR" | "DECR"
        | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "APPEND" | "STRLEN" | "GETRANGE" | "SETRANGE"
        | "SETNX" | "SETEX" | "PSETEX" | "GETEX" | "GETDEL" => string::handle(&cmd, &store).await,
        "EXISTS" | "TYPE" | "RENAME" | "RENAMENX" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
        | "PEXPIREAT" | "TTL" | "PTTL" | "PERSIST" | "KEYS" | "SCAN" | "RANDOMKEY" | "MOVE"
        | "COPY" | "DUMP" | "RESTORE" | "SORT" | "SORT_RO" | "SUBSTR" | "EXPIRETIME"
        | "PEXPIRETIME" | "UNLINK" => keys::handle(&cmd, &store).await,
        "WAIT" => replication::cmd_wait(&cmd[1..]).await,
        "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LRANGE" | "LLEN" | "LINDEX" | "LSET" | "LINSERT"
        | "LREM" | "LTRIM" | "LMOVE" | "BLPOP" | "BRPOP" | "LMPOP" | "BLMPOP" | "BLMOVE"
        | "LPOS" => list::handle(&cmd, &store).await,
        "HSET" | "HGET" | "HMGET" | "HMSET" | "HGETALL" | "HDEL" | "HEXISTS" | "HLEN" | "HKEYS"
        | "HVALS" | "HINCRBY" | "HINCRBYFLOAT" | "HSCAN" | "HRANDFIELD" => {
            hash::handle(&cmd, &store)
        }
        "SADD" | "SMEMBERS" | "SISMEMBER" | "SMISMEMBER" | "SCARD" | "SREM" | "SPOP"
        | "SRANDMEMBER" | "SMOVE" | "SUNION" | "SINTER" | "SDIFF" | "SUNIONSTORE"
        | "SINTERSTORE" | "SINTERCARD" | "SDIFFSTORE" | "SSCAN" => set::handle(&cmd, &store),
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PUBLISH" | "PUBSUB"
        | "SSUBSCRIBE" | "SUNSUBSCRIBE" | "SPUBLISH" => {
            return RespValue::Error("ERR PubSub commands must be handled in pub/sub mode".into());
        }
        "EVAL" => scripting::handle_eval_cmd(args, &store).await,
        "EVALSHA" => scripting::handle_evalsha_cmd(args, &store).await,
        "EVALRO" => scripting::handle_evalro_cmd(args, &store).await,
        "EVALSHARO" => scripting::handle_evalsharo_cmd(args, &store).await,
        "SCRIPT" => scripting::handle_script_cmd(args, &store).await,
        "FCALL" => scripting::handle_fcall_cmd(args, &store).await,
        "FCALL_RO" => scripting::handle_fcall_ro_cmd(args, &store).await,
        "FUNCTION" => scripting::handle_function_cmd(args, &store).await,
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" => {
            transaction::handle(&cmd, &store, ctx.client.clone())
                .await
                .unwrap_or_else(|| RespValue::Error("ERR internal error".into()))
        }
        "ACL" => acl::handle(args, ctx.client.clone()).await,
        "AUTH" => acl::cmd_auth(args, ctx.client.clone()).await,
        "CLUSTER" => cluster::handle(args).await,
        "MIGRATE" => migrate::handle(args, &store).await,
        "ASKING" => asking::handle(args).await,
        "BZPOPMIN" => zset::bzpopmin(&store, args).await,
        "BZPOPMAX" => zset::bzpopmax(&store, args).await,
        "BZMPOP" => zset::bzmpop(&store, args).await,
        _ => {
            let r = match name.as_str() {
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
                "ZDIFF" => zset::zdiff(&store, args),
                "ZINTER" => zset::zinter(&store, args),
                "ZUNION" => zset::zunion(&store, args),
                "ZMPOP" => zset::zmpop(&store, args),
                "PSYNC" => Ok(replication::cmd_psync(&cmd[1..], &store).await),
                _ => return RespValue::Error(format!("ERR unknown command `{}`", name).into()),
            };
            match r {
                Ok(v) => v,
                Err(e) => RespValue::Error(e),
            }
        }
    }
}
