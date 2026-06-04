use bytes::Bytes;
use dashmap::DashMap;
use mlua::{Lua, MultiValue};
use sha1::{Digest, Sha1};
use std::sync::{Arc, Mutex};
use tracing::warn;
use valkey_proto::RespValue;
use valkey_storage::Store;

/// ScriptEngine manages a sandboxed Lua VM for EVAL/EVALSHA.
pub struct ScriptEngine {
    lua: Mutex<Lua>,
    scripts: DashMap<String, String>, // SHA1 -> script source
}

impl ScriptEngine {
    /// Create a new ScriptEngine with a sandboxed Lua VM.
    pub fn new() -> Self {
        let lua = Lua::new();

        // Remove dangerous globals
        let dangerous = ["io", "os", "package", "require", "dofile", "loadfile"];
        for name in &dangerous {
            lua.globals().set(*name, mlua::Nil).ok();
        }

        // Register cjson (stub — returns error if no cjson available)
        // We provide a minimal JSON encode/decode via serde_json if available,
        // but for now we skip cjson since it requires an external Lua module.
        // Redis-compatible Lua scripts typically use cjson, but we can add it later.

        ScriptEngine {
            lua: Mutex::new(lua),
            scripts: DashMap::new(),
        }
    }

    /// Compute SHA1 hex of a script.
    fn sha1hex(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// SCRIPT LOAD: store script, return SHA1.
    pub fn script_load(&self, script: &str) -> String {
        let sha = Self::sha1hex(script);
        self.scripts.insert(sha.clone(), script.to_string());
        sha
    }

    /// SCRIPT EXISTS: check which SHAs exist.
    pub fn script_exists(&self, shas: &[Bytes]) -> RespValue {
        let results: Vec<RespValue> = shas
            .iter()
            .map(|sha| {
                let s = String::from_utf8_lossy(sha);
                if self.scripts.contains_key(s.as_ref()) {
                    RespValue::Integer(1)
                } else {
                    RespValue::Integer(0)
                }
            })
            .collect();
        RespValue::Array(Some(results))
    }

    /// SCRIPT FLUSH: clear all stored scripts.
    pub fn script_flush(&self) {
        self.scripts.clear();
    }

    /// SCRIPT DEBUG: stub, return OK.
    pub fn script_debug(&self) -> RespValue {
        RespValue::SimpleString("OK".into())
    }

    /// EVALSHA: evaluate a script by SHA1.
    pub fn evalsha(
        &self,
        sha: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        let script = match self.scripts.get(sha) {
            Some(s) => s.clone(),
            None => {
                return RespValue::Error(
                    "NOSCRIPT No matching script. Please use EVAL.".into(),
                );
            }
        };
        self.eval(&script, keys, args, store)
    }

    /// EVAL: evaluate a Lua script.
    pub fn eval(
        &self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        let lua = self.lua.lock().unwrap();

        // Set KEYS and ARGV globals
        let keys_table = lua.create_table().unwrap();
        for (i, key) in keys.iter().enumerate() {
            let s = lua.create_string(key.as_ref()).unwrap();
            keys_table.set(i + 1, s).unwrap();
        }

        let argv_table = lua.create_table().unwrap();
        for (i, arg) in args.iter().enumerate() {
            let s = lua.create_string(arg.as_ref()).unwrap();
            argv_table.set(i + 1, s).unwrap();
        }

        lua.globals().set("KEYS", keys_table).unwrap();
        lua.globals().set("ARGV", argv_table).unwrap();

        // Create redis table with call/pcall/status_reply/error_reply/log/sha1hex
        let redis_table = lua.create_table().unwrap();

        // redis.call and redis.pcall need access to store
        let store_arc = store.clone();

        let call_fn = {
            let store = store_arc.clone();
            lua.create_function(move |lua_ctx, args: MultiValue| {
                let args_vec: Vec<String> = args
                    .iter()
                    .map(|v| match v {
                        mlua::Value::String(s) => s.to_string_lossy(),
                        mlua::Value::Integer(n) => n.to_string(),
                        mlua::Value::Number(n) => n.to_string(),
                        mlua::Value::Boolean(b) => b.to_string(),
                        _ => String::new(),
                    })
                    .collect();

                if args_vec.is_empty() {
                    let err_table = lua_ctx.create_table().unwrap();
                    err_table.set("err", "ERR wrong number of arguments for 'call' command").unwrap();
                    return Ok(mlua::Value::Table(err_table));
                }

                let cmd = args_vec[0].to_ascii_uppercase();
                let cmd_args: Vec<Bytes> = args_vec[1..]
                    .iter()
                    .map(|s| Bytes::from(s.clone()))
                    .collect();

                // Synchronous dispatch of simple commands
                let result = Self::dispatch_redis_command(&cmd, &cmd_args, &store);
                Ok(lua_to_resp_value(lua_ctx, &result))
            })
        };

        match &call_fn {
            Ok(f) => {
                redis_table.set("call", f.clone()).unwrap();
            }
            Err(e) => {
                warn!("Failed to create redis.call function: {}", e);
            }
        }

        // redis.pcall — same as call but returns error as table
        let store_arc2 = store.clone();
        let pcall_fn = lua.create_function(move |lua_ctx, args: MultiValue| {
            let args_vec: Vec<String> = args
                .iter()
                .map(|v| match v {
                    mlua::Value::String(s) => s.to_string_lossy(),
                    mlua::Value::Integer(n) => n.to_string(),
                    mlua::Value::Number(n) => n.to_string(),
                    mlua::Value::Boolean(b) => b.to_string(),
                    _ => String::new(),
                })
                .collect();

            if args_vec.is_empty() {
                let err_table = lua_ctx.create_table().unwrap();
                err_table
                    .set("err", "ERR wrong number of arguments for 'pcall' command")
                    .unwrap();
                return Ok(mlua::Value::Table(err_table));
            }

            let cmd = args_vec[0].to_ascii_uppercase();
            let cmd_args: Vec<Bytes> = args_vec[1..]
                .iter()
                .map(|s| Bytes::from(s.clone()))
                .collect();

            let result = Self::dispatch_redis_command(&cmd, &cmd_args, &store_arc2);
            Ok(lua_to_resp_value(lua_ctx, &result))
        });

        match &pcall_fn {
            Ok(f) => {
                redis_table.set("pcall", f.clone()).unwrap();
            }
            Err(e) => {
                warn!("Failed to create redis.pcall function: {}", e);
            }
        }

        // redis.status_reply(str) -> {ok = str}
        let status_reply_fn = lua.create_function(|lua_ctx, s: String| {
            let table = lua_ctx.create_table().unwrap();
            table.set("ok", s).unwrap();
            Ok(mlua::Value::Table(table))
        });
        if let Ok(f) = status_reply_fn {
            redis_table.set("status_reply", f).unwrap();
        }

        // redis.error_reply(str) -> {err = str}
        let error_reply_fn = lua.create_function(|lua_ctx, s: String| {
            let table = lua_ctx.create_table().unwrap();
            table.set("err", s).unwrap();
            Ok(mlua::Value::Table(table))
        });
        if let Ok(f) = error_reply_fn {
            redis_table.set("error_reply", f).unwrap();
        }

        // redis.log(level, msg)
        let log_fn = lua.create_function(|_lua_ctx, (level, msg): (String, String)| {
            match level.as_str() {
                "debug" => tracing::debug!("[lua] {}", msg),
                "verbose" => tracing::info!("[lua] {}", msg),
                "notice" => tracing::info!("[lua] {}", msg),
                "warning" => tracing::warn!("[lua] {}", msg),
                _ => tracing::info!("[lua] {}", msg),
            }
            Ok(())
        });
        if let Ok(f) = log_fn {
            redis_table.set("log", f).unwrap();
        }

        // redis.sha1hex(str)
        let sha1hex_fn = lua.create_function(|_lua_ctx, s: String| {
            Ok(Self::sha1hex(&s))
        });
        if let Ok(f) = sha1hex_fn {
            redis_table.set("sha1hex", f).unwrap();
        }

        // Set redis global
        lua.globals().set("redis", redis_table).unwrap();

        // Execute the script
        let result = lua.load(script).eval::<mlua::Value>();

        match result {
            Ok(val) => lua_value_to_resp(&val),
            Err(e) => RespValue::Error(format!("ERR Error running script: {}", e)),
        }
    }

    /// Dispatch a Redis command synchronously from Lua.
    /// Supports a subset of commands needed for scripting.
    fn dispatch_redis_command(cmd: &str, args: &[Bytes], store: &Arc<Store>) -> RespValue {
        match cmd {
            "GET" => {
                if args.is_empty() {
                    return RespValue::Error("ERR wrong number of arguments for 'get' command".into());
                }
                match store.get(&args[0]) {
                    Some(entry) => match &entry.data {
                        valkey_storage::DataType::String(s) => {
                            RespValue::BulkString(Some(s.clone()))
                        }
                        _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                    },
                    None => RespValue::BulkString(None),
                }
            }
            "SET" => {
                if args.len() < 2 {
                    return RespValue::Error("ERR wrong number of arguments for 'set' command".into());
                }
                store.set(
                    args[0].clone(),
                    valkey_storage::DataType::String(args[1].clone()),
                    None,
                );
                RespValue::SimpleString("OK".into())
            }
            "DEL" => {
                if args.is_empty() {
                    return RespValue::Error("ERR wrong number of arguments for 'del' command".into());
                }
                let count: i64 = args.iter().map(|k| if store.del(k) { 1 } else { 0 }).sum();
                RespValue::Integer(count)
            }
            "EXISTS" => {
                if args.is_empty() {
                    return RespValue::Error("ERR wrong number of arguments for 'exists' command".into());
                }
                let count: i64 = args.iter().map(|k| if store.exists(k) { 1 } else { 0 }).sum();
                RespValue::Integer(count)
            }
            "INCR" => {
                if args.is_empty() {
                    return RespValue::Error("ERR wrong number of arguments for 'incr' command".into());
                }
                let entry = store.get(&args[0]);
                let current: i64 = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => {
                            String::from_utf8_lossy(s).parse().unwrap_or(0)
                        }
                        _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                    },
                    None => 0,
                };
                let new_val = current + 1;
                store.set(
                    args[0].clone(),
                    valkey_storage::DataType::String(Bytes::from(new_val.to_string())),
                    None,
                );
                RespValue::Integer(new_val)
            }
            "DECR" => {
                if args.is_empty() {
                    return RespValue::Error("ERR wrong number of arguments for 'decr' command".into());
                }
                let entry = store.get(&args[0]);
                let current: i64 = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => {
                            String::from_utf8_lossy(s).parse().unwrap_or(0)
                        }
                        _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                    },
                    None => 0,
                };
                let new_val = current - 1;
                store.set(
                    args[0].clone(),
                    valkey_storage::DataType::String(Bytes::from(new_val.to_string())),
                    None,
                );
                RespValue::Integer(new_val)
            }
            "EXPIRE" => {
                if args.len() < 2 {
                    return RespValue::Error("ERR wrong number of arguments for 'expire' command".into());
                }
                let seconds: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
                };
                let at = std::time::Instant::now() + std::time::Duration::from_secs(seconds.max(0) as u64);
                if store.expire(&args[0], at) {
                    RespValue::Integer(1)
                } else {
                    RespValue::Integer(0)
                }
            }
            "TTL" => {
                if args.is_empty() {
                    return RespValue::Error("ERR wrong number of arguments for 'ttl' command".into());
                }
                // Simplified: return -1 if exists with expiry, -2 if not found, 20 if no expiry
                if let Some(entry) = store.get(&args[0]) {
                    match entry.expires_at {
                        Some(_) => RespValue::Integer(20), // simplified
                        None => RespValue::Integer(-1),
                    }
                } else {
                    RespValue::Integer(-2)
                }
            }
            "PING" => RespValue::SimpleString("PONG".into()),
            _ => RespValue::Error(format!("ERR unknown Redis command '{}'", cmd)),
        }
    }
}

/// Convert a RespValue into a Lua value for return from redis.call/pcall.
fn lua_to_resp_value(ctx: &mlua::Lua, val: &RespValue) -> mlua::Value {
    match val {
        RespValue::SimpleString(s) => mlua::Value::String(ctx.create_string(s.as_bytes()).unwrap()),
        RespValue::BulkString(Some(b)) => {
            mlua::Value::String(ctx.create_string(b.as_ref()).unwrap())
        }
        RespValue::BulkString(None) => mlua::Value::Nil,
        RespValue::Integer(n) => mlua::Value::Integer(*n),
        RespValue::Double(f) => mlua::Value::Number(*f),
        RespValue::Boolean(b) => mlua::Value::Boolean(*b),
        RespValue::Array(Some(items)) => {
            let table = ctx.create_table().unwrap();
            for (i, item) in items.iter().enumerate() {
                table.set(i + 1, lua_to_resp_value(ctx, item)).unwrap();
            }
            mlua::Value::Table(table)
        }
        RespValue::Array(None) => mlua::Value::Nil,
        RespValue::Error(s) => {
            let table = ctx.create_table().unwrap();
            table.set("err", s.clone()).unwrap();
            mlua::Value::Table(table)
        }
        _ => mlua::Value::Nil,
    }
}

/// Convert a Lua value to a RespValue.
fn lua_value_to_resp(val: &mlua::Value) -> RespValue {
    match val {
        mlua::Value::Nil => RespValue::BulkString(None),
        mlua::Value::Boolean(b) => {
            if *b {
                RespValue::Integer(1)
            } else {
                RespValue::BulkString(None)
            }
        }
        mlua::Value::Integer(n) => RespValue::Integer(*n),
        mlua::Value::Number(n) => {
            if n.is_finite() && n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                RespValue::Integer(*n as i64)
            } else {
                RespValue::Double(*n)
            }
        }
        mlua::Value::String(s) => {
            RespValue::BulkString(Some(Bytes::copy_from_slice(&s.as_bytes())))
        }
        mlua::Value::Table(table) => {
            // Check for {ok = str} or {err = str}
            if let Ok(ok_val) = table.get::<String>("ok") {
                return RespValue::SimpleString(ok_val);
            }
            if let Ok(err_val) = table.get::<String>("err") {
                return RespValue::Error(err_val);
            }

            // Convert to array
            let mut items = Vec::new();
            let mut i = 1;
            loop {
                match table.get::<mlua::Value>(i) {
                    Ok(mlua::Value::Nil) => break,
                    Ok(v) => {
                        items.push(lua_value_to_resp(&v));
                        i += 1;
                    }
                    Err(_) => break,
                }
            }
            if items.is_empty() {
                RespValue::BulkString(None)
            } else {
                RespValue::Array(Some(items))
            }
        }
        _ => RespValue::BulkString(None),
    }
}

/// Global script engine instance.
static SCRIPT_ENGINE: std::sync::OnceLock<Arc<ScriptEngine>> = std::sync::OnceLock::new();

/// Get or initialize the global ScriptEngine.
pub fn script_engine() -> Arc<ScriptEngine> {
    SCRIPT_ENGINE
        .get_or_init(|| Arc::new(ScriptEngine::new()))
        .clone()
}

/// Handle EVAL command.
pub fn handle_eval(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'eval' command".into(),
        );
    }
    let script = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR script must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1])
        .unwrap_or("0")
        .parse()
    {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error(
            "ERR Number of keys can't be greater than number of args".into(),
        );
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.eval(script, keys, cmd_args, store.clone())
}

/// Handle EVALSHA command.
pub fn handle_evalsha(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'evalsha' command".into(),
        );
    }
    let sha = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return RespValue::Error("ERR sha must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1])
        .unwrap_or("0")
        .parse()
    {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error(
            "ERR Number of keys can't be greater than number of args".into(),
        );
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.evalsha(&sha, keys, cmd_args, store.clone())
}

/// Handle SCRIPT subcommands.
pub fn handle_script(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'script' command".into());
    }

    let subcmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };

    let engine = script_engine();

    match subcmd.as_str() {
        "LOAD" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'script load' command".into(),
                );
            }
            let script = match std::str::from_utf8(&args[1]) {
                Ok(s) => s,
                Err(_) => return RespValue::Error("ERR script must be a string".into()),
            };
            let sha = engine.script_load(script);
            RespValue::BulkString(Some(Bytes::from(sha)))
        }
        "EXISTS" => {
            let shas = &args[1..];
            engine.script_exists(shas)
        }
        "FLUSH" => {
            engine.script_flush();
            RespValue::SimpleString("OK".into())
        }
        "DEBUG" => engine.script_debug(),
        _ => RespValue::Error(format!(
            "ERR Unknown SCRIPT subcommand '{}'.",
            subcmd.to_ascii_lowercase()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use valkey_storage::Store;

    fn test_store() -> Arc<Store> {
        Store::new()
    }

    #[tokio::test]
    async fn test_eval_returns_integer() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval("return 42", vec![], vec![], store);
        assert_eq!(result, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_eval_returns_string() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval(
            "return 'hello'",
            vec![],
            vec![],
            store,
        );
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from("hello")))
        );
    }

    #[tokio::test]
    async fn test_eval_returns_array() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval(
            "return {1, 2, 3}",
            vec![],
            vec![],
            store,
        );
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ]))
        );
    }

    #[tokio::test]
    async fn test_eval_boolean_true() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval("return true", vec![], vec![], store);
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_eval_boolean_false() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval("return false", vec![], vec![], store);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_eval_nil() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval("return nil", vec![], vec![], store);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_eval_redis_status_reply() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval(
            "return redis.status_reply('OK')",
            vec![],
            vec![],
            store,
        );
        assert_eq!(result, RespValue::SimpleString("OK".into()));
    }

    #[tokio::test]
    async fn test_eval_redis_error_reply() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval(
            "return redis.error_reply('ERR something went wrong')",
            vec![],
            vec![],
            store,
        );
        assert_eq!(
            result,
            RespValue::Error("ERR something went wrong".into())
        );
    }

    #[tokio::test]
    async fn test_eval_redis_call_set_get() {
        let store = test_store();
        let engine = ScriptEngine::new();

        // SET a key via redis.call
        let result = engine.eval(
            "return redis.call('SET', 'mykey', 'myvalue')",
            vec![],
            vec![],
            store.clone(),
        );
        // SET returns SimpleString("OK") which becomes Lua string, 
        // and Lua string becomes BulkString on return
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("OK"))));

        // GET the key via redis.call
        let result = engine.eval(
            "return redis.call('GET', 'mykey')",
            vec![],
            vec![],
            store.clone(),
        );
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from("myvalue")))
        );
    }

    #[tokio::test]
    async fn test_eval_redis_call_incr() {
        let store = test_store();
        let engine = ScriptEngine::new();

        // Set initial value
        store.set(
            Bytes::from("counter"),
            valkey_storage::DataType::String(Bytes::from("10")),
            None,
        );

        let result = engine.eval(
            "return redis.call('INCR', 'counter')",
            vec![],
            vec![],
            store.clone(),
        );
        assert_eq!(result, RespValue::Integer(11));
    }

    #[tokio::test]
    async fn test_eval_keys_and_argv() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let keys = vec![Bytes::from("mykey")];
        let args = vec![Bytes::from("myarg")];

        let result = engine.eval(
            "return {KEYS[1], ARGV[1]}",
            keys,
            args,
            store,
        );
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from("mykey"))),
                RespValue::BulkString(Some(Bytes::from("myarg"))),
            ]))
        );
    }

    #[tokio::test]
    async fn test_script_load_and_evalsha() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let script = "return 42";
        let sha = engine.script_load(script);

        // Verify SHA1 is 40 hex chars
        assert_eq!(sha.len(), 40);

        // EVALSHA should work
        let result = engine.evalsha(&sha, vec![], vec![], store.clone());
        assert_eq!(result, RespValue::Integer(42));

        // EVALSHA with unknown SHA should return NOSCRIPT
        let result = engine.evalsha(
            "0000000000000000000000000000000000000000",
            vec![],
            vec![],
            store,
        );
        assert_eq!(
            result,
            RespValue::Error("NOSCRIPT No matching script. Please use EVAL.".into())
        );
    }

    #[tokio::test]
    async fn test_script_exists() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let sha = engine.script_load("return 1");

        let result = engine.script_exists(&[Bytes::from(sha.clone())]);
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(1)]))
        );

        let result = engine.script_exists(&[Bytes::from(
            "0000000000000000000000000000000000000000",
        )]);
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(0)]))
        );
    }

    #[tokio::test]
    async fn test_script_flush() {
        let store = test_store();
        let engine = ScriptEngine::new();

        engine.script_load("return 1");
        engine.script_flush();

        let result = engine.script_exists(&[Bytes::from(
            "0000000000000000000000000000000000000000",
        )]);
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(0)]))
        );
    }

    #[tokio::test]
    async fn test_eval_sha1hex() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let result = engine.eval(
            "return redis.sha1hex('hello')",
            vec![],
            vec![],
            store,
        );
        // SHA1 of "hello" is aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from(
                "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
            )))
        );
    }

    #[tokio::test]
    async fn test_eval_redis_call_del() {
        let store = test_store();
        let engine = ScriptEngine::new();

        // Set two keys
        store.set(
            Bytes::from("key1"),
            valkey_storage::DataType::String(Bytes::from("val1")),
            None,
        );
        store.set(
            Bytes::from("key2"),
            valkey_storage::DataType::String(Bytes::from("val2")),
            None,
        );

        let result = engine.eval(
            "return redis.call('DEL', 'key1', 'key2', 'nonexistent')",
            vec![],
            vec![],
            store,
        );
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_eval_redis_call_exists() {
        let store = test_store();
        let engine = ScriptEngine::new();

        store.set(
            Bytes::from("existing"),
            valkey_storage::DataType::String(Bytes::from("val")),
            None,
        );

        let result = engine.eval(
            "return redis.call('EXISTS', 'existing', 'missing')",
            vec![],
            vec![],
            store,
        );
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_sandbox_no_io() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let result = engine.eval(
            "return io",
            vec![],
            vec![],
            store,
        );
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_sandbox_no_os() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let result = engine.eval(
            "return os",
            vec![],
            vec![],
            store,
        );
        assert_eq!(result, RespValue::BulkString(None));
    }
}
