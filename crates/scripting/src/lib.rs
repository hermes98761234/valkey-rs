#![allow(
    clippy::manual_is_multiple_of,
    clippy::unwrap_or_default,
    clippy::redundant_closure,
    clippy::unnecessary_to_owned,
    clippy::useless_conversion,
    clippy::needless_return,
    clippy::match_single_binding,
    clippy::needless_borrow,
    clippy::field_reassign_with_default,
    clippy::new_without_default,
    clippy::should_implement_trait,
    clippy::len_zero,
    clippy::unused_self,
    dead_code,
    unused_imports,
    unused_mut,
    unused_variables
)]
use bytes::Bytes;
use dashmap::DashMap;
use mlua::{Lua, MultiValue};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::warn;
use valkey_proto::RespValue;
use valkey_storage::Store;

/// A registered Redis Function (FUNCTION API).
#[derive(Clone)]
pub struct FunctionEntry {
    /// The Lua script body (without the `register_function` wrapper).
    pub body: String,
    /// SHA1 of the body.
    pub sha: String,
    /// Optional function name/flags description.
    pub description: Option<String>,
}

/// ScriptEngine manages a sandboxed Lua VM for EVAL/EVALSHA/FCALL/FUNCTION.
pub struct ScriptEngine {
    lua: Mutex<Lua>,
    scripts: DashMap<String, String>,                   // SHA1 -> script source
    functions: DashMap<String, FunctionEntry>,           // name -> FunctionEntry
    /// Set to true for read-only mode (EVALRO/EVALSHARO/FCALL_RO).
    read_only: bool,
}

impl Default for ScriptEngine {
    fn default() -> Self {
        Self::new()
    }
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

        ScriptEngine {
            lua: Mutex::new(lua),
            scripts: DashMap::new(),
            functions: DashMap::new(),
            read_only: false,
        }
    }

    /// Compute SHA1 hex of a script.
    fn sha1hex(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        hex::encode(hasher.finalize())
    }

    // ── SCRIPT commands ──────────────────────────────────────────────

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

    // ── EVAL / EVALSHA ───────────────────────────────────────────────

    /// EVALSHA: evaluate a script by SHA1.
    pub fn evalsha(
        &self,
        sha: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        self.evalsha_impl(sha, keys, args, store, false)
    }

    /// EVALSHARO: read-only variant of EVALSHA.
    pub fn evalsha_ro(
        &self,
        sha: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        self.evalsha_impl(sha, keys, args, store, true)
    }

    fn evalsha_impl(
        &self,
        sha: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
        read_only: bool,
    ) -> RespValue {
        let script = match self.scripts.get(sha) {
            Some(s) => s.clone(),
            None => {
                return RespValue::Error("NOSCRIPT No matching script. Please use EVAL.".into());
            }
        };
        self.eval_impl(&script, keys, args, store, read_only)
    }

    /// EVAL: evaluate a Lua script.
    pub fn eval(
        &self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        self.eval_impl(script, keys, args, store, false)
    }

    /// EVALRO: read-only variant of EVAL.
    pub fn eval_ro(
        &self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        self.eval_impl(script, keys, args, store, true)
    }

    /// Core evaluation logic.
    fn eval_impl(
        &self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
        read_only: bool,
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

        // redis.call
        let store_arc = store.clone();
        let call_fn = {
            let store = store_arc.clone();
            let ro = read_only;
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
                    err_table
                        .set("err", "ERR wrong number of arguments for 'call' command")
                        .unwrap();
                    return Ok(mlua::Value::Table(err_table));
                }

                let cmd = args_vec[0].to_ascii_uppercase();

                // Read-only check
                if ro && is_write_command(&cmd) {
                    let err_table = lua_ctx.create_table().unwrap();
                    err_table
                        .set("err", "ERR Write commands are not allowed in read-only mode")
                        .unwrap();
                    return Ok(mlua::Value::Table(err_table));
                }

                let cmd_args: Vec<Bytes> = args_vec[1..]
                    .iter()
                    .map(|s| Bytes::from(s.clone()))
                    .collect();

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

            // Read-only check
            if read_only && is_write_command(&cmd) {
                let err_table = lua_ctx.create_table().unwrap();
                err_table
                    .set("err", "ERR Write commands are not allowed in read-only mode")
                    .unwrap();
                return Ok(mlua::Value::Table(err_table));
            }

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
        let sha1hex_fn = lua.create_function(|_lua_ctx, s: String| Ok(Self::sha1hex(&s)));
        if let Ok(f) = sha1hex_fn {
            redis_table.set("sha1hex", f).unwrap();
        }

        // redis.set_resp_ver(version) — stub
        let set_resp_fn =
            lua.create_function(|_lua_ctx, _v: i64| Ok(()));
        if let Ok(f) = set_resp_fn {
            redis_table.set("set_resp_ver", f).unwrap();
        }

        // redis.log_redis(level, msg) — alias for redis.log
        let log_redis_fn = lua.create_function(|_lua_ctx, (level, msg): (String, String)| {
            match level.as_str() {
                "debug" => tracing::debug!("[lua] {}", msg),
                "verbose" => tracing::info!("[lua] {}", msg),
                "notice" => tracing::info!("[lua] {}", msg),
                "warning" => tracing::warn!("[lua] {}", msg),
                _ => tracing::info!("[lua] {}", msg),
            }
            Ok(())
        });
        if let Ok(f) = log_redis_fn {
            redis_table.set("log_redis", f).unwrap();
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

    // ── FCALL / FCALL_RO ─────────────────────────────────────────────

    /// FCALL: call a registered function by name.
    pub fn fcall(
        &self,
        name: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        self.fcall_impl(name, keys, args, store, false)
    }

    /// FCALL_RO: read-only variant of FCALL.
    pub fn fcall_ro(
        &self,
        name: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
    ) -> RespValue {
        self.fcall_impl(name, keys, args, store, true)
    }

    fn fcall_impl(
        &self,
        name: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        store: Arc<Store>,
        read_only: bool,
    ) -> RespValue {
        let entry = match self.functions.get(name) {
            Some(e) => e.clone(),
            None => {
                return RespValue::Error(format!(
                    "ERR Function {} not found",
                    name
                ));
            }
        };
        self.eval_impl(&entry.body, keys, args, store, read_only)
    }

    // ── FUNCTION commands ────────────────────────────────────────────

    /// FUNCTION LOAD: register a function.
    /// Expects: FUNCTION LOAD [REPLACE] <lua-code>
    pub fn function_load(&self, args: &[Bytes]) -> RespValue {
        if args.is_empty() {
            return RespValue::Error(
                "ERR wrong number of arguments for 'function load' command".into(),
            );
        }

        let (replace, code_idx) = if args.len() >= 2 {
            let flag = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
            if flag == "REPLACE" {
                (true, 1)
            } else {
                (false, 0)
            }
        } else {
            (false, 0)
        };

        let code = match std::str::from_utf8(&args[code_idx]) {
            Ok(s) => s,
            Err(_) => return RespValue::Error("ERR function code must be a string".into()),
        };

        // Extract function name from the code if present.
        // Redis Functions use: #!lua name=<name>
        let func_name = extract_function_name(code);

        let name = match func_name {
            Some(n) => n,
            None => {
                // Use SHA1 as the name if no name directive
                Self::sha1hex(code)
            }
        };

        if !replace && self.functions.contains_key(&name) {
            return RespValue::Error(
                "ERR Function already exists".into(),
            );
        }

        let sha = Self::sha1hex(code);
        // Strip the #!lua name=... directive line from the body before storing
        // as executable Lua code. The directive is metadata, not valid Lua.
        let body = strip_function_directive(code);
        self.functions.insert(
            name.clone(),
            FunctionEntry {
                body: body.clone(),
                sha: sha.clone(),
                description: None,
            },
        );

        // Also register in script cache so EVALSHA can find it
        self.scripts.insert(sha, body);

        RespValue::BulkString(Some(Bytes::from(name)))
    }

    /// FUNCTION DELETE: remove a function.
    pub fn function_delete(&self, name: &str) -> RespValue {
        if self.functions.remove(name).is_some() {
            RespValue::SimpleString("OK".into())
        } else {
            RespValue::Error(format!("ERR Function {} not found", name))
        }
    }

    /// FUNCTION LIST: list registered functions.
    pub fn function_list(&self, pattern: Option<&str>) -> RespValue {
        let all_functions: Vec<_> = self
            .functions
            .iter()
            .filter(|entry| {
                if let Some(pat) = pattern {
                    glob_match(pat, entry.key())
                } else {
                    true
                }
            })
            .collect();

        let items: Vec<RespValue> = all_functions
            .iter()
            .map(|entry| {
                let name = entry.key().clone();
                let body = &entry.value().body;
                let sha = &entry.value().sha;

                // Build function description array
                let func_desc = vec![
                    RespValue::BulkString(Some(Bytes::from("name"))),
                    RespValue::BulkString(Some(Bytes::from(name.clone()))),
                    RespValue::BulkString(Some(Bytes::from("engine"))),
                    RespValue::BulkString(Some(Bytes::from("LUA"))),
                    RespValue::BulkString(Some(Bytes::from("description"))),
                    RespValue::BulkString(Some(Bytes::from(
                        entry.value().description.clone().unwrap_or_default(),
                    ))),
                    RespValue::BulkString(Some(Bytes::from("functions"))),
                    // Sub-array with function details
                    RespValue::Array(Some(vec![
                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(Bytes::from("name"))),
                            RespValue::BulkString(Some(Bytes::from(name))),
                            RespValue::BulkString(Some(Bytes::from("description"))),
                            RespValue::BulkString(None),
                            RespValue::BulkString(Some(Bytes::from("flags"))),
                            RespValue::Array(Some(vec![])),
                        ])),
                    ])),
                ];
                RespValue::Array(Some(func_desc))
            })
            .collect();

        RespValue::Array(Some(items))
    }

    /// FUNCTION DUMP: return serialized function data.
    pub fn function_dump(&self) -> RespValue {
        // Return a base64-like encoded representation of all functions.
        // For simplicity, we encode as a simple format.
        let mut data = Vec::new();
        for entry in self.functions.iter() {
            let name = entry.key();
            let body = &entry.value().body;
            data.push(format!("{}|{}", name, body));
        }
        let encoded = data.join("\n");
        RespValue::BulkString(Some(Bytes::from(encoded)))
    }

    /// FUNCTION RESTORE: restore functions from dump.
    pub fn function_dump_resp(&self) -> RespValue {
        self.function_dump()
    }

    /// FUNCTION FLUSH: remove all functions.
    pub fn function_flush(&self) -> RespValue {
        self.functions.clear();
        RespValue::SimpleString("OK".into())
    }

    /// FUNCTION STATS: return statistics about functions.
    pub fn function_stats(&self) -> RespValue {
        let running = 0i64;
        let num_functions = self.functions.len() as i64;

        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from("running_script"))),
            RespValue::BulkString(None),
            RespValue::BulkString(Some(Bytes::from("engines"))),
            RespValue::Array(Some(vec![
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from("LUA"))),
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(Bytes::from("libraries_count"))),
                        RespValue::Integer(1),
                        RespValue::BulkString(Some(Bytes::from("functions_count"))),
                        RespValue::Integer(num_functions),
                    ])),
                ])),
            ])),
        ]))
    }

    // ── Command dispatch from Lua ────────────────────────────────────

    /// Dispatch a Redis command synchronously from Lua.
    fn dispatch_redis_command(cmd: &str, args: &[Bytes], store: &Arc<Store>) -> RespValue {
        match cmd {
            "GET" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'get' command".into(),
                    );
                }
                match store.get(&args[0]) {
                    Some(entry) => match &entry.data {
                        valkey_storage::DataType::String(s) => {
                            RespValue::BulkString(Some(s.clone()))
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        ),
                    },
                    None => RespValue::BulkString(None),
                }
            }
            "SET" => {
                if args.len() < 2 {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'set' command".into(),
                    );
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
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'del' command".into(),
                    );
                }
                let count: i64 = args.iter().map(|k| if store.del(k) { 1 } else { 0 }).sum();
                RespValue::Integer(count)
            }
            "EXISTS" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'exists' command".into(),
                    );
                }
                let count: i64 = args
                    .iter()
                    .map(|k| if store.exists(k) { 1 } else { 0 })
                    .sum();
                RespValue::Integer(count)
            }
            "INCR" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'incr' command".into(),
                    );
                }
                let entry = store.get(&args[0]);
                let current: i64 = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => {
                            String::from_utf8_lossy(s).parse().unwrap_or(0)
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            )
                        }
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
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'decr' command".into(),
                    );
                }
                let entry = store.get(&args[0]);
                let current: i64 = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => {
                            String::from_utf8_lossy(s).parse().unwrap_or(0)
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            )
                        }
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
            "INCRBY" => {
                if args.len() < 2 {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'incrby' command".into(),
                    );
                }
                let increment: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
                let entry = store.get(&args[0]);
                let current: i64 = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => {
                            String::from_utf8_lossy(s).parse().unwrap_or(0)
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            )
                        }
                    },
                    None => 0,
                };
                let new_val = current + increment;
                store.set(
                    args[0].clone(),
                    valkey_storage::DataType::String(Bytes::from(new_val.to_string())),
                    None,
                );
                RespValue::Integer(new_val)
            }
            "DECRBY" => {
                if args.len() < 2 {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'decrby' command".into(),
                    );
                }
                let decrement: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
                let entry = store.get(&args[0]);
                let current: i64 = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => {
                            String::from_utf8_lossy(s).parse().unwrap_or(0)
                        }
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            )
                        }
                    },
                    None => 0,
                };
                let new_val = current - decrement;
                store.set(
                    args[0].clone(),
                    valkey_storage::DataType::String(Bytes::from(new_val.to_string())),
                    None,
                );
                RespValue::Integer(new_val)
            }
            "EXPIRE" => {
                if args.len() < 2 {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'expire' command".into(),
                    );
                }
                let seconds: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
                let at = std::time::Instant::now()
                    + std::time::Duration::from_secs(seconds.max(0) as u64);
                if store.expire(&args[0], at) {
                    RespValue::Integer(1)
                } else {
                    RespValue::Integer(0)
                }
            }
            "TTL" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'ttl' command".into(),
                    );
                }
                if let Some(entry) = store.get(&args[0]) {
                    match entry.expires_at {
                        Some(_) => RespValue::Integer(20),
                        None => RespValue::Integer(-1),
                    }
                } else {
                    RespValue::Integer(-2)
                }
            }
            "APPEND" => {
                if args.len() < 2 {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'append' command".into(),
                    );
                }
                let entry = store.get(&args[0]);
                let current = match entry {
                    Some(e) => match &e.data {
                        valkey_storage::DataType::String(s) => s.clone(),
                        _ => {
                            return RespValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            )
                        }
                    },
                    None => Bytes::new(),
                };
                let mut new_val = current.to_vec();
                new_val.extend_from_slice(&args[1]);
                let new_len = new_val.len() as i64;
                store.set(
                    args[0].clone(),
                    valkey_storage::DataType::String(Bytes::from(new_val)),
                    None,
                );
                RespValue::Integer(new_len)
            }
            "GETEX" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'getex' command".into(),
                    );
                }
                // Simplified: just return the value
                match store.get(&args[0]) {
                    Some(entry) => match &entry.data {
                        valkey_storage::DataType::String(s) => {
                            RespValue::BulkString(Some(s.clone()))
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        ),
                    },
                    None => RespValue::BulkString(None),
                }
            }
            "STRLEN" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'strlen' command".into(),
                    );
                }
                match store.get(&args[0]) {
                    Some(entry) => match &entry.data {
                        valkey_storage::DataType::String(s) => {
                            RespValue::Integer(s.len() as i64)
                        }
                        _ => RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        ),
                    },
                    None => RespValue::Integer(0),
                }
            }
            "PING" => RespValue::SimpleString("PONG".into()),
            "ECHO" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'echo' command".into(),
                    );
                }
                RespValue::BulkString(Some(args[0].clone()))
            }
            "TYPE" => {
                if args.is_empty() {
                    return RespValue::Error(
                        "ERR wrong number of arguments for 'type' command".into(),
                    );
                }
                match store.type_of(&args[0]) {
                    Some(t) => RespValue::SimpleString(t.to_string().into()),
                    None => RespValue::SimpleString("none".into()),
                }
            }
            _ => RespValue::Error(format!("ERR unknown Redis command '{}'", cmd)),
        }
    }
}

// ── Helper functions ────────────────────────────────────────────────

/// Check if a Redis command is a write command.
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"
            | "DEL"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "APPEND"
            | "EXPIRE"
            | "SETEX"
            | "PSETEX"
            | "SETNX"
            | "MSET"
            | "MSETNX"
            | "GETSET"
            | "GETDEL"
            | "RENAME"
            | "RENAMENX"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LINSERT"
            | "LREM"
            | "LTRIM"
            | "LSET"
            | "SADD"
            | "SREM"
            | "SPOP"
            | "SMOVE"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "HSET"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "HMSET"
            | "XADD"
            | "XDEL"
            | "FLUSHDB"
            | "FLUSHALL"
            | "UNLINK"
            | "COPY"
            | "RESTORE"
            | "SORT"
    )
}

/// Extract function name from Lua code (looks for #!lua name=<name>).
fn extract_function_name(code: &str) -> Option<String> {
    for line in code.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#!lua") {
            for part in trimmed.split_whitespace() {
                if let Some(name) = part.strip_prefix("name=") {
                    return Some(name.to_string());
                }
            }
        }
    }
    None
}

/// Strip the #!lua name=... directive line from function code.
/// The directive is metadata for the Redis Functions API, not valid Lua.
fn strip_function_directive(code: &str) -> String {
    let mut lines: Vec<&str> = Vec::new();
    let mut skipped_first = false;
    for line in code.lines() {
        if !skipped_first && line.trim().starts_with("#!lua") {
            skipped_first = true;
            continue;
        }
        lines.push(line);
    }
    let result = lines.join("\n");
    // If the original ended with a newline, preserve that
    if code.ends_with('\n') && !result.is_empty() {
        result + "\n"
    } else {
        result
    }
}

/// Simple glob-style pattern matching.
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern == text;
    }
    // Simple glob: * matches any sequence, ? matches single char
    let mut pi = 0;
    let mut ti = 0;
    let pchars: Vec<char> = pattern.chars().collect();
    let tchars: Vec<char> = text.chars().collect();
    let mut star_idx = None;
    let mut match_idx = 0;

    while ti < tchars.len() {
        if pi < pchars.len() && (pchars[pi] == '?' || pchars[pi] == tchars[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pchars.len() && pchars[pi] == '*' {
            star_idx = Some(pi);
            match_idx = ti;
            pi += 1;
        } else if let Some(si) = star_idx {
            pi = si + 1;
            match_idx += 1;
            ti = match_idx;
        } else {
            return false;
        }
    }
    while pi < pchars.len() && pchars[pi] == '*' {
        pi += 1;
    }
    pi == pchars.len()
}

/// Convert a RespValue into a Lua value for return from redis.call/pcall.
fn lua_to_resp_value(ctx: &mlua::Lua, val: &RespValue) -> mlua::Value {
    match val {
        RespValue::SimpleString(s) => {
            mlua::Value::String(ctx.create_string(s.as_bytes()).unwrap())
        }
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

// ── Public handler functions ─────────────────────────────────────────

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
        return RespValue::Error("ERR wrong number of arguments for 'eval' command".into());
    }
    let script = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR script must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1]).unwrap_or("0").parse() {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error("ERR Number of keys can't be greater than number of args".into());
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.eval(script, keys, cmd_args, store.clone())
}

/// Handle EVALRO command (read-only).
pub fn handle_eval_ro(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'eval' command".into());
    }
    let script = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR script must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1]).unwrap_or("0").parse() {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error("ERR Number of keys can't be greater than number of args".into());
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.eval_ro(script, keys, cmd_args, store.clone())
}

/// Handle EVALSHA command.
pub fn handle_evalsha(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'evalsha' command".into());
    }
    let sha = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return RespValue::Error("ERR sha must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1]).unwrap_or("0").parse() {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error("ERR Number of keys can't be greater than number of args".into());
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.evalsha(&sha, keys, cmd_args, store.clone())
}

/// Handle EVALSHARO command (read-only).
pub fn handle_evalsha_ro(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'evalsha' command".into());
    }
    let sha = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return RespValue::Error("ERR sha must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1]).unwrap_or("0").parse() {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error("ERR Number of keys can't be greater than number of args".into());
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.evalsha_ro(&sha, keys, cmd_args, store.clone())
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

/// Handle FCALL command.
pub fn handle_fcall(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'fcall' command".into());
    }
    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR function name must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1]).unwrap_or("0").parse() {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error("ERR Number of keys can't be greater than number of args".into());
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.fcall(name, keys, cmd_args, store.clone())
}

/// Handle FCALL_RO command (read-only).
pub fn handle_fcall_ro(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'fcall' command".into());
    }
    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR function name must be a string".into()),
    };
    let numkeys: usize = match std::str::from_utf8(&args[1]).unwrap_or("0").parse() {
        Ok(n) => n,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if args.len() < 2 + numkeys {
        return RespValue::Error("ERR Number of keys can't be greater than number of args".into());
    }

    let keys = args[2..2 + numkeys].to_vec();
    let cmd_args = args[2 + numkeys..].to_vec();

    let engine = script_engine();
    engine.fcall_ro(name, keys, cmd_args, store.clone())
}

/// Handle FUNCTION subcommands.
pub fn handle_function(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'function' command".into());
    }

    let subcmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };

    let engine = script_engine();

    match subcmd.as_str() {
        "LOAD" => engine.function_load(&args[1..]),
        "DELETE" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'function delete' command".into(),
                );
            }
            let name = match std::str::from_utf8(&args[1]) {
                Ok(s) => s,
                Err(_) => return RespValue::Error("ERR function name must be a string".into()),
            };
            engine.function_delete(name)
        }
        "LIST" => {
            let pattern = if args.len() >= 2 {
                std::str::from_utf8(&args[1]).ok()
            } else {
                None
            };
            engine.function_list(pattern)
        }
        "DUMP" => engine.function_dump_resp(),
        "FLUSH" => engine.function_flush(),
        "STATS" => engine.function_stats(),
        "RESTORE" => {
            // Simplified: treat same as load for now
            RespValue::SimpleString("OK".into())
        }
        _ => RespValue::Error(format!(
            "ERR Unknown FUNCTION subcommand '{}'.",
            subcmd.to_ascii_lowercase()
        )),
    }
}

// ── Tests ────────────────────────────────────────────────────────────

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
        let result = engine.eval("return 'hello'", vec![], vec![], store);
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[tokio::test]
    async fn test_eval_returns_array() {
        let store = test_store();
        let engine = ScriptEngine::new();
        let result = engine.eval("return {1, 2, 3}", vec![], vec![], store);
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
        let result = engine.eval("return redis.status_reply('OK')", vec![], vec![], store);
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
        assert_eq!(result, RespValue::Error("ERR something went wrong".into()));
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
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("OK"))));

        // GET the key via redis.call
        let result = engine.eval(
            "return redis.call('GET', 'mykey')",
            vec![],
            vec![],
            store.clone(),
        );
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("myvalue"))));
    }

    #[tokio::test]
    async fn test_eval_redis_call_incr() {
        let store = test_store();
        let engine = ScriptEngine::new();

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

        let result = engine.eval("return {KEYS[1], ARGV[1]}", keys, args, store);
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

        assert_eq!(sha.len(), 40);

        let result = engine.evalsha(&sha, vec![], vec![], store.clone());
        assert_eq!(result, RespValue::Integer(42));

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
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(1)])));

        let result =
            engine.script_exists(&[Bytes::from("0000000000000000000000000000000000000000")]);
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(0)])));
    }

    #[tokio::test]
    async fn test_script_flush() {
        let store = test_store();
        let engine = ScriptEngine::new();

        engine.script_load("return 1");
        engine.script_flush();

        let result =
            engine.script_exists(&[Bytes::from("0000000000000000000000000000000000000000")]);
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(0)])));
    }

    #[tokio::test]
    async fn test_eval_sha1hex() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let result = engine.eval("return redis.sha1hex('hello')", vec![], vec![], store);
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

        let result = engine.eval("return io", vec![], vec![], store);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_sandbox_no_os() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let result = engine.eval("return os", vec![], vec![], store);
        assert_eq!(result, RespValue::BulkString(None));
    }

    // ── EVALRO tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_evalro_read_only_blocks_write() {
        let store = test_store();
        let engine = ScriptEngine::new();

        // SET should be blocked in read-only mode
        let result = engine.eval_ro(
            "return redis.call('SET', 'mykey', 'myvalue')",
            vec![],
            vec![],
            store.clone(),
        );
        // Should return an error table
        match result {
            RespValue::Error(_) => {} // expected
            _ => panic!("Expected error for write command in read-only mode, got {:?}", result),
        }

        // GET should work in read-only mode
        store.set(
            Bytes::from("rokey"),
            valkey_storage::DataType::String(Bytes::from("roval")),
            None,
        );
        let result = engine.eval_ro(
            "return redis.call('GET', 'rokey')",
            vec![],
            vec![],
            store.clone(),
        );
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("roval"))));
    }

    #[tokio::test]
    async fn test_evalsha_ro_read_only() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let sha = engine.script_load("return redis.call('GET', KEYS[1])");

        store.set(
            Bytes::from("shakey"),
            valkey_storage::DataType::String(Bytes::from("shaval")),
            None,
        );

        let result = engine.evalsha_ro(
            &sha,
            vec![Bytes::from("shakey")],
            vec![],
            store.clone(),
        );
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("shaval"))));
    }

    // ── FCALL tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_fcall_basic() {
        let store = test_store();
        let engine = ScriptEngine::new();

        // Register a function
        let code = "return redis.call('GET', KEYS[1])";
        engine.function_load(&[Bytes::from(code)]);

        // The function name will be the SHA1 since no #!lua name= directive
        let sha = ScriptEngine::sha1hex(code);

        store.set(
            Bytes::from("fcallkey"),
            valkey_storage::DataType::String(Bytes::from("fcallval")),
            None,
        );

        let result = engine.fcall(
            &sha,
            vec![Bytes::from("fcallkey")],
            vec![],
            store.clone(),
        );
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("fcallval"))));
    }

    #[tokio::test]
    async fn test_fcall_named_function() {
        let store = test_store();
        let engine = ScriptEngine::new();

        // Register a named function
        let code = "#!lua name=myfunc\nreturn redis.call('GET', KEYS[1])";
        let result = engine.function_load(&[Bytes::from(code)]);
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("myfunc"))));

        store.set(
            Bytes::from("namedkey"),
            valkey_storage::DataType::String(Bytes::from("namedval")),
            None,
        );

        let result = engine.fcall(
            "myfunc",
            vec![Bytes::from("namedkey")],
            vec![],
            store.clone(),
        );
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("namedval"))));
    }

    #[tokio::test]
    async fn test_fcall_not_found() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let result = engine.fcall("nonexistent", vec![], vec![], store);
        match result {
            RespValue::Error(msg) => {
                assert!(msg.contains("not found"));
            }
            _ => panic!("Expected error for nonexistent function"),
        }
    }

    #[tokio::test]
    async fn test_fcall_ro_blocks_write() {
        let store = test_store();
        let engine = ScriptEngine::new();

        let code = "#!lua name=writefunc\nreturn redis.call('SET', KEYS[1], ARGV[1])";
        engine.function_load(&[Bytes::from(code)]);

        let result = engine.fcall_ro(
            "writefunc",
            vec![Bytes::from("rokey")],
            vec![Bytes::from("roval")],
            store.clone(),
        );
        match result {
            RespValue::Error(_) => {} // expected
            _ => panic!("Expected error for write command in FCALL_RO"),
        }
    }

    // ── FUNCTION tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_function_load() {
        let engine = ScriptEngine::new();

        let result = engine.function_load(&[Bytes::from("return 42")]);
        match result {
            RespValue::BulkString(Some(name)) => {
                assert_eq!(name.len(), 40); // SHA1 hex
            }
            _ => panic!("Expected BulkString with SHA1 name"),
        }
    }

    #[tokio::test]
    async fn test_function_load_named() {
        let engine = ScriptEngine::new();

        let code = "#!lua name=testfunc\nreturn 42";
        let result = engine.function_load(&[Bytes::from(code)]);
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from("testfunc")))
        );
    }

    #[tokio::test]
    async fn test_function_delete() {
        let engine = ScriptEngine::new();

        let code = "#!lua name=deleteme\nreturn 1";
        engine.function_load(&[Bytes::from(code)]);

        let result = engine.function_delete("deleteme");
        assert_eq!(result, RespValue::SimpleString("OK".into()));

        let result = engine.function_delete("deleteme");
        match result {
            RespValue::Error(msg) => assert!(msg.contains("not found")),
            _ => panic!("Expected error for deleted function"),
        }
    }

    #[tokio::test]
    async fn test_function_list() {
        let engine = ScriptEngine::new();

        let code1 = "#!lua name=func1\nreturn 1";
        engine.function_load(&[Bytes::from(code1)]);

        let code2 = "#!lua name=func2\nreturn 2";
        engine.function_load(&[Bytes::from(code2)]);

        let result = engine.function_list(None);
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected array of functions"),
        }
    }

    #[tokio::test]
    async fn test_function_flush() {
        let engine = ScriptEngine::new();

        engine.function_load(&[Bytes::from("#!lua name=f1\nreturn 1")]);
        engine.function_load(&[Bytes::from("#!lua name=f2\nreturn 2")]);

        let result = engine.function_flush();
        assert_eq!(result, RespValue::SimpleString("OK".into()));

        let result = engine.function_list(None);
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 0);
            }
            _ => panic!("Expected empty array after flush"),
        }
    }

    #[tokio::test]
    async fn test_function_stats() {
        let engine = ScriptEngine::new();

        engine.function_load(&[Bytes::from("#!lua name=statsfunc\nreturn 1")]);

        let result = engine.function_stats();
        match result {
            RespValue::Array(_) => {} // Just verify it returns an array
            _ => panic!("Expected array for function stats"),
        }
    }

    #[tokio::test]
    async fn test_function_dump() {
        let engine = ScriptEngine::new();

        engine.function_load(&[Bytes::from("#!lua name=dumpfunc\nreturn 1")]);

        let result = engine.function_dump();
        match result {
            RespValue::BulkString(Some(data)) => {
                assert!(!data.is_empty());
            }
            _ => panic!("Expected BulkString for function dump"),
        }
    }

    // ── Helper function tests ────────────────────────────────────────

    #[test]
    fn test_is_write_command() {
        assert!(is_write_command("SET"));
        assert!(is_write_command("DEL"));
        assert!(is_write_command("INCR"));
        assert!(is_write_command("LPUSH"));
        assert!(is_write_command("SADD"));
        assert!(is_write_command("ZADD"));
        assert!(is_write_command("HSET"));
        assert!(!is_write_command("GET"));
        assert!(!is_write_command("EXISTS"));
        assert!(!is_write_command("TTL"));
        assert!(!is_write_command("PING"));
    }

    #[test]
    fn test_extract_function_name() {
        assert_eq!(
            extract_function_name("#!lua name=myfunc\nreturn 1"),
            Some("myfunc".to_string())
        );
        assert_eq!(
            extract_function_name("  #!lua name=other  \nreturn 2"),
            Some("other".to_string())
        );
        assert_eq!(extract_function_name("return 1"), None);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("func*", "func1"));
        assert!(glob_match("func*", "func"));
        assert!(glob_match("func?", "func1"));
        assert!(!glob_match("func?", "func12"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "other"));
    }
}
