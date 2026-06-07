mod api;
mod ffi;

pub use api::ModuleRegistry;
pub use ffi::{RedisModuleCmdFunc, RedisModuleCtx};

use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use valkey_proto::RespValue;
use valkey_storage::Store;

// ---------------------------------------------------------------------------
// Global module registry
// ---------------------------------------------------------------------------

/// Information about a loaded module.
#[derive(Debug)]
pub struct LoadedModule {
    pub name: String,
    pub version: i32,
    /// Registered commands: name → (cmd_func, flags)
    pub commands: Vec<String>,
    /// Keep the library loaded so its symbols remain valid.
    _lib: libloading::Library,
}

pub struct Registry {
    pub modules: HashMap<String, LoadedModule>,
    /// Command name (uppercase) → module name
    pub command_map: HashMap<String, String>,
}

impl Registry {
    fn new() -> Self {
        Self {
            modules: HashMap::new(),
            command_map: HashMap::new(),
        }
    }
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

pub fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
}

// ---------------------------------------------------------------------------
// MODULE LOAD / UNLOAD / LIST
// ---------------------------------------------------------------------------

pub fn module_load(store: &Arc<Store>, path: &str, _args: &[&str]) -> RespValue {
    let path = Path::new(path);
    if !path.exists() {
        return RespValue::Error(format!(
            "ERR Error loading shared library {}: No such file",
            path.display()
        ));
    }

    let lib = unsafe {
        match libloading::Library::new(path) {
            Ok(l) => l,
            Err(e) => return RespValue::Error(format!("ERR Error loading module: {}", e)),
        }
    };

    // Get OnLoad symbol
    let on_load: libloading::Symbol<ffi::OnLoadFn> = unsafe {
        match lib.get(b"RedisModule_OnLoad\0") {
            Ok(s) => s,
            Err(e) => {
                return RespValue::Error(format!("ERR Module has no RedisModule_OnLoad: {}", e))
            }
        }
    };

    // Set up context for the load call
    let ctx = api::ModuleCtxInner::new_load(store.clone());
    let ctx_ptr = Box::into_raw(Box::new(ctx)) as *mut ffi::RedisModuleCtx;

    let rc = unsafe { on_load(ctx_ptr, std::ptr::null_mut(), 0) };

    // Retrieve results from context
    let ctx = unsafe { Box::from_raw(ctx_ptr as *mut api::ModuleCtxInner) };

    if rc != ffi::REDISMODULE_OK {
        return RespValue::Error("ERR Module initialization failed".into());
    }

    let (mod_name, mod_version, registered_cmds) = ctx.take_registration();

    if mod_name.is_empty() {
        return RespValue::Error("ERR Module did not call RedisModule_Init".into());
    }

    let mut reg = registry().lock().unwrap();
    if reg.modules.contains_key(&mod_name) {
        return RespValue::Error(format!("ERR Module {} already loaded", mod_name));
    }

    for cmd in &registered_cmds {
        reg.command_map
            .insert(cmd.to_ascii_uppercase(), mod_name.clone());
    }

    reg.modules.insert(
        mod_name.clone(),
        LoadedModule {
            name: mod_name,
            version: mod_version,
            commands: registered_cmds,
            _lib: lib,
        },
    );

    RespValue::SimpleString("OK".into())
}

pub fn module_unload(name: &str) -> RespValue {
    let mut reg = registry().lock().unwrap();
    match reg.modules.remove(name) {
        Some(m) => {
            for cmd in &m.commands {
                reg.command_map.remove(&cmd.to_ascii_uppercase());
            }
            RespValue::SimpleString("OK".into())
        }
        None => RespValue::Error(
            "ERR Error unloading module: no such module with that name".to_string(),
        ),
    }
}

pub fn module_list() -> RespValue {
    let reg = registry().lock().unwrap();
    let items: Vec<RespValue> = reg
        .modules
        .values()
        .map(|m| {
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from("name"))),
                RespValue::BulkString(Some(Bytes::from(m.name.clone()))),
                RespValue::BulkString(Some(Bytes::from("ver"))),
                RespValue::Integer(m.version as i64),
            ]))
        })
        .collect();
    RespValue::Array(Some(items))
}

/// Check if a command is registered by a module and call it.
pub fn module_call(store: &Arc<Store>, cmd: &[Bytes]) -> Option<RespValue> {
    if cmd.is_empty() {
        return None;
    }
    let name = std::str::from_utf8(&cmd[0]).ok()?.to_ascii_uppercase();

    // Get the command function pointer
    let cmd_fn = {
        let reg = registry().lock().unwrap();
        let _mod_name = reg.command_map.get(&name)?;
        api::get_registered_command(&name)?
    };

    let args: Vec<Bytes> = cmd[1..].to_vec();
    let ctx = api::ModuleCtxInner::new_call(store.clone(), args.clone());
    let ctx_ptr = Box::into_raw(Box::new(ctx)) as *mut ffi::RedisModuleCtx;

    // Convert args to RedisModuleString pointers
    let mut arg_strings: Vec<*mut ffi::RedisModuleString> = args
        .iter()
        .map(|a| {
            Box::into_raw(Box::new(api::ModuleString(a.clone()))) as *mut ffi::RedisModuleString
        })
        .collect();

    let rc = unsafe {
        cmd_fn(
            ctx_ptr,
            arg_strings.as_mut_ptr(),
            arg_strings.len() as std::os::raw::c_int,
        )
    };

    // Free arg strings
    for ptr in arg_strings {
        unsafe { drop(Box::from_raw(ptr as *mut api::ModuleString)) };
    }

    let ctx = unsafe { Box::from_raw(ctx_ptr as *mut api::ModuleCtxInner) };
    let reply = ctx.take_reply();

    if rc != ffi::REDISMODULE_OK {
        Some(RespValue::Error("ERR Module command returned error".into()))
    } else {
        Some(reply.unwrap_or_else(|| RespValue::SimpleString("OK".into())))
    }
}

// ---------------------------------------------------------------------------
// MODULE command handler
// ---------------------------------------------------------------------------

pub fn handle_module_cmd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'module' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "LOAD" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'module|load' command".into(),
                );
            }
            let path = match std::str::from_utf8(&args[1]) {
                Ok(p) => p,
                Err(_) => return RespValue::Error("ERR invalid path".into()),
            };
            let extra: Vec<&str> = args[2..]
                .iter()
                .filter_map(|a| std::str::from_utf8(a).ok())
                .collect();
            module_load(store, path, &extra)
        }
        "UNLOAD" => {
            if args.len() != 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'module|unload' command".into(),
                );
            }
            let name = match std::str::from_utf8(&args[1]) {
                Ok(n) => n,
                Err(_) => return RespValue::Error("ERR invalid module name".into()),
            };
            module_unload(name)
        }
        "LIST" => module_list(),
        _ => RespValue::Error(format!("ERR unknown subcommand '{}' for 'module'", sub)),
    }
}
