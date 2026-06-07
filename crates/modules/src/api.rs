//! C ABI functions exported for Redis module compatibility.

use crate::ffi::*;
use bytes::Bytes;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_longlong};
use std::sync::{Arc, Mutex, OnceLock};
use valkey_proto::RespValue;
use valkey_storage::Store;

// ---------------------------------------------------------------------------
// Global command function registry
// (separate from the module registry to avoid lock contention)
// ---------------------------------------------------------------------------

static COMMANDS: OnceLock<Mutex<HashMap<String, RedisModuleCmdFunc>>> = OnceLock::new();

fn commands() -> &'static Mutex<HashMap<String, RedisModuleCmdFunc>> {
    COMMANDS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn get_registered_command(name: &str) -> Option<RedisModuleCmdFunc> {
    commands().lock().ok()?.get(name).copied()
}

// ---------------------------------------------------------------------------
// Internal context struct (passed as *mut RedisModuleCtx)
// ---------------------------------------------------------------------------

pub struct ModuleCtxInner {
    #[allow(dead_code)]
    pub store: Arc<Store>,
    #[allow(dead_code)]
    pub args: Vec<Bytes>,

    // Registration state (used during MODULE LOAD)
    pub mod_name: String,
    pub mod_version: i32,
    pub registered_cmds: Vec<String>,

    // Reply accumulator (used during command execution)
    pub reply: Option<RespValue>,
    /// Stack for nested arrays being built
    pub array_stack: Vec<Vec<RespValue>>,
}

impl ModuleCtxInner {
    pub fn new_load(store: Arc<Store>) -> Self {
        Self {
            store,
            args: vec![],
            mod_name: String::new(),
            mod_version: 0,
            registered_cmds: vec![],
            reply: None,
            array_stack: vec![],
        }
    }

    pub fn new_call(store: Arc<Store>, args: Vec<Bytes>) -> Self {
        Self {
            store,
            args,
            mod_name: String::new(),
            mod_version: 0,
            registered_cmds: vec![],
            reply: None,
            array_stack: vec![],
        }
    }

    pub fn take_registration(self) -> (String, i32, Vec<String>) {
        (self.mod_name, self.mod_version, self.registered_cmds)
    }

    pub fn take_reply(self) -> Option<RespValue> {
        self.reply
    }

    fn push_reply(&mut self, v: RespValue) {
        if let Some(arr) = self.array_stack.last_mut() {
            arr.push(v);
        } else {
            self.reply = Some(v);
        }
    }
}

// ---------------------------------------------------------------------------
// Internal wrapper for RedisModuleString
// ---------------------------------------------------------------------------

pub struct ModuleString(pub Bytes);

unsafe fn ctx(ptr: *mut RedisModuleCtx) -> &'static mut ModuleCtxInner {
    &mut *(ptr as *mut ModuleCtxInner)
}

unsafe fn module_string(ptr: *const RedisModuleString) -> &'static ModuleString {
    &*(ptr as *const ModuleString)
}

// ---------------------------------------------------------------------------
// RedisModule_Init
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RedisModule_Init(
    ctx_ptr: *mut RedisModuleCtx,
    name: *const c_char,
    version: c_int,
    apiver: c_int,
) -> c_int {
    if ctx_ptr.is_null() || name.is_null() {
        return REDISMODULE_ERR;
    }
    if apiver != REDISMODULE_APIVER_1 {
        return REDISMODULE_ERR;
    }
    let ctx = ctx(ctx_ptr);
    ctx.mod_name = cstr_to_string(name);
    ctx.mod_version = version;
    REDISMODULE_OK
}

// ---------------------------------------------------------------------------
// RedisModule_CreateCommand
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RedisModule_CreateCommand(
    ctx_ptr: *mut RedisModuleCtx,
    name: *const c_char,
    cmdfunc: RedisModuleCmdFunc,
    _strflags: *const c_char,
    _firstkey: c_int,
    _lastkey: c_int,
    _keystep: c_int,
) -> c_int {
    if ctx_ptr.is_null() || name.is_null() {
        return REDISMODULE_ERR;
    }
    let ctx = ctx(ctx_ptr);
    let cmd_name = cstr_to_string(name).to_ascii_uppercase();
    ctx.registered_cmds.push(cmd_name.clone());

    let mut cmds = commands().lock().unwrap();
    cmds.insert(cmd_name, cmdfunc);
    drop(cmds);

    REDISMODULE_OK
}

// ---------------------------------------------------------------------------
// Reply functions
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplyWithBulkString(
    ctx_ptr: *mut RedisModuleCtx,
    buf: *const c_char,
    len: usize,
) -> c_int {
    if ctx_ptr.is_null() || buf.is_null() {
        return REDISMODULE_ERR;
    }
    let slice = std::slice::from_raw_parts(buf, len);
    let bytes = Bytes::copy_from_slice(slice);
    let ctx = ctx(ctx_ptr);
    ctx.push_reply(RespValue::BulkString(Some(bytes)));
    REDISMODULE_OK
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplyWithLongLong(
    ctx_ptr: *mut RedisModuleCtx,
    ll: c_longlong,
) -> c_int {
    if ctx_ptr.is_null() {
        return REDISMODULE_ERR;
    }
    let ctx = ctx(ctx_ptr);
    ctx.push_reply(RespValue::Integer(ll));
    REDISMODULE_OK
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplyWithArray(
    ctx_ptr: *mut RedisModuleCtx,
    len: c_longlong,
) -> c_int {
    if ctx_ptr.is_null() {
        return REDISMODULE_ERR;
    }
    let ctx = ctx(ctx_ptr);
    // Push a new array accumulator
    ctx.array_stack
        .push(Vec::with_capacity(len.max(0) as usize));
    REDISMODULE_OK
}

/// Call after filling all array elements to finalize the array.
#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplySetArrayLength(
    ctx_ptr: *mut RedisModuleCtx,
    _len: c_longlong,
) -> c_int {
    if ctx_ptr.is_null() {
        return REDISMODULE_ERR;
    }
    let ctx = ctx(ctx_ptr);
    if let Some(arr) = ctx.array_stack.pop() {
        let v = RespValue::Array(Some(arr));
        ctx.push_reply(v);
    }
    REDISMODULE_OK
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplyWithError(
    ctx_ptr: *mut RedisModuleCtx,
    err: *const c_char,
) -> c_int {
    if ctx_ptr.is_null() {
        return REDISMODULE_ERR;
    }
    let msg = if err.is_null() {
        "ERR module error".to_string()
    } else {
        cstr_to_string(err)
    };
    let ctx = ctx(ctx_ptr);
    ctx.push_reply(RespValue::Error(msg));
    REDISMODULE_OK
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplyWithNull(ctx_ptr: *mut RedisModuleCtx) -> c_int {
    if ctx_ptr.is_null() {
        return REDISMODULE_ERR;
    }
    let ctx = ctx(ctx_ptr);
    ctx.push_reply(RespValue::BulkString(None));
    REDISMODULE_OK
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_ReplyWithSimpleString(
    ctx_ptr: *mut RedisModuleCtx,
    msg: *const c_char,
) -> c_int {
    if ctx_ptr.is_null() {
        return REDISMODULE_ERR;
    }
    let s = if msg.is_null() {
        "OK".to_string()
    } else {
        cstr_to_string(msg)
    };
    let ctx = ctx(ctx_ptr);
    ctx.push_reply(RespValue::SimpleString(s));
    REDISMODULE_OK
}

// ---------------------------------------------------------------------------
// String argument helpers
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RedisModule_StringPtrLen(
    str_ptr: *const RedisModuleString,
    lenptr: *mut usize,
) -> *const c_char {
    if str_ptr.is_null() {
        if !lenptr.is_null() {
            *lenptr = 0;
        }
        return std::ptr::null();
    }
    let s = module_string(str_ptr);
    if !lenptr.is_null() {
        *lenptr = s.0.len();
    }
    s.0.as_ptr() as *const c_char
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_StringToLongLong(
    str_ptr: *const RedisModuleString,
    ll: *mut c_longlong,
) -> c_int {
    if str_ptr.is_null() || ll.is_null() {
        return REDISMODULE_ERR;
    }
    let s = module_string(str_ptr);
    if let Ok(text) = std::str::from_utf8(&s.0) {
        if let Ok(v) = text.parse::<c_longlong>() {
            *ll = v;
            return REDISMODULE_OK;
        }
    }
    REDISMODULE_ERR
}

// ---------------------------------------------------------------------------
// Misc API
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RedisModule_AutoMemory(_ctx: *mut RedisModuleCtx) {
    // no-op: Rust manages memory automatically
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_GetSelectedDb(_ctx_ptr: *mut RedisModuleCtx) -> c_int {
    0 // We don't support multiple DBs in this implementation
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_SelectDb(_ctx: *mut RedisModuleCtx, _db: c_int) -> c_int {
    REDISMODULE_OK
}

/// Register module (re-exported for completeness)
pub struct ModuleRegistry;

impl ModuleRegistry {
    pub fn get_command(name: &str) -> Option<RedisModuleCmdFunc> {
        get_registered_command(name)
    }
}
