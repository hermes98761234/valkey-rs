//! FFI types matching the Redis Modules C API.

use std::os::raw::{c_char, c_int};

pub const REDISMODULE_OK: c_int = 0;
pub const REDISMODULE_ERR: c_int = 1;
pub const REDISMODULE_APIVER_1: c_int = 1;

/// Opaque module context. In our implementation this is cast to/from ModuleCtxInner.
#[repr(C)]
pub struct RedisModuleCtx {
    _opaque: [u8; 0],
}

/// An opaque Redis string (used for module string args).
#[repr(C)]
pub struct RedisModuleString {
    _opaque: [u8; 0],
}

/// An opaque Redis key handle.
#[repr(C)]
#[allow(dead_code)]
pub struct RedisModuleKey {
    _opaque: [u8; 0],
}

/// Command function signature: (ctx, argv, argc) -> int
pub type RedisModuleCmdFunc = unsafe extern "C" fn(
    ctx: *mut RedisModuleCtx,
    argv: *mut *mut RedisModuleString,
    argc: c_int,
) -> c_int;

/// Type of the `RedisModule_OnLoad` entry point that every module must export.
pub type OnLoadFn = unsafe extern "C" fn(
    ctx: *mut RedisModuleCtx,
    argv: *mut *mut RedisModuleString,
    argc: c_int,
) -> c_int;

// ---------------------------------------------------------------------------
// Re-export helpers
// ---------------------------------------------------------------------------

/// Read a C string and convert to a Rust String (lossy).
pub unsafe fn cstr_to_string(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
}
