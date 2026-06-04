use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

/// Handle all String commands.
/// `args` does NOT include the command name — already stripped by dispatcher.
pub async fn handle(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".into());
    }

    let cmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };

    match cmd.as_str() {
        "GET" => cmd_get(&args[1..], store).await,
        "SET" => cmd_set(&args[1..], store).await,
        "DEL" => cmd_del(&args[1..], store).await,
        "GETSET" => cmd_getset(&args[1..], store).await,
        "MGET" => cmd_mget(&args[1..], store).await,
        "MSET" => cmd_mset(&args[1..], store).await,
        "MSETNX" => cmd_msetnx(&args[1..], store).await,
        "INCR" => cmd_incr(&args[1..], store).await,
        "DECR" => cmd_decr(&args[1..], store).await,
        "INCRBY" => cmd_incrby(&args[1..], store).await,
        "DECRBY" => cmd_decrby(&args[1..], store).await,
        "INCRBYFLOAT" => cmd_incrbyfloat(&args[1..], store).await,
        "APPEND" => cmd_append(&args[1..], store).await,
        "STRLEN" => cmd_strlen(&args[1..], store).await,
        "GETRANGE" => cmd_getrange(&args[1..], store).await,
        "SETRANGE" => cmd_setrange(&args[1..], store).await,
        "SETNX" => cmd_setnx(&args[1..], store).await,
        "SETEX" => cmd_setex(&args[1..], store).await,
        "PSETEX" => cmd_psetex(&args[1..], store).await,
        "GETEX" => cmd_getex(&args[1..], store).await,
        "GETDEL" => cmd_getdel(&args[1..], store).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", cmd)),
    }
}

// -- helpers ----------------------------------------------------------------

fn bull(bytes: Bytes) -> RespValue {
    RespValue::BulkString(Some(bytes))
}

fn null_bulk() -> RespValue {
    RespValue::BulkString(None)
}

/// Get a reference to the underlying DashMap via Deref.
fn dashmap(store: &Arc<Store>) -> &dashmap::DashMap<Bytes, Entry> {
    store
}

// ---------------------------------------------------------------------------
// GET
// ---------------------------------------------------------------------------
async fn cmd_get(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'get' command".into());
    }
    match store.get(&args[0]) {
        Some(entry) => match &entry.data {
            DataType::String(s) => bull(s.clone()),
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => null_bulk(),
    }
}

// ---------------------------------------------------------------------------
// SET key value [EX seconds] [PX ms] [NX|XX] [GET] [KEEPTTL]
// ---------------------------------------------------------------------------
async fn cmd_set(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'set' command".into());
    }

    let key = args[0].clone();
    let value = args[1].clone();
    let mut idx = 2;
    let mut ttl: Option<Duration> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut _keepttl = false;

    while idx < args.len() {
        let opt = match std::str::from_utf8(&args[idx]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        match opt.as_str() {
            "EX" => {
                if idx + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                idx += 1;
                let secs: u64 = match std::str::from_utf8(&args[idx]).unwrap_or("").parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        );
                    }
                };
                ttl = Some(Duration::from_secs(secs));
            }
            "PX" => {
                if idx + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                idx += 1;
                let ms: u64 = match std::str::from_utf8(&args[idx]).unwrap_or("").parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        );
                    }
                };
                ttl = Some(Duration::from_millis(ms));
            }
            "EXAT" => {
                if idx + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                idx += 1;
                let ts: u64 = match std::str::from_utf8(&args[idx]).unwrap_or("").parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        );
                    }
                };
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if ts > now {
                    ttl = Some(Duration::from_secs(ts - now));
                } else {
                    store.del(&key);
                    return null_bulk();
                }
            }
            "PXAT" => {
                if idx + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                idx += 1;
                let ts_ms: u64 = match std::str::from_utf8(&args[idx]).unwrap_or("").parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        );
                    }
                };
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if ts_ms > now_ms {
                    ttl = Some(Duration::from_millis(ts_ms - now_ms));
                } else {
                    store.del(&key);
                    return null_bulk();
                }
            }
            "KEEPTTL" => _keepttl = true,
            "NX" => nx = true,
            "XX" => xx = true,
            "GET" => get = true,
            _ => return RespValue::Error("ERR syntax error".into()),
        }
        idx += 1;
    }

    let key_exists = store.exists(&key);

    if nx && key_exists {
        if get {
            return match store.get(&key) {
                Some(entry) => match &entry.data {
                    DataType::String(s) => bull(s.clone()),
                    _ => RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                },
                None => null_bulk(),
            };
        }
        return null_bulk();
    }

    if xx && !key_exists {
        return null_bulk();
    }

    let old_value = if get {
        store.get(&key).and_then(|e| match &e.data {
            DataType::String(s) => Some(s.clone()),
            _ => None,
        })
    } else {
        None
    };

    store.set(key, DataType::String(value), ttl);

    if get {
        match old_value {
            Some(v) => bull(v),
            None => null_bulk(),
        }
    } else {
        RespValue::SimpleString("OK".into())
    }
}

// ---------------------------------------------------------------------------
// DEL
// ---------------------------------------------------------------------------
async fn cmd_del(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'del' command".into());
    }
    let mut count = 0i64;
    for key in args {
        if store.del(key) {
            count += 1;
        }
    }
    RespValue::Integer(count)
}

// ---------------------------------------------------------------------------
// GETSET
// ---------------------------------------------------------------------------
async fn cmd_getset(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'getset' command".into());
    }
    let key = args[0].clone();
    let value = args[1].clone();

    let old = match store.get(&key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => bull(s.clone()),
            _ => {
                store.set(key, DataType::String(value), None);
                return RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        },
        None => null_bulk(),
    };

    store.set(key, DataType::String(value), None);
    old
}

// ---------------------------------------------------------------------------
// MGET
// ---------------------------------------------------------------------------
async fn cmd_mget(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'mget' command".into());
    }
    let mut result = Vec::with_capacity(args.len());
    for key in args {
        match store.get(key) {
            Some(entry) => match &entry.data {
                DataType::String(s) => result.push(bull(s.clone())),
                _ => result.push(null_bulk()),
            },
            None => result.push(null_bulk()),
        }
    }
    RespValue::Array(Some(result))
}

// ---------------------------------------------------------------------------
// MSET
// ---------------------------------------------------------------------------
async fn cmd_mset(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() || args.len() % 2 != 0 {
        return RespValue::Error("ERR wrong number of arguments for 'mset' command".into());
    }
    for chunk in args.chunks(2) {
        store.set(chunk[0].clone(), DataType::String(chunk[1].clone()), None);
    }
    RespValue::SimpleString("OK".into())
}

// ---------------------------------------------------------------------------
// MSETNX
// ---------------------------------------------------------------------------
async fn cmd_msetnx(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() || args.len() % 2 != 0 {
        return RespValue::Error("ERR wrong number of arguments for 'msetnx' command".into());
    }

    for chunk in args.chunks(2) {
        if store.exists(&chunk[0]) {
            return RespValue::Integer(0);
        }
    }

    for chunk in args.chunks(2) {
        store.set(chunk[0].clone(), DataType::String(chunk[1].clone()), None);
    }
    RespValue::Integer(1)
}

// ---------------------------------------------------------------------------
// INCR
// ---------------------------------------------------------------------------
async fn cmd_incr(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'incr' command".into());
    }
    cmd_incrby_internal(&args[0], 1, store).await
}

// ---------------------------------------------------------------------------
// DECR
// ---------------------------------------------------------------------------
async fn cmd_decr(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'decr' command".into());
    }
    cmd_incrby_internal(&args[0], -1, store).await
}

// ---------------------------------------------------------------------------
// INCRBY
// ---------------------------------------------------------------------------
async fn cmd_incrby(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'incrby' command".into());
    }
    let increment: i64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };
    cmd_incrby_internal(&args[0], increment, store).await
}

// ---------------------------------------------------------------------------
// DECRBY
// ---------------------------------------------------------------------------
async fn cmd_decrby(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'decrby' command".into());
    }
    let decrement: i64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };
    cmd_incrby_internal(&args[0], -decrement, store).await
}

// ---------------------------------------------------------------------------
// INCRBYFLOAT
// ---------------------------------------------------------------------------
async fn cmd_incrbyfloat(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'incrbyfloat' command".into(),
        );
    }
    let key = args[0].clone();
    let increment: f64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not a valid float".into()),
    };

    let dm = dashmap(store);
    match dm.get_mut(&key) {
        Some(mut entry) => match &entry.data {
            DataType::String(s) => {
                let current: f64 = match std::str::from_utf8(s).unwrap_or("").parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error("ERR value is not a valid float".into());
                    }
                };
                let new_val = current + increment;
                let new_bytes = Bytes::from(new_val.to_string());
                let result = new_bytes.clone();
                entry.data = DataType::String(new_bytes);
                bull(result)
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => {
            let new_bytes = Bytes::from(increment.to_string());
            let result = new_bytes.clone();
            dm.insert(key, Entry::new(DataType::String(new_bytes), None));
            bull(result)
        }
    }
}

// ---------------------------------------------------------------------------
// APPEND
// ---------------------------------------------------------------------------
async fn cmd_append(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'append' command".into());
    }
    let key = args[0].clone();
    let value = args[1].clone();

    let dm = dashmap(store);
    match dm.get_mut(&key) {
        Some(mut entry) => match &entry.data {
            DataType::String(s) => {
                let mut combined = Vec::with_capacity(s.len() + value.len());
                combined.extend_from_slice(s);
                combined.extend_from_slice(&value);
                let new_len = combined.len() as i64;
                entry.data = DataType::String(Bytes::from(combined));
                RespValue::Integer(new_len)
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => {
            let new_len = value.len() as i64;
            dm.insert(key, Entry::new(DataType::String(value), None));
            RespValue::Integer(new_len)
        }
    }
}

// ---------------------------------------------------------------------------
// STRLEN
// ---------------------------------------------------------------------------
async fn cmd_strlen(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'strlen' command".into());
    }
    match store.get(&args[0]) {
        Some(entry) => match &entry.data {
            DataType::String(s) => RespValue::Integer(s.len() as i64),
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => RespValue::Integer(0),
    }
}

// ---------------------------------------------------------------------------
// GETRANGE
// ---------------------------------------------------------------------------
async fn cmd_getrange(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'getrange' command".into());
    }
    let start: i64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };
    let end: i64 = match std::str::from_utf8(&args[2]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };

    match store.get(&args[0]) {
        Some(entry) => match &entry.data {
            DataType::String(s) => {
                let len = s.len() as i64;
                if len == 0 {
                    return bull(Bytes::new());
                }
                let start_idx = normalize_index(start, len);
                let end_idx = normalize_index(end, len);
                if start_idx > end_idx || start_idx >= len {
                    return bull(Bytes::new());
                }
                let end_idx = end_idx.min(len - 1);
                bull(Bytes::copy_from_slice(
                    &s[start_idx as usize..=end_idx as usize],
                ))
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => bull(Bytes::new()),
    }
}

// ---------------------------------------------------------------------------
// SETRANGE
// ---------------------------------------------------------------------------
async fn cmd_setrange(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setrange' command".into());
    }
    let key = args[0].clone();
    let offset: usize = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };
    let value = args[2].clone();

    if offset + value.len() > 512 * 1024 * 1024 {
        return RespValue::Error("ERR string exceeds maximum allowed size".into());
    }

    let dm = dashmap(store);
    match dm.get_mut(&key) {
        Some(mut entry) => match &entry.data {
            DataType::String(s) => {
                let new_len = (offset + value.len()).max(s.len());
                let mut buf = vec![0u8; new_len];
                buf[..s.len()].copy_from_slice(s);
                buf[offset..offset + value.len()].copy_from_slice(&value);
                let result_len = buf.len() as i64;
                entry.data = DataType::String(Bytes::from(buf));
                RespValue::Integer(result_len)
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => {
            let new_len = offset + value.len();
            let mut buf = vec![0u8; new_len];
            buf[offset..].copy_from_slice(&value);
            dm.insert(
                key,
                Entry::new(DataType::String(Bytes::from(buf)), None),
            );
            RespValue::Integer(new_len as i64)
        }
    }
}

// ---------------------------------------------------------------------------
// SETNX
// ---------------------------------------------------------------------------
async fn cmd_setnx(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'setnx' command".into());
    }
    let key = args[0].clone();
    let value = args[1].clone();

    if store.exists(&key) {
        return RespValue::Integer(0);
    }
    store.set(key, DataType::String(value), None);
    RespValue::Integer(1)
}

// ---------------------------------------------------------------------------
// SETEX
// ---------------------------------------------------------------------------
async fn cmd_setex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setex' command".into());
    }
    let key = args[0].clone();
    let seconds: u64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };
    let value = args[2].clone();

    store.set(
        key,
        DataType::String(value),
        Some(Duration::from_secs(seconds)),
    );
    RespValue::SimpleString("OK".into())
}

// ---------------------------------------------------------------------------
// PSETEX
// ---------------------------------------------------------------------------
async fn cmd_psetex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'psetex' command".into());
    }
    let key = args[0].clone();
    let ms: u64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };
    let value = args[2].clone();

    store.set(
        key,
        DataType::String(value),
        Some(Duration::from_millis(ms)),
    );
    RespValue::SimpleString("OK".into())
}

// ---------------------------------------------------------------------------
// GETEX key [PERSIST]
// ---------------------------------------------------------------------------
async fn cmd_getex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::Error("ERR wrong number of arguments for 'getex' command".into());
    }
    let key = &args[0];

    match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::String(_) => {}
            _ => {
                return RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                );
            }
        },
        None => return null_bulk(),
    }

    if args.len() == 2 {
        let opt = match std::str::from_utf8(&args[1]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        match opt.as_str() {
            "PERSIST" => {
                let dm = dashmap(store);
                if let Some(mut entry) = dm.get_mut(key) {
                    entry.expires_at = None;
                }
            }
            _ => return RespValue::Error("ERR syntax error".into()),
        }
    }

    match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => bull(s.clone()),
            _ => unreachable!(),
        },
        None => null_bulk(),
    }
}

// ---------------------------------------------------------------------------
// GETDEL
// ---------------------------------------------------------------------------
async fn cmd_getdel(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'getdel' command".into());
    }
    let key = args[0].clone();

    match store.get(&key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => {
                let val = s.clone();
                store.del(&key);
                bull(val)
            }
            _ => {
                store.del(&key);
                RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )
            }
        },
        None => {
            store.del(&key);
            null_bulk()
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

async fn cmd_incrby_internal(key: &Bytes, increment: i64, store: &Arc<Store>) -> RespValue {
    let dm = dashmap(store);
    match dm.get_mut(key) {
        Some(mut entry) => match &entry.data {
            DataType::String(s) => {
                let current: i64 = match std::str::from_utf8(s).unwrap_or("").parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        );
                    }
                };
                let new_val = current + increment;
                entry.data = DataType::String(Bytes::from(new_val.to_string()));
                RespValue::Integer(new_val)
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => {
            dm.insert(
                key.clone(),
                Entry::new(DataType::String(Bytes::from(increment.to_string())), None),
            );
            RespValue::Integer(increment)
        }
    }
}

fn normalize_index(index: i64, len: i64) -> i64 {
    if index < 0 {
        let idx = len + index;
        if idx < 0 { 0 } else { idx }
    } else {
        index
    }
}
