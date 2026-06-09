use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

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
        "LCS" => cmd_lcs(&args[1..], store).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", cmd)),
    }
}

fn bull(b: Bytes) -> RespValue {
    RespValue::BulkString(Some(b))
}
fn null_bulk() -> RespValue {
    RespValue::BulkString(None)
}
fn dashmap(s: &Arc<Store>) -> &dashmap::DashMap<Bytes, Entry> {
    s
}

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
                        )
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
                        )
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
                        )
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
                        )
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
            "KEEPTTL" => ttl = None,
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
    if let Err(e) = store.maybe_evict() {
        return RespValue::Error(e);
    }
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

async fn cmd_mset(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() || args.len() % 2 != 0 {
        return RespValue::Error("ERR wrong number of arguments for 'mset' command".into());
    }
    for chunk in args.chunks(2) {
        store.set(chunk[0].clone(), DataType::String(chunk[1].clone()), None);
    }
    RespValue::SimpleString("OK".into())
}

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

async fn cmd_incr(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'incr' command".into());
    }
    cmd_incrby_internal(&args[0], 1, store).await
}

async fn cmd_decr(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'decr' command".into());
    }
    cmd_incrby_internal(&args[0], -1, store).await
}

async fn cmd_incrby(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'incrby' command".into());
    }
    let increment: i64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    cmd_incrby_internal(&args[0], increment, store).await
}

async fn cmd_decrby(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'decrby' command".into());
    }
    let decrement: i64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    cmd_incrby_internal(&args[0], -decrement, store).await
}

async fn cmd_incrbyfloat(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'incrbyfloat' command".into());
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
                    Err(_) => return RespValue::Error("ERR value is not a valid float".into()),
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

async fn cmd_getrange(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'getrange' command".into());
    }
    let start: i64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    let end: i64 = match std::str::from_utf8(&args[2]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    match store.get(&args[0]) {
        Some(entry) => match &entry.data {
            DataType::String(s) => {
                let len = s.len() as i64;
                if len == 0 {
                    return bull(Bytes::new());
                }
                let si = normalize_index(start, len);
                let ei = normalize_index(end, len);
                if si > ei || si >= len {
                    return bull(Bytes::new());
                }
                let ei = ei.min(len - 1);
                bull(Bytes::copy_from_slice(&s[si as usize..=ei as usize]))
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => bull(Bytes::new()),
    }
}

async fn cmd_setrange(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setrange' command".into());
    }
    let key = args[0].clone();
    let offset: usize = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
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
                let rl = buf.len() as i64;
                entry.data = DataType::String(Bytes::from(buf));
                RespValue::Integer(rl)
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => {
            let new_len = offset + value.len();
            let mut buf = vec![0u8; new_len];
            buf[offset..].copy_from_slice(&value);
            dm.insert(key, Entry::new(DataType::String(Bytes::from(buf)), None));
            RespValue::Integer(new_len as i64)
        }
    }
}

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

async fn cmd_setex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setex' command".into());
    }
    let key = args[0].clone();
    let seconds: u64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    let value = args[2].clone();
    store.set(
        key,
        DataType::String(value),
        Some(Duration::from_secs(seconds)),
    );
    RespValue::SimpleString("OK".into())
}

async fn cmd_psetex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'psetex' command".into());
    }
    let key = args[0].clone();
    let ms: u64 = match std::str::from_utf8(&args[1]).unwrap_or("").parse() {
        Ok(v) => v,
        Err(_) => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    let value = args[2].clone();
    store.set(
        key,
        DataType::String(value),
        Some(Duration::from_millis(ms)),
    );
    RespValue::SimpleString("OK".into())
}

async fn cmd_getex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    // GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL|PERSIST]
    if args.is_empty() || args.len() > 3 {
        return RespValue::Error("ERR wrong number of arguments for 'getex' command".into());
    }
    let key = &args[0];
    match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::String(_) => {}
            _ => {
                return RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )
            }
        },
        None => return null_bulk(),
    }
    // Parse the optional expiry argument
    let mut new_ttl: Option<Duration> = None;
    if args.len() >= 2 {
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
            "KEEPTTL" => {
                // No-op: keep existing TTL
            }
            "EX" => {
                if args.len() < 3 {
                    return RespValue::Error("ERR syntax error".into());
                }
                let secs: u64 = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                new_ttl = Some(Duration::from_secs(secs));
            }
            "PX" => {
                if args.len() < 3 {
                    return RespValue::Error("ERR syntax error".into());
                }
                let ms: u64 = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                new_ttl = Some(Duration::from_millis(ms));
            }
            "EXAT" => {
                if args.len() < 3 {
                    return RespValue::Error("ERR syntax error".into());
                }
                let ts: u64 = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if ts > now_secs {
                    new_ttl = Some(Duration::from_secs(ts - now_secs));
                } else {
                    store.del(key);
                    return null_bulk();
                }
            }
            "PXAT" => {
                if args.len() < 3 {
                    return RespValue::Error("ERR syntax error".into());
                }
                let ts_ms: u64 = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if ts_ms > now_ms {
                    new_ttl = Some(Duration::from_millis(ts_ms - now_ms));
                } else {
                    store.del(key);
                    return null_bulk();
                }
            }
            _ => return RespValue::Error("ERR syntax error".into()),
        }
    }
    if let Some(ttl) = new_ttl {
        let value = match store.get(key) {
            Some(entry) => match &entry.data {
                DataType::String(s) => s.clone(),
                _ => return null_bulk(),
            },
            None => return null_bulk(),
        };
        store.set(key.clone(), DataType::String(value), Some(ttl));
    }
    match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => bull(s.clone()),
            _ => unreachable!(),
        },
        None => null_bulk(),
    }
}

async fn cmd_getdel(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'getdel' command".into());
    }
    let key = args[0].clone();
    match store.get(&key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => {
                let val = s.clone();
                drop(entry);
                store.del(&key);
                bull(val)
            }
            _ => {
                drop(entry);
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
                        )
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

// ---------------------------------------------------------------------------
// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
// ---------------------------------------------------------------------------
async fn cmd_lcs(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'lcs' command".into());
    }
    let mut want_len = false;
    let mut want_idx = false;
    let mut with_match_len = false;
    let mut min_match_len: usize = 0;
    let mut i = 2;
    while i < args.len() {
        let opt = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        match opt.as_str() {
            "LEN" => want_len = true,
            "IDX" => want_idx = true,
            "WITHMATCHLEN" => with_match_len = true,
            "MINMATCHLEN" => {
                i += 1;
                if i >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                min_match_len = match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(v) => v,
                    None => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
            }
            _ => return RespValue::Error("ERR syntax error".into()),
        }
        i += 1;
    }
    if want_len && want_idx {
        return RespValue::Error(
            "ERR If you want both the length and indexes, please just use IDX.".into(),
        );
    }
    let fetch = |key: &Bytes| -> Result<Bytes, RespValue> {
        match store.get(key) {
            Some(entry) => match &entry.data {
                DataType::String(s) => Ok(s.clone()),
                _ => Err(RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )),
            },
            None => Ok(Bytes::new()),
        }
    };
    let a = match fetch(&args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let b = match fetch(&args[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (la, lb) = (a.len(), b.len());
    // Guard against the O(n*m) table blowing up memory.
    if la.saturating_mul(lb) > 64 * 1024 * 1024 {
        return RespValue::Error(
            "ERR Insufficient memory, failed allocating transient memory for LCS".into(),
        );
    }
    let w = lb + 1;
    let mut dp = vec![0u32; (la + 1) * w];
    for ai in 1..=la {
        for bi in 1..=lb {
            dp[ai * w + bi] = if a[ai - 1] == b[bi - 1] {
                dp[(ai - 1) * w + (bi - 1)] + 1
            } else {
                dp[(ai - 1) * w + bi].max(dp[ai * w + (bi - 1)])
            };
        }
    }
    let lcs_len = dp[la * w + lb] as i64;
    if want_len {
        return RespValue::Integer(lcs_len);
    }
    if !want_idx {
        let mut out = Vec::with_capacity(lcs_len as usize);
        let (mut ai, mut bi) = (la, lb);
        while ai > 0 && bi > 0 {
            if a[ai - 1] == b[bi - 1] {
                out.push(a[ai - 1]);
                ai -= 1;
                bi -= 1;
            } else if dp[(ai - 1) * w + bi] >= dp[ai * w + (bi - 1)] {
                ai -= 1;
            } else {
                bi -= 1;
            }
        }
        out.reverse();
        return RespValue::BulkString(Some(Bytes::from(out)));
    }
    // IDX: collect contiguous match runs while backtracking (last match first).
    let mut matches = Vec::new();
    let (mut ai, mut bi) = (la, lb);
    while ai > 0 && bi > 0 {
        if a[ai - 1] == b[bi - 1] {
            let (a_end, b_end) = (ai - 1, bi - 1);
            let mut run = 0usize;
            while ai > 0 && bi > 0 && a[ai - 1] == b[bi - 1] {
                ai -= 1;
                bi -= 1;
                run += 1;
            }
            if run >= min_match_len {
                let mut m = vec![
                    RespValue::array(vec![
                        RespValue::Integer(ai as i64),
                        RespValue::Integer(a_end as i64),
                    ]),
                    RespValue::array(vec![
                        RespValue::Integer(bi as i64),
                        RespValue::Integer(b_end as i64),
                    ]),
                ];
                if with_match_len {
                    m.push(RespValue::Integer(run as i64));
                }
                matches.push(RespValue::array(m));
            }
        } else if dp[(ai - 1) * w + bi] >= dp[ai * w + (bi - 1)] {
            ai -= 1;
        } else {
            bi -= 1;
        }
    }
    RespValue::array(vec![
        RespValue::BulkString(Some(Bytes::from("matches"))),
        RespValue::array(matches),
        RespValue::BulkString(Some(Bytes::from("len"))),
        RespValue::Integer(lcs_len),
    ])
}

fn normalize_index(index: i64, len: i64) -> i64 {
    if index < 0 {
        let idx = len + index;
        if idx < 0 {
            0
        } else {
            idx
        }
    } else {
        index
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
    async fn get_missing() {
        let s = test_store();
        let r = (handle(&[Bytes::from("GET"), Bytes::from("x")], &s)).await;
        assert_eq!(r, RespValue::BulkString(None));
    }
    #[tokio::test]
    async fn set_get() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("k"), Bytes::from("v")],
            &s,
        ))
        .await;
        let r = (handle(&[Bytes::from("GET"), Bytes::from("k")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
    }
    #[tokio::test]
    async fn set_nx() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("nx"),
                Bytes::from("v"),
                Bytes::from("NX"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        let r = (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("nx"),
                Bytes::from("v2"),
                Bytes::from("NX"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(None));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("nx")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
    }
    #[tokio::test]
    async fn set_xx() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("xx"),
                Bytes::from("v"),
                Bytes::from("XX"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(None));
        (handle(
            &[Bytes::from("SET"), Bytes::from("xx"), Bytes::from("o")],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("xx"),
                Bytes::from("u"),
                Bytes::from("XX"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("xx")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("u"))));
    }
    #[tokio::test]
    async fn del() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("d"), Bytes::from("1")],
            &s,
        ))
        .await;
        let r = (handle(&[Bytes::from("DEL"), Bytes::from("d")], &s)).await;
        assert_eq!(r, RespValue::Integer(1));
        let r = (handle(&[Bytes::from("DEL"), Bytes::from("d")], &s)).await;
        assert_eq!(r, RespValue::Integer(0));
    }
    #[tokio::test]
    async fn getset() {
        let s = test_store();
        let r = (handle(
            &[Bytes::from("GETSET"), Bytes::from("gs"), Bytes::from("new")],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(None));
        let r = (handle(
            &[
                Bytes::from("GETSET"),
                Bytes::from("gs"),
                Bytes::from("newer"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("new"))));
    }
    #[tokio::test]
    async fn mget() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("a"), Bytes::from("1")],
            &s,
        ))
        .await;
        let r = (handle(
            &[Bytes::from("MGET"), Bytes::from("a"), Bytes::from("b")],
            &s,
        ))
        .await;
        match r {
            RespValue::Array(Some(a)) => {
                assert_eq!(a[0], RespValue::BulkString(Some(Bytes::from("1"))));
                assert_eq!(a[1], RespValue::BulkString(None));
            }
            _ => panic!("expected array"),
        }
    }
    #[tokio::test]
    async fn mset() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("MSET"),
                Bytes::from("a"),
                Bytes::from("1"),
                Bytes::from("b"),
                Bytes::from("2"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("a")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("1"))));
    }
    #[tokio::test]
    async fn msetnx() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("MSETNX"),
                Bytes::from("a"),
                Bytes::from("1"),
                Bytes::from("b"),
                Bytes::from("2"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(1));
        let r = (handle(
            &[
                Bytes::from("MSETNX"),
                Bytes::from("a"),
                Bytes::from("3"),
                Bytes::from("c"),
                Bytes::from("4"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(0));
    }
    #[tokio::test]
    async fn incr() {
        let s = test_store();
        let r = (handle(&[Bytes::from("INCR"), Bytes::from("c")], &s)).await;
        assert_eq!(r, RespValue::Integer(1));
        let r = (handle(&[Bytes::from("INCR"), Bytes::from("c")], &s)).await;
        assert_eq!(r, RespValue::Integer(2));
    }
    #[tokio::test]
    async fn decr() {
        let s = test_store();
        let r = (handle(&[Bytes::from("DECR"), Bytes::from("c")], &s)).await;
        assert_eq!(r, RespValue::Integer(-1));
    }
    #[tokio::test]
    async fn incrby() {
        let s = test_store();
        let r = (handle(
            &[Bytes::from("INCRBY"), Bytes::from("c"), Bytes::from("10")],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(10));
    }
    #[tokio::test]
    async fn decrby() {
        let s = test_store();
        let r = (handle(
            &[Bytes::from("DECRBY"), Bytes::from("c"), Bytes::from("3")],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(-3));
    }
    #[tokio::test]
    async fn incrbyfloat() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("f"), Bytes::from("10.5")],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("INCRBYFLOAT"),
                Bytes::from("f"),
                Bytes::from("0.1"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("10.6"))));
    }
    #[tokio::test]
    async fn append() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("APPEND"),
                Bytes::from("ap"),
                Bytes::from("Hello"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(5));
        let r = (handle(
            &[
                Bytes::from("APPEND"),
                Bytes::from("ap"),
                Bytes::from(" World"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(11));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("ap")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("Hello World"))));
    }
    #[tokio::test]
    async fn strlen() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("s"), Bytes::from("hello")],
            &s,
        ))
        .await;
        let r = (handle(&[Bytes::from("STRLEN"), Bytes::from("s")], &s)).await;
        assert_eq!(r, RespValue::Integer(5));
        let r = (handle(&[Bytes::from("STRLEN"), Bytes::from("no")], &s)).await;
        assert_eq!(r, RespValue::Integer(0));
    }
    #[tokio::test]
    async fn getrange() {
        let s = test_store();
        (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("g"),
                Bytes::from("Hello World"),
            ],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("GETRANGE"),
                Bytes::from("g"),
                Bytes::from("0"),
                Bytes::from("4"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("Hello"))));
        let r = (handle(
            &[
                Bytes::from("GETRANGE"),
                Bytes::from("g"),
                Bytes::from("-5"),
                Bytes::from("-1"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("World"))));
    }
    #[tokio::test]
    async fn setrange() {
        let s = test_store();
        (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("sr"),
                Bytes::from("Hello World"),
            ],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("SETRANGE"),
                Bytes::from("sr"),
                Bytes::from("6"),
                Bytes::from("Redis"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(11));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("sr")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("Hello Redis"))));
    }
    #[tokio::test]
    async fn setnx() {
        let s = test_store();
        let r = (handle(
            &[Bytes::from("SETNX"), Bytes::from("sn"), Bytes::from("v")],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(1));
        let r = (handle(
            &[Bytes::from("SETNX"), Bytes::from("sn"), Bytes::from("v2")],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::Integer(0));
    }
    #[tokio::test]
    async fn setex() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("SETEX"),
                Bytes::from("sx"),
                Bytes::from("10"),
                Bytes::from("v"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("sx")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
    }
    #[tokio::test]
    async fn psetex() {
        let s = test_store();
        let r = (handle(
            &[
                Bytes::from("PSETEX"),
                Bytes::from("px"),
                Bytes::from("10000"),
                Bytes::from("v"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("px")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
    }
    #[tokio::test]
    async fn getex() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("ge"), Bytes::from("v")],
            &s,
        ))
        .await;
        let r = (handle(&[Bytes::from("GETEX"), Bytes::from("ge")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
    }
    #[tokio::test]
    async fn getdel() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("gd"), Bytes::from("v")],
            &s,
        ))
        .await;
        let r = (handle(&[Bytes::from("GETDEL"), Bytes::from("gd")], &s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
        let r = (handle(&[Bytes::from("GET"), Bytes::from("gd")], &s)).await;
        assert_eq!(r, RespValue::BulkString(None));
    }
    #[tokio::test]
    async fn dispatch_set_get() {
        let s = test_store();
        let r = (crate::dispatch(
            vec![Bytes::from("SET"), Bytes::from("dk"), Bytes::from("dv")],
            s.clone(),
        ))
        .await;
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        let r = (crate::dispatch(vec![Bytes::from("GET"), Bytes::from("dk")], s)).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("dv"))));
    }
    #[tokio::test]
    async fn getex_persist() {
        let s = test_store();
        (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("k"),
                Bytes::from("v"),
                Bytes::from("EX"),
                Bytes::from("100"),
            ],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("GETEX"),
                Bytes::from("k"),
                Bytes::from("PERSIST"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
        // After PERSIST, TTL should be -1 (no expiry)
        let r = (super::super::keys::handle(&[Bytes::from("TTL"), Bytes::from("k")], &s)).await;
        assert_eq!(r, RespValue::Integer(-1));
    }
    #[tokio::test]
    async fn getex_ex() {
        let s = test_store();
        (handle(
            &[Bytes::from("SET"), Bytes::from("k"), Bytes::from("v")],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("GETEX"),
                Bytes::from("k"),
                Bytes::from("EX"),
                Bytes::from("60"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
        let r = (super::super::keys::handle(&[Bytes::from("TTL"), Bytes::from("k")], &s)).await;
        match r {
            RespValue::Integer(n) => assert!(n > 0 && n <= 60),
            _ => panic!("expected positive TTL"),
        }
    }
    #[tokio::test]
    async fn getex_keepttl() {
        let s = test_store();
        (handle(
            &[
                Bytes::from("SET"),
                Bytes::from("k"),
                Bytes::from("v"),
                Bytes::from("EX"),
                Bytes::from("100"),
            ],
            &s,
        ))
        .await;
        let r = (handle(
            &[
                Bytes::from("GETEX"),
                Bytes::from("k"),
                Bytes::from("KEEPTTL"),
            ],
            &s,
        ))
        .await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("v"))));
        let r = (super::super::keys::handle(&[Bytes::from("TTL"), Bytes::from("k")], &s)).await;
        match r {
            RespValue::Integer(n) => assert!(n > 0 && n <= 100),
            _ => panic!("expected positive TTL"),
        }
    }
}
