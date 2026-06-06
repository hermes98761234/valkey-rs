use bytes::Bytes;
use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use valkey_proto::RespValue;
use valkey_storage::{DataType, Store};

pub async fn handle(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".into());
    }
    let cmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    match cmd.as_str() {
        "EXISTS" => cmd_exists(&args[1..], store).await,
        "TYPE" => cmd_type(&args[1..], store).await,
        "RENAME" => cmd_rename(&args[1..], store).await,
        "RENAMENX" => cmd_renamenx(&args[1..], store).await,
        "EXPIRE" => cmd_expire(&args[1..], store).await,
        "PEXPIRE" => cmd_pexpire(&args[1..], store).await,
        "EXPIREAT" => cmd_expireat(&args[1..], store).await,
        "PEXPIREAT" => cmd_pexpireat(&args[1..], store).await,
        "TTL" => cmd_ttl(&args[1..], store).await,
        "PTTL" => cmd_pttl(&args[1..], store).await,
        "PERSIST" => cmd_persist(&args[1..], store).await,
        "KEYS" => cmd_keys(&args[1..], store).await,
        "SCAN" => cmd_scan(&args[1..], store).await,
        "RANDOMKEY" => cmd_randomkey(&args[1..], store).await,
        "MOVE" => cmd_move(&args[1..], store).await,
        "COPY" => cmd_copy(&args[1..], store).await,
        "OBJECT" => cmd_object(&args[1..], store).await,
        "DUMP" => cmd_dump(&args[1..], store).await,
        "RESTORE" => cmd_restore(&args[1..], store).await,
        "WAIT" => cmd_wait(&args[1..], store).await,
        "SORT" => cmd_sort(&args[1..], store).await,
        "SORT_RO" => cmd_sort_ro(&args[1..], store).await,
        "SUBSTR" => cmd_substr(&args[1..], store).await,
        "EXPIRETIME" => cmd_expiretime(&args[1..], store).await,
        "PEXPIRETIME" => cmd_pexpiretime(&args[1..], store).await,
        "UNLINK" => cmd_unlink(&args[1..], store).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", cmd)),
    }
}
async fn cmd_exists(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'exists' command".into());
    }
    RespValue::Integer(args.iter().filter(|k| store.exists(k)).count() as i64)
}
async fn cmd_type(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'type' command".into());
    }
    match store.type_of(&args[0]) {
        Some(t) => RespValue::SimpleString(t.to_string()),
        None => RespValue::SimpleString("none".to_string()),
    }
}
async fn cmd_rename(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'rename' command".into());
    }
    let key = &args[0];
    let newkey = args[1].clone();
    let (data, ttl) = match store.get(key) {
        Some(e) => {
            let ttl_dur = e.expires_at.map(|at| {
                let now = Instant::now();
                if at > now {
                    at.duration_since(now)
                } else {
                    Duration::ZERO
                }
            });
            let result = (e.data.clone(), ttl_dur);
            drop(e);
            result
        }
        None => return RespValue::Error("ERR no such key".into()),
    };
    store.del(key);
    store.set(newkey, data, ttl);
    RespValue::ok()
}
async fn cmd_renamenx(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'renamenx' command".into());
    }
    let key = &args[0];
    let newkey = &args[1];
    if store.exists(newkey) {
        return RespValue::Integer(0);
    }
    match store.get(key) {
        Some(e) => {
            let ttl = e.expires_at.map(|at| {
                let now = Instant::now();
                if at > now {
                    at.duration_since(now)
                } else {
                    Duration::ZERO
                }
            });
            let data = e.data.clone();
            drop(e);
            store.set(newkey.clone(), data, ttl);
            store.del(key);
            RespValue::Integer(1)
        }
        None => RespValue::Integer(0),
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireCond {
    None,
    NX,
    XX,
    GT,
    LT,
}
fn parse_expire_cond(s: &str) -> ExpireCond {
    match s.to_ascii_uppercase().as_str() {
        "NX" => ExpireCond::NX,
        "XX" => ExpireCond::XX,
        "GT" => ExpireCond::GT,
        "LT" => ExpireCond::LT,
        _ => ExpireCond::None,
    }
}
fn check_expire_cond(store: &Store, key: &Bytes, new_dur: Duration, cond: ExpireCond) -> bool {
    let cur = store.ttl(key);
    match cond {
        ExpireCond::None => true,
        ExpireCond::NX => cur.is_none(),
        ExpireCond::XX => cur.is_some(),
        ExpireCond::GT => matches!(cur, Some(c) if new_dur > c) || cur.is_none(),
        ExpireCond::LT => matches!(cur, Some(c) if new_dur < c),
    }
}
async fn cmd_expire(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'expire' command".into());
    }
    let key = &args[0];
    let secs: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let cond = if args.len() >= 3 {
        parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or(""))
    } else {
        ExpireCond::None
    };
    if !store.exists(key) {
        return RespValue::Integer(0);
    }
    let dur = Duration::from_secs(secs.max(0) as u64);
    if !check_expire_cond(store, key, dur, cond) {
        return RespValue::Integer(0);
    }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}
async fn cmd_pexpire(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'pexpire' command".into());
    }
    let key = &args[0];
    let ms: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let cond = if args.len() >= 3 {
        parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or(""))
    } else {
        ExpireCond::None
    };
    if !store.exists(key) {
        return RespValue::Integer(0);
    }
    let dur = Duration::from_millis(ms.max(0) as u64);
    if !check_expire_cond(store, key, dur, cond) {
        return RespValue::Integer(0);
    }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}
async fn cmd_expireat(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'expireat' command".into());
    }
    let key = &args[0];
    let ts: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let cond = if args.len() >= 3 {
        parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or(""))
    } else {
        ExpireCond::None
    };
    if !store.exists(key) {
        return RespValue::Integer(0);
    }
    let dur = Duration::from_secs(
        (ts - SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64)
            .max(0) as u64,
    );
    if !check_expire_cond(store, key, dur, cond) {
        return RespValue::Integer(0);
    }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}
async fn cmd_pexpireat(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'pexpireat' command".into());
    }
    let key = &args[0];
    let ts_ms: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let cond = if args.len() >= 3 {
        parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or(""))
    } else {
        ExpireCond::None
    };
    if !store.exists(key) {
        return RespValue::Integer(0);
    }
    let dur = Duration::from_millis(
        (ts_ms
            - SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64)
            .max(0) as u64,
    );
    if !check_expire_cond(store, key, dur, cond) {
        return RespValue::Integer(0);
    }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}
async fn cmd_ttl(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'ttl' command".into());
    }
    if !store.exists(&args[0]) {
        return RespValue::Integer(-2);
    }
    match store.ttl(&args[0]) {
        Some(d) => RespValue::Integer(d.as_secs() as i64),
        None => RespValue::Integer(-1),
    }
}
async fn cmd_pttl(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'pttl' command".into());
    }
    if !store.exists(&args[0]) {
        return RespValue::Integer(-2);
    }
    match store.ttl(&args[0]) {
        Some(d) => RespValue::Integer(d.as_millis() as i64),
        None => RespValue::Integer(-1),
    }
}
async fn cmd_persist(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'persist' command".into());
    }
    let data = match store.get(&args[0]) {
        Some(e) => {
            if e.expires_at.is_none() {
                return RespValue::Integer(0);
            }
            let d = e.data.clone();
            drop(e);
            d
        }
        None => return RespValue::Integer(0),
    };
    store.set(args[0].clone(), data, None);
    RespValue::Integer(1)
}
async fn cmd_keys(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'keys' command".into());
    }
    let pat = std::str::from_utf8(&args[0])
        .ok()
        .map(|s| s.to_string())
        .unwrap_or_default();
    RespValue::Array(Some(
        store
            .keys(&pat)
            .into_iter()
            .map(|k| RespValue::BulkString(Some(k)))
            .collect(),
    ))
}
async fn cmd_scan(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'scan' command".into());
    }
    let cursor: u64 = std::str::from_utf8(&args[0])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut pat = None;
    let mut count = 10usize;
    let mut type_filter = None;
    let mut i = 1;
    while i < args.len() {
        match std::str::from_utf8(&args[i])
            .ok()
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default()
            .as_str()
        {
            "MATCH" => {
                i += 1;
                if i < args.len() {
                    pat = std::str::from_utf8(&args[i]).ok().map(|s| s.to_string());
                }
            }
            "COUNT" => {
                i += 1;
                if i < args.len() {
                    count = std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10);
                }
            }
            "TYPE" => {
                i += 1;
                if i < args.len() {
                    type_filter = std::str::from_utf8(&args[i]).ok().map(|s| s.to_string());
                }
            }
            _ => {}
        }
        i += 1;
    }
    let mut keys: Vec<Bytes> = store.keys("*");
    keys.retain(|k| store.exists(k));
    keys.sort();
    if let Some(ref t) = type_filter {
        keys.retain(|k| store.type_of(k).map(|x| x == t).unwrap_or(false));
    }
    if let Some(ref p) = pat {
        if p != "*" {
            if let Some((pre, suf)) = p.split_once('*') {
                keys.retain(|k| {
                    let s = std::str::from_utf8(k).unwrap_or("");
                    s.starts_with(pre) && s.ends_with(suf)
                });
            } else {
                let ex = Bytes::from(p.clone());
                keys.retain(|k| k == &ex);
            }
        }
    }
    let start = cursor as usize;
    if start >= keys.len() {
        return RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from("0"))),
            RespValue::Array(None),
        ]));
    }
    let end = (start + count).min(keys.len());
    let chunk = keys[start..end]
        .iter()
        .map(|k| RespValue::BulkString(Some(k.clone())))
        .collect::<Vec<_>>();
    let nc = if end >= keys.len() { 0 } else { end as u64 };
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(Bytes::from(nc.to_string()))),
        RespValue::Array(Some(chunk)),
    ]))
}
async fn cmd_randomkey(_args: &[Bytes], store: &Arc<Store>) -> RespValue {
    let keys = store.keys("*");
    if keys.is_empty() {
        return RespValue::BulkString(None);
    }
    RespValue::BulkString(Some(keys.choose(&mut rand::thread_rng()).unwrap().clone()))
}
async fn cmd_move(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'move' command".into());
    }
    RespValue::Integer(0)
}
async fn cmd_copy(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'copy' command".into());
    }
    let src = &args[0];
    let dst = &args[1];
    let replace = args.len() >= 3
        && std::str::from_utf8(&args[2])
            .map(|s| s.to_ascii_uppercase() == "REPLACE")
            .unwrap_or(false);
    let data = match store.get(src) {
        Some(e) => {
            let d = e.data.clone();
            drop(e);
            d
        }
        None => return RespValue::Integer(0),
    };
    if !replace && store.exists(dst) {
        return RespValue::Integer(0);
    }
    store.set(dst.clone(), data, None);
    RespValue::Integer(1)
}
async fn cmd_object(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'object' command".into());
    }
    let sub = std::str::from_utf8(&args[0])
        .ok()
        .map(|s| s.to_ascii_uppercase())
        .unwrap_or_default();
    let key = &args[1];
    match sub.as_str() {
        "ENCODING" => {
            if !store.exists(key) {
                return RespValue::Error("ERR no such key".into());
            }
            RespValue::BulkString(Some(Bytes::from(match store.type_of(key) {
                Some("string") => "embstr",
                Some("list") => "quicklist",
                Some("hash") => "hashtable",
                Some("set") => "hashtable",
                Some("zset") => "skiplist",
                Some("stream") => "listpack",
                _ => "unknown",
            })))
        }
        "REFCOUNT" => {
            if !store.exists(key) {
                return RespValue::Error("ERR no such key".into());
            }
            RespValue::Integer(1)
        }
        "IDLETIME" => {
            if !store.exists(key) {
                return RespValue::Error("ERR no such key".into());
            }
            RespValue::Integer(0)
        }
        "FREQ" => {
            if !store.exists(key) {
                return RespValue::Error("ERR no such key".into());
            }
            RespValue::Integer(0)
        }
        _ => RespValue::Error("ERR syntax error".into()),
    }
}
async fn cmd_dump(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'dump' command".into());
    }
    let key = &args[0];
    let entry = match store.get(key) {
        Some(e) => e,
        None => return RespValue::BulkString(None),
    };
    // Simple custom serialization format:
    // type_byte | data_len (4 bytes LE) | data_bytes | ttl_ms (8 bytes LE, 0 = no expiry)
    let mut buf = Vec::new();
    let type_byte: u8 = match &entry.data {
        DataType::String(_) => 0,
        DataType::List(_) => 1,
        DataType::Set(_) => 2,
        DataType::ZSet(_) => 3,
        DataType::Hash(_) => 4,
        DataType::Stream(_) => 5,
    };
    buf.push(type_byte);
    let data_bytes = match &entry.data {
        DataType::String(s) => s.clone(),
        DataType::List(l) => {
            let mut v = Vec::new();
            for item in l {
                v.extend_from_slice(&(item.len() as u32).to_le_bytes());
                v.extend_from_slice(item);
            }
            Bytes::from(v)
        }
        DataType::Set(s) => {
            let mut v = Vec::new();
            for item in s {
                v.extend_from_slice(&(item.len() as u32).to_le_bytes());
                v.extend_from_slice(item);
            }
            Bytes::from(v)
        }
        DataType::ZSet(z) => {
            let mut v = Vec::new();
            for (member, score) in &z.members {
                v.extend_from_slice(&(member.len() as u32).to_le_bytes());
                v.extend_from_slice(member);
                v.extend_from_slice(&score.0.to_le_bytes());
            }
            Bytes::from(v)
        }
        DataType::Hash(h) => {
            let mut v = Vec::new();
            for (k, val) in h {
                v.extend_from_slice(&(k.len() as u32).to_le_bytes());
                v.extend_from_slice(k);
                v.extend_from_slice(&(val.len() as u32).to_le_bytes());
                v.extend_from_slice(val);
            }
            Bytes::from(v)
        }
        DataType::Stream(_) => Bytes::new(), // Stream serialization not yet implemented
    };
    buf.extend_from_slice(&(data_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&data_bytes);
    let ttl_ms: u64 = entry
        .expires_at
        .map(|at| {
            at.checked_duration_since(std::time::Instant::now())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        })
        .unwrap_or(0);
    buf.extend_from_slice(&ttl_ms.to_le_bytes());
    // Append a simple 4-byte CRC-like checksum (sum of bytes mod 2^32)
    let checksum: u32 = buf.iter().map(|b| *b as u32).sum::<u32>();
    buf.extend_from_slice(&checksum.to_le_bytes());
    RespValue::BulkString(Some(Bytes::from(buf)))
}
async fn cmd_restore(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'restore' command".into());
    }
    let key = args[0].clone();
    let ttl_ms: u64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let serialized = &args[2];
    if serialized.len() < 14 {
        return RespValue::Error("ERR DUMP payload version or checksum are wrong".into());
    }
    // Verify checksum
    let data_part = &serialized[..serialized.len() - 4];
    let checksum_bytes = &serialized[serialized.len() - 4..];
    let stored_checksum = u32::from_le_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
    ]);
    let computed_checksum: u32 = data_part.iter().map(|b| *b as u32).sum::<u32>();
    if stored_checksum != computed_checksum {
        return RespValue::Error("ERR DUMP payload version or checksum are wrong".into());
    }
    let type_byte = data_part[0];
    let data_len = u32::from_le_bytes([data_part[1], data_part[2], data_part[3], data_part[4]]) as usize;
    if 5 + data_len + 8 > data_part.len() {
        return RespValue::Error("ERR DUMP payload version or checksum are wrong".into());
    }
    let data_bytes = &data_part[5..5 + data_len];
    let ttl_stored = u64::from_le_bytes([
        data_part[5 + data_len],
        data_part[5 + data_len + 1],
        data_part[5 + data_len + 2],
        data_part[5 + data_len + 3],
        data_part[5 + data_len + 4],
        data_part[5 + data_len + 5],
        data_part[5 + data_len + 6],
        data_part[5 + data_len + 7],
    ]);
    let data = match type_byte {
        0 => DataType::String(Bytes::copy_from_slice(data_bytes)),
        1 => {
            let mut items = std::collections::VecDeque::new();
            let mut offset = 0;
            while offset + 4 <= data_bytes.len() {
                let len = u32::from_le_bytes([
                    data_bytes[offset],
                    data_bytes[offset + 1],
                    data_bytes[offset + 2],
                    data_bytes[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + len > data_bytes.len() {
                    break;
                }
                items.push_back(Bytes::copy_from_slice(&data_bytes[offset..offset + len]));
                offset += len;
            }
            DataType::List(items)
        }
        2 => {
            let mut items = std::collections::HashSet::new();
            let mut offset = 0;
            while offset + 4 <= data_bytes.len() {
                let len = u32::from_le_bytes([
                    data_bytes[offset],
                    data_bytes[offset + 1],
                    data_bytes[offset + 2],
                    data_bytes[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + len > data_bytes.len() {
                    break;
                }
                items.insert(Bytes::copy_from_slice(&data_bytes[offset..offset + len]));
                offset += len;
            }
            DataType::Set(items)
        }
        3 => {
            let mut zset = valkey_storage::ZSetData::new();
            let mut offset = 0;
            while offset + 4 <= data_bytes.len() {
                let len = u32::from_le_bytes([
                    data_bytes[offset],
                    data_bytes[offset + 1],
                    data_bytes[offset + 2],
                    data_bytes[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + len + 8 > data_bytes.len() {
                    break;
                }
                let member = Bytes::copy_from_slice(&data_bytes[offset..offset + len]);
                offset += len;
                let score = f64::from_le_bytes([
                    data_bytes[offset],
                    data_bytes[offset + 1],
                    data_bytes[offset + 2],
                    data_bytes[offset + 3],
                    data_bytes[offset + 4],
                    data_bytes[offset + 5],
                    data_bytes[offset + 6],
                    data_bytes[offset + 7],
                ]);
                offset += 8;
                zset.add(member, score);
            }
            DataType::ZSet(zset)
        }
        4 => {
            let mut map = std::collections::HashMap::new();
            let mut offset = 0;
            while offset + 4 <= data_bytes.len() {
                let klen = u32::from_le_bytes([
                    data_bytes[offset],
                    data_bytes[offset + 1],
                    data_bytes[offset + 2],
                    data_bytes[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + klen + 4 > data_bytes.len() {
                    break;
                }
                let k = Bytes::copy_from_slice(&data_bytes[offset..offset + klen]);
                offset += klen;
                let vlen = u32::from_le_bytes([
                    data_bytes[offset],
                    data_bytes[offset + 1],
                    data_bytes[offset + 2],
                    data_bytes[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + vlen > data_bytes.len() {
                    break;
                }
                let v = Bytes::copy_from_slice(&data_bytes[offset..offset + vlen]);
                offset += vlen;
                map.insert(k, v);
            }
            DataType::Hash(map)
        }
        5 => DataType::Stream(valkey_storage::StreamData::new()),
        _ => return RespValue::Error("ERR DUMP payload version or checksum are wrong".into()),
    };
    // Use the TTL from the serialized data if the user passed 0, otherwise use user's TTL
    let effective_ttl = if ttl_ms > 0 {
        Some(std::time::Duration::from_millis(ttl_ms))
    } else if ttl_stored > 0 {
        Some(std::time::Duration::from_millis(ttl_stored))
    } else {
        None
    };
    store.set(key, data, effective_ttl);
    RespValue::ok()
}
async fn cmd_wait(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'wait' command".into());
    }
    RespValue::Integer(0)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortOrder {
    Asc,
    Desc,
}
async fn cmd_sort(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'sort' command".into());
    }
    let key = &args[0];
    let mut limit = None;
    let mut order = SortOrder::Asc;
    let mut alpha = false;
    let mut store_dest = None;
    let mut i = 1;
    while i < args.len() {
        match std::str::from_utf8(&args[i])
            .ok()
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default()
            .as_str()
        {
            "LIMIT" => {
                i += 1;
                if i + 1 < args.len() {
                    limit = Some((
                        std::str::from_utf8(&args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0),
                        std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0),
                    ));
                    i += 1;
                }
            }
            "ASC" => order = SortOrder::Asc,
            "DESC" => order = SortOrder::Desc,
            "ALPHA" => alpha = true,
            "STORE" => {
                i += 1;
                if i < args.len() {
                    store_dest = Some(args[i].clone());
                }
            }
            _ => {}
        }
        i += 1;
    }
    let mut items: Vec<Bytes> = match store.get(key) {
        Some(e) => match &e.data {
            DataType::List(l) => l.iter().cloned().collect(),
            DataType::Set(s) => s.iter().cloned().collect(),
            DataType::ZSet(z) => z.members.keys().cloned().collect(),
            _ => return RespValue::Error("WRONGTYPE".into()),
        },
        None => return RespValue::Array(None),
    };
    if alpha {
        let mut v: Vec<_> = items
            .into_iter()
            .map(|b| (String::from_utf8_lossy(&b).into_owned(), b))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        if order == SortOrder::Desc {
            v.reverse();
        }
        items = v.into_iter().map(|(_, b)| b).collect();
    } else {
        let mut v: Vec<_> = items
            .into_iter()
            .map(|b| (String::from_utf8_lossy(&b).parse::<f64>().unwrap_or(0.0), b))
            .collect();
        v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if order == SortOrder::Desc {
            v.reverse();
        }
        items = v.into_iter().map(|(_, b)| b).collect();
    }
    if let Some((off, cnt)) = limit {
        let s = off.min(items.len());
        let e = (off + cnt).min(items.len());
        items = items[s..e].to_vec();
    }
    if let Some(dst) = store_dest {
        store.set(dst, DataType::List(items.iter().cloned().collect()), None);
        return RespValue::Integer(items.len() as i64);
    }
    RespValue::Array(Some(
        items
            .into_iter()
            .map(|b| RespValue::BulkString(Some(b)))
            .collect(),
    ))
}
async fn cmd_unlink(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'unlink' command".into());
    }
    RespValue::Integer(args.iter().filter(|k| store.del(k)).count() as i64)
}

// EXPIRETIME key — returns the absolute Unix timestamp (in seconds) at which the key will expire.
async fn cmd_expiretime(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'expiretime' command".into());
    }
    let key = &args[0];
    if !store.exists(key) {
        return RespValue::Integer(-2);
    }
    match store.ttl(key) {
        Some(d) => {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            RespValue::Integer(now_secs + d.as_secs() as i64)
        }
        None => RespValue::Integer(-1),
    }
}

// PEXPIRETIME key — returns the absolute Unix timestamp (in milliseconds) at which the key will expire.
async fn cmd_pexpiretime(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'pexpiretime' command".into());
    }
    let key = &args[0];
    if !store.exists(key) {
        return RespValue::Integer(-2);
    }
    match store.ttl(key) {
        Some(d) => {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            RespValue::Integer(now_ms + d.as_millis() as i64)
        }
        None => RespValue::Integer(-1),
    }
}

// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA]
// Read-only variant of SORT — identical to SORT but refuses STORE.
async fn cmd_sort_ro(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'sort_ro' command".into());
    }
    // Reuse the same parsing logic as SORT but reject STORE.
    let key = &args[0];
    let mut limit = None;
    let mut order = SortOrder::Asc;
    let mut alpha = false;
    let mut i = 1;
    while i < args.len() {
        match std::str::from_utf8(&args[i])
            .ok()
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default()
            .as_str()
        {
            "LIMIT" => {
                i += 1;
                if i + 1 < args.len() {
                    limit = Some((
                        std::str::from_utf8(&args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0),
                        std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0),
                    ));
                    i += 1;
                }
            }
            "ASC" => order = SortOrder::Asc,
            "DESC" => order = SortOrder::Desc,
            "ALPHA" => alpha = true,
            "STORE" => return RespValue::Error("ERR SORT_RO does not support STORE".into()),
            _ => {}
        }
        i += 1;
    }
    let mut items: Vec<Bytes> = match store.get(key) {
        Some(e) => match &e.data {
            DataType::List(l) => l.iter().cloned().collect(),
            DataType::Set(s) => s.iter().cloned().collect(),
            DataType::ZSet(z) => z.members.keys().cloned().collect(),
            _ => return RespValue::Error("WRONGTYPE".into()),
        },
        None => return RespValue::Array(None),
    };
    if alpha {
        let mut v: Vec<_> = items
            .into_iter()
            .map(|b| (String::from_utf8_lossy(&b).into_owned(), b))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        if order == SortOrder::Desc {
            v.reverse();
        }
        items = v.into_iter().map(|(_, b)| b).collect();
    } else {
        let mut v: Vec<_> = items
            .into_iter()
            .map(|b| (String::from_utf8_lossy(&b).parse::<f64>().unwrap_or(0.0), b))
            .collect();
        v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if order == SortOrder::Desc {
            v.reverse();
        }
        items = v.into_iter().map(|(_, b)| b).collect();
    }
    if let Some((off, cnt)) = limit {
        let s = off.min(items.len());
        let e = (off + cnt).min(items.len());
        items = items[s..e].to_vec();
    }
    RespValue::Array(Some(
        items
            .into_iter()
            .map(|b| RespValue::BulkString(Some(b)))
            .collect(),
    ))
}

// SUBSTR key start end — legacy alias for GETRANGE
async fn cmd_substr(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    // Delegate to the same logic as GETRANGE via the string module would be ideal,
    // but to avoid cross-module duplication we replicate the logic here.
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'substr' command".into());
    }
    let start: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let end: i64 = std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    fn normalize_index(index: i64, len: i64) -> i64 {
        if index < 0 {
            let idx = len + index;
            if idx < 0 { 0 } else { idx }
        } else {
            index
        }
    }
    match store.get(&args[0]) {
        Some(entry) => match &entry.data {
            DataType::String(s) => {
                let len = s.len() as i64;
                if len == 0 {
                    return RespValue::BulkString(Some(Bytes::new()));
                }
                let si = normalize_index(start, len);
                let ei = normalize_index(end, len);
                if si > ei || si >= len {
                    return RespValue::BulkString(Some(Bytes::new()));
                }
                let ei = ei.min(len - 1);
                RespValue::BulkString(Some(Bytes::copy_from_slice(&s[si as usize..=ei as usize])))
            }
            _ => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => RespValue::BulkString(Some(Bytes::new())),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn st() -> Arc<Store> {
        Store::new()
    }
    #[tokio::test]
    async fn t_exists() {
        let s = st();
        s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        assert_eq!(
            cmd_exists(&[Bytes::from("a")], &s).await,
            RespValue::Integer(1)
        );
    }
    #[tokio::test]
    async fn t_type() {
        let s = st();
        s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(
            cmd_type(&[Bytes::from("k")], &s).await,
            RespValue::SimpleString("string".into())
        );
    }
    #[tokio::test]
    async fn t_rename() {
        let s = st();
        s.set(
            Bytes::from("old"),
            DataType::String(Bytes::from("val")),
            None,
        );
        assert_eq!(
            cmd_rename(&[Bytes::from("old"), Bytes::from("new")], &s).await,
            RespValue::ok()
        );
    }
    #[tokio::test]
    async fn t_expire() {
        let s = st();
        s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(
            cmd_expire(&[Bytes::from("k"), Bytes::from("60")], &s).await,
            RespValue::Integer(1)
        );
        match cmd_ttl(&[Bytes::from("k")], &s).await {
            RespValue::Integer(n) => assert!(n > 0 && n <= 60),
            _ => panic!(),
        }
    }
    #[tokio::test]
    async fn t_ttl_m1() {
        let s = st();
        assert_eq!(
            cmd_ttl(&[Bytes::from("x")], &s).await,
            RespValue::Integer(-2)
        );
    }
    #[tokio::test]
    async fn t_ttl_m2() {
        let s = st();
        s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(
            cmd_ttl(&[Bytes::from("k")], &s).await,
            RespValue::Integer(-1)
        );
    }
    #[tokio::test]
    async fn t_scan() {
        let s = st();
        s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        s.set(Bytes::from("c"), DataType::String(Bytes::from("3")), None);
        let r = cmd_scan(
            &[Bytes::from("0"), Bytes::from("COUNT"), Bytes::from("2")],
            &s,
        )
        .await;
        match &r {
            RespValue::Array(Some(top)) if top.len() == 2 => match &top[1] {
                RespValue::Array(Some(items)) => assert_eq!(items.len(), 2),
                _ => panic!(),
            },
            _ => panic!(),
        }
    }
    #[tokio::test]
    async fn t_copy() {
        let s = st();
        s.set(
            Bytes::from("src"),
            DataType::String(Bytes::from("val")),
            None,
        );
        assert_eq!(
            cmd_copy(&[Bytes::from("src"), Bytes::from("dst")], &s).await,
            RespValue::Integer(1)
        );
        let e = s.get(&Bytes::from("dst"));
        assert!(e.is_some());
        match &e.unwrap().data {
            DataType::String(v) => assert_eq!(v, &Bytes::from("val")),
            _ => panic!(),
        };
    }
    #[tokio::test]
    async fn t_sort() {
        let s = st();
        let mut l = VecDeque::new();
        l.push_back(Bytes::from("3"));
        l.push_back(Bytes::from("1"));
        l.push_back(Bytes::from("2"));
        s.set(Bytes::from("nums"), DataType::List(l), None);
        match cmd_sort(&[Bytes::from("nums")], &s).await {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from("1"))));
            }
            _ => panic!(),
        }
    }
    #[tokio::test]
    async fn t_unlink() {
        let s = st();
        s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        assert_eq!(
            cmd_unlink(&[Bytes::from("a"), Bytes::from("b")], &s).await,
            RespValue::Integer(2)
        );
    }
    #[tokio::test]
    async fn t_dispatch() {
        let s = st();
        s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        assert_eq!(
            handle(&[Bytes::from("EXISTS"), Bytes::from("a")], &s).await,
            RespValue::Integer(1)
        );
    }
    #[tokio::test]
    async fn t_expiretime() {
        let s = st();
        s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        cmd_expire(&[Bytes::from("k"), Bytes::from("60")], &s).await;
        match cmd_expiretime(&[Bytes::from("k")], &s).await {
            RespValue::Integer(ts) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                assert!(ts > now && ts <= now + 60);
            }
            _ => panic!("expected integer response"),
        }
    }
    #[tokio::test]
    async fn t_expiretime_missing() {
        let s = st();
        assert_eq!(
            cmd_expiretime(&[Bytes::from("missing")], &s).await,
            RespValue::Integer(-2)
        );
    }
    #[tokio::test]
    async fn t_expiretime_no_ttl() {
        let s = st();
        s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(
            cmd_expiretime(&[Bytes::from("k")], &s).await,
            RespValue::Integer(-1)
        );
    }
    #[tokio::test]
    async fn t_pexpiretime() {
        let s = st();
        s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        cmd_pexpire(&[Bytes::from("k"), Bytes::from("60000")], &s).await;
        match cmd_pexpiretime(&[Bytes::from("k")], &s).await {
            RespValue::Integer(ts) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                assert!(ts > now && ts <= now + 60000);
            }
            _ => panic!("expected integer response"),
        }
    }
    #[tokio::test]
    async fn t_sort_ro() {
        let s = st();
        let mut l = std::collections::VecDeque::new();
        l.push_back(Bytes::from("3"));
        l.push_back(Bytes::from("1"));
        l.push_back(Bytes::from("2"));
        s.set(Bytes::from("nums"), DataType::List(l), None);
        match cmd_sort_ro(&[Bytes::from("nums")], &s).await {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from("1"))));
            }
            _ => panic!("expected array"),
        }
    }
    #[tokio::test]
    async fn t_sort_ro_store_rejected() {
        let s = st();
        let mut l = std::collections::VecDeque::new();
        l.push_back(Bytes::from("1"));
        s.set(Bytes::from("nums"), DataType::List(l), None);
        let result = cmd_sort_ro(
            &[
                Bytes::from("nums"),
                Bytes::from("STORE"),
                Bytes::from("dest"),
            ],
            &s,
        )
        .await;
        assert!(matches!(result, RespValue::Error(_)));
    }
    #[tokio::test]
    async fn t_substr() {
        let s = st();
        s.set(
            Bytes::from("k"),
            DataType::String(Bytes::from("Hello World")),
            None,
        );
        assert_eq!(
            cmd_substr(&[Bytes::from("k"), Bytes::from("0"), Bytes::from("4")], &s).await,
            RespValue::BulkString(Some(Bytes::from("Hello")))
        );
        assert_eq!(
            cmd_substr(&[Bytes::from("k"), Bytes::from("-5"), Bytes::from("-1")], &s).await,
            RespValue::BulkString(Some(Bytes::from("World")))
        );
    }
    #[tokio::test]
    async fn t_dump_restore_string() {
        let s = st();
        s.set(
            Bytes::from("src"),
            DataType::String(Bytes::from("hello")),
            None,
        );
        let dumped = match cmd_dump(&[Bytes::from("src")], &s).await {
            RespValue::BulkString(Some(b)) => b,
            _ => panic!("expected bulk string from DUMP"),
        };
        assert!(dumped.len() > 0);
        // Restore to a new key with 0 TTL (keeps original TTL)
        let result = cmd_restore(
            &[Bytes::from("dst"), Bytes::from("0"), dumped],
            &s,
        )
        .await;
        assert_eq!(result, RespValue::ok());
        let val = s.get(&Bytes::from("dst")).unwrap();
        match &val.data {
            DataType::String(v) => assert_eq!(v, &Bytes::from("hello")),
            _ => panic!("expected string"),
        };
    }
    #[tokio::test]
    async fn t_dump_restore_list() {
        let s = st();
        let mut l = std::collections::VecDeque::new();
        l.push_back(Bytes::from("a"));
        l.push_back(Bytes::from("b"));
        s.set(Bytes::from("mylist"), DataType::List(l), None);
        let dumped = match cmd_dump(&[Bytes::from("mylist")], &s).await {
            RespValue::BulkString(Some(b)) => b,
            _ => panic!("expected bulk string from DUMP"),
        };
        let result = cmd_restore(
            &[Bytes::from("mylist2"), Bytes::from("0"), dumped],
            &s,
        )
        .await;
        assert_eq!(result, RespValue::ok());
        let val = s.get(&Bytes::from("mylist2")).unwrap();
        match &val.data {
            DataType::List(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Bytes::from("a"));
                assert_eq!(items[1], Bytes::from("b"));
            }
            _ => panic!("expected list"),
        };
    }
    #[tokio::test]
    async fn t_dump_missing() {
        let s = st();
        assert_eq!(
            cmd_dump(&[Bytes::from("nokey")], &s).await,
            RespValue::BulkString(None)
        );
    }
}
