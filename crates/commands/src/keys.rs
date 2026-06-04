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
        "UNLINK" => cmd_unlink(&args[1..], store).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", cmd)),
    }
}

async fn cmd_exists(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'exists' command".into()); }
    RespValue::Integer(args.iter().filter(|k| store.exists(k)).count() as i64)
}

async fn cmd_type(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 { return RespValue::Error("ERR wrong number of arguments for 'type' command".into()); }
    match store.type_of(&args[0]) {
        Some(t) => RespValue::SimpleString(t.to_string()),
        None => RespValue::SimpleString("none".to_string()),
    }
}

async fn cmd_rename(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'rename' command".into()); }
    let key = &args[0];
    let newkey = args[1].clone();
    let (data, ttl) = match store.get(key) {
        Some(e) => {
            let ttl_dur = e.expires_at.map(|at| { let now = Instant::now(); if at > now { at.duration_since(now) } else { Duration::ZERO } });
            (e.data.clone(), ttl_dur)
        }
        None => return RespValue::Error("ERR no such key".into()),
    };
    store.del(key);
    store.set(newkey, data, ttl);
    RespValue::ok()
}

async fn cmd_renamenx(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'renamenx' command".into()); }
    let key = &args[0];
    let newkey = &args[1];
    if store.exists(newkey) { return RespValue::Integer(0); }
    match store.get(key) {
        Some(e) => {
            let ttl = e.expires_at.map(|at| { let now = Instant::now(); if at > now { at.duration_since(now) } else { Duration::ZERO } });
            store.set(newkey.clone(), e.data.clone(), ttl);
            store.del(key);
            RespValue::Integer(1)
        }
        None => RespValue::Integer(0),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireCond { None, NX, XX, GT, LT }

fn parse_expire_cond(s: &str) -> ExpireCond {
    match s.to_ascii_uppercase().as_str() {
        "NX" => ExpireCond::NX, "XX" => ExpireCond::XX,
        "GT" => ExpireCond::GT, "LT" => ExpireCond::LT,
        _ => ExpireCond::None,
    }
}

fn check_expire_cond(store: &Store, key: &Bytes, new_dur: Duration, cond: ExpireCond) -> bool {
    let current_ttl = store.ttl(key);
    match cond {
        ExpireCond::None => true,
        ExpireCond::NX => current_ttl.is_none(),
        ExpireCond::XX => current_ttl.is_some(),
        ExpireCond::GT => match current_ttl { Some(cur) => new_dur > cur, None => true },
        ExpireCond::LT => match current_ttl { Some(cur) => new_dur < cur, None => false },
    }
}

async fn cmd_expire(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'expire' command".into()); }
    let key = &args[0];
    let seconds: i64 = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let cond = if args.len() >= 3 { parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or("")) } else { ExpireCond::None };
    if !store.exists(key) { return RespValue::Integer(0); }
    let dur = Duration::from_secs(seconds.max(0) as u64);
    if !check_expire_cond(store, key, dur, cond) { return RespValue::Integer(0); }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}

async fn cmd_pexpire(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'pexpire' command".into()); }
    let key = &args[0];
    let ms: i64 = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let cond = if args.len() >= 3 { parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or("")) } else { ExpireCond::None };
    if !store.exists(key) { return RespValue::Integer(0); }
    let dur = Duration::from_millis(ms.max(0) as u64);
    if !check_expire_cond(store, key, dur, cond) { return RespValue::Integer(0); }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}

async fn cmd_expireat(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'expireat' command".into()); }
    let key = &args[0];
    let ts: i64 = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let cond = if args.len() >= 3 { parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or("")) } else { ExpireCond::None };
    if !store.exists(key) { return RespValue::Integer(0); }
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let dur = Duration::from_secs((ts - now_secs).max(0) as u64);
    if !check_expire_cond(store, key, dur, cond) { return RespValue::Integer(0); }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}

async fn cmd_pexpireat(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'pexpireat' command".into()); }
    let key = &args[0];
    let ts_ms: i64 = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let cond = if args.len() >= 3 { parse_expire_cond(std::str::from_utf8(&args[2]).unwrap_or("")) } else { ExpireCond::None };
    if !store.exists(key) { return RespValue::Integer(0); }
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let dur = Duration::from_millis((ts_ms - now_ms).max(0) as u64);
    if !check_expire_cond(store, key, dur, cond) { return RespValue::Integer(0); }
    store.expire(key, Instant::now() + dur);
    RespValue::Integer(1)
}

async fn cmd_ttl(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 { return RespValue::Error("ERR wrong number of arguments for 'ttl' command".into()); }
    let key = &args[0];
    if !store.exists(key) { return RespValue::Integer(-2); }
    match store.ttl(key) { Some(dur) => RespValue::Integer(dur.as_secs() as i64), None => RespValue::Integer(-1) }
}

async fn cmd_pttl(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 { return RespValue::Error("ERR wrong number of arguments for 'pttl' command".into()); }
    let key = &args[0];
    if !store.exists(key) { return RespValue::Integer(-2); }
    match store.ttl(key) { Some(dur) => RespValue::Integer(dur.as_millis() as i64), None => RespValue::Integer(-1) }
}

async fn cmd_persist(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 { return RespValue::Error("ERR wrong number of arguments for 'persist' command".into()); }
    let key = &args[0];
    match store.get(key) {
        Some(entry) => {
            if entry.expires_at.is_some() { store.set(key.clone(), entry.data.clone(), None); RespValue::Integer(1) }
            else { RespValue::Integer(0) }
        }
        None => RespValue::Integer(0),
    }
}

async fn cmd_keys(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 { return RespValue::Error("ERR wrong number of arguments for 'keys' command".into()); }
    let pattern = std::str::from_utf8(&args[0]).ok().map(|s| s.to_string()).unwrap_or_default();
    let keys = store.keys(&pattern);
    RespValue::Array(Some(keys.into_iter().map(|k| RespValue::BulkString(Some(k))).collect()))
}

async fn cmd_scan(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'scan' command".into()); }
    let cursor: u64 = std::str::from_utf8(&args[0]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let mut pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        let arg = std::str::from_utf8(&args[i]).ok().map(|s| s.to_ascii_uppercase()).unwrap_or_default();
        match arg.as_str() {
            "MATCH" => { i += 1; if i < args.len() { pattern = std::str::from_utf8(&args[i]).ok().map(|s| s.to_string()); } }
            "COUNT" => { i += 1; if i < args.len() { count = std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse().ok()).unwrap_or(10); } }
            "TYPE" => { i += 1; if i < args.len() { type_filter = std::str::from_utf8(&args[i]).ok().map(|s| s.to_string()); } }
            _ => {}
        }
        i += 1;
    }
    let mut all_keys: Vec<Bytes> = store.keys("*");
    all_keys.retain(|k| store.exists(k));
    all_keys.sort();
    if let Some(ref tname) = type_filter { all_keys.retain(|k| store.type_of(k).map(|t| t == tname).unwrap_or(false)); }
    if let Some(ref pat) = pattern {
        if pat != "*" {
            match pat.split_once('*') {
                Some((p, s)) => { let (pre, suf) = (p.to_string(), s.to_string()); all_keys.retain(|k| { let ks = std::str::from_utf8(k).unwrap_or(""); ks.starts_with(&pre) && ks.ends_with(&suf) }); }
                None => { let exact = Bytes::from(pat.clone()); all_keys.retain(|k| k == &exact); }
            }
        }
    }
    let start = cursor as usize;
    if start >= all_keys.len() { return RespValue::Array(Some(vec![RespValue::BulkString(Some(Bytes::from("0"))), RespValue::Array(None)])); }
    let end = (start + count).min(all_keys.len());
    let chunk: Vec<RespValue> = all_keys[start..end].iter().map(|k| RespValue::BulkString(Some(k.clone()))).collect();
    let next_cursor = if end >= all_keys.len() { 0 } else { end as u64 };
    RespValue::Array(Some(vec![RespValue::BulkString(Some(Bytes::from(next_cursor.to_string()))), RespValue::Array(Some(chunk))]))
}

async fn cmd_randomkey(_args: &[Bytes], store: &Arc<Store>) -> RespValue {
    let keys: Vec<Bytes> = store.keys("*");
    if keys.is_empty() { return RespValue::BulkString(None); }
    let mut rng = rand::thread_rng();
    RespValue::BulkString(Some(keys.choose(&mut rng).unwrap().clone()))
}

async fn cmd_move(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'move' command".into()); }
    RespValue::Integer(0)
}

async fn cmd_copy(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'copy' command".into()); }
    let source = &args[0];
    let dest = &args[1];
    let replace = args.len() >= 3 && std::str::from_utf8(&args[2]).map(|s| s.to_ascii_uppercase() == "REPLACE").unwrap_or(false);
    let source_entry = match store.get(source) { Some(e) => e, None => return RespValue::Integer(0) };
    if !replace && store.exists(dest) { return RespValue::Integer(0); }
    store.set(dest.clone(), source_entry.data.clone(), None);
    RespValue::Integer(1)
}

async fn cmd_object(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'object' command".into()); }
    let subcmd = std::str::from_utf8(&args[0]).ok().map(|s| s.to_ascii_uppercase()).unwrap_or_default();
    let key = &args[1];
    match subcmd.as_str() {
        "ENCODING" => {
            if !store.exists(key) { return RespValue::Error("ERR no such key".into()); }
            let encoding = match store.type_of(key) {
                Some("string") => "embstr", Some("list") => "quicklist", Some("hash") => "hashtable",
                Some("set") => "hashtable", Some("zset") => "skiplist", Some("stream") => "listpack", _ => "unknown",
            };
            RespValue::BulkString(Some(Bytes::from(encoding)))
        }
        "REFCOUNT" => { if !store.exists(key) { return RespValue::Error("ERR no such key".into()); } RespValue::Integer(1) }
        "IDLETIME" => { if !store.exists(key) { return RespValue::Error("ERR no such key".into()); } RespValue::Integer(0) }
        "FREQ" => { if !store.exists(key) { return RespValue::Error("ERR no such key".into()); } RespValue::Integer(0) }
        _ => RespValue::Error("ERR syntax error".into()),
    }
}

async fn cmd_dump(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'dump' command".into()); }
    RespValue::Error("ERR DUMP not supported yet".into())
}

async fn cmd_restore(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'restore' command".into()); }
    RespValue::Error("ERR RESTORE not supported yet".into())
}

async fn cmd_wait(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'wait' command".into()); }
    RespValue::Integer(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortOrder { Asc, Desc }

async fn cmd_sort(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'sort' command".into()); }
    let key = &args[0];
    let mut limit: Option<(usize, usize)> = None;
    let mut order = SortOrder::Asc;
    let mut alpha = false;
    let mut store_dest: Option<Bytes> = None;
    let mut i = 1;
    while i < args.len() {
        let arg = std::str::from_utf8(&args[i]).ok().map(|s| s.to_ascii_uppercase()).unwrap_or_default();
        match arg.as_str() {
            "LIMIT" => { i += 1; if i + 1 < args.len() { limit = Some((std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse().ok()).unwrap_or(0), std::str::from_utf8(&args[i+1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0))); i += 1; } }
            "ASC" => order = SortOrder::Asc, "DESC" => order = SortOrder::Desc,
            "ALPHA" => alpha = true,
            "STORE" => { i += 1; if i < args.len() { store_dest = Some(args[i].clone()); } }
            "BY" | "GET" => { i += 1; }
            _ => {}
        }
        i += 1;
    }
    let items: Vec<Bytes> = match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::List(list) => list.iter().cloned().collect(),
            DataType::Set(set) => set.iter().cloned().collect(),
            DataType::ZSet(zset) => zset.members.keys().cloned().collect(),
            _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
        },
        None => return RespValue::Array(None),
    };
    let mut sorted = items;
    if alpha {
        let mut strs: Vec<(String, Bytes)> = sorted.into_iter().map(|b| (String::from_utf8_lossy(&b).to_string(), b)).collect();
        strs.sort_by(|a, b| a.0.cmp(&b.0));
        if order == SortOrder::Desc { strs.reverse(); }
        sorted = strs.into_iter().map(|(_, b)| b).collect();
    } else {
        let mut nums: Vec<(f64, Bytes)> = sorted.into_iter().map(|b| (String::from_utf8_lossy(&b).parse::<f64>().unwrap_or(0.0), b)).collect();
        nums.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if order == SortOrder::Desc { nums.reverse(); }
        sorted = nums.into_iter().map(|(_, b)| b).collect();
    }
    if let Some((offset, cnt)) = limit {
        let start = offset.min(sorted.len());
        let end = (offset + cnt).min(sorted.len());
        sorted = sorted[start..end].to_vec();
    }
    if let Some(dest) = store_dest {
        store.set(dest, DataType::List(sorted.iter().cloned().collect()), None);
        return RespValue::Integer(sorted.len() as i64);
    }
    RespValue::Array(Some(sorted.into_iter().map(|b| RespValue::BulkString(Some(b))).collect()))
}

async fn cmd_unlink(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'unlink' command".into()); }
    RespValue::Integer(args.iter().filter(|k| store.del(k)).count() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_store() -> Arc<Store> { Store::new() }

    #[tokio::test] async fn exists_single() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        assert_eq!(cmd_exists(&[Bytes::from("a")], &s).await, RespValue::Integer(1));
    }
    #[tokio::test] async fn exists_multiple() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        assert_eq!(cmd_exists(&[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")], &s).await, RespValue::Integer(2));
    }
    #[tokio::test] async fn exists_duplicates_counted() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        assert_eq!(cmd_exists(&[Bytes::from("a"), Bytes::from("a")], &s).await, RespValue::Integer(2));
    }
    #[tokio::test] async fn type_string() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_type(&[Bytes::from("k")], &s).await, RespValue::SimpleString("string".into()));
    }
    #[tokio::test] async fn type_none() {
        let s = test_store();
        assert_eq!(cmd_type(&[Bytes::from("missing")], &s).await, RespValue::SimpleString("none".into()));
    }
    #[tokio::test] async fn rename_ok() {
        let s = test_store(); s.set(Bytes::from("old"), DataType::String(Bytes::from("val")), None);
        assert_eq!(cmd_rename(&[Bytes::from("old"), Bytes::from("new")], &s).await, RespValue::ok());
        assert!(s.get(&Bytes::from("old")).is_none());
        assert!(s.get(&Bytes::from("new")).is_some());
    }
    #[tokio::test] async fn rename_no_such_key() {
        let s = test_store();
        assert_eq!(cmd_rename(&[Bytes::from("nope"), Bytes::from("new")], &s).await, RespValue::Error("ERR no such key".into()));
    }
    #[tokio::test] async fn renamenx_success() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        assert_eq!(cmd_renamenx(&[Bytes::from("a"), Bytes::from("b")], &s).await, RespValue::Integer(1));
    }
    #[tokio::test] async fn renamenx_dest_exists() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        assert_eq!(cmd_renamenx(&[Bytes::from("a"), Bytes::from("b")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn expire_sets_ttl() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_expire(&[Bytes::from("k"), Bytes::from("60")], &s).await, RespValue::Integer(1));
        match cmd_ttl(&[Bytes::from("k")], &s).await { RespValue::Integer(n) => assert!(n > 0 && n <= 60), _ => panic!("expected integer TTL") }
    }
    #[tokio::test] async fn expire_missing_key() {
        let s = test_store();
        assert_eq!(cmd_expire(&[Bytes::from("nope"), Bytes::from("60")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn ttl_no_expiry() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_ttl(&[Bytes::from("k")], &s).await, RespValue::Integer(-1));
    }
    #[tokio::test] async fn ttl_not_found() {
        let s = test_store();
        assert_eq!(cmd_ttl(&[Bytes::from("nope")], &s).await, RespValue::Integer(-2));
    }
    #[tokio::test] async fn pttl_returns_ms() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_pexpire(&[Bytes::from("k"), Bytes::from("60000")], &s).await, RespValue::Integer(1));
        match cmd_pttl(&[Bytes::from("k")], &s).await { RespValue::Integer(n) => assert!(n > 0 && n <= 60000), _ => panic!("expected integer PTTL") }
    }
    #[tokio::test] async fn persist_removes_ttl() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), Some(Duration::from_secs(60)));
        assert_eq!(cmd_persist(&[Bytes::from("k")], &s).await, RespValue::Integer(1));
        assert_eq!(cmd_ttl(&[Bytes::from("k")], &s).await, RespValue::Integer(-1));
    }
    #[tokio::test] async fn persist_no_ttl() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_persist(&[Bytes::from("k")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn expire_nx_only_if_no_expiry() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_expire(&[Bytes::from("k"), Bytes::from("60"), Bytes::from("NX")], &s).await, RespValue::Integer(1));
        assert_eq!(cmd_expire(&[Bytes::from("k"), Bytes::from("120"), Bytes::from("NX")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn expire_xx_only_if_has_expiry() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_expire(&[Bytes::from("k"), Bytes::from("60"), Bytes::from("XX")], &s).await, RespValue::Integer(0));
        s.expire(&Bytes::from("k"), Instant::now() + Duration::from_secs(60));
        assert_eq!(cmd_expire(&[Bytes::from("k"), Bytes::from("120"), Bytes::from("XX")], &s).await, RespValue::Integer(1));
    }
    #[tokio::test] async fn keys_glob() {
        let s = test_store(); s.set(Bytes::from("user:1"), DataType::String(Bytes::from("a")), None);
        s.set(Bytes::from("user:2"), DataType::String(Bytes::from("b")), None);
        s.set(Bytes::from("post:1"), DataType::String(Bytes::from("c")), None);
        match cmd_keys(&[Bytes::from("user:*")], &s).await { RespValue::Array(Some(items)) => assert_eq!(items.len(), 2), _ => panic!("expected array") }
    }
    #[tokio::test] async fn scan_cursor_basic() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        s.set(Bytes::from("c"), DataType::String(Bytes::from("3")), None);
        let r1 = cmd_scan(&[Bytes::from("0"), Bytes::from("COUNT"), Bytes::from("2")], &s).await;
        let (nc1, items1) = match_resp_array2(&r1);
        assert_eq!(items1.len(), 2); assert_eq!(nc1, 2);
        let r2 = cmd_scan(&[Bytes::from("2"), Bytes::from("COUNT"), Bytes::from("2")], &s).await;
        let (nc2, items2) = match_resp_array2(&r2);
        assert_eq!(items2.len(), 1); assert_eq!(nc2, 0);
    }
    #[tokio::test] async fn scan_with_match() {
        let s = test_store(); s.set(Bytes::from("user:1"), DataType::String(Bytes::from("a")), None);
        s.set(Bytes::from("user:2"), DataType::String(Bytes::from("b")), None);
        s.set(Bytes::from("post:1"), DataType::String(Bytes::from("c")), None);
        let r = cmd_scan(&[Bytes::from("0"), Bytes::from("MATCH"), Bytes::from("user:*")], &s).await;
        let (_, items) = match_resp_array2(&r);
        assert_eq!(items.len(), 2);
    }
    #[tokio::test] async fn randomkey_returns_something() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        match cmd_randomkey(&[], &s).await { RespValue::BulkString(Some(k)) => assert!(k == Bytes::from("a") || k == Bytes::from("b")), _ => panic!("expected bulk string") }
    }
    #[tokio::test] async fn randomkey_empty() {
        let s = test_store();
        assert_eq!(cmd_randomkey(&[], &s).await, RespValue::BulkString(None));
    }
    #[tokio::test] async fn move_always_zero() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_move(&[Bytes::from("k"), Bytes::from("1")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn copy_basic() {
        let s = test_store(); s.set(Bytes::from("src"), DataType::String(Bytes::from("val")), None);
        assert_eq!(cmd_copy(&[Bytes::from("src"), Bytes::from("dst")], &s).await, RespValue::Integer(1));
        assert!(s.get(&Bytes::from("src")).is_some()); assert!(s.get(&Bytes::from("dst")).is_some());
    }
    #[tokio::test] async fn copy_dest_exists_no_replace() {
        let s = test_store(); s.set(Bytes::from("src"), DataType::String(Bytes::from("val")), None);
        s.set(Bytes::from("dst"), DataType::String(Bytes::from("other")), None);
        assert_eq!(cmd_copy(&[Bytes::from("src"), Bytes::from("dst")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn copy_dest_exists_with_replace() {
        let s = test_store(); s.set(Bytes::from("src"), DataType::String(Bytes::from("val")), None);
        s.set(Bytes::from("dst"), DataType::String(Bytes::from("other")), None);
        assert_eq!(cmd_copy(&[Bytes::from("src"), Bytes::from("dst"), Bytes::from("REPLACE")], &s).await, RespValue::Integer(1));
        let entry = s.get(&Bytes::from("dst")).unwrap();
        match &entry.data { DataType::String(v) => assert_eq!(v, &Bytes::from("val")), _ => panic!("expected string") };
    }
    #[tokio::test] async fn copy_missing_source() {
        let s = test_store();
        assert_eq!(cmd_copy(&[Bytes::from("nope"), Bytes::from("dst")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn copy_deep_clone() {
        let s = test_store(); let mut list = VecDeque::new(); list.push_back(Bytes::from("item1")); list.push_back(Bytes::from("item2"));
        s.set(Bytes::from("src"), DataType::List(list), None);
        assert_eq!(cmd_copy(&[Bytes::from("src"), Bytes::from("dst")], &s).await, RespValue::Integer(1));
        let src_entry = s.get(&Bytes::from("src"));
        if let Some(e) = src_entry { if let DataType::List(l) = &e.data { assert_eq!(l.len(), 2); } };
        let dst_entry = s.get(&Bytes::from("dst"));
        if let Some(e) = dst_entry { if let DataType::List(l) = &e.data { assert_eq!(l.len(), 2); } };
    }
    #[tokio::test] async fn object_encoding_string() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_object(&[Bytes::from("ENCODING"), Bytes::from("k")], &s).await, RespValue::BulkString(Some(Bytes::from("embstr"))));
    }
    #[tokio::test] async fn object_refcount() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_object(&[Bytes::from("REFCOUNT"), Bytes::from("k")], &s).await, RespValue::Integer(1));
    }
    #[tokio::test] async fn object_idletime() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert_eq!(cmd_object(&[Bytes::from("IDLETIME"), Bytes::from("k")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn dump_not_supported() {
        let s = test_store(); s.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        match cmd_dump(&[Bytes::from("k")], &s).await { RespValue::Error(_) => {}, _ => panic!("expected error") }
    }
    #[tokio::test] async fn restore_not_supported() {
        let s = test_store();
        match cmd_restore(&[Bytes::from("k"), Bytes::from("0"), Bytes::from("data")], &s).await { RespValue::Error(_) => {}, _ => panic!("expected error") }
    }
    #[tokio::test] async fn wait_returns_zero() {
        let s = test_store();
        assert_eq!(cmd_wait(&[Bytes::from("1"), Bytes::from("1000")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn sort_numeric_asc() {
        let s = test_store(); let mut list = VecDeque::new();
        list.push_back(Bytes::from("3")); list.push_back(Bytes::from("1")); list.push_back(Bytes::from("2"));
        s.set(Bytes::from("mylist"), DataType::List(list), None);
        match cmd_sort(&[Bytes::from("mylist")], &s).await {
            RespValue::Array(Some(items)) => { assert_eq!(items.len(), 3); assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from("1")))); }
            _ => panic!("expected array"),
        }
    }
    #[tokio::test] async fn sort_alpha_desc() {
        let s = test_store(); let mut list = VecDeque::new();
        list.push_back(Bytes::from("banana")); list.push_back(Bytes::from("apple")); list.push_back(Bytes::from("cherry"));
        s.set(Bytes::from("fruits"), DataType::List(list), None);
        match cmd_sort(&[Bytes::from("fruits"), Bytes::from("DESC"), Bytes::from("ALPHA")], &s).await {
            RespValue::Array(Some(items)) => { assert_eq!(items.len(), 3); assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from("cherry")))); }
            _ => panic!("expected array"),
        }
    }
    #[tokio::test] async fn sort_with_limit() {
        let s = test_store(); let mut list = VecDeque::new();
        for i in 1..=10 { list.push_back(Bytes::from(i.to_string())); }
        s.set(Bytes::from("nums"), DataType::List(list), None);
        match cmd_sort(&[Bytes::from("nums"), Bytes::from("LIMIT"), Bytes::from("2"), Bytes::from("3")], &s).await {
            RespValue::Array(Some(items)) => { assert_eq!(items.len(), 3); assert_eq!(items[0], RespValue::BulkString(Some(Bytes::from("3")))); }
            _ => panic!("expected array"),
        }
    }
    #[tokio::test] async fn sort_store_dest() {
        let s = test_store(); let mut list = VecDeque::new();
        list.push_back(Bytes::from("3")); list.push_back(Bytes::from("1")); list.push_back(Bytes::from("2"));
        s.set(Bytes::from("src"), DataType::List(list), None);
        assert_eq!(cmd_sort(&[Bytes::from("src"), Bytes::from("STORE"), Bytes::from("sorted")], &s).await, RespValue::Integer(3));
        let entry = s.get(&Bytes::from("sorted")).unwrap();
        match &entry.data { DataType::List(l) => { assert_eq!(l.len(), 3); assert_eq!(l[0], Bytes::from("1")); } _ => panic!("expected list") };
    }
    #[tokio::test] async fn unlink_deletes_keys() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        s.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        s.set(Bytes::from("c"), DataType::String(Bytes::from("3")), None);
        assert_eq!(cmd_unlink(&[Bytes::from("a"), Bytes::from("b"), Bytes::from("nope")], &s).await, RespValue::Integer(2));
        assert!(s.get(&Bytes::from("a")).is_none()); assert!(s.get(&Bytes::from("b")).is_none()); assert!(s.get(&Bytes::from("c")).is_some());
    }
    #[tokio::test] async fn dispatch_exists() {
        let s = test_store(); s.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        assert_eq!(handle(&[Bytes::from("EXISTS"), Bytes::from("a")], &s).await, RespValue::Integer(1));
    }
    #[tokio::test] async fn dispatch_unknown_command() {
        let s = test_store();
        match handle(&[Bytes::from("FOOBAR")], &s).await { RespValue::Error(msg) => assert!(msg.contains("unknown command")), _ => panic!("expected error") }
    }

    fn match_resp_array2(v: &RespValue) -> (i64, Vec<RespValue>) {
        match v {
            RespValue::Array(Some(top)) if top.len() == 2 => {
                let nc = match &top[0] { RespValue::BulkString(Some(b)) => std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0), _ => panic!("expected bulk string cursor") };
                let items = match &top[1] { RespValue::Array(Some(a)) => a.clone(), _ => panic!("expected array") };
                (nc, items)
            }
            _ => panic!("expected array of 2"),
        }
    }
}
