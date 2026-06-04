use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

pub async fn handle(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments".into()); }
    let cmd = match std::str::from_utf8(&args[0]) { Ok(s) => s.to_ascii_uppercase(), Err(_) => return RespValue::Error("ERR invalid command name".into()) };
    match cmd.as_str() {
        "LPUSH" => cmd_lpush(&args[1..], store).await,
        "RPUSH" => cmd_rpush(&args[1..], store).await,
        "LPOP" => cmd_lpop(&args[1..], store).await,
        "RPOP" => cmd_rpop(&args[1..], store).await,
        "LRANGE" => cmd_lrange(&args[1..], store).await,
        "LLEN" => cmd_llen(&args[1..], store).await,
        "LINDEX" => cmd_lindex(&args[1..], store).await,
        "LSET" => cmd_lset(&args[1..], store).await,
        "LINSERT" => cmd_linsert(&args[1..], store).await,
        "LREM" => cmd_lrem(&args[1..], store).await,
        "LTRIM" => cmd_ltrim(&args[1..], store).await,
        "LMOVE" => cmd_lmove(&args[1..], store).await,
        "BLPOP" => cmd_blpop(&args[1..], store).await,
        "BRPOP" => cmd_brpop(&args[1..], store).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", cmd)),
    }
}

async fn cmd_lpush(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'lpush' command".into()); }
    let mut e = s.keyspace.entry(a[0].clone()).or_insert_with(|| Entry::new(DataType::List(VecDeque::new()), None));
    let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
    for v in &a[1..] { l.push_front(v.clone()); } RespValue::Integer(l.len() as i64)
}
async fn cmd_rpush(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'rpush' command".into()); }
    let mut e = s.keyspace.entry(a[0].clone()).or_insert_with(|| Entry::new(DataType::List(VecDeque::new()), None));
    let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
    for v in &a[1..] { l.push_back(v.clone()); } RespValue::Integer(l.len() as i64)
}
async fn cmd_lpop(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'lpop' command".into()); }
    let c = if a.len() >= 2 { match parse_usize(&a[1]) { Some(n) => n, None => return RespValue::Error("ERR value is not an integer or out of range".into()) } } else { 1 };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if l.is_empty() { return RespValue::BulkString(None); }
            let n = c.min(l.len());
            if n == 1 { let v = l.pop_front().unwrap(); if l.is_empty() { drop(e); s.keyspace.remove(&a[0]); } RespValue::BulkString(Some(v)) }
            else { let mut r = Vec::with_capacity(n); for _ in 0..n { r.push(l.pop_front().unwrap()); } if l.is_empty() { drop(e); s.keyspace.remove(&a[0]); } RespValue::Array(Some(r.into_iter().map(|v| RespValue::BulkString(Some(v))).collect())) }
        }
        None => if c > 1 { RespValue::Array(Some(Vec::new())) } else { RespValue::BulkString(None) }
    }
}
async fn cmd_rpop(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'rpop' command".into()); }
    let c = if a.len() >= 2 { match parse_usize(&a[1]) { Some(n) => n, None => return RespValue::Error("ERR value is not an integer or out of range".into()) } } else { 1 };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if l.is_empty() { return RespValue::BulkString(None); }
            let n = c.min(l.len());
            if n == 1 { let v = l.pop_back().unwrap(); if l.is_empty() { drop(e); s.keyspace.remove(&a[0]); } RespValue::BulkString(Some(v)) }
            else { let mut r = Vec::with_capacity(n); for _ in 0..n { r.push(l.pop_back().unwrap()); } if l.is_empty() { drop(e); s.keyspace.remove(&a[0]); } RespValue::Array(Some(r.into_iter().map(|v| RespValue::BulkString(Some(v))).collect())) }
        }
        None => if c > 1 { RespValue::Array(Some(Vec::new())) } else { RespValue::BulkString(None) }
    }
}
async fn cmd_lrange(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'lrange' command".into()); }
    let st = match parse_i64(&a[1]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    let sp = match parse_i64(&a[2]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    match s.keyspace.get(&a[0]) {
        Some(e) => { let l = match &e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if l.is_empty() { return RespValue::Array(Some(Vec::new())); }
            let len = l.len() as i64; let si = normalize_index(st, len); let ei = normalize_index(sp, len);
            if si > ei || si >= len { return RespValue::Array(Some(Vec::new())); }
            let ei = ei.min(len - 1); let mut r = Vec::new(); for i in si..=ei { r.push(l[i as usize].clone()); }
            RespValue::Array(Some(r.into_iter().map(|v| RespValue::BulkString(Some(v))).collect()))
        }
        None => RespValue::Array(Some(Vec::new())),
    }
}
async fn cmd_llen(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'llen' command".into()); }
    match s.keyspace.get(&a[0]) {
        Some(e) => match &e.data { DataType::List(l) => RespValue::Integer(l.len() as i64), _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) },
        None => RespValue::Integer(0),
    }
}
async fn cmd_lindex(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'lindex' command".into()); }
    let idx = match parse_i64(&a[1]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    match s.keyspace.get(&a[0]) {
        Some(e) => { let l = match &e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if l.is_empty() { return RespValue::BulkString(None); }
            let len = l.len() as i64; let i = normalize_index(idx, len);
            if i < 0 || i >= len { RespValue::BulkString(None) } else { RespValue::BulkString(Some(l[i as usize].clone())) }
        }
        None => RespValue::BulkString(None),
    }
}
async fn cmd_lset(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'lset' command".into()); }
    let idx = match parse_i64(&a[1]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            let len = l.len() as i64; let i = normalize_index(idx, len);
            if i < 0 || i >= len { RespValue::Error("ERR index out of range".into()) } else { l[i as usize] = a[2].clone(); RespValue::SimpleString("OK".into()) }
        }
        None => RespValue::Error("ERR no such key".into()),
    }
}
async fn cmd_linsert(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 4 { return RespValue::Error("ERR wrong number of arguments for 'linsert' command".into()); }
    let before = match std::str::from_utf8(&a[1]) { Ok(s) => match s.to_ascii_uppercase().as_str() { "BEFORE" => true, "AFTER" => false, _ => return RespValue::Error("ERR syntax error".into()) }, Err(_) => return RespValue::Error("ERR syntax error".into()) };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            match l.iter().position(|v| v == &a[2]) { Some(p) => { let ip = if before { p } else { p + 1 }; l.insert(ip, a[3].clone()); RespValue::Integer(l.len() as i64) } None => RespValue::Integer(-1) }
        }
        None => RespValue::Integer(-1),
    }
}
async fn cmd_lrem(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'lrem' command".into()); }
    let c = match parse_i64(&a[1]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            let lb = l.len();
            if c > 0 { let mut rm = c; let mut i = 0; while i < l.len() && rm > 0 { if l[i] == a[2] { l.remove(i); rm -= 1; } else { i += 1; } } }
            else if c < 0 { let mut rm = c.abs(); let mut i = l.len() as i64 - 1; while i >= 0 && rm > 0 { if l[i as usize] == a[2] { l.remove(i as usize); rm -= 1; } i -= 1; } }
            else { l.retain(|v| v != &a[2]); }
            let r = (lb - l.len()) as i64; if l.is_empty() { drop(e); s.keyspace.remove(&a[0]); } RespValue::Integer(r)
        }
        None => RespValue::Integer(0),
    }
}
async fn cmd_ltrim(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'ltrim' command".into()); }
    let st = match parse_i64(&a[1]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    let sp = match parse_i64(&a[2]) { Some(v) => v, None => return RespValue::Error("ERR value is not an integer or out of range".into()) };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if l.is_empty() { return RespValue::SimpleString("OK".into()); }
            let len = l.len() as i64; let si = normalize_index(st, len); let ei = normalize_index(sp, len);
            if si > ei || si >= len { l.clear(); } else { let si = si.max(0) as usize; let ei = (ei.min(len - 1)) as usize; let mut nl = VecDeque::new(); for i in si..=ei { nl.push_back(l[i].clone()); } *l = nl; }
            RespValue::SimpleString("OK".into())
        }
        None => RespValue::SimpleString("OK".into()),
    }
}
async fn cmd_lmove(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 4 { return RespValue::Error("ERR wrong number of arguments for 'lmove' command".into()); }
    let fl = match std::str::from_utf8(&a[2]) { Ok(x) => match x.to_ascii_uppercase().as_str() { "LEFT" => true, "RIGHT" => false, _ => return RespValue::Error("ERR syntax error".into()) }, Err(_) => return RespValue::Error("ERR syntax error".into()) };
    let tl = match std::str::from_utf8(&a[3]) { Ok(x) => match x.to_ascii_uppercase().as_str() { "LEFT" => true, "RIGHT" => false, _ => return RespValue::Error("ERR syntax error".into()) }, Err(_) => return RespValue::Error("ERR syntax error".into()) };
    let popped = match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if l.is_empty() { None } else { let v = if fl { l.pop_front().unwrap() } else { l.pop_back().unwrap() }; if l.is_empty() { drop(e); s.keyspace.remove(&a[0]); } Some(v) }
        }
        None => None,
    };
    match popped {
        Some(val) => { let mut e = s.keyspace.entry(a[1].clone()).or_insert_with(|| Entry::new(DataType::List(VecDeque::new()), None));
            let l = match &mut e.data { DataType::List(l) => l, _ => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) };
            if tl { l.push_front(val.clone()); } else { l.push_back(val.clone()); } RespValue::BulkString(Some(val))
        }
        None => RespValue::BulkString(None),
    }
}
async fn cmd_blpop(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'blpop' command".into()); }
    let _t = match parse_f64(&a[a.len()-1]) { Some(v) => v, None => return RespValue::Error("ERR timeout is not a float or out of range".into()) };
    for key in &a[..a.len()-1] { match s.keyspace.get_mut(key) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => continue };
            if !l.is_empty() { let v = l.pop_front().unwrap(); if l.is_empty() { drop(e); s.keyspace.remove(key); } return RespValue::Array(Some(vec![RespValue::BulkString(Some(key.clone())), RespValue::BulkString(Some(v))])); }
        }
        None => continue,
    }}
    RespValue::BulkString(None)
}
async fn cmd_brpop(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'brpop' command".into()); }
    let _t = match parse_f64(&a[a.len()-1]) { Some(v) => v, None => return RespValue::Error("ERR timeout is not a float or out of range".into()) };
    for key in &a[..a.len()-1] { match s.keyspace.get_mut(key) {
        Some(mut e) => { let l = match &mut e.data { DataType::List(l) => l, _ => continue };
            if !l.is_empty() { let v = l.pop_back().unwrap(); if l.is_empty() { drop(e); s.keyspace.remove(key); } return RespValue::Array(Some(vec![RespValue::BulkString(Some(key.clone())), RespValue::BulkString(Some(v))])); }
        }
        None => continue,
    }}
    RespValue::BulkString(None)
}
fn normalize_index(i: i64, len: i64) -> i64 { if i < 0 { let r = len + i; if r < 0 { 0 } else { r } } else { i } }
fn parse_i64(b: &Bytes) -> Option<i64> { std::str::from_utf8(b).ok()?.parse::<i64>().ok() }
fn parse_usize(b: &Bytes) -> Option<usize> { std::str::from_utf8(b).ok()?.parse::<usize>().ok() }
fn parse_f64(b: &Bytes) -> Option<f64> { std::str::from_utf8(b).ok()?.parse::<f64>().ok() }

#[cfg(test)]
mod tests {
    use super::*;
    fn b(s: &str) -> Bytes { Bytes::from(s.to_string()) }

    #[tokio::test] async fn test_lpush() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        assert_eq!(handle(&[b("LPUSH"),b("L"),b("a"),b("b")],&s).await, RespValue::Integer(2));
    }
    #[tokio::test] async fn test_rpush() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        assert_eq!(handle(&[b("RPUSH"),b("L"),b("a"),b("b")],&s).await, RespValue::Integer(2));
    }
    #[tokio::test] async fn test_lpop() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("x"),b("y")],&s).await;
        assert_eq!(handle(&[b("LPOP"),b("L")],&s).await, RespValue::BulkString(Some(b("x"))));
    }
    #[tokio::test] async fn test_rpop() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("x"),b("y")],&s).await;
        assert_eq!(handle(&[b("RPOP"),b("L")],&s).await, RespValue::BulkString(Some(b("y"))));
    }
    #[tokio::test] async fn test_llen() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b")],&s).await;
        assert_eq!(handle(&[b("LLEN"),b("L")],&s).await, RespValue::Integer(2));
        assert_eq!(handle(&[b("LLEN"),b("n")],&s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn test_lindex() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("c")],&s).await;
        assert_eq!(handle(&[b("LINDEX"),b("L"),b("1")],&s).await, RespValue::BulkString(Some(b("b"))));
        assert_eq!(handle(&[b("LINDEX"),b("L"),b("-1")],&s).await, RespValue::BulkString(Some(b("c"))));
        assert_eq!(handle(&[b("LINDEX"),b("L"),b("5")],&s).await, RespValue::BulkString(None));
    }
    #[tokio::test] async fn test_lrange() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("c"),b("d")],&s).await;
        assert_eq!(handle(&[b("LRANGE"),b("L"),b("1"),b("2")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b"))),RespValue::BulkString(Some(b("c")))])));
    }
    #[tokio::test] async fn test_lrange_neg() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("c")],&s).await;
        assert_eq!(handle(&[b("LRANGE"),b("L"),b("-2"),b("-1")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b"))),RespValue::BulkString(Some(b("c")))])));
    }
    #[tokio::test] async fn test_lset_ok() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b")],&s).await;
        assert_eq!(handle(&[b("LSET"),b("L"),b("0"),b("x")],&s).await, RespValue::SimpleString("OK".into()));
    }
    #[tokio::test] async fn test_lset_oor() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a")],&s).await;
        assert!(matches!(handle(&[b("LSET"),b("L"),b("5"),b("x")],&s).await, RespValue::Error(_)));
    }
    #[tokio::test] async fn test_linsert_before() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("c")],&s).await;
        assert_eq!(handle(&[b("LINSERT"),b("L"),b("BEFORE"),b("c"),b("b")],&s).await, RespValue::Integer(3));
    }
    #[tokio::test] async fn test_linsert_after() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("c")],&s).await;
        assert_eq!(handle(&[b("LINSERT"),b("L"),b("AFTER"),b("c"),b("b")],&s).await, RespValue::Integer(3));
    }
    #[tokio::test] async fn test_linsert_nf() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a")],&s).await;
        assert_eq!(handle(&[b("LINSERT"),b("L"),b("BEFORE"),b("z"),b("x")],&s).await, RespValue::Integer(-1));
    }
    #[tokio::test] async fn test_lrem_all() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("a")],&s).await;
        assert_eq!(handle(&[b("LREM"),b("L"),b("0"),b("a")],&s).await, RespValue::Integer(2));
    }
    #[tokio::test] async fn test_lrem_pos() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("a")],&s).await;
        assert_eq!(handle(&[b("LREM"),b("L"),b("1"),b("a")],&s).await, RespValue::Integer(1));
    }
    #[tokio::test] async fn test_ltrim() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("c"),b("d")],&s).await;
        handle(&[b("LTRIM"),b("L"),b("1"),b("2")],&s).await;
        assert_eq!(handle(&[b("LRANGE"),b("L"),b("0"),b("-1")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b"))),RespValue::BulkString(Some(b("c")))])));
    }
    #[tokio::test] async fn test_lmove_lr() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("src"),b("a"),b("b")],&s).await;
        assert_eq!(handle(&[b("LMOVE"),b("src"),b("dst"),b("LEFT"),b("RIGHT")],&s).await, RespValue::BulkString(Some(b("a"))));
    }
    #[tokio::test] async fn test_lmove_none() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        assert_eq!(handle(&[b("LMOVE"),b("n"),b("d"),b("LEFT"),b("RIGHT")],&s).await, RespValue::BulkString(None));
    }
    #[tokio::test] async fn test_blpop() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a")],&s).await;
        assert_eq!(handle(&[b("BLPOP"),b("L"),b("1")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("L"))),RespValue::BulkString(Some(b("a")))])));
    }
    #[tokio::test] async fn test_brpop() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b")],&s).await;
        assert_eq!(handle(&[b("BRPOP"),b("L"),b("1")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("L"))),RespValue::BulkString(Some(b("b")))])));
    }
    #[tokio::test] async fn test_lpop_count() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("c")],&s).await;
        assert_eq!(handle(&[b("LPOP"),b("L"),b("2")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("a"))),RespValue::BulkString(Some(b("b")))])));
    }
    #[tokio::test] async fn test_pop_deletes_empty() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a")],&s).await;
        handle(&[b("LPOP"),b("L")],&s).await;
        assert_eq!(handle(&[b("LLEN"),b("L")],&s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn test_lmove_rotate() {
        let s = Arc::new(Store { keyspace: dashmap::DashMap::new() });
        handle(&[b("RPUSH"),b("L"),b("a"),b("b"),b("c")],&s).await;
        handle(&[b("LMOVE"),b("L"),b("L"),b("RIGHT"),b("LEFT")],&s).await;
        assert_eq!(handle(&[b("LRANGE"),b("L"),b("0"),b("-1")],&s).await, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("c"))),RespValue::BulkString(Some(b("a"))),RespValue::BulkString(Some(b("b")))])));
    }
}
