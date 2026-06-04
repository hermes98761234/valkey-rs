use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;

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

async fn cmd_lpush(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'lpush' command".into()); }
    let key = &args[0];
    let values = &args[1..];
    let keyspace = &store.keyspace;
    let mut entry = keyspace.entry(key.clone()).or_insert_with(|| Entry::new(DataType::List(VecDeque::new()), None));
    let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
    for v in values { list.push_front(v.clone()); }
    RespValue::Integer(list.len() as i64)
}

async fn cmd_rpush(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'rpush' command".into()); }
    let key = &args[0];
    let values = &args[1..];
    let keyspace = &store.keyspace;
    let mut entry = keyspace.entry(key.clone()).or_insert_with(|| Entry::new(DataType::List(VecDeque::new()), None));
    let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
    for v in values { list.push_back(v.clone()); }
    RespValue::Integer(list.len() as i64)
}

async fn cmd_lpop(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'lpop' command".into()); }
    let key = &args[0];
    let count = if args.len() >= 2 { match parse_usize(&args[1]) { Some(n) => n, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } } } else { 1 };
    let keyspace = &store.keyspace;
    match keyspace.get_mut(key) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if list.is_empty() { return RespValue::BulkString(None); }
            let n = count.min(list.len());
            if n == 1 { let val = list.pop_front().unwrap(); if list.is_empty() { drop(entry); keyspace.remove(key); } RespValue::BulkString(Some(val)) }
            else { let mut result = Vec::with_capacity(n); for _ in 0..n { result.push(list.pop_front().unwrap()); } if list.is_empty() { drop(entry); keyspace.remove(key); } RespValue::Array(Some(result.into_iter().map(|v| RespValue::BulkString(Some(v))).collect())) }
        }
        None => { if count > 1 { RespValue::Array(Some(Vec::new())) } else { RespValue::BulkString(None) } }
    }
}

async fn cmd_rpop(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'rpop' command".into()); }
    let key = &args[0];
    let count = if args.len() >= 2 { match parse_usize(&args[1]) { Some(n) => n, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } } } else { 1 };
    let keyspace = &store.keyspace;
    match keyspace.get_mut(key) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if list.is_empty() { return RespValue::BulkString(None); }
            let n = count.min(list.len());
            if n == 1 { let val = list.pop_back().unwrap(); if list.is_empty() { drop(entry); keyspace.remove(key); } RespValue::BulkString(Some(val)) }
            else { let mut result = Vec::with_capacity(n); for _ in 0..n { result.push(list.pop_back().unwrap()); } if list.is_empty() { drop(entry); keyspace.remove(key); } RespValue::Array(Some(result.into_iter().map(|v| RespValue::BulkString(Some(v))).collect())) }
        }
        None => { if count > 1 { RespValue::Array(Some(Vec::new())) } else { RespValue::BulkString(None) } }
    }
}

async fn cmd_lrange(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'lrange' command".into()); }
    let key = &args[0];
    let start = match parse_i64(&args[1]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let stop = match parse_i64(&args[2]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let keyspace = &store.keyspace;
    match keyspace.get(key) {
        Some(entry) => {
            let list = match &entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if list.is_empty() { return RespValue::Array(Some(Vec::new())); }
            let len = list.len() as i64;
            let start_idx = normalize_index(start, len);
            let stop_idx = normalize_index(stop, len);
            if start_idx > stop_idx || start_idx >= len { return RespValue::Array(Some(Vec::new())); }
            let stop_idx = stop_idx.min(len - 1);
            let mut result = Vec::new();
            for i in start_idx..=stop_idx { result.push(list[i as usize].clone()); }
            RespValue::Array(Some(result.into_iter().map(|v| RespValue::BulkString(Some(v))).collect()))
        }
        None => RespValue::Array(Some(Vec::new())),
    }
}

async fn cmd_llen(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() { return RespValue::Error("ERR wrong number of arguments for 'llen' command".into()); }
    let key = &args[0];
    let keyspace = &store.keyspace;
    match keyspace.get(key) {
        Some(entry) => match &entry.data { DataType::List(l) => RespValue::Integer(l.len() as i64), _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()) },
        None => RespValue::Integer(0),
    }
}

async fn cmd_lindex(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'lindex' command".into()); }
    let key = &args[0];
    let index = match parse_i64(&args[1]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let keyspace = &store.keyspace;
    match keyspace.get(key) {
        Some(entry) => {
            let list = match &entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if list.is_empty() { return RespValue::BulkString(None); }
            let len = list.len() as i64;
            let idx = normalize_index(index, len);
            if idx < 0 || idx >= len { RespValue::BulkString(None) } else { RespValue::BulkString(Some(list[idx as usize].clone())) }
        }
        None => RespValue::BulkString(None),
    }
}

async fn cmd_lset(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'lset' command".into()); }
    let key = &args[0];
    let index = match parse_i64(&args[1]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let value = args[2].clone();
    let keyspace = &store.keyspace;
    match keyspace.get_mut(key) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            let len = list.len() as i64;
            let idx = normalize_index(index, len);
            if idx < 0 || idx >= len { RespValue::Error("ERR index out of range".into()) } else { list[idx as usize] = value; RespValue::SimpleString("OK".into()) }
        }
        None => RespValue::Error("ERR no such key".into()),
    }
}

async fn cmd_linsert(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 4 { return RespValue::Error("ERR wrong number of arguments for 'linsert' command".into()); }
    let key = &args[0];
    let before = match std::str::from_utf8(&args[1]) { Ok(s) => match s.to_ascii_uppercase().as_str() { "BEFORE" => true, "AFTER" => false, _ => return RespValue::Error("ERR syntax error".into()) }, Err(_) => return RespValue::Error("ERR syntax error".into()) };
    let pivot = args[2].clone();
    let value = args[3].clone();
    let keyspace = &store.keyspace;
    match keyspace.get_mut(key) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            match list.iter().position(|v| v == &pivot) {
                Some(pos) => { let insert_pos = if before { pos } else { pos + 1 }; list.insert(insert_pos, value); RespValue::Integer(list.len() as i64) }
                None => RespValue::Integer(-1),
            }
        }
        None => RespValue::Integer(-1),
    }
}

async fn cmd_lrem(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'lrem' command".into()); }
    let key = &args[0];
    let count = match parse_i64(&args[1]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let value = args[2].clone();
    let keyspace = &store.keyspace;
    match keyspace.get_mut(key) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            let len_before = list.len();
            if count > 0 { let mut remaining = count; let mut i = 0; while i < list.len() && remaining > 0 { if list[i] == value { list.remove(i); remaining -= 1; } else { i += 1; } } }
            else if count < 0 { let mut remaining = count.abs(); let mut i = list.len() as i64 - 1; while i >= 0 && remaining > 0 { if list[i as usize] == value { list.remove(i as usize); remaining -= 1; } i -= 1; } }
            else { list.retain(|v| v != &value); }
            let removed = (len_before - list.len()) as i64;
            if list.is_empty() { drop(entry); keyspace.remove(key); }
            RespValue::Integer(removed)
        }
        None => RespValue::Integer(0),
    }
}

async fn cmd_ltrim(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 3 { return RespValue::Error("ERR wrong number of arguments for 'ltrim' command".into()); }
    let key = &args[0];
    let start = match parse_i64(&args[1]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let stop = match parse_i64(&args[2]) { Some(v) => v, None => { return RespValue::Error("ERR value is not an integer or out of range".into()); } };
    let keyspace = &store.keyspace;
    match keyspace.get_mut(key) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if list.is_empty() { return RespValue::SimpleString("OK".into()); }
            let len = list.len() as i64;
            let start_idx = normalize_index(start, len);
            let stop_idx = normalize_index(stop, len);
            if start_idx > stop_idx || start_idx >= len { list.clear(); } else { let si = start_idx.max(0) as usize; let ei = (stop_idx.min(len - 1)) as usize; let mut nl = VecDeque::new(); for i in si..=ei { nl.push_back(list[i].clone()); } *list = nl; }
            RespValue::SimpleString("OK".into())
        }
        None => RespValue::SimpleString("OK".into()),
    }
}

async fn cmd_lmove(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 4 { return RespValue::Error("ERR wrong number of arguments for 'lmove' command".into()); }
    let source = &args[0];
    let dest = &args[1];
    let from_left = match std::str::from_utf8(&args[2]) { Ok(s) => match s.to_ascii_uppercase().as_str() { "LEFT" => true, "RIGHT" => false, _ => return RespValue::Error("ERR syntax error".into()) }, Err(_) => return RespValue::Error("ERR syntax error".into()) };
    let to_left = match std::str::from_utf8(&args[3]) { Ok(s) => match s.to_ascii_uppercase().as_str() { "LEFT" => true, "RIGHT" => false, _ => return RespValue::Error("ERR syntax error".into()) }, Err(_) => return RespValue::Error("ERR syntax error".into()) };
    let keyspace = &store.keyspace;
    let popped = match keyspace.get_mut(source) {
        Some(mut entry) => {
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if list.is_empty() { None } else {
                let val = if from_left { list.pop_front().unwrap() } else { list.pop_back().unwrap() };
                if list.is_empty() { drop(entry); keyspace.remove(source); }
                Some(val)
            }
        }
        None => None,
    };
    match popped {
        Some(val) => {
            let mut entry = keyspace.entry(dest.clone()).or_insert_with(|| Entry::new(DataType::List(VecDeque::new()), None));
            let list = match &mut entry.data { DataType::List(l) => l, _ => { return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()); } };
            if to_left { list.push_front(val.clone()); } else { list.push_back(val.clone()); }
            RespValue::BulkString(Some(val))
        }
        None => RespValue::BulkString(None),
    }
}

async fn cmd_blpop(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'blpop' command".into()); }
    let _timeout = match parse_f64(&args[args.len() - 1]) { Some(v) => v, None => { return RespValue::Error("ERR timeout is not a float or out of range".into()); } };
    let keys = &args[..args.len() - 1];
    let keyspace = &store.keyspace;
    for key in keys {
        match keyspace.get_mut(key) {
            Some(mut entry) => {
                let list = match &mut entry.data { DataType::List(l) => l, _ => continue, };
                if !list.is_empty() { let val = list.pop_front().unwrap(); if list.is_empty() { drop(entry); keyspace.remove(key); } return RespValue::Array(Some(vec![RespValue::BulkString(Some(key.clone())), RespValue::BulkString(Some(val))])); }
            }
            None => continue,
        }
    }
    RespValue::BulkString(None)
}

async fn cmd_brpop(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 { return RespValue::Error("ERR wrong number of arguments for 'brpop' command".into()); }
    let _timeout = match parse_f64(&args[args.len() - 1]) { Some(v) => v, None => { return RespValue::Error("ERR timeout is not a float or out of range".into()); } };
    let keys = &args[..args.len() - 1];
    let keyspace = &store.keyspace;
    for key in keys {
        match keyspace.get_mut(key) {
            Some(mut entry) => {
                let list = match &mut entry.data { DataType::List(l) => l, _ => continue, };
                if !list.is_empty() { let val = list.pop_back().unwrap(); if list.is_empty() { drop(entry); keyspace.remove(key); } return RespValue::Array(Some(vec![RespValue::BulkString(Some(key.clone())), RespValue::BulkString(Some(val))])); }
            }
            None => continue,
        }
    }
    RespValue::BulkString(None)
}

fn normalize_index(index: i64, len: i64) -> i64 { if index < 0 { let idx = len + index; if idx < 0 { 0 } else { idx } } else { index } }
fn parse_i64(b: &Bytes) -> Option<i64> { std::str::from_utf8(b).ok()?.parse::<i64>().ok() }
fn parse_usize(b: &Bytes) -> Option<usize> { std::str::from_utf8(b).ok()?.parse::<usize>().ok() }
fn parse_f64(b: &Bytes) -> Option<f64> { std::str::from_utf8(b).ok()?.parse::<f64>().ok() }

#[cfg(test)]
mod tests {
    use super::*;
    fn test_store() -> Arc<Store> { Arc::new(Store { keyspace: dashmap::DashMap::new() }) }
    fn b(s: &str) -> Bytes { Bytes::from(s.to_string()) }

    #[tokio::test] async fn test_lpush_basic() {
        let s = test_store();
        let r = handle(&[b("LPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(r, RespValue::Integer(3));
    }
    #[tokio::test] async fn test_rpush_basic() {
        let s = test_store();
        let r = handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(r, RespValue::Integer(3));
    }
    #[tokio::test] async fn test_lpop_basic() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        let r = handle(&[b("LPOP"), b("mylist")], &s).await;
        assert_eq!(r, RespValue::BulkString(Some(b("a"))));
    }
    #[tokio::test] async fn test_rpop_basic() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        let r = handle(&[b("RPOP"), b("mylist")], &s).await;
        assert_eq!(r, RespValue::BulkString(Some(b("c"))));
    }
    #[tokio::test] async fn test_lrange_basic() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c"), b("d")], &s).await;
        let r = handle(&[b("LRANGE"), b("mylist"), b("1"), b("3")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b"))), RespValue::BulkString(Some(b("c"))), RespValue::BulkString(Some(b("d")))])));
    }
    #[tokio::test] async fn test_lrange_negative() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        let r = handle(&[b("LRANGE"), b("mylist"), b("-2"), b("-1")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b"))), RespValue::BulkString(Some(b("c")))])));
    }
    #[tokio::test] async fn test_llen() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b")], &s).await;
        assert_eq!(handle(&[b("LLEN"), b("mylist")], &s).await, RespValue::Integer(2));
        assert_eq!(handle(&[b("LLEN"), b("nope")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn test_lindex() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(handle(&[b("LINDEX"), b("mylist"), b("1")], &s).await, RespValue::BulkString(Some(b("b"))));
        assert_eq!(handle(&[b("LINDEX"), b("mylist"), b("-1")], &s).await, RespValue::BulkString(Some(b("c"))));
        assert_eq!(handle(&[b("LINDEX"), b("mylist"), b("5")], &s).await, RespValue::BulkString(None));
    }
    #[tokio::test] async fn test_lset() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b")], &s).await;
        assert_eq!(handle(&[b("LSET"), b("mylist"), b("0"), b("x")], &s).await, RespValue::SimpleString("OK".into()));
        assert_eq!(handle(&[b("LINDEX"), b("mylist"), b("0")], &s).await, RespValue::BulkString(Some(b("x"))));
    }
    #[tokio::test] async fn test_lset_out_of_range() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a")], &s).await;
        let r = handle(&[b("LSET"), b("mylist"), b("5"), b("x")], &s).await;
        assert!(matches!(r, RespValue::Error(_)));
    }
    #[tokio::test] async fn test_linsert() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("c")], &s).await;
        assert_eq!(handle(&[b("LINSERT"), b("mylist"), b("BEFORE"), b("c"), b("b")], &s).await, RespValue::Integer(3));
        assert_eq!(handle(&[b("LINSERT"), b("mylist"), b("BEFORE"), b("z"), b("x")], &s).await, RespValue::Integer(-1));
    }
    #[tokio::test] async fn test_lrem() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("a"), b("c"), b("a")], &s).await;
        assert_eq!(handle(&[b("LREM"), b("mylist"), b("0"), b("a")], &s).await, RespValue::Integer(3));
    }
    #[tokio::test] async fn test_lrem_positive() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("a"), b("c"), b("a")], &s).await;
        assert_eq!(handle(&[b("LREM"), b("mylist"), b("2"), b("a")], &s).await, RespValue::Integer(2));
    }
    #[tokio::test] async fn test_ltrim() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c"), b("d"), b("e")], &s).await;
        assert_eq!(handle(&[b("LTRIM"), b("mylist"), b("1"), b("3")], &s).await, RespValue::SimpleString("OK".into()));
        let r = handle(&[b("LRANGE"), b("mylist"), b("0"), b("-1")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b"))), RespValue::BulkString(Some(b("c"))), RespValue::BulkString(Some(b("d")))])));
    }
    #[tokio::test] async fn test_lmove() {
        let s = test_store();
        handle(&[b("RPUSH"), b("src"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(handle(&[b("LMOVE"), b("src"), b("dest"), b("LEFT"), b("RIGHT")], &s).await, RespValue::BulkString(Some(b("a"))));
    }
    #[tokio::test] async fn test_lmove_nonexistent() {
        let s = test_store();
        assert_eq!(handle(&[b("LMOVE"), b("nope"), b("dest"), b("LEFT"), b("RIGHT")], &s).await, RespValue::BulkString(None));
    }
    #[tokio::test] async fn test_blpop() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b")], &s).await;
        let r = handle(&[b("BLPOP"), b("mylist"), b("1")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("mylist"))), RespValue::BulkString(Some(b("a")))])));
    }
    #[tokio::test] async fn test_brpop() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b")], &s).await;
        let r = handle(&[b("BRPOP"), b("mylist"), b("1")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("mylist"))), RespValue::BulkString(Some(b("b")))])));
    }
    #[tokio::test] async fn test_lpop_count() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a"), b("b"), b("c")], &s).await;
        let r = handle(&[b("LPOP"), b("mylist"), b("2")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("a"))), RespValue::BulkString(Some(b("b")))])));
    }
    #[tokio::test] async fn test_pop_deletes_empty() {
        let s = test_store();
        handle(&[b("RPUSH"), b("mylist"), b("a")], &s).await;
        handle(&[b("LPOP"), b("mylist")], &s).await;
        assert_eq!(handle(&[b("LLEN"), b("mylist")], &s).await, RespValue::Integer(0));
    }
    #[tokio::test] async fn test_mixed_push_pop() {
        let s = test_store();
        handle(&[b("LPUSH"), b("mylist"), b("a")], &s).await;
        handle(&[b("RPUSH"), b("mylist"), b("b")], &s).await;
        handle(&[b("LPUSH"), b("mylist"), b("c")], &s).await;
        let r = handle(&[b("LRANGE"), b("mylist"), b("0"), b("-1")], &s).await;
        assert_eq!(r, RespValue::Array(Some(vec![RespValue::BulkString(Some(b("c"))), RespValue::BulkString(Some(b("a"))), RespValue::BulkString(Some(b("b")))])));
    }
}
