use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Store};

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------
pub fn sadd(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'sadd' command".into());
    }
    let key = &args[0];
    let members = &args[1..];

    let mut entry = store
        .get(key)
        .map(|e| e.data.clone())
        .unwrap_or_else(|| DataType::Set(HashSet::new()));

    let set = match &mut entry {
        DataType::Set(s) => s,
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    };

    let mut added: i64 = 0;
    for m in members {
        if set.insert(m.clone()) {
            added += 1;
        }
    }

    store.set(key.clone(), entry, None);
    RespValue::Integer(added)
}

// ---------------------------------------------------------------------------
// SMEMBERS key
// ---------------------------------------------------------------------------
pub fn smembers(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'smembers' command".into());
    }
    let key = &args[0];

    let entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => {
            return RespValue::Array(Some(Vec::new()));
        }
    };

    match entry {
        DataType::Set(s) => {
            let arr: Vec<RespValue> = s.iter().map(|m| bulk(m.clone())).collect();
            RespValue::Array(Some(arr))
        }
        _ => RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        ),
    }
}

// ---------------------------------------------------------------------------
// SISMEMBER key member
// ---------------------------------------------------------------------------
pub fn sismember(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'sismember' command".into());
    }
    let key = &args[0];
    let member = &args[1];

    let entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => return RespValue::Integer(0),
    };

    match entry {
        DataType::Set(s) => {
            if s.contains(member) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        _ => RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        ),
    }
}

// ---------------------------------------------------------------------------
// SMISMEMBER key member [member ...]
// ---------------------------------------------------------------------------
pub fn smismember(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'smismember' command".into());
    }
    let key = &args[0];
    let members = &args[1..];

    let entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => {
            return RespValue::Array(Some(
                members.iter().map(|_| RespValue::Integer(0)).collect(),
            ));
        }
    };

    match entry {
        DataType::Set(s) => {
            let arr: Vec<RespValue> = members
                .iter()
                .map(|m| {
                    if s.contains(m) {
                        RespValue::Integer(1)
                    } else {
                        RespValue::Integer(0)
                    }
                })
                .collect();
            RespValue::Array(Some(arr))
        }
        _ => RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        ),
    }
}

// ---------------------------------------------------------------------------
// SCARD key
// ---------------------------------------------------------------------------
pub fn scard(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'scard' command".into());
    }
    let key = &args[0];

    let entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => return RespValue::Integer(0),
    };

    match entry {
        DataType::Set(s) => RespValue::Integer(s.len() as i64),
        _ => RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        ),
    }
}

// ---------------------------------------------------------------------------
// SREM key member [member ...]
// ---------------------------------------------------------------------------
pub fn srem(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'srem' command".into());
    }
    let key = &args[0];
    let members = &args[1..];

    let mut entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => return RespValue::Integer(0),
    };

    let set = match &mut entry {
        DataType::Set(s) => s,
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    };

    let mut removed: i64 = 0;
    for m in members {
        if set.remove(m) {
            removed += 1;
        }
    }

    store.set(key.clone(), entry, None);
    RespValue::Integer(removed)
}

// ---------------------------------------------------------------------------
// SPOP key [count]
// ---------------------------------------------------------------------------
pub fn spop(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::Error("ERR wrong number of arguments for 'spop' command".into());
    }
    let key = &args[0];
    let count: usize = if args.len() == 2 {
        match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => {
                return RespValue::Error("ERR value is not an integer or out of range".into());
            }
        }
    } else {
        1
    };

    let mut entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => {
            return if args.len() == 2 {
                RespValue::Array(Some(Vec::new()))
            } else {
                RespValue::BulkString(None)
            };
        }
    };

    let set = match &mut entry {
        DataType::Set(s) => s,
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    };

    if set.is_empty() {
        return if args.len() == 2 {
            RespValue::Array(Some(Vec::new()))
        } else {
            RespValue::BulkString(None)
        };
    }

    let popped: Vec<Bytes> = (0..count)
        .filter_map(|_| {
            let idx = fastrand::usize(..set.len());
            let chosen = set.iter().nth(idx)?.clone();
            set.remove(&chosen);
            Some(chosen)
        })
        .collect();

    store.set(key.clone(), entry, None);

    if args.len() == 2 {
        RespValue::Array(Some(popped.into_iter().map(bulk).collect()))
    } else {
        RespValue::BulkString(popped.into_iter().next())
    }
}

// ---------------------------------------------------------------------------
// SRANDMEMBER key [count]
// ---------------------------------------------------------------------------
pub fn srandmember(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::Error("ERR wrong number of arguments for 'srandmember' command".into());
    }
    let key = &args[0];

    let entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => {
            return if args.len() == 2 {
                RespValue::Array(Some(Vec::new()))
            } else {
                RespValue::BulkString(None)
            };
        }
    };

    let set = match &entry {
        DataType::Set(s) => s,
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    };

    if set.is_empty() {
        return if args.len() == 2 {
            RespValue::Array(Some(Vec::new()))
        } else {
            RespValue::BulkString(None)
        };
    }

    let mut rng = fastrand::Rng::new();

    if args.len() == 2 {
        let count: i64 = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => {
                return RespValue::Error("ERR value is not an integer or out of range".into());
            }
        };

        let count_abs = count.unsigned_abs() as usize;
        let allow_duplicates = count < 0;

        let result: Vec<RespValue> = if allow_duplicates {
            (0..count_abs)
                .map(|_| {
                    let idx = rng.usize(..set.len());
                    let m = set.iter().nth(idx).unwrap().clone();
                    bulk(m)
                })
                .collect()
        } else {
            let limit = count_abs.min(set.len());
            let mut chosen: Vec<Bytes> = set.iter().cloned().collect();
            // Fisher-Yates shuffle using fastrand
            for i in (1..chosen.len()).rev() {
                let j = rng.usize(..=i);
                chosen.swap(i, j);
            }
            chosen.into_iter().take(limit).map(bulk).collect()
        };

        RespValue::Array(Some(result))
    } else {
        let idx = rng.usize(..set.len());
        let m = set.iter().nth(idx).unwrap().clone();
        RespValue::BulkString(Some(m))
    }
}

// ---------------------------------------------------------------------------
// SMOVE source dest member
// ---------------------------------------------------------------------------
pub fn smove(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'smove' command".into());
    }
    let source = &args[0];
    let dest = &args[1];
    let member = &args[2];

    let mut src_entry = match store.get(source) {
        Some(e) => e.data.clone(),
        None => return RespValue::Integer(0),
    };

    let src_set = match &mut src_entry {
        DataType::Set(s) => s,
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    };

    if !src_set.remove(member) {
        return RespValue::Integer(0);
    }

    store.set(source.clone(), src_entry, None);

    let mut dst_entry = store
        .get(dest)
        .map(|e| e.data.clone())
        .unwrap_or_else(|| DataType::Set(HashSet::new()));

    match &mut dst_entry {
        DataType::Set(s) => {
            s.insert(member.clone());
        }
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    }

    store.set(dest.clone(), dst_entry, None);
    RespValue::Integer(1)
}

// ---------------------------------------------------------------------------
// SUNION key [key ...]
// ---------------------------------------------------------------------------
pub fn sunion(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'sunion' command".into());
    }

    let mut result_set: HashSet<Bytes> = HashSet::new();
    for key in args {
        if let Some(entry) = store.get(key) {
            match &entry.data {
                DataType::Set(s) => result_set.extend(s.iter().cloned()),
                _ => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    );
                }
            }
        }
    }

    let arr: Vec<RespValue> = result_set.into_iter().map(bulk).collect();
    RespValue::Array(Some(arr))
}

// ---------------------------------------------------------------------------
// SINTER key [key ...]
// ---------------------------------------------------------------------------
pub fn sinter(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'sinter' command".into());
    }

    let mut sets: Vec<HashSet<Bytes>> = Vec::new();
    for key in args {
        if let Some(entry) = store.get(key) {
            match &entry.data {
                DataType::Set(s) => sets.push(s.clone()),
                _ => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    );
                }
            }
        } else {
            return RespValue::Array(Some(Vec::new()));
        }
    }

    if sets.is_empty() {
        return RespValue::Array(Some(Vec::new()));
    }

    let min_idx = sets
        .iter()
        .enumerate()
        .min_by_key(|(_, s)| s.len())
        .map(|(i, _)| i)
        .unwrap_or(0);

    let mut result: HashSet<Bytes> = sets[min_idx].clone();
    for (i, s) in sets.iter().enumerate() {
        if i == min_idx {
            continue;
        }
        result.retain(|item| s.contains(item));
    }

    let arr: Vec<RespValue> = result.into_iter().map(bulk).collect();
    RespValue::Array(Some(arr))
}

// ---------------------------------------------------------------------------
// SDIFF key [key ...]
// ---------------------------------------------------------------------------
pub fn sdiff(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'sdiff' command".into());
    }

    let mut sets: Vec<HashSet<Bytes>> = Vec::new();
    for (idx, key) in args.iter().enumerate() {
        if let Some(entry) = store.get(key) {
            match &entry.data {
                DataType::Set(s) => sets.push(s.clone()),
                _ => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    );
                }
            }
        } else if idx == 0 {
            return RespValue::Array(Some(Vec::new()));
        } else {
            sets.push(HashSet::new());
        }
    }

    if sets.is_empty() {
        return RespValue::Array(Some(Vec::new()));
    }

    let mut result = sets[0].clone();
    for s in &sets[1..] {
        result.retain(|item| !s.contains(item));
    }

    let arr: Vec<RespValue> = result.into_iter().map(bulk).collect();
    RespValue::Array(Some(arr))
}

// ---------------------------------------------------------------------------
// SUNIONSTORE dest key [key ...]
// ---------------------------------------------------------------------------
pub fn sunionstore(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'sunionstore' command".into());
    }
    let dest = &args[0];
    let keys = &args[1..];

    let mut result_set: HashSet<Bytes> = HashSet::new();
    for key in keys {
        if let Some(entry) = store.get(key) {
            match &entry.data {
                DataType::Set(s) => result_set.extend(s.iter().cloned()),
                _ => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    );
                }
            }
        }
    }

    let count = result_set.len() as i64;
    store.set(dest.clone(), DataType::Set(result_set), None);
    RespValue::Integer(count)
}

// ---------------------------------------------------------------------------
// SINTERSTORE dest key [key ...]
// ---------------------------------------------------------------------------
pub fn sinterstore(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'sinterstore' command".into());
    }
    let dest = &args[0];
    let keys = &args[1..];

    let mut sets: Vec<HashSet<Bytes>> = Vec::new();
    for key in keys {
        if let Some(entry) = store.get(key) {
            match &entry.data {
                DataType::Set(s) => sets.push(s.clone()),
                _ => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    );
                }
            }
        } else {
            store.set(dest.clone(), DataType::Set(HashSet::new()), None);
            return RespValue::Integer(0);
        }
    }

    let result = if sets.is_empty() {
        HashSet::new()
    } else {
        let min_idx = sets
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| s.len())
            .map(|(i, _)| i)
            .unwrap_or(0);
        let mut r = sets[min_idx].clone();
        for (i, s) in sets.iter().enumerate() {
            if i == min_idx {
                continue;
            }
            r.retain(|item| s.contains(item));
        }
        r
    };

    let count = result.len() as i64;
    store.set(dest.clone(), DataType::Set(result), None);
    RespValue::Integer(count)
}

// ---------------------------------------------------------------------------
// SDIFFSTORE dest key [key ...]
// ---------------------------------------------------------------------------
pub fn sdiffstore(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'sdiffstore' command".into());
    }
    let dest = &args[0];
    let keys = &args[1..];

    let mut sets: Vec<HashSet<Bytes>> = Vec::new();
    for (idx, key) in keys.iter().enumerate() {
        if let Some(entry) = store.get(key) {
            match &entry.data {
                DataType::Set(s) => sets.push(s.clone()),
                _ => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    );
                }
            }
        } else if idx == 0 {
            store.set(dest.clone(), DataType::Set(HashSet::new()), None);
            return RespValue::Integer(0);
        } else {
            sets.push(HashSet::new());
        }
    }

    let result = if sets.is_empty() {
        HashSet::new()
    } else {
        let mut r = sets[0].clone();
        for s in &sets[1..] {
            r.retain(|item| !s.contains(item));
        }
        r
    };

    let count = result.len() as i64;
    store.set(dest.clone(), DataType::Set(result), None);
    RespValue::Integer(count)
}

// ---------------------------------------------------------------------------
// SSCAN key cursor [MATCH pattern] [COUNT count]
// ---------------------------------------------------------------------------
pub fn sscan(store: &Arc<Store>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'sscan' command".into());
    }
    let key = &args[0];
    let cursor: usize = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) => n,
        None => {
            return RespValue::Error("ERR value is not an integer or out of range".into());
        }
    };

    let mut pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = std::str::from_utf8(&args[i])
            .ok()
            .map(|s| s.to_ascii_uppercase());
        match opt.as_deref() {
            Some("MATCH") => {
                if i + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                pattern = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .map(|s| s.to_string());
                i += 2;
            }
            Some("COUNT") => {
                if i + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                count = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                if count == 0 {
                    return RespValue::Error("ERR value is not an integer or out of range".into());
                }
                i += 2;
            }
            _ => return RespValue::Error("ERR syntax error".into()),
        }
    }

    let entry = match store.get(key) {
        Some(e) => e.data.clone(),
        None => {
            return RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from("0"))),
                RespValue::Array(Some(Vec::new())),
            ]));
        }
    };

    let set = match entry {
        DataType::Set(s) => s,
        _ => {
            return RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            );
        }
    };

    let mut members: Vec<Bytes> = set.iter().cloned().collect();
    members.sort();

    let filtered: Vec<Bytes> = if let Some(pat) = pattern {
        if let Some((prefix, suffix)) = pat.split_once('*') {
            members
                .into_iter()
                .filter(|m| {
                    let s = std::str::from_utf8(m).unwrap_or("");
                    s.starts_with(prefix) && s.ends_with(suffix)
                })
                .collect()
        } else {
            members
                .into_iter()
                .filter(|m| {
                    let s = std::str::from_utf8(m).unwrap_or("");
                    s == pat.as_str()
                })
                .collect()
        }
    } else {
        members
    };

    let next_cursor = if cursor + count >= filtered.len() {
        0
    } else {
        cursor + count
    };

    let page: Vec<RespValue> = filtered
        .iter()
        .skip(cursor)
        .take(count)
        .map(|m| bulk(m.clone()))
        .collect();

    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(Bytes::from(next_cursor.to_string()))),
        RespValue::Array(Some(page)),
    ]))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
fn bulk(b: Bytes) -> RespValue {
    RespValue::BulkString(Some(b))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Arc<Store> {
        Store::new()
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    // --- SADD ---
    #[test]
    fn test_sadd_new_key() {
        let store = test_store();
        let result = sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_sadd_existing_members() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b")]);
        let result = sadd(&store, &[b("myset"), b("a"), b("b")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sadd_partial_new() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a")]);
        let result = sadd(&store, &[b("myset"), b("a"), b("b")]);
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_sadd_wrong_type() {
        let store = test_store();
        store.set(b("mykey"), DataType::String(b("hello")), None);
        let result = sadd(&store, &[b("mykey"), b("a")]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn test_sadd_syntax_error() {
        let store = test_store();
        let result = sadd(&store, &[b("myset")]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // --- SMEMBERS ---
    #[test]
    fn test_smembers_empty() {
        let store = test_store();
        let result = smembers(&store, &[b("myset")]);
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_smembers_with_data() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = smembers(&store, &[b("myset")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn test_smembers_wrong_type() {
        let store = test_store();
        store.set(b("mykey"), DataType::String(b("hello")), None);
        let result = smembers(&store, &[b("mykey")]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // --- SISMEMBER ---
    #[test]
    fn test_sismember_exists() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a")]);
        let result = sismember(&store, &[b("myset"), b("a")]);
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_not_exists() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a")]);
        let result = sismember(&store, &[b("myset"), b("b")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_missing_key() {
        let store = test_store();
        let result = sismember(&store, &[b("myset"), b("a")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_wrong_type() {
        let store = test_store();
        store.set(b("mykey"), DataType::String(b("hello")), None);
        let result = sismember(&store, &[b("mykey"), b("a")]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // --- SMISMEMBER ---
    #[test]
    fn test_smismember_mixed() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("c")]);
        let result = smismember(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Integer(0),
                RespValue::Integer(1),
            ]))
        );
    }

    #[test]
    fn test_smismember_missing_key() {
        let store = test_store();
        let result = smismember(&store, &[b("myset"), b("a"), b("b")]);
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(0), RespValue::Integer(0)]))
        );
    }

    // --- SCARD ---
    #[test]
    fn test_scard_empty() {
        let store = test_store();
        let result = scard(&store, &[b("myset")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_scard_with_data() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = scard(&store, &[b("myset")]);
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_scard_wrong_type() {
        let store = test_store();
        store.set(b("mykey"), DataType::String(b("hello")), None);
        let result = scard(&store, &[b("mykey")]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // --- SREM ---
    #[test]
    fn test_srem_existing() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = srem(&store, &[b("myset"), b("a"), b("c")]);
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(scard(&store, &[b("myset")]), RespValue::Integer(1));
    }

    #[test]
    fn test_srem_missing_member() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a")]);
        let result = srem(&store, &[b("myset"), b("b")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_srem_missing_key() {
        let store = test_store();
        let result = srem(&store, &[b("myset"), b("a")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    // --- SPOP ---
    #[test]
    fn test_spop_single() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = spop(&store, &[b("myset")]);
        assert!(matches!(result, RespValue::BulkString(Some(_))));
        assert_eq!(scard(&store, &[b("myset")]), RespValue::Integer(2));
    }

    #[test]
    fn test_spop_with_count() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = spop(&store, &[b("myset"), b("2")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 2);
        assert_eq!(scard(&store, &[b("myset")]), RespValue::Integer(1));
    }

    #[test]
    fn test_spop_missing_key() {
        let store = test_store();
        let result = spop(&store, &[b("myset")]);
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[test]
    fn test_spop_missing_key_with_count() {
        let store = test_store();
        let result = spop(&store, &[b("myset"), b("2")]);
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // --- SRANDMEMBER ---
    #[test]
    fn test_srandmember_single() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = srandmember(&store, &[b("myset")]);
        assert!(matches!(result, RespValue::BulkString(Some(_))));
        // No removal
        assert_eq!(scard(&store, &[b("myset")]), RespValue::Integer(3));
    }

    #[test]
    fn test_srandmember_with_positive_count() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b"), b("c")]);
        let result = srandmember(&store, &[b("myset"), b("2")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_srandmember_with_negative_count() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a"), b("b")]);
        let result = srandmember(&store, &[b("myset"), b("-5")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 5);
    }

    #[test]
    fn test_srandmember_missing_key() {
        let store = test_store();
        let result = srandmember(&store, &[b("myset")]);
        assert_eq!(result, RespValue::BulkString(None));
    }

    // --- SMOVE ---
    #[test]
    fn test_smove_success() {
        let store = test_store();
        sadd(&store, &[b("src"), b("a"), b("b")]);
        let result = smove(&store, &[b("src"), b("dst"), b("a")]);
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            sismember(&store, &[b("src"), b("a")]),
            RespValue::Integer(0)
        );
        assert_eq!(
            sismember(&store, &[b("dst"), b("a")]),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_smove_not_in_source() {
        let store = test_store();
        sadd(&store, &[b("src"), b("a")]);
        let result = smove(&store, &[b("src"), b("dst"), b("b")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_smove_missing_source() {
        let store = test_store();
        let result = smove(&store, &[b("src"), b("dst"), b("a")]);
        assert_eq!(result, RespValue::Integer(0));
    }

    // --- SUNION ---
    #[test]
    fn test_sunion_basic() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a"), b("b")]);
        sadd(&store, &[b("s2"), b("b"), b("c")]);
        let result = sunion(&store, &[b("s1"), b("s2")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn test_sunion_with_missing_key() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a")]);
        let result = sunion(&store, &[b("s1"), b("missing")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 1);
    }

    #[test]
    fn test_sunion_empty() {
        let store = test_store();
        let result = sunion(&store, &[b("missing1"), b("missing2")]);
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // --- SINTER ---
    #[test]
    fn test_sinter_basic() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a"), b("b"), b("c")]);
        sadd(&store, &[b("s2"), b("b"), b("c"), b("d")]);
        let result = sinter(&store, &[b("s1"), b("s2")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_sinter_no_overlap() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a")]);
        sadd(&store, &[b("s2"), b("b")]);
        let result = sinter(&store, &[b("s1"), b("s2")]);
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_sinter_missing_key() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a")]);
        let result = sinter(&store, &[b("s1"), b("missing")]);
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // --- SDIFF ---
    #[test]
    fn test_sdiff_basic() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a"), b("b"), b("c")]);
        sadd(&store, &[b("s2"), b("b")]);
        let result = sdiff(&store, &[b("s1"), b("s2")]);
        let arr = match result {
            RespValue::Array(Some(a)) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_sdiff_first_missing() {
        let store = test_store();
        sadd(&store, &[b("s2"), b("a")]);
        let result = sdiff(&store, &[b("missing"), b("s2")]);
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    // --- SUNIONSTORE ---
    #[test]
    fn test_sunionstore_basic() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a"), b("b")]);
        sadd(&store, &[b("s2"), b("b"), b("c")]);
        let result = sunionstore(&store, &[b("dest"), b("s1"), b("s2")]);
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(scard(&store, &[b("dest")]), RespValue::Integer(3));
    }

    // --- SINTERSTORE ---
    #[test]
    fn test_sinterstore_basic() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a"), b("b")]);
        sadd(&store, &[b("s2"), b("b"), b("c")]);
        let result = sinterstore(&store, &[b("dest"), b("s1"), b("s2")]);
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            sismember(&store, &[b("dest"), b("b")]),
            RespValue::Integer(1)
        );
    }

    // --- SDIFFSTORE ---
    #[test]
    fn test_sdiffstore_basic() {
        let store = test_store();
        sadd(&store, &[b("s1"), b("a"), b("b")]);
        sadd(&store, &[b("s2"), b("b")]);
        let result = sdiffstore(&store, &[b("dest"), b("s1"), b("s2")]);
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            sismember(&store, &[b("dest"), b("a")]),
            RespValue::Integer(1)
        );
    }

    // --- SSCAN ---
    #[test]
    fn test_sscan_full_iteration() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a1"), b("a2"), b("b1"), b("b2")]);

        let result = sscan(&store, &[b("myset"), b("0"), b("COUNT"), b("2")]);
        let (cursor, items) = match result {
            RespValue::Array(Some(arr)) if arr.len() == 2 => {
                let c = match &arr[0] {
                    RespValue::BulkString(Some(b)) => {
                        std::str::from_utf8(b).unwrap().parse::<usize>().unwrap()
                    }
                    _ => panic!("expected bulk cursor"),
                };
                let items = match &arr[1] {
                    RespValue::Array(Some(a)) => a.clone(),
                    _ => panic!("expected array"),
                };
                (c, items)
            }
            _ => panic!("expected [cursor, [items]]"),
        };
        assert_eq!(items.len(), 2);
        assert!(cursor > 0);

        let cursor_bytes = b(&cursor.to_string());
        let result2 = sscan(&store, &[b("myset"), cursor_bytes, b("COUNT"), b("2")]);
        let (cursor2, items2) = match result2 {
            RespValue::Array(Some(arr)) if arr.len() == 2 => {
                let c = match &arr[0] {
                    RespValue::BulkString(Some(b)) => {
                        std::str::from_utf8(b).unwrap().parse::<usize>().unwrap()
                    }
                    _ => panic!("expected bulk cursor"),
                };
                let items = match &arr[1] {
                    RespValue::Array(Some(a)) => a.clone(),
                    _ => panic!("expected array"),
                };
                (c, items)
            }
            _ => panic!("expected [cursor, [items]]"),
        };
        assert_eq!(items2.len(), 2);
        assert_eq!(cursor2, 0);
    }

    #[test]
    fn test_sscan_with_match() {
        let store = test_store();
        sadd(&store, &[b("myset"), b("a1"), b("a2"), b("b1")]);
        let result = sscan(
            &store,
            &[b("myset"), b("0"), b("MATCH"), b("a*"), b("COUNT"), b("10")],
        );
        let items = match result {
            RespValue::Array(Some(arr)) if arr.len() == 2 => match &arr[1] {
                RespValue::Array(Some(a)) => a.clone(),
                _ => panic!("expected array"),
            },
            _ => panic!("expected [cursor, [items]]"),
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_sscan_missing_key() {
        let store = test_store();
        let result = sscan(&store, &[b("myset"), b("0")]);
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("0"))),
                RespValue::Array(Some(vec![])),
            ]))
        );
    }

    #[test]
    fn test_sscan_wrong_type() {
        let store = test_store();
        store.set(b("mykey"), DataType::String(b("hello")), None);
        let result = sscan(&store, &[b("mykey"), b("0")]);
        assert!(matches!(result, RespValue::Error(_)));
    }
}

// ---------------------------------------------------------------------------
// Dispatch handle
// ---------------------------------------------------------------------------

pub fn handle(cmd: &[Bytes], store: &Arc<Store>) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".into());
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    let args = &cmd[1..];
    match name.as_str() {
        "SADD" => sadd(store, args),
        "SMEMBERS" => smembers(store, args),
        "SISMEMBER" => sismember(store, args),
        "SMISMEMBER" => smismember(store, args),
        "SCARD" => scard(store, args),
        "SREM" => srem(store, args),
        "SPOP" => spop(store, args),
        "SRANDMEMBER" => srandmember(store, args),
        "SMOVE" => smove(store, args),
        "SUNION" => sunion(store, args),
        "SINTER" => sinter(store, args),
        "SDIFF" => sdiff(store, args),
        "SUNIONSTORE" => sunionstore(store, args),
        "SINTERSTORE" => sinterstore(store, args),
        "SDIFFSTORE" => sdiffstore(store, args),
        "SSCAN" => sscan(store, args),
        _ => RespValue::Error(format!("ERR unknown command '{}'", name).into()),
    }
}
