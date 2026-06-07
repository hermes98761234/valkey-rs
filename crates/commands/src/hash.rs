use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

fn wrongtype() -> RespValue {
    RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

pub fn handle(cmd: &[Bytes], store: &Arc<Store>) -> RespValue {
    if cmd.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments".into());
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    let args = &cmd[1..];
    match name.as_str() {
        "HSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return RespValue::Error("ERR wrong number of arguments for 'hset' command".into());
            }
            let mut e = store
                .keyspace
                .entry(args[0].clone())
                .or_insert_with(|| Entry::new(DataType::Hash(HashMap::new()), None));
            let h = match &mut e.data {
                DataType::Hash(h) => h,
                _ => return wrongtype(),
            };
            let mut added = 0i64;
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    if !h.contains_key(&chunk[0]) {
                        added += 1;
                    }
                    h.insert(chunk[0].clone(), chunk[1].clone());
                }
            }
            RespValue::int(added)
        }
        "HGET" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for 'hget' command".into());
            }
            match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::Hash(h) => match h.get(&args[1]) {
                        Some(v) => RespValue::bulk(v.clone()),
                        None => RespValue::null_bulk(),
                    },
                    _ => wrongtype(),
                },
                None => RespValue::null_bulk(),
            }
        }
        "HMGET" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hmget' command".into(),
                );
            }
            let fields = &args[1..];
            match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::Hash(h) => RespValue::array(
                        fields
                            .iter()
                            .map(|f| match h.get(f) {
                                Some(v) => RespValue::bulk(v.clone()),
                                None => RespValue::null_bulk(),
                            })
                            .collect(),
                    ),
                    _ => wrongtype(),
                },
                None => RespValue::array(fields.iter().map(|_| RespValue::null_bulk()).collect()),
            }
        }
        "HMSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hmset' command".into(),
                );
            }
            let mut e = store
                .keyspace
                .entry(args[0].clone())
                .or_insert_with(|| Entry::new(DataType::Hash(HashMap::new()), None));
            let h = match &mut e.data {
                DataType::Hash(h) => h,
                _ => return wrongtype(),
            };
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    h.insert(chunk[0].clone(), chunk[1].clone());
                }
            }
            RespValue::ok()
        }
        "HGETALL" => match store.get(&args[0]) {
            Some(entry) => match &entry.data {
                DataType::Hash(h) => {
                    let mut r = Vec::with_capacity(h.len() * 2);
                    for (k, v) in h.iter() {
                        r.push(RespValue::bulk(k.clone()));
                        r.push(RespValue::bulk(v.clone()));
                    }
                    RespValue::array(r)
                }
                _ => wrongtype(),
            },
            None => RespValue::array(Vec::new()),
        },
        "HDEL" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for 'hdel' command".into());
            }
            match store.keyspace.get_mut(&args[0]) {
                Some(mut entry) => match &mut entry.data {
                    DataType::Hash(h) => {
                        let mut count = 0i64;
                        for f in &args[1..] {
                            if h.remove(f).is_some() {
                                count += 1;
                            }
                        }
                        RespValue::int(count)
                    }
                    _ => wrongtype(),
                },
                None => RespValue::int(0),
            }
        }
        "HEXISTS" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hexists' command".into(),
                );
            }
            match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::Hash(h) => {
                        RespValue::int(if h.contains_key(&args[1]) { 1 } else { 0 })
                    }
                    _ => wrongtype(),
                },
                None => RespValue::int(0),
            }
        }
        "HLEN" => match store.get(&args[0]) {
            Some(entry) => match &entry.data {
                DataType::Hash(h) => RespValue::int(h.len() as i64),
                _ => wrongtype(),
            },
            None => RespValue::int(0),
        },
        "HKEYS" => match store.get(&args[0]) {
            Some(entry) => match &entry.data {
                DataType::Hash(h) => {
                    RespValue::array(h.keys().map(|k| RespValue::bulk(k.clone())).collect())
                }
                _ => wrongtype(),
            },
            None => RespValue::array(Vec::new()),
        },
        "HVALS" => match store.get(&args[0]) {
            Some(entry) => match &entry.data {
                DataType::Hash(h) => {
                    RespValue::array(h.values().map(|v| RespValue::bulk(v.clone())).collect())
                }
                _ => wrongtype(),
            },
            None => RespValue::array(Vec::new()),
        },
        "HINCRBY" => {
            if args.len() < 3 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hincrby' command".into(),
                );
            }
            let incr = match std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n,
                None => {
                    return RespValue::Error("ERR value is not an integer or out of range".into())
                }
            };
            let mut e = store
                .keyspace
                .entry(args[0].clone())
                .or_insert_with(|| Entry::new(DataType::Hash(HashMap::new()), None));
            let h = match &mut e.data {
                DataType::Hash(h) => h,
                _ => return wrongtype(),
            };
            let cur = h
                .get(&args[1])
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let nv = cur + incr;
            h.insert(args[1].clone(), Bytes::from(nv.to_string()));
            RespValue::int(nv)
        }
        "HINCRBYFLOAT" => {
            if args.len() < 3 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hincrbyfloat' command".into(),
                );
            }
            let incr = match std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
            {
                Some(n) => n,
                None => {
                    return RespValue::Error(
                        "ERR value is not a valid float or out of range".into(),
                    )
                }
            };
            let mut e = store
                .keyspace
                .entry(args[0].clone())
                .or_insert_with(|| Entry::new(DataType::Hash(HashMap::new()), None));
            let h = match &mut e.data {
                DataType::Hash(h) => h,
                _ => return wrongtype(),
            };
            let cur = h
                .get(&args[1])
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0_f64);
            let nv = cur + incr;
            let repr = format!("{}", nv);
            h.insert(args[1].clone(), Bytes::from(repr.clone()));
            RespValue::bulk(Bytes::from(repr))
        }
        "HSCAN" => {
            if args.is_empty() {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hscan' command".into(),
                );
            }
            let key = &args[0];
            let mut cursor = 0usize;
            let mut pattern: Option<String> = None;
            let mut count = 10usize;
            let mut i = 1;
            while i < args.len() {
                let tok = std::str::from_utf8(&args[i])
                    .unwrap_or("")
                    .to_ascii_uppercase();
                match tok.as_str() {
                    "MATCH" => {
                        if i + 1 >= args.len() {
                            return RespValue::Error("ERR syntax error".into());
                        }
                        pattern = std::str::from_utf8(&args[i + 1])
                            .ok()
                            .map(|s| s.to_string());
                        i += 2;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return RespValue::Error("ERR syntax error".into());
                        }
                        count = match std::str::from_utf8(&args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => {
                                return RespValue::Error(
                                    "ERR value is not an integer or out of range".into(),
                                )
                            }
                        };
                        i += 2;
                    }
                    _ => {
                        cursor = match std::str::from_utf8(&args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => return RespValue::Error("ERR syntax error".into()),
                        };
                        i += 1;
                    }
                }
            }
            let pairs = match store.get(key) {
                Some(entry) => match &entry.data {
                    DataType::Hash(h) => {
                        let mut sorted: Vec<(Bytes, Bytes)> =
                            h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        sorted.sort_by(|a, b| a.0.cmp(&b.0));
                        let filtered: Vec<(Bytes, Bytes)> = match &pattern {
                            Some(pat) if pat != "*" => sorted
                                .into_iter()
                                .filter(|(k, _)| {
                                    let ks = std::str::from_utf8(k).unwrap_or("");
                                    if pat.starts_with('*') && pat.ends_with('*') {
                                        ks.contains(&pat[1..pat.len() - 1])
                                    } else if pat.starts_with('*') {
                                        ks.ends_with(&pat[1..])
                                    } else if pat.ends_with('*') {
                                        ks.starts_with(&pat[..pat.len() - 1])
                                    } else {
                                        ks == pat
                                    }
                                })
                                .collect(),
                            _ => sorted,
                        };
                        let start = cursor.min(filtered.len());
                        let end = (start + count.max(1)).min(filtered.len());
                        let next = if end >= filtered.len() { 0 } else { end };
                        let items: Vec<RespValue> = filtered[start..end]
                            .iter()
                            .flat_map(|(k, v)| {
                                vec![RespValue::bulk(k.clone()), RespValue::bulk(v.clone())]
                            })
                            .collect();
                        (next, items)
                    }
                    _ => return wrongtype(),
                },
                None => (0, Vec::new()),
            };
            RespValue::array(vec![
                RespValue::bulk(Bytes::from(pairs.0.to_string())),
                RespValue::array(pairs.1),
            ])
        }
        "HRANDFIELD" => {
            if args.is_empty() {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'hrandfield' command".into(),
                );
            }
            let key = &args[0];
            let mut count = 1i64;
            let mut with_values = false;
            let mut i = 1;
            while i < args.len() {
                let tok = std::str::from_utf8(&args[i])
                    .unwrap_or("")
                    .to_ascii_uppercase();
                match tok.as_str() {
                    "WITHVALUES" => {
                        with_values = true;
                        i += 1;
                    }
                    _ => {
                        count = match std::str::from_utf8(&args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                        {
                            Some(n) => n,
                            None => {
                                return RespValue::Error(
                                    "ERR value is not an integer or out of range".into(),
                                )
                            }
                        };
                        i += 1;
                    }
                }
            }
            match store.get(key) {
                Some(entry) => match &entry.data {
                    DataType::Hash(h) if !h.is_empty() => {
                        let fields: Vec<Bytes> = h.keys().cloned().collect();
                        let n = fields.len();
                        let abs_c = count.unsigned_abs() as usize;
                        let mut r = Vec::new();
                        if count >= 0 {
                            let take = abs_c.min(n);
                            let mut idx: Vec<usize> = (0..n).collect();
                            for i in 0..take {
                                let j = fastrand::usize(i..n);
                                idx.swap(i, j);
                            }
                            for &idx in &idx[..take] {
                                let f = &fields[idx];
                                r.push(RespValue::bulk(f.clone()));
                                if with_values {
                                    r.push(RespValue::bulk(h[f].clone()));
                                }
                            }
                        } else {
                            for _ in 0..abs_c {
                                let i = fastrand::usize(0..n);
                                let f = &fields[i];
                                r.push(RespValue::bulk(f.clone()));
                                if with_values {
                                    r.push(RespValue::bulk(h[f].clone()));
                                }
                            }
                        }
                        if r.is_empty() {
                            RespValue::null_bulk()
                        } else {
                            RespValue::array(r)
                        }
                    }
                    DataType::Hash(_) => RespValue::null_bulk(),
                    _ => wrongtype(),
                },
                None => RespValue::null_bulk(),
            }
        }
        _ => RespValue::Error(format!("ERR unknown command '{}'", name).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use valkey_storage::Store;

    fn st() -> Arc<Store> {
        Store::new()
    }
    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }
    fn cmd(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| b(s)).collect()
    }

    #[tokio::test]
    async fn hset_new_fields() {
        let s = st();
        let r = handle(&cmd(&["HSET", "k", "f1", "v1", "f2", "v2"]), &s);
        assert_eq!(r, RespValue::int(2));
    }

    #[tokio::test]
    async fn hset_overwrite() {
        let s = st();
        handle(&cmd(&["HSET", "k", "f1", "v1"]), &s);
        let r = handle(&cmd(&["HSET", "k", "f1", "x"]), &s);
        assert_eq!(r, RespValue::int(0));
    }

    #[tokio::test]
    async fn hset_mixed() {
        let s = st();
        handle(&cmd(&["HSET", "k", "f1", "v1"]), &s);
        let r = handle(&cmd(&["HSET", "k", "f1", "x", "f2", "v2"]), &s);
        assert_eq!(r, RespValue::int(1));
    }

    #[tokio::test]
    async fn hget_existing() {
        let s = st();
        handle(&cmd(&["HSET", "k", "f", "hello"]), &s);
        let r = handle(&cmd(&["HGET", "k", "f"]), &s);
        assert_eq!(r, RespValue::bulk(b("hello")));
    }

    #[tokio::test]
    async fn hget_missing() {
        let s = st();
        handle(&cmd(&["HSET", "k", "f", "v"]), &s);
        assert_eq!(
            handle(&cmd(&["HGET", "k", "x"]), &s),
            RespValue::null_bulk()
        );
        assert_eq!(
            handle(&cmd(&["HGET", "z", "f"]), &s),
            RespValue::null_bulk()
        );
    }

    #[tokio::test]
    async fn hmget_mixed() {
        let s = st();
        handle(&cmd(&["HSET", "k", "a", "1", "b", "2"]), &s);
        let r = handle(&cmd(&["HMGET", "k", "a", "b", "c"]), &s);
        assert_eq!(
            r,
            RespValue::array(vec![
                RespValue::bulk(b("1")),
                RespValue::bulk(b("2")),
                RespValue::null_bulk(),
            ])
        );
    }

    #[tokio::test]
    async fn hmget_missing_key() {
        let s = st();
        let r = handle(&cmd(&["HMGET", "z", "a"]), &s);
        assert_eq!(r, RespValue::array(vec![RespValue::null_bulk()]));
    }

    #[tokio::test]
    async fn hmset_ok() {
        let s = st();
        assert_eq!(
            handle(&cmd(&["HMSET", "k", "f1", "v1"]), &s),
            RespValue::ok()
        );
        assert_eq!(
            handle(&cmd(&["HGET", "k", "f1"]), &s),
            RespValue::bulk(b("v1"))
        );
    }

    #[tokio::test]
    async fn hgetall_pairs() {
        let s = st();
        handle(&cmd(&["HSET", "k", "a", "1", "b", "2"]), &s);
        let r = handle(&cmd(&["HGETALL", "k"]), &s);
        if let RespValue::Array(Some(items)) = r {
            assert_eq!(items.len(), 4);
        } else {
            panic!("expected array");
        }
    }

    #[tokio::test]
    async fn hgetall_empty() {
        let s = st();
        let r = handle(&cmd(&["HGETALL", "z"]), &s);
        assert_eq!(r, RespValue::array(Vec::new()));
    }

    #[tokio::test]
    async fn hdel_existing() {
        let s = st();
        handle(&cmd(&["HSET", "k", "a", "1", "b", "2", "c", "3"]), &s);
        assert_eq!(
            handle(&cmd(&["HDEL", "k", "a", "c"]), &s),
            RespValue::int(2)
        );
        assert_eq!(
            handle(&cmd(&["HGET", "k", "a"]), &s),
            RespValue::null_bulk()
        );
        assert_eq!(
            handle(&cmd(&["HGET", "k", "b"]), &s),
            RespValue::bulk(b("2"))
        );
    }

    #[tokio::test]
    async fn hdel_missing() {
        let s = st();
        assert_eq!(handle(&cmd(&["HDEL", "z", "a"]), &s), RespValue::int(0));
        handle(&cmd(&["HSET", "k", "a", "1"]), &s);
        assert_eq!(handle(&cmd(&["HDEL", "k", "z"]), &s), RespValue::int(0));
    }

    #[tokio::test]
    async fn hexists_yes() {
        let s = st();
        handle(&cmd(&["HSET", "k", "a", "1"]), &s);
        assert_eq!(handle(&cmd(&["HEXISTS", "k", "a"]), &s), RespValue::int(1));
    }

    #[tokio::test]
    async fn hexists_no() {
        let s = st();
        handle(&cmd(&["HSET", "k", "a", "1"]), &s);
        assert_eq!(handle(&cmd(&["HEXISTS", "k", "b"]), &s), RespValue::int(0));
        assert_eq!(handle(&cmd(&["HEXISTS", "z", "a"]), &s), RespValue::int(0));
    }

    #[tokio::test]
    async fn hlen_count() {
        let s = st();
        handle(&cmd(&["HSET", "k", "a", "1", "b", "2"]), &s);
        assert_eq!(handle(&cmd(&["HLEN", "k"]), &s), RespValue::int(2));
        assert_eq!(handle(&cmd(&["HLEN", "z"]), &s), RespValue::int(0));
    }

    #[tokio::test]
    async fn hkeys_all() {
        let s = st();
        handle(&cmd(&["HSET", "k", "x", "1", "y", "2"]), &s);
        let r = handle(&cmd(&["HKEYS", "k"]), &s);
        if let RespValue::Array(Some(items)) = r {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected array");
        }
        assert_eq!(
            handle(&cmd(&["HKEYS", "z"]), &s),
            RespValue::array(Vec::new())
        );
    }

    #[tokio::test]
    async fn hvals_all() {
        let s = st();
        handle(&cmd(&["HSET", "k", "x", "10", "y", "20"]), &s);
        let r = handle(&cmd(&["HVALS", "k"]), &s);
        if let RespValue::Array(Some(items)) = r {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected array");
        }
        assert_eq!(
            handle(&cmd(&["HVALS", "z"]), &s),
            RespValue::array(Vec::new())
        );
    }

    #[tokio::test]
    async fn hincrby_new() {
        let s = st();
        assert_eq!(
            handle(&cmd(&["HINCRBY", "k", "c", "5"]), &s),
            RespValue::int(5)
        );
    }

    #[tokio::test]
    async fn hincrby_existing() {
        let s = st();
        handle(&cmd(&["HSET", "k", "c", "10"]), &s);
        assert_eq!(
            handle(&cmd(&["HINCRBY", "k", "c", "3"]), &s),
            RespValue::int(13)
        );
    }

    #[tokio::test]
    async fn hincrby_negative() {
        let s = st();
        handle(&cmd(&["HSET", "k", "c", "10"]), &s);
        assert_eq!(
            handle(&cmd(&["HINCRBY", "k", "c", "-3"]), &s),
            RespValue::int(7)
        );
    }

    #[tokio::test]
    async fn hincrby_non_numeric() {
        let s = st();
        handle(&cmd(&["HSET", "k", "f", "abc"]), &s);
        assert_eq!(
            handle(&cmd(&["HINCRBY", "k", "f", "5"]), &s),
            RespValue::int(5)
        );
    }

    #[tokio::test]
    async fn hincrbyfloat_new() {
        let s = st();
        let r = handle(&cmd(&["HINCRBYFLOAT", "k", "f", "1.5"]), &s);
        assert_eq!(r, RespValue::bulk(b("1.5")));
    }

    #[tokio::test]
    async fn hincrbyfloat_existing() {
        let s = st();
        handle(&cmd(&["HSET", "k", "f", "10"]), &s);
        let r = handle(&cmd(&["HINCRBYFLOAT", "k", "f", "0.5"]), &s);
        assert_eq!(r, RespValue::bulk(b("10.5")));
    }
}
