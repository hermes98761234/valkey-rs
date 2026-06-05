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
    match store.get(&args[0]) {
        Some(e) => {
            if e.expires_at.is_some() {
                store.set(args[0].clone(), e.data.clone(), None);
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }
        None => RespValue::Integer(0),
    }
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
    let entry = match store.get(src) {
        Some(e) => e,
        None => return RespValue::Integer(0),
    };
    if !replace && store.exists(dst) {
        return RespValue::Integer(0);
    }
    store.set(dst.clone(), entry.data.clone(), None);
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
async fn cmd_dump(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'dump' command".into());
    }
    RespValue::Error("ERR DUMP not supported yet".into())
}
async fn cmd_restore(args: &[Bytes], _store: &Arc<Store>) -> RespValue {
    if args.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'restore' command".into());
    }
    RespValue::Error("ERR RESTORE not supported yet".into())
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
}
