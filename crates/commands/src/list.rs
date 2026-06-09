use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, ListPack, Store};

const LIST_COMPACT_THRESHOLD: usize = 128;

pub async fn handle(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".into());
    }
    let cmd = match std::str::from_utf8(&a[0]) {
        Ok(x) => x.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    match cmd.as_str() {
        "LPUSH" => lp(&a[1..], s).await,
        "RPUSH" => rp(&a[1..], s).await,
        "LPUSHX" => pushx(&a[1..], s, true).await,
        "RPUSHX" => pushx(&a[1..], s, false).await,
        "RPOPLPUSH" => rpoplpush(&a[1..], s).await,
        "BRPOPLPUSH" => brpoplpush(&a[1..], s).await,
        "LPOP" => lpo(&a[1..], s).await,
        "RPOP" => rpo(&a[1..], s).await,
        "LRANGE" => lr(&a[1..], s).await,
        "LLEN" => ll(&a[1..], s).await,
        "LINDEX" => li(&a[1..], s).await,
        "LSET" => ls(&a[1..], s).await,
        "LINSERT" => lins(&a[1..], s).await,
        "LREM" => lre(&a[1..], s).await,
        "LTRIM" => ltr(&a[1..], s).await,
        "LMOVE" => lmo(&a[1..], s).await,
        "BLPOP" => bl(&a[1..], s).await,
        "BRPOP" => br(&a[1..], s).await,
        "LMPOP" => lmpop(&a[1..], s).await,
        "BLMPOP" => blmpop(&a[1..], s).await,
        "BLMOVE" => blmove(&a[1..], s).await,
        "LPOS" => lpos(&a[1..], s).await,
        _ => RespValue::Error(format!("ERR unknown command `{}`", cmd)),
    }
}

// ---------------------------------------------------------------------------
// LPUSHX / RPUSHX — push only when the key already holds a list
// ---------------------------------------------------------------------------
async fn pushx(a: &[Bytes], s: &Arc<Store>, left: bool) -> RespValue {
    if a.len() < 2 {
        let name = if left { "lpushx" } else { "rpushx" };
        return RespValue::Error(format!(
            "ERR wrong number of arguments for '{}' command",
            name
        ));
    }
    {
        match s.get(&a[0]) {
            Some(e) if e.data.is_list_type() => {}
            Some(_) => {
                return RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )
            }
            None => return RespValue::Integer(0),
        }
    }
    if left {
        lp(a, s).await
    } else {
        rp(a, s).await
    }
}

// ---------------------------------------------------------------------------
// RPOPLPUSH / BRPOPLPUSH — legacy aliases of LMOVE src dst RIGHT LEFT
// ---------------------------------------------------------------------------
async fn rpoplpush(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'rpoplpush' command".into());
    }
    let v = vec![
        a[0].clone(),
        a[1].clone(),
        Bytes::from_static(b"RIGHT"),
        Bytes::from_static(b"LEFT"),
    ];
    lmo(&v, s).await
}

async fn brpoplpush(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'brpoplpush' command".into());
    }
    let v = vec![
        a[0].clone(),
        a[1].clone(),
        Bytes::from_static(b"RIGHT"),
        Bytes::from_static(b"LEFT"),
        a[2].clone(),
    ];
    blmove(&v, s).await
}

// ---------------------------------------------------------------------------
// LPUSH
// ---------------------------------------------------------------------------
async fn lp(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'lpush' command".into());
    }
    let mut e = s
        .keyspace
        .entry(a[0].clone())
        .or_insert_with(|| Entry::new(DataType::ListPack(ListPack::new()), None));
    if !e.data.is_list_type() {
        return RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        );
    }
    let len = match &mut e.data {
        DataType::ListPack(lp) => {
            for v in &a[1..] {
                lp.push_front(valkey_storage::ListPackEntry::String(v.clone()));
            }
            lp.len()
        }
        DataType::List(l) => {
            for v in &a[1..] {
                l.push_front(v.clone());
            }
            l.len()
        }
        _ => unreachable!(),
    };
    e.data.maybe_upgrade_list(LIST_COMPACT_THRESHOLD);
    s.notify_watchers(&a[0]);
    RespValue::Integer(len as i64)
}

// ---------------------------------------------------------------------------
// RPUSH
// ---------------------------------------------------------------------------
async fn rp(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'rpush' command".into());
    }
    let mut e = s
        .keyspace
        .entry(a[0].clone())
        .or_insert_with(|| Entry::new(DataType::ListPack(ListPack::new()), None));
    if !e.data.is_list_type() {
        return RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        );
    }
    let len = match &mut e.data {
        DataType::ListPack(lp) => {
            for v in &a[1..] {
                lp.push_back_bytes(v.clone());
            }
            lp.len()
        }
        DataType::List(l) => {
            for v in &a[1..] {
                l.push_back(v.clone());
            }
            l.len()
        }
        _ => unreachable!(),
    };
    e.data.maybe_upgrade_list(LIST_COMPACT_THRESHOLD);
    s.notify_watchers(&a[0]);
    RespValue::Integer(len as i64)
}

// ---------------------------------------------------------------------------
// LPOP
// ---------------------------------------------------------------------------
async fn lpo(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'lpop' command".into());
    }
    let c = if a.len() >= 2 {
        match pu(&a[1]) {
            Some(n) => n,
            None => return RespValue::Error("ERR value is not an integer or out of range".into()),
        }
    } else {
        1
    };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            if l.is_empty() {
                return RespValue::BulkString(None);
            }
            let n = c.min(l.len());
            if n == 1 {
                let v = l.pop_front().unwrap();
                if l.is_empty() {
                    drop(e);
                    s.keyspace.remove(&a[0]);
                }
                RespValue::BulkString(Some(v))
            } else {
                let mut r = Vec::with_capacity(n);
                for _ in 0..n {
                    r.push(l.pop_front().unwrap());
                }
                if l.is_empty() {
                    drop(e);
                    s.keyspace.remove(&a[0]);
                }
                RespValue::Array(Some(
                    r.into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                ))
            }
        }
        None => {
            if c > 1 {
                RespValue::Array(Some(Vec::new()))
            } else {
                RespValue::BulkString(None)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RPOP
// ---------------------------------------------------------------------------
async fn rpo(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'rpop' command".into());
    }
    let c = if a.len() >= 2 {
        match pu(&a[1]) {
            Some(n) => n,
            None => return RespValue::Error("ERR value is not an integer or out of range".into()),
        }
    } else {
        1
    };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            if l.is_empty() {
                return RespValue::BulkString(None);
            }
            let n = c.min(l.len());
            if n == 1 {
                let v = l.pop_back().unwrap();
                if l.is_empty() {
                    drop(e);
                    s.keyspace.remove(&a[0]);
                }
                RespValue::BulkString(Some(v))
            } else {
                let mut r = Vec::with_capacity(n);
                for _ in 0..n {
                    r.push(l.pop_back().unwrap());
                }
                if l.is_empty() {
                    drop(e);
                    s.keyspace.remove(&a[0]);
                }
                RespValue::Array(Some(
                    r.into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                ))
            }
        }
        None => {
            if c > 1 {
                RespValue::Array(Some(Vec::new()))
            } else {
                RespValue::BulkString(None)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LRANGE
// ---------------------------------------------------------------------------
async fn lr(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'lrange' command".into());
    }
    let st = match pi(&a[1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    let sp = match pi(&a[2]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    match s.keyspace.get(&a[0]) {
        Some(e) => match e.data.list_range(st, sp) {
            Some(items) => RespValue::Array(Some(
                items
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            )),
            None => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => RespValue::Array(Some(Vec::new())),
    }
}

// ---------------------------------------------------------------------------
// LLEN
// ---------------------------------------------------------------------------
async fn ll(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'llen' command".into());
    }
    match s.keyspace.get(&a[0]) {
        Some(e) => match e.data.list_len() {
            Some(n) => RespValue::Integer(n as i64),
            None => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => RespValue::Integer(0),
    }
}

// ---------------------------------------------------------------------------
// LINDEX
// ---------------------------------------------------------------------------
async fn li(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'lindex' command".into());
    };
    let idx = match pi(&a[1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    match s.keyspace.get(&a[0]) {
        Some(e) => match e.data.list_get(idx) {
            Some(v) => RespValue::BulkString(Some(v)),
            None if e.data.is_list_type() => RespValue::BulkString(None),
            None => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
        },
        None => RespValue::BulkString(None),
    }
}

// ---------------------------------------------------------------------------
// LSET
// ---------------------------------------------------------------------------
async fn ls(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'lset' command".into());
    };
    let idx = match pi(&a[1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            let len = l.len() as i64;
            let i = ni(idx, len);
            if i < 0 || i >= len {
                RespValue::Error("ERR index out of range".into())
            } else {
                l[i as usize] = a[2].clone();
                RespValue::SimpleString("OK".into())
            }
        }
        None => RespValue::Error("ERR no such key".into()),
    }
}

// ---------------------------------------------------------------------------
// LINSERT
// ---------------------------------------------------------------------------
async fn lins(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 4 {
        return RespValue::Error("ERR wrong number of arguments for 'linsert' command".into());
    };
    let before = match std::str::from_utf8(&a[1]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            match l.iter().position(|v| v == &a[2]) {
                Some(p) => {
                    let ip = if before { p } else { p + 1 };
                    l.insert(ip, a[3].clone());
                    RespValue::Integer(l.len() as i64)
                }
                None => RespValue::Integer(-1),
            }
        }
        None => RespValue::Integer(-1),
    }
}

// ---------------------------------------------------------------------------
// LREM
// ---------------------------------------------------------------------------
async fn lre(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'lrem' command".into());
    };
    let c = match pi(&a[1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            let lb = l.len();
            if c > 0 {
                let mut rm = c;
                let mut i = 0;
                while i < l.len() && rm > 0 {
                    if l[i] == a[2] {
                        l.remove(i);
                        rm -= 1;
                    } else {
                        i += 1;
                    }
                }
            } else if c < 0 {
                let mut rm = c.abs();
                let mut i = l.len() as i64 - 1;
                while i >= 0 && rm > 0 {
                    if l[i as usize] == a[2] {
                        l.remove(i as usize);
                        rm -= 1;
                    }
                    i -= 1;
                }
            } else {
                l.retain(|v| v != &a[2]);
            }
            let r = (lb - l.len()) as i64;
            if l.is_empty() {
                drop(e);
                s.keyspace.remove(&a[0]);
            }
            RespValue::Integer(r)
        }
        None => RespValue::Integer(0),
    }
}

// ---------------------------------------------------------------------------
// LTRIM
// ---------------------------------------------------------------------------
async fn ltr(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'ltrim' command".into());
    };
    let st = match pi(&a[1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    let sp = match pi(&a[2]) {
        Some(v) => v,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };
    match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            if l.is_empty() {
                return RespValue::SimpleString("OK".into());
            }
            let len = l.len() as i64;
            let si = ni(st, len);
            let ei = ni(sp, len);
            if si > ei || si >= len {
                l.clear();
            } else {
                let si = si.max(0) as usize;
                let ei = (ei.min(len - 1)) as usize;
                let mut nl = VecDeque::new();
                for i in si..=ei {
                    nl.push_back(l[i].clone());
                }
                *l = nl;
            }
            RespValue::SimpleString("OK".into())
        }
        None => RespValue::SimpleString("OK".into()),
    }
}

// ---------------------------------------------------------------------------
// LMOVE
// ---------------------------------------------------------------------------
async fn lmo(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 4 {
        return RespValue::Error("ERR wrong number of arguments for 'lmove' command".into());
    };
    let fl = match std::str::from_utf8(&a[2]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };
    let tl = match std::str::from_utf8(&a[3]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };
    let popped = match s.keyspace.get_mut(&a[0]) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            if l.is_empty() {
                None
            } else {
                let v = if fl {
                    l.pop_front().unwrap()
                } else {
                    l.pop_back().unwrap()
                };
                if l.is_empty() {
                    drop(e);
                    s.keyspace.remove(&a[0]);
                }
                Some(v)
            }
        }
        None => None,
    };
    match popped {
        Some(val) => {
            let mut e = s
                .keyspace
                .entry(a[1].clone())
                .or_insert_with(|| Entry::new(DataType::ListPack(ListPack::new()), None));
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            if tl {
                l.push_front(val.clone());
            } else {
                l.push_back(val.clone());
            }
            s.notify_watchers(&a[1]);
            RespValue::BulkString(Some(val))
        }
        None => RespValue::BulkString(None),
    }
}

// ---------------------------------------------------------------------------
// BLPOP — blocking left pop
// ---------------------------------------------------------------------------
async fn bl(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'blpop' command".into());
    };
    let timeout = match pf(&a[a.len() - 1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR timeout is not a float or out of range".into()),
    };
    let keys = &a[..a.len() - 1];

    // Fast path: try non-blocking first
    for key in keys {
        match s.keyspace.get_mut(key) {
            Some(mut e) => {
                if !e.data.is_list_type() {
                    continue;
                }
                let l = e.data.as_list_mut().unwrap();
                if !l.is_empty() {
                    let v = l.pop_front().unwrap();
                    if l.is_empty() {
                        drop(e);
                        s.keyspace.remove(key);
                    }
                    return RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::BulkString(Some(v)),
                    ]));
                }
            }
            None => continue,
        }
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return blpop_block_forever(keys, s).await;
    }

    // Blocking path with timeout
    let dur = Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(dur, blpop_block_forever(keys, s)).await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

async fn blpop_block_forever(keys: &[Bytes], s: &Arc<Store>) -> RespValue {
    // Register watchers on all keys
    let mut receivers: Vec<(Bytes, std::sync::mpsc::Receiver<()>)> = Vec::new();
    for key in keys {
        receivers.push((key.clone(), s.watch(key)));
    }

    loop {
        // Check all keys after getting a notification
        for (key, _) in &receivers {
            match s.keyspace.get_mut(key) {
                Some(mut e) => {
                    if !e.data.is_list_type() {
                        continue;
                    }
                    let l = e.data.as_list_mut().unwrap();
                    if !l.is_empty() {
                        let v = l.pop_front().unwrap();
                        if l.is_empty() {
                            drop(e);
                            s.keyspace.remove(key);
                        }
                        return RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(key.clone())),
                            RespValue::BulkString(Some(v)),
                        ]));
                    }
                }
                None => continue,
            }
        }

        // Wait for any key to be modified
        let mut done = false;
        for (_, rx) in &receivers {
            if rx.try_recv().is_ok() {
                done = true;
                break;
            }
        }
        if !done {
            // Small sleep to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// BRPOP — blocking right pop
// ---------------------------------------------------------------------------
async fn br(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'brpop' command".into());
    };
    let timeout = match pf(&a[a.len() - 1]) {
        Some(v) => v,
        None => return RespValue::Error("ERR timeout is not a float or out of range".into()),
    };
    let keys = &a[..a.len() - 1];

    // Fast path: try non-blocking first
    for key in keys {
        match s.keyspace.get_mut(key) {
            Some(mut e) => {
                if !e.data.is_list_type() {
                    continue;
                }
                let l = e.data.as_list_mut().unwrap();
                if !l.is_empty() {
                    let v = l.pop_back().unwrap();
                    if l.is_empty() {
                        drop(e);
                        s.keyspace.remove(key);
                    }
                    return RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::BulkString(Some(v)),
                    ]));
                }
            }
            None => continue,
        }
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return brpop_block_forever(keys, s).await;
    }

    // Blocking path with timeout
    let dur = Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(dur, brpop_block_forever(keys, s)).await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

async fn brpop_block_forever(keys: &[Bytes], s: &Arc<Store>) -> RespValue {
    // Register watchers on all keys
    let mut receivers: Vec<(Bytes, std::sync::mpsc::Receiver<()>)> = Vec::new();
    for key in keys {
        receivers.push((key.clone(), s.watch(key)));
    }

    loop {
        // Check all keys after getting a notification
        for (key, _) in &receivers {
            match s.keyspace.get_mut(key) {
                Some(mut e) => {
                    if !e.data.is_list_type() {
                        continue;
                    }
                    let l = e.data.as_list_mut().unwrap();
                    if !l.is_empty() {
                        let v = l.pop_back().unwrap();
                        if l.is_empty() {
                            drop(e);
                            s.keyspace.remove(key);
                        }
                        return RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(key.clone())),
                            RespValue::BulkString(Some(v)),
                        ]));
                    }
                }
                None => continue,
            }
        }

        // Wait for any key to be modified
        let mut done = false;
        for (_, rx) in &receivers {
            if rx.try_recv().is_ok() {
                done = true;
                break;
            }
        }
        if !done {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// LPOS — find position of element in list
// ---------------------------------------------------------------------------
async fn lpos(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'lpos' command".into());
    }

    let key = &a[0];
    let element = &a[1];

    // Parse optional arguments
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: Option<usize> = None;

    let mut i = 2;
    while i < a.len() {
        let arg = match std::str::from_utf8(&a[i]) {
            Ok(x) => x.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        match arg.as_str() {
            "RANK" => {
                i += 1;
                if i >= a.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                rank = match pi(&a[i]) {
                    Some(v) => v,
                    None => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
            }
            "COUNT" => {
                i += 1;
                if i >= a.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                count = match pu(&a[i]) {
                    Some(v) => Some(v),
                    None => {
                        return RespValue::Error(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
            }
            "MAXLEN" => {
                i += 1;
                if i >= a.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                maxlen = match pu(&a[i]) {
                    Some(v) => Some(v),
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

    let list: Vec<Bytes> = match s.keyspace.get(key) {
        Some(e) => match e.data.list_range(i64::MIN, i64::MAX) {
            Some(items) => items,
            None => {
                return RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )
            }
        },
        None => return RespValue::Integer(-1),
    };

    if list.is_empty() {
        return RespValue::Integer(-1);
    }

    let len = list.len();
    let maxlen = maxlen.unwrap_or(len);
    let count = count.unwrap_or(1);

    // Build positions list based on rank
    let positions: Vec<usize> = if rank > 0 {
        (0..len).take(maxlen).collect()
    } else {
        // Negative rank: search from the tail
        let _ = rank.unsigned_abs();
        (0..len).rev().take(maxlen).collect()
    };

    let mut found = Vec::new();
    if rank > 0 {
        for (idx, pos) in positions.iter().enumerate() {
            if list[*pos] == *element {
                found.push(*pos as i64);
                if found.len() >= count {
                    break;
                }
            }
        }
    } else {
        // Negative rank: search from tail, return first match from tail
        for pos in positions.iter().rev() {
            if list[*pos] == *element {
                found.push(*pos as i64);
                if found.len() >= count {
                    break;
                }
            }
        }
    }

    if found.is_empty() {
        if count > 1 {
            RespValue::Array(Some(Vec::new()))
        } else {
            RespValue::Integer(-1)
        }
    } else if count == 1 {
        RespValue::Integer(found[0])
    } else {
        RespValue::Array(Some(found.into_iter().map(RespValue::Integer).collect()))
    }
}

// ---------------------------------------------------------------------------
// LMPOP — non-blocking multi-pop for lists
// ---------------------------------------------------------------------------
async fn lmpop(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'lmpop' command".into());
    }

    let num_keys = match pu(&a[0]) {
        Some(n) => n,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if num_keys == 0 {
        return RespValue::Error("ERR value is not an integer or out of range".into());
    };

    if a.len() < 2 + num_keys {
        return RespValue::Error("ERR syntax error".into());
    }

    let direction = match std::str::from_utf8(&a[1]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };

    let keys = &a[2..2 + num_keys];

    // Parse optional COUNT argument
    let mut count: Option<usize> = None;
    if a.len() > 2 + num_keys {
        let arg = match std::str::from_utf8(&a[2 + num_keys]) {
            Ok(x) => x.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        if arg == "COUNT" {
            if a.len() <= 3 + num_keys {
                return RespValue::Error("ERR syntax error".into());
            }
            count = match pu(&a[3 + num_keys]) {
                Some(n) => Some(n),
                None => {
                    return RespValue::Error("ERR value is not an integer or out of range".into())
                }
            };
        } else {
            return RespValue::Error("ERR syntax error".into());
        }
    }

    // Try each key in order
    for key in keys {
        match s.keyspace.get_mut(key) {
            Some(mut e) => {
                if !e.data.is_list_type() {
                    continue;
                }
                let l = e.data.as_list_mut().unwrap();
                if !l.is_empty() {
                    let pop_count = count.unwrap_or(1).min(l.len());
                    let mut popped = Vec::with_capacity(pop_count);
                    for _ in 0..pop_count {
                        let v = if direction {
                            l.pop_front().unwrap()
                        } else {
                            l.pop_back().unwrap()
                        };
                        popped.push(RespValue::BulkString(Some(v)));
                    }
                    if l.is_empty() {
                        drop(e);
                        s.keyspace.remove(key);
                    }
                    return RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::Array(Some(popped)),
                    ]));
                }
            }
            None => continue,
        }
    }

    RespValue::BulkString(None)
}

// ---------------------------------------------------------------------------
// BLMPOP — blocking multi-pop for lists
// ---------------------------------------------------------------------------
async fn blmpop(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 4 {
        return RespValue::Error("ERR wrong number of arguments for 'blmpop' command".into());
    }

    let timeout = match pf(&a[0]) {
        Some(v) => v,
        None => return RespValue::Error("ERR timeout is not a float or out of range".into()),
    };

    let num_keys = match pu(&a[1]) {
        Some(n) => n,
        None => return RespValue::Error("ERR value is not an integer or out of range".into()),
    };

    if num_keys == 0 {
        return RespValue::Error("ERR value is not an integer or out of range".into());
    };

    if a.len() < 3 + num_keys {
        return RespValue::Error("ERR syntax error".into());
    }

    let direction = match std::str::from_utf8(&a[2]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };

    let keys = &a[3..3 + num_keys];

    // Parse optional COUNT argument
    let mut count: Option<usize> = None;
    if a.len() > 3 + num_keys {
        let arg = match std::str::from_utf8(&a[3 + num_keys]) {
            Ok(x) => x.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        if arg == "COUNT" {
            if a.len() <= 4 + num_keys {
                return RespValue::Error("ERR syntax error".into());
            }
            count = match pu(&a[4 + num_keys]) {
                Some(n) => Some(n),
                None => {
                    return RespValue::Error("ERR value is not an integer or out of range".into())
                }
            };
        } else {
            return RespValue::Error("ERR syntax error".into());
        }
    }

    // Fast path: try non-blocking first
    for key in keys {
        match s.keyspace.get_mut(key) {
            Some(mut e) => {
                if !e.data.is_list_type() {
                    continue;
                }
                let l = e.data.as_list_mut().unwrap();
                if !l.is_empty() {
                    let pop_count = count.unwrap_or(1).min(l.len());
                    let mut popped = Vec::with_capacity(pop_count);
                    for _ in 0..pop_count {
                        let v = if direction {
                            l.pop_front().unwrap()
                        } else {
                            l.pop_back().unwrap()
                        };
                        popped.push(RespValue::BulkString(Some(v)));
                    }
                    if l.is_empty() {
                        drop(e);
                        s.keyspace.remove(key);
                    }
                    return RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::Array(Some(popped)),
                    ]));
                }
            }
            None => continue,
        }
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return blmpop_block_forever(keys, direction, count, s).await;
    }

    // Blocking path with timeout
    let dur = Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(dur, blmpop_block_forever(keys, direction, count, s)).await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

async fn blmpop_block_forever(
    keys: &[Bytes],
    left: bool,
    count: Option<usize>,
    s: &Arc<Store>,
) -> RespValue {
    let mut receivers: Vec<(Bytes, std::sync::mpsc::Receiver<()>)> = Vec::new();
    for key in keys {
        receivers.push((key.clone(), s.watch(key)));
    }

    loop {
        for (key, _) in &receivers {
            match s.keyspace.get_mut(key) {
                Some(mut e) => {
                    if !e.data.is_list_type() {
                        continue;
                    }
                    let l = e.data.as_list_mut().unwrap();
                    if !l.is_empty() {
                        let pop_count = count.unwrap_or(1).min(l.len());
                        let mut popped = Vec::with_capacity(pop_count);
                        for _ in 0..pop_count {
                            let v = if left {
                                l.pop_front().unwrap()
                            } else {
                                l.pop_back().unwrap()
                            };
                            popped.push(RespValue::BulkString(Some(v)));
                        }
                        if l.is_empty() {
                            drop(e);
                            s.keyspace.remove(key);
                        }
                        return RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(key.clone())),
                            RespValue::Array(Some(popped)),
                        ]));
                    }
                }
                None => continue,
            }
        }

        let mut done = false;
        for (_, rx) in &receivers {
            if rx.try_recv().is_ok() {
                done = true;
                break;
            }
        }
        if !done {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// BLMOVE — blocking version of LMOVE
// ---------------------------------------------------------------------------
async fn blmove(a: &[Bytes], s: &Arc<Store>) -> RespValue {
    if a.len() < 5 {
        return RespValue::Error("ERR wrong number of arguments for 'blmove' command".into());
    }

    let source = &a[0];
    let destination = &a[1];

    let from_dir = match std::str::from_utf8(&a[2]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };

    let to_dir = match std::str::from_utf8(&a[3]) {
        Ok(x) => match x.to_ascii_uppercase().as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        },
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };

    let timeout = match pf(&a[4]) {
        Some(v) => v,
        None => return RespValue::Error("ERR timeout is not a float or out of range".into()),
    };

    // Fast path: try non-blocking first
    let popped = match s.keyspace.get_mut(source) {
        Some(mut e) => {
            let l = match e.data.as_list_mut() {
                Some(l) => l,
                None => {
                    return RespValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            };
            if l.is_empty() {
                None
            } else {
                let v = if from_dir {
                    l.pop_front().unwrap()
                } else {
                    l.pop_back().unwrap()
                };
                if l.is_empty() {
                    drop(e);
                    s.keyspace.remove(source);
                }
                Some(v)
            }
        }
        None => None,
    };

    if let Some(val) = popped {
        push_to_dest(s, destination, val.clone(), to_dir);
        return RespValue::BulkString(Some(val));
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return blmove_block_forever(source, destination, from_dir, to_dir, s).await;
    }

    // Blocking path with timeout
    let dur = Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(
        dur,
        blmove_block_forever(source, destination, from_dir, to_dir, s),
    )
    .await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

fn push_to_dest(s: &Arc<Store>, dest: &Bytes, val: Bytes, to_left: bool) {
    let mut e = s
        .keyspace
        .entry(dest.clone())
        .or_insert_with(|| Entry::new(DataType::ListPack(ListPack::new()), None));
    let l = match e.data.as_list_mut() {
        Some(l) => l,
        None => return,
    };
    if to_left {
        l.push_front(val);
    } else {
        l.push_back(val);
    }
    s.notify_watchers(dest);
}

async fn blmove_block_forever(
    source: &Bytes,
    destination: &Bytes,
    from_left: bool,
    to_left: bool,
    s: &Arc<Store>,
) -> RespValue {
    let rx = s.watch(source);

    loop {
        // Try to pop from source
        let popped = match s.keyspace.get_mut(source) {
            Some(mut e) => {
                let l = match e.data.as_list_mut() {
                    Some(l) => l,
                    None => {
                        return RespValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                };
                if l.is_empty() {
                    None
                } else {
                    let v = if from_left {
                        l.pop_front().unwrap()
                    } else {
                        l.pop_back().unwrap()
                    };
                    if l.is_empty() {
                        drop(e);
                        s.keyspace.remove(source);
                    }
                    Some(v)
                }
            }
            None => None,
        };

        if let Some(val) = popped {
            push_to_dest(s, destination, val.clone(), to_left);
            return RespValue::BulkString(Some(val));
        }

        // Wait for source to be modified
        if rx.try_recv().is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------
fn ni(i: i64, len: i64) -> i64 {
    if i < 0 {
        let r = len + i;
        if r < 0 {
            0
        } else {
            r
        }
    } else {
        i
    }
}

fn pi(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

fn pu(b: &Bytes) -> Option<usize> {
    std::str::from_utf8(b).ok()?.parse::<usize>().ok()
}

fn pf(b: &Bytes) -> Option<f64> {
    std::str::from_utf8(b).ok()?.parse::<f64>().ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn ts() -> Arc<Store> {
        Store::new()
    }

    #[tokio::test]
    async fn t_lpush() {
        let s = ts();
        assert_eq!(
            handle(&[b("LPUSH"), b("L"), b("a"), b("b")], &s).await,
            RespValue::Integer(2)
        );
    }

    #[tokio::test]
    async fn t_rpush() {
        let s = ts();
        assert_eq!(
            handle(&[b("RPUSH"), b("L"), b("a"), b("b")], &s).await,
            RespValue::Integer(2)
        );
    }

    #[tokio::test]
    async fn t_lpop() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("x"), b("y")], &s).await;
        assert_eq!(
            handle(&[b("LPOP"), b("L")], &s).await,
            RespValue::BulkString(Some(b("x")))
        );
    }

    #[tokio::test]
    async fn t_rpop() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("x"), b("y")], &s).await;
        assert_eq!(
            handle(&[b("RPOP"), b("L")], &s).await,
            RespValue::BulkString(Some(b("y")))
        );
    }

    #[tokio::test]
    async fn t_llen() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b")], &s).await;
        assert_eq!(
            handle(&[b("LLEN"), b("L")], &s).await,
            RespValue::Integer(2)
        );
        assert_eq!(
            handle(&[b("LLEN"), b("n")], &s).await,
            RespValue::Integer(0)
        );
    }

    #[tokio::test]
    async fn t_lindex() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(
            handle(&[b("LINDEX"), b("L"), b("1")], &s).await,
            RespValue::BulkString(Some(b("b")))
        );
        assert_eq!(
            handle(&[b("LINDEX"), b("L"), b("-1")], &s).await,
            RespValue::BulkString(Some(b("c")))
        );
        assert_eq!(
            handle(&[b("LINDEX"), b("L"), b("5")], &s).await,
            RespValue::BulkString(None)
        );
    }

    #[tokio::test]
    async fn t_lrange() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c"), b("d")], &s).await;
        assert_eq!(
            handle(&[b("LRANGE"), b("L"), b("1"), b("2")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("b"))),
                RespValue::BulkString(Some(b("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lrange_neg() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(
            handle(&[b("LRANGE"), b("L"), b("-2"), b("-1")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("b"))),
                RespValue::BulkString(Some(b("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lset_ok() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b")], &s).await;
        assert_eq!(
            handle(&[b("LSET"), b("L"), b("0"), b("x")], &s).await,
            RespValue::SimpleString("OK".into())
        );
    }

    #[tokio::test]
    async fn t_lset_oor() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a")], &s).await;
        assert!(matches!(
            handle(&[b("LSET"), b("L"), b("5"), b("x")], &s).await,
            RespValue::Error(_)
        ));
    }

    #[tokio::test]
    async fn t_linsert_b() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("c")], &s).await;
        assert_eq!(
            handle(&[b("LINSERT"), b("L"), b("BEFORE"), b("c"), b("b")], &s).await,
            RespValue::Integer(3)
        );
    }

    #[tokio::test]
    async fn t_linsert_a() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("c")], &s).await;
        assert_eq!(
            handle(&[b("LINSERT"), b("L"), b("AFTER"), b("c"), b("b")], &s).await,
            RespValue::Integer(3)
        );
    }

    #[tokio::test]
    async fn t_linsert_nf() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a")], &s).await;
        assert_eq!(
            handle(&[b("LINSERT"), b("L"), b("BEFORE"), b("z"), b("x")], &s).await,
            RespValue::Integer(-1)
        );
    }

    #[tokio::test]
    async fn t_lrem_all() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("a")], &s).await;
        assert_eq!(
            handle(&[b("LREM"), b("L"), b("0"), b("a")], &s).await,
            RespValue::Integer(2)
        );
    }

    #[tokio::test]
    async fn t_lrem_pos() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("a")], &s).await;
        assert_eq!(
            handle(&[b("LREM"), b("L"), b("1"), b("a")], &s).await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn t_ltrim() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c"), b("d")], &s).await;
        handle(&[b("LTRIM"), b("L"), b("1"), b("2")], &s).await;
        assert_eq!(
            handle(&[b("LRANGE"), b("L"), b("0"), b("-1")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("b"))),
                RespValue::BulkString(Some(b("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lmove_lr() {
        let s = ts();
        handle(&[b("RPUSH"), b("src"), b("a"), b("b")], &s).await;
        assert_eq!(
            handle(&[b("LMOVE"), b("src"), b("dst"), b("LEFT"), b("RIGHT")], &s).await,
            RespValue::BulkString(Some(b("a")))
        );
    }

    #[tokio::test]
    async fn t_lmove_none() {
        let s = ts();
        assert_eq!(
            handle(&[b("LMOVE"), b("n"), b("d"), b("LEFT"), b("RIGHT")], &s).await,
            RespValue::BulkString(None)
        );
    }

    #[tokio::test]
    async fn t_blpop() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a")], &s).await;
        assert_eq!(
            handle(&[b("BLPOP"), b("L"), b("1")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("L"))),
                RespValue::BulkString(Some(b("a")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_brpop() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b")], &s).await;
        assert_eq!(
            handle(&[b("BRPOP"), b("L"), b("1")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("L"))),
                RespValue::BulkString(Some(b("b")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_blpop_timeout_empty() {
        let s = ts();
        // BLPOP on empty list with 0.5s timeout should return nil after timeout
        let start = std::time::Instant::now();
        let result = handle(&[b("BLPOP"), b("empty"), b("0.5")], &s).await;
        let elapsed = start.elapsed();
        assert_eq!(result, RespValue::BulkString(None));
        assert!(elapsed.as_millis() >= 400, "should have waited ~500ms");
    }

    #[tokio::test]
    async fn t_brpop_timeout_empty() {
        let s = ts();
        let start = std::time::Instant::now();
        let result = handle(&[b("BRPOP"), b("empty"), b("0.5")], &s).await;
        let elapsed = start.elapsed();
        assert_eq!(result, RespValue::BulkString(None));
        assert!(elapsed.as_millis() >= 400, "should have waited ~500ms");
    }

    #[tokio::test]
    async fn t_blpop_multi_key() {
        let s = ts();
        handle(&[b("RPUSH"), b("k2"), b("v2")], &s).await;
        assert_eq!(
            handle(&[b("BLPOP"), b("k1"), b("k2"), b("1")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("k2"))),
                RespValue::BulkString(Some(b("v2")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lpop_count() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c")], &s).await;
        assert_eq!(
            handle(&[b("LPOP"), b("L"), b("2")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("a"))),
                RespValue::BulkString(Some(b("b")))
            ]))
        );
    }

    #[tokio::test]
    async fn t_pop_del() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a")], &s).await;
        handle(&[b("LPOP"), b("L")], &s).await;
        assert_eq!(
            handle(&[b("LLEN"), b("L")], &s).await,
            RespValue::Integer(0)
        );
    }

    #[tokio::test]
    async fn t_rotate() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c")], &s).await;
        handle(&[b("LMOVE"), b("L"), b("L"), b("RIGHT"), b("LEFT")], &s).await;
        assert_eq!(
            handle(&[b("LRANGE"), b("L"), b("0"), b("-1")], &s).await,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("c"))),
                RespValue::BulkString(Some(b("a"))),
                RespValue::BulkString(Some(b("b")))
            ]))
        );
    }

    // --- LPOS tests ---

    #[tokio::test]
    async fn t_lpos_basic() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b"), b("c"), b("b")], &s).await;
        assert_eq!(
            handle(&[b("LPOS"), b("L"), b("b")], &s).await,
            RespValue::Integer(1)
        );
    }

    #[tokio::test]
    async fn t_lpos_not_found() {
        let s = ts();
        handle(&[b("RPUSH"), b("L"), b("a"), b("b")], &s).await;
        assert_eq!(
            handle(&[b("LPOS"), b("L"), b("z")], &s).await,
            RespValue::Integer(-1)
        );
    }

    #[tokio::test]
    async fn t_lpos_no_key() {
        let s = ts();
        assert_eq!(
            handle(&[b("LPOS"), b("missing"), b("a")], &s).await,
            RespValue::Integer(-1)
        );
    }

    #[tokio::test]
    async fn t_lpos_count() {
        let s = ts();
        handle(
            &[b("RPUSH"), b("L"), b("a"), b("b"), b("a"), b("b"), b("a")],
            &s,
        )
        .await;
        let result = handle(&[b("LPOS"), b("L"), b("a"), b("COUNT"), b("2")], &s).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(0), RespValue::Integer(2)]))
        );
    }

    // --- LMPOP tests ---

    #[tokio::test]
    async fn t_lmpop_left() {
        let s = ts();
        handle(&[b("RPUSH"), b("k1"), b("a"), b("b")], &s).await;
        let result = handle(&[b("LMPOP"), b("1"), b("LEFT"), b("k1")], &s).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("k1"))),
                RespValue::Array(Some(vec![RespValue::BulkString(Some(b("a")))]))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lmpop_right() {
        let s = ts();
        handle(&[b("RPUSH"), b("k1"), b("a"), b("b")], &s).await;
        let result = handle(&[b("LMPOP"), b("1"), b("RIGHT"), b("k1")], &s).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("k1"))),
                RespValue::Array(Some(vec![RespValue::BulkString(Some(b("b")))]))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lmpop_count() {
        let s = ts();
        handle(&[b("RPUSH"), b("k1"), b("a"), b("b"), b("c")], &s).await;
        let result = handle(
            &[b("LMPOP"), b("1"), b("LEFT"), b("k1"), b("COUNT"), b("2")],
            &s,
        )
        .await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("k1"))),
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b("a"))),
                    RespValue::BulkString(Some(b("b")))
                ]))
            ]))
        );
    }

    #[tokio::test]
    async fn t_lmpop_empty() {
        let s = ts();
        let result = handle(&[b("LMPOP"), b("1"), b("LEFT"), b("empty")], &s).await;
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn t_lmpop_multi_key() {
        let s = ts();
        handle(&[b("RPUSH"), b("k2"), b("x")], &s).await;
        let result = handle(&[b("LMPOP"), b("2"), b("LEFT"), b("k1"), b("k2")], &s).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("k2"))),
                RespValue::Array(Some(vec![RespValue::BulkString(Some(b("x")))]))
            ]))
        );
    }

    // --- BLMPOP tests ---

    #[tokio::test]
    async fn t_blmpop_immediate() {
        let s = ts();
        handle(&[b("RPUSH"), b("k1"), b("a"), b("b")], &s).await;
        let result = handle(&[b("BLMPOP"), b("0.5"), b("1"), b("LEFT"), b("k1")], &s).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b("k1"))),
                RespValue::Array(Some(vec![RespValue::BulkString(Some(b("a")))]))
            ]))
        );
    }

    #[tokio::test]
    async fn t_blmpop_timeout() {
        let s = ts();
        let start = std::time::Instant::now();
        let result = handle(&[b("BLMPOP"), b("0.5"), b("1"), b("LEFT"), b("empty")], &s).await;
        let elapsed = start.elapsed();
        assert_eq!(result, RespValue::BulkString(None));
        assert!(elapsed.as_millis() >= 400);
    }

    // --- BLMOVE tests ---

    #[tokio::test]
    async fn t_blmove_immediate() {
        let s = ts();
        handle(&[b("RPUSH"), b("src"), b("a"), b("b")], &s).await;
        let result = handle(
            &[
                b("BLMOVE"),
                b("src"),
                b("dst"),
                b("LEFT"),
                b("RIGHT"),
                b("1"),
            ],
            &s,
        )
        .await;
        assert_eq!(result, RespValue::BulkString(Some(b("a"))));
    }

    #[tokio::test]
    async fn t_blmove_timeout() {
        let s = ts();
        let start = std::time::Instant::now();
        let result = handle(
            &[
                b("BLMOVE"),
                b("empty"),
                b("dst"),
                b("LEFT"),
                b("RIGHT"),
                b("0.5"),
            ],
            &s,
        )
        .await;
        let elapsed = start.elapsed();
        assert_eq!(result, RespValue::BulkString(None));
        assert!(elapsed.as_millis() >= 400);
    }
}
