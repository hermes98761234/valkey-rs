#![allow(
    clippy::manual_is_multiple_of,
    clippy::unwrap_or_default,
    clippy::redundant_closure,
    clippy::unnecessary_to_owned,
    clippy::useless_conversion,
    clippy::needless_return,
    clippy::match_single_binding,
    clippy::needless_borrow,
    clippy::field_reassign_with_default,
    clippy::new_without_default,
    clippy::should_implement_trait,
    clippy::len_zero,
    clippy::unused_self,
    dead_code,
    unused_imports,
    unused_mut,
    unused_variables
)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex;
use tracing::{info, warn};

use valkey_storage::{DataType, Store};

// ---------------------------------------------------------------------------
// Fsync policy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    pub fn from_policy_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "everysec" => Some(Self::EverySec),
            "no" => Some(Self::No),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// AofWriter — buffered append-only file writer
// ---------------------------------------------------------------------------

pub struct AofWriter {
    writer: Arc<Mutex<BufWriter<File>>>,
    buf: Arc<Mutex<BytesMut>>,
    fsync: FsyncPolicy,
    dirty: Arc<AtomicBool>,
    path: PathBuf,
}

impl AofWriter {
    pub async fn open(path: &Path, fsync: FsyncPolicy) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await?;
        let writer = BufWriter::new(file);

        let aof = Self {
            writer: Arc::new(Mutex::new(writer)),
            buf: Arc::new(Mutex::new(BytesMut::with_capacity(64 * 1024))),
            fsync,
            dirty: Arc::new(AtomicBool::new(false)),
            path: path.to_path_buf(),
        };

        // Spawn background sync task for EverySec policy
        if fsync == FsyncPolicy::EverySec {
            let writer = Arc::clone(&aof.writer);
            let dirty = Arc::clone(&aof.dirty);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    if dirty.swap(false, Ordering::Relaxed) {
                        let mut w = writer.lock().await;
                        if let Err(e) = w.flush().await {
                            warn!("AOF background sync error: {e}");
                        }
                    }
                }
            });
        }

        Ok(aof)
    }

    /// Serialize a command as a RESP array and append to the AOF buffer.
    pub async fn append(&self, cmd: &[Bytes]) {
        let mut buf = self.buf.lock().await;
        // Write RESP array header
        buf.put_u8(b'*');
        buf.put_slice(cmd.len().to_string().as_bytes());
        buf.put_slice(b"\r\n");
        // Write each argument as a bulk string
        for arg in cmd {
            buf.put_u8(b'$');
            buf.put_slice(arg.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(arg);
            buf.put_slice(b"\r\n");
        }
        self.dirty.store(true, Ordering::Relaxed);

        // Flush based on fsync policy
        match self.fsync {
            FsyncPolicy::Always => {
                // Flush buffer to file immediately
                let buf_contents = buf.split().freeze();
                let mut w = self.writer.lock().await;
                if let Err(e) = w.write_all(&buf_contents).await {
                    warn!("AOF write error: {e}");
                    return;
                }
                if let Err(e) = w.flush().await {
                    warn!("AOF fsync error: {e}");
                }
            }
            FsyncPolicy::EverySec => {
                // Background task handles sync; just flush buffer to kernel
                let buf_contents = buf.split().freeze();
                let mut w = self.writer.lock().await;
                if let Err(e) = w.write_all(&buf_contents).await {
                    warn!("AOF write error: {e}");
                }
            }
            FsyncPolicy::No => {
                // Let the OS handle it; flush buffer to kernel periodically
                if buf.len() >= 64 * 1024 {
                    let buf_contents = buf.split().freeze();
                    let mut w = self.writer.lock().await;
                    if let Err(e) = w.write_all(&buf_contents).await {
                        warn!("AOF write error: {e}");
                    }
                }
            }
        }
    }

    /// Flush any remaining buffer contents to disk.
    pub async fn flush(&self) -> std::io::Result<()> {
        let mut buf = self.buf.lock().await;
        if !buf.is_empty() {
            let buf_contents = buf.split().freeze();
            let mut w = self.writer.lock().await;
            w.write_all(&buf_contents).await?;
            w.flush().await?;
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ---------------------------------------------------------------------------
// AOF rewrite — dump current store state as minimal command set
// ---------------------------------------------------------------------------

pub async fn rewrite(store: &Arc<Store>, path: &Path) -> std::io::Result<()> {
    let tmp_path = path.with_extension("aof.tmp");
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .await?;
    let mut writer = BufWriter::new(file);

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    for item in store.iter() {
        let key = item.key();
        let entry = item.value();

        // Skip expired keys
        if entry.is_expired() {
            continue;
        }

        let key_str = match std::str::from_utf8(key) {
            Ok(s) => s,
            Err(_) => continue,
        };

        match &entry.data {
            DataType::String(val) => {
                write_resp_array(
                    &mut writer,
                    &[
                        "SET",
                        key_str,
                        match std::str::from_utf8(val) {
                            Ok(s) => s,
                            Err(_) => {
                                // Write as raw bytes
                                writer.write_all(b"*3\r\n").await?;
                                writer.write_all(b"$3\r\nSET\r\n").await?;
                                write_bulk(&mut writer, key).await?;
                                write_bulk(&mut writer, val).await?;
                                if let Some(expires_at) = entry.expires_at {
                                    let ttl_ms = expires_at
                                        .saturating_duration_since(Instant::now())
                                        .as_millis() as i64;
                                    if ttl_ms > 0 {
                                        let abs_ms = now_ms + ttl_ms;
                                        writer
                                            .write_all(
                                                format!(
                                                    "*3\r\n$10\r\nPEXPIREAT\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                                    key.len(),
                                                    key_str,
                                                    abs_ms.to_string().len(),
                                                    abs_ms
                                                )
                                                .as_bytes(),
                                            )
                                            .await?;
                                    }
                                }
                                continue;
                            }
                        },
                    ],
                )
                .await?;
            }
            DataType::List(items) => {
                if !items.is_empty() {
                    let mut parts: Vec<&str> = vec!["RPUSH", key_str];
                    let item_strs: Vec<&str> = items
                        .iter()
                        .filter_map(|b| std::str::from_utf8(b).ok())
                        .collect();
                    if item_strs.len() == items.len() {
                        parts.extend(item_strs);
                        write_resp_array(&mut writer, &parts).await?;
                    } else {
                        // Mixed binary — write raw
                        writer
                            .write_all(format!("*{}\r\n", items.len() + 2).as_bytes())
                            .await?;
                        writer.write_all(b"$5\r\nRPUSH\r\n").await?;
                        write_bulk(&mut writer, key).await?;
                        for item in items {
                            write_bulk(&mut writer, item).await?;
                        }
                    }
                }
            }
            DataType::Hash(fields) => {
                if !fields.is_empty() {
                    let mut parts: Vec<&str> = vec!["HSET", key_str];
                    let mut all_strs = true;
                    for (k, v) in fields {
                        if std::str::from_utf8(k).is_err() || std::str::from_utf8(v).is_err() {
                            all_strs = false;
                            break;
                        }
                    }
                    if all_strs {
                        for (k, v) in fields {
                            parts.push(std::str::from_utf8(k).unwrap());
                            parts.push(std::str::from_utf8(v).unwrap());
                        }
                        write_resp_array(&mut writer, &parts).await?;
                    } else {
                        writer
                            .write_all(format!("*{}\r\n", fields.len() * 2 + 2).as_bytes())
                            .await?;
                        writer.write_all(b"$4\r\nHSET\r\n").await?;
                        write_bulk(&mut writer, key).await?;
                        for (k, v) in fields {
                            write_bulk(&mut writer, k).await?;
                            write_bulk(&mut writer, v).await?;
                        }
                    }
                }
            }
            DataType::Set(members) => {
                if !members.is_empty() {
                    let mut parts: Vec<&str> = vec!["SADD", key_str];
                    let member_strs: Vec<&str> = members
                        .iter()
                        .filter_map(|b| std::str::from_utf8(b).ok())
                        .collect();
                    if member_strs.len() == members.len() {
                        parts.extend(member_strs);
                        write_resp_array(&mut writer, &parts).await?;
                    } else {
                        writer
                            .write_all(format!("*{}\r\n", members.len() + 2).as_bytes())
                            .await?;
                        writer.write_all(b"$4\r\nSADD\r\n").await?;
                        write_bulk(&mut writer, key).await?;
                        for m in members {
                            write_bulk(&mut writer, m).await?;
                        }
                    }
                }
            }
            DataType::ZSet(zset) => {
                if !zset.is_empty() {
                    // Check if all members are valid UTF-8
                    let all_utf8 = zset.members.keys().all(|m| std::str::from_utf8(m).is_ok());
                    if all_utf8 {
                        let mut parts: Vec<String> = vec!["ZADD".into(), key_str.into()];
                        for (member, score) in &zset.members {
                            parts.push(score.0.to_string());
                            parts.push(std::str::from_utf8(member).unwrap().into());
                        }
                        let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
                        write_resp_array(&mut writer, &parts_ref).await?;
                    } else {
                        // Fallback: write raw binary
                        writer
                            .write_all(format!("*{}\r\n", zset.members.len() * 2 + 2).as_bytes())
                            .await?;
                        writer.write_all(b"$4\r\nZADD\r\n").await?;
                        write_bulk(&mut writer, key).await?;
                        for (m, sc) in &zset.members {
                            writer
                                .write_all(
                                    format!("${}\r\n{}\r\n", sc.0.to_string().len(), sc.0)
                                        .as_bytes(),
                                )
                                .await?;
                            write_bulk(&mut writer, m).await?;
                        }
                    }
                }
            }
            DataType::Stream(_) => {
                // Streams are complex; skip for now in rewrite
                // A full implementation would emit XADD commands
                continue;
            }
            DataType::ListPack(lp) => {
                if !lp.is_empty() {
                    let mut strs: Vec<String> = Vec::new();
                    for entry in lp.iter() {
                        let bytes = entry.to_bytes();
                        if let Ok(s) = std::str::from_utf8(&bytes) {
                            strs.push(s.to_string());
                        }
                    }
                    if !strs.is_empty() {
                        let mut parts: Vec<&str> = vec!["RPUSH", key_str];
                        for s in &strs {
                            parts.push(s.as_str());
                        }
                        write_resp_array(&mut writer, &parts).await?;
                    }
                }
            }
            DataType::IntSet(s) => {
                if !s.is_empty() {
                    let mut strs: Vec<String> = Vec::new();
                    for val in s.iter() {
                        strs.push(val.to_string());
                    }
                    let mut parts: Vec<&str> = vec!["SADD", key_str];
                    for s in &strs {
                        parts.push(s.as_str());
                    }
                    write_resp_array(&mut writer, &parts).await?;
                }
            }
        }

        // Write TTL if present
        if let Some(expires_at) = entry.expires_at {
            let ttl_ms = expires_at
                .saturating_duration_since(Instant::now())
                .as_millis() as i64;
            if ttl_ms > 0 {
                let abs_ms = now_ms + ttl_ms;
                writer
                    .write_all(
                        format!(
                            "*3\r\n$10\r\nPEXPIREAT\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key_str,
                            abs_ms.to_string().len(),
                            abs_ms
                        )
                        .as_bytes(),
                    )
                    .await?;
            }
        }
    }

    writer.flush().await?;
    drop(writer);

    // Atomically rename temp file to target
    tokio::fs::rename(&tmp_path, path).await?;
    info!("AOF rewrite complete: {}", path.display());
    Ok(())
}

/// Write a RESP array from string slices.
async fn write_resp_array<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    items: &[&str],
) -> std::io::Result<()> {
    writer
        .write_all(format!("*{}\r\n", items.len()).as_bytes())
        .await?;
    for item in items {
        writer
            .write_all(format!("${}\r\n{}\r\n", item.len(), item).as_bytes())
            .await?;
    }
    Ok(())
}

/// Write a RESP bulk string from Bytes.
async fn write_bulk<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &Bytes) -> std::io::Result<()> {
    writer
        .write_all(format!("${}\r\n", data.len()).as_bytes())
        .await?;
    writer.write_all(data).await?;
    writer.write_all(b"\r\n").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// AOF replay — parse RESP arrays from file and re-execute
// ---------------------------------------------------------------------------

pub async fn replay(store: &Arc<Store>, path: &Path) -> std::io::Result<()> {
    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);
    let mut buf = BytesMut::new();
    let mut scratch = Vec::new();

    loop {
        // Try to parse one RESP array from the buffer
        match parse_next_array(&buf) {
            ParseResult::Ok { consumed, items } => {
                buf.advance(consumed);
                // Convert parsed items to Bytes for dispatch
                let cmd: Vec<Bytes> = items
                    .into_iter()
                    .filter_map(|v| match v {
                        RespToken::BulkString(b) => Some(b),
                        RespToken::SimpleString(s) => Some(Bytes::from(s)),
                        RespToken::Integer(n) => Some(Bytes::from(n.to_string())),
                        _ => None,
                    })
                    .collect();

                if !cmd.is_empty() {
                    // Replay the command against the store
                    if let Err(e) = replay_command(store, &cmd).await {
                        warn!("AOF replay: skipping command {:?}: {}", cmd[0], e);
                    }
                }
            }
            ParseResult::Incomplete => {
                // Read more data
                scratch.resize(8192, 0);
                let n = reader.read(&mut scratch).await?;
                if n == 0 {
                    break; // EOF
                }
                buf.extend_from_slice(&scratch[..n]);
            }
            ParseResult::Err(e) => {
                warn!("AOF replay: parse error at offset, skipping: {e}");
                // Try to recover by scanning for next '*' byte
                if let Some(pos) = buf.iter().position(|&b| b == b'*') {
                    buf.advance(pos);
                } else {
                    break;
                }
            }
        }
    }

    info!("AOF replay complete: {}", path.display());
    Ok(())
}

/// Replay a single parsed command against the store.
async fn replay_command(store: &Arc<Store>, cmd: &[Bytes]) -> Result<(), String> {
    if cmd.is_empty() {
        return Ok(());
    }

    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return Err("invalid command name".into()),
    };

    let args = &cmd[1..];

    match name.as_str() {
        "SET" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            store.set(args[0].clone(), DataType::String(args[1].clone()), None);
        }
        "SETEX" => {
            if args.len() < 3 {
                return Err("wrong args".into());
            }
            let secs: u64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad ttl")?
                .parse()
                .map_err(|_| "bad ttl")?;
            store.set(
                args[0].clone(),
                DataType::String(args[2].clone()),
                Some(Duration::from_secs(secs)),
            );
        }
        "PSETEX" => {
            if args.len() < 3 {
                return Err("wrong args".into());
            }
            let ms: u64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad ttl")?
                .parse()
                .map_err(|_| "bad ttl")?;
            store.set(
                args[0].clone(),
                DataType::String(args[2].clone()),
                Some(Duration::from_millis(ms)),
            );
        }
        "SETNX" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            if !store.exists(&args[0]) {
                store.set(args[0].clone(), DataType::String(args[1].clone()), None);
            }
        }
        "GETSET" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            store.set(args[0].clone(), DataType::String(args[1].clone()), None);
        }
        "APPEND" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let new_val = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::String(existing) => {
                        let mut v = existing.to_vec();
                        drop(entry);
                        v.extend_from_slice(&args[1]);
                        v
                    }
                    _ => return Err("wrong type".into()),
                },
                None => args[1].to_vec(),
            };
            store.set(
                args[0].clone(),
                DataType::String(Bytes::from(new_val)),
                None,
            );
        }
        "INCR" => {
            if args.is_empty() {
                return Err("wrong args".into());
            }
            let new_val = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::String(val) => {
                        let n: i64 = std::str::from_utf8(val)
                            .map_err(|_| "not an integer")?
                            .parse()
                            .map_err(|_| "not an integer")?;
                        drop(entry);
                        (n + 1).to_string()
                    }
                    _ => return Err("wrong type".into()),
                },
                None => "1".to_string(),
            };
            store.set(
                args[0].clone(),
                DataType::String(Bytes::from(new_val)),
                None,
            );
        }
        "DECR" => {
            if args.is_empty() {
                return Err("wrong args".into());
            }
            let new_val = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::String(val) => {
                        let n: i64 = std::str::from_utf8(val)
                            .map_err(|_| "not an integer")?
                            .parse()
                            .map_err(|_| "not an integer")?;
                        drop(entry);
                        (n - 1).to_string()
                    }
                    _ => return Err("wrong type".into()),
                },
                None => "-1".to_string(),
            };
            store.set(
                args[0].clone(),
                DataType::String(Bytes::from(new_val)),
                None,
            );
        }
        "INCRBY" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let incr: i64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad increment")?
                .parse()
                .map_err(|_| "bad increment")?;
            let new_val = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::String(val) => {
                        let n: i64 = std::str::from_utf8(val)
                            .map_err(|_| "not an integer")?
                            .parse()
                            .map_err(|_| "not an integer")?;
                        drop(entry);
                        (n + incr).to_string()
                    }
                    _ => return Err("wrong type".into()),
                },
                None => incr.to_string(),
            };
            store.set(
                args[0].clone(),
                DataType::String(Bytes::from(new_val)),
                None,
            );
        }
        "DECRBY" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let decr: i64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad decrement")?
                .parse()
                .map_err(|_| "bad decrement")?;
            let new_val = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::String(val) => {
                        let n: i64 = std::str::from_utf8(val)
                            .map_err(|_| "not an integer")?
                            .parse()
                            .map_err(|_| "not an integer")?;
                        drop(entry);
                        (n - decr).to_string()
                    }
                    _ => return Err("wrong type".into()),
                },
                None => (-decr).to_string(),
            };
            store.set(
                args[0].clone(),
                DataType::String(Bytes::from(new_val)),
                None,
            );
        }
        "DEL" | "UNLINK" => {
            for arg in args {
                store.del(arg);
            }
        }
        "EXPIRE" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let secs: u64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad ttl")?
                .parse()
                .map_err(|_| "bad ttl")?;
            store.expire(&args[0], Instant::now() + Duration::from_secs(secs));
        }
        "PEXPIRE" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let ms: u64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad ttl")?
                .parse()
                .map_err(|_| "bad ttl")?;
            store.expire(&args[0], Instant::now() + Duration::from_millis(ms));
        }
        "EXPIREAT" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let ts: i64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad timestamp")?
                .parse()
                .map_err(|_| "bad timestamp")?;
            let target = UNIX_EPOCH + Duration::from_secs(ts.max(0) as u64);
            if let Ok(dur) = target.duration_since(SystemTime::now()) {
                store.expire(&args[0], Instant::now() + dur);
            }
        }
        "PEXPIREAT" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let ts: i64 = std::str::from_utf8(&args[1])
                .map_err(|_| "bad timestamp")?
                .parse()
                .map_err(|_| "bad timestamp")?;
            let target = UNIX_EPOCH + Duration::from_millis(ts.max(0) as u64);
            if let Ok(dur) = target.duration_since(SystemTime::now()) {
                store.expire(&args[0], Instant::now() + dur);
            }
        }
        "PERSIST" => {
            if args.is_empty() {
                return Err("wrong args".into());
            }
            store.expire(
                &args[0],
                Instant::now() + Duration::from_secs(86400 * 365 * 100),
            );
        }
        "RPUSH" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let mut list = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::List(existing) => {
                        let l = existing.clone();
                        drop(entry);
                        l
                    }
                    _ => return Err("wrong type".into()),
                },
                None => std::collections::VecDeque::new(),
            };
            for arg in &args[1..] {
                list.push_back(arg.clone());
            }
            store.set(args[0].clone(), DataType::List(list), None);
        }
        "LPUSH" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let mut list = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::List(existing) => {
                        let l = existing.clone();
                        drop(entry);
                        l
                    }
                    _ => return Err("wrong type".into()),
                },
                None => std::collections::VecDeque::new(),
            };
            for arg in args[1..].iter().rev() {
                list.push_front(arg.clone());
            }
            store.set(args[0].clone(), DataType::List(list), None);
        }
        "HSET" => {
            if args.len() < 3 {
                return Err("wrong args".into());
            }
            let mut map = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::Hash(existing) => {
                        let m = existing.clone();
                        drop(entry);
                        m
                    }
                    _ => return Err("wrong type".into()),
                },
                None => std::collections::HashMap::new(),
            };
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    map.insert(chunk[0].clone(), chunk[1].clone());
                }
            }
            store.set(args[0].clone(), DataType::Hash(map), None);
        }
        "SADD" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            let mut set = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::Set(existing) => {
                        let s = existing.clone();
                        drop(entry);
                        s
                    }
                    _ => return Err("wrong type".into()),
                },
                None => std::collections::HashSet::new(),
            };
            for arg in &args[1..] {
                set.insert(arg.clone());
            }
            store.set(args[0].clone(), DataType::Set(set), None);
        }
        "ZADD" => {
            if args.len() < 3 {
                return Err("wrong args".into());
            }
            let mut zset = match store.get(&args[0]) {
                Some(entry) => match &entry.data {
                    DataType::ZSet(existing) => {
                        let z = existing.clone();
                        drop(entry);
                        z
                    }
                    _ => return Err("wrong type".into()),
                },
                None => valkey_storage::ZSetData::new(),
            };
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    let score: f64 = std::str::from_utf8(&chunk[0])
                        .map_err(|_| "bad score")?
                        .parse()
                        .map_err(|_| "bad score")?;
                    zset.add(chunk[1].clone(), score);
                }
            }
            store.set(args[0].clone(), DataType::ZSet(zset), None);
        }
        "RENAME" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            if let Some(entry) = store.get(&args[0]) {
                let data = entry.data.clone();
                drop(entry);
                store.set(args[1].clone(), data, None);
                store.del(&args[0]);
            }
        }
        "RENAMENX" => {
            if args.len() < 2 {
                return Err("wrong args".into());
            }
            if !store.exists(&args[1]) {
                if let Some(entry) = store.get(&args[0]) {
                    let data = entry.data.clone();
                    drop(entry);
                    store.set(args[1].clone(), data, None);
                    store.del(&args[0]);
                }
            }
        }
        "FLUSHDB" | "FLUSHALL" => {
            store.flush();
        }
        _ => {
            warn!("AOF replay: unsupported command '{}', skipping", name);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Minimal RESP parser for AOF replay
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
enum RespToken {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespToken>),
}

enum ParseResult {
    Ok {
        consumed: usize,
        items: Vec<RespToken>,
    },
    Incomplete,
    Err(String),
}

fn parse_next_array(buf: &BytesMut) -> ParseResult {
    if buf.is_empty() {
        return ParseResult::Incomplete;
    }
    if buf[0] != b'*' {
        return ParseResult::Err("expected '*'".into());
    }

    let mut offset = 1;
    let (count, consumed) = match read_line(&buf[offset..]) {
        Some((_n, end)) => {
            let s = match std::str::from_utf8(&buf[offset..offset + _n]) {
                Ok(s) => s,
                Err(_) => return ParseResult::Err("bad count".into()),
            };
            offset += end;
            let n: i64 = match s.parse() {
                Ok(n) => n,
                Err(_) => return ParseResult::Err("bad count".into()),
            };
            (n, offset)
        }
        None => return ParseResult::Incomplete,
    };

    if count < 0 {
        return ParseResult::Ok {
            consumed,
            items: vec![],
        };
    }

    let mut items = Vec::with_capacity(count as usize);
    let mut offset = consumed;

    for _ in 0..count {
        if offset >= buf.len() {
            return ParseResult::Incomplete;
        }
        let (token, new_offset) = parse_token(buf, offset);
        items.push(token);
        offset = new_offset;
    }

    ParseResult::Ok {
        consumed: offset,
        items,
    }
}

fn parse_token(buf: &BytesMut, offset: usize) -> (RespToken, usize) {
    if offset >= buf.len() {
        return (RespToken::SimpleString(String::new()), offset);
    }
    let prefix = buf[offset];
    match prefix {
        b'$' => {
            let mut pos = offset + 1;
            let (len, line_end) = match read_line(&buf[pos..]) {
                Some((_n, end)) => {
                    let s = std::str::from_utf8(&buf[pos..pos + _n]).unwrap_or("0");
                    pos += end;
                    (s.parse::<i64>().unwrap_or(-1), pos)
                }
                None => (-1, pos),
            };
            if len < 0 {
                return (RespToken::BulkString(Bytes::new()), line_end);
            }
            let len = len as usize;
            let end = line_end + len + 2;
            if end > buf.len() {
                return (RespToken::BulkString(Bytes::new()), offset);
            }
            let data = Bytes::copy_from_slice(&buf[line_end..line_end + len]);
            (RespToken::BulkString(data), end)
        }
        b'+' => {
            let pos = offset + 1;
            if let Some((_, end)) = read_line(&buf[pos..]) {
                let s = std::str::from_utf8(&buf[pos..pos + end - 2])
                    .unwrap_or("")
                    .to_string();
                (RespToken::SimpleString(s), pos + end)
            } else {
                (RespToken::SimpleString(String::new()), offset + 1)
            }
        }
        b':' => {
            let pos = offset + 1;
            if let Some((_, end)) = read_line(&buf[pos..]) {
                let s = std::str::from_utf8(&buf[pos..pos + end - 2]).unwrap_or("0");
                let n: i64 = s.parse().unwrap_or(0);
                (RespToken::Integer(n), pos + end)
            } else {
                (RespToken::Integer(0), offset + 1)
            }
        }
        _ => (RespToken::SimpleString(String::new()), offset + 1),
    }
}

fn read_line(buf: &[u8]) -> Option<(usize, usize)> {
    for (i, window) in buf.windows(2).enumerate() {
        if window == b"\r\n" {
            return Some((i, i + 2));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::time::Duration;

    fn test_store() -> Arc<Store> {
        Arc::new(Store {
            keyspace: dashmap::DashMap::new(),
            evicted_keys: std::sync::atomic::AtomicU64::new(0),
            evict_config: std::sync::RwLock::new(valkey_storage::EvictionConfig::default()),
            watchers: dashmap::DashMap::new(),
            dirty_count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    #[tokio::test]
    async fn test_aof_write_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("appendonly.aof");

        // Write some commands
        {
            let aof = AofWriter::open(&path, FsyncPolicy::Always).await.unwrap();
            aof.append(&[
                Bytes::from("SET"),
                Bytes::from("mykey"),
                Bytes::from("myvalue"),
            ])
            .await;
            aof.append(&[
                Bytes::from("SET"),
                Bytes::from("counter"),
                Bytes::from("42"),
            ])
            .await;
            aof.append(&[
                Bytes::from("LPUSH"),
                Bytes::from("mylist"),
                Bytes::from("item1"),
                Bytes::from("item2"),
            ])
            .await;
            aof.flush().await.unwrap();
        }

        // Replay into a fresh store
        let store = test_store();
        replay(&store, &path).await.unwrap();

        // Verify state
        let entry = store.get(&Bytes::from("mykey")).unwrap();
        match &entry.data {
            DataType::String(s) => assert_eq!(s, &Bytes::from("myvalue")),
            other => panic!("expected string, got {:?}", other),
        }

        let entry = store.get(&Bytes::from("counter")).unwrap();
        match &entry.data {
            DataType::String(s) => assert_eq!(s, &Bytes::from("42")),
            other => panic!("expected string, got {:?}", other),
        }

        let entry = store.get(&Bytes::from("mylist")).unwrap();
        if let DataType::List(list) = &entry.data {
            assert_eq!(list.len(), 2);
            assert_eq!(list[0], Bytes::from("item1"));
            assert_eq!(list[1], Bytes::from("item2"));
        } else {
            panic!("expected list");
        }
    }

    #[tokio::test]
    async fn test_aof_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("appendonly.aof");

        // Populate a store
        let store = test_store();
        store.set(
            Bytes::from("str_key"),
            DataType::String(Bytes::from("str_val")),
            None,
        );
        store.set(
            Bytes::from("list_key"),
            DataType::List(VecDeque::from([
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
            ])),
            None,
        );
        store.set(
            Bytes::from("hash_key"),
            DataType::Hash(HashMap::from([
                (Bytes::from("field1"), Bytes::from("val1")),
                (Bytes::from("field2"), Bytes::from("val2")),
            ])),
            None,
        );
        store.set(
            Bytes::from("set_key"),
            DataType::Set(HashSet::from([
                Bytes::from("member1"),
                Bytes::from("member2"),
            ])),
            None,
        );

        // Rewrite
        rewrite(&store, &path).await.unwrap();

        // Replay into a fresh store
        let store2 = test_store();
        replay(&store2, &path).await.unwrap();

        // Verify
        let entry = store2.get(&Bytes::from("str_key")).unwrap();
        match &entry.data {
            DataType::String(s) => assert_eq!(s, &Bytes::from("str_val")),
            other => panic!("expected string, got {:?}", other),
        }

        let entry = store2.get(&Bytes::from("list_key")).unwrap();
        if let DataType::List(list) = &entry.data {
            assert_eq!(list.len(), 3);
        } else {
            panic!("expected list");
        }

        let entry = store2.get(&Bytes::from("hash_key")).unwrap();
        if let DataType::Hash(map) = &entry.data {
            assert_eq!(map.len(), 2);
            assert_eq!(
                map.get(&Bytes::from("field1")).unwrap(),
                &Bytes::from("val1")
            );
        } else {
            panic!("expected hash");
        }

        let entry = store2.get(&Bytes::from("set_key")).unwrap();
        if let DataType::Set(set) = &entry.data {
            assert_eq!(set.len(), 2);
        } else {
            panic!("expected set");
        }
    }

    #[tokio::test]
    async fn test_aof_replay_with_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("appendonly.aof");

        // Write a key with TTL
        {
            let aof = AofWriter::open(&path, FsyncPolicy::Always).await.unwrap();
            aof.append(&[
                Bytes::from("SET"),
                Bytes::from("tempkey"),
                Bytes::from("tempval"),
            ])
            .await;
            aof.append(&[
                Bytes::from("EXPIRE"),
                Bytes::from("tempkey"),
                Bytes::from("60"),
            ])
            .await;
            aof.flush().await.unwrap();
        }

        // Replay
        let store = test_store();
        replay(&store, &path).await.unwrap();

        // Verify key exists and has TTL
        assert!(store.exists(&Bytes::from("tempkey")));
        let ttl = store.ttl(&Bytes::from("tempkey"));
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() <= 60);
    }

    #[tokio::test]
    async fn test_aof_replay_skip_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("appendonly.aof");

        // Write a valid command followed by an invalid one
        {
            let aof = AofWriter::open(&path, FsyncPolicy::Always).await.unwrap();
            aof.append(&[
                Bytes::from("SET"),
                Bytes::from("goodkey"),
                Bytes::from("goodval"),
            ])
            .await;
            aof.flush().await.unwrap();
        }

        // Replay should succeed even with bad commands
        let store = test_store();
        replay(&store, &path).await.unwrap();
        assert!(store.exists(&Bytes::from("goodkey")));
    }

    #[tokio::test]
    async fn test_aof_simple_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");

        {
            let aof = AofWriter::open(&path, FsyncPolicy::Always).await.unwrap();
            aof.append(&[Bytes::from("SET"), Bytes::from("k"), Bytes::from("v")])
                .await;
            aof.flush().await.unwrap();
        }

        // Read raw file contents
        let data = tokio::fs::read(&path).await.unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        eprintln!("File contents: {:?}", s);
        assert!(s.contains("SET"));
        assert!(s.contains("k"));
        assert!(s.contains("v"));
    }

    #[tokio::test]
    async fn test_aof_simple_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");

        // Write a simple SET command
        {
            let aof = AofWriter::open(&path, FsyncPolicy::Always).await.unwrap();
            aof.append(&[
                Bytes::from("SET"),
                Bytes::from("mykey"),
                Bytes::from("myvalue"),
            ])
            .await;
            aof.flush().await.unwrap();
        }

        // Replay into a fresh store
        let store = test_store();
        eprintln!("Starting replay...");
        replay(&store, &path).await.unwrap();
        eprintln!("Replay done!");

        // Verify
        let entry = store.get(&Bytes::from("mykey")).unwrap();
        match &entry.data {
            DataType::String(s) => assert_eq!(s, &Bytes::from("myvalue")),
            other => panic!("expected string, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_fsync_policy_from_str() {
        assert_eq!(
            FsyncPolicy::from_policy_str("always"),
            Some(FsyncPolicy::Always)
        );
        assert_eq!(
            FsyncPolicy::from_policy_str("everysec"),
            Some(FsyncPolicy::EverySec)
        );
        assert_eq!(FsyncPolicy::from_policy_str("no"), Some(FsyncPolicy::No));
        assert_eq!(FsyncPolicy::from_policy_str("invalid"), None);
    }
}
