use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use valkey_storage::{DataType, Entry, Store};

// ---------------------------------------------------------------------------
// RDB format constants
// ---------------------------------------------------------------------------

const RDB_MAGIC: &[u8; 9] = b"REDIS0010";

// Type encoding
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_STREAM: u8 = 19;

// Special opcodes
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_EOF: u8 = 0xFF;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum RdbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid RDB magic: expected REDIS0010")]
    InvalidMagic,
    #[error("CRC64 checksum mismatch")]
    CrcMismatch,
    #[error("Unknown type byte: {0}")]
    UnknownType(u8),
    #[error("Unexpected EOF")]
    UnexpectedEof,
    #[error("Store error: {0}")]
    Store(String),
}

pub type Result<T> = std::result::Result<T, RdbError>;

// ---------------------------------------------------------------------------
// Length encoding (variable-length integer)
// ---------------------------------------------------------------------------

fn encode_length(buf: &mut Vec<u8>, len: u64) {
    if len < (1 << 6) {
        buf.push(len as u8);
    } else if len < (1 << 14) {
        buf.push(0x40 | ((len >> 8) as u8));
        buf.push((len & 0xFF) as u8);
    } else {
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
    }
}

fn decode_length(input: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos >= input.len() {
        return Err(RdbError::UnexpectedEof);
    }
    let first = input[*pos];
    *pos += 1;
    match first >> 6 {
        0b00 => Ok(first as u64),
        0b01 => {
            if *pos >= input.len() {
                return Err(RdbError::UnexpectedEof);
            }
            let second = input[*pos];
            *pos += 1;
            Ok((((first & 0x3F) as u64) << 8) | (second as u64))
        }
        0b10 => {
            if first == 0x80 {
                if *pos + 4 > input.len() {
                    return Err(RdbError::UnexpectedEof);
                }
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&input[*pos..*pos + 4]);
                *pos += 4;
                Ok(u32::from_le_bytes(bytes) as u64)
            } else {
                Err(RdbError::InvalidMagic)
            }
        }
        0b11 => match first & 0x3F {
            0 => {
                if *pos >= input.len() {
                    return Err(RdbError::UnexpectedEof);
                }
                let v = input[*pos];
                *pos += 1;
                Ok(v as u64)
            }
            1 => {
                if *pos + 2 > input.len() {
                    return Err(RdbError::UnexpectedEof);
                }
                let mut bytes = [0u8; 2];
                bytes.copy_from_slice(&input[*pos..*pos + 2]);
                *pos += 2;
                Ok(u16::from_le_bytes(bytes) as u64)
            }
            2 => {
                if *pos + 4 > input.len() {
                    return Err(RdbError::UnexpectedEof);
                }
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&input[*pos..*pos + 4]);
                *pos += 4;
                Ok(u32::from_le_bytes(bytes) as u64)
            }
            _ => Err(RdbError::InvalidMagic),
        },
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// String encoding (length-prefixed bytes)
// ---------------------------------------------------------------------------

fn encode_string(buf: &mut Vec<u8>, data: &[u8]) {
    encode_length(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

fn decode_string(input: &[u8], pos: &mut usize) -> Result<Vec<u8>> {
    let len = decode_length(input, pos)?;
    let end = *pos + len as usize;
    if end > input.len() {
        return Err(RdbError::UnexpectedEof);
    }
    let data = input[*pos..end].to_vec();
    *pos = end;
    Ok(data)
}

// ---------------------------------------------------------------------------
// CRC64
// ---------------------------------------------------------------------------

fn crc64(data: &[u8]) -> u64 {
    let crc = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);
    crc.checksum(data)
}

// ---------------------------------------------------------------------------
// Save
// ---------------------------------------------------------------------------

pub async fn save(store: &Arc<Store>, path: &Path) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();

    // Magic
    buf.extend_from_slice(RDB_MAGIC);

    // For each key in the store
    for item in store.iter() {
        let key = item.key();
        let entry = item.value();

        // TTL
        if let Some(expires_at) = entry.expires_at {
            let remaining = expires_at.saturating_duration_since(Instant::now());
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let expire_ms = now_ms + remaining.as_millis() as u64;
            buf.push(RDB_OPCODE_EXPIRETIME_MS);
            buf.extend_from_slice(&expire_ms.to_le_bytes());
        }

        // Type byte
        let type_byte: u8 = match &entry.data {
            DataType::String(_) => RDB_TYPE_STRING,
            DataType::List(_) => RDB_TYPE_LIST,
            DataType::Set(_) => RDB_TYPE_SET,
            DataType::ZSet(_) => RDB_TYPE_ZSET,
            DataType::Hash(_) => RDB_TYPE_HASH,
            DataType::Stream(_) => RDB_TYPE_STREAM,
        };
        buf.push(type_byte);

        // Key
        encode_string(&mut buf, key);

        // Value
        match &entry.data {
            DataType::String(s) => {
                encode_string(&mut buf, s);
            }
            DataType::List(list) => {
                encode_length(&mut buf, list.len() as u64);
                for elem in list {
                    encode_string(&mut buf, elem);
                }
            }
            DataType::Set(set) => {
                encode_length(&mut buf, set.len() as u64);
                for member in set {
                    encode_string(&mut buf, member);
                }
            }
            DataType::ZSet(zset) => {
                encode_length(&mut buf, zset.len() as u64);
                for (member, score) in &zset.members {
                    encode_string(&mut buf, member);
                    buf.extend_from_slice(&score.0.to_le_bytes());
                }
            }
            DataType::Hash(hash) => {
                encode_length(&mut buf, hash.len() as u64);
                for (field, value) in hash {
                    encode_string(&mut buf, field);
                    encode_string(&mut buf, value);
                }
            }
            DataType::Stream(stream) => {
                let total_entries: usize = stream.entries.values().map(|v| v.len()).sum();
                encode_length(&mut buf, stream.entries.len() as u64);
                encode_length(&mut buf, total_entries as u64);
                for (id, fields) in &stream.entries {
                    buf.extend_from_slice(&id.ms.to_le_bytes());
                    buf.extend_from_slice(&id.seq.to_le_bytes());
                    encode_length(&mut buf, fields.len() as u64);
                    for (f, v) in fields {
                        encode_string(&mut buf, f);
                        encode_string(&mut buf, v);
                    }
                }
                buf.extend_from_slice(&stream.last_id.ms.to_le_bytes());
                buf.extend_from_slice(&stream.last_id.seq.to_le_bytes());
            }
        }
    }

    // EOF
    buf.push(RDB_OPCODE_EOF);

    // CRC64 checksum
    let checksum = crc64(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    // Write file
    let mut file = File::create(path).await?;
    file.write_all(&buf).await?;
    file.flush().await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

pub async fn load(store: &Arc<Store>, path: &Path) -> Result<()> {
    let mut file = File::open(path).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;

    let pos = &mut 0;

    // Verify magic
    if buf.len() < 9 {
        return Err(RdbError::InvalidMagic);
    }
    if &buf[0..9] != RDB_MAGIC {
        return Err(RdbError::InvalidMagic);
    }
    *pos = 9;

    // Parse entries
    loop {
        if *pos >= buf.len() {
            return Err(RdbError::UnexpectedEof);
        }

        let type_byte = buf[*pos];

        // Check for EOF
        if type_byte == RDB_OPCODE_EOF {
            *pos += 1;
            break;
        }

        // Check for expire-time opcodes
        let mut expires_at: Option<Instant> = None;
        let actual_type = if type_byte == RDB_OPCODE_EXPIRETIME_MS {
            *pos += 1;
            if *pos + 8 > buf.len() {
                return Err(RdbError::UnexpectedEof);
            }
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&buf[*pos..*pos + 8]);
            *pos += 8;
            let expire_ms = u64::from_le_bytes(ts_bytes);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            if expire_ms > now_ms {
                expires_at = Some(Instant::now() + Duration::from_millis(expire_ms - now_ms));
            }
            if *pos >= buf.len() {
                return Err(RdbError::UnexpectedEof);
            }
            let t = buf[*pos];
            *pos += 1;
            t
        } else if type_byte == RDB_OPCODE_EXPIRETIME {
            *pos += 1;
            if *pos + 4 > buf.len() {
                return Err(RdbError::UnexpectedEof);
            }
            let mut ts_bytes = [0u8; 4];
            ts_bytes.copy_from_slice(&buf[*pos..*pos + 4]);
            *pos += 4;
            let expire_sec = u32::from_le_bytes(ts_bytes) as u64;
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if expire_sec > now_sec {
                expires_at = Some(Instant::now() + Duration::from_secs(expire_sec - now_sec));
            }
            if *pos >= buf.len() {
                return Err(RdbError::UnexpectedEof);
            }
            let t = buf[*pos];
            *pos += 1;
            t
        } else {
            *pos += 1;
            type_byte
        };

        let key = Bytes::from(decode_string(&buf, pos)?);
        let data = decode_value(&buf, pos, actual_type)?;

        let ttl = expires_at.and_then(|at| at.checked_duration_since(Instant::now()));
        let entry = Entry::new(data, ttl);
        store.keyspace.insert(key, entry);
    }

    // Verify CRC64
    if buf.len() < 8 {
        return Err(RdbError::UnexpectedEof);
    }
    let data_end = buf.len() - 8;
    let expected_crc = crc64(&buf[..data_end]);
    let mut crc_bytes = [0u8; 8];
    crc_bytes.copy_from_slice(&buf[data_end..]);
    let stored_crc = u64::from_le_bytes(crc_bytes);

    if stored_crc != expected_crc {
        return Err(RdbError::CrcMismatch);
    }

    Ok(())
}

fn decode_value(input: &[u8], pos: &mut usize, type_byte: u8) -> Result<DataType> {
    match type_byte {
        RDB_TYPE_STRING => {
            let data = decode_string(input, pos)?;
            Ok(DataType::String(Bytes::from(data)))
        }
        RDB_TYPE_LIST => {
            let len = decode_length(input, pos)?;
            let mut list = VecDeque::new();
            for _ in 0..len {
                let elem = decode_string(input, pos)?;
                list.push_back(Bytes::from(elem));
            }
            Ok(DataType::List(list))
        }
        RDB_TYPE_SET => {
            let len = decode_length(input, pos)?;
            let mut set = HashSet::new();
            for _ in 0..len {
                let member = decode_string(input, pos)?;
                set.insert(Bytes::from(member));
            }
            Ok(DataType::Set(set))
        }
        RDB_TYPE_ZSET => {
            let len = decode_length(input, pos)?;
            let mut members = HashMap::new();
            let mut scores = BTreeMap::new();
            for _ in 0..len {
                let member = Bytes::from(decode_string(input, pos)?);
                if *pos + 8 > input.len() {
                    return Err(RdbError::UnexpectedEof);
                }
                let mut score_bytes = [0u8; 8];
                score_bytes.copy_from_slice(&input[*pos..*pos + 8]);
                *pos += 8;
                let score = ordered_float::OrderedFloat(f64::from_le_bytes(score_bytes));
                members.insert(member.clone(), score);
                scores.entry(score).or_insert_with(BTreeSet::new).insert(member);
            }
            Ok(DataType::ZSet(valkey_storage::ZSetData { scores, members }))
        }
        RDB_TYPE_HASH => {
            let len = decode_length(input, pos)?;
            let mut hash = HashMap::new();
            for _ in 0..len {
                let field = Bytes::from(decode_string(input, pos)?);
                let value = Bytes::from(decode_string(input, pos)?);
                hash.insert(field, value);
            }
            Ok(DataType::Hash(hash))
        }
        RDB_TYPE_STREAM => {
            let num_ids = decode_length(input, pos)?;
            let _total_pairs = decode_length(input, pos)?;
            let mut entries = BTreeMap::new();
            for _ in 0..num_ids {
                if *pos + 16 > input.len() {
                    return Err(RdbError::UnexpectedEof);
                }
                let mut ms_bytes = [0u8; 8];
                ms_bytes.copy_from_slice(&input[*pos..*pos + 8]);
                *pos += 8;
                let mut seq_bytes = [0u8; 8];
                seq_bytes.copy_from_slice(&input[*pos..*pos + 8]);
                *pos += 8;
                let id = valkey_storage::StreamId {
                    ms: u64::from_le_bytes(ms_bytes),
                    seq: u64::from_le_bytes(seq_bytes),
                };
                let num_fields = decode_length(input, pos)?;
                let mut fields = Vec::new();
                for _ in 0..num_fields {
                    let f = Bytes::from(decode_string(input, pos)?);
                    let v = Bytes::from(decode_string(input, pos)?);
                    fields.push((f, v));
                }
                entries.insert(id, fields);
            }
            if *pos + 16 > input.len() {
                return Err(RdbError::UnexpectedEof);
            }
            let mut ms_bytes = [0u8; 8];
            ms_bytes.copy_from_slice(&input[*pos..*pos + 8]);
            *pos += 8;
            let mut seq_bytes = [0u8; 8];
            seq_bytes.copy_from_slice(&input[*pos..*pos + 8]);
            *pos += 8;
            let last_id = valkey_storage::StreamId {
                ms: u64::from_le_bytes(ms_bytes),
                seq: u64::from_le_bytes(seq_bytes),
            };
            Ok(DataType::Stream(valkey_storage::StreamData {
                entries,
                groups: HashMap::new(),
                last_id,
                deleted_ids: BTreeSet::new(),
            }))
        }
        other => Err(RdbError::UnknownType(other)),
    }
}

// ---------------------------------------------------------------------------
// Background save
// ---------------------------------------------------------------------------

pub async fn bgsave(store: Arc<Store>, path: std::path::PathBuf) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        save(&store, &path).await
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use valkey_storage::{StreamData, StreamId, ZSetData};

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
    async fn test_save_load_string() {
        let store = test_store();
        store.set(Bytes::from("hello"), DataType::String(Bytes::from("world")), None);

        let path = std::env::temp_dir().join("test_rdb_string.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let entry = store2.get(&Bytes::from("hello")).unwrap();
        match &entry.data {
            DataType::String(s) => assert_eq!(s, &Bytes::from("world")),
            other => panic!("expected string, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_save_load_list() {
        let store = test_store();
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.set(Bytes::from("mylist"), DataType::List(list), None);

        let path = std::env::temp_dir().join("test_rdb_list.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let entry = store2.get(&Bytes::from("mylist")).unwrap();
        match &entry.data {
            DataType::List(l) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], Bytes::from("a"));
                assert_eq!(l[1], Bytes::from("b"));
                assert_eq!(l[2], Bytes::from("c"));
            }
            other => panic!("expected list, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_save_load_set() {
        let store = test_store();
        let mut set = HashSet::new();
        set.insert(Bytes::from("x"));
        set.insert(Bytes::from("y"));
        store.set(Bytes::from("myset"), DataType::Set(set), None);

        let path = std::env::temp_dir().join("test_rdb_set.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let entry = store2.get(&Bytes::from("myset")).unwrap();
        match &entry.data {
            DataType::Set(s) => {
                assert_eq!(s.len(), 2);
                assert!(s.contains(&Bytes::from("x")));
                assert!(s.contains(&Bytes::from("y")));
            }
            other => panic!("expected set, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_save_load_hash() {
        let store = test_store();
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("val1"));
        hash.insert(Bytes::from("field2"), Bytes::from("val2"));
        store.set(Bytes::from("myhash"), DataType::Hash(hash), None);

        let path = std::env::temp_dir().join("test_rdb_hash.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let entry = store2.get(&Bytes::from("myhash")).unwrap();
        match &entry.data {
            DataType::Hash(h) => {
                assert_eq!(h.len(), 2);
                assert_eq!(h.get(&Bytes::from("field1")), Some(&Bytes::from("val1")));
                assert_eq!(h.get(&Bytes::from("field2")), Some(&Bytes::from("val2")));
            }
            other => panic!("expected hash, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_save_load_zset() {
        let store = test_store();
        let mut zset = ZSetData::new();
        zset.add(Bytes::from("member1"), 1.0);
        zset.add(Bytes::from("member2"), 2.5);
        store.set(Bytes::from("myzset"), DataType::ZSet(zset), None);

        let path = std::env::temp_dir().join("test_rdb_zset.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let entry = store2.get(&Bytes::from("myzset")).unwrap();
        match &entry.data {
            DataType::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(&Bytes::from("member1")), Some(1.0));
                assert_eq!(z.score(&Bytes::from("member2")), Some(2.5));
            }
            other => panic!("expected zset, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_save_load_stream() {
        let store = test_store();
        let mut stream = StreamData::new();
        stream.add(
            StreamId::new(1000, 0),
            vec![
                (Bytes::from("field1"), Bytes::from("val1")),
                (Bytes::from("field2"), Bytes::from("val2")),
            ],
        );
        store.set(Bytes::from("mystream"), DataType::Stream(stream), None);

        let path = std::env::temp_dir().join("test_rdb_stream.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let entry = store2.get(&Bytes::from("mystream")).unwrap();
        match &entry.data {
            DataType::Stream(s) => {
                assert_eq!(s.entries.len(), 1);
                assert_eq!(s.last_id.ms, 1000);
                assert_eq!(s.last_id.seq, 1);
            }
            other => panic!("expected stream, got {:?}", other),
        }

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_save_load_all_types() {
        let store = test_store();

        store.set(Bytes::from("str"), DataType::String(Bytes::from("value")), None);

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        store.set(Bytes::from("list"), DataType::List(list), None);

        let mut set = HashSet::new();
        set.insert(Bytes::from("x"));
        set.insert(Bytes::from("y"));
        store.set(Bytes::from("set"), DataType::Set(set), None);

        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        store.set(Bytes::from("hash"), DataType::Hash(hash), None);

        let mut zset = ZSetData::new();
        zset.add(Bytes::from("m1"), 1.0);
        zset.add(Bytes::from("m2"), 2.0);
        store.set(Bytes::from("zset"), DataType::ZSet(zset), None);

        let mut stream = StreamData::new();
        stream.add(StreamId::new(1000, 0), vec![(Bytes::from("k"), Bytes::from("v"))]);
        store.set(Bytes::from("stream"), DataType::Stream(stream), None);

        store.set(
            Bytes::from("expires"),
            DataType::String(Bytes::from("ttl_value")),
            Some(Duration::from_secs(3600)),
        );

        let path = std::env::temp_dir().join("test_rdb_all.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();

        let e = store2.get(&Bytes::from("str")).unwrap();
        match &e.data {
            DataType::String(s) => assert_eq!(s, &Bytes::from("value")),
            other => panic!("expected string, got {:?}", other),
        }

        let e = store2.get(&Bytes::from("list")).unwrap();
        match &e.data {
            DataType::List(l) => {
                assert_eq!(l.len(), 2);
                assert_eq!(l[0], Bytes::from("a"));
            }
            other => panic!("expected list, got {:?}", other),
        }

        let e = store2.get(&Bytes::from("set")).unwrap();
        match &e.data {
            DataType::Set(s) => assert_eq!(s.len(), 2),
            other => panic!("expected set, got {:?}", other),
        }

        let e = store2.get(&Bytes::from("hash")).unwrap();
        match &e.data {
            DataType::Hash(h) => {
                assert_eq!(h.get(&Bytes::from("f1")), Some(&Bytes::from("v1")));
            }
            other => panic!("expected hash, got {:?}", other),
        }

        let e = store2.get(&Bytes::from("zset")).unwrap();
        match &e.data {
            DataType::ZSet(z) => {
                assert_eq!(z.len(), 2);
                assert_eq!(z.score(&Bytes::from("m1")), Some(1.0));
            }
            other => panic!("expected zset, got {:?}", other),
        }

        let e = store2.get(&Bytes::from("stream")).unwrap();
        match &e.data {
            DataType::Stream(s) => assert_eq!(s.entries.len(), 1),
            other => panic!("expected stream, got {:?}", other),
        }

        assert!(store2.exists(&Bytes::from("expires")));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_bgsave() {
        let store = test_store();
        store.set(Bytes::from("key"), DataType::String(Bytes::from("val")), None);

        let path = std::env::temp_dir().join("test_rdb_bgsave.rdb");
        let handle = bgsave(store.clone(), path.clone()).await;
        handle.await.unwrap().unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();
        assert!(store2.exists(&Bytes::from("key")));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_crc_verification() {
        let store = test_store();
        store.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);

        let path = std::env::temp_dir().join("test_rdb_crc.rdb");
        save(&store, &path).await.unwrap();

        // Corrupt a data byte (not the header or CRC itself)
        let mut data = tokio::fs::read(&path).await.unwrap();
        // Byte 15 is well past the 9-byte magic and into the type/key data
        if data.len() > 20 {
            data[15] ^= 0xFF;
        }
        tokio::fs::write(&path, &data).await.unwrap();

        let store2 = test_store();
        let result = load(&store2, &path).await;
        assert!(result.is_err());

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_empty_store() {
        let store = test_store();
        let path = std::env::temp_dir().join("test_rdb_empty.rdb");
        save(&store, &path).await.unwrap();

        let store2 = test_store();
        load(&store2, &path).await.unwrap();
        assert_eq!(store2.dbsize(), 0);

        let _ = tokio::fs::remove_file(&path).await;
    }
}
