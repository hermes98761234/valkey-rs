use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use ordered_float::OrderedFloat;

#[derive(Debug, Clone)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    ZSet(ZSetData),
    Stream(StreamData),
}

#[derive(Debug, Clone, Default)]
pub struct ZSetData {
    pub scores: BTreeMap<OrderedFloat<f64>, BTreeSet<Bytes>>,
    pub members: HashMap<Bytes, OrderedFloat<f64>>,
}

impl ZSetData {
    pub fn new() -> Self { Self::default() }

    pub fn add(&mut self, member: Bytes, score: f64) {
        let score = OrderedFloat(score);
        if let Some(old_score) = self.members.get(&member) {
            if let Some(bucket) = self.scores.get_mut(old_score) {
                bucket.remove(&member);
                if bucket.is_empty() { self.scores.remove(old_score); }
            }
        }
        self.members.insert(member.clone(), score);
        self.scores.entry(score).or_default().insert(member);
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn len(&self) -> usize { self.members.len() }
    pub fn is_empty(&self) -> bool { self.members.is_empty() }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self { Self { ms, seq } }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamData {
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
}

impl StreamData {
    pub fn new() -> Self { Self::default() }

    pub fn add(&mut self, id: StreamId, fields: Vec<(Bytes, Bytes)>) {
        self.entries.insert(id, fields);
    }

    pub fn len(&self) -> usize { self.entries.len() }
}

#[derive(Debug)]
pub struct Entry {
    pub data: DataType,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(data: DataType, ttl: Option<Duration>) -> Self {
        Self { data, expires_at: ttl.map(|d| Instant::now() + d) }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|t| t < Instant::now()).unwrap_or(false)
    }
}

pub struct Store {
    keyspace: DashMap<Bytes, Entry>,
}

impl Store {
    pub fn new() -> Arc<Self> {
        let store = Arc::new(Self { keyspace: DashMap::new() });
        let store_weak = Arc::downgrade(&store);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if let Some(store) = store_weak.upgrade() {
                    store.evict_expired();
                } else { break; }
            }
        });
        store
    }

    fn evict_expired(&self) {
        self.keyspace.retain(|_key, entry| {
            !entry.expires_at.map(|t| t < Instant::now()).unwrap_or(false)
        });
    }

    pub fn get(&self, key: &Bytes) -> Option<Ref<'_, Bytes, Entry>> {
        let entry = self.keyspace.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.keyspace.remove(key);
            return None;
        }
        Some(entry)
    }

    pub fn set(&self, key: Bytes, data: DataType, ttl: Option<Duration>) {
        self.keyspace.insert(key, Entry::new(data, ttl));
    }

    pub fn del(&self, key: &Bytes) -> bool {
        self.keyspace.remove(key).is_some()
    }

    pub fn exists(&self, key: &Bytes) -> bool {
        if let Some(entry) = self.keyspace.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.keyspace.remove(key);
                return false;
            }
            true
        } else { false }
    }

    pub fn expire(&self, key: &Bytes, at: Instant) -> bool {
        self.keyspace.get_mut(key).map(|mut e| {
            e.expires_at = Some(at);
        }).is_some()
    }

    pub fn ttl(&self, key: &Bytes) -> Option<Duration> {
        if let Some(entry) = self.keyspace.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.keyspace.remove(key);
                return None;
            }
            entry.expires_at.and_then(|at| at.checked_duration_since(Instant::now()))
        } else { None }
    }

    pub fn type_of(&self, key: &Bytes) -> Option<&'static str> {
        if let Some(entry) = self.keyspace.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.keyspace.remove(key);
                return None;
            }
            Some(match &entry.data {
                DataType::String(_) => "string",
                DataType::List(_) => "list",
                DataType::Hash(_) => "hash",
                DataType::Set(_) => "set",
                DataType::ZSet(_) => "zset",
                DataType::Stream(_) => "stream",
            })
        } else { None }
    }

    pub fn keys(&self, pattern: &str) -> Vec<Bytes> {
        if pattern == "*" {
            return self.keyspace.iter()
                .filter(|e| !e.is_expired())
                .map(|e| e.key().clone())
                .collect();
        }
        let mut result = Vec::new();
        match pattern.split_once('*') {
            Some((prefix, suffix)) => {
                for entry in self.keyspace.iter() {
                    if entry.is_expired() { continue; }
                    let key_str = std::str::from_utf8(entry.key()).unwrap_or("");
                    if key_str.starts_with(prefix) && key_str.ends_with(suffix) {
                        result.push(entry.key().clone());
                    }
                }
            }
            None => {
                let key = Bytes::from(pattern.to_owned());
                if self.exists(&key) { result.push(key); }
            }
        }
        result
    }

    pub fn dbsize(&self) -> usize { self.keyspace.len() }
    pub fn flush(&self) { self.keyspace.clear(); }
}

impl Deref for Store {
    type Target = DashMap<Bytes, Entry>;
    fn deref(&self) -> &Self::Target { &self.keyspace }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Arc<Store> {
        Arc::new(Store { keyspace: DashMap::new() })
    }

    #[test]
    fn set_get_del() {
        let store = test_store();
        let key = Bytes::from("hello");
        let val = Bytes::from("world");
        store.set(key.clone(), DataType::String(val.clone()), None);
        {
            let entry = store.get(&key).expect("key should exist");
            match &entry.data {
                DataType::String(v) => assert_eq!(v, &val),
                _ => panic!("expected string"),
            }
        }
        assert!(store.del(&key));
        assert!(store.get(&key).is_none());
        assert!(!store.del(&key));
    }

    #[tokio::test]
    async fn ttl_expiry() {
        let store = Store::new();
        let key = Bytes::from("temp");
        store.set(
            key.clone(),
            DataType::String(Bytes::from("gone soon")),
            Some(Duration::from_millis(50)),
        );
        assert!(store.get(&key).is_some());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(store.get(&key).is_none());
        assert!(!store.exists(&key));
    }

    #[test]
    fn type_of() {
        let store = test_store();
        store.set(Bytes::from("s"), DataType::String(Bytes::from("x")), None);
        store.set(Bytes::from("l"), DataType::List(VecDeque::new()), None);
        store.set(Bytes::from("h"), DataType::Hash(HashMap::new()), None);
        store.set(Bytes::from("set"), DataType::Set(HashSet::new()), None);
        store.set(Bytes::from("z"), DataType::ZSet(ZSetData::new()), None);
        store.set(Bytes::from("st"), DataType::Stream(StreamData::new()), None);
        assert_eq!(store.type_of(&Bytes::from("s")), Some("string"));
        assert_eq!(store.type_of(&Bytes::from("l")), Some("list"));
        assert_eq!(store.type_of(&Bytes::from("h")), Some("hash"));
        assert_eq!(store.type_of(&Bytes::from("set")), Some("set"));
        assert_eq!(store.type_of(&Bytes::from("z")), Some("zset"));
        assert_eq!(store.type_of(&Bytes::from("st")), Some("stream"));
        assert_eq!(store.type_of(&Bytes::from("nope")), None);
    }

    #[test]
    fn flush() {
        let store = test_store();
        store.set(Bytes::from("a"), DataType::String(Bytes::from("1")), None);
        store.set(Bytes::from("b"), DataType::String(Bytes::from("2")), None);
        store.set(Bytes::from("c"), DataType::String(Bytes::from("3")), None);
        assert_eq!(store.dbsize(), 3);
        store.flush();
        assert_eq!(store.dbsize(), 0);
        assert!(store.get(&Bytes::from("a")).is_none());
    }

    #[test]
    fn exists() {
        let store = test_store();
        let key = Bytes::from("k");
        assert!(!store.exists(&key));
        store.set(key.clone(), DataType::String(Bytes::from("v")), None);
        assert!(store.exists(&key));
        store.del(&key);
        assert!(!store.exists(&key));
    }

    #[test]
    fn keys_glob() {
        let store = test_store();
        store.set(Bytes::from("user:1"), DataType::String(Bytes::from("a")), None);
        store.set(Bytes::from("user:2"), DataType::String(Bytes::from("b")), None);
        store.set(Bytes::from("post:1"), DataType::String(Bytes::from("c")), None);
        let mut keys = store.keys("user:*");
        keys.sort();
        assert_eq!(keys, vec![Bytes::from("user:1"), Bytes::from("user:2")]);
        let mut all = store.keys("*");
        all.sort();
        assert_eq!(all, vec![Bytes::from("post:1"), Bytes::from("user:1"), Bytes::from("user:2")]);
        let exact = store.keys("user:1");
        assert_eq!(exact, vec![Bytes::from("user:1")]);
    }

    #[test]
    fn zset_data() {
        let mut zset = ZSetData::new();
        zset.add(Bytes::from("alice"), 1.0);
        zset.add(Bytes::from("bob"), 2.0);
        zset.add(Bytes::from("charlie"), 1.0);
        assert_eq!(zset.len(), 3);
        assert_eq!(zset.score(&Bytes::from("alice")), Some(1.0));
        assert_eq!(zset.score(&Bytes::from("bob")), Some(2.0));
        zset.add(Bytes::from("alice"), 3.0);
        assert_eq!(zset.score(&Bytes::from("alice")), Some(3.0));
        assert_eq!(zset.len(), 3);
    }

    #[test]
    fn stream_id_display() {
        let id = StreamId::new(1700000000000, 0);
        assert_eq!(format!("{}", id), "1700000000000-0");
    }
}
