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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, member: Bytes, score: f64) {
        let score = OrderedFloat(score);
        if let Some(old_score) = self.members.get(&member) {
            if let Some(bucket) = self.scores.get_mut(old_score) {
                bucket.remove(&member);
                if bucket.is_empty() {
                    self.scores.remove(old_score);
                }
            }
        }
        self.members.insert(member.clone(), score);
        self.scores.entry(score).or_default().insert(member);
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

// ---------------------------------------------------------------------------
// Consumer group types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_delivered_id: StreamId,
    pub pending: BTreeMap<StreamId, PendingEntry>,
    pub consumers: HashMap<String, Consumer>,
    pub entries_read: i64,
}

impl ConsumerGroup {
    pub fn new(name: String, last_delivered_id: StreamId, entries_read: i64) -> Self {
        Self {
            name,
            last_delivered_id,
            pending: BTreeMap::new(),
            consumers: HashMap::new(),
            entries_read,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub consumer: String,
    pub delivered_at: Instant,
    pub delivery_count: u64,
}

impl PendingEntry {
    pub fn new(consumer: String) -> Self {
        Self {
            consumer,
            delivered_at: Instant::now(),
            delivery_count: 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    pub name: String,
    pub seen_time: Instant,
    pub pending: BTreeMap<StreamId, PendingEntry>,
}

impl Consumer {
    pub fn new(name: String) -> Self {
        Self {
            name,
            seen_time: Instant::now(),
            pending: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamData {
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub groups: HashMap<String, ConsumerGroup>,
    pub last_id: StreamId,
    pub deleted_ids: BTreeSet<StreamId>,
}

impl StreamData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, id: StreamId, fields: Vec<(Bytes, Bytes)>) {
        if id.ms > self.last_id.ms || (id.ms == self.last_id.ms && id.seq >= self.last_id.seq) {
            self.last_id = StreamId::new(id.ms, id.seq + 1);
        }
        self.entries.insert(id, fields);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn generate_id(&self, requested: Option<StreamId>) -> Result<StreamId, String> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let id = match requested {
            Some(id) => {
                if id.ms == 0 && id.seq == 0 {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".into());
                }
                if id.ms < self.last_id.ms
                    || (id.ms == self.last_id.ms && id.seq <= self.last_id.seq)
                {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
                }
                id
            }
            None => {
                let seq = if now_ms == self.last_id.ms {
                    self.last_id.seq
                } else {
                    0
                };
                StreamId::new(now_ms, seq)
            }
        };
        Ok(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    Noeviction,
    AllKeysLru,
    AllKeysLfu,
    VolatileLru,
    VolatileLfu,
    VolatileTtl,
    AllKeysRandom,
    VolatileRandom,
}

impl EvictionPolicy {
    pub fn from_policy_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Some(Self::Noeviction),
            "allkeys-lru" => Some(Self::AllKeysLru),
            "allkeys-lfu" => Some(Self::AllKeysLfu),
            "volatile-lru" => Some(Self::VolatileLru),
            "volatile-lfu" => Some(Self::VolatileLfu),
            "volatile-ttl" => Some(Self::VolatileTtl),
            "allkeys-random" => Some(Self::AllKeysRandom),
            "volatile-random" => Some(Self::VolatileRandom),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Noeviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLru => "volatile-lru",
            Self::VolatileLfu => "volatile-lfu",
            Self::VolatileTtl => "volatile-ttl",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
        }
    }

    pub fn is_volatile(&self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileLfu | Self::VolatileTtl | Self::VolatileRandom
        )
    }
}

pub fn lfu_log_incr(counter: u64) -> u64 {
    if counter < 255 {
        if counter == 0 {
            return 1;
        }
        let r = fastrand::u64(..);
        let threshold = counter.saturating_mul(10);
        if r.is_multiple_of(threshold) {
            counter + 1
        } else {
            counter
        }
    } else {
        counter
    }
}

#[derive(Debug, Clone)]
pub struct EvictionConfig {
    pub maxmemory: u64,
    pub policy: EvictionPolicy,
    pub maxmemory_samples: u8,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            maxmemory: 0,
            policy: EvictionPolicy::Noeviction,
            maxmemory_samples: 5,
        }
    }
}

#[derive(Debug)]
pub struct Entry {
    pub data: DataType,
    pub expires_at: Option<Instant>,
    pub last_access: Instant,
    pub access_count: u64,
    pub lfu_counter: u64,
}

impl Entry {
    pub fn new(data: DataType, ttl: Option<Duration>) -> Self {
        let now = Instant::now();
        Self {
            data,
            expires_at: ttl.map(|d| now + d),
            last_access: now,
            access_count: 1,
            lfu_counter: 1,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|t| t < Instant::now()).unwrap_or(false)
    }

    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count = self.access_count.saturating_add(1);
        self.lfu_counter = lfu_log_incr(self.lfu_counter);
    }

    pub fn memory_usage(&self, key_len: usize) -> usize {
        let data_size = match &self.data {
            DataType::String(s) => s.len(),
            DataType::List(v) => v.iter().map(|b| b.len()).sum::<usize>(),
            DataType::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
            DataType::Set(s) => s.iter().map(|b| b.len()).sum::<usize>(),
            DataType::ZSet(_) => std::mem::size_of::<ZSetData>(),
            DataType::Stream(_) => std::mem::size_of::<StreamData>(),
        };
        key_len + data_size + 64
    }
}

pub struct Store {
    pub keyspace: DashMap<Bytes, Entry>,
    pub evicted_keys: AtomicU64,
    pub evict_config: RwLock<EvictionConfig>,
    pub watchers: DashMap<Bytes, Vec<std::sync::mpsc::Sender<()>>>,
    pub dirty_count: AtomicU64,
}

impl Store {
    pub fn new() -> Arc<Self> {
        let store = Arc::new(Self {
            keyspace: DashMap::new(),
            evicted_keys: AtomicU64::new(0),
            evict_config: RwLock::new(EvictionConfig::default()),
            watchers: DashMap::new(),
            dirty_count: AtomicU64::new(0),
        });
        let store_weak = Arc::downgrade(&store);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if let Some(store) = store_weak.upgrade() {
                    store.evict_expired();
                } else {
                    break;
                }
            }
        });
        store
    }

    fn evict_expired(&self) {
        self.keyspace.retain(|_key, entry| {
            !entry
                .expires_at
                .map(|t| t < Instant::now())
                .unwrap_or(false)
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
        self.notify_watchers(&key);
        let entry = Entry::new(data, ttl);
        self.keyspace.insert(key, entry);
        self.dirty_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn del(&self, key: &Bytes) -> bool {
        self.notify_watchers(key);
        let result = self.keyspace.remove(key).is_some();
        if result {
            self.dirty_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn expire(&self, key: &Bytes, at: Instant) -> bool {
        self.notify_watchers(key);
        let result = self
            .keyspace
            .get_mut(key)
            .map(|mut e| {
                e.expires_at = Some(at);
            })
            .is_some();
        if result {
            self.dirty_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn exists(&self, key: &Bytes) -> bool {
        if let Some(entry) = self.keyspace.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.keyspace.remove(key);
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    pub fn ttl(&self, key: &Bytes) -> Option<Duration> {
        if let Some(entry) = self.keyspace.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.keyspace.remove(key);
                return None;
            }
            entry
                .expires_at
                .and_then(|at| at.checked_duration_since(Instant::now()))
        } else {
            None
        }
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
        } else {
            None
        }
    }

    pub fn keys(&self, pattern: &str) -> Vec<Bytes> {
        if pattern == "*" {
            return self
                .keyspace
                .iter()
                .filter(|e| !e.is_expired())
                .map(|e| e.key().clone())
                .collect();
        }
        let mut result = Vec::new();
        match pattern.split_once('*') {
            Some((prefix, suffix)) => {
                for entry in self.keyspace.iter() {
                    if entry.is_expired() {
                        continue;
                    }
                    let key_str = std::str::from_utf8(entry.key()).unwrap_or("");
                    if key_str.starts_with(prefix) && key_str.ends_with(suffix) {
                        result.push(entry.key().clone());
                    }
                }
            }
            None => {
                let key = Bytes::from(pattern.to_owned());
                if self.exists(&key) {
                    result.push(key);
                }
            }
        }
        result
    }

    pub fn dbsize(&self) -> usize {
        self.keyspace.len()
    }
    pub fn flush(&self) {
        self.keyspace.clear();
        self.watchers.clear();
    }

    /// Register a watcher for a key. Returns a receiver that will be notified
    /// when the key is modified.
    pub fn watch(&self, key: &Bytes) -> std::sync::mpsc::Receiver<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.watchers.entry(key.clone()).or_default().push(tx);
        rx
    }

    fn notify_watchers(&self, key: &Bytes) {
        if let Some((_, senders)) = self.watchers.remove(key) {
            for tx in senders {
                let _ = tx.send(());
            }
        }
    }

    pub fn memory_usage_bytes(&self) -> u64 {
        let mut total: u64 = 0;
        for entry in self.keyspace.iter() {
            total += entry.memory_usage(entry.key().len()) as u64;
        }
        total
    }

    pub fn key_memory_usage(&self, key: &Bytes) -> Option<u64> {
        self.keyspace
            .get(key)
            .map(|e| e.memory_usage(key.len()) as u64)
    }

    pub fn eviction_config(&self) -> EvictionConfig {
        self.evict_config.read().unwrap().clone()
    }

    pub fn set_eviction_config(&self, config: EvictionConfig) {
        *self.evict_config.write().unwrap() = config;
    }

    pub fn maybe_evict(&self) -> Result<(), String> {
        let config = self.eviction_config();
        if config.maxmemory == 0 {
            return Ok(());
        }
        if self.memory_usage_bytes() <= config.maxmemory {
            return Ok(());
        }

        let policy = config.policy;
        if policy == EvictionPolicy::Noeviction {
            return Err("ERR OOM command not allowed when used memory > 'maxmemory'.".into());
        }

        let samples = config.maxmemory_samples.max(1) as usize;
        let all_keys: Vec<Bytes> = self.keyspace.iter().map(|e| e.key().clone()).collect();
        if all_keys.is_empty() {
            return Ok(());
        }

        let mut candidates: Vec<Bytes> = Vec::new();
        if all_keys.len() <= samples {
            candidates = all_keys;
        } else {
            let mut rng_keys = all_keys;
            for _ in 0..samples {
                if rng_keys.is_empty() {
                    break;
                }
                let idx = fastrand::usize(..rng_keys.len());
                candidates.push(rng_keys.swap_remove(idx));
            }
        }

        let mut victim: Option<(Bytes, i64)> = None;
        for key in &candidates {
            let entry = match self.keyspace.get(key) {
                Some(e) => e,
                None => continue,
            };
            if policy.is_volatile() && entry.expires_at.is_none() {
                continue;
            }

            let score: i64 = match policy {
                EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
                    entry.last_access.elapsed().as_nanos().min(i64::MAX as u128) as i64
                }
                EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
                    i64::MAX - (entry.lfu_counter.min(255) as i64)
                }
                EvictionPolicy::VolatileTtl => match entry.expires_at {
                    Some(at) => {
                        let remaining = at.saturating_duration_since(Instant::now()).as_nanos();
                        i64::MAX - (remaining.min(i64::MAX as u128) as i64)
                    }
                    None => -1,
                },
                EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => fastrand::i64(..),
                EvictionPolicy::Noeviction => unreachable!(),
            };
            if victim.is_none() || score > victim.as_ref().unwrap().1 {
                victim = Some((key.clone(), score));
            }
        }

        if let Some((ref key, _)) = victim {
            self.keyspace.remove(key);
            self.evicted_keys.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn evicted_count(&self) -> u64 {
        self.evicted_keys.load(Ordering::Relaxed)
    }

    pub fn dirty_count(&self) -> u64 {
        self.dirty_count.load(Ordering::Relaxed)
    }

    pub fn reset_dirty_count(&self) -> u64 {
        self.dirty_count.swap(0, Ordering::Relaxed)
    }
}

impl Deref for Store {
    type Target = DashMap<Bytes, Entry>;
    fn deref(&self) -> &Self::Target {
        &self.keyspace
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Arc<Store> {
        Arc::new(Store {
            keyspace: DashMap::new(),
            evicted_keys: AtomicU64::new(0),
            evict_config: RwLock::new(EvictionConfig::default()),
            watchers: DashMap::new(),
            dirty_count: AtomicU64::new(0),
        })
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
            };
        }
        assert!(store.del(&key));
        assert!(store.get(&key).is_none());
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
        store.set(
            Bytes::from("user:1"),
            DataType::String(Bytes::from("a")),
            None,
        );
        store.set(
            Bytes::from("user:2"),
            DataType::String(Bytes::from("b")),
            None,
        );
        store.set(
            Bytes::from("post:1"),
            DataType::String(Bytes::from("c")),
            None,
        );
        let mut keys = store.keys("user:*");
        keys.sort();
        assert_eq!(keys, vec![Bytes::from("user:1"), Bytes::from("user:2")]);
        let mut all = store.keys("*");
        all.sort();
        assert_eq!(
            all,
            vec![
                Bytes::from("post:1"),
                Bytes::from("user:1"),
                Bytes::from("user:2")
            ]
        );
    }

    #[test]
    fn zset_data() {
        let mut zset = ZSetData::new();
        zset.add(Bytes::from("alice"), 1.0);
        zset.add(Bytes::from("bob"), 2.0);
        zset.add(Bytes::from("charlie"), 1.0);
        assert_eq!(zset.len(), 3);
        assert_eq!(zset.score(&Bytes::from("alice")), Some(1.0));
        zset.add(Bytes::from("alice"), 3.0);
        assert_eq!(zset.score(&Bytes::from("alice")), Some(3.0));
        assert_eq!(zset.len(), 3);
    }

    #[test]
    fn stream_id_display() {
        let id = StreamId::new(1700000000000, 0);
        assert_eq!(format!("{}", id), "1700000000000-0");
    }

    #[test]
    fn eviction_policy_from_str() {
        assert_eq!(
            EvictionPolicy::from_policy_str("noeviction"),
            Some(EvictionPolicy::Noeviction)
        );
        assert_eq!(
            EvictionPolicy::from_policy_str("allkeys-lru"),
            Some(EvictionPolicy::AllKeysLru)
        );
        assert_eq!(EvictionPolicy::from_policy_str("invalid"), None);
    }

    #[test]
    fn eviction_policy_is_volatile() {
        assert!(!EvictionPolicy::Noeviction.is_volatile());
        assert!(EvictionPolicy::VolatileLru.is_volatile());
    }

    #[test]
    fn lfu_log_incr_basic() {
        assert_eq!(lfu_log_incr(0), 1);
        let mut c = 1u64;
        for _ in 0..1000 {
            c = lfu_log_incr(c);
        }
        assert!(c > 1);
        assert_eq!(lfu_log_incr(255), 255);
    }

    #[test]
    fn noeviction_returns_oom() {
        let store = test_store();
        store.set_eviction_config(EvictionConfig {
            maxmemory: 1,
            policy: EvictionPolicy::Noeviction,
            maxmemory_samples: 5,
        });
        store.set(
            Bytes::from("existing"),
            DataType::String(Bytes::from("data")),
            None,
        );
        assert!(store.maybe_evict().is_err());
    }

    #[test]
    fn volatile_lru_only_evicts_ttl_keys() {
        let store = test_store();
        store.set_eviction_config(EvictionConfig {
            maxmemory: 1,
            policy: EvictionPolicy::VolatileLru,
            maxmemory_samples: 5,
        });
        store.set(
            Bytes::from("no_ttl"),
            DataType::String(Bytes::from("data")),
            None,
        );
        store.set(
            Bytes::from("ttl"),
            DataType::String(Bytes::from("data")),
            Some(Duration::from_secs(60)),
        );
        store.maybe_evict().unwrap();
        assert!(store.exists(&Bytes::from("no_ttl")));
    }

    #[test]
    fn volatile_ttl_prefers_smaller_ttl() {
        let store = test_store();
        store.set_eviction_config(EvictionConfig {
            maxmemory: 1,
            policy: EvictionPolicy::VolatileTtl,
            maxmemory_samples: 5,
        });
        store.set(
            Bytes::from("long_ttl"),
            DataType::String(Bytes::from("data")),
            Some(Duration::from_secs(300)),
        );
        store.set(
            Bytes::from("short_ttl"),
            DataType::String(Bytes::from("data")),
            Some(Duration::from_secs(1)),
        );
        store.maybe_evict().unwrap();
        assert!(store.exists(&Bytes::from("long_ttl")));
    }

    #[test]
    fn allkeys_random_evicts() {
        let store = test_store();
        store.set_eviction_config(EvictionConfig {
            maxmemory: 1,
            policy: EvictionPolicy::AllKeysRandom,
            maxmemory_samples: 5,
        });
        for i in 0..10 {
            store.set(
                Bytes::from(format!("k{}", i)),
                DataType::String(Bytes::from("d")),
                None,
            );
        }
        store.maybe_evict().unwrap();
        assert_eq!(store.dbsize(), 9);
    }

    #[test]
    fn maxmemory_zero_never_evicts() {
        let store = test_store();
        store.set_eviction_config(EvictionConfig {
            maxmemory: 0,
            policy: EvictionPolicy::AllKeysLru,
            maxmemory_samples: 5,
        });
        for i in 0..50 {
            store.set(
                Bytes::from(format!("k{}", i)),
                DataType::String(Bytes::from("d")),
                None,
            );
        }
        store.maybe_evict().unwrap();
        assert_eq!(store.dbsize(), 50);
    }

    #[test]
    fn eviction_config_roundtrip() {
        let store = test_store();
        let cfg = EvictionConfig {
            maxmemory: 1024,
            policy: EvictionPolicy::AllKeysLfu,
            maxmemory_samples: 10,
        };
        store.set_eviction_config(cfg.clone());
        let r = store.eviction_config();
        assert_eq!(r.maxmemory, cfg.maxmemory);
        assert_eq!(r.policy, cfg.policy);
    }

    #[test]
    fn memory_usage_nonzero() {
        let store = test_store();
        assert_eq!(store.memory_usage_bytes(), 0);
        store.set(Bytes::from("k"), DataType::String(Bytes::from("v")), None);
        assert!(store.memory_usage_bytes() > 0);
    }

    #[test]
    fn key_memory_usage_missing() {
        let store = test_store();
        assert!(store.key_memory_usage(&Bytes::from("missing")).is_none());
    }
}
