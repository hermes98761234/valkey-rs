use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Thread-safe hash storage: key -> (field -> value)
pub type HashStore = Arc<DashMap<Bytes, HashMap<Bytes, Bytes>>>;

/// Create a new HashStore.
pub fn new_hash_store() -> HashStore {
    Arc::new(DashMap::new())
}

// ---------------------------------------------------------------------------
// HSET
// ---------------------------------------------------------------------------
pub struct HSet {
    pub key: Bytes,
    pub pairs: Vec<(Bytes, Bytes)>,
}

impl HSet {
    pub fn new(key: Bytes, pairs: Vec<(Bytes, Bytes)>) -> Self {
        Self { key, pairs }
    }

    pub fn execute(&self, store: &HashStore) -> i64 {
        let mut entry = store.entry(self.key.clone()).or_insert_with(HashMap::new);
        let mut added = 0i64;
        for (field, value) in &self.pairs {
            if !entry.contains_key(field) {
                added += 1;
            }
            entry.insert(field.clone(), value.clone());
        }
        added
    }
}

// ---------------------------------------------------------------------------
// HGET
// ---------------------------------------------------------------------------
pub struct HGet {
    pub key: Bytes,
    pub field: Bytes,
}

impl HGet {
    pub fn new(key: Bytes, field: Bytes) -> Self {
        Self { key, field }
    }

    pub fn execute(&self, store: &HashStore) -> Option<Bytes> {
        store.get(&self.key).and_then(|e| e.get(&self.field).cloned())
    }
}

// ---------------------------------------------------------------------------
// HMGET
// ---------------------------------------------------------------------------
pub struct HMGet {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}

impl HMGet {
    pub fn new(key: Bytes, fields: Vec<Bytes>) -> Self {
        Self { key, fields }
    }

    pub fn execute(&self, store: &HashStore) -> Vec<Option<Bytes>> {
        match store.get(&self.key) {
            Some(e) => self.fields.iter().map(|f| e.get(f).cloned()).collect(),
            None => vec![None; self.fields.len()],
        }
    }
}

// ---------------------------------------------------------------------------
// HMSET (deprecated alias for HSET)
// ---------------------------------------------------------------------------
pub struct HMSet {
    pub key: Bytes,
    pub pairs: Vec<(Bytes, Bytes)>,
}

impl HMSet {
    pub fn new(key: Bytes, pairs: Vec<(Bytes, Bytes)>) -> Self {
        Self { key, pairs }
    }

    pub fn execute(&self, store: &HashStore) -> &'static str {
        let mut entry = store.entry(self.key.clone()).or_insert_with(HashMap::new);
        for (field, value) in &self.pairs {
            entry.insert(field.clone(), value.clone());
        }
        "OK"
    }
}

// ---------------------------------------------------------------------------
// HGETALL
// ---------------------------------------------------------------------------
pub struct HGetAll {
    pub key: Bytes,
}

impl HGetAll {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    pub fn execute(&self, store: &HashStore) -> Vec<Bytes> {
        match store.get(&self.key) {
            Some(e) => {
                let mut r = Vec::with_capacity(e.len() * 2);
                for (k, v) in e.iter() {
                    r.push(k.clone());
                    r.push(v.clone());
                }
                r
            }
            None => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// HDEL
// ---------------------------------------------------------------------------
pub struct HDel {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}

impl HDel {
    pub fn new(key: Bytes, fields: Vec<Bytes>) -> Self {
        Self { key, fields }
    }

    pub fn execute(&self, store: &HashStore) -> i64 {
        match store.get_mut(&self.key) {
            Some(mut e) => {
                let mut c = 0i64;
                for f in &self.fields {
                    if e.remove(f).is_some() {
                        c += 1;
                    }
                }
                c
            }
            None => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// HEXISTS
// ---------------------------------------------------------------------------
pub struct HExists {
    pub key: Bytes,
    pub field: Bytes,
}

impl HExists {
    pub fn new(key: Bytes, field: Bytes) -> Self {
        Self { key, field }
    }

    pub fn execute(&self, store: &HashStore) -> i32 {
        match store.get(&self.key) {
            Some(e) if e.contains_key(&self.field) => 1,
            _ => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// HLEN
// ---------------------------------------------------------------------------
pub struct HLen {
    pub key: Bytes,
}

impl HLen {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    pub fn execute(&self, store: &HashStore) -> i64 {
        store.get(&self.key).map(|e| e.len() as i64).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// HKEYS
// ---------------------------------------------------------------------------
pub struct HKeys {
    pub key: Bytes,
}

impl HKeys {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    pub fn execute(&self, store: &HashStore) -> Vec<Bytes> {
        match store.get(&self.key) {
            Some(e) => e.keys().cloned().collect(),
            None => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// HVALS
// ---------------------------------------------------------------------------
pub struct HVals {
    pub key: Bytes,
}

impl HVals {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    pub fn execute(&self, store: &HashStore) -> Vec<Bytes> {
        match store.get(&self.key) {
            Some(e) => e.values().cloned().collect(),
            None => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// HINCRBY
// ---------------------------------------------------------------------------
pub struct HIncrBy {
    pub key: Bytes,
    pub field: Bytes,
    pub increment: i64,
}

impl HIncrBy {
    pub fn new(key: Bytes, field: Bytes, increment: i64) -> Self {
        Self { key, field, increment }
    }

    pub fn execute(&self, store: &HashStore) -> Result<i64, String> {
        let mut entry = store.entry(self.key.clone()).or_insert_with(HashMap::new);
        let cur = entry
            .get(&self.field)
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let nv = cur + self.increment;
        entry.insert(self.field.clone(), Bytes::from(nv.to_string()));
        Ok(nv)
    }
}

// ---------------------------------------------------------------------------
// HINCRBYFLOAT
// ---------------------------------------------------------------------------
pub struct HIncrByFloat {
    pub key: Bytes,
    pub field: Bytes,
    pub increment: f64,
}

impl HIncrByFloat {
    pub fn new(key: Bytes, field: Bytes, increment: f64) -> Self {
        Self { key, field, increment }
    }

    pub fn execute(&self, store: &HashStore) -> Result<Bytes, String> {
        let mut entry = store.entry(self.key.clone()).or_insert_with(HashMap::new);
        let cur = entry
            .get(&self.field)
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0_f64);
        let nv = cur + self.increment;
        let repr = format!("{:.}", nv);
        entry.insert(self.field.clone(), Bytes::from(repr.clone()));
        Ok(Bytes::from(repr))
    }
}

// ---------------------------------------------------------------------------
// HSCAN
// ---------------------------------------------------------------------------
pub struct HScan {
    pub key: Bytes,
    pub cursor: usize,
    pub pattern: Option<Bytes>,
    pub count: usize,
}

impl HScan {
    pub fn new(key: Bytes, cursor: usize, pattern: Option<Bytes>, count: usize) -> Self {
        Self { key, cursor, pattern, count: count.max(1) }
    }

    pub fn execute(&self, store: &HashStore) -> (usize, Vec<Bytes>) {
        let entry = match store.get(&self.key) {
            Some(e) => e,
            None => return (0, Vec::new()),
        };
        let mut sorted: Vec<(&Bytes, &Bytes)> = entry.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));

        let filtered: Vec<(&Bytes, &Bytes)> = match &self.pattern {
            Some(pat) if pat.as_ref() != b"*" => {
                let pat_str = match std::str::from_utf8(pat) {
                    Ok(s) => s.to_string(),
                    Err(_) => return (0, Vec::new()),
                };
                sorted.into_iter().filter(|(k, _)| {
                    let ks = std::str::from_utf8(k).unwrap_or("");
                    if pat_str.starts_with('*') && pat_str.ends_with('*') {
                        ks.contains(&pat_str[1..pat_str.len()-1])
                    } else if pat_str.starts_with('*') {
                        ks.ends_with(&pat_str[1..])
                    } else if pat_str.ends_with('*') {
                        ks.starts_with(&pat_str[..pat_str.len()-1])
                    } else {
                        ks == pat_str
                    }
                }).collect()
            }
            _ => sorted,
        };

        let start = self.cursor.min(filtered.len());
        let end = (start + self.count).min(filtered.len());
        let next = if end >= filtered.len() { 0 } else { end };
        let mut r = Vec::with_capacity((end - start) * 2);
        for (k, v) in &filtered[start..end] {
            r.push((*k).clone());
            r.push((*v).clone());
        }
        (next, r)
    }
}

// ---------------------------------------------------------------------------
// HRANDFIELD
// ---------------------------------------------------------------------------
pub struct HRandField {
    pub key: Bytes,
    pub count: i64,
    pub with_values: bool,
}

impl HRandField {
    pub fn new(key: Bytes, count: i64, with_values: bool) -> Self {
        Self { key, count, with_values }
    }

    pub fn execute(&self, store: &HashStore) -> Vec<Bytes> {
        let entry = match store.get(&self.key) {
            Some(e) if !e.is_empty() => e,
            _ => return Vec::new(),
        };
        let fields: Vec<Bytes> = entry.keys().cloned().collect();
        let n = fields.len();
        let abs_c = self.count.unsigned_abs() as usize;

        if self.count >= 0 {
            let take = abs_c.min(n);
            let mut idx: Vec<usize> = (0..n).collect();
            for i in 0..take {
                let j = fastrand::usize(i..n);
                idx.swap(i, j);
            }
            let mut r = Vec::with_capacity(if self.with_values { take * 2 } else { take });
            for &i in &idx[..take] {
                let f = &fields[i];
                r.push(f.clone());
                if self.with_values {
                    r.push(entry[f].clone());
                }
            }
            r
        } else {
            let mut r = Vec::with_capacity(if self.with_values { abs_c * 2 } else { abs_c });
            for _ in 0..abs_c {
                let i = fastrand::usize(0..n);
                let f = &fields[i];
                r.push(f.clone());
                if self.with_values {
                    r.push(entry[f].clone());
                }
            }
            r
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> HashStore { new_hash_store() }
    fn b(s: &str) -> Bytes { Bytes::from(s.to_string()) }

    #[test]
    fn hset_new_fields() {
        let s = st();
        assert_eq!(HSet::new(b("k"), vec![(b("f1"), b("v1")), (b("f2"), b("v2"))]).execute(&s), 2);
    }

    #[test]
    fn hset_overwrite() {
        let s = st();
        HSet::new(b("k"), vec![(b("f1"), b("v1"))]).execute(&s);
        assert_eq!(HSet::new(b("k"), vec![(b("f1"), b("x"))]).execute(&s), 0);
    }

    #[test]
    fn hset_mixed() {
        let s = st();
        HSet::new(b("k"), vec![(b("f1"), b("v1"))]).execute(&s);
        assert_eq!(HSet::new(b("k"), vec![(b("f1"), b("x")), (b("f2"), b("v2"))]).execute(&s), 1);
    }

    #[test]
    fn hget_existing() {
        let s = st();
        HSet::new(b("k"), vec![(b("f"), b("hello"))]).execute(&s);
        assert_eq!(HGet::new(b("k"), b("f")).execute(&s), Some(b("hello")));
    }

    #[test]
    fn hget_missing() {
        let s = st();
        HSet::new(b("k"), vec![(b("f"), b("v"))]).execute(&s);
        assert_eq!(HGet::new(b("k"), b("x")).execute(&s), None);
        assert_eq!(HGet::new(b("z"), b("f")).execute(&s), None);
    }

    #[test]
    fn hmget_mixed() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2"))]).execute(&s);
        let r = HMGet::new(b("k"), vec![b("a"), b("b"), b("c")]).execute(&s);
        assert_eq!(r, vec![Some(b("1")), Some(b("2")), None]);
    }

    #[test]
    fn hmget_missing_key() {
        let s = st();
        let r = HMGet::new(b("z"), vec![b("a")]).execute(&s);
        assert_eq!(r, vec![None]);
    }

    #[test]
    fn hmset_ok() {
        let s = st();
        assert_eq!(HMSet::new(b("k"), vec![(b("f1"), b("v1"))]).execute(&s), "OK");
        assert_eq!(HGet::new(b("k"), b("f1")).execute(&s), Some(b("v1")));
    }

    #[test]
    fn hgetall_pairs() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2"))]).execute(&s);
        let r = HGetAll::new(b("k")).execute(&s);
        assert_eq!(r.len(), 4);
    }

    #[test]
    fn hgetall_empty() {
        let s = st();
        assert!(HGetAll::new(b("z")).execute(&s).is_empty());
    }

    #[test]
    fn hdel_existing() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))]).execute(&s);
        assert_eq!(HDel::new(b("k"), vec![b("a"), b("c")]).execute(&s), 2);
        assert_eq!(HGet::new(b("k"), b("a")).execute(&s), None);
        assert_eq!(HGet::new(b("k"), b("b")).execute(&s), Some(b("2")));
    }

    #[test]
    fn hdel_missing() {
        let s = st();
        assert_eq!(HDel::new(b("z"), vec![b("a")]).execute(&s), 0);
        HSet::new(b("k"), vec![(b("a"), b("1"))]).execute(&s);
        assert_eq!(HDel::new(b("k"), vec![b("z")]).execute(&s), 0);
    }

    #[test]
    fn hexists_yes() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1"))]).execute(&s);
        assert_eq!(HExists::new(b("k"), b("a")).execute(&s), 1);
    }

    #[test]
    fn hexists_no() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1"))]).execute(&s);
        assert_eq!(HExists::new(b("k"), b("b")).execute(&s), 0);
        assert_eq!(HExists::new(b("z"), b("a")).execute(&s), 0);
    }

    #[test]
    fn hlen_count() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2"))]).execute(&s);
        assert_eq!(HLen::new(b("k")).execute(&s), 2);
        assert_eq!(HLen::new(b("z")).execute(&s), 0);
    }

    #[test]
    fn hkeys_all() {
        let s = st();
        HSet::new(b("k"), vec![(b("x"), b("1")), (b("y"), b("2"))]).execute(&s);
        let k = HKeys::new(b("k")).execute(&s);
        assert_eq!(k.len(), 2);
        assert!(k.contains(&b("x")));
        assert!(k.contains(&b("y")));
        assert!(HKeys::new(b("z")).execute(&s).is_empty());
    }

    #[test]
    fn hvals_all() {
        let s = st();
        HSet::new(b("k"), vec![(b("x"), b("10")), (b("y"), b("20"))]).execute(&s);
        let v = HVals::new(b("k")).execute(&s);
        assert_eq!(v.len(), 2);
        assert!(v.contains(&b("10")));
        assert!(v.contains(&b("20")));
        assert!(HVals::new(b("z")).execute(&s).is_empty());
    }

    #[test]
    fn hincrby_new() {
        let s = st();
        assert_eq!(HIncrBy::new(b("k"), b("c"), 5).execute(&s), Ok(5));
    }

    #[test]
    fn hincrby_existing() {
        let s = st();
        HSet::new(b("k"), vec![(b("c"), b("10"))]).execute(&s);
        assert_eq!(HIncrBy::new(b("k"), b("c"), 3).execute(&s), Ok(13));
    }

    #[test]
    fn hincrby_negative() {
        let s = st();
        HSet::new(b("k"), vec![(b("c"), b("10"))]).execute(&s);
        assert_eq!(HIncrBy::new(b("k"), b("c"), -3).execute(&s), Ok(7));
    }

    #[test]
    fn hincrby_non_numeric() {
        let s = st();
        HSet::new(b("k"), vec![(b("f"), b("abc"))]).execute(&s);
        assert_eq!(HIncrBy::new(b("k"), b("f"), 5).execute(&s), Ok(5));
    }

    #[test]
    fn hincrbyfloat_new() {
        let s = st();
        let r = HIncrByFloat::new(b("k"), b("t"), 1.5).execute(&s).unwrap();
        assert_eq!(r.as_ref(), b"1.5");
    }

    #[test]
    fn hincrbyfloat_existing() {
        let s = st();
        HSet::new(b("k"), vec![(b("t"), b("2.5"))]).execute(&s);
        let r = HIncrByFloat::new(b("k"), b("t"), 1.0).execute(&s).unwrap();
        assert_eq!(r.as_ref(), b"3.5");
    }

    #[test]
    fn hincrbyfloat_negative() {
        let s = st();
        HSet::new(b("k"), vec![(b("t"), b("5.0"))]).execute(&s);
        let r = HIncrByFloat::new(b("k"), b("t"), -2.5).execute(&s).unwrap();
        assert_eq!(r.as_ref(), b"2.5");
    }

    #[test]
    fn hscan_full() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3")), (b("d"), b("4"))]).execute(&s);
        let mut all = Vec::new();
        let mut cur = 0;
        loop {
            let (next, pairs) = HScan::new(b("k"), cur, None, 2).execute(&s);
            all.extend(pairs);
            if next == 0 { break; }
            cur = next;
        }
        assert_eq!(all.len(), 8);
    }

    #[test]
    fn hscan_empty() {
        let s = st();
        let (c, p) = HScan::new(b("z"), 0, None, 10).execute(&s);
        assert_eq!(c, 0);
        assert!(p.is_empty());
    }

    #[test]
    fn hscan_count() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))]).execute(&s);
        let (next, pairs) = HScan::new(b("k"), 0, None, 1).execute(&s);
        assert_eq!(pairs.len(), 2);
        assert!(next > 0);
    }

    #[test]
    fn hrandfield_positive() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))]).execute(&s);
        let r = HRandField::new(b("k"), 2, false).execute(&s);
        assert_eq!(r.len(), 2);
        for f in &r {
            assert!(f.as_ref() == b"a" || f.as_ref() == b"b" || f.as_ref() == b"c");
        }
    }

    #[test]
    fn hrandfield_withvalues() {
        let s = st();
        HSet::new(b("k"), vec![(b("x"), b("10"))]).execute(&s);
        let r = HRandField::new(b("k"), 1, true).execute(&s);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].as_ref(), b"x");
        assert_eq!(r[1].as_ref(), b"10");
    }

    #[test]
    fn hrandfield_negative() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2"))]).execute(&s);
        let r = HRandField::new(b("k"), -5, false).execute(&s);
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn hrandfield_empty() {
        let s = st();
        assert!(HRandField::new(b("z"), 1, false).execute(&s).is_empty());
    }

    #[test]
    fn hrandfield_exceeds() {
        let s = st();
        HSet::new(b("k"), vec![(b("a"), b("1")), (b("b"), b("2"))]).execute(&s);
        let r = HRandField::new(b("k"), 10, false).execute(&s);
        assert_eq!(r.len(), 2);
    }
}
