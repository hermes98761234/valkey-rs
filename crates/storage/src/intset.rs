//! IntSet — compact sorted integer set encoding.
//!
//! Stores a set of integers in the smallest encoding that fits:
//! - int16: 2 bytes per value (range -32768..32767)
//! - int32: 4 bytes per value (range -2147483648..2147483647)
//! - int64: 8 bytes per value (full i64 range)
//!
//! Layout: raw values concatenated, encoding tracked in the struct.

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntSet {
    data: Vec<u8>,
    length: usize,
    encoding: IntSetEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntSetEncoding {
    Int16 = 2,
    Int32 = 4,
    Int64 = 8,
}

impl IntSet {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            length: 0,
            encoding: IntSetEncoding::Int16,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn encoding(&self) -> &'static str {
        match self.encoding {
            IntSetEncoding::Int16 => "int16",
            IntSetEncoding::Int32 => "int32",
            IntSetEncoding::Int64 => "int64",
        }
    }

    /// Returns the minimum encoding needed to hold the value.
    fn encoding_for(val: i64) -> IntSetEncoding {
        if val >= i16::MIN as i64 && val <= i16::MAX as i64 {
            IntSetEncoding::Int16
        } else if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
            IntSetEncoding::Int32
        } else {
            IntSetEncoding::Int64
        }
    }

    /// Upgrade the encoding to at least `min_enc`.
    fn upgrade_to(&mut self, min_enc: IntSetEncoding) {
        if min_enc as u16 <= self.encoding as u16 {
            return;
        }
        // Collect existing values
        let values: Vec<i64> = self.iter().collect();
        let new_enc = min_enc;
        self.encoding = new_enc;
        self.data.clear();
        self.length = 0;
        // Re-encode all values with new encoding
        for v in values {
            self.push_raw(v);
            self.length += 1;
        }
    }

    fn push_raw(&mut self, val: i64) {
        match self.encoding {
            IntSetEncoding::Int16 => {
                self.data.extend_from_slice(&(val as i16).to_le_bytes());
            }
            IntSetEncoding::Int32 => {
                self.data.extend_from_slice(&(val as i32).to_le_bytes());
            }
            IntSetEncoding::Int64 => {
                self.data.extend_from_slice(&val.to_le_bytes());
            }
        }
    }

    pub fn add(&mut self, val: i64) -> bool {
        if self.contains(val) {
            return false;
        }
        let needed = Self::encoding_for(val);
        if needed as u16 > self.encoding as u16 {
            self.upgrade_to(needed);
        }
        // Insert in sorted position
        let pos = self.iter().position(|v| v > val).unwrap_or(self.length);
        let width = self.encoding as usize;
        let byte_pos = pos * width;
        let bytes_to_insert = match self.encoding {
            IntSetEncoding::Int16 => (val as i16).to_le_bytes().to_vec(),
            IntSetEncoding::Int32 => (val as i32).to_le_bytes().to_vec(),
            IntSetEncoding::Int64 => val.to_le_bytes().to_vec(),
        };
        for (i, &b) in bytes_to_insert.iter().enumerate() {
            self.data.insert(byte_pos + i, b);
        }
        self.length += 1;
        true
    }

    pub fn remove(&mut self, val: i64) -> bool {
        if let Some(pos) = self.iter().position(|v| v == val) {
            let width = self.encoding as usize;
            let byte_pos = pos * width;
            for _ in 0..width {
                self.data.remove(byte_pos);
            }
            self.length -= 1;
            true
        } else {
            false
        }
    }

    pub fn contains(&self, val: i64) -> bool {
        // Binary search since values are sorted
        if self.length == 0 {
            return false;
        }
        let mut lo = 0usize;
        let mut hi = self.length;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let mid_val = self.get_value(mid);
            if mid_val < val {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo < self.length && self.get_value(lo) == val
    }

    fn get_value(&self, index: usize) -> i64 {
        let width = self.encoding as usize;
        let offset = index * width;
        match self.encoding {
            IntSetEncoding::Int16 => {
                i16::from_le_bytes([self.data[offset], self.data[offset + 1]]) as i64
            }
            IntSetEncoding::Int32 => {
                i32::from_le_bytes([
                    self.data[offset],
                    self.data[offset + 1],
                    self.data[offset + 2],
                    self.data[offset + 3],
                ]) as i64
            }
            IntSetEncoding::Int64 => i64::from_le_bytes([
                self.data[offset],
                self.data[offset + 1],
                self.data[offset + 2],
                self.data[offset + 3],
                self.data[offset + 4],
                self.data[offset + 5],
                self.data[offset + 6],
                self.data[offset + 7],
            ]),
        }
    }

    pub fn iter(&self) -> IntSetIter<'_> {
        IntSetIter {
            intset: self,
            pos: 0,
        }
    }

    pub fn to_vec(&self) -> Vec<i64> {
        self.iter().collect()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Create from a sorted slice of i64 values (no duplicates).
    pub fn from_sorted_slice(values: &[i64]) -> Self {
        let mut iset = Self::new();
        if values.is_empty() {
            return iset;
        }
        // Determine encoding
        let mut max_enc = IntSetEncoding::Int16;
        for &v in values {
            let enc = Self::encoding_for(v);
            if enc as u16 > max_enc as u16 {
                max_enc = enc;
            }
        }
        iset.encoding = max_enc;
        for &v in values {
            iset.push_raw(v);
        }
        iset.length = values.len();
        iset
    }
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

pub struct IntSetIter<'a> {
    intset: &'a IntSet,
    pos: usize,
}

impl<'a> Iterator for IntSetIter<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.intset.length {
            return None;
        }
        let val = self.intset.get_value(self.pos);
        self.pos += 1;
        Some(val)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.intset.length - self.pos;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for IntSetIter<'_> {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_intset() {
        let is = IntSet::new();
        assert_eq!(is.len(), 0);
        assert!(is.is_empty());
        assert!(!is.contains(1));
    }

    #[test]
    fn add_and_contains() {
        let mut is = IntSet::new();
        assert!(is.add(1));
        assert!(is.add(5));
        assert!(is.add(3));
        assert!(is.contains(1));
        assert!(is.contains(3));
        assert!(is.contains(5));
        assert!(!is.contains(2));
        assert!(!is.contains(4));
        assert_eq!(is.len(), 3);
    }

    #[test]
    fn add_duplicate() {
        let mut is = IntSet::new();
        assert!(is.add(1));
        assert!(!is.add(1));
        assert_eq!(is.len(), 1);
    }

    #[test]
    fn sorted_order() {
        let mut is = IntSet::new();
        is.add(5);
        is.add(1);
        is.add(3);
        is.add(2);
        is.add(4);
        let v: Vec<_> = is.iter().collect();
        assert_eq!(v, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn remove() {
        let mut is = IntSet::new();
        is.add(1);
        is.add(2);
        is.add(3);
        assert!(is.remove(2));
        assert!(!is.contains(2));
        assert_eq!(is.len(), 2);
        let v: Vec<_> = is.iter().collect();
        assert_eq!(v, vec![1, 3]);
    }

    #[test]
    fn remove_nonexistent() {
        let mut is = IntSet::new();
        is.add(1);
        assert!(!is.remove(99));
        assert_eq!(is.len(), 1);
    }

    #[test]
    fn encoding_upgrade_int16_to_int32() {
        let mut is = IntSet::new();
        is.add(100);
        assert_eq!(is.encoding(), "int16");
        is.add(50000); // exceeds i16::MAX
        assert_eq!(is.encoding(), "int32");
        assert!(is.contains(100));
        assert!(is.contains(50000));
    }

    #[test]
    fn encoding_upgrade_int32_to_int64() {
        let mut is = IntSet::new();
        is.add(100000); // needs int32
        assert_eq!(is.encoding(), "int32");
        is.add(i64::MAX); // needs int64
        assert_eq!(is.encoding(), "int64");
        assert!(is.contains(100000));
        assert!(is.contains(i64::MAX));
    }

    #[test]
    fn from_sorted_slice() {
        let values = vec![1i64, 2, 3, 10, 20];
        let is = IntSet::from_sorted_slice(&values);
        assert_eq!(is.len(), 5);
        let v: Vec<_> = is.iter().collect();
        assert_eq!(v, values);
    }

    #[test]
    fn from_sorted_slice_empty() {
        let is = IntSet::from_sorted_slice(&[]);
        assert_eq!(is.len(), 0);
    }

    #[test]
    fn negative_values() {
        let mut is = IntSet::new();
        is.add(-100);
        is.add(-50);
        is.add(0);
        is.add(50);
        assert!(is.contains(-100));
        assert!(is.contains(0));
        let v: Vec<_> = is.iter().collect();
        assert_eq!(v, vec![-100, -50, 0, 50]);
    }

    #[test]
    fn many_values() {
        let mut is = IntSet::new();
        for i in 0..1000 {
            is.add(i);
        }
        assert_eq!(is.len(), 1000);
        for i in 0..1000 {
            assert!(is.contains(i));
        }
    }

    #[test]
    fn iter_exact_size() {
        let mut is = IntSet::new();
        is.add(1);
        is.add(2);
        is.add(3);
        assert_eq!(is.iter().len(), 3);
    }
}
