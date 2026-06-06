//! Listpack — compact byte-array encoding for small aggregates.
//!
//! Each entry is serialized as:
//!   [prev_len (1–5 bytes)] [encoding + data] [back_len (1 byte)]

use bytes::Bytes;
use std::fmt;

// ---------------------------------------------------------------------------
// Varint helpers
// ---------------------------------------------------------------------------

fn encode_prevlen(buf: &mut Vec<u8>, len: usize) {
    if len < 128 {
        buf.push(len as u8);
    } else {
        buf.push(0xFE);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
    }
}

fn decode_prevlen(buf: &[u8], pos: usize) -> Option<(usize, usize)> {
    if pos >= buf.len() {
        return None;
    }
    let first = buf[pos];
    if first < 0xFE {
        Some((first as usize, 1))
    } else if first == 0xFE && pos + 5 <= buf.len() {
        let val = u32::from_le_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]);
        Some((val as usize, 5))
    } else {
        None
    }
}

fn encode_backlen(buf: &mut Vec<u8>, len: usize) {
    if len < 128 {
        buf.push(len as u8);
    } else {
        buf.push(0xFE);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
    }
}

// ---------------------------------------------------------------------------
// Integer encoding
// ---------------------------------------------------------------------------

fn encode_integer(buf: &mut Vec<u8>, val: i64) -> usize {
    let start = buf.len();
    if val >= 0 && val <= 63 {
        // 6-bit unsigned in 1 byte: 00xxxxxx
        buf.push(val as u8);
    } else if val >= -8192 && val <= 8191 {
        // 14-bit signed in 2 bytes: 01xxxxxx xxxxxxxx
        let v = val as i16;
        let encoded = v as u16; // Reinterpret bits
        buf.push(0x40 | ((encoded >> 8) as u8 & 0x3F));
        buf.push(encoded as u8);
    } else if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
        // 32-bit signed in 5 bytes: 10000000 + 4 bytes LE
        buf.push(0x80);
        buf.extend_from_slice(&(val as i32).to_le_bytes());
    } else {
        // 64-bit signed in 9 bytes: 10000001 + 8 bytes LE
        buf.push(0x81);
        buf.extend_from_slice(&val.to_le_bytes());
    }
    buf.len() - start
}

fn decode_integer(buf: &[u8], pos: usize) -> Option<(i64, usize)> {
    if pos >= buf.len() {
        return None;
    }
    let first = buf[pos];
    let top2 = first >> 6;
    match top2 {
        0b00 => {
            // 6-bit unsigned: 00xxxxxx
            Some((first as i64, 1))
        }
        0b01 => {
            // 14-bit signed in 2 bytes
            if pos + 2 > buf.len() {
                return None;
            }
            let hi = (first & 0x3F) as u16;
            let lo = buf[pos + 1] as u16;
            let raw = (hi << 8) | lo;
            // Sign-extend from 14 bits
            let val = if raw & 0x2000 != 0 {
                (raw as i64) - 16384
            } else {
                raw as i64
            };
            Some((val, 2))
        }
        0b10 => {
            if first == 0x80 && pos + 5 <= buf.len() {
                let val =
                    i32::from_le_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]);
                Some((val as i64, 5))
            } else if first == 0x81 && pos + 9 <= buf.len() {
                let val = i64::from_le_bytes([
                    buf[pos + 1],
                    buf[pos + 2],
                    buf[pos + 3],
                    buf[pos + 4],
                    buf[pos + 5],
                    buf[pos + 6],
                    buf[pos + 7],
                    buf[pos + 8],
                ]);
                Some((val, 9))
            } else {
                None
            }
        }
        0b11 => None, // String encoding, not integer
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// String encoding
// ---------------------------------------------------------------------------

fn encode_string(buf: &mut Vec<u8>, data: &[u8]) -> usize {
    let start = buf.len();
    let len = data.len();
    if len < 12 {
        buf.push(0xE0 | (len as u8 & 0x0F));
    } else if len < 256 {
        buf.push(0xF0);
        buf.push(len as u8);
    } else if len < 65536 {
        buf.push(0xF1);
        buf.extend_from_slice(&(len as u16).to_le_bytes());
    } else if len < (1u64 << 32) as usize {
        buf.push(0xF2);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
    } else {
        buf.push(0xF3);
        buf.extend_from_slice(&(len as u64).to_le_bytes());
    }
    buf.extend_from_slice(data);
    buf.len() - start
}

fn decode_string(buf: &[u8], pos: usize) -> Option<(Bytes, usize)> {
    if pos >= buf.len() {
        return None;
    }
    let first = buf[pos];
    if first >> 6 != 0b11 {
        return None;
    }
    let low4 = first & 0x0F;
    // Check extended-length prefixes FIRST (0xF0-0xF3 have low4 0-3 which would
    // incorrectly match the short-string branch if checked after).
    let (len, header_size) = if first == 0xF0 {
        if pos + 2 > buf.len() {
            return None;
        }
        (buf[pos + 1] as usize, 2)
    } else if first == 0xF1 {
        if pos + 3 > buf.len() {
            return None;
        }
        (
            u16::from_le_bytes([buf[pos + 1], buf[pos + 2]]) as usize,
            3,
        )
    } else if first == 0xF2 {
        if pos + 5 > buf.len() {
            return None;
        }
        (
            u32::from_le_bytes([
                buf[pos + 1],
                buf[pos + 2],
                buf[pos + 3],
                buf[pos + 4],
            ]) as usize,
            5,
        )
    } else if first == 0xF3 {
        if pos + 9 > buf.len() {
            return None;
        }
        (
            u64::from_le_bytes([
                buf[pos + 1],
                buf[pos + 2],
                buf[pos + 3],
                buf[pos + 4],
                buf[pos + 5],
                buf[pos + 6],
                buf[pos + 7],
                buf[pos + 8],
            ]) as usize,
            9,
        )
    } else if low4 < 12 {
        // Short string: 0xE0 | len  (len 0..11)
        (low4 as usize, 1)
    } else {
        return None;
    };
    let data_start = pos + header_size;
    let data_end = data_start + len;
    if data_end > buf.len() {
        return None;
    }
    Some((
        Bytes::copy_from_slice(&buf[data_start..data_end]),
        header_size + len,
    ))
}

// ---------------------------------------------------------------------------
// Entry type tag
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPackEntry {
    Integer(i64),
    String(Bytes),
}

impl ListPackEntry {
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            ListPackEntry::String(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            ListPackEntry::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        match self {
            ListPackEntry::String(b) => b.clone(),
            ListPackEntry::Integer(i) => Bytes::from(i.to_string()),
        }
    }
}

impl fmt::Display for ListPackEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ListPackEntry::Integer(i) => write!(f, "{}", i),
            ListPackEntry::String(s) => write!(f, "{}", String::from_utf8_lossy(s)),
        }
    }
}

// ---------------------------------------------------------------------------
// ListPack
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ListPack {
    data: Vec<u8>,
    count: usize,
}

impl ListPack {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_bytes(data: Vec<u8>) -> Self {
        let count = Self::count_entries(&data);
        Self { data, count }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn push_back(&mut self, entry: ListPackEntry) {
        let prev_len = self.data.len();
        let mut entry_buf = Vec::new();

        match &entry {
            ListPackEntry::Integer(i) => {
                encode_integer(&mut entry_buf, *i);
            }
            ListPackEntry::String(s) => {
                encode_string(&mut entry_buf, s);
            }
        }

        let data_len = entry_buf.len();
        // Total entry = prevlen + data + backlen
        // backlen is 1 byte if total < 128, else 5 bytes
        let total_with_1byte_backlen = Self::prevlen_size(prev_len) + data_len + 1;
        let backlen_size = if total_with_1byte_backlen < 128 { 1 } else { 5 };
        let total_entry_len = Self::prevlen_size(prev_len) + data_len + backlen_size;

        encode_prevlen(&mut self.data, prev_len);
        self.data.extend_from_slice(&entry_buf);
        encode_backlen(&mut self.data, total_entry_len);

        self.count += 1;
    }

    pub fn push_front(&mut self, entry: ListPackEntry) {
        let mut entries = self.iter().collect::<Vec<_>>();
        entries.insert(0, entry);
        *self = Self::from_entries(&entries);
    }

    pub fn insert(&mut self, index: usize, entry: ListPackEntry) {
        let mut entries = self.iter().collect::<Vec<_>>();
        if index > entries.len() {
            entries.push(entry);
        } else {
            entries.insert(index, entry);
        }
        *self = Self::from_entries(&entries);
    }

    pub fn remove(&mut self, index: usize) -> Option<ListPackEntry> {
        let mut entries = self.iter().collect::<Vec<_>>();
        if index < entries.len() {
            let removed = entries.remove(index);
            *self = Self::from_entries(&entries);
            Some(removed)
        } else {
            None
        }
    }

    pub fn get(&self, index: usize) -> Option<ListPackEntry> {
        self.iter().nth(index)
    }

    pub fn iter(&self) -> ListPackIter<'_> {
        ListPackIter {
            data: &self.data,
            pos: 0,
            remaining: self.count,
        }
    }

    pub fn to_vec(&self) -> Vec<Bytes> {
        self.iter().map(|e| e.to_bytes()).collect()
    }

    pub fn from_entries(entries: &[ListPackEntry]) -> Self {
        let mut lp = Self::new();
        for entry in entries {
            lp.push_back(entry.clone());
        }
        lp
    }

    pub fn from_vec(items: Vec<Bytes>) -> Self {
        let entries: Vec<ListPackEntry> = items.into_iter().map(ListPackEntry::String).collect();
        Self::from_entries(&entries)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn push_back_bytes(&mut self, val: Bytes) {
        self.push_back(ListPackEntry::String(val));
    }

    pub fn push_back_int(&mut self, val: i64) {
        self.push_back(ListPackEntry::Integer(val));
    }

    fn prevlen_size(len: usize) -> usize {
        if len < 128 { 1 } else { 5 }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn count_entries(data: &[u8]) -> usize {
        let mut pos = 0;
        let mut count = 0;
        while pos < data.len() {
            let (_, prevlen_size) = match decode_prevlen(data, pos) {
                Some(v) => v,
                None => break,
            };
            let entry_start = pos;
            let data_start = pos + prevlen_size;

            if data_start >= data.len() {
                break;
            }

            let first = data[data_start];
            let top2 = first >> 6;

            let data_len = match top2 {
                0b00 => 1,
                0b01 => 2,
                0b10 => {
                    if first == 0x80 {
                        5
                    } else if first == 0x81 {
                        9
                    } else {
                        break;
                    }
                }
                0b11 => {
                    let low4 = first & 0x0F;
                    // Check extended-length prefixes FIRST (0xF0-0xF3 have low4 0-3 which would
                    // incorrectly match the short-string branch if checked after).
                    let (str_len, header) = if first == 0xF0 {
                        if data_start + 2 > data.len() {
                            break;
                        }
                        (data[data_start + 1] as usize, 2)
                    } else if first == 0xF1 {
                        if data_start + 3 > data.len() {
                            break;
                        }
                        (
                            u16::from_le_bytes([data[data_start + 1], data[data_start + 2]])
                                as usize,
                            3,
                        )
                    } else if first == 0xF2 {
                        if data_start + 5 > data.len() {
                            break;
                        }
                        (
                            u32::from_le_bytes([
                                data[data_start + 1],
                                data[data_start + 2],
                                data[data_start + 3],
                                data[data_start + 4],
                            ]) as usize,
                            5,
                        )
                    } else if first == 0xF3 {
                        if data_start + 9 > data.len() {
                            break;
                        }
                        (
                            u64::from_le_bytes([
                                data[data_start + 1],
                                data[data_start + 2],
                                data[data_start + 3],
                                data[data_start + 4],
                                data[data_start + 5],
                                data[data_start + 6],
                                data[data_start + 7],
                                data[data_start + 8],
                            ]) as usize,
                            9,
                        )
                    } else if low4 < 12 {
                        (low4 as usize, 1)
                    } else {
                        break;
                    };
                    header + str_len
                }
                _ => break,
            };
            let entry_total = prevlen_size + data_len;
            // Read backlen (1 or 5 bytes) at the end
            let backlen_pos = entry_start + entry_total;
            if backlen_pos >= data.len() {
                break;
            }
            let backlen_val = if data[backlen_pos] < 0xFE {
                data[backlen_pos] as usize
            } else if backlen_pos + 5 <= data.len() {
                u32::from_le_bytes([
                    data[backlen_pos + 1],
                    data[backlen_pos + 2],
                    data[backlen_pos + 3],
                    data[backlen_pos + 4],
                ]) as usize
            } else {
                break;
            };
            pos = backlen_pos + ListPack::prevlen_size(backlen_val);
            count += 1;
        }
        count
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

pub struct ListPackIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ListPackIter<'a> {
    type Item = ListPackEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.pos >= self.data.len() {
            return None;
        }

        let (_, prevlen_size) = decode_prevlen(self.data, self.pos)?;
        let data_start = self.pos + prevlen_size;

        if data_start >= self.data.len() {
            return None;
        }

        let first = self.data[data_start];
        let top2 = first >> 6;

        let (entry, data_len) = match top2 {
            0b00 => {
                let val = decode_integer(self.data, data_start)?;
                (ListPackEntry::Integer(val.0), val.1)
            }
            0b01 => {
                let val = decode_integer(self.data, data_start)?;
                (ListPackEntry::Integer(val.0), val.1)
            }
            0b10 => {
                let val = decode_integer(self.data, data_start)?;
                (ListPackEntry::Integer(val.0), val.1)
            }
            0b11 => {
                let (bytes, consumed) = decode_string(self.data, data_start)?;
                (ListPackEntry::String(bytes), consumed)
            }
            _ => unreachable!(),
        };

        let backlen_pos = data_start + data_len;
        if backlen_pos >= self.data.len() {
            return None;
        }
        let backlen_val = if self.data[backlen_pos] < 0xFE {
            self.data[backlen_pos] as usize
        } else if backlen_pos + 5 <= self.data.len() {
            u32::from_le_bytes([
                self.data[backlen_pos + 1],
                self.data[backlen_pos + 2],
                self.data[backlen_pos + 3],
                self.data[backlen_pos + 4],
            ]) as usize
        } else {
            return None;
        };
        self.pos = backlen_pos + ListPack::prevlen_size(backlen_val);
        self.remaining -= 1;

        Some(entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for ListPackIter<'_> {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_listpack() {
        let lp = ListPack::new();
        assert_eq!(lp.len(), 0);
        assert!(lp.is_empty());
        assert_eq!(lp.iter().count(), 0);
    }

    #[test]
    fn push_back_strings() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("hello"));
        lp.push_back_bytes(Bytes::from("world"));
        assert_eq!(lp.len(), 2);
        assert_eq!(
            lp.get(0).unwrap().as_bytes().unwrap(),
            &Bytes::from("hello")
        );
        assert_eq!(
            lp.get(1).unwrap().as_bytes().unwrap(),
            &Bytes::from("world")
        );
    }

    #[test]
    fn push_back_integers() {
        let mut lp = ListPack::new();
        lp.push_back_int(42);
        lp.push_back_int(-1);
        lp.push_back_int(0);
        assert_eq!(lp.len(), 3);
        assert_eq!(lp.get(0).unwrap().as_integer().unwrap(), 42);
        assert_eq!(lp.get(1).unwrap().as_integer().unwrap(), -1);
        assert_eq!(lp.get(2).unwrap().as_integer().unwrap(), 0);
    }

    #[test]
    fn push_back_mixed() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("foo"));
        lp.push_back_int(100);
        lp.push_back_bytes(Bytes::from("bar"));
        assert_eq!(lp.len(), 3);
        assert_eq!(lp.get(0).unwrap().as_bytes().unwrap(), &Bytes::from("foo"));
        assert_eq!(lp.get(1).unwrap().as_integer().unwrap(), 100);
        assert_eq!(lp.get(2).unwrap().as_bytes().unwrap(), &Bytes::from("bar"));
    }

    #[test]
    fn push_front() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("b"));
        lp.push_back_bytes(Bytes::from("c"));
        lp.push_front(ListPackEntry::String(Bytes::from("a")));
        assert_eq!(lp.len(), 3);
        let v: Vec<_> = lp.iter().map(|e| e.to_bytes()).collect();
        assert_eq!(
            v,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[test]
    fn insert() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("a"));
        lp.push_back_bytes(Bytes::from("c"));
        lp.insert(1, ListPackEntry::String(Bytes::from("b")));
        let v: Vec<_> = lp.iter().map(|e| e.to_bytes()).collect();
        assert_eq!(
            v,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[test]
    fn remove() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("a"));
        lp.push_back_bytes(Bytes::from("b"));
        lp.push_back_bytes(Bytes::from("c"));
        let removed = lp.remove(1).unwrap();
        assert_eq!(removed.as_bytes().unwrap(), &Bytes::from("b"));
        assert_eq!(lp.len(), 2);
        let v: Vec<_> = lp.iter().map(|e| e.to_bytes()).collect();
        assert_eq!(v, vec![Bytes::from("a"), Bytes::from("c")]);
    }

    #[test]
    fn remove_out_of_bounds() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("a"));
        assert!(lp.remove(5).is_none());
    }

    #[test]
    fn get_out_of_bounds() {
        let lp = ListPack::new();
        assert!(lp.get(0).is_none());
    }

    #[test]
    fn to_vec_roundtrip() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("x"));
        lp.push_back_bytes(Bytes::from("y"));
        lp.push_back_bytes(Bytes::from("z"));
        let v = lp.to_vec();
        assert_eq!(
            v,
            vec![Bytes::from("x"), Bytes::from("y"), Bytes::from("z")]
        );

        let lp2 = ListPack::from_vec(v);
        assert_eq!(lp2.len(), 3);
        assert_eq!(lp2.get(0).unwrap().as_bytes().unwrap(), &Bytes::from("x"));
    }

    #[test]
    fn from_entries() {
        let entries = vec![
            ListPackEntry::String(Bytes::from("one")),
            ListPackEntry::Integer(2),
            ListPackEntry::String(Bytes::from("three")),
        ];
        let lp = ListPack::from_entries(&entries);
        assert_eq!(lp.len(), 3);
        assert_eq!(lp.get(1).unwrap().as_integer().unwrap(), 2);
    }

    #[test]
    fn large_integer() {
        let mut lp = ListPack::new();
        lp.push_back_int(i64::MAX);
        lp.push_back_int(i64::MIN);
        assert_eq!(lp.get(0).unwrap().as_integer().unwrap(), i64::MAX);
        assert_eq!(lp.get(1).unwrap().as_integer().unwrap(), i64::MIN);
    }

    #[test]
    fn medium_integer() {
        let mut lp = ListPack::new();
        lp.push_back_int(1000);
        lp.push_back_int(-1000);
        assert_eq!(lp.get(0).unwrap().as_integer().unwrap(), 1000);
        assert_eq!(lp.get(1).unwrap().as_integer().unwrap(), -1000);
    }

    #[test]
    fn long_string() {
        let mut lp = ListPack::new();
        let long_str = "x".repeat(200);
        lp.push_back_bytes(Bytes::from(long_str.clone()));
        assert_eq!(lp.get(0).unwrap().as_bytes().unwrap().len(), 200);
        assert_eq!(
            lp.get(0).unwrap().as_bytes().unwrap(),
            &Bytes::from(long_str)
        );
    }

    #[test]
    fn many_entries() {
        let mut lp = ListPack::new();
        for i in 0..200 {
            lp.push_back_int(i);
        }
        assert_eq!(lp.len(), 200);
        for i in 0..200 {
            assert_eq!(lp.get(i).unwrap().as_integer().unwrap(), i as i64);
        }
    }

    #[test]
    fn iter_exact_size() {
        let mut lp = ListPack::new();
        lp.push_back_int(1);
        lp.push_back_int(2);
        lp.push_back_int(3);
        let it = lp.iter();
        assert_eq!(it.len(), 3);
    }

    #[test]
    fn as_bytes_roundtrip() {
        let mut lp = ListPack::new();
        lp.push_back_bytes(Bytes::from("test"));
        lp.push_back_int(42);
        let raw = lp.as_bytes().to_vec();
        let lp2 = ListPack::from_bytes(raw);
        assert_eq!(lp2.len(), 2);
        assert_eq!(
            lp2.get(0).unwrap().as_bytes().unwrap(),
            &Bytes::from("test")
        );
        assert_eq!(lp2.get(1).unwrap().as_integer().unwrap(), 42);
    }
}
