//! HyperLogLog commands (PFADD, PFCOUNT, PFMERGE).
//!
//! Values are stored as plain strings (like upstream) with a 16-byte
//! "HYLL"-magic header followed by 16384 one-byte registers. The register
//! layout is simpler than upstream's 6-bit packing, but the hash function
//! (MurmurHash64A, seed 0xadc83b19) and the estimator match, so cardinality
//! results are equivalent.

use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

const HLL_P: u32 = 14;
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
const HDR_LEN: usize = 16;

fn wrongtype() -> RespValue {
    RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

fn invalid_hll() -> RespValue {
    RespValue::Error("WRONGTYPE Key is not a valid HyperLogLog string value.".into())
}

pub fn handle(cmd: &[Bytes], store: &Arc<Store>) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR wrong number of arguments".into());
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    let args = &cmd[1..];
    match name.as_str() {
        "PFADD" => cmd_pfadd(args, store),
        "PFCOUNT" => cmd_pfcount(args, store),
        "PFMERGE" => cmd_pfmerge(args, store),
        _ => RespValue::Error(format!("ERR unknown command `{}`", name)),
    }
}

fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h: u64 = seed ^ (key.len() as u64).wrapping_mul(M);
    let chunks = key.chunks_exact(8);
    let rem = chunks.remainder();
    for c in chunks {
        let mut k = u64::from_le_bytes(c.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !rem.is_empty() {
        let mut k: u64 = 0;
        for (i, &b) in rem.iter().enumerate() {
            k |= (b as u64) << (8 * i);
        }
        h ^= k;
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Map an element to its register index and the rank (position of the first
/// set bit in the remaining hash bits, 1-based).
fn index_and_rank(ele: &[u8]) -> (usize, u8) {
    let h = murmur64a(ele, 0xadc83b19);
    let index = (h & (HLL_REGISTERS as u64 - 1)) as usize;
    let rest = (h >> HLL_P) | (1u64 << (64 - HLL_P));
    (index, rest.trailing_zeros() as u8 + 1)
}

fn new_hll() -> Vec<u8> {
    let mut v = vec![0u8; HDR_LEN + HLL_REGISTERS];
    v[..4].copy_from_slice(b"HYLL");
    v
}

fn is_hll(buf: &[u8]) -> bool {
    buf.len() == HDR_LEN + HLL_REGISTERS && &buf[..4] == b"HYLL"
}

/// Load the value for `key` as an HLL buffer. Ok(None) when missing.
fn load(store: &Arc<Store>, key: &Bytes) -> Result<Option<Vec<u8>>, RespValue> {
    match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => {
                if is_hll(s) {
                    Ok(Some(s.to_vec()))
                } else {
                    Err(invalid_hll())
                }
            }
            _ => Err(wrongtype()),
        },
        None => Ok(None),
    }
}

fn save(store: &Arc<Store>, key: &Bytes, buf: Vec<u8>) {
    let dm: &dashmap::DashMap<Bytes, Entry> = store;
    match dm.get_mut(key) {
        Some(mut entry) => entry.data = DataType::String(Bytes::from(buf)),
        None => {
            dm.insert(
                key.clone(),
                Entry::new(DataType::String(Bytes::from(buf)), None),
            );
        }
    }
}

fn estimate(regs: &[u8]) -> u64 {
    let m = HLL_REGISTERS as f64;
    let mut sum = 0f64;
    let mut zeros = 0usize;
    for &r in regs {
        sum += 1.0 / (1u64 << r) as f64;
        if r == 0 {
            zeros += 1;
        }
    }
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let e = alpha * m * m / sum;
    if e <= 2.5 * m && zeros > 0 {
        (m * (m / zeros as f64).ln()).round() as u64
    } else {
        e.round() as u64
    }
}

// ---------------------------------------------------------------------------
// PFADD key [element ...]
// ---------------------------------------------------------------------------
fn cmd_pfadd(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'pfadd' command".into());
    }
    let key = &args[0];
    let (mut buf, mut changed) = match load(store, key) {
        Ok(Some(b)) => (b, false),
        Ok(None) => (new_hll(), true),
        Err(e) => return e,
    };
    for ele in &args[1..] {
        let (idx, rank) = index_and_rank(ele);
        let reg = &mut buf[HDR_LEN + idx];
        if rank > *reg {
            *reg = rank;
            changed = true;
        }
    }
    if changed {
        save(store, key, buf);
    }
    RespValue::Integer(if changed { 1 } else { 0 })
}

// ---------------------------------------------------------------------------
// PFCOUNT key [key ...]
// ---------------------------------------------------------------------------
fn cmd_pfcount(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'pfcount' command".into());
    }
    let mut merged = vec![0u8; HLL_REGISTERS];
    let mut found = false;
    for key in args {
        match load(store, key) {
            Ok(Some(buf)) => {
                found = true;
                for (i, m) in merged.iter_mut().enumerate() {
                    let r = buf[HDR_LEN + i];
                    if r > *m {
                        *m = r;
                    }
                }
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }
    if !found {
        return RespValue::Integer(0);
    }
    RespValue::Integer(estimate(&merged) as i64)
}

// ---------------------------------------------------------------------------
// PFMERGE destkey [sourcekey ...]
// ---------------------------------------------------------------------------
fn cmd_pfmerge(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'pfmerge' command".into());
    }
    let dest = &args[0];
    let mut buf = match load(store, dest) {
        Ok(Some(b)) => b,
        Ok(None) => new_hll(),
        Err(e) => return e,
    };
    for key in &args[1..] {
        match load(store, key) {
            Ok(Some(src)) => {
                for i in 0..HLL_REGISTERS {
                    if src[HDR_LEN + i] > buf[HDR_LEN + i] {
                        buf[HDR_LEN + i] = src[HDR_LEN + i];
                    }
                }
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }
    save(store, dest, buf);
    RespValue::SimpleString("OK".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn t_pfadd_pfcount() {
        let s = Store::new();
        assert_eq!(
            handle(&[b("PFADD"), b("hll"), b("a"), b("b"), b("c")], &s),
            RespValue::Integer(1)
        );
        // Re-adding the same elements changes nothing
        assert_eq!(
            handle(&[b("PFADD"), b("hll"), b("a"), b("b"), b("c")], &s),
            RespValue::Integer(0)
        );
        assert_eq!(handle(&[b("PFCOUNT"), b("hll")], &s), RespValue::Integer(3));
        assert_eq!(
            handle(&[b("PFCOUNT"), b("missing")], &s),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn t_accuracy() {
        let s = Store::new();
        for i in 0..10000 {
            let e = format!("element-{}", i);
            handle(&[b("PFADD"), b("big"), Bytes::from(e)], &s);
        }
        let count = match handle(&[b("PFCOUNT"), b("big")], &s) {
            RespValue::Integer(n) => n,
            other => panic!("unexpected reply {:?}", other),
        };
        // HLL with 16k registers has ~0.81% standard error; allow 5%.
        assert!(
            (count - 10000).abs() < 500,
            "estimate {} too far off",
            count
        );
    }

    #[test]
    fn t_pfmerge() {
        let s = Store::new();
        for i in 0..1000 {
            handle(&[b("PFADD"), b("h1"), Bytes::from(format!("a{}", i))], &s);
            handle(&[b("PFADD"), b("h2"), Bytes::from(format!("b{}", i))], &s);
        }
        assert_eq!(
            handle(&[b("PFMERGE"), b("dst"), b("h1"), b("h2")], &s),
            RespValue::SimpleString("OK".into())
        );
        let count = match handle(&[b("PFCOUNT"), b("dst")], &s) {
            RespValue::Integer(n) => n,
            other => panic!("unexpected reply {:?}", other),
        };
        assert!((count - 2000).abs() < 150, "estimate {} too far off", count);
        // PFCOUNT with multiple keys unions on the fly
        let union = match handle(&[b("PFCOUNT"), b("h1"), b("h2")], &s) {
            RespValue::Integer(n) => n,
            other => panic!("unexpected reply {:?}", other),
        };
        assert_eq!(union, count);
    }

    #[test]
    fn t_wrongtype() {
        let s = Store::new();
        s.set(b("str"), DataType::String(b("not-an-hll")), None);
        assert!(matches!(
            handle(&[b("PFADD"), b("str"), b("x")], &s),
            RespValue::Error(_)
        ));
    }
}
