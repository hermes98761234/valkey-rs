use bytes::Bytes;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

fn wrongtype() -> RespValue {
    RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

fn not_int() -> RespValue {
    RespValue::Error("ERR value is not an integer or out of range".into())
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
        "SETBIT" => cmd_setbit(args, store),
        "GETBIT" => cmd_getbit(args, store),
        "BITCOUNT" => cmd_bitcount(args, store),
        "BITPOS" => cmd_bitpos(args, store),
        "BITOP" => cmd_bitop(args, store),
        "BITFIELD" => cmd_bitfield(args, store, false),
        "BITFIELD_RO" => cmd_bitfield(args, store, true),
        _ => RespValue::Error(format!("ERR unknown command `{}`", name)),
    }
}

/// Fetch the string value of a key. Ok(None) when the key is missing,
/// Err(reply) when it holds a non-string value.
fn get_string(store: &Arc<Store>, key: &Bytes) -> Result<Option<Bytes>, RespValue> {
    match store.get(key) {
        Some(entry) => match &entry.data {
            DataType::String(s) => Ok(Some(s.clone())),
            _ => Err(wrongtype()),
        },
        None => Ok(None),
    }
}

fn set_string(store: &Arc<Store>, key: &Bytes, buf: Vec<u8>) {
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

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

// ---------------------------------------------------------------------------
// SETBIT key offset value
// ---------------------------------------------------------------------------
fn cmd_setbit(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 3 {
        return RespValue::Error("ERR wrong number of arguments for 'setbit' command".into());
    }
    let offset: u64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return RespValue::Error("ERR bit offset is not an integer or out of range".into()),
    };
    if offset >= 4 * 1024 * 1024 * 1024 * 8 {
        return RespValue::Error("ERR bit offset is not an integer or out of range".into());
    }
    let bit = match std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
    {
        Some(0) => 0u8,
        Some(1) => 1u8,
        _ => return RespValue::Error("ERR bit is not an integer or out of range".into()),
    };
    let mut buf = match get_string(store, &args[0]) {
        Ok(Some(s)) => s.to_vec(),
        Ok(None) => Vec::new(),
        Err(e) => return e,
    };
    let byte = (offset / 8) as usize;
    let shift = 7 - (offset % 8) as u32;
    if buf.len() <= byte {
        buf.resize(byte + 1, 0);
    }
    let old = (buf[byte] >> shift) & 1;
    if bit == 1 {
        buf[byte] |= 1 << shift;
    } else {
        buf[byte] &= !(1 << shift);
    }
    set_string(store, &args[0], buf);
    RespValue::Integer(old as i64)
}

// ---------------------------------------------------------------------------
// GETBIT key offset
// ---------------------------------------------------------------------------
fn cmd_getbit(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'getbit' command".into());
    }
    let offset: u64 = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return RespValue::Error("ERR bit offset is not an integer or out of range".into()),
    };
    let buf = match get_string(store, &args[0]) {
        Ok(Some(s)) => s,
        Ok(None) => return RespValue::Integer(0),
        Err(e) => return e,
    };
    let byte = (offset / 8) as usize;
    if byte >= buf.len() {
        return RespValue::Integer(0);
    }
    let shift = 7 - (offset % 8) as u32;
    RespValue::Integer(((buf[byte] >> shift) & 1) as i64)
}

/// Resolve a possibly-negative index against a length, clamping into [0, len).
fn normalize_index(idx: i64, len: i64) -> i64 {
    let v = if idx < 0 { len + idx } else { idx };
    v.clamp(0, len.max(0))
}

/// Count set bits in buf within the inclusive bit range [start_bit, end_bit].
fn count_bits_range(buf: &[u8], start_bit: u64, end_bit: u64) -> i64 {
    let mut count = 0i64;
    let first_byte = (start_bit / 8) as usize;
    let last_byte = (end_bit / 8) as usize;
    for byte in first_byte..=last_byte.min(buf.len().saturating_sub(1)) {
        let mut b = buf[byte];
        if byte == first_byte {
            let skip = (start_bit % 8) as u32;
            b &= 0xffu8 >> skip;
        }
        if byte == last_byte {
            let keep = (end_bit % 8) as u32;
            b &= 0xffu8 << (7 - keep);
        }
        count += b.count_ones() as i64;
    }
    count
}

// ---------------------------------------------------------------------------
// BITCOUNT key [start end [BYTE|BIT]]
// ---------------------------------------------------------------------------
fn cmd_bitcount(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() || args.len() == 2 || args.len() > 4 {
        return RespValue::Error("ERR syntax error".into());
    }
    let buf = match get_string(store, &args[0]) {
        Ok(Some(s)) => s,
        Ok(None) => return RespValue::Integer(0),
        Err(e) => return e,
    };
    if args.len() == 1 {
        return RespValue::Integer(buf.iter().map(|b| b.count_ones() as i64).sum());
    }
    let (start, end) = match (parse_i64(&args[1]), parse_i64(&args[2])) {
        (Some(s), Some(e)) => (s, e),
        _ => return not_int(),
    };
    let bit_mode = if args.len() == 4 {
        match std::str::from_utf8(&args[3]).map(|s| s.to_ascii_uppercase()) {
            Ok(u) if u == "BIT" => true,
            Ok(u) if u == "BYTE" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        }
    } else {
        false
    };
    let total = if bit_mode {
        buf.len() as i64 * 8
    } else {
        buf.len() as i64
    };
    if total == 0 {
        return RespValue::Integer(0);
    }
    let mut s = if start < 0 { total + start } else { start };
    let mut e = if end < 0 { total + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= total {
        e = total - 1;
    }
    if e < 0 || s > e {
        return RespValue::Integer(0);
    }
    let (sb, eb) = if bit_mode {
        (s as u64, e as u64)
    } else {
        (s as u64 * 8, e as u64 * 8 + 7)
    };
    RespValue::Integer(count_bits_range(&buf, sb, eb))
}

// ---------------------------------------------------------------------------
// BITPOS key bit [start [end [BYTE|BIT]]]
// ---------------------------------------------------------------------------
fn cmd_bitpos(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 2 || args.len() > 5 {
        return RespValue::Error("ERR syntax error".into());
    }
    let bit = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
    {
        Some(0) => 0u8,
        Some(1) => 1u8,
        _ => return RespValue::Error("ERR The bit argument must be 1 or 0.".into()),
    };
    let buf = match get_string(store, &args[0]) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return RespValue::Integer(if bit == 0 { 0 } else { -1 });
        }
        Err(e) => return e,
    };
    let bit_mode = if args.len() == 5 {
        match std::str::from_utf8(&args[4]).map(|s| s.to_ascii_uppercase()) {
            Ok(u) if u == "BIT" => true,
            Ok(u) if u == "BYTE" => false,
            _ => return RespValue::Error("ERR syntax error".into()),
        }
    } else {
        false
    };
    let total = if bit_mode {
        buf.len() as i64 * 8
    } else {
        buf.len() as i64
    };
    let user_end = args.len() >= 4;
    let start = if args.len() >= 3 {
        match parse_i64(&args[2]) {
            Some(v) => v,
            None => return not_int(),
        }
    } else {
        0
    };
    let end = if user_end {
        match parse_i64(&args[3]) {
            Some(v) => v,
            None => return not_int(),
        }
    } else {
        total - 1
    };
    if total == 0 {
        return RespValue::Integer(if bit == 0 && !user_end { 0 } else { -1 });
    }
    let s = normalize_index(start, total).min(total - 1);
    let e = normalize_index(end, total).min(total - 1);
    if s > e {
        return RespValue::Integer(-1);
    }
    let (sb, eb) = if bit_mode {
        (s as u64, e as u64)
    } else {
        (s as u64 * 8, e as u64 * 8 + 7)
    };
    for pos in sb..=eb {
        let byte = (pos / 8) as usize;
        let shift = 7 - (pos % 8) as u32;
        if (buf[byte] >> shift) & 1 == bit {
            return RespValue::Integer(pos as i64);
        }
    }
    // Searching for 0 with no explicit end behaves as if the string had
    // trailing zero bits, so the answer is one past the last bit.
    if bit == 0 && !user_end {
        return RespValue::Integer(buf.len() as i64 * 8);
    }
    RespValue::Integer(-1)
}

// ---------------------------------------------------------------------------
// BITOP AND|OR|XOR|NOT destkey srckey [srckey ...]
// ---------------------------------------------------------------------------
fn cmd_bitop(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() < 3 {
        return RespValue::Error("ERR wrong number of arguments for 'bitop' command".into());
    }
    let op = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR syntax error".into()),
    };
    let dest = &args[1];
    let src_keys = &args[2..];
    if op == "NOT" && src_keys.len() != 1 {
        return RespValue::Error("ERR BITOP NOT must be called with a single source key.".into());
    }
    let mut sources: Vec<Bytes> = Vec::with_capacity(src_keys.len());
    for key in src_keys {
        match get_string(store, key) {
            Ok(Some(s)) => sources.push(s),
            Ok(None) => sources.push(Bytes::new()),
            Err(e) => return e,
        }
    }
    let max_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut result = vec![0u8; max_len];
    match op.as_str() {
        "AND" => {
            for i in 0..max_len {
                let mut v = 0xffu8;
                for s in &sources {
                    v &= s.get(i).copied().unwrap_or(0);
                }
                result[i] = v;
            }
        }
        "OR" => {
            for i in 0..max_len {
                let mut v = 0u8;
                for s in &sources {
                    v |= s.get(i).copied().unwrap_or(0);
                }
                result[i] = v;
            }
        }
        "XOR" => {
            for i in 0..max_len {
                let mut v = 0u8;
                for s in &sources {
                    v ^= s.get(i).copied().unwrap_or(0);
                }
                result[i] = v;
            }
        }
        "NOT" => {
            for i in 0..max_len {
                result[i] = !sources[0][i];
            }
        }
        _ => return RespValue::Error("ERR syntax error".into()),
    }
    let len = result.len() as i64;
    if result.is_empty() {
        store.del(dest);
    } else {
        set_string(store, dest, result);
    }
    RespValue::Integer(len)
}

// ---------------------------------------------------------------------------
// BITFIELD key [GET ty off] [SET ty off val] [INCRBY ty off incr] [OVERFLOW ..]
// ---------------------------------------------------------------------------
#[derive(Clone, Copy, PartialEq)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

struct FieldType {
    signed: bool,
    width: u32,
}

fn parse_field_type(b: &Bytes) -> Option<FieldType> {
    let s = std::str::from_utf8(b).ok()?;
    let (signed, rest) = match s.as_bytes().first()? {
        b'i' => (true, &s[1..]),
        b'u' => (false, &s[1..]),
        _ => return None,
    };
    let width: u32 = rest.parse().ok()?;
    if width == 0 || width > 64 || (!signed && width > 63) {
        return None;
    }
    Some(FieldType { signed, width })
}

fn parse_offset(b: &Bytes, width: u32) -> Option<u64> {
    let s = std::str::from_utf8(b).ok()?;
    if let Some(stripped) = s.strip_prefix('#') {
        let n: u64 = stripped.parse().ok()?;
        n.checked_mul(width as u64)
    } else {
        s.parse().ok()
    }
}

/// Read `width` bits starting at bit `off` (big-endian bit order). Bits past
/// the end of the buffer read as 0.
fn get_bits(buf: &[u8], off: u64, width: u32) -> u64 {
    let mut value = 0u64;
    for i in 0..width as u64 {
        let pos = off + i;
        let byte = (pos / 8) as usize;
        let bit = if byte < buf.len() {
            (buf[byte] >> (7 - (pos % 8) as u32)) & 1
        } else {
            0
        };
        value = (value << 1) | bit as u64;
    }
    value
}

fn set_bits(buf: &mut Vec<u8>, off: u64, width: u32, value: u64) {
    let needed = (off + width as u64).div_ceil(8) as usize;
    if buf.len() < needed {
        buf.resize(needed, 0);
    }
    for i in 0..width as u64 {
        let pos = off + i;
        let byte = (pos / 8) as usize;
        let shift = 7 - (pos % 8) as u32;
        let bit = ((value >> (width as u64 - 1 - i)) & 1) as u8;
        if bit == 1 {
            buf[byte] |= 1 << shift;
        } else {
            buf[byte] &= !(1 << shift);
        }
    }
}

/// Interpret raw register bits as a signed/unsigned value (returned as i128
/// so u63 max and i64 min both fit).
fn decode_value(raw: u64, ty: &FieldType) -> i128 {
    if ty.signed && ty.width < 64 && (raw >> (ty.width - 1)) & 1 == 1 {
        raw as i128 - (1i128 << ty.width)
    } else if ty.signed && ty.width == 64 {
        raw as i64 as i128
    } else {
        raw as i128
    }
}

fn type_bounds(ty: &FieldType) -> (i128, i128) {
    if ty.signed {
        let min = -(1i128 << (ty.width - 1));
        let max = (1i128 << (ty.width - 1)) - 1;
        (min, max)
    } else {
        (0, (1i128 << ty.width) - 1)
    }
}

/// Apply overflow policy to `val` for the given type. None means FAIL.
fn apply_overflow(val: i128, ty: &FieldType, ovf: Overflow) -> Option<i128> {
    let (min, max) = type_bounds(ty);
    if val >= min && val <= max {
        return Some(val);
    }
    match ovf {
        Overflow::Fail => None,
        Overflow::Sat => Some(if val < min { min } else { max }),
        Overflow::Wrap => {
            let span = 1i128 << ty.width;
            let wrapped = ((val - min).rem_euclid(span)) + min;
            Some(wrapped)
        }
    }
}

fn encode_value(val: i128, ty: &FieldType) -> u64 {
    let mask = if ty.width == 64 {
        u64::MAX
    } else {
        (1u64 << ty.width) - 1
    };
    (val as i64 as u64) & mask
}

fn cmd_bitfield(args: &[Bytes], store: &Arc<Store>, read_only: bool) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'bitfield' command".into());
    }
    let key = &args[0];
    enum Op {
        Get(FieldType, u64),
        Set(FieldType, u64, i128),
        IncrBy(FieldType, u64, i128),
        Overflow(Overflow),
    }
    let mut ops = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let word = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR syntax error".into()),
        };
        match word.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                let ty = match parse_field_type(&args[i + 1]) {
                    Some(t) => t,
                    None => return RespValue::Error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".into()),
                };
                let off = match parse_offset(&args[i + 2], ty.width) {
                    Some(o) => o,
                    None => {
                        return RespValue::Error(
                            "ERR bit offset is not an integer or out of range".into(),
                        )
                    }
                };
                ops.push(Op::Get(ty, off));
                i += 3;
            }
            "SET" | "INCRBY" => {
                if read_only {
                    return RespValue::Error(
                        "ERR BITFIELD_RO only supports the GET subcommand".into(),
                    );
                }
                if i + 3 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                let ty = match parse_field_type(&args[i + 1]) {
                    Some(t) => t,
                    None => return RespValue::Error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".into()),
                };
                let off = match parse_offset(&args[i + 2], ty.width) {
                    Some(o) => o,
                    None => {
                        return RespValue::Error(
                            "ERR bit offset is not an integer or out of range".into(),
                        )
                    }
                };
                let val: i128 = match std::str::from_utf8(&args[i + 3])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(v) => v as i128,
                    None => return not_int(),
                };
                if word == "SET" {
                    ops.push(Op::Set(ty, off, val));
                } else {
                    ops.push(Op::IncrBy(ty, off, val));
                }
                i += 4;
            }
            "OVERFLOW" => {
                if i + 1 >= args.len() {
                    return RespValue::Error("ERR syntax error".into());
                }
                let mode = match std::str::from_utf8(&args[i + 1]).map(|s| s.to_ascii_uppercase()) {
                    Ok(m) if m == "WRAP" => Overflow::Wrap,
                    Ok(m) if m == "SAT" => Overflow::Sat,
                    Ok(m) if m == "FAIL" => Overflow::Fail,
                    _ => return RespValue::Error("ERR Invalid OVERFLOW type specified".into()),
                };
                ops.push(Op::Overflow(mode));
                i += 2;
            }
            _ => return RespValue::Error("ERR syntax error".into()),
        }
    }
    let mut buf = match get_string(store, key) {
        Ok(Some(s)) => s.to_vec(),
        Ok(None) => Vec::new(),
        Err(e) => return e,
    };
    let mut overflow = Overflow::Wrap;
    let mut results = Vec::new();
    let mut dirty = false;
    for op in ops {
        match op {
            Op::Overflow(m) => overflow = m,
            Op::Get(ty, off) => {
                let raw = get_bits(&buf, off, ty.width);
                results.push(RespValue::Integer(decode_value(raw, &ty) as i64));
            }
            Op::Set(ty, off, val) => match apply_overflow(val, &ty, overflow) {
                Some(v) => {
                    let old = decode_value(get_bits(&buf, off, ty.width), &ty);
                    set_bits(&mut buf, off, ty.width, encode_value(v, &ty));
                    dirty = true;
                    results.push(RespValue::Integer(old as i64));
                }
                None => results.push(RespValue::BulkString(None)),
            },
            Op::IncrBy(ty, off, incr) => {
                let cur = decode_value(get_bits(&buf, off, ty.width), &ty);
                match apply_overflow(cur + incr, &ty, overflow) {
                    Some(v) => {
                        set_bits(&mut buf, off, ty.width, encode_value(v, &ty));
                        dirty = true;
                        results.push(RespValue::Integer(v as i64));
                    }
                    None => results.push(RespValue::BulkString(None)),
                }
            }
        }
    }
    if dirty {
        set_string(store, key, buf);
    }
    RespValue::array(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn store() -> Arc<Store> {
        Store::new()
    }

    #[test]
    fn t_setbit_getbit() {
        let s = store();
        assert_eq!(
            handle(&[b("SETBIT"), b("k"), b("7"), b("1")], &s),
            RespValue::Integer(0)
        );
        assert_eq!(
            handle(&[b("GETBIT"), b("k"), b("7")], &s),
            RespValue::Integer(1)
        );
        assert_eq!(
            handle(&[b("GETBIT"), b("k"), b("6")], &s),
            RespValue::Integer(0)
        );
        assert_eq!(
            handle(&[b("SETBIT"), b("k"), b("7"), b("0")], &s),
            RespValue::Integer(1)
        );
        // "\x01" was written then cleared -> value is "\x00"
        assert_eq!(
            handle(&[b("GETBIT"), b("k"), b("100")], &s),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn t_bitcount() {
        let s = store();
        s.set(b("k"), DataType::String(b("foobar")), None);
        assert_eq!(handle(&[b("BITCOUNT"), b("k")], &s), RespValue::Integer(26));
        assert_eq!(
            handle(&[b("BITCOUNT"), b("k"), b("0"), b("0")], &s),
            RespValue::Integer(4)
        );
        assert_eq!(
            handle(&[b("BITCOUNT"), b("k"), b("1"), b("1")], &s),
            RespValue::Integer(6)
        );
        assert_eq!(
            handle(&[b("BITCOUNT"), b("k"), b("5"), b("30"), b("BIT")], &s),
            RespValue::Integer(17)
        );
        assert_eq!(
            handle(&[b("BITCOUNT"), b("missing")], &s),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn t_bitpos() {
        let s = store();
        s.set(
            b("k"),
            DataType::String(Bytes::from(vec![0xffu8, 0xf0, 0x00])),
            None,
        );
        assert_eq!(
            handle(&[b("BITPOS"), b("k"), b("0")], &s),
            RespValue::Integer(12)
        );
        assert_eq!(
            handle(&[b("BITPOS"), b("k"), b("1")], &s),
            RespValue::Integer(0)
        );
        assert_eq!(
            handle(&[b("BITPOS"), b("k"), b("1"), b("2")], &s),
            RespValue::Integer(-1)
        );
        assert_eq!(
            handle(&[b("BITPOS"), b("missing"), b("1")], &s),
            RespValue::Integer(-1)
        );
        assert_eq!(
            handle(&[b("BITPOS"), b("missing"), b("0")], &s),
            RespValue::Integer(0)
        );
        // All ones, no end given: first 0 is one past the end.
        let s2 = store();
        s2.set(b("k"), DataType::String(Bytes::from(vec![0xffu8; 2])), None);
        assert_eq!(
            handle(&[b("BITPOS"), b("k"), b("0")], &s2),
            RespValue::Integer(16)
        );
        assert_eq!(
            handle(&[b("BITPOS"), b("k"), b("0"), b("0"), b("-1")], &s2),
            RespValue::Integer(-1)
        );
    }

    #[test]
    fn t_bitop() {
        let s = store();
        s.set(b("a"), DataType::String(b("abc")), None);
        s.set(b("b"), DataType::String(b("abd")), None);
        assert_eq!(
            handle(&[b("BITOP"), b("AND"), b("dest"), b("a"), b("b")], &s),
            RespValue::Integer(3)
        );
        assert_eq!(
            handle(&[b("BITOP"), b("XOR"), b("dest"), b("a"), b("b")], &s),
            RespValue::Integer(3)
        );
        match s.get(&b("dest")) {
            Some(e) => match &e.data {
                DataType::String(v) => assert_eq!(&v[..], &[0u8, 0, b'c' ^ b'd']),
                _ => panic!("wrong type"),
            },
            None => panic!("dest missing"),
        }
        assert_eq!(
            handle(&[b("BITOP"), b("NOT"), b("dest"), b("a")], &s),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn t_bitfield() {
        let s = store();
        // SET then GET round-trip, unsigned
        let r = handle(
            &[
                b("BITFIELD"),
                b("k"),
                b("SET"),
                b("u8"),
                b("0"),
                b("255"),
                b("GET"),
                b("u8"),
                b("0"),
            ],
            &s,
        );
        assert_eq!(
            r,
            RespValue::array(vec![RespValue::Integer(0), RespValue::Integer(255)])
        );
        // signed wrap: i8 127 + 1 -> -128
        let r = handle(
            &[
                b("BITFIELD"),
                b("k2"),
                b("SET"),
                b("i8"),
                b("0"),
                b("127"),
                b("INCRBY"),
                b("i8"),
                b("0"),
                b("1"),
            ],
            &s,
        );
        assert_eq!(
            r,
            RespValue::array(vec![RespValue::Integer(0), RespValue::Integer(-128)])
        );
        // SAT overflow
        let r = handle(
            &[
                b("BITFIELD"),
                b("k3"),
                b("OVERFLOW"),
                b("SAT"),
                b("SET"),
                b("u8"),
                b("0"),
                b("250"),
                b("INCRBY"),
                b("u8"),
                b("0"),
                b("100"),
            ],
            &s,
        );
        assert_eq!(
            r,
            RespValue::array(vec![RespValue::Integer(0), RespValue::Integer(255)])
        );
        // FAIL overflow -> nil
        let r = handle(
            &[
                b("BITFIELD"),
                b("k4"),
                b("OVERFLOW"),
                b("FAIL"),
                b("SET"),
                b("u8"),
                b("0"),
                b("250"),
                b("INCRBY"),
                b("u8"),
                b("0"),
                b("100"),
            ],
            &s,
        );
        assert_eq!(
            r,
            RespValue::array(vec![RespValue::Integer(0), RespValue::BulkString(None)])
        );
        // #-style offset
        let r = handle(
            &[
                b("BITFIELD"),
                b("k5"),
                b("SET"),
                b("u8"),
                b("#1"),
                b("7"),
                b("GET"),
                b("u8"),
                b("8"),
            ],
            &s,
        );
        assert_eq!(
            r,
            RespValue::array(vec![RespValue::Integer(0), RespValue::Integer(7)])
        );
        // BITFIELD_RO rejects writes
        let r = handle(
            &[b("BITFIELD_RO"), b("k5"), b("SET"), b("u8"), b("0"), b("1")],
            &s,
        );
        assert!(matches!(r, RespValue::Error(_)));
        let r = handle(&[b("BITFIELD_RO"), b("k5"), b("GET"), b("u8"), b("8")], &s);
        assert_eq!(r, RespValue::array(vec![RespValue::Integer(7)]));
    }
}
