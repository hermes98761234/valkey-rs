use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Error)]
pub enum RespError {
    #[error("incomplete")]
    Incomplete,
    #[error("invalid prefix: {0}")]
    InvalidPrefix(String),
    #[error("parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
    Map(Vec<(RespValue, RespValue)>),
    Double(f64),
    Boolean(bool),
    BigNumber(i128),
    BlobError(Bytes, Bytes),
    VerbatimString(String, Bytes),
}

pub struct RespEncoder;

impl Encoder<RespValue> for RespEncoder {
    type Error = RespError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_value(&item, dst);
        Ok(())
    }
}

fn encode_value(val: &RespValue, dst: &mut BytesMut) {
    match val {
        RespValue::SimpleString(s) => {
            dst.put_u8(b'+');
            dst.put_slice(s.as_bytes());
            dst.put_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            dst.put_u8(b'-');
            dst.put_slice(s.as_bytes());
            dst.put_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            dst.put_u8(b':');
            dst.put_slice(n.to_string().as_bytes());
            dst.put_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            dst.put_slice(b"$-1\r\n");
        }
        RespValue::BulkString(Some(b)) => {
            dst.put_u8(b'$');
            dst.put_slice(b.len().to_string().as_bytes());
            dst.put_slice(b"\r\n");
            dst.put_slice(b);
            dst.put_slice(b"\r\n");
        }
        RespValue::Array(None) => {
            dst.put_slice(b"*-1\r\n");
        }
        RespValue::Array(Some(arr)) => {
            dst.put_u8(b'*');
            dst.put_slice(arr.len().to_string().as_bytes());
            dst.put_slice(b"\r\n");
            for v in arr {
                encode_value(v, dst);
            }
        }
        RespValue::Map(entries) => {
            dst.put_u8(b'%');
            dst.put_slice(entries.len().to_string().as_bytes());
            dst.put_slice(b"\r\n");
            for (k, v) in entries {
                encode_value(k, dst);
                encode_value(v, dst);
            }
        }
        RespValue::Double(d) => {
            dst.put_u8(b',');
            if d.is_infinite() {
                if d.is_sign_negative() {
                    dst.put_slice(b"-inf");
                } else {
                    dst.put_slice(b"inf");
                }
            } else {
                let s = format!("{:.17}", d);
                let s = s.trim_end_matches('0');
                let s = s.trim_end_matches('.');
                dst.put_slice(s.as_bytes());
            }
            dst.put_slice(b"\r\n");
        }
        RespValue::Boolean(b) => {
            dst.put_u8(b'#');
            if *b { dst.put_slice(b"t\r\n"); } else { dst.put_slice(b"f\r\n"); }
        }
        RespValue::BigNumber(n) => {
            dst.put_u8(b'(');
            dst.put_slice(n.to_string().as_bytes());
            dst.put_slice(b"\r\n");
        }
        RespValue::BlobError(_, data) => {
            dst.put_u8(b'!');
            dst.put_slice(data.len().to_string().as_bytes());
            dst.put_slice(b"\r\n");
            dst.put_slice(data);
            dst.put_slice(b"\r\n");
        }
        RespValue::VerbatimString(_, data) => {
            dst.put_u8(b'=');
            dst.put_slice(data.len().to_string().as_bytes());
            dst.put_slice(b"\r\n");
            dst.put_slice(data);
            dst.put_slice(b"\r\n");
        }
    }
}

pub struct RespDecoder {
    pub resp3: bool,
    depth: u32,
}

impl RespDecoder {
    pub fn new() -> Self {
        Self { resp3: false, depth: 0 }
    }
}

impl Default for RespDecoder {
    fn default() -> Self {
        Self::new()
    }
}

const MAX_DEPTH: u32 = 1024;

impl Decoder for RespDecoder {
    type Item = RespValue;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let (consumed, value) = match parse_value(src, 0) {
            ParseResult::Ok { consumed, value } => (consumed, value),
            ParseResult::Incomplete => return Ok(None),
            ParseResult::Err(e) => return Err(e),
        };

        if let RespValue::Array(Some(items)) = &value {
            if let Some(RespValue::BulkString(Some(cmd))) = items.first() {
                if cmd.eq_ignore_ascii_case(b"HELLO") {
                    self.resp3 = true;
                }
            }
        }

        src.advance(consumed);
        Ok(Some(value))
    }
}

enum ParseResult {
    Ok { consumed: usize, value: RespValue },
    Incomplete,
    Err(RespError),
}

fn parse_value(buf: &[u8], depth: u32) -> ParseResult {
    if depth > MAX_DEPTH {
        return ParseResult::Err(RespError::InvalidPrefix("nesting too deep".into()));
    }
    if buf.is_empty() {
        return ParseResult::Incomplete;
    }

    let prefix = buf[0];
    match prefix {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf, depth),
        b'*' => parse_array(buf, depth),
        b'%' => parse_map(buf, depth),
        b',' => parse_double(buf),
        b'#' => parse_boolean(buf),
        b'(' => parse_bignumber(buf),
        b'!' => parse_blob_error(buf),
        b'=' => parse_verbatim(buf),
        _ => parse_inline(buf),
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let rest = &buf[start..];
    for (i, w) in rest.windows(2).enumerate() {
        if w == b"\r\n" {
            return Some(start + i);
        }
    }
    None
}

fn read_line(buf: &[u8]) -> Result<(usize, usize), ParseResult> {
    match find_crlf(buf, 0) {
        Some(cr) => {
            if cr + 1 >= buf.len() {
                return Err(ParseResult::Incomplete);
            }
            Ok((cr, cr + 2))
        }
        None => Err(ParseResult::Incomplete),
    }
}

fn parse_simple_string(buf: &[u8]) -> ParseResult {
    let (line_end, consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let s = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s.to_owned(),
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    ParseResult::Ok { consumed, value: RespValue::SimpleString(s) }
}

fn parse_error(buf: &[u8]) -> ParseResult {
    let (line_end, consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let s = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s.to_owned(),
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    ParseResult::Ok { consumed, value: RespValue::Error(s) }
}

fn parse_integer(buf: &[u8]) -> ParseResult {
    let (line_end, consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let num_str = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let n: i64 = match num_str.parse() {
        Ok(n) => n,
        Err(e) => return ParseResult::Err(RespError::ParseInt(e)),
    };
    ParseResult::Ok { consumed, value: RespValue::Integer(n) }
}

fn parse_double(buf: &[u8]) -> ParseResult {
    let (line_end, consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let s = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let d: f64 = match s {
        "inf" | "+inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => match s.parse() {
            Ok(d) => d,
            Err(e) => return ParseResult::Err(RespError::ParseFloat(e)),
        },
    };
    ParseResult::Ok { consumed, value: RespValue::Double(d) }
}

fn parse_boolean(buf: &[u8]) -> ParseResult {
    if buf.len() < 4 {
        return ParseResult::Incomplete;
    }
    if buf[2] != b'\r' || buf[3] != b'\n' {
        return ParseResult::Err(RespError::InvalidPrefix("bad boolean".into()));
    }
    let b = match buf[1] {
        b't' => true,
        b'f' => false,
        other => {
            return ParseResult::Err(RespError::InvalidPrefix(
                format!("expected t/f, got {}", other as char),
            ))
        }
    };
    ParseResult::Ok { consumed: 4, value: RespValue::Boolean(b) }
}

fn parse_bignumber(buf: &[u8]) -> ParseResult {
    let (line_end, consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let s = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let n: i128 = match s.parse() {
        Ok(n) => n,
        Err(_) => return ParseResult::Err(RespError::InvalidPrefix("invalid bignumber".into())),
    };
    ParseResult::Ok { consumed, value: RespValue::BigNumber(n) }
}

fn parse_bulk_string(buf: &[u8], _depth: u32) -> ParseResult {
    let (line_end, line_consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let len_str = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let len: i64 = match len_str.parse() {
        Ok(n) => n,
        Err(e) => return ParseResult::Err(RespError::ParseInt(e)),
    };

    if len < 0 {
        return ParseResult::Ok {
            consumed: line_consumed,
            value: RespValue::BulkString(None),
        };
    }

    let len = len as usize;
    let total = line_consumed + len + 2;
    if buf.len() < total {
        return ParseResult::Incomplete;
    }

    let data = Bytes::copy_from_slice(&buf[line_consumed..line_consumed + len]);
    ParseResult::Ok {
        consumed: total,
        value: RespValue::BulkString(Some(data)),
    }
}

fn parse_array(buf: &[u8], depth: u32) -> ParseResult {
    let (line_end, line_consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let count_str = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let count: i64 = match count_str.parse() {
        Ok(n) => n,
        Err(e) => return ParseResult::Err(RespError::ParseInt(e)),
    };

    if count < 0 {
        return ParseResult::Ok {
            consumed: line_consumed,
            value: RespValue::Array(None),
        };
    }

    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut offset = line_consumed;

    for _ in 0..count {
        if offset >= buf.len() {
            return ParseResult::Incomplete;
        }
        match parse_value(&buf[offset..], depth + 1) {
            ParseResult::Ok { consumed, value } => {
                offset += consumed;
                items.push(value);
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Err(e) => return ParseResult::Err(e),
        }
    }

    ParseResult::Ok {
        consumed: offset,
        value: RespValue::Array(Some(items)),
    }
}

fn parse_map(buf: &[u8], depth: u32) -> ParseResult {
    let (line_end, line_consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let count_str = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let count: usize = match count_str.parse::<usize>() {
        Ok(n) => n,
        Err(e) => return ParseResult::Err(RespError::ParseInt(e)),
    };

    let mut entries = Vec::with_capacity(count);
    let mut offset = line_consumed;

    for _ in 0..count {
        if offset >= buf.len() {
            return ParseResult::Incomplete;
        }
        let key = match parse_value(&buf[offset..], depth + 1) {
            ParseResult::Ok { consumed, value } => {
                offset += consumed;
                value
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        if offset >= buf.len() {
            return ParseResult::Incomplete;
        }
        let val = match parse_value(&buf[offset..], depth + 1) {
            ParseResult::Ok { consumed, value } => {
                offset += consumed;
                value
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Err(e) => return ParseResult::Err(e),
        };
        entries.push((key, val));
    }

    ParseResult::Ok {
        consumed: offset,
        value: RespValue::Map(entries),
    }
}

fn parse_blob_error(buf: &[u8]) -> ParseResult {
    let (line_end, line_consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let len_str = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let len: i64 = match len_str.parse() {
        Ok(n) => n,
        Err(e) => return ParseResult::Err(RespError::ParseInt(e)),
    };

    if len < 0 {
        return ParseResult::Err(RespError::InvalidPrefix("blob error cannot be null".into()));
    }

    let len = len as usize;
    let total = line_consumed + len + 2;
    if buf.len() < total {
        return ParseResult::Incomplete;
    }

    let data = &buf[line_consumed..line_consumed + len];
    let (type_desc, payload) = if data.len() >= 3 {
        (Bytes::copy_from_slice(&data[..3]), Bytes::copy_from_slice(&data[3..]))
    } else {
        (Bytes::copy_from_slice(data), Bytes::new())
    };

    ParseResult::Ok {
        consumed: total,
        value: RespValue::BlobError(type_desc, payload),
    }
}

fn parse_verbatim(buf: &[u8]) -> ParseResult {
    let (line_end, line_consumed) = match read_line(&buf[1..]) {
        Ok((le, c)) => (le, c + 1),
        Err(e) => return e,
    };
    let len_str = match std::str::from_utf8(&buf[1..1 + line_end]) {
        Ok(s) => s,
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    let len: usize = match len_str.parse::<usize>() {
        Ok(n) => n,
        Err(e) => return ParseResult::Err(RespError::ParseInt(e)),
    };

    let total = line_consumed + len + 2;
    if buf.len() < total {
        return ParseResult::Incomplete;
    }

    let data = &buf[line_consumed..line_consumed + len];
    let (tag, content) = if data.len() >= 4 && data[3] == b':' {
        let tag = match std::str::from_utf8(&data[..3]) {
            Ok(s) => s.to_owned(),
            Err(e) => return ParseResult::Err(RespError::Utf8(e)),
        };
        (tag, Bytes::copy_from_slice(&data[4..]))
    } else {
        let tag = match std::str::from_utf8(data) {
            Ok(s) => s.to_owned(),
            Err(e) => return ParseResult::Err(RespError::Utf8(e)),
        };
        (tag, Bytes::new())
    };

    ParseResult::Ok {
        consumed: total,
        value: RespValue::VerbatimString(tag, content),
    }
}

fn parse_inline(buf: &[u8]) -> ParseResult {
    let (line_end, consumed) = match read_line(buf) {
        Ok((le, c)) => (le, c),
        Err(e) => return e,
    };
    let s = match std::str::from_utf8(&buf[..line_end]) {
        Ok(s) => s.to_owned(),
        Err(e) => return ParseResult::Err(RespError::Utf8(e)),
    };
    ParseResult::Ok { consumed, value: RespValue::SimpleString(s) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(val: RespValue) -> RespValue {
        let mut buf = BytesMut::new();
        let mut enc = RespEncoder;
        enc.encode(val.clone(), &mut buf).unwrap();
        let mut dec = RespDecoder::new();
        dec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn test_simple_string_round_trip() {
        let val = RespValue::SimpleString("OK".into());
        assert_eq!(round_trip(val), RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn test_error_round_trip() {
        let val = RespValue::Error("ERR unknown command".into());
        assert_eq!(round_trip(val), RespValue::Error("ERR unknown command".into()));
    }

    #[test]
    fn test_integer_round_trip() {
        assert_eq!(round_trip(RespValue::Integer(42)), RespValue::Integer(42));
    }

    #[test]
    fn test_integer_negative() {
        assert_eq!(round_trip(RespValue::Integer(-1)), RespValue::Integer(-1));
    }

    #[test]
    fn test_integer_zero() {
        assert_eq!(round_trip(RespValue::Integer(0)), RespValue::Integer(0));
    }

    #[test]
    fn test_bulk_string_round_trip() {
        let val = RespValue::BulkString(Some(Bytes::from_static(b"hello")));
        assert_eq!(round_trip(val), RespValue::BulkString(Some(Bytes::from_static(b"hello"))));
    }

    #[test]
    fn test_null_bulk_string() {
        assert_eq!(round_trip(RespValue::BulkString(None)), RespValue::BulkString(None));
    }

    #[test]
    fn test_empty_bulk_string() {
        assert_eq!(round_trip(RespValue::BulkString(Some(Bytes::new()))), RespValue::BulkString(Some(Bytes::new())));
    }

    #[test]
    fn test_array_round_trip() {
        let val = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ]));
        assert_eq!(round_trip(val), RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ])));
    }

    #[test]
    fn test_null_array() {
        assert_eq!(round_trip(RespValue::Array(None)), RespValue::Array(None));
    }

    #[test]
    fn test_nested_array() {
        let val = RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(2)])),
            RespValue::BulkString(Some(Bytes::from_static(b"hi"))),
        ]));
        assert_eq!(round_trip(val), RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(2)])),
            RespValue::BulkString(Some(Bytes::from_static(b"hi"))),
        ])));
    }

    #[test]
    fn test_double_round_trip() {
        assert_eq!(round_trip(RespValue::Double(3.14)), RespValue::Double(3.14));
    }

    #[test]
    fn test_double_infinity() {
        assert_eq!(round_trip(RespValue::Double(f64::INFINITY)), RespValue::Double(f64::INFINITY));
    }

    #[test]
    fn test_double_neg_infinity() {
        assert_eq!(round_trip(RespValue::Double(f64::NEG_INFINITY)), RespValue::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn test_double_zero() {
        assert_eq!(round_trip(RespValue::Double(0.0)), RespValue::Double(0.0));
    }

    #[test]
    fn test_double_negative() {
        assert_eq!(round_trip(RespValue::Double(-1.5)), RespValue::Double(-1.5));
    }

    #[test]
    fn test_boolean_true() {
        assert_eq!(round_trip(RespValue::Boolean(true)), RespValue::Boolean(true));
    }

    #[test]
    fn test_boolean_false() {
        assert_eq!(round_trip(RespValue::Boolean(false)), RespValue::Boolean(false));
    }

    #[test]
    fn test_bignumber_round_trip() {
        let val = RespValue::BigNumber(123456789012345678901234567890i128);
        assert_eq!(round_trip(val), RespValue::BigNumber(123456789012345678901234567890i128));
    }

    #[test]
    fn test_bignumber_negative() {
        let val = RespValue::BigNumber(-99999999999999999999i128);
        assert_eq!(round_trip(val), RespValue::BigNumber(-99999999999999999999i128));
    }

    #[test]
    fn test_blob_error_round_trip() {
        let val = RespValue::BlobError(Bytes::from_static(b"ERR"), Bytes::from_static(b"something went wrong"));
        assert_eq!(round_trip(val), RespValue::BlobError(Bytes::from_static(b"ERR"), Bytes::from_static(b"something went wrong")));
    }

    #[test]
    fn test_verbatim_string_round_trip() {
        let val = RespValue::VerbatimString("txt".into(), Bytes::from_static(b"Hello, World!"));
        assert_eq!(round_trip(val), RespValue::VerbatimString("txt".into(), Bytes::from_static(b"Hello, World!")));
    }

    #[test]
    fn test_map_round_trip() {
        let val = RespValue::Map(vec![
            (RespValue::BulkString(Some(Bytes::from_static(b"key1"))), RespValue::BulkString(Some(Bytes::from_static(b"val1")))),
            (RespValue::BulkString(Some(Bytes::from_static(b"key2"))), RespValue::Integer(42)),
        ]);
        assert_eq!(round_trip(val), RespValue::Map(vec![
            (RespValue::BulkString(Some(Bytes::from_static(b"key1"))), RespValue::BulkString(Some(Bytes::from_static(b"val1")))),
            (RespValue::BulkString(Some(Bytes::from_static(b"key2"))), RespValue::Integer(42)),
        ]));
    }

    #[test]
    fn test_empty_array() {
        assert_eq!(round_trip(RespValue::Array(Some(vec![]))), RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_empty_map() {
        assert_eq!(round_trip(RespValue::Map(vec![])), RespValue::Map(vec![]));
    }

    #[test]
    fn test_bulk_string_with_binary_data() {
        let data = vec![0u8, 1, 2, 255, 128];
        let val = RespValue::BulkString(Some(Bytes::from(data.clone())));
        assert_eq!(round_trip(val), RespValue::BulkString(Some(Bytes::from(data))));
    }

    #[test]
    fn test_partial_buffer_returns_none() {
        let mut buf = BytesMut::from("*3\r\n$3\r\nGET\r\n$3\r\n".as_bytes());
        let mut dec = RespDecoder::new();
        let result = dec.decode(&mut buf).unwrap();
        assert!(result.is_none());
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_partial_buffer_then_complete() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r".as_bytes());
        let mut dec = RespDecoder::new();
        assert!(dec.decode(&mut buf).unwrap().is_none());
        buf.put_slice(b"\nkey\r\n");
        let result = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ])));
    }

    #[test]
    fn test_inline_command() {
        let mut buf = BytesMut::from("PING\r\n".as_bytes());
        let mut dec = RespDecoder::new();
        assert_eq!(dec.decode(&mut buf).unwrap().unwrap(), RespValue::SimpleString("PING".into()));
    }

    #[test]
    fn test_inline_command_multiple_words() {
        let mut buf = BytesMut::from("SET key value\r\n".as_bytes());
        let mut dec = RespDecoder::new();
        assert_eq!(dec.decode(&mut buf).unwrap().unwrap(), RespValue::SimpleString("SET key value".into()));
    }

    #[test]
    fn test_hello_sets_resp3_flag() {
        let mut buf = BytesMut::from("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".as_bytes());
        let mut dec = RespDecoder::new();
        assert!(!dec.resp3);
        let _ = dec.decode(&mut buf).unwrap();
        assert!(dec.resp3);
    }

    #[test]
    fn test_multiple_messages_in_buffer() {
        let data = b"+OK\r\n:42\r\n";
        let mut buf = BytesMut::from(&data[..]);
        let mut dec = RespDecoder::new();
        assert_eq!(dec.decode(&mut buf).unwrap().unwrap(), RespValue::SimpleString("OK".into()));
        assert_eq!(dec.decode(&mut buf).unwrap().unwrap(), RespValue::Integer(42));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_ping_command() {
        let mut buf = BytesMut::from("*1\r\n$4\r\nPING\r\n".as_bytes());
        let mut dec = RespDecoder::new();
        let val = dec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"PING"))),
        ])));
    }

    #[test]
    fn test_encode_ping_command() {
        let mut buf = BytesMut::new();
        let val = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"PING"))),
        ]));
        RespEncoder.encode(val, &mut buf).unwrap();
        assert_eq!(&buf[..], b"*1\r\n$4\r\nPING\r\n");
    }
}
