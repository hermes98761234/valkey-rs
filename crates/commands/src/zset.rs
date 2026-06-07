use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Store, ZSetData};

pub type Db = Arc<Store>;

enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
    NegInfinity,
    PosInfinity,
}

impl ScoreBound {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "-inf" => Some(ScoreBound::NegInfinity),
            "+inf" => Some(ScoreBound::PosInfinity),
            _ if s.starts_with('(') => s[1..].parse::<f64>().ok().map(ScoreBound::Exclusive),
            _ => s.parse::<f64>().ok().map(ScoreBound::Inclusive),
        }
    }
    /// Check if score is within this upper bound (score <= v or score < v)
    fn contains_upper(&self, score: f64) -> bool {
        match self {
            ScoreBound::Inclusive(v) => score <= *v,
            ScoreBound::Exclusive(v) => score < *v,
            ScoreBound::NegInfinity => false,
            ScoreBound::PosInfinity => true,
        }
    }
    /// Check if score is within this lower bound (score >= v or score > v)
    fn contains_lower(&self, score: f64) -> bool {
        match self {
            ScoreBound::Inclusive(v) => score >= *v,
            ScoreBound::Exclusive(v) => score > *v,
            ScoreBound::NegInfinity => true,
            ScoreBound::PosInfinity => false,
        }
    }
    fn value(&self) -> f64 {
        match self {
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => *v,
            ScoreBound::NegInfinity => f64::NEG_INFINITY,
            ScoreBound::PosInfinity => f64::INFINITY,
        }
    }
}

#[derive(Debug, Clone)]
enum LexBound {
    Inclusive(Bytes),
    Exclusive(Bytes),
    NegInfinity,
    PosInfinity,
}

impl LexBound {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "-" => Some(LexBound::NegInfinity),
            "+" => Some(LexBound::PosInfinity),
            _ if s.starts_with('(') => Some(LexBound::Exclusive(Bytes::from(s[1..].to_string()))),
            _ if s.starts_with('[') => Some(LexBound::Inclusive(Bytes::from(s[1..].to_string()))),
            _ => None,
        }
    }
    fn contains(&self, member: &Bytes) -> bool {
        match self {
            LexBound::Inclusive(v) => member >= v,
            LexBound::Exclusive(v) => member > v,
            _ => true,
        }
    }
}

fn parse_score(b: &Bytes) -> Result<f64, String> {
    let s = std::str::from_utf8(b).map_err(|_| "ERR value is not a valid float")?;
    match s {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        _ => s
            .parse::<f64>()
            .map_err(|_| "ERR value is not a valid float".into()),
    }
}

fn str_from_bytes(b: &Bytes) -> Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "ERR value is not a valid string".into())
}

fn zset_entries(zset: &ZSetData) -> Vec<(Bytes, f64)> {
    let mut result = Vec::new();
    for (score, members) in &zset.scores {
        for member in members {
            result.push((member.clone(), score.0));
        }
    }
    result
}

fn zset_rank(zset: &ZSetData, member: &Bytes) -> Option<usize> {
    let target = zset.members.get(member)?;
    let mut rank = 0;
    for (score, members) in &zset.scores {
        if score == target {
            for m in members {
                if m == member {
                    return Some(rank);
                }
                rank += 1;
            }
        } else {
            rank += members.len();
        }
    }
    None
}

fn zset_rev_rank(zset: &ZSetData, member: &Bytes) -> Option<usize> {
    let target = zset.members.get(member)?;
    let mut rank = 0;
    for (score, members) in zset.scores.iter().rev() {
        if score == target {
            for m in members.iter().rev() {
                if m == member {
                    return Some(rank);
                }
                rank += 1;
            }
        } else {
            rank += members.len();
        }
    }
    None
}

fn zset_range(zset: &ZSetData, start: isize, stop: isize) -> Vec<(Bytes, f64)> {
    let all = zset_entries(zset);
    let len = all.len() as isize;
    if len == 0 {
        return Vec::new();
    }
    let s = norm_idx(start, len);
    let e = norm_idx(stop, len);
    if s > e || s >= len as usize {
        return Vec::new();
    }
    let e = (e.min(len as usize - 1)).max(s);
    all[s..=e].to_vec()
}

fn zset_rev_range(zset: &ZSetData, start: isize, stop: isize) -> Vec<(Bytes, f64)> {
    let mut all = zset_entries(zset);
    all.reverse();
    let len = all.len() as isize;
    if len == 0 {
        return Vec::new();
    }
    let s = norm_idx(start, len);
    let e = norm_idx(stop, len);
    if s > e || s >= len as usize {
        return Vec::new();
    }
    let e = (e.min(len as usize - 1)).max(s);
    all[s..=e].to_vec()
}

fn zset_range_by_score(zset: &ZSetData, min: &ScoreBound, max: &ScoreBound) -> Vec<(Bytes, f64)> {
    let mut result = Vec::new();
    for (score, members) in &zset.scores {
        let s = score.0;
        if !min.contains_lower(s) {
            continue;
        }
        if *score > OrderedFloat(max.value()) {
            break;
        }
        if !max.contains_upper(s) {
            continue;
        }
        for member in members {
            result.push((member.clone(), s));
        }
    }
    result
}

fn zset_range_by_lex(
    zset: &ZSetData,
    min: &LexBound,
    max: &LexBound,
    offset: usize,
    count: usize,
) -> Vec<Bytes> {
    let mut all: Vec<Bytes> = zset.members.keys().cloned().collect();
    all.sort();
    let result: Vec<Bytes> = all
        .into_iter()
        .filter(|m| min.contains(m) && max.contains(m))
        .collect();
    if offset >= result.len() {
        return Vec::new();
    }
    let end = if count == usize::MAX {
        result.len()
    } else {
        (offset + count).min(result.len())
    };
    result[offset..end].to_vec()
}

fn norm_idx(idx: isize, len: isize) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx as usize
    }
}

fn get_zset(db: &Db, key: &str) -> Option<ZSetData> {
    let entry = db.get(&Bytes::from(key.to_string()))?;
    match &entry.data {
        DataType::ZSet(zs) => Some(zs.clone()),
        _ => None,
    }
}

fn set_zset(db: &Db, key: &str, zset: ZSetData) {
    db.set(Bytes::from(key.to_string()), DataType::ZSet(zset), None);
}

fn rebuild_scores(zset: &mut ZSetData) {
    let mut ns: BTreeMap<OrderedFloat<f64>, BTreeSet<Bytes>> = BTreeMap::new();
    for (m, s) in &zset.members {
        ns.entry(*s).or_default().insert(m.clone());
    }
    zset.scores = ns;
}

fn apply_limit(
    entries: Vec<(Bytes, f64)>,
    offset: usize,
    count: usize,
    ws: bool,
) -> Vec<RespValue> {
    let l = apply_lim(entries, offset, count);
    if ws {
        l.into_iter()
            .flat_map(|(m, s)| {
                vec![
                    RespValue::BulkString(Some(m)),
                    RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                ]
            })
            .collect()
    } else {
        l.into_iter()
            .map(|(m, _)| RespValue::BulkString(Some(m)))
            .collect()
    }
}

fn apply_lim(entries: Vec<(Bytes, f64)>, offset: usize, count: usize) -> Vec<(Bytes, f64)> {
    if offset >= entries.len() {
        return Vec::new();
    }
    let end = if count == usize::MAX {
        entries.len()
    } else {
        (offset + count).min(entries.len())
    };
    entries[offset..end].to_vec()
}

// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
pub fn zadd(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zadd' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let mut idx = 1;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut incr = false;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            "CH" => {}
            "INCR" => incr = true,
            _ => break,
        }
        idx += 1;
    }
    if nx && xx {
        return Err("ERR XX and NX options at the same time are not compatible".into());
    }
    if gt && lt {
        return Err("ERR GT and LT options at the same time are not compatible".into());
    }
    let remaining = args.len() - idx;
    if remaining < 2 || remaining % 2 != 0 {
        return Err("ERR syntax error".into());
    }
    if incr {
        if remaining != 2 {
            return Err("ERR INCR option supports a single score-member pair".into());
        }
        let score = parse_score(&args[idx])?;
        let member = args[idx + 1].clone();
        let mut zset = get_zset(db, key).unwrap_or_default();
        let current = zset.members.get(&member).map(|s| s.0).unwrap_or(0.0);
        zset.add(member, current + score);
        set_zset(db, key, zset);
        return Ok(RespValue::BulkString(Some(Bytes::from(format!(
            "{}",
            current + score
        )))));
    }
    let mut zset = get_zset(db, key).unwrap_or_default();
    let mut changed: usize = 0;
    let mut i = idx;
    while i + 1 < args.len() {
        let score = parse_score(&args[i])?;
        let member = args[i + 1].clone();
        let old = zset.members.get(&member).copied();
        let is_new = old.is_none();
        let should = match (nx, xx, gt, lt, old) {
            (true, _, _, _, Some(_)) => false,
            (_, true, _, _, None) => false,
            (_, _, true, _, Some(o)) if score <= o.0 => false,
            (_, _, _, true, Some(o)) if score >= o.0 => false,
            _ => true,
        };
        if should {
            zset.add(member, score);
            if is_new || old.unwrap().0 != score {
                changed += 1;
            }
        }
        i += 2;
    }
    set_zset(db, key, zset);
    Ok(RespValue::Integer(changed as i64))
}

// ZSCORE key member
pub fn zscore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'zscore' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let member = &args[1];
    match get_zset(db, key) {
        Some(zset) => match zset.members.get(member) {
            Some(s) => Ok(RespValue::BulkString(Some(Bytes::from(format!("{}", s.0))))),
            None => Ok(RespValue::BulkString(None)),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

// ZMSCORE key member [member ...]
pub fn zmscore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zmscore' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let members = &args[1..];
    match get_zset(db, key) {
        Some(zset) => Ok(RespValue::Array(Some(
            members
                .iter()
                .map(|m| match zset.members.get(m) {
                    Some(s) => RespValue::BulkString(Some(Bytes::from(format!("{}", s.0)))),
                    None => RespValue::BulkString(None),
                })
                .collect(),
        ))),
        None => Ok(RespValue::Array(Some(
            members
                .iter()
                .map(|_| RespValue::BulkString(None))
                .collect(),
        ))),
    }
}

// ZRANK key member [WITHSCORE]
pub fn zrank(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 || args.len() > 3 {
        return Err("ERR wrong number of arguments for 'zrank' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let member = &args[1];
    let ws = args.len() == 3
        && std::str::from_utf8(&args[2])
            .map(|s| s.eq_ignore_ascii_case("WITHSCORE"))
            .unwrap_or(false);
    match get_zset(db, key) {
        Some(zset) => match zset_rank(&zset, member) {
            Some(rank) => {
                if ws {
                    Ok(RespValue::Array(Some(vec![
                        RespValue::Integer(rank as i64),
                        RespValue::BulkString(Some(Bytes::from(format!(
                            "{}",
                            zset.members.get(member).map(|s| s.0).unwrap_or(0.0)
                        )))),
                    ])))
                } else {
                    Ok(RespValue::Integer(rank as i64))
                }
            }
            None => Ok(RespValue::BulkString(None)),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

// ZREVRANK key member [WITHSCORE]
pub fn zrevrank(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 || args.len() > 3 {
        return Err("ERR wrong number of arguments for 'zrevrank' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let member = &args[1];
    let ws = args.len() == 3
        && std::str::from_utf8(&args[2])
            .map(|s| s.eq_ignore_ascii_case("WITHSCORE"))
            .unwrap_or(false);
    match get_zset(db, key) {
        Some(zset) => match zset_rev_rank(&zset, member) {
            Some(rank) => {
                if ws {
                    Ok(RespValue::Array(Some(vec![
                        RespValue::Integer(rank as i64),
                        RespValue::BulkString(Some(Bytes::from(format!(
                            "{}",
                            zset.members.get(member).map(|s| s.0).unwrap_or(0.0)
                        )))),
                    ])))
                } else {
                    Ok(RespValue::Integer(rank as i64))
                }
            }
            None => Ok(RespValue::BulkString(None)),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

// ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
pub fn zrange(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zrange' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let ss = str_from_bytes(&args[1])?;
    let se = str_from_bytes(&args[2])?;
    let mut idx = 3;
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut lo: usize = 0;
    let mut lc: usize = usize::MAX;
    let mut ws = false;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "BYSCORE" => {
                by_score = true;
                idx += 1;
            }
            "BYLEX" => {
                by_lex = true;
                idx += 1;
            }
            "REV" => {
                rev = true;
                idx += 1;
            }
            "LIMIT" => {
                lo = str_from_bytes(&args[idx + 1])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                lc = str_from_bytes(&args[idx + 2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                idx += 3;
            }
            "WITHSCORES" => {
                ws = true;
                idx += 1;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    match get_zset(db, key) {
        Some(zset) => {
            if by_score {
                let min = ScoreBound::from_str(ss).ok_or("ERR min or max is not a float")?;
                let max = ScoreBound::from_str(se).ok_or("ERR min or max is not a float")?;
                let mut e = zset_range_by_score(&zset, &min, &max);
                if rev {
                    e.reverse();
                }
                return Ok(RespValue::Array(Some(apply_limit(e, lo, lc, ws))));
            }
            if by_lex {
                let min =
                    LexBound::from_str(ss).ok_or("ERR min or max not valid string range item")?;
                let max =
                    LexBound::from_str(se).ok_or("ERR min or max not valid string range item")?;
                let members = zset_range_by_lex(&zset, &min, &max, lo, lc);
                let r: Vec<RespValue> = if rev {
                    members
                        .into_iter()
                        .rev()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect()
                } else {
                    members
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect()
                };
                return Ok(RespValue::Array(Some(r)));
            }
            let start: isize = ss
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range")?;
            let stop: isize = se
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range")?;
            let e = if rev {
                zset_rev_range(&zset, start, stop)
            } else {
                zset_range(&zset, start, stop)
            };
            let r = if ws {
                e.into_iter()
                    .flat_map(|(m, s)| {
                        vec![
                            RespValue::BulkString(Some(m)),
                            RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                        ]
                    })
                    .collect()
            } else {
                e.into_iter()
                    .map(|(m, _)| RespValue::BulkString(Some(m)))
                    .collect()
            };
            Ok(RespValue::Array(Some(r)))
        }
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZREVRANGE key start stop [WITHSCORES]
pub fn zrevrange(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 || args.len() > 4 {
        return Err("ERR wrong number of arguments for 'zrevrange' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let start: isize = str_from_bytes(&args[1])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    let stop: isize = str_from_bytes(&args[2])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    let ws = args.len() == 4
        && std::str::from_utf8(&args[3])
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);
    match get_zset(db, key) {
        Some(zset) => {
            let e = zset_rev_range(&zset, start, stop);
            let r = if ws {
                e.into_iter()
                    .flat_map(|(m, s)| {
                        vec![
                            RespValue::BulkString(Some(m)),
                            RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                        ]
                    })
                    .collect()
            } else {
                e.into_iter()
                    .map(|(m, _)| RespValue::BulkString(Some(m)))
                    .collect()
            };
            Ok(RespValue::Array(Some(r)))
        }
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
pub fn zrangebyscore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zrangebyscore' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let min =
        ScoreBound::from_str(str_from_bytes(&args[1])?).ok_or("ERR min or max is not a float")?;
    let max =
        ScoreBound::from_str(str_from_bytes(&args[2])?).ok_or("ERR min or max is not a float")?;
    let mut idx = 3;
    let mut ws = false;
    let mut lo: usize = 0;
    let mut lc: usize = usize::MAX;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "WITHSCORES" => {
                ws = true;
                idx += 1;
            }
            "LIMIT" => {
                lo = str_from_bytes(&args[idx + 1])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                lc = str_from_bytes(&args[idx + 2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                idx += 3;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    match get_zset(db, key) {
        Some(zset) => Ok(RespValue::Array(Some(apply_limit(
            zset_range_by_score(&zset, &min, &max),
            lo,
            lc,
            ws,
        )))),
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
pub fn zrevrangebyscore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zrevrangebyscore' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let max =
        ScoreBound::from_str(str_from_bytes(&args[1])?).ok_or("ERR min or max is not a float")?;
    let min =
        ScoreBound::from_str(str_from_bytes(&args[2])?).ok_or("ERR min or max is not a float")?;
    let mut idx = 3;
    let mut ws = false;
    let mut lo: usize = 0;
    let mut lc: usize = usize::MAX;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "WITHSCORES" => {
                ws = true;
                idx += 1;
            }
            "LIMIT" => {
                lo = str_from_bytes(&args[idx + 1])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                lc = str_from_bytes(&args[idx + 2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                idx += 3;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    match get_zset(db, key) {
        Some(zset) => {
            let mut e = zset_range_by_score(&zset, &min, &max);
            e.reverse();
            Ok(RespValue::Array(Some(apply_limit(e, lo, lc, ws))))
        }
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZRANGEBYLEX key min max [LIMIT offset count]
pub fn zrangebylex(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zrangebylex' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let min = LexBound::from_str(str_from_bytes(&args[1])?)
        .ok_or("ERR min or max not valid string range item")?;
    let max = LexBound::from_str(str_from_bytes(&args[2])?)
        .ok_or("ERR min or max not valid string range item")?;
    let mut idx = 3;
    let mut lo: usize = 0;
    let mut lc: usize = usize::MAX;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "LIMIT" => {
                lo = str_from_bytes(&args[idx + 1])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                lc = str_from_bytes(&args[idx + 2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                idx += 3;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    match get_zset(db, key) {
        Some(zset) => {
            let m = zset_range_by_lex(&zset, &min, &max, lo, lc);
            Ok(RespValue::Array(Some(
                m.into_iter()
                    .map(|x| RespValue::BulkString(Some(x)))
                    .collect(),
            )))
        }
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZCOUNT key min max
pub fn zcount(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'zcount' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let min =
        ScoreBound::from_str(str_from_bytes(&args[1])?).ok_or("ERR min or max is not a float")?;
    let max =
        ScoreBound::from_str(str_from_bytes(&args[2])?).ok_or("ERR min or max is not a float")?;
    match get_zset(db, key) {
        Some(zset) => Ok(RespValue::Integer(
            zset_range_by_score(&zset, &min, &max).len() as i64,
        )),
        None => Ok(RespValue::Integer(0)),
    }
}

// ZLEXCOUNT key min max
pub fn zlexcount(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'zlexcount' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let min = LexBound::from_str(str_from_bytes(&args[1])?)
        .ok_or("ERR min or max not valid string range item")?;
    let max = LexBound::from_str(str_from_bytes(&args[2])?)
        .ok_or("ERR min or max not valid string range item")?;
    match get_zset(db, key) {
        Some(zset) => Ok(RespValue::Integer(
            zset_range_by_lex(&zset, &min, &max, 0, usize::MAX).len() as i64,
        )),
        None => Ok(RespValue::Integer(0)),
    }
}

// ZREM key member [member ...]
pub fn zrem(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zrem' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let members = &args[1..];
    match get_zset(db, key) {
        Some(mut zset) => {
            let mut count = 0;
            for m in members {
                if zset.members.remove(m).is_some() {
                    count += 1;
                }
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            Ok(RespValue::Integer(count as i64))
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ZREMRANGEBYRANK key start stop
pub fn zremrangebyrank(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'zremrangebyrank' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let start: isize = str_from_bytes(&args[1])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    let stop: isize = str_from_bytes(&args[2])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    match get_zset(db, key) {
        Some(mut zset) => {
            let e = zset_range(&zset, start, stop);
            let count = e.len();
            for (m, _) in &e {
                zset.members.remove(m);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            Ok(RespValue::Integer(count as i64))
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ZREMRANGEBYSCORE key min max
pub fn zremrangebyscore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'zremrangebyscore' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let min =
        ScoreBound::from_str(str_from_bytes(&args[1])?).ok_or("ERR min or max is not a float")?;
    let max =
        ScoreBound::from_str(str_from_bytes(&args[2])?).ok_or("ERR min or max is not a float")?;
    match get_zset(db, key) {
        Some(mut zset) => {
            let e = zset_range_by_score(&zset, &min, &max);
            let count = e.len();
            for (m, _) in &e {
                zset.members.remove(m);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            Ok(RespValue::Integer(count as i64))
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ZREMRANGEBYLEX key min max
pub fn zremrangebylex(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'zremrangebylex' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let min = LexBound::from_str(str_from_bytes(&args[1])?)
        .ok_or("ERR min or max not valid string range item")?;
    let max = LexBound::from_str(str_from_bytes(&args[2])?)
        .ok_or("ERR min or max not valid string range item")?;
    match get_zset(db, key) {
        Some(mut zset) => {
            let m = zset_range_by_lex(&zset, &min, &max, 0, usize::MAX);
            let count = m.len();
            for x in &m {
                zset.members.remove(x);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            Ok(RespValue::Integer(count as i64))
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ZCARD key
pub fn zcard(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 1 {
        return Err("ERR wrong number of arguments for 'zcard' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    match get_zset(db, key) {
        Some(zset) => Ok(RespValue::Integer(zset.len() as i64)),
        None => Ok(RespValue::Integer(0)),
    }
}

// ZINCRBY key increment member
pub fn zincrby(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'zincrby' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let increment = parse_score(&args[1])?;
    let member = args[2].clone();
    let mut zset = get_zset(db, key).unwrap_or_default();
    let current = zset.members.get(&member).map(|s| s.0).unwrap_or(0.0);
    let new_score = current + increment;
    zset.add(member, new_score);
    set_zset(db, key, zset);
    Ok(RespValue::BulkString(Some(Bytes::from(format!(
        "{new_score}"
    )))))
}

// ZPOPMIN key [count]
pub fn zpopmin(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 1 || args.len() > 2 {
        return Err("ERR wrong number of arguments for 'zpopmin' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let count = if args.len() == 2 {
        str_from_bytes(&args[1])?
            .parse::<usize>()
            .map_err(|_| "ERR value is not an integer or out of range")?
    } else {
        1
    };
    match get_zset(db, key) {
        Some(mut zset) => {
            let mut e = zset_entries(&zset);
            let count = count.min(e.len());
            let popped: Vec<(Bytes, f64)> = e.drain(..count).collect();
            for (m, _) in &popped {
                zset.members.remove(m);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            let r: Vec<RespValue> = popped
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect();
            Ok(RespValue::Array(Some(r)))
        }
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZPOPMAX key [count]
pub fn zpopmax(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 1 || args.len() > 2 {
        return Err("ERR wrong number of arguments for 'zpopmax' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let count = if args.len() == 2 {
        str_from_bytes(&args[1])?
            .parse::<usize>()
            .map_err(|_| "ERR value is not an integer or out of range")?
    } else {
        1
    };
    match get_zset(db, key) {
        Some(mut zset) => {
            let mut e = zset_entries(&zset);
            e.reverse();
            let count = count.min(e.len());
            let popped: Vec<(Bytes, f64)> = e.drain(..count).collect();
            for (m, _) in &popped {
                zset.members.remove(m);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            let r: Vec<RespValue> = popped
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect();
            Ok(RespValue::Array(Some(r)))
        }
        None => Ok(RespValue::Array(Some(vec![]))),
    }
}

// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
pub fn zunionstore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zunionstore' command".into());
    }
    let dest = str_from_bytes(&args[0])?;
    let numkeys: usize = str_from_bytes(&args[1])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 2 + numkeys {
        return Err("ERR syntax error".into());
    }
    let mut idx = 2 + numkeys;
    let mut weights = vec![1.0f64; numkeys];
    let mut agg = "SUM";
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "WEIGHTS" => {
                for i in 0..numkeys {
                    weights[i] = parse_score(&args[idx + 1 + i])?;
                }
                idx += 1 + numkeys;
            }
            "AGGREGATE" => {
                agg = match str_from_bytes(&args[idx + 1])?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "SUM" | "MIN" | "MAX" => {
                        str_from_bytes(&args[idx + 1])?.to_ascii_uppercase().leak()
                    }
                    _ => return Err("ERR syntax error".into()),
                };
                idx += 2;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    let mut all: HashMap<Bytes, Vec<f64>> = HashMap::new();
    for i in 0..numkeys {
        if let Some(zset) = get_zset(db, str_from_bytes(&args[2 + i])?) {
            for (m, s) in &zset.members {
                all.entry(m.clone()).or_default().push(s.0 * weights[i]);
            }
        }
    }
    let mut rz = ZSetData::new();
    for (m, scores) in all {
        rz.add(
            m,
            match agg {
                "SUM" => scores.iter().sum(),
                "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                _ => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
            },
        );
    }
    let count = rz.len();
    set_zset(db, dest, rz);
    Ok(RespValue::Integer(count as i64))
}

// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
pub fn zinterstore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zinterstore' command".into());
    }
    let dest = str_from_bytes(&args[0])?;
    let numkeys: usize = str_from_bytes(&args[1])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 2 + numkeys {
        return Err("ERR syntax error".into());
    }
    let mut idx = 2 + numkeys;
    let mut weights = vec![1.0f64; numkeys];
    let mut agg = "SUM";
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "WEIGHTS" => {
                for i in 0..numkeys {
                    weights[i] = parse_score(&args[idx + 1 + i])?;
                }
                idx += 1 + numkeys;
            }
            "AGGREGATE" => {
                agg = match str_from_bytes(&args[idx + 1])?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "SUM" | "MIN" | "MAX" => {
                        str_from_bytes(&args[idx + 1])?.to_ascii_uppercase().leak()
                    }
                    _ => return Err("ERR syntax error".into()),
                };
                idx += 2;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    let sets: Vec<ZSetData> = (0..numkeys)
        .filter_map(|i| get_zset(db, str_from_bytes(&args[2 + i]).ok()?))
        .collect();
    let mut rz = ZSetData::new();
    if let Some(first) = sets.first() {
        for (m, _s) in &first.members {
            if sets.iter().all(|st| st.members.contains_key(m)) {
                let scores: Vec<f64> = sets
                    .iter()
                    .enumerate()
                    .map(|(i, st)| st.members.get(m).unwrap().0 * weights[i])
                    .collect();
                rz.add(
                    m.clone(),
                    match agg {
                        "SUM" => scores.iter().sum(),
                        "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                        _ => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
                    },
                );
            }
        }
    }
    let count = rz.len();
    set_zset(db, dest, rz);
    Ok(RespValue::Integer(count as i64))
}

// ZDIFFSTORE destination numkeys key [key ...]
pub fn zdiffstore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zdiffstore' command".into());
    }
    let dest = str_from_bytes(&args[0])?;
    let numkeys: usize = str_from_bytes(&args[1])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 2 + numkeys {
        return Err("ERR syntax error".into());
    }
    let sets: Vec<ZSetData> = (0..numkeys)
        .filter_map(|i| get_zset(db, str_from_bytes(&args[2 + i]).ok()?))
        .collect();
    let mut rz = ZSetData::new();
    if let Some(first) = sets.first() {
        for (m, s) in &first.members {
            if !sets[1..].iter().any(|st| st.members.contains_key(m)) {
                rz.add(m.clone(), s.0);
            }
        }
    }
    let count = rz.len();
    set_zset(db, dest, rz);
    Ok(RespValue::Integer(count as i64))
}

// ZSCAN key cursor [MATCH pattern] [COUNT count]
pub fn zscan(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zscan' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let cursor: usize = str_from_bytes(&args[1])?
        .parse()
        .map_err(|_| "ERR invalid cursor")?;
    let mut idx = 2;
    let mut pat: Option<String> = None;
    let mut count: usize = 10;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "MATCH" => {
                pat = Some(str_from_bytes(&args[idx + 1])?.to_string());
                idx += 2;
            }
            "COUNT" => {
                count = str_from_bytes(&args[idx + 1])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                idx += 2;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    match get_zset(db, key) {
        Some(zset) => {
            let all = zset_entries(&zset);
            let len = all.len();
            if cursor >= len {
                return Ok(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from("0"))),
                    RespValue::Array(Some(vec![])),
                ])));
            }
            let end = (cursor + count).min(len);
            let nc = if end >= len { 0 } else { end };
            let chunk: Vec<RespValue> = all[cursor..end]
                .iter()
                .filter(|(m, _)| match &pat {
                    Some(p) if p != "*" => std::str::from_utf8(m)
                        .map(|s| glob_match(s, p))
                        .unwrap_or(true),
                    _ => true,
                })
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m.clone())),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect();
            Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from(nc.to_string()))),
                RespValue::Array(Some(chunk)),
            ])))
        }
        None => Ok(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from("0"))),
            RespValue::Array(Some(vec![])),
        ]))),
    }
}

fn glob_match(s: &str, p: &str) -> bool {
    if p == "*" {
        return true;
    }
    if let Some(pos) = p.find('*') {
        s.starts_with(&p[..pos]) && (p[pos + 1..].is_empty() || s.ends_with(&p[pos + 1..]))
    } else {
        s == p
    }
}

// ZRANDMEMBER key [count [WITHSCORES]]
pub fn zrandmember(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 1 || args.len() > 3 {
        return Err("ERR wrong number of arguments for 'zrandmember' command".into());
    }
    let key = str_from_bytes(&args[0])?;
    let count: usize = if args.len() >= 2 {
        str_from_bytes(&args[1])?
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range")?
    } else {
        1
    };
    let ws = args.len() == 3
        && std::str::from_utf8(&args[2])
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false);
    match get_zset(db, key) {
        Some(zset) => {
            if zset.is_empty() {
                return if args.len() < 2 {
                    Ok(RespValue::BulkString(None))
                } else {
                    Ok(RespValue::Array(Some(vec![])))
                };
            }
            let all = zset_entries(&zset);
            let e: Vec<(Bytes, f64)> = all.into_iter().take(count).collect();
            if args.len() < 2 {
                if let Some((m, _)) = e.first() {
                    return Ok(RespValue::BulkString(Some(m.clone())));
                }
                return Ok(RespValue::BulkString(None));
            }
            if ws {
                Ok(RespValue::Array(Some(
                    e.into_iter()
                        .flat_map(|(m, s)| {
                            vec![
                                RespValue::BulkString(Some(m)),
                                RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                            ]
                        })
                        .collect(),
                )))
            } else {
                Ok(RespValue::Array(Some(
                    e.into_iter()
                        .map(|(m, _)| RespValue::BulkString(Some(m)))
                        .collect(),
                )))
            }
        }
        None => {
            if args.len() < 2 {
                Ok(RespValue::BulkString(None))
            } else {
                Ok(RespValue::Array(Some(vec![])))
            }
        }
    }
}

// ZRANGESTORE dest src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
pub fn zrangestore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 4 {
        return Err("ERR wrong number of arguments for 'zrangestore' command".into());
    }
    let dest = str_from_bytes(&args[0])?;
    let src = str_from_bytes(&args[1])?;
    let ms = str_from_bytes(&args[2])?;
    let me_ = str_from_bytes(&args[3])?;
    let mut idx = 4;
    let mut bs = false;
    let mut bl = false;
    let mut rev = false;
    let mut lo: usize = 0;
    let mut lc: usize = usize::MAX;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "BYSCORE" => {
                bs = true;
                idx += 1;
            }
            "BYLEX" => {
                bl = true;
                idx += 1;
            }
            "REV" => {
                rev = true;
                idx += 1;
            }
            "LIMIT" => {
                lo = str_from_bytes(&args[idx + 1])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                lc = str_from_bytes(&args[idx + 2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                idx += 3;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    let count = match get_zset(db, src) {
        Some(zset) => {
            let e = if bs {
                let min = ScoreBound::from_str(ms).ok_or("ERR min or max is not a float")?;
                let max = ScoreBound::from_str(me_).ok_or("ERR min or max is not a float")?;
                let mut ev = zset_range_by_score(&zset, &min, &max);
                if rev {
                    ev.reverse();
                }
                apply_lim(ev, lo, lc)
            } else if bl {
                let min =
                    LexBound::from_str(ms).ok_or("ERR min or max not valid string range item")?;
                let max =
                    LexBound::from_str(me_).ok_or("ERR min or max not valid string range item")?;
                zset_range_by_lex(&zset, &min, &max, lo, lc)
                    .into_iter()
                    .map(|m| (m, 0.0))
                    .collect()
            } else {
                let s: isize = ms
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                let e: isize = me_
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                apply_lim(
                    if rev {
                        zset_rev_range(&zset, s, e)
                    } else {
                        zset_range(&zset, s, e)
                    },
                    lo,
                    lc,
                )
            };
            let mut nz = ZSetData::new();
            for (m, s) in e {
                nz.add(m, s);
            }
            let c = nz.len();
            set_zset(db, dest, nz);
            c
        }
        None => 0,
    };
    Ok(RespValue::Integer(count as i64))
}

// ZDIFF numkeys key [key ...] [WITHSCORES]
pub fn zdiff(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zdiff' command".into());
    }
    let numkeys: usize = str_from_bytes(&args[0])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 1 + numkeys {
        return Err("ERR syntax error".into());
    }
    let mut idx = 1 + numkeys;
    let ws = if idx < args.len() {
        let v = std::str::from_utf8(&args[idx])
            .map_err(|_| "ERR syntax error")?
            .eq_ignore_ascii_case("WITHSCORES");
        if v {
            idx += 1;
        }
        v
    } else {
        false
    };
    if idx != args.len() {
        return Err("ERR syntax error".into());
    }
    let sets: Vec<ZSetData> = (0..numkeys)
        .filter_map(|i| get_zset(db, str_from_bytes(&args[1 + i]).ok()?))
        .collect();
    let entries: Vec<(Bytes, f64)> = if let Some(first) = sets.first() {
        zset_entries(first)
            .into_iter()
            .filter(|(m, _)| !sets[1..].iter().any(|st| st.members.contains_key(m)))
            .collect()
    } else {
        Vec::new()
    };
    if ws {
        Ok(RespValue::Array(Some(
            entries
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect(),
        )))
    } else {
        Ok(RespValue::Array(Some(
            entries
                .into_iter()
                .map(|(m, _)| RespValue::BulkString(Some(m)))
                .collect(),
        )))
    }
}

// ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub fn zinter(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zinter' command".into());
    }
    let numkeys: usize = str_from_bytes(&args[0])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 1 + numkeys {
        return Err("ERR syntax error".into());
    }
    let mut idx = 1 + numkeys;
    let mut weights = vec![1.0f64; numkeys];
    let mut agg = "SUM";
    let mut ws = false;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "WEIGHTS" => {
                for i in 0..numkeys {
                    weights[i] = parse_score(&args[idx + 1 + i])?;
                }
                idx += 1 + numkeys;
            }
            "AGGREGATE" => {
                agg = match str_from_bytes(&args[idx + 1])?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "SUM" | "MIN" | "MAX" => {
                        str_from_bytes(&args[idx + 1])?.to_ascii_uppercase().leak()
                    }
                    _ => return Err("ERR syntax error".into()),
                };
                idx += 2;
            }
            "WITHSCORES" => {
                ws = true;
                idx += 1;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    let sets: Vec<ZSetData> = (0..numkeys)
        .filter_map(|i| get_zset(db, str_from_bytes(&args[1 + i]).ok()?))
        .collect();
    if sets.len() != numkeys {
        return Ok(RespValue::Array(Some(vec![])));
    }
    let mut entries: Vec<(Bytes, f64)> = Vec::new();
    if let Some(first) = sets.first() {
        for (m, _s) in &first.members {
            if sets.iter().all(|st| st.members.contains_key(m)) {
                let scores: Vec<f64> = sets
                    .iter()
                    .enumerate()
                    .map(|(i, st)| st.members.get(m).unwrap().0 * weights[i])
                    .collect();
                let score = match agg {
                    "SUM" => scores.iter().sum(),
                    "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                    _ => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
                };
                entries.push((m.clone(), score));
            }
        }
    }
    if ws {
        Ok(RespValue::Array(Some(
            entries
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect(),
        )))
    } else {
        Ok(RespValue::Array(Some(
            entries
                .into_iter()
                .map(|(m, _)| RespValue::BulkString(Some(m)))
                .collect(),
        )))
    }
}

// ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub fn zunion(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zunion' command".into());
    }
    let numkeys: usize = str_from_bytes(&args[0])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 1 + numkeys {
        return Err("ERR syntax error".into());
    }
    let mut idx = 1 + numkeys;
    let mut weights = vec![1.0f64; numkeys];
    let mut agg = "SUM";
    let mut ws = false;
    while idx < args.len() {
        match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
            "WEIGHTS" => {
                for i in 0..numkeys {
                    weights[i] = parse_score(&args[idx + 1 + i])?;
                }
                idx += 1 + numkeys;
            }
            "AGGREGATE" => {
                agg = match str_from_bytes(&args[idx + 1])?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "SUM" | "MIN" | "MAX" => {
                        str_from_bytes(&args[idx + 1])?.to_ascii_uppercase().leak()
                    }
                    _ => return Err("ERR syntax error".into()),
                };
                idx += 2;
            }
            "WITHSCORES" => {
                ws = true;
                idx += 1;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    let mut all: HashMap<Bytes, Vec<f64>> = HashMap::new();
    for i in 0..numkeys {
        if let Some(zset) = get_zset(db, str_from_bytes(&args[1 + i])?) {
            for (m, s) in &zset.members {
                all.entry(m.clone()).or_default().push(s.0 * weights[i]);
            }
        }
    }
    let mut entries: Vec<(Bytes, f64)> = all
        .into_iter()
        .map(|(m, scores)| {
            let score = match agg {
                "SUM" => scores.iter().sum(),
                "MIN" => scores.into_iter().fold(f64::INFINITY, f64::min),
                _ => scores.into_iter().fold(f64::NEG_INFINITY, f64::max),
            };
            (m, score)
        })
        .collect();
    entries.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    if ws {
        Ok(RespValue::Array(Some(
            entries
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect(),
        )))
    } else {
        Ok(RespValue::Array(Some(
            entries
                .into_iter()
                .map(|(m, _)| RespValue::BulkString(Some(m)))
                .collect(),
        )))
    }
}

// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
pub fn zmpop(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 {
        return Err("ERR wrong number of arguments for 'zmpop' command".into());
    }
    let numkeys: usize = str_from_bytes(&args[0])?
        .parse()
        .map_err(|_| "ERR value is not an integer or out of range")?;
    if args.len() < 2 + numkeys {
        return Err("ERR syntax error".into());
    }
    let mut idx = 1 + numkeys;
    let is_min = match str_from_bytes(&args[idx])?.to_ascii_uppercase().as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => return Err("ERR syntax error".into()),
    };
    idx += 1;
    let mut count: usize = 1;
    if idx < args.len() && str_from_bytes(&args[idx])?.to_ascii_uppercase() == "COUNT" {
        count = str_from_bytes(&args[idx + 1])?
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range")?;
        idx += 2;
    }
    if idx != args.len() {
        return Err("ERR syntax error".into());
    }
    // Find the first non-empty zset
    for i in 0..numkeys {
        let key = str_from_bytes(&args[1 + i])?;
        if let Some(mut zset) = get_zset(db, key) {
            if zset.is_empty() {
                continue;
            }
            let mut entries = zset_entries(&zset);
            if !is_min {
                entries.reverse();
            }
            let count = count.min(entries.len());
            let popped: Vec<(Bytes, f64)> = entries.drain(..count).collect();
            for (m, _) in &popped {
                zset.members.remove(m);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            let members: Vec<RespValue> = popped
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect();
            return Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from(key.to_string()))),
                RespValue::Array(Some(members)),
            ])));
        }
    }
    Ok(RespValue::BulkString(None))
}

// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
pub async fn bzmpop(db: &Db, args: &[Bytes]) -> RespValue {
    if args.len() < 4 {
        return RespValue::Error("ERR wrong number of arguments for 'bzmpop' command".into());
    }
    let timeout = match parse_score(&args[0]) {
        Ok(v) => v,
        Err(e) => return RespValue::Error(e),
    };
    let numkeys: usize = match str_from_bytes(&args[1]).and_then(|s| {
        s.parse()
            .map_err(|_| "ERR value is not an integer or out of range".to_string())
    }) {
        Ok(v) => v,
        Err(e) => return RespValue::Error(e),
    };
    if args.len() < 3 + numkeys {
        return RespValue::Error("ERR syntax error".into());
    }
    let mut idx = 2 + numkeys;
    let is_min = match str_from_bytes(&args[idx]).map(|s| s.to_ascii_uppercase()) {
        Ok(s) if s == "MIN" => true,
        Ok(s) if s == "MAX" => false,
        _ => return RespValue::Error("ERR syntax error".into()),
    };
    idx += 1;
    let mut count: usize = 1;
    if idx < args.len()
        && str_from_bytes(&args[idx]).map(|s| s.to_ascii_uppercase()) == Ok("COUNT".to_string())
    {
        count = match str_from_bytes(&args[idx + 1]).and_then(|s| {
            s.parse()
                .map_err(|_| "ERR value is not an integer or out of range".to_string())
        }) {
            Ok(v) => v,
            Err(e) => return RespValue::Error(e),
        };
        idx += 2;
    }
    if idx != args.len() {
        return RespValue::Error("ERR syntax error".into());
    }
    let keys: Vec<String> = (0..numkeys)
        .filter_map(|i| str_from_bytes(&args[2 + i]).ok())
        .map(|s| s.to_string())
        .collect();

    // Fast path: try non-blocking first
    for key in &keys {
        if let Some(mut zset) = get_zset(db, key) {
            if zset.is_empty() {
                continue;
            }
            let mut entries = zset_entries(&zset);
            if !is_min {
                entries.reverse();
            }
            let count = count.min(entries.len());
            let popped: Vec<(Bytes, f64)> = entries.drain(..count).collect();
            for (m, _) in &popped {
                zset.members.remove(m);
            }
            rebuild_scores(&mut zset);
            set_zset(db, key, zset);
            let members: Vec<RespValue> = popped
                .into_iter()
                .flat_map(|(m, s)| {
                    vec![
                        RespValue::BulkString(Some(m)),
                        RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                    ]
                })
                .collect();
            return RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from(key.clone()))),
                RespValue::Array(Some(members)),
            ]));
        }
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return bzmpop_block_forever(db, &keys, is_min, count).await;
    }

    // Blocking path with timeout
    let dur = std::time::Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(dur, bzmpop_block_forever(db, &keys, is_min, count)).await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

async fn bzmpop_block_forever(db: &Db, keys: &[String], is_min: bool, count: usize) -> RespValue {
    let mut receivers: Vec<(String, std::sync::mpsc::Receiver<()>)> = Vec::new();
    for key in keys {
        receivers.push((key.clone(), db.watch(&Bytes::from(key.clone()))));
    }

    loop {
        for (key, _) in &receivers {
            if let Some(mut zset) = get_zset(db, key) {
                if !zset.is_empty() {
                    let mut entries = zset_entries(&zset);
                    if !is_min {
                        entries.reverse();
                    }
                    let count = count.min(entries.len());
                    let popped: Vec<(Bytes, f64)> = entries.drain(..count).collect();
                    for (m, _) in &popped {
                        zset.members.remove(m);
                    }
                    rebuild_scores(&mut zset);
                    set_zset(db, key, zset);
                    let members: Vec<RespValue> = popped
                        .into_iter()
                        .flat_map(|(m, s)| {
                            vec![
                                RespValue::BulkString(Some(m)),
                                RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                            ]
                        })
                        .collect();
                    return RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(Bytes::from(key.clone()))),
                        RespValue::Array(Some(members)),
                    ]));
                }
            }
        }

        let mut done = false;
        for (_, rx) in &receivers {
            if rx.try_recv().is_ok() {
                done = true;
                break;
            }
        }
        if !done {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// BZPOPMIN key [key ...] timeout
pub async fn bzpopmin(db: &Db, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'bzpopmin' command".into());
    }
    let timeout = match parse_score(&args[args.len() - 1]) {
        Ok(v) => v,
        Err(e) => return RespValue::Error(e),
    };
    let key_count = args.len() - 1;

    // Fast path: try non-blocking first
    for i in 0..key_count {
        let key = match str_from_bytes(&args[i]) {
            Ok(v) => v,
            Err(e) => return RespValue::Error(e),
        };
        if let Some(mut zset) = get_zset(db, key) {
            if zset.is_empty() {
                continue;
            }
            let mut entries = zset_entries(&zset);
            if let Some((m, s)) = entries.first() {
                let m = m.clone();
                let s = *s;
                zset.members.remove(&m);
                rebuild_scores(&mut zset);
                set_zset(db, key, zset);
                return RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from(key.to_string()))),
                    RespValue::BulkString(Some(m)),
                    RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                ]));
            }
        }
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return bzpopmin_block_forever(db, args, key_count).await;
    }

    // Blocking path with timeout
    let dur = Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(dur, bzpopmin_block_forever(db, args, key_count)).await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

async fn bzpopmin_block_forever(db: &Db, args: &[Bytes], key_count: usize) -> RespValue {
    let mut receivers: Vec<(String, std::sync::mpsc::Receiver<()>)> = Vec::new();
    for i in 0..key_count {
        let key = str_from_bytes(&args[i]).unwrap_or_default().to_string();
        receivers.push((key.clone(), db.watch(&Bytes::from(key))));
    }

    loop {
        for (key, _) in &receivers {
            if let Some(mut zset) = get_zset(db, key) {
                if !zset.is_empty() {
                    let mut entries = zset_entries(&zset);
                    if let Some((m, s)) = entries.first() {
                        let m = m.clone();
                        let s = *s;
                        zset.members.remove(&m);
                        rebuild_scores(&mut zset);
                        set_zset(db, key, zset);
                        return RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(Bytes::from(key.clone()))),
                            RespValue::BulkString(Some(m)),
                            RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                        ]));
                    }
                }
            }
        }

        let mut done = false;
        for (_, rx) in &receivers {
            if rx.try_recv().is_ok() {
                done = true;
                break;
            }
        }
        if !done {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// BZPOPMAX key [key ...] timeout
pub async fn bzpopmax(db: &Db, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'bzpopmax' command".into());
    }
    let timeout = match parse_score(&args[args.len() - 1]) {
        Ok(v) => v,
        Err(e) => return RespValue::Error(e),
    };
    let key_count = args.len() - 1;

    // Fast path: try non-blocking first
    for i in 0..key_count {
        let key = match str_from_bytes(&args[i]) {
            Ok(v) => v,
            Err(e) => return RespValue::Error(e),
        };
        if let Some(mut zset) = get_zset(db, key) {
            if zset.is_empty() {
                continue;
            }
            let mut entries = zset_entries(&zset);
            entries.reverse();
            if let Some((m, s)) = entries.first() {
                let m = m.clone();
                let s = *s;
                zset.members.remove(&m);
                rebuild_scores(&mut zset);
                set_zset(db, key, zset);
                return RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(Bytes::from(key.to_string()))),
                    RespValue::BulkString(Some(m)),
                    RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                ]));
            }
        }
    }

    // Timeout 0 means block forever
    if timeout == 0.0 {
        return bzpopmax_block_forever(db, args, key_count).await;
    }

    // Blocking path with timeout
    let dur = Duration::from_secs_f64(timeout);
    let result = tokio::time::timeout(dur, bzpopmax_block_forever(db, args, key_count)).await;
    match result {
        Ok(v) => v,
        Err(_) => RespValue::BulkString(None),
    }
}

async fn bzpopmax_block_forever(db: &Db, args: &[Bytes], key_count: usize) -> RespValue {
    let mut receivers: Vec<(String, std::sync::mpsc::Receiver<()>)> = Vec::new();
    for i in 0..key_count {
        let key = str_from_bytes(&args[i]).unwrap_or_default().to_string();
        receivers.push((key.clone(), db.watch(&Bytes::from(key))));
    }

    loop {
        for (key, _) in &receivers {
            if let Some(mut zset) = get_zset(db, key) {
                if !zset.is_empty() {
                    let mut entries = zset_entries(&zset);
                    entries.reverse();
                    if let Some((m, s)) = entries.first() {
                        let m = m.clone();
                        let s = *s;
                        zset.members.remove(&m);
                        rebuild_scores(&mut zset);
                        set_zset(db, key, zset);
                        return RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(Bytes::from(key.clone()))),
                            RespValue::BulkString(Some(m)),
                            RespValue::BulkString(Some(Bytes::from(format!("{s}")))),
                        ]));
                    }
                }
            }
        }

        let mut done = false;
        for (_, rx) in &receivers {
            if rx.try_recv().is_ok() {
                done = true;
                break;
            }
        }
        if !done {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Db {
        Arc::new(Store {
            keyspace: ::dashmap::DashMap::new(),
            evicted_keys: ::std::sync::atomic::AtomicU64::new(0),
            evict_config: ::std::sync::RwLock::new(valkey_storage::EvictionConfig::default()),
            watchers: ::dashmap::DashMap::new(),
            dirty_count: ::std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn db_with_zset(pairs: Vec<(&str, f64)>) -> Db {
        let db = test_store();
        let mut zset = ZSetData::new();
        for (member, score) in pairs {
            zset.add(Bytes::from(member.to_string()), score);
        }
        set_zset(&db, "myset", zset);
        db
    }

    fn bs(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[tokio::test]
    async fn test_zadd_basic() {
        let db = test_store();
        let args = vec![bs("myset"), bs("1.0"), bs("a"), bs("2.0"), bs("b")];
        let result = zadd(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(get_zset(&db, "myset").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_zadd_nx() {
        let db = db_with_zset(vec![("a", 1.0)]);
        let args = vec![
            bs("myset"),
            bs("NX"),
            bs("1.5"),
            bs("a"),
            bs("2.0"),
            bs("b"),
        ];
        let result = zadd(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            get_zset(&db, "myset")
                .unwrap()
                .members
                .get(&bs("a"))
                .unwrap()
                .0,
            1.0
        );
        assert_eq!(
            get_zset(&db, "myset")
                .unwrap()
                .members
                .get(&bs("b"))
                .unwrap()
                .0,
            2.0
        );
    }

    #[tokio::test]
    async fn test_zadd_xx() {
        let db = db_with_zset(vec![("a", 1.0)]);
        let args = vec![
            bs("myset"),
            bs("XX"),
            bs("3.0"),
            bs("a"),
            bs("2.0"),
            bs("b"),
        ];
        let result = zadd(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            get_zset(&db, "myset")
                .unwrap()
                .members
                .get(&bs("a"))
                .unwrap()
                .0,
            3.0
        );
        assert!(get_zset(&db, "myset")
            .unwrap()
            .members
            .get(&bs("b"))
            .is_none());
    }

    #[tokio::test]
    async fn test_zadd_incr() {
        let db = db_with_zset(vec![("a", 1.0)]);
        let args = vec![bs("myset"), bs("INCR"), bs("2.5"), bs("a")];
        let result = zadd(&db, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(bs("3.5"))));
    }

    #[tokio::test]
    async fn test_zscore_existing() {
        let db = db_with_zset(vec![("a", 1.5)]);
        let args = vec![bs("myset"), bs("a")];
        let result = zscore(&db, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(bs("1.5"))));
    }

    #[tokio::test]
    async fn test_zscore_missing() {
        let db = db_with_zset(vec![("a", 1.5)]);
        let args = vec![bs("myset"), bs("z")];
        let result = zscore(&db, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_zmscore() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0)]);
        let args = vec![bs("myset"), bs("a"), bs("b"), bs("c")];
        let result = zmscore(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("1"))),
                RespValue::BulkString(Some(bs("2"))),
                RespValue::BulkString(None)
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrank() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("b")];
        let result = zrank(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_zrevrank() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("b")];
        let result = zrevrank(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_zrange_basic() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("0"), bs("2")];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrange_withscores() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0)]);
        let args = vec![bs("myset"), bs("0"), bs("1"), bs("WITHSCORES")];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("1"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("2")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrange_negative() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("-2"), bs("-1")];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrange_byscore() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("1"), bs("2"), bs("BYSCORE")];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("b")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrange_byscore_limit() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);
        let args = vec![
            bs("myset"),
            bs("-inf"),
            bs("+inf"),
            bs("BYSCORE"),
            bs("LIMIT"),
            bs("1"),
            bs("2"),
        ];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrange_bylex() {
        let db = db_with_zset(vec![("a", 0.0), ("b", 0.0), ("c", 0.0)]);
        let args = vec![bs("myset"), bs("-"), bs("+"), bs("BYLEX")];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrevrange() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("0"), bs("2")];
        let result = zrevrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("c"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("a")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrangebyscore() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]);
        let args = vec![bs("myset"), bs("2"), bs("3")];
        let result = zrangebyscore(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zrevrangebyscore() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("3"), bs("1")];
        let result = zrevrangebyscore(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("c"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("a")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zcount() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("1"), bs("2")];
        let result = zcount(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zlexcount() {
        let db = db_with_zset(vec![("a", 0.0), ("b", 0.0), ("c", 0.0)]);
        let args = vec![bs("myset"), bs("-"), bs("+")];
        let result = zlexcount(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[tokio::test]
    async fn test_zrem() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("a"), bs("c")];
        let result = zrem(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(get_zset(&db, "myset").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_zremrangebyrank() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("0"), bs("1")];
        let result = zremrangebyrank(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zremrangebyscore() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("1"), bs("2")];
        let result = zremrangebyscore(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zremrangebylex() {
        let db = db_with_zset(vec![("a", 0.0), ("b", 0.0), ("c", 0.0)]);
        let args = vec![bs("myset"), bs("-"), bs("[b")];
        let result = zremrangebylex(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zcard() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0)]);
        let args = vec![bs("myset")];
        assert_eq!(zcard(&db, &args).unwrap(), RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zincrby() {
        let db = db_with_zset(vec![("a", 1.0)]);
        let args = vec![bs("myset"), bs("2.5"), bs("a")];
        let result = zincrby(&db, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(bs("3.5"))));
    }

    #[tokio::test]
    async fn test_zpopmin() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset")];
        let result = zpopmin(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("1")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zpopmax() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset")];
        let result = zpopmax(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("c"))),
                RespValue::BulkString(Some(bs("3")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zunionstore() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            z2.add(bs("c"), 4.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("dest"), bs("2"), bs("z1"), bs("z2")];
        let result = zunionstore(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let d = get_zset(&db, "dest").unwrap();
        assert_eq!(d.members.get(&bs("b")).unwrap().0, 5.0);
    }

    #[tokio::test]
    async fn test_zinterstore() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            z2.add(bs("c"), 4.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("dest"), bs("2"), bs("z1"), bs("z2")];
        let result = zinterstore(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_zdiffstore() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            z1.add(bs("c"), 3.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 2.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("dest"), bs("2"), bs("z1"), bs("z2")];
        let result = zdiffstore(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zscan() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("myset"), bs("0"), bs("COUNT"), bs("2")];
        let result = zscan(&db, &args).unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString(Some(bs("2"))));
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_zrandmember() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0)]);
        let args = vec![bs("myset")];
        let result = zrandmember(&db, &args).unwrap();
        assert!(matches!(result, RespValue::BulkString(Some(_))));
    }

    #[tokio::test]
    async fn test_zrangestore() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
        let args = vec![bs("dest"), bs("myset"), bs("0"), bs("1")];
        let result = zrangestore(&db, &args).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_tie_breaking() {
        let db = db_with_zset(vec![("c", 1.0), ("a", 1.0), ("b", 1.0)]);
        let args = vec![bs("myset"), bs("0"), bs("2")];
        let result = zrange(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("c")))
            ]))
        );
    }

    #[tokio::test]
    async fn test_zincrby_negative() {
        let db = db_with_zset(vec![("a", 5.0)]);
        let args = vec![bs("myset"), bs("-3"), bs("a")];
        let result = zincrby(&db, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(bs("2"))));
    }

    #[tokio::test]
    async fn test_zdiff_basic() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            z1.add(bs("c"), 3.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 2.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2")];
        let result = zdiff(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("c"))),
            ]))
        );
    }

    #[tokio::test]
    async fn test_zdiff_withscores() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            z1.add(bs("c"), 3.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 2.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2"), bs("WITHSCORES")];
        let result = zdiff(&db, &args).unwrap();
        // diff = {a, c} (b is in z2)
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("1"))),
                RespValue::BulkString(Some(bs("c"))),
                RespValue::BulkString(Some(bs("3"))),
            ]))
        );
    }

    #[tokio::test]
    async fn test_zdiff_no_common() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 2.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2")];
        let result = zdiff(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::BulkString(Some(bs("a")))]))
        );
    }

    #[tokio::test]
    async fn test_zinter_basic() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            z2.add(bs("c"), 4.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2")];
        let result = zinter(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::BulkString(Some(bs("b")))]))
        );
    }

    #[tokio::test]
    async fn test_zinter_withscores() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            z2.add(bs("c"), 4.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2"), bs("WITHSCORES")];
        let result = zinter(&db, &args).unwrap();
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("5"))),
            ]))
        );
    }

    #[tokio::test]
    async fn test_zinter_weights() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![
            bs("2"),
            bs("z1"),
            bs("z2"),
            bs("WEIGHTS"),
            bs("2"),
            bs("3"),
            bs("WITHSCORES"),
        ];
        let result = zinter(&db, &args).unwrap();
        // b: 2*2 + 3*3 = 13
        match &result {
            RespValue::Array(Some(r)) => {
                assert_eq!(r.len(), 2); // member + score
                assert_eq!(r[0], RespValue::BulkString(Some(bs("b"))));
                assert_eq!(r[1], RespValue::BulkString(Some(bs("13"))));
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_zinter_aggregate_min() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 10.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 20.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![
            bs("2"),
            bs("z1"),
            bs("z2"),
            bs("AGGREGATE"),
            bs("MIN"),
            bs("WITHSCORES"),
        ];
        let result = zinter(&db, &args).unwrap();
        // b: min(10, 20) = 10
        match &result {
            RespValue::Array(Some(r)) => {
                assert_eq!(r[0], RespValue::BulkString(Some(bs("b"))));
                assert_eq!(r[1], RespValue::BulkString(Some(bs("10"))));
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_zunion_basic() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            z2.add(bs("c"), 4.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2")];
        let result = zunion(&db, &args).unwrap();
        // sorted by score: a:1, c:4, b:5
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("c"))),
                RespValue::BulkString(Some(bs("b"))),
            ]))
        );
    }

    #[tokio::test]
    async fn test_zunion_withscores() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        {
            let mut z2 = ZSetData::new();
            z2.add(bs("b"), 3.0);
            z2.add(bs("c"), 4.0);
            set_zset(&db, "z2", z2);
        }
        let args = vec![bs("2"), bs("z1"), bs("z2"), bs("WITHSCORES")];
        let result = zunion(&db, &args).unwrap();
        // sorted by score: a:1, c:4, b:5
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("1"))),
                RespValue::BulkString(Some(bs("c"))),
                RespValue::BulkString(Some(bs("4"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("5"))),
            ]))
        );
    }

    #[tokio::test]
    async fn test_zmpop_min() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            z1.add(bs("c"), 3.0);
            set_zset(&db, "z1", z1);
        }
        let args = vec![bs("1"), bs("z1"), bs("MIN")];
        let result = zmpop(&db, &args).unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString(Some(bs("z1"))));
                match &items[1] {
                    RespValue::Array(Some(members)) => {
                        assert_eq!(members.len(), 2); // one member + score
                        assert_eq!(members[0], RespValue::BulkString(Some(bs("a"))));
                        assert_eq!(members[1], RespValue::BulkString(Some(bs("1"))));
                    }
                    _ => panic!("Expected array of members"),
                }
            }
            _ => panic!("Expected array"),
        }
        // Verify "a" was removed
        assert_eq!(get_zset(&db, "z1").unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_zmpop_max() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            z1.add(bs("c"), 3.0);
            set_zset(&db, "z1", z1);
        }
        let args = vec![bs("1"), bs("z1"), bs("MAX")];
        let result = zmpop(&db, &args).unwrap();
        match &result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items[0], RespValue::BulkString(Some(bs("z1"))));
                match &items[1] {
                    RespValue::Array(Some(members)) => {
                        assert_eq!(members[0], RespValue::BulkString(Some(bs("c"))));
                        assert_eq!(members[1], RespValue::BulkString(Some(bs("3"))));
                    }
                    _ => panic!("Expected array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_zmpop_count() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            z1.add(bs("c"), 3.0);
            set_zset(&db, "z1", z1);
        }
        let args = vec![bs("1"), bs("z1"), bs("MIN"), bs("COUNT"), bs("2")];
        let result = zmpop(&db, &args).unwrap();
        match &result {
            RespValue::Array(Some(items)) => {
                match &items[1] {
                    RespValue::Array(Some(members)) => {
                        assert_eq!(members.len(), 4); // 2 members * 2 (member + score)
                    }
                    _ => panic!("Expected array"),
                }
            }
            _ => panic!("Expected array"),
        }
        assert_eq!(get_zset(&db, "z1").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_zmpop_empty() {
        let db = test_store();
        let args = vec![bs("1"), bs("nonexistent"), bs("MIN")];
        let result = zmpop(&db, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_bzpopmin_fast_path() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0)]);
        let args = vec![bs("myset"), bs("0")];
        let result = bzpopmin(&db, &args).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("myset"))),
                RespValue::BulkString(Some(bs("a"))),
                RespValue::BulkString(Some(bs("1"))),
            ]))
        );
        assert_eq!(get_zset(&db, "myset").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_bzpopmax_fast_path() {
        let db = db_with_zset(vec![("a", 1.0), ("b", 2.0)]);
        let args = vec![bs("myset"), bs("0")];
        let result = bzpopmax(&db, &args).await;
        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(bs("myset"))),
                RespValue::BulkString(Some(bs("b"))),
                RespValue::BulkString(Some(bs("2"))),
            ]))
        );
        assert_eq!(get_zset(&db, "myset").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_bzpopmin_timeout() {
        let db = test_store();
        let args = vec![bs("nonexistent"), bs("0.1")];
        let result = bzpopmin(&db, &args).await;
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_bzpopmax_timeout() {
        let db = test_store();
        let args = vec![bs("nonexistent"), bs("0.1")];
        let result = bzpopmax(&db, &args).await;
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_bzmpop_fast_path() {
        let db = test_store();
        {
            let mut z1 = ZSetData::new();
            z1.add(bs("a"), 1.0);
            z1.add(bs("b"), 2.0);
            set_zset(&db, "z1", z1);
        }
        let args = vec![bs("0"), bs("1"), bs("z1"), bs("MIN")];
        let result = bzmpop(&db, &args).await;
        match &result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items[0], RespValue::BulkString(Some(bs("z1"))));
                match &items[1] {
                    RespValue::Array(Some(members)) => {
                        assert_eq!(members[0], RespValue::BulkString(Some(bs("a"))));
                        assert_eq!(members[1], RespValue::BulkString(Some(bs("1"))));
                    }
                    _ => panic!("Expected array"),
                }
            }
            _ => panic!("Expected array, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_bzmpop_timeout() {
        let db = test_store();
        let args = vec![bs("0.1"), bs("1"), bs("nonexistent"), bs("MIN")];
        let result = bzmpop(&db, &args).await;
        assert_eq!(result, RespValue::BulkString(None));
    }
}
