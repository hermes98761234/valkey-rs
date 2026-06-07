use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::Arc;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store, ZSetData};

pub type Db = Arc<Store>;

// ---------------------------------------------------------------------------
// Geohash encoding / decoding (52-bit interleaved lat/lon)
// ---------------------------------------------------------------------------
// Longitude: -180..180,  Latitude: -85.05112878..85.05112878
// Each axis gets 26 bits → interleaved into 52 bits stored as f64 score.

const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const BITS: u32 = 26;
const EARTH_RADIUS_M: f64 = 6372797.560856;

fn encode_axis(val: f64, min: f64, max: f64) -> u32 {
    let norm = (val - min) / (max - min);
    let steps = (1u64 << BITS) as f64;
    (norm * steps).min(steps - 1.0) as u32
}

fn decode_axis(bits: u32, min: f64, max: f64) -> f64 {
    let steps = (1u64 << BITS) as f64;
    let norm = (bits as f64 + 0.5) / steps;
    min + norm * (max - min)
}

fn interleave(x: u32, y: u32) -> u64 {
    let mut r: u64 = 0;
    for i in 0..26u64 {
        r |= (((x as u64 >> i) & 1) << (2 * i + 1)) | (((y as u64 >> i) & 1) << (2 * i));
    }
    r
}

fn deinterleave(v: u64) -> (u32, u32) {
    let mut x: u32 = 0;
    let mut y: u32 = 0;
    for i in 0..26u32 {
        x |= (((v >> (2 * i as u64 + 1)) & 1) as u32) << i;
        y |= (((v >> (2 * i as u64)) & 1) as u32) << i;
    }
    (x, y)
}

pub fn encode_geohash(lon: f64, lat: f64) -> f64 {
    let lon_bits = encode_axis(lon, GEO_LON_MIN, GEO_LON_MAX);
    let lat_bits = encode_axis(lat, GEO_LAT_MIN, GEO_LAT_MAX);
    interleave(lon_bits, lat_bits) as f64
}

pub fn decode_geohash(score: f64) -> (f64, f64) {
    let v = score as u64;
    let (lon_bits, lat_bits) = deinterleave(v);
    let lon = decode_axis(lon_bits, GEO_LON_MIN, GEO_LON_MAX);
    let lat = decode_axis(lat_bits, GEO_LAT_MIN, GEO_LAT_MAX);
    (lon, lat)
}

// Base32 alphabet used by Redis geohash strings
const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

fn encode_geohash_string(lon: f64, lat: f64) -> String {
    // Produce an 11-character geohash string (55 bits; we use the 52-bit score as base)
    let mut lon_range = (GEO_LON_MIN, GEO_LON_MAX);
    let mut lat_range = (GEO_LAT_MIN, GEO_LAT_MAX);
    let mut result = String::with_capacity(11);
    let mut bits = 0u8;
    let mut bit_count = 0u8;
    let mut is_lon = true;

    for _ in 0..55 {
        if is_lon {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                bits = (bits << 1) | 1;
                lon_range.0 = mid;
            } else {
                bits <<= 1;
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                bits = (bits << 1) | 1;
                lat_range.0 = mid;
            } else {
                bits <<= 1;
                lat_range.1 = mid;
            }
        }
        is_lon = !is_lon;
        bit_count += 1;
        if bit_count == 5 {
            result.push(BASE32[bits as usize] as char);
            bits = 0;
            bit_count = 0;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Haversine distance (meters)
// ---------------------------------------------------------------------------

pub fn haversine_dist(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let to_rad = |d: f64| d * std::f64::consts::PI / 180.0;
    let dlat = to_rad(lat2 - lat1);
    let dlon = to_rad(lon2 - lon1);
    let a = (dlat / 2.0).sin().powi(2)
        + to_rad(lat1).cos() * to_rad(lat2).cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * c
}

fn dist_in_unit(meters: f64, unit: &str) -> Option<f64> {
    match unit.to_lowercase().as_str() {
        "m" => Some(meters),
        "km" => Some(meters / 1000.0),
        "mi" => Some(meters / 1609.344),
        "ft" => Some(meters * 3.28084),
        _ => None,
    }
}

fn meters_from_unit(val: f64, unit: &str) -> Option<f64> {
    match unit.to_lowercase().as_str() {
        "m" => Some(val),
        "km" => Some(val * 1000.0),
        "mi" => Some(val * 1609.344),
        "ft" => Some(val / 3.28084),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ZSet helpers
// ---------------------------------------------------------------------------

fn get_zset(db: &Db, key: &[u8]) -> Option<ZSetData> {
    let k = Bytes::copy_from_slice(key);
    let entry = db.get(&k)?;
    match &entry.data {
        DataType::ZSet(zs) => Some(zs.clone()),
        _ => None,
    }
}

fn set_zset(db: &Db, key: &[u8], zset: ZSetData) {
    db.set(Bytes::copy_from_slice(key), DataType::ZSet(zset), None);
}

fn get_or_create_zset(db: &Db, key: &[u8]) -> ZSetData {
    let k = Bytes::copy_from_slice(key);
    let entry = db.get(&k);
    match entry {
        Some(e) => match &e.data {
            DataType::ZSet(zs) => zs.clone(),
            _ => ZSetData::default(),
        },
        None => ZSetData::default(),
    }
}

fn sb(s: impl Into<String>) -> Bytes {
    Bytes::from(s.into())
}

fn fmt_f64(v: f64) -> Bytes {
    Bytes::from(format!("{}", v))
}

// ---------------------------------------------------------------------------
// GEOADD key [NX|XX] [CH] longitude latitude member [...]
// ---------------------------------------------------------------------------

pub fn geoadd(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 4 {
        return Err("ERR wrong number of arguments for 'geoadd' command".into());
    }
    let key = &args[0];
    let mut idx = 1;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    while idx < args.len() {
        match str_arg(&args[idx]).to_ascii_uppercase().as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "CH" => {
                ch = true;
                idx += 1;
            }
            _ => break,
        }
    }
    if nx && xx {
        return Err("ERR XX and NX options at the same time are not compatible".into());
    }
    let remaining = args.len() - idx;
    if remaining < 3 || remaining % 3 != 0 {
        return Err("ERR syntax error".into());
    }
    let mut zset = get_or_create_zset(db, key);
    let mut added = 0i64;
    let mut changed = 0i64;
    while idx + 2 < args.len() {
        let lon: f64 = str_arg(&args[idx])
            .parse()
            .map_err(|_| "ERR invalid longitude")?;
        let lat: f64 = str_arg(&args[idx + 1])
            .parse()
            .map_err(|_| "ERR invalid latitude")?;
        if !(GEO_LON_MIN..=GEO_LON_MAX).contains(&lon) {
            return Err("ERR invalid longitude".into());
        }
        if !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
            return Err("ERR invalid latitude".into());
        }
        let member = args[idx + 2].clone();
        let score = encode_geohash(lon, lat);
        let is_new = !zset.members.contains_key(&member);
        if (nx && !is_new) || (xx && is_new) {
            idx += 3;
            continue;
        }
        let old_score = zset.members.get(&member).map(|s| s.0);
        zset.add(member, score);
        if is_new {
            added += 1;
            changed += 1;
        } else if old_score != Some(score) {
            changed += 1;
        }
        idx += 3;
    }
    set_zset(db, key, zset);
    Ok(RespValue::Integer(if ch { changed } else { added }))
}

// ---------------------------------------------------------------------------
// GEODIST key member1 member2 [m|km|mi|ft]
// ---------------------------------------------------------------------------

pub fn geodist(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 3 || args.len() > 4 {
        return Err("ERR wrong number of arguments for 'geodist' command".into());
    }
    let unit = if args.len() == 4 {
        str_arg(&args[3])
    } else {
        "m".into()
    };
    let zset = match get_zset(db, &args[0]) {
        Some(z) => z,
        None => return Ok(RespValue::BulkString(None)),
    };
    let s1 = match zset.members.get(&args[1]) {
        Some(s) => s.0,
        None => return Ok(RespValue::BulkString(None)),
    };
    let s2 = match zset.members.get(&args[2]) {
        Some(s) => s.0,
        None => return Ok(RespValue::BulkString(None)),
    };
    let (lon1, lat1) = decode_geohash(s1);
    let (lon2, lat2) = decode_geohash(s2);
    let dist = haversine_dist(lon1, lat1, lon2, lat2);
    let d = dist_in_unit(dist, &unit).ok_or("ERR unsupported unit")?;
    Ok(RespValue::BulkString(Some(fmt_f64(d))))
}

// ---------------------------------------------------------------------------
// GEOHASH key member [member ...]
// ---------------------------------------------------------------------------

pub fn geohash(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'geohash' command".into());
    }
    let zset = get_zset(db, &args[0]);
    let results = args[1..]
        .iter()
        .map(|member| {
            let score = zset
                .as_ref()
                .and_then(|z| z.members.get(member))
                .map(|s| s.0);
            match score {
                Some(s) => {
                    let (lon, lat) = decode_geohash(s);
                    RespValue::BulkString(Some(sb(encode_geohash_string(lon, lat))))
                }
                None => RespValue::BulkString(None),
            }
        })
        .collect();
    Ok(RespValue::Array(Some(results)))
}

// ---------------------------------------------------------------------------
// GEOPOS key member [member ...]
// ---------------------------------------------------------------------------

pub fn geopos(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'geopos' command".into());
    }
    let zset = get_zset(db, &args[0]);
    let results = args[1..]
        .iter()
        .map(|member| {
            let score = zset
                .as_ref()
                .and_then(|z| z.members.get(member))
                .map(|s| s.0);
            match score {
                Some(s) => {
                    let (lon, lat) = decode_geohash(s);
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(fmt_f64(lon))),
                        RespValue::BulkString(Some(fmt_f64(lat))),
                    ]))
                }
                None => RespValue::Array(None),
            }
        })
        .collect();
    Ok(RespValue::Array(Some(results)))
}

// ---------------------------------------------------------------------------
// GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat
//            BYRADIUS radius m|km|mi|ft | BYBOX w h m|km|mi|ft
//            [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
// ---------------------------------------------------------------------------

pub fn geosearch(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    geosearch_inner(db, args, None)
}

// GEOSEARCHSTORE dest source ...
pub fn geosearchstore(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'geosearchstore' command".into());
    }
    let dest = args[0].clone();
    let rest = &args[1..];
    geosearch_inner(db, rest, Some(&dest))
}

struct GeoSearchResult {
    member: Bytes,
    dist_m: f64,
    lon: f64,
    lat: f64,
    score: f64,
}

fn geosearch_inner(
    db: &Db,
    args: &[Bytes],
    store_dest: Option<&Bytes>,
) -> Result<RespValue, String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments".into());
    }
    let key = &args[0];
    let mut idx = 1;

    // Origin
    let (origin_lon, origin_lat) = {
        let kind = str_upper(&args[idx]);
        idx += 1;
        match kind.as_str() {
            "FROMMEMBER" => {
                let member = &args[idx];
                idx += 1;
                let zset = get_zset(db, key).ok_or(
                    "ERR could not perform this operation on a key holding the wrong kind of value",
                )?;
                let s = zset
                    .members
                    .get(member)
                    .ok_or("ERR could not decode requested zset member")?
                    .0;
                decode_geohash(s)
            }
            "FROMLONLAT" => {
                let lon: f64 = str_arg(&args[idx])
                    .parse()
                    .map_err(|_| "ERR invalid longitude")?;
                let lat: f64 = str_arg(&args[idx + 1])
                    .parse()
                    .map_err(|_| "ERR invalid latitude")?;
                idx += 2;
                (lon, lat)
            }
            _ => return Err("ERR syntax error".into()),
        }
    };

    // Shape
    enum Shape {
        Radius(f64),   // meters
        Box(f64, f64), // width, height in meters
    }
    let shape = {
        let kind = str_upper(&args[idx]);
        idx += 1;
        match kind.as_str() {
            "BYRADIUS" => {
                let r: f64 = str_arg(&args[idx])
                    .parse()
                    .map_err(|_| "ERR invalid radius")?;
                let unit = str_arg(&args[idx + 1]);
                idx += 2;
                let r_m = meters_from_unit(r, &unit).ok_or("ERR unsupported unit")?;
                Shape::Radius(r_m)
            }
            "BYBOX" => {
                let w: f64 = str_arg(&args[idx])
                    .parse()
                    .map_err(|_| "ERR invalid width")?;
                let h: f64 = str_arg(&args[idx + 1])
                    .parse()
                    .map_err(|_| "ERR invalid height")?;
                let unit = str_arg(&args[idx + 2]);
                idx += 3;
                let w_m = meters_from_unit(w, &unit).ok_or("ERR unsupported unit")?;
                let h_m = meters_from_unit(h, &unit).ok_or("ERR unsupported unit")?;
                Shape::Box(w_m, h_m)
            }
            _ => return Err("ERR syntax error".into()),
        }
    };

    // Options
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    let mut _any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;
    let mut storedist = false;

    while idx < args.len() {
        match str_upper(&args[idx]).as_str() {
            "ASC" => {
                asc = true;
                idx += 1;
            }
            "DESC" => {
                desc = true;
                idx += 1;
            }
            "COUNT" => {
                count = Some(
                    str_arg(&args[idx + 1])
                        .parse()
                        .map_err(|_| "ERR invalid count")?,
                );
                idx += 2;
                if idx < args.len() && str_upper(&args[idx]) == "ANY" {
                    _any = true;
                    idx += 1;
                }
            }
            "WITHCOORD" => {
                withcoord = true;
                idx += 1;
            }
            "WITHDIST" => {
                withdist = true;
                idx += 1;
            }
            "WITHHASH" => {
                withhash = true;
                idx += 1;
            }
            "STOREDIST" => {
                storedist = true;
                idx += 1;
            }
            _ => {
                idx += 1;
            }
        }
    }

    let zset = match get_zset(db, key) {
        Some(z) => z,
        None => {
            if store_dest.is_some() {
                return Ok(RespValue::Integer(0));
            }
            return Ok(RespValue::Array(Some(vec![])));
        }
    };

    // Filter members by shape
    let mut results: Vec<GeoSearchResult> = zset
        .members
        .iter()
        .filter_map(|(member, scored)| {
            let (lon, lat) = decode_geohash(scored.0);
            let dist_m = haversine_dist(origin_lon, origin_lat, lon, lat);
            let inside = match &shape {
                Shape::Radius(r) => dist_m <= *r,
                Shape::Box(w, h) => {
                    let dist_lon = haversine_dist(origin_lon, origin_lat, lon, origin_lat);
                    let dist_lat = haversine_dist(origin_lon, origin_lat, origin_lon, lat);
                    dist_lon <= w / 2.0 && dist_lat <= h / 2.0
                }
            };
            if inside {
                Some(GeoSearchResult {
                    member: member.clone(),
                    dist_m,
                    lon,
                    lat,
                    score: scored.0,
                })
            } else {
                None
            }
        })
        .collect();

    // Sort
    if asc {
        results.sort_by(|a, b| {
            a.dist_m
                .partial_cmp(&b.dist_m)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    } else if desc {
        results.sort_by(|a, b| {
            b.dist_m
                .partial_cmp(&a.dist_m)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    if let Some(c) = count {
        results.truncate(c);
    }

    // If storing results
    if let Some(dest) = store_dest {
        let mut dest_zset = ZSetData::default();
        for r in &results {
            let score = if storedist { r.dist_m } else { r.score };
            dest_zset.add(r.member.clone(), score);
        }
        let count = dest_zset.members.len() as i64;
        set_zset(db, dest, dest_zset);
        return Ok(RespValue::Integer(count));
    }

    // Build reply
    let reply = results
        .iter()
        .map(|r| {
            let only_name = !withcoord && !withdist && !withhash;
            if only_name {
                return RespValue::BulkString(Some(r.member.clone()));
            }
            let mut arr = vec![RespValue::BulkString(Some(r.member.clone()))];
            if withdist {
                arr.push(RespValue::BulkString(Some(fmt_f64(r.dist_m))));
            }
            if withhash {
                arr.push(RespValue::Integer(r.score as i64));
            }
            if withcoord {
                arr.push(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(fmt_f64(r.lon))),
                    RespValue::BulkString(Some(fmt_f64(r.lat))),
                ])));
            }
            RespValue::Array(Some(arr))
        })
        .collect();

    Ok(RespValue::Array(Some(reply)))
}

// ---------------------------------------------------------------------------
// GEORADIUS key longitude latitude radius m|km|mi|ft [options...] (deprecated)
// ---------------------------------------------------------------------------

pub fn georadius(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 5 {
        return Err("ERR wrong number of arguments for 'georadius' command".into());
    }
    // Rewrite as GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit [options]
    let key = args[0].clone();
    let lon = args[1].clone();
    let lat = args[2].clone();
    let radius = args[3].clone();
    let unit = args[4].clone();
    let opts = &args[5..];
    let mut new_args = vec![
        key,
        sb("FROMLONLAT"),
        lon,
        lat,
        sb("BYRADIUS"),
        radius,
        unit,
    ];
    new_args.extend_from_slice(opts);
    geosearch(db, &new_args)
}

// GEORADIUS ... STORE dest  (store variant — handled inside georadius args via STORE option)
// We expose georadius_store separately for dispatch
pub fn georadius_store(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    georadius(db, args)
}

// GEORADIUSBYMEMBER key member radius unit [options...] (deprecated)
pub fn georadiusbymember(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len() < 4 {
        return Err("ERR wrong number of arguments for 'georadiusbymember' command".into());
    }
    let key = args[0].clone();
    let member = args[1].clone();
    let radius = args[2].clone();
    let unit = args[3].clone();
    let opts = &args[4..];
    let mut new_args = vec![key, sb("FROMMEMBER"), member, sb("BYRADIUS"), radius, unit];
    new_args.extend_from_slice(opts);
    geosearch(db, &new_args)
}

pub fn georadiusbymember_store(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    georadiusbymember(db, args)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn str_arg(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn str_upper(b: &Bytes) -> String {
    str_arg(b).to_ascii_uppercase()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn handle(cmd: &[Bytes], db: &Db) -> RespValue {
    if cmd.is_empty() {
        return RespValue::Error("ERR empty command".into());
    }
    let name = String::from_utf8_lossy(&cmd[0]).to_ascii_uppercase();
    let args = &cmd[1..];
    let r = match name.as_str() {
        "GEOADD" => geoadd(db, args),
        "GEODIST" => geodist(db, args),
        "GEOHASH" => geohash(db, args),
        "GEOPOS" => geopos(db, args),
        "GEOSEARCH" => geosearch(db, args),
        "GEOSEARCHSTORE" => geosearchstore(db, args),
        "GEORADIUS" | "GEORADIUS_RO" => georadius(db, args),
        "GEORADIUSBYMEMBER" | "GEORADIUSBYMEMBER_RO" => georadiusbymember(db, args),
        _ => return RespValue::Error(format!("ERR unknown geo command `{}`", name).into()),
    };
    match r {
        Ok(v) => v,
        Err(e) => RespValue::Error(e.into()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let cases = [
            (2.3522, 48.8566),   // Paris
            (-0.1276, 51.5074),  // London
            (-74.006, 40.7128),  // New York
            (139.6917, 35.6895), // Tokyo
            (0.0, 0.0),
        ];
        for (lon, lat) in cases {
            let score = encode_geohash(lon, lat);
            let (dlon, dlat) = decode_geohash(score);
            assert!((dlon - lon).abs() < 0.001, "lon: {} vs {}", dlon, lon);
            assert!((dlat - lat).abs() < 0.001, "lat: {} vs {}", dlat, lat);
        }
    }

    #[test]
    fn test_haversine_paris_london() {
        // ~341 km
        let dist = haversine_dist(2.3522, 48.8566, -0.1276, 51.5074);
        let km = dist / 1000.0;
        assert!((km - 341.0).abs() < 5.0, "Paris-London: {} km", km);
    }
}
