use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use valkey_proto::RespValue;
use valkey_storage::{Consumer, ConsumerGroup, DataType, PendingEntry, StreamData, StreamId, Store};

pub type Db = Arc<Store>;

fn s(b: &Bytes) -> Result<&str, String> { std::str::from_utf8(b).map_err(|_| "ERR invalid utf-8".into()) }
fn parse_id(st: &str) -> Result<StreamId, String> {
    let p: Vec<&str> = st.splitn(2,'-').collect();
    if p.len()!=2 { return Err("ERR Invalid stream ID".into()); }
    Ok(StreamId::new(p[0].parse().map_err(|_| "ERR Invalid stream ID".into())?, p[1].parse().map_err(|_| "ERR Invalid stream ID".into())?))
}
fn get_stream(db: &Db, key: &Bytes) -> Result<StreamData, String> {
    match db.keyspace.get_mut(key) {
        Some(mut e) => match &mut e.data { DataType::Stream(sd) => Ok(sd.clone()), _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()) },
        None => Ok(StreamData::new()),
    }
}
fn save_stream(db: &Db, key: &Bytes, sd: StreamData) {
    db.keyspace.entry(key.clone()).and_modify(|e| e.data = DataType::Stream(sd.clone()))
        .or_insert_with(|| valkey_storage::Entry::new(DataType::Stream(sd.clone()), None));
}

pub fn xadd(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    if args.len()<3 { return Err("ERR wrong number of arguments for 'xadd' command".into()); }
    let key=&args[0]; let mut i=1; let mut nomk=false; let mut maxlen:Option<(bool,u64)>=None;
    while i<args.len() {
        let o=s(&args[i])?.to_ascii_uppercase();
        match o.as_str() {
            "NOMKSTREAM" => { nomk=true; i+=1; }
            "MAXLEN"|"MINID" => {
                let is_max=o=="MAXLEN"; i+=1;
                if i>=args.len() { return Err("ERR syntax error".into()); }
                if s(&args[i])?=="~" { i+=1; }
                if i>=args.len() { return Err("ERR syntax error".into()); }
                let t:u64=s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?;
                maxlen=Some((is_max,t)); i+=1;
                if i<args.len() && s(&args[i])?.to_ascii_uppercase()=="LIMIT" { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } i+=1; }
            }
            _ => break,
        }
    }
    if i>=args.len() { return Err("ERR syntax error".into()); }
    let id_str=s(&args[i])?; i+=1;
    let req=if id_str=="*" { None } else { Some(parse_id(id_str)?) };
    let rem=&args[i..];
    if rem.is_empty()||rem.len()%2!=0 { return Err("ERR wrong number of arguments for 'xadd' command".into()); }
    let mut f=Vec::new(); let mut j=0;
    while j<rem.len() { f.push((rem[j].clone(),rem[j+1].clone())); j+=2; }
    if nomk&&!db.exists(key) { return Ok(RespValue::BulkString(None)); }
    let mut sd=get_stream(db,key)?; let id=sd.generate_id(req)?; sd.add(id,f);
    if let Some((is_max,threshold))=maxlen {
        if is_max { let cl=sd.entries.len(); if cl>threshold as usize { let tr=cl-threshold as usize; let ids:Vec<StreamId>=sd.entries.keys().take(tr).copied().collect(); for id in ids { sd.entries.remove(&id); } } }
        else { let min_ms=threshold; let tr:Vec<StreamId>=sd.entries.keys().filter(|id|id.ms<min_ms).copied().collect(); for id in tr { sd.entries.remove(&id); } }
    }
    save_stream(db,key,sd);
    Ok(RespValue::BulkString(Some(Bytes::from(format!("{}-{}",id.ms,id.seq)))))
}

fn enc_fields(fields:&[(Bytes,Bytes)]) -> Vec<RespValue> { fields.iter().flat_map(|(k,v)| vec![RespValue::BulkString(Some(k.clone())),RespValue::BulkString(Some(v.clone()))]).collect() }

pub fn xread(db: &Db, args: &[Bytes]) -> Result<RespValue, String> {
    let mut i=0; let mut count=None; let mut block_ms=None;
    while i<args.len() {
        match s(&args[i])?.to_ascii_uppercase().as_str() {
            "COUNT" => { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } count=Some(s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?); }
            "BLOCK" => { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } block_ms=Some(s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?); }
            "STREAMS" => break,
            _ => return Err("ERR syntax error".into()),
        } i+=1;
    }
    if i>=args.len()||s(&args[i])?.to_ascii_uppercase()!="STREAMS" { return Err("ERR syntax error".into()); } i+=1;
    let rem=args.len()-i; if rem==0||rem%2!=0 { return Err("ERR syntax error".into()); }
    let h=rem/2; let keys=&args[i..i+h]; let ids=&args[i+h..];
    if block_ms.is_none() { return xri(db,keys,ids,count); }
    let bd=Duration::from_millis(block_ms.unwrap()); let st=std::time::Instant::now();
    loop { let r=xri(db,keys,ids,count)?; if !matches!(r,RespValue::BulkString(None)) { return Ok(r); } if st.elapsed()>=bd { return Ok(RespValue::BulkString(None)); } std::thread::sleep(Duration::from_millis(10)); }
}

fn xri(db:&Db,keys:&[Bytes],ids:&[Bytes],count:Option<usize>)->Result<RespValue,String> {
    let mut res=Vec::new();
    for (ki,key) in keys.iter().enumerate() {
        let id_str=s(&ids[ki])?;
        let after=if id_str=="$" { match db.get(key) { Some(e)=>match &e.data{DataType::Stream(sd)=>Some(sd.last_id),_=>return Err("WRONGTYPE Operation against a key holding the wrong kind of value".into())}, None=>continue } } else { Some(parse_id(id_str)?) };
        if let Some(entry)=db.get(key) {
            if let DataType::Stream(sd)=&entry.data {
                let a=match after { Some(id)=>id, None=>continue };
                let mut entries=Vec::new();
                for (sid,fields) in sd.entries.range((std::ops::Bound::Excluded(&a),std::ops::Bound::Unbounded)) {
                    entries.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",sid.ms,sid.seq)))), RespValue::Array(enc_fields(fields))]));
                    if let Some(c)=count { if entries.len()>=c { break; } }
                }
                if !entries.is_empty() { res.push(RespValue::Array(vec![RespValue::BulkString(Some(key.clone())), RespValue::Array(entries)])); }
            }
        }
    }
    if res.is_empty() { Ok(RespValue::BulkString(None)) } else { Ok(RespValue::Array(res)) }
}

pub fn xrange(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<3 { return Err("ERR wrong number of arguments for 'xrange' command".into()); }
    let key=&args[0];
    let start=if s(&args[1])?=="-" { StreamId::new(0,0) } else { parse_id(s(&args[1])?)? };
    let end=if s(&args[2])?=="+" { StreamId::new(u64::MAX,u64::MAX) } else { parse_id(s(&args[2])?)? };
    let entry=db.get(key).ok_or("ERR no such key")?;
    match &entry.data {
        DataType::Stream(sd)=>{
            let mut r=Vec::new();
            for (sid,fields) in sd.entries.range(start..=end) { r.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",sid.ms,sid.seq)))), RespValue::Array(enc_fields(fields))])); }
            Ok(RespValue::Array(r))
        }
        _=>Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
    }
}

pub fn xrevrange(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<3 { return Err("ERR wrong number of arguments for 'xrevrange' command".into()); }
    let key=&args[0];
    let start=if s(&args[2])?=="-" { StreamId::new(0,0) } else { parse_id(s(&args[2])?)? };
    let end=if s(&args[1])?=="+" { StreamId::new(u64::MAX,u64::MAX) } else { parse_id(s(&args[1])?)? };
    let entry=db.get(key).ok_or("ERR no such key")?;
    match &entry.data {
        DataType::Stream(sd)=>{
            let mut r=Vec::new();
            for (sid,fields) in sd.entries.range(start..=end).rev() { r.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",sid.ms,sid.seq)))), RespValue::Array(enc_fields(fields))])); }
            Ok(RespValue::Array(r))
        }
        _=>Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
    }
}

pub fn xlen(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()!=1 { return Err("ERR wrong number of arguments for 'xlen' command".into()); }
    match db.get(&args[0]) {
        Some(e)=>match &e.data { DataType::Stream(sd)=>Ok(RespValue::Integer(sd.entries.len() as i64)), _=>Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()) },
        None=>Ok(RespValue::Integer(0)),
    }
}

pub fn xtrim(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<3 { return Err("ERR wrong number of arguments for 'xtrim' command".into()); }
    let key=&args[0]; let strat=s(&args[1])?.to_ascii_uppercase();
    let ti=if s(&args[2])?=="~" { 3 } else { 2 };
    if ti>=args.len() { return Err("ERR syntax error".into()); }
    let mut sd=get_stream(db,key)?;
    let removed=match strat.as_str() {
        "MAXLEN"=>{ let th:usize=s(&args[ti])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?; let c=sd.entries.len(); if c<=th { 0 } else { let tr=c-th; let ids:Vec<StreamId>=sd.entries.keys().take(tr).copied().collect(); for id in &ids { sd.entries.remove(id); } ids.len() as i64 } }
        "MINID"=>{ let min_id=parse_id(s(&args[ti])?)?; let tr:Vec<StreamId>=sd.entries.keys().filter(|id|*id<&min_id).copied().collect(); let c=tr.len(); for id in &tr { sd.entries.remove(id); } c as i64 }
        _=>return Err("ERR syntax error".into()),
    };
    save_stream(db,key,sd); Ok(RespValue::Integer(removed))
}

pub fn xdel(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<2 { return Err("ERR wrong number of arguments for 'xdel' command".into()); }
    let key=&args[0]; let mut sd=get_stream(db,key)?; let mut c=0i64;
    for i in 1..args.len() { let id=parse_id(s(&args[i])?)?; if sd.entries.remove(&id).is_some() { c+=1; } }
    save_stream(db,key,sd); Ok(RespValue::Integer(c))
}

pub fn xinfo(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<2 { return Err("ERR wrong number of arguments for 'xinfo' command".into()); }
    let sub=s(&args[0])?.to_ascii_uppercase(); let key=&args[1];
    match sub.as_str() {
        "STREAM"=>xinfo_stream(db,key),
        "GROUPS"=>xinfo_groups(db,key),
        "CONSUMERS"=>{ if args.len()<3 { return Err("ERR wrong number of arguments".into()); } xinfo_consumers(db,key,&args[2]) }
        _=>Err("ERR syntax error".into()),
    }
}

fn xinfo_stream(db:&Db,key:&Bytes)->Result<RespValue,String> {
    let entry=db.get(key).ok_or("ERR no such key")?;
    match &entry.data {
        DataType::Stream(sd)=>{
            let fi=sd.entries.keys().next().map(|f|format!("{}-{}",f.ms,f.seq)).unwrap_or_default();
            let li=sd.entries.keys().next_back().map(|l|format!("{}-{}",l.ms,l.seq)).unwrap_or_default();
            Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("length"))), RespValue::BulkString(Some(Bytes::from(sd.entries.len().to_string()))),
                RespValue::BulkString(Some(Bytes::from("last-generated-id"))), RespValue::BulkString(Some(Bytes::from(format!("{}-{}",sd.last_id.ms,sd.last_id.seq)))),
                RespValue::BulkString(Some(Bytes::from("first-entry"))), if fi.is_empty() { RespValue::BulkString(None) } else { RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(fi))), RespValue::Array(vec![])]) },
                RespValue::BulkString(Some(Bytes::from("last-entry"))), if li.is_empty() { RespValue::BulkString(None) } else { RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(li))), RespValue::Array(vec![])]) },
                RespValue::BulkString(Some(Bytes::from("groups"))), RespValue::BulkString(Some(Bytes::from(sd.groups.len().to_string()))),
            ]))
        }
        _=>Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
    }
}

fn xinfo_groups(db:&Db,key:&Bytes)->Result<RespValue,String> {
    let entry=db.get(key).ok_or("ERR no such key")?;
    match &entry.data {
        DataType::Stream(sd)=>{
            let mut groups=Vec::new();
            for (name,group) in &sd.groups {
                groups.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from("name"))), RespValue::BulkString(Some(Bytes::from(name.clone()))),
                    RespValue::BulkString(Some(Bytes::from("consumers"))), RespValue::BulkString(Some(Bytes::from(group.consumers.len().to_string()))),
                    RespValue::BulkString(Some(Bytes::from("pending"))), RespValue::BulkString(Some(Bytes::from(group.pending.len().to_string()))),
                    RespValue::BulkString(Some(Bytes::from("last-delivered-id"))), RespValue::BulkString(Some(Bytes::from(format!("{}-{}",group.last_delivered_id.ms,group.last_delivered_id.seq)))),
                ]));
            }
            Ok(RespValue::Array(groups))
        }
        _=>Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
    }
}

fn xinfo_consumers(db:&Db,key:&Bytes,gn:&Bytes)->Result<RespValue,String> {
    let gns=s(gn)?; let entry=db.get(key).ok_or("ERR no such key")?;
    match &entry.data { DataType::Stream(sd)=>{
        let g=sd.groups.get(gns).ok_or("ERR Consumer Group not found")?;
        let mut cs=Vec::new();
        for (name,consumer) in &g.consumers { cs.push(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from("name"))), RespValue::BulkString(Some(Bytes::from(name.clone()))),
            RespValue::BulkString(Some(Bytes::from("pending"))), RespValue::BulkString(Some(Bytes::from(consumer.pending.len().to_string()))),
            RespValue::BulkString(Some(Bytes::from("idle"))), RespValue::BulkString(Some(Bytes::from(consumer.seen_time.elapsed().as_millis().to_string()))),
        ])); } Ok(RespValue::Array(cs))
    } _=>Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()) }
}

pub fn xgroup(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<2 { return Err("ERR wrong number of arguments for 'xgroup' command".into()); }
    let sub=s(&args[0])?.to_ascii_uppercase(); let key=&args[1];
    match sub.as_str() {
        "CREATE"=>{ if args.len()<4 { return Err("ERR wrong number of arguments".into()); } xgc(db,key,&args[2],&args[3],&args[4..]) }
        "SETID"=>{ if args.len()<4 { return Err("ERR wrong number of arguments".into()); } xgsid(db,key,&args[2],&args[3],&args[4..]) }
        "DESTROY"=>{ if args.len()!=3 { return Err("ERR wrong number of arguments".into()); } xgd(db,key,&args[2]) }
        "CREATECONSUMER"=>{ if args.len()!=4 { return Err("ERR wrong number of arguments".into()); } xgcc(db,key,&args[2],&args[3]) }
        "DELCONSUMER"=>{ if args.len()!=4 { return Err("ERR wrong number of arguments".into()); } xgdc(db,key,&args[2],&args[3]) }
        _=>Err("ERR unknown subcommand".into()),
    }
}

fn xgc(db:&Db,key:&Bytes,gn:&Bytes,id:&Bytes,extra:&[Bytes])->Result<RespValue,String> {
    let gns=s(gn)?.to_string(); let ids=s(id)?;
    let er=if !extra.is_empty()&&s(&extra[0])?.to_ascii_uppercase()=="ENTRIESREAD" { if extra.len()>1 { s(&extra[1])?.parse::<i64>().map_err(|_| "ERR value is not an integer or out of range".into())? } else { return Err("ERR syntax error".into()); } } else { 0 };
    let mut sd=get_stream(db,key)?;
    let ldi=if ids=="$" { sd.last_id } else if ids=="0" { StreamId::new(0,0) } else { parse_id(ids)? };
    if sd.groups.contains_key(&gns) { return Err("BUSYGROUP Consumer Group name already exists".into()); }
    sd.groups.insert(gns.clone(),ConsumerGroup::new(gns,ldi,er)); save_stream(db,key,sd); Ok(RespValue::SimpleString("OK".into()))
}

fn xgsid(db:&Db,key:&Bytes,gn:&Bytes,id:&Bytes,extra:&[Bytes])->Result<RespValue,String> {
    let gns=s(gn)?; let ids=s(id)?;
    let er=if !extra.is_empty()&&s(&extra[0])?.to_ascii_uppercase()=="ENTRIESREAD" { if extra.len()>1 { s(&extra[1])?.parse::<i64>().map_err(|_| "ERR value is not an integer or out of range".into())? } else { return Err("ERR syntax error".into()); } } else { 0 };
    let mut sd=get_stream(db,key)?;
    let g=sd.groups.get_mut(gns).ok_or("ERR No such consumer group")?;
    g.last_delivered_id=if ids=="$" { sd.last_id } else { parse_id(ids)? }; g.entries_read=er; save_stream(db,key,sd); Ok(RespValue::SimpleString("OK".into()))
}

fn xgd(db:&Db,key:&Bytes,gn:&Bytes)->Result<RespValue,String> {
    let gns=s(gn)?; let mut sd=get_stream(db,key)?;
    if sd.groups.remove(gns).is_some() { save_stream(db,key,sd); Ok(RespValue::Integer(1)) } else { Ok(RespValue::Integer(0)) }
}

fn xgcc(db:&Db,key:&Bytes,gn:&Bytes,cn:&Bytes)->Result<RespValue,String> {
    let gns=s(gn)?; let cns=s(cn)?.to_string(); let mut sd=get_stream(db,key)?;
    let g=sd.groups.get_mut(gns).ok_or("ERR No such consumer group")?;
    g.consumers.entry(cns.clone()).or_insert_with(||Consumer::new(cns)); save_stream(db,key,sd); Ok(RespValue::Integer(1))
}

fn xgdc(db:&Db,key:&Bytes,gn:&Bytes,cn:&Bytes)->Result<RespValue,String> {
    let gns=s(gn)?; let cns=s(cn)?; let mut sd=get_stream(db,key)?;
    let g=sd.groups.get_mut(gns).ok_or("ERR No such consumer group")?;
    let r=g.consumers.remove(cns).is_some(); save_stream(db,key,sd); Ok(RespValue::Integer(if r{1}else{0}))
}

pub fn xreadgroup(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<5 { return Err("ERR wrong number of arguments for 'xreadgroup' command".into()); }
    if s(&args[0])?.to_ascii_uppercase()!="GROUP" { return Err("ERR syntax error".into()); }
    let gn=s(&args[1])?.to_string(); let cn=s(&args[2])?.to_string();
    let mut i=3; let mut count=None; let mut noack=false;
    while i<args.len() {
        match s(&args[i])?.to_ascii_uppercase().as_str() {
            "COUNT" => { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } count=Some(s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?); }
            "NOACK" => { noack=true; }
            "STREAMS" => break,
            _ => return Err("ERR syntax error".into()),
        } i+=1;
    }
    if i>=args.len()||s(&args[i])?.to_ascii_uppercase()!="STREAMS" { return Err("ERR syntax error".into()); } i+=1;
    let rem=args.len()-i; if rem==0||rem%2!=0 { return Err("ERR syntax error".into()); }
    let h=rem/2; let keys=&args[i..i+h]; let ids=&args[i+h..];
    let mut results=Vec::new();
    for (ki,key) in keys.iter().enumerate() {
        let id_str=s(&ids[ki])?;
        let mut sd=match get_stream(db,key) { Ok(sd)=>sd, Err(_)=>continue };
        let group=match sd.groups.get_mut(&gn) { Some(g)=>g, None=>continue };
        group.consumers.entry(cn.clone()).or_insert_with(||Consumer::new(cn.clone()));
        let mut entries=Vec::new();
        if id_str==">" {
            let after=group.last_delivered_id;
            for (sid,fields) in sd.entries.range((std::ops::Bound::Excluded(&after),std::ops::Bound::Unbounded)) {
                entries.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",sid.ms,sid.seq)))), RespValue::Array(enc_fields(fields))]));
                if !noack { group.pending.insert(*sid,PendingEntry::new(cn.clone())); }
                group.last_delivered_id=*sid;
                if let Some(c)=count { if entries.len()>=c { break; } }
            }
        } else {
            let pids:Vec<StreamId>=group.pending.keys().copied().collect();
            for sid in &pids {
                if let Some(fields)=sd.entries.get(sid) {
                    entries.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",sid.ms,sid.seq)))), RespValue::Array(enc_fields(fields))]));
                    if let Some(pe)=group.pending.get_mut(sid) { pe.delivery_count+=1; pe.delivered_at=std::time::Instant::now(); }
                }
                if let Some(c)=count { if entries.len()>=c { break; } }
            }
        }
        if !entries.is_empty() { results.push(RespValue::Array(vec![RespValue::BulkString(Some(key.clone())), RespValue::Array(entries)])); }
        save_stream(db,key,sd);
    }
    if results.is_empty() { Ok(RespValue::BulkString(None)) } else { Ok(RespValue::Array(results)) }
}

pub fn xack(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<3 { return Err("ERR wrong number of arguments for 'xack' command".into()); }
    let key=&args[0]; let gn=s(&args[1])?; let mut sd=get_stream(db,key)?;
    let g=sd.groups.get_mut(gn).ok_or("ERR No such consumer group")?;
    let mut c=0i64; for i in 2..args.len() { let id=parse_id(s(&args[i])?)?; if g.pending.remove(&id).is_some() { c+=1; } }
    save_stream(db,key,sd); Ok(RespValue::Integer(c))
}

pub fn xclaim(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<5 { return Err("ERR wrong number of arguments for 'xclaim' command".into()); }
    let key=&args[0]; let gn=s(&args[1])?; let cn=s(&args[2])?;
    let mi:u64=s(&args[3])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?;
    let mut i=4; let mut ids=Vec::new();
    while i<args.len() { let u=s(&args[i])?.to_ascii_uppercase(); if ["IDLE","TIME","RETRYCOUNT","FORCE","JUSTID"].contains(&u.as_str()) { break; } ids.push(parse_id(s(&args[i])?)?); i+=1; }
    let mut new_idle=None; let mut just_id=false;
    while i<args.len() {
        match s(&args[i])?.to_ascii_uppercase().as_str() {
            "IDLE"|"TIME" => { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } new_idle=Some(s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?); }
            "RETRYCOUNT" => { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } i+=1; }
            "JUSTID" => { just_id=true; }
            _ => {},
        } i+=1;
    }
    let mut sd=get_stream(db,key)?; let g=sd.groups.get_mut(gn).ok_or("ERR No such consumer group")?;
    let mut claimed=Vec::new();
    for id in &ids {
        if !sd.entries.contains_key(id) { continue; }
        if let Some(pe)=g.pending.get(id) { if pe.delivered_at.elapsed().as_millis() as u64<mi { continue; } }
        if let Some(mut pe)=g.pending.remove(id) {
            pe.consumer=cn.to_string();
            pe.delivered_at=if let Some(ni)=new_idle { std::time::Instant::now()-Duration::from_millis(ni) } else { std::time::Instant::now() };
            pe.delivery_count+=1; g.pending.insert(*id,pe);
        } else { g.pending.insert(*id,PendingEntry::new(cn.to_string())); }
        if just_id { claimed.push(RespValue::BulkString(Some(Bytes::from(format!("{}-{}",id.ms,id.seq))))); }
        else if let Some(fields)=sd.entries.get(id) { claimed.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",id.ms,id.seq)))), RespValue::Array(enc_fields(fields))])); }
    }
    save_stream(db,key,sd); Ok(RespValue::Array(claimed))
}

pub fn xpending(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<2 { return Err("ERR wrong number of arguments for 'xpending' command".into()); }
    let key=&args[0]; let gn=s(&args[1])?;
    let mut i=2; let mut min_idle=None;
    if i<args.len()&&s(&args[i])?.to_ascii_uppercase()=="IDLE" { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } min_idle=Some(s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?); i+=1; }
    let (si,ei,ct,cf)=if i<args.len() {
        let start=if s(&args[i])?=="-" { StreamId::new(0,0) } else { parse_id(s(&args[i])?)?.into() };
        i+=1; if i>=args.len() { return Err("ERR syntax error".into()); }
        let end=if s(&args[i])?=="+" { StreamId::new(u64::MAX,u64::MAX) } else { parse_id(s(&args[i])?)?.into() };
        i+=1; if i>=args.len() { return Err("ERR syntax error".into()); }
        let cnt:usize=s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?; i+=1;
        let c=if i<args.len() { Some(s(&args[i])?.to_string()) } else { None };
        (start,end,cnt,c)
    } else { (StreamId::new(0,0),StreamId::new(u64::MAX,u64::MAX),10,None) };
    let sd=get_stream(db,key)?;
    let g=sd.groups.get(gn).ok_or("ERR No such consumer group")?;
    let mut pl=Vec::new();
    for (id,pe) in g.pending.range(si..=ei) {
        if let Some(ref cf)=cf { if &pe.consumer!=cf { continue; } }
        if let Some(mi)=min_idle { if pe.delivered_at.elapsed().as_millis() as u64<mi { continue; } }
        pl.push(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from(format!("{}-{}",id.ms,id.seq)))),
            RespValue::BulkString(Some(Bytes::from(pe.consumer.clone()))),
            RespValue::BulkString(Some(Bytes::from(pe.delivered_at.elapsed().as_millis().to_string()))),
            RespValue::BulkString(Some(Bytes::from(pe.delivery_count.to_string()))),
        ]));
        if pl.len()>=ct { break; }
    }
    if i>2 { Ok(RespValue::Array(pl)) } else {
        let t=g.pending.len() as i64;
        let min_id=g.pending.keys().next().map(|id|format!("{}-{}",id.ms,id.seq)).unwrap_or_else(||"0-0".to_string());
        let max_id=g.pending.keys().next_back().map(|id|format!("{}-{}",id.ms,id.seq)).unwrap_or_else(||"0-0".to_string());
        let mut cc:HashMap<String,usize>=HashMap::new();
        for pe in g.pending.values() { *cc.entry(pe.consumer.clone()).or_default()+=1; }
        let ca:Vec<RespValue>=cc.into_iter().flat_map(|(n,c)|vec![RespValue::BulkString(Some(Bytes::from(n))),RespValue::BulkString(Some(Bytes::from(c.to_string())))]).collect();
        Ok(RespValue::Array(vec![RespValue::Integer(t),RespValue::BulkString(Some(Bytes::from(min_id))),RespValue::BulkString(Some(Bytes::from(max_id))),RespValue::Array(ca)]))
    }
}

pub fn xautoclaim(db:&Db,args:&[Bytes])->Result<RespValue,String> {
    if args.len()<5 { return Err("ERR wrong number of arguments for 'xautoclaim' command".into()); }
    let key=&args[0]; let gn=s(&args[1])?; let cn=s(&args[2])?;
    let mi:u64=s(&args[3])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?;
    let start=s(&args[4])?;
    let mut ct:usize=100; let mut just_id=false; let mut i=5;
    while i<args.len() {
        match s(&args[i])?.to_ascii_uppercase().as_str() {
            "COUNT" => { i+=1; if i>=args.len() { return Err("ERR syntax error".into()); } ct=s(&args[i])?.parse().map_err(|_| "ERR value is not an integer or out of range".into())?; }
            "JUSTID" => { just_id=true; }
            _ => {},
        } i+=1;
    }
    let sid=if start=="0-0" { StreamId::new(0,0) } else { parse_id(start)? };
    let mut sd=get_stream(db,key)?; let g=sd.groups.get_mut(gn).ok_or("ERR No such consumer group")?;
    let mut claimed=Vec::new(); let mut cursor=sid;
    let pids:Vec<StreamId>=g.pending.keys().filter(|id|*id>=&sid).copied().collect();
    for id in &pids {
        if claimed.len()>=ct { break; }
        if let Some(pe)=g.pending.get(id) { if pe.delivered_at.elapsed().as_millis() as u64<mi { continue; } }
        if let Some(mut pe)=g.pending.remove(id) { pe.consumer=cn.to_string(); pe.delivered_at=std::time::Instant::now(); pe.delivery_count+=1; g.pending.insert(*id,pe); }
        cursor=StreamId::new(id.ms,id.seq+1);
        if just_id { claimed.push(RespValue::BulkString(Some(Bytes::from(format!("{}-{}",id.ms,id.seq))))); }
        else if let Some(fields)=sd.entries.get(id) { claimed.push(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(format!("{}-{}",id.ms,id.seq)))), RespValue::Array(enc_fields(fields))])); }
    }
    let nc=if claimed.len()>=ct { format!("{}-{}",cursor.ms,cursor.seq) } else { "0-0".to_string() };
    save_stream(db,key,sd);
    Ok(RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from(nc))), RespValue::Array(claimed), RespValue::Array(vec![])]))
}
