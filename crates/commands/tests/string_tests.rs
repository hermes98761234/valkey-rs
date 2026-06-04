use bytes::Bytes;
use std::sync::Arc;

use valkey_commands::string;
use valkey_proto::RespValue;
use valkey_storage::{DataType, Entry, Store};

fn test_store() -> Arc<Store> {
    Store::new()
}

#[test]
fn test_get_missing() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("missing")], &store));
    assert_eq!(result, RespValue::BulkString(None));
}

#[test]
fn test_set_and_get() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("key1"), Bytes::from("hello")], &store));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("key1")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("hello"))));
}

#[test]
fn test_set_overwrite() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("k"), Bytes::from("first")], &store));
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("k"), Bytes::from("second")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("k")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("second"))));
}

#[test]
fn test_set_nx() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("nx"), Bytes::from("val"), Bytes::from("NX")], &store));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("nx"), Bytes::from("val2"), Bytes::from("NX")], &store));
    assert_eq!(result, RespValue::BulkString(None));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("nx")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
}

#[test]
fn test_set_xx() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("xx"), Bytes::from("val"), Bytes::from("XX")], &store));
    assert_eq!(result, RespValue::BulkString(None));
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("xx"), Bytes::from("original")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("xx"), Bytes::from("updated"), Bytes::from("XX")], &store));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("xx")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("updated"))));
}

#[test]
fn test_del() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("d1"), Bytes::from("v1")], &store));
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("d2"), Bytes::from("v2")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("DEL"), Bytes::from("d1"), Bytes::from("d2")], &store));
    assert_eq!(result, RespValue::Integer(2));
    let result = rt.block_on(string::handle(&[Bytes::from("DEL"), Bytes::from("d1")], &store));
    assert_eq!(result, RespValue::Integer(0));
}

#[test]
fn test_getset() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("GETSET"), Bytes::from("gs"), Bytes::from("new")], &store));
    assert_eq!(result, RespValue::BulkString(None));
    let result = rt.block_on(string::handle(&[Bytes::from("GETSET"), Bytes::from("gs"), Bytes::from("newer")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("new"))));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("gs")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("newer"))));
}

#[test]
fn test_mget() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("m1"), Bytes::from("a")], &store));
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("m2"), Bytes::from("b")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("MGET"), Bytes::from("m1"), Bytes::from("m2"), Bytes::from("m3")], &store));
    match result {
        RespValue::Array(Some(arr)) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
            assert_eq!(arr[2], RespValue::BulkString(None));
        }
        _ => panic!("expected array, got {:?}", result),
    }
}

#[test]
fn test_mset() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("MSET"), Bytes::from("a"), Bytes::from("1"), Bytes::from("b"), Bytes::from("2")], &store));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("a")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("1"))));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("b")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("2"))));
}

#[test]
fn test_msetnx() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("MSETNX"), Bytes::from("mn1"), Bytes::from("1"), Bytes::from("mn2"), Bytes::from("2")], &store));
    assert_eq!(result, RespValue::Integer(1));
    let result = rt.block_on(string::handle(&[Bytes::from("MSETNX"), Bytes::from("mn1"), Bytes::from("x"), Bytes::from("mn3"), Bytes::from("3")], &store));
    assert_eq!(result, RespValue::Integer(0));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("mn3")], &store));
    assert_eq!(result, RespValue::BulkString(None));
}

#[test]
fn test_incr_decr() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("INCR"), Bytes::from("cnt")], &store));
    assert_eq!(result, RespValue::Integer(1));
    let result = rt.block_on(string::handle(&[Bytes::from("INCR"), Bytes::from("cnt")], &store));
    assert_eq!(result, RespValue::Integer(2));
    let result = rt.block_on(string::handle(&[Bytes::from("DECR"), Bytes::from("cnt")], &store));
    assert_eq!(result, RespValue::Integer(1));
}

#[test]
fn test_incrby_decrby() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("INCRBY"), Bytes::from("cnt2"), Bytes::from("10")], &store));
    assert_eq!(result, RespValue::Integer(10));
    let result = rt.block_on(string::handle(&[Bytes::from("DECRBY"), Bytes::from("cnt2"), Bytes::from("3")], &store));
    assert_eq!(result, RespValue::Integer(7));
}

#[test]
fn test_incr_non_integer_fails() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("ni"), Bytes::from("hello")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("INCR"), Bytes::from("ni")], &store));
    match result {
        RespValue::Error(_) => {}
        _ => panic!("expected error for INCR on non-integer, got {:?}", result),
    }
}

#[test]
fn test_incrbyfloat() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("f"), Bytes::from("10.5")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("INCRBYFLOAT"), Bytes::from("f"), Bytes::from("0.1")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("10.6"))));
    let result = rt.block_on(string::handle(&[Bytes::from("INCRBYFLOAT"), Bytes::from("nf"), Bytes::from("3.14")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("3.14"))));
}

#[test]
fn test_append() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("APPEND"), Bytes::from("ap"), Bytes::from("Hello")], &store));
    assert_eq!(result, RespValue::Integer(5));
    let result = rt.block_on(string::handle(&[Bytes::from("APPEND"), Bytes::from("ap"), Bytes::from(" World")], &store));
    assert_eq!(result, RespValue::Integer(11));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("ap")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("Hello World"))));
}

#[test]
fn test_strlen() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("sl"), Bytes::from("hello")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("STRLEN"), Bytes::from("sl")], &store));
    assert_eq!(result, RespValue::Integer(5));
    let result = rt.block_on(string::handle(&[Bytes::from("STRLEN"), Bytes::from("nokey")], &store));
    assert_eq!(result, RespValue::Integer(0));
}

#[test]
fn test_getrange() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("gr"), Bytes::from("Hello World")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("GETRANGE"), Bytes::from("gr"), Bytes::from("0"), Bytes::from("4")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("Hello"))));
    let result = rt.block_on(string::handle(&[Bytes::from("GETRANGE"), Bytes::from("gr"), Bytes::from("-5"), Bytes::from("-1")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("World"))));
    let result = rt.block_on(string::handle(&[Bytes::from("GETRANGE"), Bytes::from("gr"), Bytes::from("0"), Bytes::from("-1")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("Hello World"))));
    let result = rt.block_on(string::handle(&[Bytes::from("GETRANGE"), Bytes::from("nokey"), Bytes::from("0"), Bytes::from("1")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::new())));
}

#[test]
fn test_setrange() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("sr"), Bytes::from("Hello World")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("SETRANGE"), Bytes::from("sr"), Bytes::from("6"), Bytes::from("Redis")], &store));
    assert_eq!(result, RespValue::Integer(11));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("sr")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("Hello Redis"))));
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("sr2"), Bytes::from("abc")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("SETRANGE"), Bytes::from("sr2"), Bytes::from("5"), Bytes::from("z")], &store));
    assert_eq!(result, RespValue::Integer(6));
}

#[test]
fn test_setnx() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("SETNX"), Bytes::from("sn"), Bytes::from("val")], &store));
    assert_eq!(result, RespValue::Integer(1));
    let result = rt.block_on(string::handle(&[Bytes::from("SETNX"), Bytes::from("sn"), Bytes::from("val2")], &store));
    assert_eq!(result, RespValue::Integer(0));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("sn")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
}

#[test]
fn test_setex() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("SETEX"), Bytes::from("sex"), Bytes::from("10"), Bytes::from("val")], &store));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("sex")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
}

#[test]
fn test_psetex() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("PSETEX"), Bytes::from("psx"), Bytes::from("10000"), Bytes::from("val")], &store));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("psx")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
}

#[test]
fn test_getex() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("ge"), Bytes::from("val")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("GETEX"), Bytes::from("ge")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
    rt.block_on(string::handle(&[Bytes::from("SETEX"), Bytes::from("ge2"), Bytes::from("10"), Bytes::from("val2")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("GETEX"), Bytes::from("ge2"), Bytes::from("PERSIST")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val2"))));
}

#[test]
fn test_getdel() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    rt.block_on(string::handle(&[Bytes::from("SET"), Bytes::from("gd"), Bytes::from("val")], &store));
    let result = rt.block_on(string::handle(&[Bytes::from("GETDEL"), Bytes::from("gd")], &store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("gd")], &store));
    assert_eq!(result, RespValue::BulkString(None));
}

#[test]
fn test_getdel_missing() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(string::handle(&[Bytes::from("GETDEL"), Bytes::from("nokey")], &store));
    assert_eq!(result, RespValue::BulkString(None));
}

#[test]
fn test_wrontype_error() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    store.set(Bytes::from("listkey"), valkey_storage::DataType::List(std::collections::VecDeque::new()), None);
    let result = rt.block_on(string::handle(&[Bytes::from("GET"), Bytes::from("listkey")], &store));
    match result {
        RespValue::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        _ => panic!("expected WRONGTYPE error, got {:?}", result),
    }
}

#[test]
fn test_dispatch_set_get() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let store = test_store();
    let result = rt.block_on(valkey_commands::dispatch(vec![Bytes::from("SET"), Bytes::from("dk"), Bytes::from("dv")], store.clone()));
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    let result = rt.block_on(valkey_commands::dispatch(vec![Bytes::from("GET"), Bytes::from("dk")], store));
    assert_eq!(result, RespValue::BulkString(Some(Bytes::from("dv"))));
}
