use bytes::Bytes;
use std::sync::Arc;

use valkey_commands::string;
use valkey_proto::RespValue;
use valkey_storage::Store;

fn test_store() -> Arc<Store> {
    Store::new()
}

fn run_test<F: std::future::Future>(f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(f)
}

#[test]
fn test_get_missing() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("missing")], &store)).await;
        assert_eq!(result, RespValue::BulkString(None));
    })
}
#[test]
fn test_set_and_get() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("key1"),
                Bytes::from("hello"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("key1")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("hello"))));
    })
}
#[test]
fn test_set_overwrite() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("k"), Bytes::from("first")],
            &store,
        ))
        .await;
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("k"), Bytes::from("second")],
            &store,
        ))
        .await;
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("k")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("second"))));
    })
}
#[test]
fn test_set_nx() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("nx"),
                Bytes::from("val"),
                Bytes::from("NX"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result = (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("nx"),
                Bytes::from("val2"),
                Bytes::from("NX"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(None));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("nx")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
    })
}
#[test]
fn test_set_xx() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("xx"),
                Bytes::from("val"),
                Bytes::from("XX"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(None));
        (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("xx"),
                Bytes::from("original"),
            ],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("xx"),
                Bytes::from("updated"),
                Bytes::from("XX"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("xx")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("updated"))));
    })
}
#[test]
fn test_del() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("d1"), Bytes::from("v1")],
            &store,
        ))
        .await;
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("d2"), Bytes::from("v2")],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[Bytes::from("DEL"), Bytes::from("d1"), Bytes::from("d2")],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(2));
        let result = (string::handle(&[Bytes::from("DEL"), Bytes::from("d1")], &store)).await;
        assert_eq!(result, RespValue::Integer(0));
    })
}
#[test]
fn test_getset() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[Bytes::from("GETSET"), Bytes::from("gs"), Bytes::from("new")],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(None));
        let result = (string::handle(
            &[
                Bytes::from("GETSET"),
                Bytes::from("gs"),
                Bytes::from("newer"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("new"))));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("gs")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("newer"))));
    })
}
#[test]
fn test_mget() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("m1"), Bytes::from("a")],
            &store,
        ))
        .await;
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("m2"), Bytes::from("b")],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("MGET"),
                Bytes::from("m1"),
                Bytes::from("m2"),
                Bytes::from("m3"),
            ],
            &store,
        ))
        .await;
        match result {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
                assert_eq!(arr[2], RespValue::BulkString(None));
            }
            _ => panic!("expected array, got {:?}", result),
        }
    })
}
#[test]
fn test_mset() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("MSET"),
                Bytes::from("a"),
                Bytes::from("1"),
                Bytes::from("b"),
                Bytes::from("2"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("a")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("1"))));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("b")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("2"))));
    })
}
#[test]
fn test_msetnx() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("MSETNX"),
                Bytes::from("mn1"),
                Bytes::from("1"),
                Bytes::from("mn2"),
                Bytes::from("2"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(1));
        let result = (string::handle(
            &[
                Bytes::from("MSETNX"),
                Bytes::from("mn1"),
                Bytes::from("x"),
                Bytes::from("mn3"),
                Bytes::from("3"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(0));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("mn3")], &store)).await;
        assert_eq!(result, RespValue::BulkString(None));
    })
}
#[test]
fn test_incr_decr() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(&[Bytes::from("INCR"), Bytes::from("cnt")], &store)).await;
        assert_eq!(result, RespValue::Integer(1));
        let result = (string::handle(&[Bytes::from("INCR"), Bytes::from("cnt")], &store)).await;
        assert_eq!(result, RespValue::Integer(2));
        let result = (string::handle(&[Bytes::from("DECR"), Bytes::from("cnt")], &store)).await;
        assert_eq!(result, RespValue::Integer(1));
    })
}
#[test]
fn test_incrby_decrby() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("INCRBY"),
                Bytes::from("cnt2"),
                Bytes::from("10"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(10));
        let result = (string::handle(
            &[Bytes::from("DECRBY"), Bytes::from("cnt2"), Bytes::from("3")],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(7));
    })
}
#[test]
fn test_incr_non_integer_fails() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("ni"), Bytes::from("hello")],
            &store,
        ))
        .await;
        let result = (string::handle(&[Bytes::from("INCR"), Bytes::from("ni")], &store)).await;
        match result {
            RespValue::Error(_) => {}
            _ => panic!("expected error for INCR on non-integer, got {:?}", result),
        }
    })
}
#[test]
fn test_incrbyfloat() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("f"), Bytes::from("10.5")],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("INCRBYFLOAT"),
                Bytes::from("f"),
                Bytes::from("0.1"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("10.6"))));
        let result = (string::handle(
            &[
                Bytes::from("INCRBYFLOAT"),
                Bytes::from("nf"),
                Bytes::from("3.14"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("3.14"))));
    })
}
#[test]
fn test_append() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("APPEND"),
                Bytes::from("ap"),
                Bytes::from("Hello"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(5));
        let result = (string::handle(
            &[
                Bytes::from("APPEND"),
                Bytes::from("ap"),
                Bytes::from(" World"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(11));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("ap")], &store)).await;
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from("Hello World")))
        );
    })
}
#[test]
fn test_strlen() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("sl"), Bytes::from("hello")],
            &store,
        ))
        .await;
        let result = (string::handle(&[Bytes::from("STRLEN"), Bytes::from("sl")], &store)).await;
        assert_eq!(result, RespValue::Integer(5));
        let result = (string::handle(&[Bytes::from("STRLEN"), Bytes::from("nokey")], &store)).await;
        assert_eq!(result, RespValue::Integer(0));
    })
}
#[test]
fn test_getrange() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("gr"),
                Bytes::from("Hello World"),
            ],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("GETRANGE"),
                Bytes::from("gr"),
                Bytes::from("0"),
                Bytes::from("4"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("Hello"))));
        let result = (string::handle(
            &[
                Bytes::from("GETRANGE"),
                Bytes::from("gr"),
                Bytes::from("-5"),
                Bytes::from("-1"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("World"))));
        let result = (string::handle(
            &[
                Bytes::from("GETRANGE"),
                Bytes::from("gr"),
                Bytes::from("0"),
                Bytes::from("-1"),
            ],
            &store,
        ))
        .await;
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from("Hello World")))
        );
        let result = (string::handle(
            &[
                Bytes::from("GETRANGE"),
                Bytes::from("nokey"),
                Bytes::from("0"),
                Bytes::from("1"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::new())));
    })
}
#[test]
fn test_setrange() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[
                Bytes::from("SET"),
                Bytes::from("sr"),
                Bytes::from("Hello World"),
            ],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("SETRANGE"),
                Bytes::from("sr"),
                Bytes::from("6"),
                Bytes::from("Redis"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(11));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("sr")], &store)).await;
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from("Hello Redis")))
        );
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("sr2"), Bytes::from("abc")],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("SETRANGE"),
                Bytes::from("sr2"),
                Bytes::from("5"),
                Bytes::from("z"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(6));
    })
}
#[test]
fn test_setnx() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[Bytes::from("SETNX"), Bytes::from("sn"), Bytes::from("val")],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(1));
        let result = (string::handle(
            &[Bytes::from("SETNX"), Bytes::from("sn"), Bytes::from("val2")],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::Integer(0));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("sn")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
    })
}
#[test]
fn test_setex() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("SETEX"),
                Bytes::from("sex"),
                Bytes::from("10"),
                Bytes::from("val"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("sex")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
    })
}
#[test]
fn test_psetex() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(
            &[
                Bytes::from("PSETEX"),
                Bytes::from("psx"),
                Bytes::from("10000"),
                Bytes::from("val"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("psx")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
    })
}
#[test]
fn test_getex() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("ge"), Bytes::from("val")],
            &store,
        ))
        .await;
        let result = (string::handle(&[Bytes::from("GETEX"), Bytes::from("ge")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
        (string::handle(
            &[
                Bytes::from("SETEX"),
                Bytes::from("ge2"),
                Bytes::from("10"),
                Bytes::from("val2"),
            ],
            &store,
        ))
        .await;
        let result = (string::handle(
            &[
                Bytes::from("GETEX"),
                Bytes::from("ge2"),
                Bytes::from("PERSIST"),
            ],
            &store,
        ))
        .await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val2"))));
    })
}
#[test]
fn test_getdel() {
    run_test(async {
        let store = test_store();
        (string::handle(
            &[Bytes::from("SET"), Bytes::from("gd"), Bytes::from("val")],
            &store,
        ))
        .await;
        let result = (string::handle(&[Bytes::from("GETDEL"), Bytes::from("gd")], &store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("val"))));
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("gd")], &store)).await;
        assert_eq!(result, RespValue::BulkString(None));
    })
}
#[test]
fn test_getdel_missing() {
    run_test(async {
        let store = test_store();
        let result = (string::handle(&[Bytes::from("GETDEL"), Bytes::from("nokey")], &store)).await;
        assert_eq!(result, RespValue::BulkString(None));
    })
}
#[test]
fn test_wrontype_error() {
    run_test(async {
        let store = test_store();
        store.set(
            Bytes::from("listkey"),
            valkey_storage::DataType::List(std::collections::VecDeque::new()),
            None,
        );
        let result = (string::handle(&[Bytes::from("GET"), Bytes::from("listkey")], &store)).await;
        match result {
            RespValue::Error(msg) => assert!(msg.contains("WRONGTYPE")),
            _ => panic!("expected WRONGTYPE error, got {:?}", result),
        }
    })
}
#[test]
fn test_dispatch_set_get() {
    run_test(async {
        let store = test_store();
        let result = (valkey_commands::dispatch(
            vec![Bytes::from("SET"), Bytes::from("dk"), Bytes::from("dv")],
            store.clone(),
        ))
        .await;
        assert_eq!(result, RespValue::SimpleString("OK".into()));
        let result =
            (valkey_commands::dispatch(vec![Bytes::from("GET"), Bytes::from("dk")], store)).await;
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("dv"))));
    })
}
