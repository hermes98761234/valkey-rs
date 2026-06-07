# valkey-rs

![CI](https://github.com/hermes98761234/valkey-rs/workflows/CI/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-1.78%2B-orange.svg)

A production-quality, full-featured Valkey/Redis rewrite in Rust.

## Features

- **Async Tokio runtime** — high-performance, non-blocking I/O with full concurrency
- **RESP2 / RESP3 protocol** — complete Redis Serialization Protocol support
- **All command families** — Strings, Lists, Sets, Sorted Sets, Hashes, Streams, Transactions
- **RDB + AOF persistence** — snapshot-based RDB dumps and Append-Only File journaling
- **Pub/Sub with keyspace notifications** — real-time publish/subscribe with key-change events
- **Lua scripting (EVAL)** — server-side scripting via Lua 5.4 engine
- **ACL (Access Control Lists)** — user-based authentication and command-level permissions
- **TLS support** — encrypted connections via `tokio-rustls` with optional client cert auth
- **Replication (PSYNC)** — leader-follower replication with full/partial sync and backlog
- **Cluster mode** — hash slot routing (CRC16 + hashtags), gossip protocol, MOVED/ASK redirections
- **Sentinel mode** — automatic failover with leader election, quorum voting, and replica promotion
- **Geo commands** — GEOADD, GEODIST, GEOPOS, GEOSEARCH, GEORADIUS with geohash encoding
- **Compact encodings** — listpack and intset for memory-efficient small aggregates
- **Client-side caching** — CLIENT TRACKING with invalidation messages (RESP3)
- **Redis Modules API** — C ABI compatibility layer for loading external `.so` modules

## Getting Started

### Prerequisites

- **Rust** 1.78 or later ([rustup](https://rustup.rs/))

### Build

```sh
cargo build --release -p valkey-server
```

### Run

```sh
./target/release/valkey-server
```

The server starts listening on `0.0.0.0:6379` by default.

### Connect

```sh
redis-cli ping
# PONG
```

## Docker

### Standalone

```sh
docker compose up -d
docker compose exec valkey sh -c 'echo PING | nc -w 1 localhost 6379'
# PONG
```

### Replication (Primary + Replica)

```sh
docker compose -f docker-compose.replication.yml up -d
docker compose -f docker-compose.replication.yml exec valkey-primary sh -c 'echo PING | nc -w 1 localhost 6379'
docker compose -f docker-compose.replication.yml exec valkey-replica sh -c 'echo PING | nc -w 1 localhost 6379'
```

### Cluster (3 Nodes)

```sh
docker compose -f docker-compose.cluster.yml up -d
docker compose -f docker-compose.cluster.yml exec valkey-node1 sh -c 'echo PING | nc -w 1 localhost 6379'
```

### Build Image Manually

```sh
docker build -t valkey-rs .
docker run -d -p 6379:6379 -v ./data:/data valkey-rs
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| Port | `6379` | Plain TCP listen address: `0.0.0.0:{port}` |
| AppendOnly | auto-detected | Enable AOF persistence when `appendonly.aof` exists on startup |
| AOF filename | `./appendonly.aof` | Path to the AOF journal file |
| RDB filename | `./dump.rdb` | Path to the RDB snapshot file |
| Auto-save interval | `10s` | Interval for automatic RDB snapshots (dirty keys > 0) |
| TLS port | *none* | TLS listen port (set via `TLS_PORT` env var) |
| TLS cert file | *none* | PEM certificate chain (set via `TLS_CERT` env var) |
| TLS key file | *none` | PEM private key (set via `TLS_KEY` env var) |
| TLS CA cert | *none* | PEM CA certificate for client auth (set via `TLS_CA_CERT` env var) |
| TLS client auth | `no` | Client certificate policy: `no`, `yes`, or `optional` (`TLS_AUTH_CLIENTS`) |
| Replication | enabled | Built-in PSYNC replication manager with backlog |
| Cluster mode | in-code | Hash slot routing with CRC16 and hashtag extraction |

Environment variables:

```sh
# enable TLS on port 6380
TLS_PORT=6380 TLS_CERT=cert.pem TLS_KEY=key.pem ./target/release/valkey-server

# require client certificates
TLS_PORT=6380 TLS_CERT=cert.pem TLS_KEY=key.pem TLS_CA_CERT=ca.pem TLS_AUTH_CLIENTS=yes ./target/release/valkey-server
```

## Command Coverage

| Family | Commands | Status |
|--------|----------|--------|
| **Strings** | GET, SET, SETEX, PSETEX, SETNX, GETSET, APPEND, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, STRLEN, MGET, MSET | ✅ Implemented |
| **Keys** | DEL, UNLINK, EXISTS, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TTL, PTTL, TYPE, RENAME, RENAMENX, KEYS, SCAN, FLUSHDB, FLUSHALL, RANDOMKEY | ✅ Implemented |
| **Lists** | RPUSH, LPUSH, RPOP, LPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, BLPOP, BRPOP | ✅ Implemented |
| **Sets** | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SMOVE, SUNION, SUNIONSTORE, SINTER, SINTERSTORE, SDIFF, SDIFFSTORE, SRANDMEMBER, SSCAN | ✅ Implemented |
| **Sorted Sets** | ZADD, ZREM, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZINCRBY, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZPOPMIN, ZPOPMAX, ZRANGEBYLEX, ZLEXCOUNT, ZSCAN | ✅ Implemented |
| **Hashes** | HSET, HGET, HDEL, HGETALL, HEXISTS, HINCRBY, HINCRBYFLOAT, HKEYS, HVALS, HLEN, HMSET, HMGET, HSETNX, HSCAN | ✅ Implemented |
| **Streams** | XADD, XREAD, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM, XGROUP, XACK, XREADGROUP | ✅ Implemented |
| **Pub/Sub** | SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB, LISTEN | ✅ Implemented |
| **Transactions** | MULTI, EXEC, DISCARD, WATCH | ✅ Implemented |
| **Scripting** | EVAL, EVALSHA, SCRIPT LOAD, SCRIPT FLUSH, SCRIPT EXISTS | ✅ Implemented |
| **Server** | PING, INFO, CONFIG, DBSIZE, TIME, CLIENT, COMMAND, SELECT, AUTH, ACL, SHUTDOWN, SAVE, BGSAVE, SLOWLOG, LATENCY, MODULE | ✅ Implemented |
| **Replication** | REPLICAOF, PSYNC, ROLE, REPLCONF | ✅ Implemented |
| **Cluster** | CLUSTER INFO, CLUSTER NODES, CLUSTER SLOTS, CLUSTER KEYSLOT, CLUSTER ADDSLOTS, CLUSTER DELSLOTS, CLUSTER MEET, CLUSTER FORGET, CLUSTER REPLICATE | ✅ Implemented |
| **Geo** | GEOADD, GEODIST, GEOPOS, GEOSEARCH, GEOSEARCHSTORE, GEORADIUS, GEORADIUSBYMEMBER | ✅ Implemented |
| **Modules** | MODULE LOAD, MODULE UNLOAD, MODULE LIST | ✅ Implemented |

## Architecture

The project is a Cargo workspace of 11 crates, each handling a distinct responsibility:

```
valkey-rs/
├── crates/
│   ├── proto/         RESP2/RESP3 protocol encoding and decoding (RespEncoder, RespDecoder)
│   ├── storage/       In-memory key-value store (DashMap-based, sharded, async-safe)
│   ├── commands/      Command dispatch and per-family handlers (ACL, strings, hashes, lists, sets, zsets, streams, transactions, pubsub, scripting, server, replication, geo)
│   ├── persistence/   RDB snapshot save/load and AOF journaling (FsyncPolicy: EverySec)
│   ├── replication/   Leader-follower replication (PSYNC, circular buffer backlog, command propagation)
│   ├── cluster/       Cluster mode (CRC16 hash slots, hashtag extraction, gossip, MOVED/ASK, ClusterState)
│   ├── pubsub/        Publish/subscribe engine with keyspace notification support
│   ├── scripting/     Lua 5.4 scripting via mlua (EVAL, EVALSHA, SCRIPT commands)
│   ├── modules/       Redis Modules C API compatibility layer (MODULE LOAD/UNLOAD/LIST, FFI bindings)
│   ├── sentinel/      Sentinel mode server with automatic failover, monitoring, and leader election
│   └── server/        Main entry point — TCP/TLS listeners, connection handler, ClientStream abstraction, AOF auto-append, auto-save
```

**Request flow:**

```
Client → TcpListener / TlsAcceptor → ClientStream (Plain / TLS)
  → Framed<RespDecoder> → dispatch() → [AOF append] → [replica propagate]
    → Framed<RespEncoder> → Client
```

## Testing

```sh
# run all unit tests across the workspace
cargo test --all
```

## Roadmap

- [ ] Full blocking WAIT semantics for synchronous replication guarantee
- [ ] Cluster resharding (live slot migration between nodes)
- [ ] CLUSTER BUMPEPOCH and CONFIG REWRITE cluster commands
- [x] Sentinel mode for automatic failover
- [x] CLIENT TRACKING with invalidation messages
- [x] Memory-efficient encoding for small aggregates (listpack, intset)
- [x] Redis Modules API compatibility layer
- [x] Geo commands (GEOADD, GEODIST, GEORADIUS, GEOSEARCH)
- [x] OBJECT ENCODING for compact type introspection

## License

[MIT](LICENSE)

## Contributing

Contributions are welcome! Here's how:

1. **Fork** the repository
2. **Create a branch** (`git checkout -b feature/your-feature`)
3. **Make your changes** — follow the existing code style
4. **Run tests** (`cargo test --all`, `cargo clippy --all`)
5. **Commit** with clear messages (conventional commits preferred)
6. **Open a Pull Request** against `main`

Please ensure:
- `cargo fmt --all --check` passes
- `cargo clippy --all` reports no new warnings
- New features include corresponding tests

For significant changes, open an issue first to discuss design direction.
