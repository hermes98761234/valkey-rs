# valkey-rs vs Valkey Gap Report

Generated: 2026-06-05

> **Update 2026-06-09:** Many entries below are out of date. Since this report
> was generated the following gaps were closed:
>
> - **Streams**: the entire `stream.rs` module was unreachable (never declared
>   in `lib.rs`); it is now compiled, fixed, and dispatched. XADD, XREAD,
>   XRANGE, XREVRANGE, XLEN, XTRIM, XDEL, XINFO (incl. FULL), XGROUP,
>   XREADGROUP, XACK, XCLAIM, XPENDING, XAUTOCLAIM all work, and XSETID was
>   added.
> - **Bitmap family (new)**: SETBIT, GETBIT, BITCOUNT (BYTE/BIT), BITPOS
>   (BYTE/BIT), BITOP (AND/OR/XOR/NOT), BITFIELD, BITFIELD_RO.
> - **HyperLogLog (new)**: PFADD, PFCOUNT (multi-key union), PFMERGE.
> - **Strings**: LCS (plain/LEN/IDX/MINMATCHLEN/WITHMATCHLEN); GETEX now
>   supports all options; SUBSTR exists.
> - **Hashes**: HSETNX, HSTRLEN.
> - **Lists**: LPUSHX, RPUSHX, RPOPLPUSH, BRPOPLPUSH (plus the previously
>   added LMPOP/BLMPOP/BLMOVE/LPOS).
> - **Sorted sets**: ZINTERCARD, ZREVRANGEBYLEX (plus the previously added
>   ZDIFF/ZINTER/ZUNION/ZMPOP/BZMPOP/BZPOPMIN/BZPOPMAX). Also fixed: lex max
>   bounds in ZRANGEBYLEX/ZLEXCOUNT/ZREMRANGEBYLEX/ZRANGESTORE were applied
>   with lower-bound semantics, so finite `[x` / `(x` max bounds returned
>   wrong results.
> - **Keys**: TOUCH (EXPIRETIME/PEXPIRETIME/SORT_RO/DUMP/RESTORE already
>   landed earlier).
> - **Server**: HELLO, ROLE, SWAPDB added; SHUTDOWN and LOLWUT were
>   implemented but unrouted, now dispatched. Bare INFO/ECHO/COMMAND/SAVE,
>   CLIENT LIST, MEMORY DOCTOR, CONFIG RESETSTAT etc. no longer rejected by a
>   faulty arity guard. The listen port is configurable via the `PORT` env
>   var.
> - **Scripting**: EVAL/EVALSHA/SCRIPT are implemented via mlua (the table
>   below predates this).
>
> Remaining known gaps: true blocking semantics for BLPOP/BRPOP, pub/sub
> push-mode wiring in the connection handler (handlers exist but the server
> never enters subscribe mode), cluster mode, FUNCTION/FCALL, and the newer
> hash-field-TTL family (HEXPIRE etc.).

Legend:
- ✅ = Fully implemented
- ⚠️ = Partially implemented (notes included)
- ❌ = Missing entirely

---

## String Commands

| Command | Status | Notes |
|---------|--------|-------|
| GET | ✅ | |
| SET | ✅ | Supports NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL |
| MGET | ✅ | |
| MSET | ✅ | |
| APPEND | ✅ | |
| INCR | ✅ | |
| INCRBY | ✅ | |
| INCRBYFLOAT | ✅ | |
| DECR | ✅ | |
| DECRBY | ✅ | |
| GETSET | ✅ | |
| GETDEL | ✅ | |
| GETEX | ✅ | Supports PERSIST option; EX/PX/EXAT/PXAT KEEPTTL missing |
| SETNX | ✅ | |
| SETEX | ✅ | |
| PSETEX | ✅ | |
| MSETNX | ✅ | |
| STRLEN | ✅ | |
| GETRANGE | ✅ | |
| SETRANGE | ✅ | |
| SUBSTR | ❌ | Not implemented (legacy alias for GETRANGE) |

**String notes:**
- GETEX is missing EX, PX, EXAT, PXAT, and KEEPTTL options — only PERSIST is handled.

---

## List Commands

| Command | Status | Notes |
|---------|--------|-------|
| LPUSH | ✅ | |
| RPUSH | ✅ | |
| LPOP | ✅ | Supports count variant |
| RPOP | ✅ | Supports count variant |
| LLEN | ✅ | |
| LRANGE | ✅ | |
| LINDEX | ✅ | |
| LSET | ✅ | |
| LINSERT | ✅ | |
| LREM | ✅ | |
| LTRIM | ✅ | |
| LMOVE | ✅ | |
| LMPOP | ❌ | Not implemented |
| BLPOP | ⚠️ | Returns immediately (non-blocking); timeout parsed but not used for blocking |
| BRPOP | ⚠️ | Returns immediately (non-blocking); timeout parsed but not used for blocking |
| BLMOVE | ❌ | Not implemented |
| BLMPOP | ❌ | Not implemented |
| LPOS | ❌ | Not implemented |

**List notes:**
- BLPOP/BRPOP parse the timeout argument but return immediately without actual blocking behavior.
- No blocking list operations have true async blocking semantics.

---

## Hash Commands

| Command | Status | Notes |
|---------|--------|-------|
| HSET | ✅ | |
| HGET | ✅ | |
| HMGET | ✅ | |
| HMSET | ✅ | Alias for HSET |
| HDEL | ✅ | |
| HEXISTS | ✅ | |
| HGETALL | ✅ | |
| HKEYS | ✅ | |
| HVALS | ✅ | |
| HLEN | ✅ | |
| HINCRBY | ✅ | |
| HINCRBYFLOAT | ✅ | |
| HSCAN | ✅ | |
| HRANDFIELD | ✅ | Supports COUNT + WITHVALUES |

---

## Set Commands

| Command | Status | Notes |
|---------|--------|-------|
| SADD | ✅ | |
| SMEMBERS | ✅ | |
| SCARD | ✅ | |
| SISMEMBER | ✅ | |
| SMISMEMBER | ✅ | |
| SREM | ✅ | |
| SPOP | ✅ | Supports count variant |
| SRANDMEMBER | ✅ | Supports negative count (with duplicates) |
| SMOVE | ✅ | |
| SDIFF | ✅ | |
| SDIFFSTORE | ✅ | |
| SINTER | ✅ | |
| SINTERSTORE | ✅ | |
| SUNION | ✅ | |
| SUNIONSTORE | ✅ | |
| SSCAN | ✅ | |
| SINTERCARD | ❌ | Not implemented |

---

## Sorted Set Commands

| Command | Status | Notes |
|---------|--------|-------|
| ZADD | ✅ | Supports NX/XX/GT/LT/CH/INCR |
| ZRANGE | ✅ | Supports BYSCORE/BYLEX/REV/LIMIT/WITHSCORES |
| ZRANGEBYSCORE | ✅ | Supports LIMIT/WITHSCORES |
| ZRANGEBYLEX | ✅ | Supports LIMIT |
| ZREVRANGE | ✅ | Supports WITHSCORES |
| ZREVRANGEBYSCORE | ✅ | Supports LIMIT/WITHSCORES |
| ZRANK | ✅ | Supports WITHSCORE |
| ZREVRANK | ✅ | Supports WITHSCORE |
| ZRANGESTORE | ✅ | Supports BYSCORE/BYLEX/REV/LIMIT |
| ZSCORE | ✅ | |
| ZMSCORE | ✅ | |
| ZCOUNT | ✅ | |
| ZLEXCOUNT | ✅ | |
| ZCARD | ✅ | |
| ZINCRBY | ✅ | |
| ZPOPMIN | ✅ | Supports count variant |
| ZPOPMAX | ✅ | Supports count variant |
| ZRANDMEMBER | ✅ | Supports count + WITHSCORES |
| ZDIFF | ❌ | Not implemented |
| ZDIFFSTORE | ✅ | |
| ZINTER | ❌ | Not implemented |
| ZINTERSTORE | ✅ | Supports WEIGHTS/AGGREGATE |
| ZUNION | ❌ | Not implemented |
| ZUNIONSTORE | ✅ | Supports WEIGHTS/AGGREGATE |
| ZSCAN | ✅ | Supports MATCH/COUNT |
| ZMPOP | ❌ | Not implemented |
| BZMPOP | ❌ | Not implemented |
| BZPOPMIN | ❌ | Not implemented |
| BZPOPMAX | ❌ | Not implemented |
| ZREM | ✅ | |
| ZREMRANGEBYRANK | ✅ | |
| ZREMRANGEBYSCORE | ✅ | |
| ZREMRANGEBYLEX | ✅ | |

---

## Key / Generic Commands

| Command | Status | Notes |
|---------|--------|-------|
| DEL | ✅ | |
| EXISTS | ✅ | |
| EXPIRE | ✅ | Supports NX/XX/GT/LT conditions |
| EXPIREAT | ✅ | Supports NX/XX/GT/LT conditions |
| PEXPIRE | ✅ | Supports NX/XX/GT/LT conditions |
| PEXPIREAT | ✅ | Supports NX/XX/GT/LT conditions |
| TTL | ✅ | Returns -2 for missing, -1 for no expiry |
| PTTL | ✅ | Returns -2 for missing, -1 for no expiry |
| PERSIST | ✅ | |
| RENAME | ✅ | Preserves TTL |
| RENAMENX | ✅ | |
| TYPE | ✅ | |
| KEYS | ✅ | Simple glob matching |
| SCAN | ✅ | Supports MATCH/COUNT/TYPE filters |
| OBJECT | ✅ | ENCODING, REFCOUNT, IDLETIME, FREQ subcommands |
| UNLINK | ✅ | Same as DEL (non-blocking variant not truly async) |
| EXPIRETIME | ❌ | Not implemented |
| PEXPIRETIME | ❌ | Not implemented |
| COPY | ✅ | Supports REPLACE option |
| MOVE | ⚠️ | Stub — always returns 0 (single DB only) |
| DUMP | ⚠️ | Returns error "DUMP not supported yet" |
| RESTORE | ⚠️ | Returns error "RESTORE not supported yet" |
| SORT | ⚠️ | Partial — basic implementation, BY/GET/STORE/ALPHA parsed |
| SORT_RO | ❌ | Not implemented (read-only variant of SORT) |
| WAIT | ✅ | Implemented via replication manager |

---

## Server Commands

| Command | Status | Notes |
|---------|--------|-------|
| PING | ✅ | Supports optional message argument |
| ECHO | ✅ | |
| SELECT | ✅ | Accepts any index (single keyspace) |
| DBSIZE | ✅ | |
| FLUSHDB | ✅ | Supports ASYNC/SYNC (no-op distinction) |
| FLUSHALL | ✅ | Supports ASYNC/SYNC (no-op distinction) |
| INFO | ✅ | All major sections (SERVER, CLIENTS, MEMORY, STATS, REPLICATION, CPU, KEYSPACE) |
| CONFIG GET | ✅ | Supports glob patterns |
| CONFIG SET | ✅ | Validates known params, updates store config |
| CONFIG REWRITE | ✅ | Returns OK (no-op, no config file) |
| CONFIG RESETSTAT | ✅ | Returns OK (no-op) |
| DEBUG | ⚠️ | Accepted but minimal implementation |
| SLOWLOG | ⚠️ | Accepted but minimal implementation |
| LATENCY | ⚠️ | Accepted but minimal implementation |
| COMMAND | ✅ | COUNT, INFO, DOCS, HELP subcommands |
| COMMAND COUNT | ✅ | |
| COMMAND DOCS | ✅ | Returns empty docs |
| COMMAND GETKEYS | ❌ | Not implemented |
| COMMAND INFO | ✅ | |
| COMMAND LIST | ❌ | Not implemented |
| MEMORY USAGE | ⚠️ | Stub in MEMORY handler |
| MEMORY DOCTOR | ❌ | Not implemented |
| MEMORY STATS | ❌ | Not implemented |
| CLIENT ID | ⚠️ | Minimal — in COMMANDS table only |
| CLIENT GETNAME | ⚠️ | Minimal |
| CLIENT SETNAME | ⚠️ | Minimal |
| CLIENT LIST | ⚠️ | Minimal |
| CLIENT INFO | ❌ | Not implemented |
| CLIENT KILL | ❌ | Not implemented |
| CLIENT PAUSE | ❌ | Not implemented |
| CLIENT UNPAUSE | ❌ | Not implemented |
| CLIENT NO-EVICT | ❌ | Not implemented |
| CLIENT NO-TOUCH | ❌ | Not implemented |
| CLIENT REPLY | ❌ | Not implemented |
| LOLWUT | ❌ | Not implemented |
| RESET | ✅ | Resets client state |
| QUIT | ✅ | Returns OK |
| SHUTDOWN | ❌ | Not implemented |
| SAVE | ⚠️ | Stub — returns OK (no RDB persistence) |
| BGSAVE | ⚠️ | Stub — returns OK |
| BGREWRITEAOF | ⚠️ | Stub — returns OK (no AOF) |
| LASTSAVE | ✅ | Returns current timestamp |
| REPLICAOF | ✅ | Delegates to replication crate |
| SLAVEOF | ✅ | Alias for REPLICAOF |
| TIME | ✅ | |
| RANDOMKEY | ✅ | |
| OBJECT ENCODING | ✅ | Via keys::cmd_object |
| OBJECT REFCOUNT | ✅ | Always returns 1 |
| OBJECT IDLETIME | ✅ | Always returns 0 |
| OBJECT FREQ | ✅ | Always returns 0 |
| OBJECT HELP | ❌ | Not implemented |

---

## Pub/Sub Commands

| Command | Status | Notes |
|---------|--------|-------|
| SUBSCRIBE | ✅ | |
| UNSUBSCRIBE | ✅ | |
| PUBLISH | ✅ | Returns subscriber count |
| PSUBSCRIBE | ✅ | |
| PUNSUBSCRIBE | ✅ | |
| PUBSUB CHANNELS | ✅ | Supports pattern filter |
| PUBSUB NUMSUB | ✅ | |
| PUBSUB NUMPAT | ✅ | |
| PUBSUB SHARDCHANNELS | ⚠️ | Stub — returns empty array (cluster mode) |
| PUBSUB SHARDNUMSUB | ⚠️ | Stub — returns empty array (cluster mode) |
| SSUBSCRIBE | ⚠️ | Returns error "Cluster mode not implemented" |
| SUNSUBSCRIBE | ⚠️ | Returns error "Cluster mode not implemented" |
| SPUBLISH | ❌ | Not implemented |

---

## Transaction Commands

| Command | Status | Notes |
|---------|--------|-------|
| MULTI | ✅ | |
| EXEC | ✅ | Handles WATCH dirty check |
| DISCARD | ✅ | |
| WATCH | ✅ | Optimistic locking with store watch |
| UNWATCH | ✅ | |

---

## Scripting Commands

| Command | Status | Notes |
|---------|--------|-------|
| EVAL | ❌ | Returns "not yet implemented" |
| EVALSHA | ❌ | Returns "not yet implemented" |
| EVALRO | ❌ | Not implemented |
| EVALSHARO | ❌ | Not implemented |
| SCRIPT LOAD | ❌ | Returns "not yet implemented" |
| SCRIPT EXISTS | ❌ | Returns "not yet implemented" |
| SCRIPT FLUSH | ❌ | Returns "not yet implemented" |
| SCRIPT DEBUG | ❌ | Returns "not yet implemented" |
| FCALL | ❌ | Not implemented |
| FCALL_RO | ❌ | Not implemented |
| FUNCTION LOAD | ❌ | Not implemented |
| FUNCTION DELETE | ❌ | Not implemented |
| FUNCTION LIST | ❌ | Not implemented |
| FUNCTION DUMP | ❌ | Not implemented |
| FUNCTION RESTORE | ❌ | Not implemented |
| FUNCTION FLUSH | ❌ | Not implemented |
| FUNCTION STATS | ❌ | Not implemented |

**Scripting notes:**
- The entire Lua scripting engine is unimplemented. This is the single largest gap.
- All scripting commands return "ERR not yet implemented".

---

## Stream Commands

| Command | Status | Notes |
|---------|--------|-------|
| XADD | ✅ | Supports NOMKSTREAM, MAXLEN, MINID, LIMIT |
| XREAD | ✅ | Supports COUNT, BLOCK, $, STREAMS |
| XRANGE | ✅ | |
| XREVRANGE | ✅ | Note: start/end args are swapped vs spec |
| XLEN | ✅ | |
| XACK | ✅ | |
| XDEL | ✅ | |
| XTRIM | ✅ | Supports MAXLEN, MINID |
| XGROUP CREATE | ✅ | Supports ENTRIESREAD |
| XGROUP SETID | ✅ | Supports ENTRIESREAD |
| XGROUP DELCONSUMER | ✅ | |
| XGROUP DESTROY | ✅ | |
| XREADGROUP | ✅ | Supports COUNT, NOACK, > and specific IDs |
| XPENDING | ✅ | Supports IDLE, range, consumer filter |
| XCLAIM | ✅ | Supports IDLE, TIME, RETRYCOUNT, FORCE, JUSTID |
| XAUTOCLAIM | ✅ | Supports COUNT, JUSTID |
| XINFO GROUPS | ✅ | |
| XINFO CONSUMERS | ✅ | |
| XINFO STREAM | ✅ | |
| XINFO FULL | ❌ | Not implemented |

---

## Persistence Commands

| Command | Status | Notes |
|---------|--------|-------|
| SAVE | ⚠️ | Stub — no actual RDB persistence |
| BGSAVE | ⚠️ | Stub — no actual RDB persistence |
| BGREWRITEAOF | ⚠️ | Stub — no AOF implemented |

**Persistence notes:**
- No actual RDB or AOF persistence is implemented. All persistence commands are stubs.

---

## ACL Commands

| Command | Status | Notes |
|---------|--------|-------|
| ACL WHOAMI | ✅ | |
| ACL LIST | ✅ | |
| ACL GETUSER | ✅ | |
| ACL SETUSER | ✅ | |
| ACL DELUSER | ✅ | |
| ACL CAT | ✅ | Returns category list |
| ACL LOG | ✅ | |
| ACL LOAD | ✅ | |
| ACL SAVE | ✅ | |

**ACL notes:**
- Full ACL system implemented with categories, key patterns, channel patterns, and command permissions.

---

## Cluster Commands

| Command | Status | Notes |
|---------|--------|-------|
| CLUSTER INFO | ❌ | Not implemented |
| CLUSTER NODES | ❌ | Not implemented |
| CLUSTER MEET | ❌ | Not implemented |
| CLUSTER RESET | ❌ | Not implemented |
| CLUSTER KEYSLOT | ❌ | Not implemented |
| CLUSTER MYID | ❌ | Not implemented |

**Cluster notes:**
- No cluster mode support at all. All cluster commands are missing.
- SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH are cluster-related and also missing.

---

## Summary

### Fully Implemented Command Families
- **Hash**: 15/15 (100%)
- **Transactions**: 5/5 (100%)
- **ACL**: 9/9 (100%)
- **Pub/Sub**: 10/13 (77% — cluster pub/sub missing)

### Partially Implemented Command Families
- **String**: 20/21 (95% — SUBSTR missing, GETEX incomplete)
- **List**: 15/20 (75% — blocking ops non-functional, LMPOP/LPOS/BLMOVE/BLMPOP missing)
- **Set**: 18/19 (95% — SINTERCARD missing)
- **Sorted Set**: 24/32 (75% — ZMPOP/BZMPOP/BZPOPMIN/BZPOPMAX/ZDIFF/ZINTER/ZUNION missing)
- **Key/Generic**: 18/22 (82% — EXPIRETIME/PEXPIRETIME/SORT_RO missing, DUMP/RESTORE stubs)
- **Server**: 30/48 (63% — many CLIENT subcommands, SHUTDOWN, LOLWUT, MEMORY subcommands missing)
- **Streams**: 19/20 (95% — XINFO FULL missing)

### Not Implemented At All
- **Scripting**: 0/16 (0%) — Entire Lua scripting engine
- **Cluster**: 0/6 (0%) — No cluster support

### Total Counts
- **Total commands audited**: ~175
- **Fully implemented**: ~120 (69%)
- **Partially implemented**: ~15 (9%)
- **Missing entirely**: ~40 (23%)
