# wrk-rs Design Spec

**Date:** 2026-06-08
**Project:** Rust rewrite of [wrk](https://github.com/wg/wrk) HTTP benchmarking tool
**Status:** Approved

---

## Overview

`wrk-rs` is a drop-in Rust replacement for the wrk HTTP benchmarking tool. It preserves wrk's CLI flags, output format, and LuaJIT scripting API while replacing the C + libevent core with a Tokio-based async engine and rustls for TLS.

---

## Goals

- wrk-compatible CLI (`-t`, `-c`, `-d`, `-s`, `--latency`, `--timeout`, `-H`)
- Identical output format (thread stats, latency distribution, summary line)
- Full Lua scripting API parity (`setup`, `init`, `request`, `response`, `delay`, `done` hooks; `wrk` table)
- HTTP/1.1 keep-alive benchmarking with TLS via rustls
- HTTP/2 tracked as a future milestone (not in v1)
- First release: `v0.0.1-beta`

---

## Architecture: N Independent Single-Thread Runtimes

Each OS thread owns a `tokio::runtime::Builder::new_current_thread()` runtime and an isolated Lua state. No cross-thread synchronization on the hot path. Stats flow back to main via `mpsc` channel after the duration expires.

```
main thread
  parse args → DNS resolve → load script path
  spawn N OS threads
        │ (×N)
  OS thread (engine crate)
    current_thread tokio runtime
    isolated mlua Lua state
    call setup(thread), init(args)
    spawn C/N async connection tasks
      loop: request() → send → recv → response() → delay() → repeat
    on timeout: send ThreadResult { histogram, reqs, bytes, errors }
        │
  main: aggregate all ThreadResults
  call done(summary, latency, requests)
  print report
```

---

## Crate Workspace

```
wrk-rs/
├── Cargo.toml               # workspace root
└── crates/
    ├── cli/                 # clap arg parsing, main, output formatting
    ├── engine/              # thread spawning, connection loop, HTTP state machine
    ├── scripting/           # mlua integration, wrk table, hook dispatch
    ├── stats/               # HDR histogram, per-thread stats, aggregation
    └── http/                # HTTP/1.1 request builder, httparse response parser
```

### Key Dependencies

| Crate | Purpose |
|---|---|
| `tokio` | `current_thread` runtime per OS thread |
| `mlua` | Lua 5.4 scripting; `luajit` feature flag for LuaJIT |
| `rustls` + `tokio-rustls` + `webpki-roots` | TLS, activated on `https://` |
| `httparse` | Zero-copy HTTP/1.1 response parsing |
| `hdrhistogram` | Latency distribution with HDR precision |
| `clap` | CLI flag parsing |

---

## CLI (wrk-compatible)

```
Usage: wrk <options> <url>

Options:
  -t <N>          Number of threads [default: 2]
  -c <N>          Number of connections [default: 10]
  -d <T>          Duration (e.g. 30s, 1m) [default: 10s]
  -s <script>     Lua script path
  -H <header>     Add header (repeatable)
  --latency       Print latency distribution
  --timeout <T>   Socket/request timeout [default: 2s]
```

---

## Lua Scripting (mlua)

### `wrk` global table

```lua
wrk.scheme   -- "http" | "https"
wrk.host     -- target host string
wrk.port     -- target port number
wrk.method   -- HTTP method (default "GET")
wrk.path     -- request path (default "/")
wrk.headers  -- table of header name → value
wrk.body     -- request body string
```

### Functions

```lua
wrk.format(method, path, headers, body)  -- build raw HTTP/1.1 request string
wrk.lookup(host, service)                -- DNS lookup → list of addresses
wrk.connect(addr)                        -- open connection to addr
```

### Hook dispatch order (per thread)

| Hook | When | Returns |
|---|---|---|
| `setup(thread)` | once after DNS resolve | — |
| `init(args)` | once at thread start | — |
| `request()` | before every request | raw HTTP/1.1 request string |
| `response(status, headers, body)` | after every response | — |
| `delay()` | after response | milliseconds to wait (number) |
| `done(summary, latency, requests)` | after thread finishes | — |

All hooks are optional. Missing hooks fall back to defaults (static `GET /`, no delay).

---

## Stats & Output

### Per-thread stats

- `HdrHistogram<u64>` — latency in microseconds, range 1µs–60s, 3 significant figures
- `u64` request count, error count, bytes transferred
- Wall-clock timestamps for accurate req/sec

### Aggregation

- Merge all per-thread HDR histograms (lossless)
- Sum requests, errors, bytes
- Compute percentiles: 50th, 75th, 90th, 99th, 99.9th

### Output format (wrk-identical)

```
Running 30s test @ http://localhost:8080
  12 threads and 400 connections

  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   635.91us  617.47us  48.36ms   93.22%
    Req/Sec    56.20k     8.07k   78.30k    68.50%

  Latency Distribution      (only with --latency)
     50%  453.00us
     75%  693.00us
     90%    1.20ms
     99%    3.70ms

  19559904 requests in 30.10s, 3.01GB read
Requests/sec:  649873.91
Transfer/sec:    102.24MB
```

---

## Feature Flags

| Flag | Default | Effect |
|---|---|---|
| `tls` | on | Include rustls TLS support |
| `luajit` | off | Link LuaJIT instead of Lua 5.4 |

---

## CI & Release

### `ci.yml` (push + PR on main)

1. `cargo build --all`
2. `cargo test --all`
3. `cargo clippy --all -- -D warnings`
4. `cargo fmt --all --check`

### `release.yml` (push on `v*` tags)

- Matrix: `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`
- Strip debug symbols
- Upload binaries + `SHA256SUMS.txt` as GitHub Release assets
- Pre-release detection: tags containing `-beta`, `-alpha`, `-rc`

### First release

Tag `v0.0.1-beta` after all implementation tasks pass CI.

---

## Testing Strategy

| Crate | Test type | What |
|---|---|---|
| `stats` | Unit | histogram merge, percentile accuracy, unit formatting (µs/ms/s, KB/MB/GB) |
| `http` | Unit | request builder output, response parser edge cases (chunked, keep-alive, errors) |
| `scripting` | Unit | each hook — load minimal Lua script, assert correct return values |
| `engine` | Integration | spin up local tokio HTTP echo server, run 2s benchmark, assert non-zero req/sec + zero errors |

No mocking — real sockets, real Lua execution.
