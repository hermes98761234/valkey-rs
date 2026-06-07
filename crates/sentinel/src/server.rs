use crate::config::SentinelConfig;
use crate::failover::force_failover;
use crate::monitor::start_monitor;
use crate::state::{MasterState, SharedState};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

/// The main Sentinel server.
pub struct SentinelServer {
    state: SharedState,
}

impl SentinelServer {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }

    /// Run the sentinel: start the monitor loop and accept TCP connections.
    pub async fn run(self, config: SentinelConfig) -> anyhow::Result<()> {
        let addr = format!("{}:{}", config.bind_addr, config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("Valkey Sentinel listening on {}", addr);

        // Start the health-check monitor in a background task.
        let monitor_state = Arc::clone(&self.state);
        tokio::spawn(async move {
            start_monitor(monitor_state, 1000).await;
        });

        // Accept loop.
        loop {
            let (stream, peer) = listener.accept().await?;
            let state = Arc::clone(&self.state);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, peer, state).await {
                    warn!("Connection error from {}: {}", peer, e);
                }
            });
        }
    }
}

/// Handle a single client connection.
async fn handle_connection(
    mut stream: TcpStream,
    _peer: SocketAddr,
    state: SharedState,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 4096];

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break; // client disconnected
        }

        let request = String::from_utf8_lossy(&buf[..n]);
        let response = dispatch(&request, &state).await;

        let resp_bytes = encode_resp(&response);
        stream.write_all(&resp_bytes).await?;
    }

    Ok(())
}

/// Parse a simple RESP array from the request and dispatch the command.
async fn dispatch(request: &str, state: &SharedState) -> RespValue {
    let args = parse_request(request);
    if args.is_empty() {
        return RespValue::Error("empty command".into());
    }

    let cmd = args[0].to_uppercase();
    let rest: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();

    match cmd.as_str() {
        "PING" => RespValue::SimpleString("PONG".into()),

        "SENTINEL" => handle_sentinel(&rest, state).await,

        "INFO" => handle_info(&rest, state).await,

        "QUIT" => RespValue::SimpleString("OK".into()),

        _ => RespValue::Error(format!("unknown command '{}'", cmd)),
    }
}

/// Handle SENTINEL subcommands.
async fn handle_sentinel(args: &[&str], state: &SharedState) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("SENTINEL requires a subcommand".into());
    }

    match args[0].to_lowercase().as_str() {
        "masters" => sentinel_masters(state).await,
        "master" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL MASTER requires a name".into());
            }
            sentinel_master(args[1], state).await
        }
        "slaves" | "replicas" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL REPLICAS requires a name".into());
            }
            sentinel_replicas(args[1], state).await
        }
        "sentinels" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL SENTINELS requires a name".into());
            }
            sentinel_sentinels(args[1], state).await
        }
        "get-master-addr-by-name" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL GET-MASTER-ADDR-BY-NAME requires a name".into());
            }
            sentinel_get_master_addr(args[1], state).await
        }
        "is-master-down-by-addr" => {
            if args.len() < 5 {
                return RespValue::Error(
                    "SENTINEL IS-MASTER-DOWN-BY-ADDR requires ip port epoch runid".into(),
                );
            }
            sentinel_is_master_down(args[1], args[2], args[3], args[4], state).await
        }
        "ckquorum" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL CKQUORUM requires a name".into());
            }
            sentinel_ckquorum(args[1], state).await
        }
        "failover" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL FAILOVER requires a name".into());
            }
            sentinel_failover(args[1], state).await
        }
        "myid" => sentinel_myid(state).await,
        "reset" => {
            if args.len() < 2 {
                return RespValue::Error("SENTINEL RESET requires a pattern".into());
            }
            sentinel_reset(args[1], state).await
        }
        "set" => {
            if args.len() < 4 {
                return RespValue::Error("SENTINEL SET requires name option value".into());
            }
            sentinel_set(args[1], args[2], args[3], state).await
        }
        _ => RespValue::Error(format!("unknown SENTINEL subcommand '{}'", args[0])),
    }
}

// ---------------------------------------------------------------------------
// SENTINEL command implementations
// ---------------------------------------------------------------------------

#[allow(clippy::vec_init_then_push)]
async fn sentinel_masters(state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let mut result = Vec::new();

    for (name, info) in masters.iter() {
        let mut fields = Vec::new();
        fields.push(RespValue::BulkString(Some(Bytes::from("name"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(name.clone()))));
        fields.push(RespValue::BulkString(Some(Bytes::from("ip"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            info.current_primary.ip().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("port"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            info.current_primary.port().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("runid"))));
        fields.push(RespValue::BulkString(None)); // unknown
        fields.push(RespValue::BulkString(Some(Bytes::from("flags"))));
        flags_str(info, &mut fields);
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "link-pending-commands",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("link-refcount"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-sent"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "last-ok-ping-reply",
        ))));
        fields.push(RespValue::Integer(
            info.last_ping_reply.map(|_| 1).unwrap_or(0),
        ));
        fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-reply"))));
        fields.push(RespValue::Integer(
            info.last_ping_reply.map(|_| 1).unwrap_or(0),
        ));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "down-after-milliseconds",
        ))));
        fields.push(RespValue::Integer(info.down_after_ms as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from("info-refresh"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("role-reported"))));
        fields.push(RespValue::BulkString(Some(Bytes::from("master"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "role-reported-time",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("config-epoch"))));
        fields.push(RespValue::Integer(info.failover_epoch as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from("num-slaves"))));
        fields.push(RespValue::Integer(info.replicas.len() as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "num-other-sentinels",
        ))));
        fields.push(RespValue::Integer(info.sentinels.len() as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from("quorum"))));
        fields.push(RespValue::Integer(info.quorum as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from("failover-timeout"))));
        fields.push(RespValue::Integer(info.failover_timeout as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from("parallel-syncs"))));
        fields.push(RespValue::Integer(1));

        result.push(RespValue::Array(Some(fields)));
    }

    RespValue::Array(Some(result))
}

fn flags_str(info: &crate::state::MasterInfo, fields: &mut Vec<RespValue>) {
    let flags = match info.state {
        MasterState::Ok => "master",
        MasterState::Sdown => "s_down,master",
        MasterState::Odown => "o_down,master",
        MasterState::Failover => "failover,master",
    };
    fields.push(RespValue::BulkString(Some(Bytes::from(flags))));
}

#[allow(clippy::vec_init_then_push)]
async fn sentinel_master(name: &str, state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let info = match masters.get(name) {
        Some(m) => m,
        None => return RespValue::Error(format!("master '{}' not found", name)),
    };

    let mut fields = Vec::new();
    fields.push(RespValue::BulkString(Some(Bytes::from("name"))));
    fields.push(RespValue::BulkString(Some(Bytes::from(name.to_string()))));
    fields.push(RespValue::BulkString(Some(Bytes::from("ip"))));
    fields.push(RespValue::BulkString(Some(Bytes::from(
        info.current_primary.ip().to_string(),
    ))));
    fields.push(RespValue::BulkString(Some(Bytes::from("port"))));
    fields.push(RespValue::BulkString(Some(Bytes::from(
        info.current_primary.port().to_string(),
    ))));
    fields.push(RespValue::BulkString(Some(Bytes::from("runid"))));
    fields.push(RespValue::BulkString(None));
    fields.push(RespValue::BulkString(Some(Bytes::from("flags"))));
    flags_str(info, &mut fields);
    fields.push(RespValue::BulkString(Some(Bytes::from(
        "link-pending-commands",
    ))));
    fields.push(RespValue::Integer(0));
    fields.push(RespValue::BulkString(Some(Bytes::from("link-refcount"))));
    fields.push(RespValue::Integer(0));
    fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-sent"))));
    fields.push(RespValue::Integer(0));
    fields.push(RespValue::BulkString(Some(Bytes::from(
        "last-ok-ping-reply",
    ))));
    fields.push(RespValue::Integer(
        info.last_ping_reply.map(|_| 1).unwrap_or(0),
    ));
    fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-reply"))));
    fields.push(RespValue::Integer(
        info.last_ping_reply.map(|_| 1).unwrap_or(0),
    ));
    fields.push(RespValue::BulkString(Some(Bytes::from(
        "down-after-milliseconds",
    ))));
    fields.push(RespValue::Integer(info.down_after_ms as i64));
    fields.push(RespValue::BulkString(Some(Bytes::from("info-refresh"))));
    fields.push(RespValue::Integer(0));
    fields.push(RespValue::BulkString(Some(Bytes::from("role-reported"))));
    fields.push(RespValue::BulkString(Some(Bytes::from("master"))));
    fields.push(RespValue::BulkString(Some(Bytes::from(
        "role-reported-time",
    ))));
    fields.push(RespValue::Integer(0));
    fields.push(RespValue::BulkString(Some(Bytes::from("config-epoch"))));
    fields.push(RespValue::Integer(info.failover_epoch as i64));
    fields.push(RespValue::BulkString(Some(Bytes::from("num-slaves"))));
    fields.push(RespValue::Integer(info.replicas.len() as i64));
    fields.push(RespValue::BulkString(Some(Bytes::from(
        "num-other-sentinels",
    ))));
    fields.push(RespValue::Integer(info.sentinels.len() as i64));
    fields.push(RespValue::BulkString(Some(Bytes::from("quorum"))));
    fields.push(RespValue::Integer(info.quorum as i64));
    fields.push(RespValue::BulkString(Some(Bytes::from("failover-timeout"))));
    fields.push(RespValue::Integer(info.failover_timeout as i64));
    fields.push(RespValue::BulkString(Some(Bytes::from("parallel-syncs"))));
    fields.push(RespValue::Integer(1));

    RespValue::Array(Some(fields))
}

#[allow(clippy::vec_init_then_push)]
async fn sentinel_replicas(name: &str, state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let info = match masters.get(name) {
        Some(m) => m,
        None => return RespValue::Error(format!("master '{}' not found", name)),
    };

    let mut result = Vec::new();
    for replica in &info.replicas {
        let mut fields = Vec::new();
        fields.push(RespValue::BulkString(Some(Bytes::from("name"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            replica.addr.to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("ip"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            replica.addr.ip().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("port"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            replica.addr.port().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("runid"))));
        fields.push(RespValue::BulkString(None));
        fields.push(RespValue::BulkString(Some(Bytes::from("flags"))));
        fields.push(RespValue::BulkString(Some(Bytes::from("slave"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "link-pending-commands",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("link-refcount"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-sent"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "last-ok-ping-reply",
        ))));
        fields.push(RespValue::Integer(1));
        fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-reply"))));
        fields.push(RespValue::Integer(1));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "down-after-milliseconds",
        ))));
        fields.push(RespValue::Integer(info.down_after_ms as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from("info-refresh"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("role-reported"))));
        fields.push(RespValue::BulkString(Some(Bytes::from("slave"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "role-reported-time",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "master-link-down-time",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "master-link-status",
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("ok"))));
        fields.push(RespValue::BulkString(Some(Bytes::from("master-host"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            info.current_primary.ip().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("master-port"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            info.current_primary.port().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("slave-priority"))));
        fields.push(RespValue::Integer(replica.priority));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "slave-repl-offset",
        ))));
        fields.push(RespValue::Integer(replica.replication_offset as i64));

        result.push(RespValue::Array(Some(fields)));
    }

    RespValue::Array(Some(result))
}

#[allow(clippy::vec_init_then_push)]
async fn sentinel_sentinels(name: &str, state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let info = match masters.get(name) {
        Some(m) => m,
        None => return RespValue::Error(format!("master '{}' not found", name)),
    };

    let mut result = Vec::new();
    for sentinel in &info.sentinels {
        let mut fields = Vec::new();
        fields.push(RespValue::BulkString(Some(Bytes::from("name"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            sentinel.run_id.as_deref().unwrap_or("?").to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("ip"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            sentinel.addr.ip().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("port"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            sentinel.addr.port().to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("runid"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            sentinel.run_id.as_deref().unwrap_or("?").to_string(),
        ))));
        fields.push(RespValue::BulkString(Some(Bytes::from("flags"))));
        fields.push(RespValue::BulkString(Some(Bytes::from("sentinel"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "link-pending-commands",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("link-refcount"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-sent"))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "last-ok-ping-reply",
        ))));
        fields.push(RespValue::Integer(1));
        fields.push(RespValue::BulkString(Some(Bytes::from("last-ping-reply"))));
        fields.push(RespValue::Integer(1));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "down-after-milliseconds",
        ))));
        fields.push(RespValue::Integer(info.down_after_ms as i64));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "last-hello-message",
        ))));
        fields.push(RespValue::Integer(0));
        fields.push(RespValue::BulkString(Some(Bytes::from("voted-leader"))));
        fields.push(RespValue::BulkString(Some(Bytes::from("?"))));
        fields.push(RespValue::BulkString(Some(Bytes::from(
            "voted-leader-epoch",
        ))));
        fields.push(RespValue::Integer(0));

        result.push(RespValue::Array(Some(fields)));
    }

    RespValue::Array(Some(result))
}

async fn sentinel_get_master_addr(name: &str, state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let info = match masters.get(name) {
        Some(m) => m,
        None => return RespValue::Error(format!("master '{}' not found", name)),
    };

    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(Bytes::from(info.current_primary.ip().to_string()))),
        RespValue::BulkString(Some(Bytes::from(info.current_primary.port().to_string()))),
    ]))
}

async fn sentinel_is_master_down(
    ip: &str,
    port: &str,
    _epoch: &str,
    _runid: &str,
    state: &SharedState,
) -> RespValue {
    let port: u16 = match port.parse() {
        Ok(p) => p,
        Err(_) => return RespValue::Error("invalid port".into()),
    };
    let addr: SocketAddr = format!("{}:{}", ip, port).parse().unwrap();

    let masters = state.masters.read().await;
    for (_name, info) in masters.iter() {
        if info.current_primary == addr {
            let down = info.state == MasterState::Sdown || info.state == MasterState::Odown;
            // Return: down, leader, epoch
            return RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from(if down { "1" } else { "0" }))),
                RespValue::BulkString(Some(Bytes::from("*"))), // no leader
                RespValue::Integer(info.failover_epoch as i64),
            ]));
        }
    }

    // Not monitored by us.
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(Bytes::from("0"))),
        RespValue::BulkString(Some(Bytes::from("*"))),
        RespValue::Integer(0),
    ]))
}

async fn sentinel_ckquorum(name: &str, state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let info = match masters.get(name) {
        Some(m) => m,
        None => return RespValue::Error(format!("master '{}' not found", name)),
    };

    let total_sentinels = 1 + info.sentinels.len() as u32; // us + peers
    let ok = total_sentinels >= info.quorum;

    if ok {
        RespValue::SimpleString(format!(
            "OK {} usable Sentinels. Quorum and failover authorization can be reached",
            total_sentinels
        ))
    } else {
        RespValue::Error(format!(
            "NOQUORUM {} usable Sentinels. Not enough to reach the quorum",
            total_sentinels
        ))
    }
}

async fn sentinel_failover(name: &str, state: &SharedState) -> RespValue {
    let ok = force_failover(Arc::clone(state), name).await;
    if ok {
        RespValue::SimpleString("OK".into())
    } else {
        RespValue::Error(format!("failover failed for '{}'", name))
    }
}

async fn sentinel_myid(state: &SharedState) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(state.myid.clone())))
}

async fn sentinel_reset(pattern: &str, state: &SharedState) -> RespValue {
    let mut masters = state.masters.write().await;
    let names_to_reset: Vec<String> = masters
        .keys()
        .filter(|n| glob_match(pattern, n))
        .cloned()
        .collect();

    let count = names_to_reset.len();
    for name in &names_to_reset {
        if let Some(m) = masters.get_mut(name) {
            m.state = MasterState::Ok;
            m.sdown_since = None;
            m.failover_epoch = 0;
            m.current_primary = m.addr;
        }
    }

    RespValue::Integer(count as i64)
}

async fn sentinel_set(name: &str, option: &str, value: &str, state: &SharedState) -> RespValue {
    let mut masters = state.masters.write().await;
    let info = match masters.get_mut(name) {
        Some(m) => m,
        None => return RespValue::Error(format!("master '{}' not found", name)),
    };

    match option.to_lowercase().as_str() {
        "down-after-milliseconds" => {
            if let Ok(ms) = value.parse::<u64>() {
                info.down_after_ms = ms;
                RespValue::SimpleString("OK".into())
            } else {
                RespValue::Error("invalid value".into())
            }
        }
        "failover-timeout" => {
            if let Ok(ms) = value.parse::<u64>() {
                info.failover_timeout = ms;
                RespValue::SimpleString("OK".into())
            } else {
                RespValue::Error("invalid value".into())
            }
        }
        "quorum" => {
            if let Ok(q) = value.parse::<u32>() {
                info.quorum = q;
                RespValue::SimpleString("OK".into())
            } else {
                RespValue::Error("invalid value".into())
            }
        }
        _ => RespValue::Error(format!("unknown option '{}'", option)),
    }
}

async fn handle_info(_args: &[&str], state: &SharedState) -> RespValue {
    let masters = state.masters.read().await;
    let mut lines = Vec::new();
    lines.push("# Sentinel".to_string());
    lines.push(format!("sentinel_masters:{}", masters.len()));
    lines.push("sentinel_tilt:0".to_string());
    lines.push("sentinel_running_scripts:0".to_string());
    lines.push("sentinel_scripts_queue_length:0".to_string());

    for (name, info) in masters.iter() {
        lines.push(format!(
            "master{}:name={},status={},address={}:{},slaves={},sentinels={}",
            name,
            name,
            match info.state {
                MasterState::Ok => "ok",
                MasterState::Sdown => "sdown",
                MasterState::Odown => "odown",
                MasterState::Failover => "failover",
            },
            info.current_primary.ip(),
            info.current_primary.port(),
            info.replicas.len(),
            info.sentinels.len() + 1,
        ));
    }

    RespValue::BulkString(Some(Bytes::from(lines.join("\r\n"))))
}

// ---------------------------------------------------------------------------
// RESP encoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn error(s: impl Into<String>) -> Self {
        RespValue::Error(s.into())
    }
}

fn encode_resp(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_resp_inner(value, &mut out);
    out
}

fn encode_resp_inner(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            out.push(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(Some(data)) => {
            out.push(b'$');
            out.extend_from_slice(data.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            out.extend_from_slice(b"$-1\r\n");
        }
        RespValue::Array(Some(items)) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_resp_inner(item, out);
            }
        }
        RespValue::Array(None) => {
            out.extend_from_slice(b"*-1\r\n");
        }
    }
}

// ---------------------------------------------------------------------------
// Simple request parser
// ---------------------------------------------------------------------------

/// Parse a RESP request into a list of string arguments.
/// Handles both raw text commands (e.g. "PING") and RESP arrays.
fn parse_request(input: &str) -> Vec<String> {
    let input = input.trim();

    // Check if it's a RESP array.
    if input.starts_with('*') {
        parse_resp_array(input)
    } else {
        // Plain text command: split by whitespace.
        input.split_whitespace().map(String::from).collect()
    }
}

fn parse_resp_array(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut lines = input.lines();

    // First line: *<count>\r\n
    let first = match lines.next() {
        Some(l) => l,
        None => return result,
    };

    if !first.starts_with('*') {
        return result;
    }

    let count: usize = match first[1..].trim().parse() {
        Ok(n) => n,
        Err(_) => return result,
    };

    let mut in_bulk = false;

    for line in lines {
        if in_bulk {
            result.push(line.to_string());
            in_bulk = false;
            if result.len() >= count {
                break;
            }
            continue;
        }

        if let Some(stripped) = line.strip_prefix('$') {
            let _bulk_len: usize = match stripped.trim().parse() {
                Ok(n) => n,
                Err(_) => return result,
            };
            in_bulk = true;
        } else if !line.is_empty() && !line.starts_with('+') && !line.starts_with('-') {
            // Inline argument.
            result.push(line.to_string());
            if result.len() >= count {
                break;
            }
        }
    }

    result
}

/// Simple glob matching for SENTINEL RESET pattern.
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            return text.starts_with(parts[0]) && text.ends_with(parts[1]);
        }
    }
    pattern == text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request_plain() {
        let args = parse_request("PING");
        assert_eq!(args, vec!["PING"]);
    }

    #[test]
    fn test_parse_request_resp() {
        let input = "*2\r\n$7\r\nSENTINEL\r\n$6\r\nMASTERS\r\n";
        let args = parse_request(input);
        assert_eq!(args, vec!["SENTINEL", "MASTERS"]);
    }

    #[test]
    fn test_encode_simple_string() {
        let v = RespValue::SimpleString("PONG".into());
        let encoded = encode_resp(&v);
        assert_eq!(String::from_utf8_lossy(&encoded), "+PONG\r\n");
    }

    #[test]
    fn test_encode_error() {
        let v = RespValue::Error("ERR foo".into());
        let encoded = encode_resp(&v);
        assert_eq!(String::from_utf8_lossy(&encoded), "-ERR foo\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let v = RespValue::Integer(42);
        let encoded = encode_resp(&v);
        assert_eq!(String::from_utf8_lossy(&encoded), ":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let v = RespValue::BulkString(Some(Bytes::from("hello")));
        let encoded = encode_resp(&v);
        assert_eq!(String::from_utf8_lossy(&encoded), "$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_array() {
        let v = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from("a"))),
            RespValue::BulkString(Some(Bytes::from("bb"))),
        ]));
        let encoded = encode_resp(&v);
        assert_eq!(
            String::from_utf8_lossy(&encoded),
            "*2\r\n$1\r\na\r\n$2\r\nbb\r\n"
        );
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("master*", "master1"));
        assert!(glob_match("*1", "master1"));
        assert!(!glob_match("master*", "slave1"));
        assert!(glob_match("exact", "exact"));
    }
}
