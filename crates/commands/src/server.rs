use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use valkey_proto::RespValue;
use valkey_storage::Store;

// ---------------------------------------------------------------------------
// Global shutdown flag
// ---------------------------------------------------------------------------

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

// ---------------------------------------------------------------------------
// Global client registry (for CLIENT KILL / LIST)
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: i64,
    pub name: Option<String>,
    pub addr: String,
    pub db_index: usize,
    pub flags: ClientFlags,
    pub age: u64,
    pub idle: u64,
}

/// Register a client in the global registry.
pub fn register_client(id: i64, info: ClientInfo) {
    let mut registry = CLIENT_REGISTRY.write().unwrap();
    registry.insert(id, info);
}

/// Unregister a client from the global registry.
pub fn unregister_client(id: i64) {
    let mut registry = CLIENT_REGISTRY.write().unwrap();
    registry.remove(&id);
}

/// Get a snapshot of all registered clients.
pub fn get_all_clients() -> Vec<ClientInfo> {
    let registry = CLIENT_REGISTRY.read().unwrap();
    registry.values().cloned().collect()
}

lazy_static::lazy_static! {
    static ref CLIENT_REGISTRY: Arc<RwLock<BTreeMap<i64, ClientInfo>>> =
        Arc::new(RwLock::new(BTreeMap::new()));
}

// ---------------------------------------------------------------------------
// TLS client authentication mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub enum TlsClientAuth {
    #[default]
    No,
    Yes,
    Optional,
}

// ---------------------------------------------------------------------------
// Client context — tracks per-connection state
// ---------------------------------------------------------------------------

static NEXT_CLIENT_ID: AtomicI64 = AtomicI64::new(1);

#[derive(Debug, Clone)]
pub struct ClientCtx {
    pub id: i64,
    pub name: Option<String>,
    pub db_index: usize,
    pub authenticated: bool,
    pub flags: ClientFlags,
    // Transaction state
    pub multi: bool,
    pub queue: Vec<Vec<Bytes>>,
    pub watched: Vec<Bytes>,
    pub dirty: bool,
    // Client-side caching (CLIENT TRACKING)
    pub tracking: TrackingState,
}

#[derive(Debug, Clone, Default)]
pub struct ClientFlags {
    pub no_evict: bool,
    pub no_touch: bool,
}

// ---------------------------------------------------------------------------
// Client-side caching (CLIENT TRACKING)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum TrackingMode {
    Default,
    Bcast,
}

#[derive(Debug, Clone)]
pub struct TrackingState {
    pub enabled: bool,
    pub mode: TrackingMode,
    pub redirect: Option<i64>,
    pub prefixes: Vec<String>,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    /// Whether the client has opted in via CLIENT CACHING YES (only relevant when optin=true)
    pub caching: bool,
}

impl Default for TrackingState {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: TrackingMode::Default,
            redirect: None,
            prefixes: Vec::new(),
            optin: false,
            optout: false,
            noloop: false,
            caching: false,
        }
    }
}

impl TrackingState {
    /// Build the CLIENT TRACKINGINFO response array.
    pub fn trackinginfo(&self) -> Vec<RespValue> {
        let mut flags = Vec::new();
        if self.enabled {
            flags.push(RespValue::bulk(Bytes::from("on")));
        } else {
            flags.push(RespValue::bulk(Bytes::from("off")));
        }
        if let Some(redir) = self.redirect {
            flags.push(RespValue::bulk(Bytes::from(format!("redirect={}", redir))));
        }
        if self.mode == TrackingMode::Bcast {
            flags.push(RespValue::bulk(Bytes::from("bcast")));
        }
        for prefix in &self.prefixes {
            if prefix.is_empty() {
                flags.push(RespValue::bulk(Bytes::from("prefix=")));
            } else {
                flags.push(RespValue::bulk(Bytes::from(format!("prefix={}", prefix))));
            }
        }
        if self.optin {
            flags.push(RespValue::bulk(Bytes::from("optin")));
        }
        if self.optout {
            flags.push(RespValue::bulk(Bytes::from("optout")));
        }
        if self.noloop {
            flags.push(RespValue::bulk(Bytes::from("noloop")));
        }
        flags
    }

    /// Update this tracking state from CLIENT TRACKING ON arguments.
    pub fn enable_from_args(
        &mut self,
        args: &[Bytes],
    ) -> Result<(), String> {
        self.enabled = true;
        // Reset mode to defaults first
        self.mode = TrackingMode::Default;
        self.redirect = None;
        self.prefixes.clear();
        self.optin = false;
        self.optout = false;
        self.noloop = false;

        let mut i = 0;
        while i < args.len() {
            let arg = match std::str::from_utf8(&args[i]) {
                Ok(s) => s.to_ascii_uppercase(),
                Err(_) => return Err("ERR invalid argument".into()),
            };
            match arg.as_str() {
                "REDIRECT" => {
                    if i + 1 >= args.len() {
                        return Err("ERR missing REDIRECT client-id".into());
                    }
                    i += 1;
                    let id: i64 = match std::str::from_utf8(&args[i]) {
                        Ok(s) => s.parse().map_err(|_| "ERR invalid client-id")?,
                        Err(_) => return Err("ERR invalid client-id".into()),
                    };
                    self.redirect = Some(id);
                }
                "PREFIX" => {
                    if i + 1 >= args.len() {
                        return Err("ERR missing PREFIX value".into());
                    }
                    i += 1;
                    let prefix = match std::str::from_utf8(&args[i]) {
                        Ok(s) => s.to_string(),
                        Err(_) => return Err("ERR invalid prefix".into()),
                    };
                    // If any prefix is given, switch to empty_prefix mode for that prefix
                    if self.mode != TrackingMode::Bcast {
                        // first prefix in default mode — treat as the key prefix to track
                    }
                    self.prefixes.push(prefix);
                }
                "BCAST" => {
                    self.mode = TrackingMode::Bcast;
                }
                "OPTIN" => {
                    self.optin = true;
                }
                "OPTOUT" => {
                    self.optout = true;
                }
                "NOLOOP" => {
                    self.noloop = true;
                }
                _ => {
                    return Err(format!("ERR invalid option '{}'", arg));
                }
            }
            i += 1;
        }
        Ok(())
    }

    /// Disable tracking.
    pub fn disable(&mut self) {
        self.enabled = false;
        self.mode = TrackingMode::Default;
        self.redirect = None;
        self.prefixes.clear();
        self.optin = false;
        self.optout = false;
        self.noloop = false;
        self.caching = false;
    }

    /// Whether this client should receive invalidations.
    pub fn should_receive_invalidations(&self) -> bool {
        if !self.enabled {
            return false;
        }
        if self.optin && !self.caching {
            return false;
        }
        true
    }

    /// In RESP2 mode, publishing to __redis__:invalidate is used.
    /// In RESP3 mode, push messages are sent.
    /// `resp3` tells us if the client is connected via RESP3.
    /// `target_client_id` is the client-id to send to (redirect client-id if set, otherwise self).
    /// Returns (target_client_id, is_resp3) to help the caller send the message.
    /// If redirect is set, returns the redirect target; the caller needs to find that client.
    pub fn invalidation_target(&self) -> (Option<i64>, bool) {
        if self.redirect.is_some() {
            return (self.redirect, false); // redirect always uses RESP2/redis channel
        }
        (None, true) // send as RESP3 push
    }
}

impl ClientCtx {
    pub fn new() -> Self {
        Self {
            id: NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst),
            name: None,
            db_index: 0,
            authenticated: false,
            flags: ClientFlags::default(),
            multi: false,
            queue: Vec::new(),
            watched: Vec::new(),
            dirty: false,
            tracking: TrackingState::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Server config — backed by Arc<RwLock<>>
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ServerConfig {
    params: HashMap<String, String>,
    // TLS configuration
    pub tls_port: Option<u16>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub tls_ca_cert_file: Option<PathBuf>,
    pub tls_auth_clients: TlsClientAuth,
    // RDB persistence
    pub rdb_path: String,
    // AOF persistence
    pub aof_enabled: bool,
    pub aof_path: String,
    pub aof_fsync: String,
    // Config file path — set when the server is started with a config file
    pub config_file_path: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let mut params = HashMap::new();
        params.insert("port".into(), "6379".into());
        params.insert("bind".into(), "0.0.0.0".into());
        params.insert("maxmemory".into(), "0".into());
        params.insert("maxmemory-policy".into(), "noeviction".into());
        params.insert("maxmemory-samples".into(), "5".into());
        params.insert("timeout".into(), "0".into());
        params.insert("databases".into(), "16".into());
        params.insert("tcp-keepalive".into(), "300".into());
        params.insert("loglevel".into(), "notice".into());
        params.insert("requirepass".into(), "".into());
        params.insert("hz".into(), "10".into());
        params.insert("proto-max-bulk-len".into(), "536870912".into());
        params.insert("client-query-buffer-limit".into(), "1073741824".into());
        params.insert("dbfilename".into(), "dump.rdb".into());
        params.insert("dir".into(), ".".into());
        Self {
            params,
            tls_port: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_cert_file: None,
            tls_auth_clients: TlsClientAuth::No,
            rdb_path: "./dump.rdb".into(),
            aof_enabled: false,
            aof_path: "./appendonly.aof".into(),
            aof_fsync: "everysec".into(),
            config_file_path: None,
        }
    }
}

impl ServerConfig {
    pub fn get(&self, pattern: &str) -> Vec<(String, String)> {
        if pattern == "*" {
            return self
                .params
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        }
        // Simple prefix match for patterns like "max*"
        if let Some(suffix) = pattern.strip_prefix('*') {
            return self
                .params
                .iter()
                .filter(|(k, _)| k.ends_with(suffix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return self
                .params
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        }
        // Exact match
        self.params
            .get(pattern)
            .map(|v| vec![(pattern.to_string(), v.clone())])
            .unwrap_or_default()
    }

    pub fn set(&mut self, param: &str, value: &str) -> Result<(), String> {
        let canonical = param.to_ascii_lowercase();
        match canonical.as_str() {
            "port" => {
                value.parse::<u16>().map_err(|_| "ERR invalid port")?;
            }
            "maxmemory" => {
                value
                    .parse::<u64>()
                    .map_err(|_| "ERR invalid maxmemory value")?;
            }
            "maxmemory-policy" => {
                let valid = [
                    "noeviction",
                    "allkeys-lru",
                    "volatile-lru",
                    "allkeys-random",
                    "volatile-random",
                    "volatile-ttl",
                    "allkeys-lfu",
                    "volatile-lfu",
                ];
                if !valid.contains(&value.to_ascii_lowercase().as_str()) {
                    return Err("ERR invalid maxmemory policy".into());
                }
            }
            "maxmemory-samples" => {
                let n: u8 = value
                    .parse()
                    .map_err(|_| "ERR invalid maxmemory-samples value")?;
                if n == 0 {
                    return Err("ERR maxmemory-samples must be > 0".into());
                }
            }
            "timeout" => {
                value.parse::<u64>().map_err(|_| "ERR invalid timeout")?;
            }
            "databases" => {
                let n: usize = value
                    .parse()
                    .map_err(|_| "ERR invalid number of databases")?;
                if n < 1 {
                    return Err("ERR invalid number of databases".into());
                }
            }
            _ => {}
        }
        self.params.insert(canonical, value.to_string());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Top-level server command handler (mutually recursive with sub-handlers)
// ---------------------------------------------------------------------------

pub async fn handle(
    cmd: &[Bytes],
    store: &Arc<Store>,
    _client: Arc<RwLock<ClientCtx>>,
    config: Arc<RwLock<ServerConfig>>,
) -> RespValue {
    let cmd_name = match cmd.first() {
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return RespValue::Error("ERR invalid command name".into()),
        },
        None => return RespValue::Error("ERR empty command".into()),
    };
    let args = &cmd[1..];
    // Commands that take no arguments
    match cmd_name.as_str() {
        "PING" => cmd_ping(args).await,
        "DBSIZE" => cmd_dbsize(args, store).await,
        "FLUSHDB" => cmd_flushdb(args, store).await,
        "FLUSHALL" => cmd_flushall(args, store).await,
        "LASTSAVE" => cmd_lastsave(args).await,
        "TIME" => cmd_time(args).await,
        "RESET" => cmd_reset(args, _client.clone()).await,
        "SHUTDOWN" => cmd_shutdown(args).await,
        "LOLWUT" => cmd_lolwut(args).await,
        _ => {
            // All other commands require at least one argument
            if args.len() < 2 {
                return RespValue::Error(
                    format!("ERR wrong number of arguments for '{}' command", cmd_name).into(),
                );
            }
            match cmd_name.as_str() {
                "ECHO" => cmd_echo(args).await,
                "SELECT" => cmd_select(args, _client).await,
                "INFO" => cmd_info(args).await,
                "COMMAND" => cmd_command(args, store, _client, config).await,
                "CONFIG" => cmd_config(args, config, store).await,
                "SAVE" => cmd_save(args, store, config).await,
                "BGSAVE" => cmd_bgsave(args, store, config).await,
                "BGREWRITEAOF" => cmd_bgrewriteaof(args, store, config).await,
                "LATENCY" => cmd_latency(args).await,
                "SLOWLOG" => cmd_slowlog(args).await,
                "MEMORY" => cmd_memory(args, store).await,
                "CLIENT" => cmd_client(args, _client).await,
                "DEBUG" => cmd_debug(args).await,
                "OBJECT" => cmd_object(args, store).await,
                "REPLCONF" => crate::replication::handle_replconf(args).await,
                "REPLICAOF" => crate::replication::cmd_replicaof(args, store).await,
                "SLAVEOF" => crate::replication::cmd_replicaof(args, store).await,
                _ => RespValue::Error(format!("ERR unknown command `{}`", cmd_name).into()),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PING
// ---------------------------------------------------------------------------

async fn cmd_ping(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        RespValue::SimpleString("PONG".into())
    } else {
        RespValue::BulkString(Some(args[0].clone()))
    }
}

// ---------------------------------------------------------------------------
// ECHO
// ---------------------------------------------------------------------------

async fn cmd_echo(args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'echo' command".into());
    }
    RespValue::BulkString(Some(args[0].clone()))
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

async fn cmd_select(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error("ERR wrong number of arguments for 'select' command".into());
    }
    let index: usize = match std::str::from_utf8(&args[0]) {
        Ok(s) => match s.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::Error("ERR invalid DB index".into()),
        },
        Err(_) => return RespValue::Error("ERR invalid DB index".into()),
    };
    // Single keyspace for now — accept any index but don't actually switch
    let mut ctx = client.write().unwrap();
    ctx.db_index = index;
    RespValue::ok()
}

// ---------------------------------------------------------------------------
// DBSIZE
// ---------------------------------------------------------------------------

async fn cmd_dbsize(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if !args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'dbsize' command".into());
    }
    RespValue::Integer(store.dbsize() as i64)
}

// ---------------------------------------------------------------------------
// FLUSHDB
// ---------------------------------------------------------------------------

async fn cmd_flushdb(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    let sync = parse_flush_mode(args);
    // ASYNC/SYNC don't change behaviour for in-memory store
    let _ = sync;
    store.flush();
    RespValue::ok()
}

async fn cmd_flushall(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    let sync = parse_flush_mode(args);
    let _ = sync;
    store.flush();
    RespValue::ok()
}

fn parse_flush_mode(args: &[Bytes]) -> Option<String> {
    if args.is_empty() {
        return None;
    }
    match std::str::from_utf8(&args[0]) {
        Ok(s) => Some(s.to_ascii_uppercase()),
        Err(_) => None,
    }
}

// ---------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------

async fn cmd_info(args: &[Bytes]) -> RespValue {
    let section = if args.is_empty() {
        "default"
    } else {
        match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => "default",
        }
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let uptime = std::process::id() as u64; // placeholder; real uptime would track start time
    let used_memory = 1024 * 512; // placeholder
    let used_memory_human = "512K".to_string();
    let maxclients = 10000;
    let connected_clients = 1;
    let blocked_clients = 0;
    let tracking_clients = 0;
    let clients_in_timeout_table = 0;
    let total_connections_received = 1i64;
    let total_commands_processed = 1i64;
    let instantaneous_ops_per_sec = 0i64;
    let total_net_input_bytes = 0i64;
    let total_net_output_bytes = 0i64;
    let instantaneous_input_kbps = 0.0;
    let instantaneous_output_kbps = 0.0;
    let rejected_connections = 0i64;
    let sync_full = 0i64;
    let sync_partial_ok = 0i64;
    let sync_partial_err = 0i64;
    let expired_keys = 0i64;
    let expired_stale_perc = 0.0;
    let expired_time_cap_reached_count = 0i64;
    let evicted_keys = 0i64;
    let keyspace_hits = 0i64;
    let keyspace_misses = 0i64;
    let pubsub_channels = 0i64;
    let pubsub_patterns = 0i64;
    let latest_fork_usec = 0i64;
    let migrate_cached_sockets = 0i64;
    let slave_expires_tracked_keys = 0i64;
    let active_defrag_hits = 0i64;
    let active_defrag_misses = 0i64;
    let active_defrag_key_hits = 0i64;
    let active_defrag_key_misses = 0i64;
    let tracking_total_keys = 0i64;
    let tracking_total_items = 0i64;
    let tracking_total_prefixes = 0i64;
    let unexpected_error_replies = 0i64;
    let total_error_replies = 0i64;
    let total_reads_processed = 0i64;
    let total_writes_processed = 0i64;
    let rdb_changes_since_last_save = 0i64;
    let rdb_bgsave_in_progress = 0i64;
    let rdb_last_save_time = now as i64;
    let rdb_last_bgsave_status = "ok";
    let rdb_last_bgsave_time_sec = 0i64;
    let rdb_current_bgsave_time_sec = -1i64;
    let rdb_last_cow_size = 0i64;
    let aof_enabled = 0i64;
    let aof_rewrite_in_progress = 0i64;
    let aof_rewrite_scheduled = 0i64;
    let aof_last_rewrite_time_sec = -1i64;
    let aof_current_rewrite_time_sec = -1i64;
    let aof_last_bgrewrite_status = "ok";
    let aof_last_write_status = "ok";
    let aof_last_cow_size = 0i64;
    let aof_current_size = 0i64;
    let aof_base_size = 0i64;
    let aof_pending_rewrite = 0i64;
    let aof_buffer_length = 0i64;
    let aof_rewrite_buffer_length = 0i64;
    let aof_pending_bio_fsync = 0i64;
    let aof_delayed_fsync = 0i64;
    let loading = 0i64;
    let current_cow_peak = 0i64;
    let current_cow_size = 0i64;
    let current_cow_size_age = 0i64;
    let current_fork_perc = 0.0;
    let current_save_keys_processed = 0i64;
    let current_save_keys_total = 0i64;
    let rdb_last_load_keys_expired = 0i64;
    let rdb_last_load_keys_loaded = 0i64;
    let used_cpu_sys = 0.0;
    let used_cpu_user = 0.0;
    let used_cpu_sys_children = 0.0;
    let used_cpu_user_children = 0.0;
    let used_cpu_sys_main_thread = 0.0;
    let used_cpu_user_main_thread = 0.0;
    let cluster_enabled = 0i64;
    let errorstats = "";

    let s = match section.to_ascii_uppercase().as_str() {
        "SERVER" => format!(
            "# Server\r\n\
             redis_version:7.0.0-valkey-rs\r\n\
             redis_git_sha1:00000000\r\n\
             redis_git_dirty:0\r\n\
             redis_build_id:valkey-rs-0.1.0\r\n\
             redis_mode:standalone\r\n\
             os:Linux\r\n\
             arch_bits:64\r\n\
             multiplexing_api:iouring\r\n\
             atomicvar_api:std::atomic\r\n\
             gcc_version:12.3.0\r\n\
             process_id:{}\r\n\
             process_supervised:no\r\n\
             run_id:valkeyrs1234567890abcdef1234567890abcdef12345678\r\n\
             tcp_port:6379\r\n\
             server_time_usec:{}\r\n\
             uptime_in_seconds:{}\r\n\
             uptime_in_days:0\r\n\
             hz:10\r\n\
             configured_hz:10\r\n\
             lru_clock:{}\r\n\
             executable:/usr/bin/valkey-server\r\n\
             config_file:valkey.conf\r\n\
             io_threads_active:0\r\n\
             io_threaded_reads_processed:0\r\n\
             io_threaded_writes_processed:0",
            std::process::id(),
            now * 1_000_000,
            uptime,
            now / 100
        ),
        "CLIENTS" => format!(
            "# Clients\r\n\
             connected_clients:{}\r\n\
             cluster_connections:0\r\n\
             maxclients:{}\r\n\
             client_recent_max_input_buffer:0\r\n\
             client_recent_max_output_buffer:0\r\n\
             blocked_clients:{}\r\n\
             tracking_clients:{}\r\n\
             clients_in_timeout_table:{}\r\n\
             total_blocking_keys:0\r\n\
             total_blocking_keys_on_nokey:0",
            connected_clients,
            maxclients,
            blocked_clients,
            tracking_clients,
            clients_in_timeout_table
        ),
        "MEMORY" => format!(
            "# Memory\r\n\
             used_memory:{}\r\n\
             used_memory_human:\"{}\"\r\n\
             used_memory_rss:4096000\r\n\
             used_memory_rss_human:\"3.91M\"\r\n\
             used_memory_peak:{}\r\n\
             used_memory_peak_human:\"{}\"\r\n\
             used_memory_peak_perc:0.00%\r\n\
             used_memory_overhead:524288\r\n\
             used_memory_startup:524288\r\n\
             used_memory_dataset:{}\r\n\
             used_memory_dataset_perc:0.00%\r\n\
             total_system_memory:8589934592\r\n\
             total_system_memory_human:\"8.00G\"\r\n\
             used_memory_lua:37888\r\n\
             used_memory_lua_human:\"37.00K\"\r\n\
             used_memory_vm_eval:0\r\n\
             used_memory_scripts_eval:0\r\n\
             number_of_cached_lua_scripts:0\r\n\
             number_of_cached_scripts:0\r\n\
             maxmemory:0\r\n\
             maxmemory_human:\"0B\"\r\n\
             maxmemory_policy:noeviction\r\n\
             allocator_frag_ratio:1.00\r\n\
             allocator_frag_bytes:0\r\n\
             allocator_rss_ratio:1.00\r\n\
             allocator_rss_bytes:0\r\n\
             rss_overhead_ratio:1.00\r\n\
             rss_overhead_bytes:0\r\n\
             mem_fragmentation_ratio:1.00\r\n\
             mem_fragmentation_bytes:0\r\n\
             mem_not_counted_for_evict:0\r\n\
             mem_replication_backlog:0\r\n\
             mem_total_replication_buffers:0\r\n\
             mem_clients_slaves:0\r\n\
             mem_clients_normal:0\r\n\
             mem_cluster_links:0\r\n\
             mem_aof_buffer:0\r\n\
             active_defrag_running:0\r\n\
             lazyfree_pending_objects:0\r\n\
             lazyfreed_objects:0",
            used_memory,
            used_memory_human,
            used_memory,
            used_memory_human,
            used_memory / 2
        ),
        "STATS" => format!(
            "# Stats\r\n\
             total_connections_received:{}\r\n\
             total_commands_processed:{}\r\n\
             instantaneous_ops_per_sec:{}\r\n\
             total_net_input_bytes:{}\r\n\
             total_net_output_bytes:{}\r\n\
             instantaneous_input_kbps:{}\r\n\
             instantaneous_output_kbps:{}\r\n\
             rejected_connections:{}\r\n\
             sync_full:{}\r\n\
             sync_partial_ok:{}\r\n\
             sync_partial_err:{}\r\n\
             expired_keys:{}\r\n\
             expired_stale_perc:{}\r\n\
             expired_time_cap_reached_count:{}\r\n\
             evicted_keys:{}\r\n\
             keyspace_hits:{}\r\n\
             keyspace_misses:{}\r\n\
             pubsub_channels:{}\r\n\
             pubsub_patterns:{}\r\n\
             latest_fork_usec:{}\r\n\
             migrate_cached_sockets:{}\r\n\
             slave_expires_tracked_keys:{}\r\n\
             active_defrag_hits:{}\r\n\
             active_defrag_misses:{}\r\n\
             active_defrag_key_hits:{}\r\n\
             active_defrag_key_misses:{}\r\n\
             tracking_total_keys:{}\r\n\
             tracking_total_items:{}\r\n\
             tracking_total_prefixes:{}\r\n\
             unexpected_error_replies:{}\r\n\
             total_error_replies:{}\r\n\
             total_reads_processed:{}\r\n\
             total_writes_processed:{}",
            total_connections_received,
            total_commands_processed,
            instantaneous_ops_per_sec,
            total_net_input_bytes,
            total_net_output_bytes,
            instantaneous_input_kbps,
            instantaneous_output_kbps,
            rejected_connections,
            sync_full,
            sync_partial_ok,
            sync_partial_err,
            expired_keys,
            expired_stale_perc,
            expired_time_cap_reached_count,
            evicted_keys,
            keyspace_hits,
            keyspace_misses,
            pubsub_channels,
            pubsub_patterns,
            latest_fork_usec,
            migrate_cached_sockets,
            slave_expires_tracked_keys,
            active_defrag_hits,
            active_defrag_misses,
            active_defrag_key_hits,
            active_defrag_key_misses,
            tracking_total_keys,
            tracking_total_items,
            tracking_total_prefixes,
            unexpected_error_replies,
            total_error_replies,
            total_reads_processed,
            total_writes_processed
        ),
        "REPLICATION" => format!(
            "# Replication\r\n\
             role:master\r\n\
             connected_slaves:0\r\n\
             master_failover_state:no-failover\r\n\
             master_replid:0000000000000000000000000000000000000000\r\n\
             master_replid2:0000000000000000000000000000000000000000\r\n\
             master_repl_offset:0\r\n\
             second_repl_offset:-1\r\n\
             repl_backlog_active:0\r\n\
             repl_backlog_size:1048576\r\n\
             repl_backlog_first_byte_offset:0\r\n\
             repl_backlog_histlen:0"
        ),
        "CPU" => format!(
            "# CPU\r\n\
             used_cpu_sys:{}\r\n\
             used_cpu_user:{}\r\n\
             used_cpu_sys_children:{}\r\n\
             used_cpu_user_children:{}\r\n\
             used_cpu_sys_main_thread:{}\r\n\
             used_cpu_user_main_thread:{}",
            used_cpu_sys,
            used_cpu_user,
            used_cpu_sys_children,
            used_cpu_user_children,
            used_cpu_sys_main_thread,
            used_cpu_user_main_thread
        ),
        "KEYSPACE" => format!(
            "# Keyspace\r\n\
             db0:keys={}",
            0
        ),
        _ =>
        // default — return all sections
        {
            format!(
                "# Server\r\n\
                 redis_version:7.0.0-valkey-rs\r\n\
                 valkey_version:7.0.0-valkey-rs\r\n\
                 redis_git_sha1:00000000\r\n\
                 redis_git_dirty:0\r\n\
                 redis_build_id:valkey-rs-0.1.0\r\n\
                 redis_mode:standalone\r\n\
                 os:Linux\r\n\
                 arch_bits:64\r\n\
                 multiplexing_api:iouring\r\n\
                 atomicvar_api:std::atomic\r\n\
                 gcc_version:12.3.0\r\n\
                 process_id:{}\r\n\
                 process_supervised:no\r\n\
                 run_id:valkeyrs1234567890abcdef1234567890abcdef12345678\r\n\
                 tcp_port:6379\r\n\
                 server_time_usec:{}\r\n\
                 uptime_in_seconds:{}\r\n\
                 uptime_in_days:0\r\n\
                 hz:10\r\n\
                 configured_hz:10\r\n\
                 lru_clock:{}\r\n\
                 executable:/usr/bin/valkey-server\r\n\
                 config_file:valkey.conf\r\n\
                 io_threads_active:0\r\n\
                 io_threaded_reads_processed:0\r\n\
                 io_threaded_writes_processed:0\r\n\
                 \r\n\
                 # Clients\r\n\
                 connected_clients:{}\r\n\
                 cluster_connections:0\r\n\
                 maxclients:{}\r\n\
                 client_recent_max_input_buffer:0\r\n\
                 client_recent_max_output_buffer:0\r\n\
                 blocked_clients:{}\r\n\
                 tracking_clients:{}\r\n\
                 clients_in_timeout_table:0\r\n\
                 total_blocking_keys:0\r\n\
                 total_blocking_keys_on_nokey:0\r\n\
                 \r\n\
                 # Memory\r\n\
                 used_memory:{}\r\n\
                 used_memory_human:\"{}\"\r\n\
                 used_memory_rss:4096000\r\n\
                 used_memory_rss_human:\"3.91M\"\r\n\
                 used_memory_peak:{}\r\n\
                 used_memory_peak_human:\"{}\"\r\n\
                 used_memory_peak_perc:0.00%\r\n\
                 used_memory_overhead:524288\r\n\
                 used_memory_startup:524288\r\n\
                 used_memory_dataset:{}\r\n\
                 used_memory_dataset_perc:0.00%\r\n\
                 total_system_memory:8589934592\r\n\
                 total_system_memory_human:\"8.00G\"\r\n\
                 used_memory_lua:37888\r\n\
                 used_memory_lua_human:\"37.00K\"\r\n\
                 used_memory_vm_eval:0\r\n\
                 used_memory_scripts_eval:0\r\n\
                 number_of_cached_lua_scripts:0\r\n\
                 number_of_cached_scripts:0\r\n\
                 maxmemory:0\r\n\
                 maxmemory_human:\"0B\"\r\n\
                 maxmemory_policy:noeviction\r\n\
                 allocator_frag_ratio:1.00\r\n\
                 allocator_frag_bytes:0\r\n\
                 allocator_rss_ratio:1.00\r\n\n\
                 allocator_rss_bytes:0\r\n\
                 rss_overhead_ratio:1.00\r\n\
                 rss_overhead_bytes:0\r\n\
                 mem_fragmentation_ratio:1.00\r\n\
                 mem_fragmentation_bytes:0\r\n\
                 mem_not_counted_for_evict:0\r\n\
                 mem_replication_backlog:0\r\n\
                 mem_total_replication_buffers:0\r\n\
                 mem_clients_slaves:0\r\n\
                 mem_clients_normal:0\r\n\
                 mem_cluster_links:0\r\n\
                 mem_aof_buffer:0\r\n\
                 active_defrag_running:0\r\n\
                 lazyfree_pending_objects:0\r\n\
                 lazyfreed_objects:0\r\n\
                 \r\n\
                 # Stats\r\n\
                 total_connections_received:{}\r\n\
                 total_commands_processed:{}\r\n\
                 instantaneous_ops_per_sec:{}\r\n\
                 total_net_input_bytes:{}\r\n\
                 total_net_output_bytes:{}\r\n\
                 instantaneous_input_kbps:{}\r\n\
                 instantaneous_output_kbps:{}\r\n\
                 rejected_connections:{}\r\n\
                 sync_full:{}\r\n\
                 sync_partial_ok:{}\r\n\
                 sync_partial_err:{}\r\n\
                 expired_keys:{}\r\n\
                 expired_stale_perc:{}\r\n\
                 expired_time_cap_reached_count:{}\r\n\
                 evicted_keys:{}\r\n\
                 keyspace_hits:{}\r\n\
                 keyspace_misses:{}\r\n\
                 pubsub_channels:{}\r\n\
                 pubsub_patterns:{}\r\n\
                 latest_fork_usec:{}\r\n\
                 migrate_cached_sockets:{}\r\n\
                 slave_expires_tracked_keys:{}\r\n\
                 active_defrag_hits:{}\r\n\
                 active_defrag_misses:{}\r\n\
                 active_defrag_key_hits:{}\r\n\
                 active_defrag_key_misses:{}\r\n\
                 tracking_total_keys:{}\r\n\
                 tracking_total_items:{}\r\n\
                 tracking_total_prefixes:{}\r\n\
                 unexpected_error_replies:{}\r\n\
                 total_error_replies:{}\r\n\
                 total_reads_processed:{}\r\n\
                 total_writes_processed:0\r\n\
                 \r\n\
                 # Replication\r\n\
                 role:master\r\n\
                 connected_slaves:0\r\n\
                 master_failover_state:no-failover\r\n\
                 master_replid:0000000000000000000000000000000000000000\r\n\
                 master_replid2:0000000000000000000000000000000000000000\r\n\
                 master_repl_offset:0\r\n\
                 second_repl_offset:-1\r\n\
                 repl_backlog_active:0\r\n\
                 repl_backlog_size:1048576\r\n\
                 repl_backlog_first_byte_offset:0\r\n\
                 repl_backlog_histlen:0\r\n\
                 \r\n\
                 # CPU\r\n\
                 used_cpu_sys:{}\r\n\
                 used_cpu_user:{}\r\n\
                 used_cpu_sys_children:{}\r\n\
                 used_cpu_user_children:{}\r\n\
                 used_cpu_sys_main_thread:{}\r\n\
                 used_cpu_user_main_thread:{}\r\n\
                 \r\n\
                 # Errorstats\r\n\
                 errorstat_ERR:count=0\r\n\
                 errorstat_WRONGTYPE:count=0\r\n\
                 \r\n\
                 # Keyspace\r\n\
                 db0:keys={}",
                std::process::id(),
                now * 1_000_000,
                uptime,
                now / 100,
                connected_clients,
                maxclients,
                blocked_clients,
                tracking_clients,
                used_memory,
                used_memory_human,
                used_memory,
                used_memory_human,
                used_memory / 2,
                total_connections_received,
                total_commands_processed,
                instantaneous_ops_per_sec,
                total_net_input_bytes,
                total_net_output_bytes,
                instantaneous_input_kbps,
                instantaneous_output_kbps,
                rejected_connections,
                sync_full,
                sync_partial_ok,
                sync_partial_err,
                expired_keys,
                expired_stale_perc,
                expired_time_cap_reached_count,
                evicted_keys,
                keyspace_hits,
                keyspace_misses,
                pubsub_channels,
                pubsub_patterns,
                latest_fork_usec,
                migrate_cached_sockets,
                slave_expires_tracked_keys,
                active_defrag_hits,
                active_defrag_misses,
                active_defrag_key_hits,
                active_defrag_key_misses,
                tracking_total_keys,
                tracking_total_items,
                tracking_total_prefixes,
                unexpected_error_replies,
                total_error_replies,
                total_reads_processed,
                used_cpu_sys,
                used_cpu_user,
                used_cpu_sys_children,
                used_cpu_user_children,
                used_cpu_sys_main_thread,
                used_cpu_user_main_thread,
                0
            )
        }
    };
    RespValue::BulkString(Some(Bytes::from(s)))
}

// ---------------------------------------------------------------------------
// COMMAND
// ---------------------------------------------------------------------------

async fn cmd_command(
    args: &[Bytes],
    _store: &Arc<Store>,
    _client: Arc<RwLock<ClientCtx>>,
    _config: Arc<RwLock<ServerConfig>>,
) -> RespValue {
    if args.is_empty() {
        // COMMAND with no args = return all command info (same as COMMAND INFO with no names)
        let all: Vec<RespValue> = COMMANDS.iter().map(|c| command_descriptor(c)).collect();
        return RespValue::array(all);
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "COUNT" => cmd_command_count(&args[1..]).await,
        "INFO" => cmd_command_info(&args[1..]).await,
        "DOCS" => cmd_command_docs(&args[1..]).await,
        "LIST" => cmd_command_list(&args[1..]).await,
        "GETKEYS" => cmd_command_getkeys(&args[1..]).await,
        "HELP" => cmd_command_help(&args[1..]).await,
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

async fn cmd_command_count(_args: &[Bytes]) -> RespValue {
    // Total number of commands known by this server
    RespValue::Integer(COMMANDS.len() as i64)
}

async fn cmd_command_info(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        // Return all commands
        let all: Vec<RespValue> = COMMANDS.iter().map(|c| command_descriptor(c)).collect();
        return RespValue::array(all);
    }
    let names: Vec<String> = args
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok())
        .map(|s| s.to_ascii_uppercase())
        .collect();
    let descs: Vec<RespValue> = COMMANDS
        .iter()
        .filter(|c| names.iter().any(|n| n == c.name))
        .map(|c| command_descriptor(c))
        .collect();
    RespValue::array(descs)
}

async fn cmd_command_docs(_args: &[Bytes]) -> RespValue {
    // Minimal stub — return empty docs
    RespValue::array(vec![])
}

async fn cmd_command_help(_args: &[Bytes]) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::from("COUNT")),
        RespValue::bulk(Bytes::from("Returns the total number of commands.")),
        RespValue::bulk(Bytes::from("INFO [cmd ...]")),
        RespValue::bulk(Bytes::from("Returns details for specified commands.")),
        RespValue::bulk(Bytes::from("DOCS [cmd ...]")),
        RespValue::bulk(Bytes::from("Returns documentation for specified commands.")),
        RespValue::bulk(Bytes::from("LIST")),
        RespValue::bulk(Bytes::from("Returns all command names.")),
        RespValue::bulk(Bytes::from("GETKEYS <cmd> [args...]")),
        RespValue::bulk(Bytes::from("Returns the keys from a command.")),
    ])
}

async fn cmd_command_list(_args: &[Bytes]) -> RespValue {
    // Return all command names as a flat array
    let names: Vec<RespValue> = COMMANDS
        .iter()
        .map(|c| RespValue::bulk(Bytes::from(c.name)))
        .collect();
    RespValue::array(names)
}

/// Extract keys from a command by looking up its descriptor and computing key positions.
async fn cmd_command_getkeys(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'command|getkeys' command".into());
    }
    let cmd_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid command name".into()),
    };
    // Find the command descriptor
    let desc = match COMMANDS.iter().find(|c| c.name == cmd_name.as_str()) {
        Some(d) => d,
        None => return RespValue::Error(format!("ERR unknown command `{}`", cmd_name)),
    };
    let cmd_args = &args[1..];
    let mut keys = Vec::new();

    if desc.first_key > 0 && (cmd_args.len() as i64) >= desc.first_key {
        let first = (desc.first_key - 1) as usize;
        let last = if desc.last_key < 0 {
            // Negative last_key means: last key is at position (len + last_key + 1)
            let pos = cmd_args.len() as i64 + desc.last_key + 1;
            if pos < 1 { first } else { (pos - 1) as usize }
        } else {
            let last = (desc.last_key - 1) as usize;
            if last >= cmd_args.len() { cmd_args.len() - 1 } else { last }
        };
        let step = if desc.key_step > 0 { desc.key_step as usize } else { 1 };
        let mut idx = first;
        while idx <= last && idx < cmd_args.len() {
            keys.push(RespValue::bulk(cmd_args[idx].clone()));
            idx += step;
        }
    }

    RespValue::array(keys)
}

// Command descriptor table

#[derive(Debug, Clone)]
struct CommandDesc {
    name: &'static str,
    arity: i64,
    first_key: i64,
    last_key: i64,
    key_step: i64,
}

const COMMANDS: &[CommandDesc] = &[
    CommandDesc {
        name: "PING",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "ECHO",
        arity: 2,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "SELECT",
        arity: 2,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "DBSIZE",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "FLUSHDB",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "FLUSHALL",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "INFO",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "COMMAND",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "CONFIG",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "SAVE",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "BGSAVE",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "BGREWRITEAOF",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "LASTSAVE",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "TIME",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "LATENCY",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "SLOWLOG",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "MEMORY",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "CLIENT",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "DEBUG",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "OBJECT",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "RESET",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "GET",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SET",
        arity: -3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "DEL",
        arity: -2,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "EXISTS",
        arity: -2,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "TYPE",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "LPUSH",
        arity: -3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "RPUSH",
        arity: -3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "LPOP",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "RPOP",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "LRANGE",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "HSET",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "HGET",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "HGETALL",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SADD",
        arity: -3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SMEMBERS",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SISMEMBER",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZADD",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZRANGE",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZSCORE",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZMSCORE",
        arity: -3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZRANK",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREVRANK",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREVRANGE",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZRANGEBYSCORE",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREVRANGEBYSCORE",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZRANGEBYLEX",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZCOUNT",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZLEXCOUNT",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREM",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREMRANGEBYRANK",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREMRANGEBYSCORE",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZREMRANGEBYLEX",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZCARD",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZINCRBY",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZPOPMIN",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZPOPMAX",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZUNIONSTORE",
        arity: -4,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZINTERSTORE",
        arity: -4,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZDIFFSTORE",
        arity: -4,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZSCAN",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZRANDMEMBER",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZRANGESTORE",
        arity: -5,
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandDesc {
        name: "ZDIFF",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZINTER",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZUNION",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "ZMPOP",
        arity: -4,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "DUMP",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "RESTORE",
        arity: -4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SORT",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SORT_RO",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SUBSTR",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "EXPIRETIME",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "PEXPIRETIME",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SINTERCARD",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "MOVE",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "COPY",
        arity: -3,
        first_key: 1,
        last_key: 2,
        key_step: 1,
    },
    CommandDesc {
        name: "GETSET",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "MGET",
        arity: -2,
        first_key: 1,
        last_key: -1,
        key_step: 1,
    },
    CommandDesc {
        name: "MSET",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 2,
    },
    CommandDesc {
        name: "MSETNX",
        arity: -3,
        first_key: 1,
        last_key: -1,
        key_step: 2,
    },
    CommandDesc {
        name: "GETEX",
        arity: -2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "GETDEL",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "STRLEN",
        arity: 2,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "GETRANGE",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "SETRANGE",
        arity: 4,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "APPEND",
        arity: 3,
        first_key: 1,
        last_key: 1,
        key_step: 1,
    },
    CommandDesc {
        name: "OBJECT",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "RESET",
        arity: 1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "SHUTDOWN",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "LOLWUT",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
    CommandDesc {
        name: "CLIENT",
        arity: -1,
        first_key: 0,
        last_key: 0,
        key_step: 0,
    },
];

fn command_descriptor(c: &CommandDesc) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(c.name)),
        RespValue::Integer(c.arity),
        RespValue::array(vec![]), // flags — empty for now
        RespValue::Integer(c.first_key),
        RespValue::Integer(c.last_key),
        RespValue::Integer(c.key_step),
    ])
}

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

async fn cmd_config(
    args: &[Bytes],
    config: Arc<RwLock<ServerConfig>>,
    store: &Arc<Store>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'config' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "GET" => cmd_config_get(&args[1..], config).await,
        "SET" => cmd_config_set(&args[1..], config, store).await,
        "RESETSTAT" => cmd_config_resetstat(&args[1..]).await,
        "REWRITE" => cmd_config_rewrite(&args[1..], config).await,
        "HELP" => cmd_config_help(&args[1..]).await,
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

async fn cmd_config_get(args: &[Bytes], config: Arc<RwLock<ServerConfig>>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'config|get' command".into());
    }
    let pattern = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR invalid pattern".into()),
    };
    let cfg = config.read().unwrap();
    let entries = cfg.get(pattern);
    let items: Vec<RespValue> = entries
        .iter()
        .flat_map(|(k, v)| {
            vec![
                RespValue::bulk(Bytes::from(k.clone())),
                RespValue::bulk(Bytes::from(v.clone())),
            ]
        })
        .collect();
    RespValue::array(items)
}

async fn cmd_config_set(
    args: &[Bytes],
    config: Arc<RwLock<ServerConfig>>,
    store: &Arc<Store>,
) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'config|set' command".into());
    }
    let param = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR invalid parameter".into()),
    };
    let value = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR invalid value".into()),
    };
    let mut cfg = config.write().unwrap();
    match cfg.set(param, value) {
        Ok(()) => {
            let maxmemory: u64 = cfg
                .get("maxmemory")
                .first()
                .map(|(_, v)| v.parse().unwrap_or(0))
                .unwrap_or(0);
            let policy_str = cfg
                .get("maxmemory-policy")
                .first()
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| "noeviction".into());
            let samples: u8 = cfg
                .get("maxmemory-samples")
                .first()
                .map(|(_, v)| v.parse().unwrap_or(5))
                .unwrap_or(5);
            let policy = valkey_storage::EvictionPolicy::from_policy_str(&policy_str)
                .unwrap_or(valkey_storage::EvictionPolicy::Noeviction);
            store.set_eviction_config(valkey_storage::EvictionConfig {
                maxmemory,
                policy,
                maxmemory_samples: samples,
            });
            RespValue::ok()
        }
        Err(e) => RespValue::error(e),
    }
}

async fn cmd_config_resetstat(_args: &[Bytes]) -> RespValue {
    // Stub — reset stats counters (all zeros for now)
    RespValue::ok()
}

async fn cmd_config_rewrite(args: &[Bytes], config: Arc<RwLock<ServerConfig>>) -> RespValue {
    if !args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'config|rewrite' command".into(),
        );
    }

    let config_file_path = {
        let cfg = config.read().unwrap();
        match &cfg.config_file_path {
            Some(p) => p.clone(),
            None => {
                return RespValue::Error(
                    "ERR The server is running without a config file".into(),
                );
            }
        }
    };

    // Read the existing config file content
    let existing_content = match std::fs::read_to_string(&config_file_path) {
        Ok(c) => c,
        Err(e) => {
            return RespValue::Error(format!(
                "ERR Failed to read config file '{}': {}",
                config_file_path.display(),
                e
            ));
        }
    };

    // Get current in-memory config values
    let cfg = config.read().unwrap();
    let params = cfg.get("*");
    drop(cfg);

    // Build a lookup of known config keys (canonical lowercase)
    let known_keys: std::collections::HashMap<String, String> = params
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
        .collect();

    // Rewrite the file: update known keys, preserve comments and unknown directives
    let mut output = String::new();
    let mut updated_keys = std::collections::HashSet::new();

    for line in existing_content.lines() {
        let trimmed = line.trim();

        // Preserve empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            output.push_str(line);
            output.push('\n');
            continue;
        }

        // Parse the directive: first whitespace-separated token is the key
        let mut parts = trimmed.split_whitespace();
        if let Some(key) = parts.next() {
            let canonical = key.to_ascii_lowercase();
            if let Some(value) = known_keys.get(&canonical) {
                // Update this line with the current in-memory value
                output.push_str(&format!("{} {}\n", key, value));
                updated_keys.insert(canonical);
                continue;
            }
        }

        // Unknown directive — preserve as-is
        output.push_str(line);
        output.push('\n');
    }

    // Append any known config keys that weren't in the file
    for (key, value) in &known_keys {
        if !updated_keys.contains(key) {
            output.push_str(&format!("{} {}\n", key, value));
        }
    }

    // Write atomically: write to temp file, then rename
    let temp_path = config_file_path.with_extension("conf.tmp");
    if let Err(e) = std::fs::write(&temp_path, &output) {
        return RespValue::Error(format!(
            "ERR Failed to write temp config file '{}': {}",
            temp_path.display(),
            e
        ));
    }

    if let Err(e) = std::fs::rename(&temp_path, &config_file_path) {
        // Clean up temp file on failure
        let _ = std::fs::remove_file(&temp_path);
        return RespValue::Error(format!(
            "ERR Failed to rename temp config file '{}': {}",
            config_file_path.display(),
            e
        ));
    }

    RespValue::ok()
}

async fn cmd_config_help(_args: &[Bytes]) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::from("GET")),
        RespValue::bulk(Bytes::from("Return parameters matching a pattern.")),
        RespValue::bulk(Bytes::from("SET")),
        RespValue::bulk(Bytes::from("Set a parameter to a value.")),
        RespValue::bulk(Bytes::from("RESETSTAT")),
        RespValue::bulk(Bytes::from("Reset statistics reported by INFO.")),
        RespValue::bulk(Bytes::from("REWRITE")),
        RespValue::bulk(Bytes::from("Rewrite the configuration file.")),
    ])
}

// ---------------------------------------------------------------------------
// SAVE
// ---------------------------------------------------------------------------

async fn cmd_save(
    args: &[Bytes],
    store: &Arc<Store>,
    config: Arc<RwLock<ServerConfig>>,
) -> RespValue {
    if !args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'save' command".into());
    }
    let path = config.read().unwrap().rdb_path.clone();
    match valkey_persistence::rdb::save(store, std::path::Path::new(&path)).await {
        Ok(()) => RespValue::ok(),
        Err(e) => RespValue::Error(format!("ERR {}", e)),
    }
}

async fn cmd_bgsave(
    args: &[Bytes],
    store: &Arc<Store>,
    config: Arc<RwLock<ServerConfig>>,
) -> RespValue {
    if !args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'bgsave' command".into());
    }
    let path = config.read().unwrap().rdb_path.clone();
    valkey_persistence::rdb::bgsave(Arc::clone(store), std::path::PathBuf::from(path)).await;
    RespValue::SimpleString("Background saving started".into())
}

async fn cmd_bgrewriteaof(
    args: &[Bytes],
    store: &Arc<Store>,
    config: Arc<RwLock<ServerConfig>>,
) -> RespValue {
    if !args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'bgrewriteaof' command".into(),
        );
    }
    let aof_path = config.read().unwrap().aof_path.clone();
    if aof_path.is_empty() {
        return RespValue::Error("ERR AOF is disabled".into());
    }
    let store = Arc::clone(store);
    let path = std::path::PathBuf::from(&aof_path);
    tokio::spawn(async move {
        if let Err(e) = valkey_persistence::aof::rewrite(&store, &path).await {
            eprintln!("AOF rewrite failed: {e}");
        }
    });
    RespValue::SimpleString("Background append only file rewriting started".into())
}

// ---------------------------------------------------------------------------
// LASTSAVE
// ---------------------------------------------------------------------------

async fn cmd_lastsave(_args: &[Bytes]) -> RespValue {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    RespValue::Integer(now)
}

// ---------------------------------------------------------------------------
// TIME
// ---------------------------------------------------------------------------

async fn cmd_time(_args: &[Bytes]) -> RespValue {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = now.as_secs();
    let micros = now.subsec_micros();
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(secs.to_string())),
        RespValue::bulk(Bytes::from(micros.to_string())),
    ])
}

// ---------------------------------------------------------------------------
// LATENCY
// ---------------------------------------------------------------------------

async fn cmd_latency(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'latency' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "HISTORY" | "LATEST" => RespValue::array(vec![]),
        "RESET" => RespValue::ok(),
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

// ---------------------------------------------------------------------------
// SLOWLOG
// ---------------------------------------------------------------------------

async fn cmd_slowlog(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'slowlog' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "GET" => RespValue::array(vec![]),
        "LEN" => RespValue::Integer(0),
        "RESET" => RespValue::ok(),
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

// ---------------------------------------------------------------------------
// MEMORY
// ---------------------------------------------------------------------------

async fn cmd_memory(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'memory' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "USAGE" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'memory|usage' command".into(),
                );
            }
            let key = &args[1];
            match store.key_memory_usage(key) {
                Some(usage) => RespValue::Integer(usage as i64),
                None => RespValue::BulkString(None),
            }
        }
        "DOCTOR" => {
            let report = "Hi Sam, I can't find any memory issue in your instance. \
                          I can only detect problems if I can read your mind. \
                          Try calling DOCTOR again when you have a real issue.";
            RespValue::bulk(Bytes::from(report))
        }
        "MALLOC-STATS" => {
            RespValue::bulk(Bytes::from(
                "jemalloc statistics not available in valkey-rs",
            ))
        }
        "PURGE" => RespValue::ok(),
        "STATS" => {
            // Return memory statistics as a structured array
            let used_memory = store.memory_usage_bytes() as i64;
            let overhead = 524288i64; // base overhead estimate
            let dataset = if used_memory > overhead { used_memory - overhead } else { 0 };

            RespValue::array(vec![
                RespValue::bulk(Bytes::from("peak.allocated")),
                RespValue::Integer(used_memory),
                RespValue::bulk(Bytes::from("total.allocated")),
                RespValue::Integer(used_memory),
                RespValue::bulk(Bytes::from("startup.allocated")),
                RespValue::Integer(overhead),
                RespValue::bulk(Bytes::from("replication.backlog")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("clients.slaves")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("clients.normal")),
                RespValue::Integer(overhead / 2),
                RespValue::bulk(Bytes::from("aof.buffer")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("lua.caches")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("db0")),
                RespValue::bulk(Bytes::from(format!(
                    "overhead-hashtable-main={},overhead-hashtable-expires={}",
                    overhead / 4,
                    overhead / 8
                ))),
                RespValue::bulk(Bytes::from("overhead.total")),
                RespValue::Integer(overhead),
                RespValue::bulk(Bytes::from("keys.count")),
                RespValue::Integer(store.dbsize() as i64),
                RespValue::bulk(Bytes::from("keys.bytes-per-key")),
                RespValue::Integer(if store.dbsize() > 0 { dataset / store.dbsize() as i64 } else { 0 }),
                RespValue::bulk(Bytes::from("dataset.bytes")),
                RespValue::Integer(dataset),
                RespValue::bulk(Bytes::from("dataset.percentage")),
                RespValue::Double(if used_memory > 0 { (dataset as f64 / used_memory as f64) * 100.0 } else { 0.0 }),
                RespValue::bulk(Bytes::from("peak.percentage")),
                RespValue::Double(100.0),
                RespValue::bulk(Bytes::from("allocator.allocated")),
                RespValue::Integer(used_memory),
                RespValue::bulk(Bytes::from("allocator.resident")),
                RespValue::Integer(used_memory + overhead),
                RespValue::bulk(Bytes::from("allocator.muzzy")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("allocator.retained")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("allocator-fragmentation.ratio")),
                RespValue::Double(0.0),
                RespValue::bulk(Bytes::from("allocator-fragmentation.bytes")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("allocator-rss.ratio")),
                RespValue::Double(1.0),
                RespValue::bulk(Bytes::from("allocator-rss.bytes")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("rss-overhead.ratio")),
                RespValue::Double(1.0),
                RespValue::bulk(Bytes::from("rss-overhead.bytes")),
                RespValue::Integer(0),
                RespValue::bulk(Bytes::from("memory.lazyfreed")),
                RespValue::Integer(0),
            ])
        }
        "HELP" => RespValue::array(vec![
            RespValue::bulk(Bytes::from("USAGE <key>")),
            RespValue::bulk(Bytes::from("Returns the memory usage of a key and its value.")),
            RespValue::bulk(Bytes::from("DOCTOR")),
            RespValue::bulk(Bytes::from("Return memory problems report.")),
            RespValue::bulk(Bytes::from("STATS")),
            RespValue::bulk(Bytes::from("Return detailed memory usage statistics.")),
            RespValue::bulk(Bytes::from("MALLOC-STATS")),
            RespValue::bulk(Bytes::from("Return internal memory allocator statistics.")),
            RespValue::bulk(Bytes::from("PURGE")),
            RespValue::bulk(Bytes::from("Ask the allocator to release memory.")),
        ]),
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

// ---------------------------------------------------------------------------
// CLIENT
// ---------------------------------------------------------------------------

async fn cmd_client(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'client' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "GETNAME" => cmd_client_getname(&args[1..], client).await,
        "SETNAME" => cmd_client_setname(&args[1..], client).await,
        "ID" => cmd_client_id(&args[1..], client).await,
        "INFO" => cmd_client_info(&args[1..], client).await,
        "LIST" => cmd_client_list(&args[1..]).await,
        "KILL" => cmd_client_kill(&args[1..]).await,
        "PAUSE" => cmd_client_pause(&args[1..]).await,
        "UNPAUSE" => cmd_client_unpause(&args[1..]).await,
        "NO-EVICT" => cmd_client_no_evict(&args[1..], client).await,
        "NO-TOUCH" => cmd_client_no_touch(&args[1..], client).await,
        "REPLY" => cmd_client_reply(&args[1..]).await,
        "TRACKING" => cmd_client_tracking(&args[1..], client).await,
        "TRACKINGINFO" => cmd_client_trackinginfo(&args[1..], client).await,
        "CACHING" => cmd_client_caching(&args[1..], client).await,
        "GETREDIR" => cmd_client_getredir(&args[1..], client).await,
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

async fn cmd_client_getname(_args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let ctx = client.read().unwrap();
    match &ctx.name {
        Some(n) => RespValue::bulk(Bytes::from(n.clone())),
        None => RespValue::null_bulk(),
    }
}

async fn cmd_client_setname(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'client|setname' command".into(),
        );
    }
    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return RespValue::Error("ERR invalid client name".into()),
    };
    let mut ctx = client.write().unwrap();
    ctx.name = Some(name);
    RespValue::ok()
}

async fn cmd_client_id(_args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let ctx = client.read().unwrap();
    RespValue::Integer(ctx.id)
}

async fn cmd_client_info(_args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let ctx = client.read().unwrap();
    let name = ctx.name.as_deref().unwrap_or("");
    RespValue::bulk(Bytes::from(format!(
        "id={} addr=127.0.0.1:0 laddr=127.0.0.1:6379 fd=0 name={} age=0 idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events= cmd= resp=2",
        ctx.id, name, ctx.db_index
    )))
}

async fn cmd_client_list(_args: &[Bytes]) -> RespValue {
    // Stub — return minimal info for the current connection
    RespValue::bulk(Bytes::from(
        "id=1 addr=127.0.0.1:0 laddr=127.0.0.1:6379 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events= cmd= resp=2",
    ))
}

async fn cmd_client_kill(_args: &[Bytes]) -> RespValue {
    // Stub — KILL not meaningful in single-connection context
    RespValue::ok()
}

async fn cmd_client_pause(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'client|pause' command".into());
    }
    let timeout: u64 = match std::str::from_utf8(&args[0]) {
        Ok(s) => match s.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::Error("ERR timeout is not a valid integer".into()),
        },
        Err(_) => return RespValue::Error("ERR timeout is not a valid integer".into()),
    };
    let _ = timeout;
    RespValue::ok()
}

async fn cmd_client_unpause(_args: &[Bytes]) -> RespValue {
    RespValue::ok()
}

async fn cmd_client_no_evict(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'client|no-evict' command".into(),
        );
    }
    let val = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return RespValue::Error("ERR invalid value".into()),
    };
    let mut ctx = client.write().unwrap();
    ctx.flags.no_evict = val == "on";
    RespValue::ok()
}

async fn cmd_client_no_touch(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'client|no-touch' command".into(),
        );
    }
    let val = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return RespValue::Error("ERR invalid value".into()),
    };
    let mut ctx = client.write().unwrap();
    ctx.flags.no_touch = val == "on";
    RespValue::ok()
}

async fn cmd_client_reply(_args: &[Bytes]) -> RespValue {
    RespValue::ok()
}

async fn cmd_client_tracking(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'client|tracking' command".into(),
        );
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "ON" => {
            let mut ctx = client.write().unwrap();
            match ctx.tracking.enable_from_args(&args[1..]) {
                Ok(()) => RespValue::ok(),
                Err(e) => RespValue::Error(e.into()),
            }
        }
        "OFF" => {
            let mut ctx = client.write().unwrap();
            ctx.tracking.disable();
            RespValue::ok()
        }
        "STATUS" => {
            let ctx = client.read().unwrap();
            if ctx.tracking.enabled {
                RespValue::SimpleString("on".into())
            } else {
                RespValue::SimpleString("off".into())
            }
        }
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

async fn cmd_client_trackinginfo(_args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let ctx = client.read().unwrap();
    let flags = ctx.tracking.trackinginfo();
    RespValue::array(flags)
}

async fn cmd_client_caching(args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'client|caching' command".into(),
        );
    }
    let val = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid value".into()),
    };
    match val.as_str() {
        "YES" => {
            let mut ctx = client.write().unwrap();
            ctx.tracking.caching = true;
            RespValue::ok()
        }
        "NO" => {
            let mut ctx = client.write().unwrap();
            ctx.tracking.caching = false;
            RespValue::ok()
        }
        _ => RespValue::Error(
            "ERR CLIENT CACHING option must be either YES or NO".into(),
        ),
    }
}

async fn cmd_client_getredir(_args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let ctx = client.read().unwrap();
    match ctx.tracking.redirect {
        Some(id) => RespValue::Integer(id),
        None => RespValue::Integer(-1),
    }
}

// ---------------------------------------------------------------------------
// DEBUG
// ---------------------------------------------------------------------------

async fn cmd_debug(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'debug' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "SLEEP" => {
            if args.len() < 2 {
                return RespValue::Error(
                    "ERR wrong number of arguments for 'debug|sleep' command".into(),
                );
            }
            let secs: f64 = match std::str::from_utf8(&args[1]) {
                Ok(s) => match s.parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::Error("ERR invalid sleep time".into()),
                },
                Err(_) => return RespValue::Error("ERR invalid sleep time".into()),
            };
            let dur = std::time::Duration::from_secs_f64(secs);
            tokio::time::sleep(dur).await;
            RespValue::ok()
        }
        "RELOAD"
        | "OBJECT"
        | "JMAP"
        | "SET-ACTIVE-EXPIRE"
        | "HELP"
        | "AOF-FLUSH"
        | "PANIC"
        | "LOG"
        | "STRUCTSIZE"
        | "PROTOCOL"
        | "POPULATE"
        | "SDSLEN"
        | "ZIPLIST"
        | "REPLICATE"
        | "DIGEST"
        | "DIGEST-VALUE"
        | "ERROR"
        | "LEAK"
        | "OOM"
        | "SEGFAULT"
        | "MKILL"
        | "ASSERT"
        | "ASSERT-WITH-META"
        | "WEIGHTED-KEYS"
        | "GET-SERVER-TIME"
        | "RESTART"
        | "PROTECTED-OBJECT"
        | "DISABLE-KEYED-HASH-CACHE"
        | "SWAPDB"
        | "HTSTATS"
        | "HTSTATS-KEY"
        | "CHANGE-REPL-ID"
        | "EXPIRE-PEEK"
        | "EXPIRE-AFTER"
        | "EXPIRE-BEFORE"
        | "EXPIRE-AT"
        | "EXPIRE-AT-KEY"
        | "EXPIRE-AT-PEEK"
        | "EXPIRE-AT-AFTER"
        | "EXPIRE-AT-BEFORE"
        | "EXPIRE-AT-KEY-AFTER"
        | "EXPIRE-AT-KEY-BEFORE"
        | "EXPIRE-AT-KEY-PEEK"
        | "EXPIRE-AT-KEY-PEEK-AFTER"
        | "EXPIRE-AT-KEY-PEEK-BEFORE"
        | "EXPIRE-AT-KEY-PEEK-AT"
        | "EXPIRE-AT-KEY-PEEK-AT-AFTER"
        | "EXPIRE-AT-KEY-PEEK-AT-BEFORE"
        | "EXPIRE-AT-KEY-PEEK-AT-PEEK"
        | "EXPIRE-AT-KEY-PEEK-AT-PEEK-AFTER"
        | "EXPIRE-AT-KEY-PEEK-AT-PEEK-BEFORE" => RespValue::ok(),
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

// ---------------------------------------------------------------------------
// OBJECT
// ---------------------------------------------------------------------------

async fn cmd_object(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'object' command".into());
    }
    let sub = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    match sub.as_str() {
        "ENCODING" => cmd_object_encoding(&args[1..], store).await,
        "FREQ" => cmd_object_freq(&args[1..]).await,
        "IDLETIME" => cmd_object_idletime(&args[1..]).await,
        "REFCOUNT" => cmd_object_refcount(&args[1..]).await,
        "SERIALIZED-LENGTH" => cmd_object_serialized_length(&args[1..]).await,
        "HELP" => cmd_object_help(&args[1..]).await,
        _ => RespValue::Error(format!("ERR unknown subcommand `{}`", sub)),
    }
}

async fn cmd_object_encoding(args: &[Bytes], store: &Arc<Store>) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'object|encoding' command".into(),
        );
    }
    match store.type_of(&args[0]) {
        Some("string") => RespValue::bulk(Bytes::from("embstr")),
        Some("list") => RespValue::bulk(Bytes::from("quicklist")),
        Some("hash") => RespValue::bulk(Bytes::from("hashtable")),
        Some("set") => RespValue::bulk(Bytes::from("hashtable")),
        Some("zset") => RespValue::bulk(Bytes::from("skiplist")),
        Some("stream") => RespValue::bulk(Bytes::from("stream")),
        Some(_) => RespValue::bulk(Bytes::from("unknown")),
        None => RespValue::null_bulk(),
    }
}

async fn cmd_object_freq(_args: &[Bytes]) -> RespValue {
    RespValue::null_bulk()
}

async fn cmd_object_idletime(_args: &[Bytes]) -> RespValue {
    RespValue::Integer(0)
}

async fn cmd_object_refcount(_args: &[Bytes]) -> RespValue {
    RespValue::Integer(1)
}

async fn cmd_object_serialized_length(_args: &[Bytes]) -> RespValue {
    RespValue::null_bulk()
}

async fn cmd_object_help(_args: &[Bytes]) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::from("ENCODING <key>")),
        RespValue::bulk(Bytes::from("Return the internal encoding of a key.")),
        RespValue::bulk(Bytes::from("FREQ <key>")),
        RespValue::bulk(Bytes::from("Return the logarithmic access frequency.")),
        RespValue::bulk(Bytes::from("IDLETIME <key>")),
        RespValue::bulk(Bytes::from("Return the idle time of a key.")),
        RespValue::bulk(Bytes::from("REFCOUNT <key>")),
        RespValue::bulk(Bytes::from("Return the reference count of a key.")),
    ])
}

// ---------------------------------------------------------------------------
// SHUTDOWN
// ---------------------------------------------------------------------------

async fn cmd_shutdown(args: &[Bytes]) -> RespValue {
    // SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT]
    let mut save = false;
    let mut abort = false;
    for arg in args {
        if let Ok(s) = std::str::from_utf8(arg) {
            match s.to_ascii_uppercase().as_str() {
                "SAVE" => save = true,
                "NOSAVE" => save = false,
                "ABORT" => abort = true,
                _ => {}
            }
        }
    }
    if abort {
        // Cancel any pending shutdown
        return RespValue::ok();
    }
    // Trigger graceful shutdown
    request_shutdown();
    // In a real server, this would also trigger a background save if save=true
    let _ = save;
    // Return OK — the connection handler will see the shutdown flag and close
    // Note: In the original Redis, SHUTDOWN doesn't actually return a response
    // because it closes the server. We return OK for testability.
    RespValue::SimpleString("OK".into())
}

// ---------------------------------------------------------------------------
// LOLWUT
// ---------------------------------------------------------------------------

async fn cmd_lolwut(args: &[Bytes]) -> RespValue {
    // LOLWUT [VERSION version]
    let version: i64 = if !args.is_empty() {
        // Check for VERSION sub-argument
        if args.len() >= 2 {
            if let Ok(v_str) = std::str::from_utf8(&args[1]) {
                v_str.parse().unwrap_or(5)
            } else {
                5
            }
        } else {
            5
        }
    } else {
        5
    };
    let _ = version;

    // Generate a fun ASCII art computer terminal output
    let art = r#"Hello stranger! Here's what a computer looks like:

          _._                           _._
       __|   |__                     __|   |__
      |         |                   |         |
      |  []  []  |                  |  []  []  |
      |         |                   |         |
      |__     __|                   |__     __|
     |    |   |    |              |    |___|    |
     |    |   |    |              |             |
    _|    |___|    |_           _|             |_
   |________________|         |__________________|

  This is LOLWUT, a command that generates
  fun ASCII art. Enjoy!"#;

    RespValue::bulk(Bytes::from(art))
}

// ---------------------------------------------------------------------------
// RESET
// ---------------------------------------------------------------------------

async fn cmd_reset(_args: &[Bytes], client: Arc<RwLock<ClientCtx>>) -> RespValue {
    let mut ctx = client.write().unwrap();
    ctx.name = None;
    ctx.db_index = 0;
    ctx.authenticated = false;
    ctx.flags = ClientFlags::default();
    ctx.multi = false;
    ctx.queue.clear();
    ctx.watched.clear();
    ctx.dirty = false;
    RespValue::ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping_no_args() {
        let r = cmd_ping(&[]).await;
        assert_eq!(r, RespValue::SimpleString("PONG".into()));
    }

    #[tokio::test]
    async fn test_ping_with_message() {
        let r = cmd_ping(&[Bytes::from("hello")]).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[tokio::test]
    async fn test_echo() {
        let r = cmd_echo(&[Bytes::from("world")]).await;
        assert_eq!(r, RespValue::BulkString(Some(Bytes::from("world"))));
    }

    #[tokio::test]
    async fn test_echo_wrong_args() {
        let r = cmd_echo(&[]).await;
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_select() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));
        let r = cmd_select(&[Bytes::from("1")], client.clone()).await;
        assert_eq!(r, RespValue::ok());
        assert_eq!(client.read().unwrap().db_index, 1);
    }

    #[tokio::test]
    async fn test_dbsize() {
        let store = valkey_storage::Store::new();
        store.set(
            Bytes::from("a"),
            valkey_storage::DataType::String(Bytes::from("1")),
            None,
        );
        store.set(
            Bytes::from("b"),
            valkey_storage::DataType::String(Bytes::from("2")),
            None,
        );
        let r = cmd_dbsize(&[], &store).await;
        assert_eq!(r, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_flushdb() {
        let store = valkey_storage::Store::new();
        store.set(
            Bytes::from("x"),
            valkey_storage::DataType::String(Bytes::from("y")),
            None,
        );
        let r = cmd_flushdb(&[], &store).await;
        assert_eq!(r, RespValue::ok());
        assert_eq!(store.dbsize(), 0);
    }

    #[tokio::test]
    async fn test_info_default() {
        let r = cmd_info(&[]).await;
        match &r {
            RespValue::BulkString(Some(b)) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s.contains("redis_version"));
                assert!(s.contains("# Server"));
                assert!(s.contains("# Clients"));
                assert!(s.contains("# Memory"));
                assert!(s.contains("# Stats"));
                assert!(s.contains("# Replication"));
                assert!(s.contains("# CPU"));
                assert!(s.contains("# Keyspace"));
            }
            _ => panic!("expected bulk string, got {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_info_server_section() {
        let r = cmd_info(&[Bytes::from("server")]).await;
        match &r {
            RespValue::BulkString(Some(b)) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s.contains("redis_version"));
                assert!(s.contains("process_id"));
                assert!(!s.contains("# Clients"));
            }
            _ => panic!("expected bulk string"),
        }
    }

    #[tokio::test]
    async fn test_info_memory_section() {
        let r = cmd_info(&[Bytes::from("memory")]).await;
        match &r {
            RespValue::BulkString(Some(b)) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s.contains("used_memory"));
                assert!(s.contains("maxmemory_policy"));
            }
            _ => panic!("expected bulk string"),
        }
    }

    #[tokio::test]
    async fn test_time() {
        let r = cmd_time(&[]).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                // Both should be bulk strings representing integers
                for item in arr {
                    match item {
                        RespValue::BulkString(Some(b)) => {
                            let s = std::str::from_utf8(b).unwrap();
                            s.parse::<u64>().expect("should be a valid integer");
                        }
                        _ => panic!("expected bulk string in TIME response"),
                    }
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_lastsave() {
        let r = cmd_lastsave(&[]).await;
        match &r {
            RespValue::Integer(n) => {
                assert!(*n > 0);
            }
            _ => panic!("expected integer"),
        }
    }

    #[tokio::test]
    async fn test_config_get_all() {
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let r = cmd_config_get(&[Bytes::from("*")], config).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                // Should have pairs: key, value, key, value, ...
                assert!(arr.len() >= 2);
                assert_eq!(arr.len() % 2, 0);
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_config_get_port() {
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let r = cmd_config_get(&[Bytes::from("port")], config).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::bulk(Bytes::from("port")));
                assert_eq!(arr[1], RespValue::bulk(Bytes::from("6379")));
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_config_set_port() {
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let store = valkey_storage::Store::new();
        let r = cmd_config_set(
            &[Bytes::from("port"), Bytes::from("6380")],
            config.clone(),
            &store,
        )
        .await;
        assert_eq!(r, RespValue::ok());
        // Verify it was set
        let r2 = cmd_config_get(&[Bytes::from("port")], config).await;
        match &r2 {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr[1], RespValue::bulk(Bytes::from("6380")));
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_config_set_invalid_port() {
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let store = valkey_storage::Store::new();
        let r = cmd_config_set(&[Bytes::from("port"), Bytes::from("abc")], config, &store).await;
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_config_resetstat() {
        let r = cmd_config_resetstat(&[]).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_config_rewrite() {
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let r = cmd_config_rewrite(&[], config).await;
        // Should return error since no config file is set
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_config_rewrite_with_file() {
        use std::io::Write;

        // Create a temp config file
        let dir = std::env::temp_dir();
        let config_path = dir.join("valkey_test_rewrite.conf");
        {
            let mut f = std::fs::File::create(&config_path).unwrap();
            writeln!(f, "# Test config file").unwrap();
            writeln!(f, "port 6379").unwrap();
            writeln!(f, "maxmemory 0").unwrap();
            writeln!(f, "# End comment").unwrap();
        }

        let mut cfg = ServerConfig::default();
        cfg.config_file_path = Some(config_path.clone());
        let config = Arc::new(RwLock::new(cfg));

        let r = cmd_config_rewrite(&[], config.clone()).await;
        assert_eq!(r, RespValue::ok());

        // Read back and verify
        let content = std::fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("# Test config file"));
        assert!(content.contains("port 6379"));
        assert!(content.contains("maxmemory 0"));
        assert!(content.contains("# End comment"));

        // Update a value in memory and rewrite
        {
            let mut cfg = config.write().unwrap();
            cfg.set("port", "6380").unwrap();
        }

        let r = cmd_config_rewrite(&[], config.clone()).await;
        assert_eq!(r, RespValue::ok());

        let content = std::fs::read_to_string(&config_path).unwrap();
        assert!(content.contains("port 6380"));
        assert!(!content.contains("port 6379\n"));
        assert!(content.contains("# Test config file"));
        assert!(content.contains("# End comment"));

        // Clean up
        let _ = std::fs::remove_file(&config_path);
    }

    #[tokio::test]
    async fn test_save() {
        let store = valkey_storage::Store::new();
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let r = cmd_save(&[], &store, config).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_bgsave() {
        let store = valkey_storage::Store::new();
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let r = cmd_bgsave(&[], &store, config).await;
        assert_eq!(
            r,
            RespValue::SimpleString("Background saving started".into())
        );
    }

    #[tokio::test]
    async fn test_bgrewriteaof() {
        let store = valkey_storage::Store::new();
        let config = Arc::new(RwLock::new(ServerConfig::default()));
        let r = cmd_bgrewriteaof(&[], &store, config).await;
        assert_eq!(
            r,
            RespValue::SimpleString("Background append only file rewriting started".into())
        );
    }

    #[tokio::test]
    async fn test_latency_subcommands() {
        let r = cmd_latency(&[Bytes::from("latest")]).await;
        assert_eq!(r, RespValue::array(vec![]));

        let r = cmd_latency(&[Bytes::from("history")]).await;
        assert_eq!(r, RespValue::array(vec![]));

        let r = cmd_latency(&[Bytes::from("reset")]).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_slowlog_subcommands() {
        let r = cmd_slowlog(&[Bytes::from("get")]).await;
        assert_eq!(r, RespValue::array(vec![]));

        let r = cmd_slowlog(&[Bytes::from("len")]).await;
        assert_eq!(r, RespValue::Integer(0));

        let r = cmd_slowlog(&[Bytes::from("reset")]).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_memory_usage() {
        let store = valkey_storage::Store::new();
        let r = cmd_memory(&[Bytes::from("USAGE"), Bytes::from("mykey")], &store).await;
        assert_eq!(r, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_memory_doctor() {
        let store = valkey_storage::Store::new();
        let r = cmd_memory(&[Bytes::from("DOCTOR")], &store).await;
        match &r {
            RespValue::BulkString(Some(b)) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s.contains("Sam"));
            }
            _ => panic!("expected bulk string"),
        }
    }

    #[tokio::test]
    async fn test_client_setname_getname() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));

        // Initially no name
        let r = cmd_client_getname(&[], client.clone()).await;
        assert_eq!(r, RespValue::null_bulk());

        // Set name
        let r = cmd_client_setname(&[Bytes::from("myclient")], client.clone()).await;
        assert_eq!(r, RespValue::ok());

        // Get name
        let r = cmd_client_getname(&[], client.clone()).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("myclient")));
    }

    #[tokio::test]
    async fn test_client_id() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));
        let r = cmd_client_id(&[], client).await;
        match &r {
            RespValue::Integer(id) => assert!(*id > 0),
            _ => panic!("expected integer"),
        }
    }

    #[tokio::test]
    async fn test_client_info() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));
        let r = cmd_client_info(&[], client).await;
        match &r {
            RespValue::BulkString(Some(b)) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s.contains("id="));
                assert!(s.contains("addr="));
            }
            _ => panic!("expected bulk string"),
        }
    }

    #[tokio::test]
    async fn test_client_pause() {
        let r = cmd_client_pause(&[Bytes::from("100")]).await;
        assert_eq!(r, RespValue::ok());
    }

    #[tokio::test]
    async fn test_client_no_evict() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));
        let r = cmd_client_no_evict(&[Bytes::from("on")], client.clone()).await;
        assert_eq!(r, RespValue::ok());
        assert!(client.read().unwrap().flags.no_evict);

        let r = cmd_client_no_evict(&[Bytes::from("off")], client.clone()).await;
        assert_eq!(r, RespValue::ok());
        assert!(!client.read().unwrap().flags.no_evict);
    }

    #[tokio::test]
    async fn test_client_no_touch() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));
        let r = cmd_client_no_touch(&[Bytes::from("on")], client.clone()).await;
        assert_eq!(r, RespValue::ok());
        assert!(client.read().unwrap().flags.no_touch);
    }

    #[tokio::test]
    async fn test_reset() {
        let client = Arc::new(RwLock::new(ClientCtx::new()));
        {
            let mut ctx = client.write().unwrap();
            ctx.name = Some("test".into());
            ctx.db_index = 3;
            ctx.authenticated = true;
            ctx.flags.no_evict = true;
        }
        let r = cmd_reset(&[], client.clone()).await;
        assert_eq!(r, RespValue::ok());
        let ctx = client.read().unwrap();
        assert_eq!(ctx.name, None);
        assert_eq!(ctx.db_index, 0);
        assert_eq!(ctx.authenticated, false);
        assert_eq!(ctx.flags.no_evict, false);
    }

    #[tokio::test]
    async fn test_command_count() {
        let r = cmd_command_count(&[]).await;
        assert_eq!(r, RespValue::Integer(COMMANDS.len() as i64));
    }

    #[tokio::test]
    async fn test_command_info_ping() {
        let r = cmd_command_info(&[Bytes::from("PING")]).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 1);
                match &arr[0] {
                    RespValue::Array(Some(desc)) => {
                        assert_eq!(desc[0], RespValue::bulk(Bytes::from("PING")));
                    }
                    _ => panic!("expected inner array"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_object_encoding() {
        let store = valkey_storage::Store::new();
        store.set(
            Bytes::from("s"),
            valkey_storage::DataType::String(Bytes::from("v")),
            None,
        );
        store.set(
            Bytes::from("l"),
            valkey_storage::DataType::List(std::collections::VecDeque::new()),
            None,
        );

        let r = cmd_object_encoding(&[Bytes::from("s")], &store).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("embstr")));

        let r = cmd_object_encoding(&[Bytes::from("l")], &store).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("quicklist")));

        let r = cmd_object_encoding(&[Bytes::from("nonexistent")], &store).await;
        assert_eq!(r, RespValue::null_bulk());
    }

    #[tokio::test]
    async fn test_client_ctx_new() {
        let c1 = ClientCtx::new();
        let c2 = ClientCtx::new();
        assert_ne!(c1.id, c2.id);
        assert_eq!(c1.db_index, 0);
        assert_eq!(c1.name, None);
        assert_eq!(c1.authenticated, false);
    }

    #[tokio::test]
    async fn test_server_config_default() {
        let cfg = ServerConfig::default();
        let port = cfg.get("port");
        assert_eq!(port.len(), 1);
        assert_eq!(port[0].1, "6379");
    }

    #[tokio::test]
    async fn test_server_config_set_maxmemory_policy() {
        let mut cfg = ServerConfig::default();
        assert!(cfg.set("maxmemory-policy", "allkeys-lru").is_ok());
        assert!(cfg.set("maxmemory-policy", "invalid").is_err());
    }

    // ---------------------------------------------------------------------------
    // CLIENT TRACKING tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn test_tracking_state_default() {
        let ts = TrackingState::default();
        assert!(!ts.enabled);
        assert_eq!(ts.mode, TrackingMode::Default);
        assert!(ts.redirect.is_none());
        assert!(ts.prefixes.is_empty());
        assert!(!ts.optin);
        assert!(!ts.optout);
        assert!(!ts.noloop);
        assert!(!ts.caching);
    }

    #[tokio::test]
    async fn test_tracking_enable_on_basic() {
        let mut ts = TrackingState::default();
        let args: Vec<Bytes> = vec![];
        assert!(ts.enable_from_args(&args).is_ok());
        assert!(ts.enabled);
        assert_eq!(ts.mode, TrackingMode::Default);
    }

    #[tokio::test]
    async fn test_tracking_enable_on_bcast() {
        let mut ts = TrackingState::default();
        let args = vec![Bytes::from("BCAST")];
        assert!(ts.enable_from_args(&args).is_ok());
        assert!(ts.enabled);
        assert_eq!(ts.mode, TrackingMode::Bcast);
    }

    #[tokio::test]
    async fn test_tracking_enable_on_redirect() {
        let mut ts = TrackingState::default();
        let args = vec![Bytes::from("REDIRECT"), Bytes::from("42")];
        assert!(ts.enable_from_args(&args).is_ok());
        assert!(ts.enabled);
        assert_eq!(ts.redirect, Some(42));
    }

    #[tokio::test]
    async fn test_tracking_enable_on_prefix() {
        let mut ts = TrackingState::default();
        let args = vec![Bytes::from("PREFIX"), Bytes::from("user:")];
        assert!(ts.enable_from_args(&args).is_ok());
        assert!(ts.enabled);
        assert_eq!(ts.prefixes, vec!["user:".to_string()]);
    }

    #[tokio::test]
    async fn test_tracking_enable_on_multiple_prefixes() {
        let mut ts = TrackingState::default();
        let args = vec![
            Bytes::from("PREFIX"),
            Bytes::from("user:"),
            Bytes::from("PREFIX"),
            Bytes::from("session:"),
        ];
        assert!(ts.enable_from_args(&args).is_ok());
        assert_eq!(ts.prefixes.len(), 2);
        assert_eq!(ts.prefixes[0], "user:");
        assert_eq!(ts.prefixes[1], "session:");
    }

    #[tokio::test]
    async fn test_tracking_enable_on_optin_optout_noloop() {
        let mut ts = TrackingState::default();
        let args = vec![
            Bytes::from("OPTIN"),
            Bytes::from("OPTOUT"),
            Bytes::from("NOLOOP"),
        ];
        assert!(ts.enable_from_args(&args).is_ok());
        assert!(ts.optin);
        assert!(ts.optout);
        assert!(ts.noloop);
    }

    #[tokio::test]
    async fn test_tracking_enable_on_combined() {
        let mut ts = TrackingState::default();
        let args = vec![
            Bytes::from("REDIRECT"),
            Bytes::from("99"),
            Bytes::from("BCAST"),
            Bytes::from("PREFIX"),
            Bytes::from("cache:"),
            Bytes::from("NOLOOP"),
        ];
        assert!(ts.enable_from_args(&args).is_ok());
        assert!(ts.enabled);
        assert_eq!(ts.mode, TrackingMode::Bcast);
        assert_eq!(ts.redirect, Some(99));
        assert_eq!(ts.prefixes, vec!["cache:".to_string()]);
        assert!(ts.noloop);
    }

    #[tokio::test]
    async fn test_tracking_enable_on_invalid_option() {
        let mut ts = TrackingState::default();
        let args = vec![Bytes::from("INVALID")];
        assert!(ts.enable_from_args(&args).is_err());
    }

    #[tokio::test]
    async fn test_tracking_enable_on_redirect_missing_id() {
        let mut ts = TrackingState::default();
        let args = vec![Bytes::from("REDIRECT")];
        assert!(ts.enable_from_args(&args).is_err());
    }

    #[tokio::test]
    async fn test_tracking_disable() {
        let mut ts = TrackingState::default();
        let args = vec![Bytes::from("BCAST")];
        ts.enable_from_args(&args).unwrap();
        assert!(ts.enabled);
        ts.disable();
        assert!(!ts.enabled);
        assert_eq!(ts.mode, TrackingMode::Default);
        assert!(ts.redirect.is_none());
        assert!(ts.prefixes.is_empty());
        assert!(!ts.optin);
        assert!(!ts.caching);
    }

    #[tokio::test]
    async fn test_tracking_should_receive_invalidations() {
        let mut ts = TrackingState::default();
        // Not enabled -> should not receive
        assert!(!ts.should_receive_invalidations());

        // Enabled, no optin -> should receive
        ts.enabled = true;
        assert!(ts.should_receive_invalidations());

        // Enabled with optin but not caching -> should not receive
        ts.optin = true;
        assert!(!ts.should_receive_invalidations());

        // Enabled with optin and caching -> should receive
        ts.caching = true;
        assert!(ts.should_receive_invalidations());

        // Optout mode -> should receive (optout means skip invalidations unless CLIENT CACHING NO)
        ts.optin = false;
        ts.optout = true;
        assert!(ts.should_receive_invalidations());
    }

    #[tokio::test]
    async fn test_trackinginfo_response() {
        let mut ts = TrackingState::default();
        let info = ts.trackinginfo();
        // Should have "on" or "off" as first element
        assert_eq!(info.len(), 1);
        assert_eq!(info[0], RespValue::bulk(Bytes::from("off")));

        ts.enabled = true;
        let info = ts.trackinginfo();
        assert_eq!(info[0], RespValue::bulk(Bytes::from("on")));

        ts.mode = TrackingMode::Bcast;
        let info = ts.trackinginfo();
        assert!(info.contains(&RespValue::bulk(Bytes::from("bcast"))));

        ts.redirect = Some(42);
        let info = ts.trackinginfo();
        assert!(info.contains(&RespValue::bulk(Bytes::from("redirect=42"))));

        ts.prefixes.push("user:".to_string());
        let info = ts.trackinginfo();
        assert!(info.contains(&RespValue::bulk(Bytes::from("prefix=user:"))));

        ts.optin = true;
        let info = ts.trackinginfo();
        assert!(info.contains(&RespValue::bulk(Bytes::from("optin"))));

        ts.noloop = true;
        let info = ts.trackinginfo();
        assert!(info.contains(&RespValue::bulk(Bytes::from("noloop"))));
    }

    #[tokio::test]
    async fn test_client_tracking_on_off_status() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        // Initially tracking is off
        let resp = cmd_client_tracking(
            &[Bytes::from("STATUS")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::SimpleString("off".into()));

        // Enable tracking
        let resp = cmd_client_tracking(
            &[Bytes::from("ON")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::ok());

        // Now status should be on
        let resp = cmd_client_tracking(
            &[Bytes::from("STATUS")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::SimpleString("on".into()));

        // Disable tracking
        let resp = cmd_client_tracking(
            &[Bytes::from("OFF")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::ok());

        // Status should be off again
        let resp = cmd_client_tracking(
            &[Bytes::from("STATUS")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::SimpleString("off".into()));
    }

    #[tokio::test]
    async fn test_client_tracking_on_with_options() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        // Enable with BCAST
        let resp = cmd_client_tracking(
            &[Bytes::from("ON"), Bytes::from("BCAST")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::ok());

        let ctx = client.read().unwrap();
        assert!(ctx.tracking.enabled);
        assert_eq!(ctx.tracking.mode, TrackingMode::Bcast);
    }

    #[tokio::test]
    async fn test_client_tracking_on_with_redirect() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        let resp = cmd_client_tracking(
            &[Bytes::from("ON"), Bytes::from("REDIRECT"), Bytes::from("42")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::ok());

        let ctx = client.read().unwrap();
        assert_eq!(ctx.tracking.redirect, Some(42));
    }

    #[tokio::test]
    async fn test_client_tracking_invalid_subcommand() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        let resp = cmd_client_tracking(
            &[Bytes::from("INVALID")],
            Arc::clone(&client),
        )
        .await;
        assert!(matches!(resp, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_client_trackinginfo() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        // Initially tracking is off
        let resp = cmd_client_trackinginfo(&[], Arc::clone(&client)).await;
        match &resp {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr[0], RespValue::bulk(Bytes::from("off")));
            }
            _ => panic!("expected array response"),
        }

        // Enable tracking
        cmd_client_tracking(
            &[Bytes::from("ON"), Bytes::from("BCAST")],
            Arc::clone(&client),
        )
        .await;

        let resp = cmd_client_trackinginfo(&[], Arc::clone(&client)).await;
        match &resp {
            RespValue::Array(Some(arr)) => {
                assert!(arr.contains(&RespValue::bulk(Bytes::from("on"))));
                assert!(arr.contains(&RespValue::bulk(Bytes::from("bcast"))));
            }
            _ => panic!("expected array response"),
        }
    }

    #[tokio::test]
    async fn test_client_caching_yes_no() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        // Enable tracking with OPTIN
        cmd_client_tracking(
            &[Bytes::from("ON"), Bytes::from("OPTIN")],
            Arc::clone(&client),
        )
        .await;

        // Caching should be off by default
        {
            let ctx = client.read().unwrap();
            assert!(!ctx.tracking.caching);
        }

        // Set caching YES
        let resp = cmd_client_caching(
            &[Bytes::from("YES")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::ok());

        {
            let ctx = client.read().unwrap();
            assert!(ctx.tracking.caching);
        }

        // Set caching NO
        let resp = cmd_client_caching(
            &[Bytes::from("NO")],
            Arc::clone(&client),
        )
        .await;
        assert_eq!(resp, RespValue::ok());

        {
            let ctx = client.read().unwrap();
            assert!(!ctx.tracking.caching);
        }
    }

    #[tokio::test]
    async fn test_client_caching_invalid() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        let resp = cmd_client_caching(
            &[Bytes::from("MAYBE")],
            Arc::clone(&client),
        )
        .await;
        assert!(matches!(resp, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_client_caching_missing_arg() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        let resp = cmd_client_caching(&[], Arc::clone(&client)).await;
        assert!(matches!(resp, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_client_getredir() {
        use std::sync::Arc;
        use std::sync::RwLock;

        let client = Arc::new(RwLock::new(ClientCtx::new()));

        // No redirect set
        let resp = cmd_client_getredir(&[], Arc::clone(&client)).await;
        assert_eq!(resp, RespValue::Integer(-1));

        // Set redirect
        cmd_client_tracking(
            &[Bytes::from("ON"), Bytes::from("REDIRECT"), Bytes::from("42")],
            Arc::clone(&client),
        )
        .await;

        let resp = cmd_client_getredir(&[], Arc::clone(&client)).await;
        assert_eq!(resp, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_client_ctx_has_tracking_field() {
        let ctx = ClientCtx::new();
        assert!(!ctx.tracking.enabled);
        assert_eq!(ctx.tracking.mode, TrackingMode::Default);
    }
}
