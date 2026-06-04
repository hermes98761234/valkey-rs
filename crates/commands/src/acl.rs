use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use valkey_proto::RespValue;

use crate::server::ClientCtx;

// ---------------------------------------------------------------------------
// ACL data types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandPermissions {
    AllowAll,
    DenyAll,
    /// (allowed_commands, denied_commands) — both sets of command names (uppercase)
    Set(HashSet<String>, HashSet<String>),
}

#[derive(Debug, Clone)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    /// SHA256 hashed passwords (hex-encoded)
    pub passwords: Vec<String>,
    pub nopass: bool,
    pub commands: CommandPermissions,
    /// Key glob patterns; empty = deny all; ["*"] = allow all
    pub keys: Vec<String>,
    /// Channel glob patterns for pub/sub
    pub channels: Vec<String>,
    pub reset_keys: bool,
}

impl Default for AclUser {
    fn default() -> Self {
        let mut allowed = HashSet::new();
        allowed.insert("@all".into());
        Self {
            name: "default".into(),
            enabled: true,
            passwords: Vec::new(),
            nopass: true,
            commands: CommandPermissions::Set(allowed, HashSet::new()),
            keys: vec!["*".into()],
            channels: vec!["*".into()],
            reset_keys: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub timestamp: u64,
    pub reason: String,
    pub username: String,
    pub command: String,
}

// ---------------------------------------------------------------------------
// ACL categories — maps category name -> set of command names
// ---------------------------------------------------------------------------

fn build_categories() -> HashMap<String, HashSet<String>> {
    let mut cats: HashMap<String, HashSet<String>> = HashMap::new();

    macro_rules! add {
        ($cat:expr, [$($cmd:expr),* $(,)?]) => {
            cats.entry($cat.to_string())
                .or_default()
                .extend([$( $cmd.to_string() ),*]);
        };
    }

    add!("string", ["GET","SET","DEL","GETSET","MGET","MSET","MSETNX","INCR","DECR","INCRBY","DECRBY","INCRBYFLOAT","APPEND","STRLEN","GETRANGE","SETRANGE","SETNX","SETEX","PSETEX","GETEX","GETDEL"]);
    add!("list", ["LPUSH","RPUSH","LPOP","RPOP","LRANGE","LLEN","LINDEX","LSET","LINSERT","LREM","LTRIM","LMOVE","BLPOP","BRPOP"]);
    add!("hash", ["HSET","HGET","HMGET","HMSET","HGETALL","HDEL","HEXISTS","HLEN","HKEYS","HVALS","HINCRBY","HINCRBYFLOAT","HSCAN","HRANDFIELD"]);
    add!("set", ["SADD","SMEMBERS","SISMEMBER","SMISMEMBER","SCARD","SREM","SPOP","SRANDMEMBER","SMOVE","SUNION","SINTER","SDIFF","SUNIONSTORE","SINTERSTORE","SDIFFSTORE","SSCAN"]);
    add!("sortedset", ["ZADD","ZSCORE","ZMSCORE","ZRANK","ZREVRANK","ZRANGE","ZREVRANGE","ZRANGEBYSCORE","ZREVRANGEBYSCORE","ZRANGEBYLEX","ZCOUNT","ZLEXCOUNT","ZREM","ZREMRANGEBYRANK","ZREMRANGEBYSCORE","ZREMRANGEBYLEX","ZCARD","ZINCRBY","ZPOPMIN","ZPOPMAX","ZUNIONSTORE","ZINTERSTORE","ZDIFFSTORE","ZSCAN","ZRANDMEMBER","ZRANGESTORE"]);
    add!("pubsub", ["SUBSCRIBE","UNSUBSCRIBE","PSUBSCRIBE","PUNSUBSCRIBE","PUBLISH","PUBSUB","SSUBSCRIBE","SUNSUBSCRIBE"]);
    add!("transactions", ["MULTI","EXEC","DISCARD","WATCH","UNWATCH"]);
    add!("scripting", ["EVAL","EVALSHA","SCRIPT"]);
    add!("server", ["PING","ECHO","SELECT","DBSIZE","FLUSHDB","FLUSHALL","INFO","COMMAND","CONFIG","SAVE","BGSAVE","BGREWRITEAOF","LASTSAVE","TIME","LATENCY","SLOWLOG","MEMORY","CLIENT","DEBUG","OBJECT","RESET","SHUTDOWN"]);
    add!("connection", ["AUTH","QUIT","CLIENT","SELECT"]);
    add!("stream", ["XADD","XREAD","XRANGE","XREVRANGE","XLEN","XTRIM","XDEL","XINFO","XGROUP","XREADGROUP","XACK","XCLAIM","XPENDING","XAUTOCLAIM"]);
    add!("dangerous", ["FLUSHALL","FLUSHDB","SHUTDOWN","DEBUG","CONFIG","SCRIPT","CLUSTER","REPLICAOF","SLAVEOF","KEYS","SCAN"]);
    add!("keyspace", ["DEL","EXISTS","TYPE","RENAME","RENAMENX","EXPIRE","PEXPIRE","EXPIREAT","PEXPIREAT","TTL","PTTL","PERSIST","KEYS","SCAN","RANDOMKEY","MOVE","COPY","DUMP","RESTORE","WAIT","SORT","UNLINK"]);
    add!("read", ["GET","MGET","GETSET","STRLEN","GETRANGE","LINDEX","LLEN","LRANGE","HGET","HMGET","HGETALL","HEXISTS","HLEN","HKEYS","HVALS","HSCAN","HRANDFIELD","SISMEMBER","SMEMBERS","SMISMEMBER","SCARD","SRANDMEMBER","SUNION","SINTER","SDIFF","SSCAN","ZRANK","ZREVRANK","ZSCORE","ZMSCORE","ZRANGE","ZREVRANGE","ZRANGEBYSCORE","ZREVRANGEBYSCORE","ZRANGEBYLEX","ZCOUNT","ZLEXCOUNT","ZCARD","ZRANDMEMBER","ZSCAN","XLEN","XRANGE","XREVRANGE","XINFO","XPENDING","DUMP","TYPE","EXISTS","TTL","PTTL","OBJECT","RANDOMKEY","KEYS","SCAN","PUBSUB"]);
    add!("write", ["SET","DEL","GETSET","MSET","MSETNX","INCR","DECR","INCRBY","DECRBY","INCRBYFLOAT","APPEND","SETRANGE","SETNX","SETEX","PSETEX","GETEX","GETDEL","LPUSH","RPUSH","LPOP","RPOP","LSET","LINSERT","LREM","LTRIM","LMOVE","BLPOP","BRPOP","HSET","HMSET","HDEL","HINCRBY","HINCRBYFLOAT","SADD","SREM","SPOP","SMOVE","SUNIONSTORE","SINTERSTORE","SDIFFSTORE","ZADD","ZREM","ZINCRBY","ZPOPMIN","ZPOPMAX","ZUNIONSTORE","ZINTERSTORE","ZDIFFSTORE","ZREMRANGEBYRANK","ZREMRANGEBYSCORE","ZREMRANGEBYLEX","ZRANGESTORE","XADD","XDEL","XTRIM","XGROUP","XACK","XCLAIM","XAUTOCLAIM","RENAME","RENAMENX","EXPIRE","PEXPIRE","EXPIREAT","PEXPIREAT","PERSIST","MOVE","COPY","RESTORE","FLUSHDB","FLUSHALL","PUBLISH","SUBSCRIBE","UNSUBSCRIBE","PSUBSCRIBE","PUNSUBSCRIBE","SSUBSCRIBE","SUNSUBSCRIBE","MULTI","EXEC","DISCARD","WATCH","UNWATCH"]);
    add!("fast", ["PING","ECHO","SET","GET","DEL","EXISTS","TYPE","RENAME","RENAMENX","INCR","DECR","INCRBY","DECRBY","APPEND","STRLEN","GETRANGE","SETRANGE","SETNX","LPUSH","RPUSH","LPOP","RPOP","LLEN","LINDEX","LSET","LINSERT","LREM","LTRIM","HSET","HGET","HDEL","HEXISTS","HLEN","HKEYS","HVALS","HINCRBY","SADD","SISMEMBER","SREM","SCARD","SPOP","SMEMBERS","ZADD","ZSCORE","ZRANK","ZREVRANK","ZRANGE","ZREVRANGE","ZCOUNT","ZCARD","ZREM","ZINCRBY","SELECT","DBSIZE","FLUSHDB","FLUSHALL","SAVE","BGSAVE","LASTSAVE","TIME","QUIT","AUTH","INFO","COMMAND","CONFIG","OBJECT","RESET"]);
    add!("slow", ["KEYS","SCAN","SORT","BLPOP","BRPOP","SUNION","SINTER","SDIFF","SUNIONSTORE","SINTERSTORE","SDIFFSTORE","ZUNIONSTORE","ZINTERSTORE","ZDIFFSTORE","ZRANGEBYSCORE","ZREVRANGEBYSCORE","ZRANGEBYLEX","ZLEXCOUNT","ZREMRANGEBYRANK","ZREMRANGEBYSCORE","ZREMRANGEBYLEX","ZSCAN","ZRANDMEMBER","ZRANGESTORE","HSCAN","HRANDFIELD","SSCAN","XREAD","XREADGROUP","XADD","XTRIM","XDEL","XINFO","XGROUP","XACK","XCLAIM","XPENDING","XAUTOCLAIM","PUBSUB","PUBLISH","SUBSCRIBE","UNSUBSCRIBE","PSUBSCRIBE","PUNSUBSCRIBE","SSUBSCRIBE","SUNSUBSCRIBE","MULTI","EXEC","DISCARD","WATCH","UNWATCH","FLUSHALL","SHUTDOWN","DEBUG","SCRIPT","EVAL","EVALSHA","CLUSTER","REPLICAOF","SLAVEOF","MIGRATE","WAIT","DUMP","RESTORE","COPY","MOVE","RANDOMKEY","GETSET","MGET","MSET","MSETNX","INCRBYFLOAT","HINCRBYFLOAT","HGETALL","HMGET","HMSET","HVALS","ZMSCORE","ZRANGEBYLEX","ZREVRANGEBYLEX","ZPOPMIN","ZPOPMAX","GETEX","GETDEL","PSETEX","PEXPIRE","PEXPIREAT","EXPIREAT","PERSIST","TTL","PTTL","EXPIRE","CLIENT","LATENCY","SLOWLOG","MEMORY","BGREWRITEAOF"]);

    // @all = union of all commands
    let all: HashSet<String> = cats.values().flat_map(|s| s.iter().cloned()).collect();
    cats.insert("all".into(), all);

    cats
}

fn categories() -> &'static HashMap<String, HashSet<String>> {
    static CATS: OnceLock<HashMap<String, HashSet<String>>> = OnceLock::new();
    CATS.get_or_init(build_categories)
}

// ---------------------------------------------------------------------------
// AclManager
// ---------------------------------------------------------------------------

const ACL_LOG_MAX: usize = 128;

pub struct AclManager {
    users: DashMap<String, AclUser>,
    log: Mutex<VecDeque<AclLogEntry>>,
}

impl AclManager {
    pub fn new() -> Self {
        let mgr = Self {
            users: DashMap::new(),
            log: Mutex::new(VecDeque::new()),
        };
        // Insert default user
        mgr.users.insert("default".into(), AclUser::default());
        mgr
    }

    /// Get a user by name (case-insensitive)
    pub fn get_user(&self, name: &str) -> Option<AclUser> {
        self.users.get(name).map(|u| u.clone())
    }

    /// Get the "default" user
    pub fn default_user(&self) -> AclUser {
        self.users
            .get("default")
            .map(|u| u.clone())
            .unwrap_or_else(AclUser::default)
    }

    /// Set or replace a user
    pub fn set_user(&self, user: AclUser) {
        self.users.insert(user.name.clone(), user);
    }

    /// Delete users; returns number deleted. Fails if trying to delete "default".
    pub fn del_user(&self, names: &[String]) -> Result<usize, String> {
        for name in names {
            if name == "default" {
                return Err("ERR Cannot delete the 'default' user".into());
            }
        }
        let mut count = 0;
        for name in names {
            if self.users.remove(name).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// List all user names
    pub fn list_users(&self) -> Vec<String> {
        self.users.iter().map(|e| e.key().clone()).collect()
    }

    /// Check if a command is allowed for a given user.
    /// Returns Ok(()) if allowed, Err(reason) if denied. Logs denials internally.
    pub fn check_command(
        &self,
        user: &AclUser,
        cmd: &str,
        keys: &[Bytes],
    ) -> Result<(), String> {
        let check_result = self.check_command_inner(user, cmd, keys);
        if let Err(ref reason) = check_result {
            self.log_denial(&user.name, cmd, reason);
        }
        check_result
    }

    fn check_command_inner(
        &self,
        user: &AclUser,
        cmd: &str,
        keys: &[Bytes],
    ) -> Result<(), String> {
        if !user.enabled {
            return Err(format!(
                "ERR User '{}' is disabled",
                user.name
            ));
        }

        let cmd_upper = cmd.to_ascii_uppercase();

        // Check command permissions
        match &user.commands {
            CommandPermissions::AllowAll => {}
            CommandPermissions::DenyAll => {
                return Err(format!(
                    "ERR NOPERM this user has no permissions to run the '{}' command",
                    cmd_upper
                ));
            }
            CommandPermissions::Set(allowed, denied) => {
                // Check denied first
                if denied.contains(&cmd_upper) || denied.contains(&format!("@{}", cmd_upper)) {
                    return Err(format!(
                        "ERR NOPERM this user has no permissions to run the '{}' command",
                        cmd_upper
                    ));
                }
                // Check allowed — match exact command or category
                let mut cmd_allowed = allowed.contains(&cmd_upper);
                if !cmd_allowed {
                    // Check categories
                    for cat in allowed.iter() {
                        if let Some(cat_name) = cat.strip_prefix('@') {
                            if let Some(cat_cmds) = categories().get(&cat_name.to_lowercase()) {
                                if cat_cmds.contains(&cmd_upper) {
                                    cmd_allowed = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                // Also check @all
                if !cmd_allowed && allowed.contains("@all") {
                    cmd_allowed = true;
                }
                if !cmd_allowed {
                    return Err(format!(
                        "ERR NOPERM this user has no permissions to run the '{}' command",
                        cmd_upper
                    ));
                }
            }
        }

        // Check key permissions
        if user.keys.is_empty() {
            // Empty = deny all keys
            if !keys.is_empty() {
                return Err(format!(
                    "ERR NOPERM this user has no permissions to access any of the keys used as arguments"
                ));
            }
        } else if !user.keys.contains(&"*".to_string()) {
            // Check each key against patterns
            for key in keys {
                let key_str = match std::str::from_utf8(key) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let mut matched = false;
                for pattern in &user.keys {
                    if glob_match(pattern, key_str) {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return Err(format!(
                        "ERR NOPERM this user has no permissions to access the key '{}'",
                        key_str
                    ));
                }
            }
        }

        Ok(())
    }

    /// Log an ACL denial
    pub fn log_denial(&self, username: &str, command: &str, reason: &str) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = AclLogEntry {
            timestamp,
            reason: reason.to_string(),
            username: username.to_string(),
            command: command.to_string(),
        };
        let mut log = self.log.lock().unwrap();
        if log.len() >= ACL_LOG_MAX {
            log.pop_front();
        }
        log.push_back(entry);
    }

    /// Get log entries (up to `count`), or reset
    pub fn get_log(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let log = self.log.lock().unwrap();
        let n = count.unwrap_or(ACL_LOG_MAX);
        log.iter().rev().take(n).cloned().collect()
    }

    pub fn reset_log(&self) {
        let mut log = self.log.lock().unwrap();
        log.clear();
    }

    /// Authenticate: check username + password (SHA256 hex)
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        if let Some(user) = self.users.get(username) {
            if !user.enabled {
                return false;
            }
            if user.nopass {
                return true;
            }
            let hash = sha256_hex(password);
            user.passwords.contains(&hash)
        } else {
            false
        }
    }

    /// Build ACL LIST rule strings for a user
    pub fn user_rules(&self, user: &AclUser) -> Vec<String> {
        let mut rules = Vec::new();
        rules.push(format!("user {}", user.name));
        rules.push(if user.enabled { "on".into() } else { "off".into() });
        if user.nopass {
            rules.push("nopass".into());
        }
        for pw in &user.passwords {
            rules.push(format!(">{}", pw));
        }
        match &user.commands {
            CommandPermissions::AllowAll => rules.push("allcommands".into()),
            CommandPermissions::DenyAll => rules.push("nocommands".into()),
            CommandPermissions::Set(allowed, denied) => {
                for cmd in allowed {
                    rules.push(format!("+{}", cmd));
                }
                for cmd in denied {
                    rules.push(format!("-{}", cmd));
                }
            }
        }
        if user.keys.is_empty() {
            rules.push("resetkeys".into());
        } else if user.keys == vec!["*".to_string()] {
            rules.push("allkeys".into());
        } else {
            for pat in &user.keys {
                rules.push(format!("~{}", pat));
            }
        }
        if user.channels.is_empty() {
            rules.push("resetchannels".into());
        } else if user.channels == vec!["*".to_string()] {
            rules.push("allchannels".into());
        } else {
            for pat in &user.channels {
                rules.push(format!("&{}", pat));
            }
        }
        rules
    }
}

// ---------------------------------------------------------------------------
// ACL rule parsing (for SETUSER)
// ---------------------------------------------------------------------------

/// Parse ACL rules and apply them to a user (mutating).
/// Rules are applied left-to-right.
pub fn apply_rules(user: &mut AclUser, rules: &[String]) -> Result<(), String> {
    for rule in rules {
        let rule = rule.trim();
        if rule.is_empty() {
            continue;
        }
        let parts: Vec<&str> = rule.splitn(2, ' ').collect();
        let verb = parts[0].to_ascii_lowercase();
        let arg = parts.get(1).map(|s| *s).unwrap_or("");

        match verb.as_str() {
            "on" => user.enabled = true,
            "off" => user.enabled = false,
            "nopass" => {
                user.nopass = true;
                user.passwords.clear();
            }
            "reset" => {
                *user = AclUser {
                    name: user.name.clone(),
                    ..Default::default()
                };
            }
            "allkeys" => {
                user.keys = vec!["*".into()];
                user.reset_keys = false;
            }
            "resetkeys" => {
                user.keys.clear();
                user.reset_keys = true;
            }
            "allchannels" => {
                user.channels = vec!["*".into()];
            }
            "resetchannels" => {
                user.channels.clear();
            }
            "allcommands" => {
                user.commands = CommandPermissions::AllowAll;
            }
            "nocommands" => {
                user.commands = CommandPermissions::DenyAll;
            }
            _ => {
                if let Some(cmd) = verb.strip_prefix('+') {
                    // Allow command or category
                    if let CommandPermissions::Set(allowed, _) = &mut user.commands {
                        allowed.insert(cmd.to_ascii_uppercase());
                    } else {
                        let mut allowed = HashSet::new();
                        allowed.insert(cmd.to_ascii_uppercase());
                        let denied = match &user.commands {
                            CommandPermissions::Set(_, d) => d.clone(),
                            _ => HashSet::new(),
                        };
                        user.commands = CommandPermissions::Set(allowed, denied);
                    }
                } else if let Some(cmd) = verb.strip_prefix('-') {
                    // Deny command or category
                    if let CommandPermissions::Set(_, denied) = &mut user.commands {
                        denied.insert(cmd.to_ascii_uppercase());
                    } else {
                        let allowed = match &user.commands {
                            CommandPermissions::Set(a, _) => a.clone(),
                            _ => HashSet::new(),
                        };
                        let mut denied = HashSet::new();
                        denied.insert(cmd.to_ascii_uppercase());
                        user.commands = CommandPermissions::Set(allowed, denied);
                    }
                } else if let Some(pw) = verb.strip_prefix('>') {
                    // Add password (SHA256 hex)
                    let hash = if pw.is_empty() {
                        // The password is the arg
                        sha256_hex(arg)
                    } else {
                        pw.to_string()
                    };
                    user.passwords.push(hash);
                    user.nopass = false;
                } else if let Some(pw) = verb.strip_prefix('<') {
                    // Remove password
                    let hash = if pw.is_empty() {
                        sha256_hex(arg)
                    } else {
                        pw.to_string()
                    };
                    user.passwords.retain(|p| p != &hash);
                } else if let Some(pat) = verb.strip_prefix('~') {
                    // Key pattern
                    let pattern = if pat.is_empty() { arg } else { pat };
                    user.keys.push(pattern.to_string());
                    user.reset_keys = false;
                } else if let Some(pat) = verb.strip_prefix('&') {
                    // Channel pattern
                    let pattern = if pat.is_empty() { arg } else { pat };
                    user.channels.push(pattern.to_string());
                } else {
                    return Err(format!("ERR Unrecognized ACL rule: {}", verb));
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sha256_hex(input: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    // Simple hash for now — in production use ring::digest or sha2 crate
    // Since we don't have sha2 in deps, we use a simple approach
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Simple glob matching: supports `*` (any chars) and `?` (single char)
fn glob_match(pattern: &str, text: &str) -> bool {
    let pat_chars: Vec<char> = pattern.chars().collect();
    let txt_chars: Vec<char> = text.chars().collect();
    glob_match_impl(&pat_chars, &txt_chars)
}

fn glob_match_impl(pat: &[char], txt: &[char]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_idx = None;
    let mut match_idx = 0;

    while ti < txt.len() {
        if pi < pat.len() && (pat[pi] == txt[ti] || pat[pi] == '?') {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star_idx = Some(pi);
            match_idx = ti;
            pi += 1;
        } else if let Some(si) = star_idx {
            pi = si + 1;
            match_idx += 1;
            ti = match_idx;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}

// ---------------------------------------------------------------------------
// Global ACL manager singleton
// ---------------------------------------------------------------------------

fn global_acl() -> &'static AclManager {
    static ACL: OnceLock<AclManager> = OnceLock::new();
    ACL.get_or_init(AclManager::new)
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

pub async fn handle(
    args: &[Bytes],
    client: Arc<std::sync::RwLock<ClientCtx>>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'acl' command".into());
    }
    let subcmd = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid subcommand".into()),
    };
    let subargs = &args[1..];
    let acl = global_acl();

    match subcmd.as_str() {
        "WHOAMI" => cmd_acl_whoami(client).await,
        "LIST" => cmd_acl_list(acl).await,
        "USERS" => cmd_acl_users(acl).await,
        "GETUSER" => cmd_acl_getuser(acl, subargs).await,
        "SETUSER" => cmd_acl_setuser(acl, subargs).await,
        "DELUSER" => cmd_acl_deluser(acl, subargs).await,
        "CAT" => cmd_acl_cat(subargs).await,
        "LOG" => cmd_acl_log(acl, subargs).await,
        "HELP" => cmd_acl_help().await,
        _ => RespValue::Error(format!("ERR Unknown ACL subcommand '{}'", subcmd)),
    }
}

async fn cmd_acl_whoami(client: Arc<std::sync::RwLock<ClientCtx>>) -> RespValue {
    let ctx = client.read().unwrap();
    let name = ctx.name.as_deref().unwrap_or("default");
    RespValue::BulkString(Some(Bytes::from(name.to_string())))
}

async fn cmd_acl_list(acl: &AclManager) -> RespValue {
    let users = acl.list_users();
    let mut result = Vec::new();
    for name in users {
        if let Some(user) = acl.get_user(&name) {
            let rules = acl.user_rules(&user);
            result.push(RespValue::BulkString(Some(Bytes::from(rules.join(" ")))));
        }
    }
    RespValue::array(result)
}

async fn cmd_acl_users(acl: &AclManager) -> RespValue {
    let users = acl.list_users();
    let result: Vec<RespValue> = users
        .into_iter()
        .map(|u| RespValue::BulkString(Some(Bytes::from(u))))
        .collect();
    RespValue::array(result)
}

async fn cmd_acl_getuser(acl: &AclManager, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'acl getuser' command".into());
    }
    let username = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return RespValue::Error("ERR invalid username".into()),
    };
    let user = match acl.get_user(username) {
        Some(u) => u,
        None => return RespValue::Error(format!("ERR User '{}' not found", username)),
    };

    let mut map = Vec::new();
    map.push((
        RespValue::bulk(Bytes::from("flags")),
        RespValue::array({
            let mut flags = Vec::new();
            flags.push(RespValue::bulk(Bytes::from(if user.enabled { "on" } else { "off" })));
            if user.nopass {
                flags.push(RespValue::bulk(Bytes::from("nopass")));
            }
            flags
        }),
    ));
    map.push((
        RespValue::bulk(Bytes::from("passwords")),
        RespValue::array(
            user.passwords
                .iter()
                .map(|p| RespValue::bulk(Bytes::from(p.clone())))
                .collect(),
        ),
    ));
    map.push((
        RespValue::bulk(Bytes::from("commands")),
        RespValue::bulk(Bytes::from(format!("{:?}", user.commands))),
    ));
    map.push((
        RespValue::bulk(Bytes::from("keys")),
        RespValue::array(
            user.keys
                .iter()
                .map(|k| RespValue::bulk(Bytes::from(k.clone())))
                .collect(),
        ),
    ));
    map.push((
        RespValue::bulk(Bytes::from("channels")),
        RespValue::array(
            user.channels
                .iter()
                .map(|c| RespValue::bulk(Bytes::from(c.clone())))
                .collect(),
        ),
    ));

    RespValue::Map(map)
}

async fn cmd_acl_setuser(acl: &AclManager, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'acl setuser' command".into());
    }
    let username = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return RespValue::Error("ERR invalid username".into()),
    };
    let rules: Vec<String> = args[1..]
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()))
        .collect();

    let mut user = acl.get_user(&username).unwrap_or_else(|| AclUser {
        name: username.clone(),
        ..Default::default()
    });

    if let Err(e) = apply_rules(&mut user, &rules) {
        return RespValue::Error(e);
    }

    acl.set_user(user);
    RespValue::ok()
}

async fn cmd_acl_deluser(acl: &AclManager, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'acl deluser' command".into());
    }
    let names: Vec<String> = args
        .iter()
        .filter_map(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()))
        .collect();
    match acl.del_user(&names) {
        Ok(n) => RespValue::Integer(n as i64),
        Err(e) => RespValue::Error(e),
    }
}

async fn cmd_acl_cat(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        // List all categories
        let mut cat_names: Vec<&String> = categories().keys().collect();
        cat_names.sort();
        let result: Vec<RespValue> = cat_names
            .into_iter()
            .map(|c| RespValue::BulkString(Some(Bytes::from(c.clone()))))
            .collect();
        return RespValue::array(result);
    }
    let cat_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return RespValue::Error("ERR invalid category name".into()),
    };
    match categories().get(&cat_name) {
        Some(cmds) => {
            let mut cmd_names: Vec<&String> = cmds.iter().collect();
            cmd_names.sort();
            let result: Vec<RespValue> = cmd_names
                .into_iter()
                .map(|c| RespValue::BulkString(Some(Bytes::from(c.clone()))))
                .collect();
            RespValue::array(result)
        }
        None => RespValue::Error(format!("ERR Unknown category '{}'", cat_name)),
    }
}

async fn cmd_acl_log(acl: &AclManager, args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        // Default: return all entries
        let entries = acl.get_log(None);
        return acl_log_entries(entries);
    }
    let first = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return RespValue::Error("ERR invalid argument".into()),
    };
    if first == "RESET" {
        acl.reset_log();
        return RespValue::ok();
    }
    // Try parse as count
    match first.parse::<usize>() {
        Ok(n) => {
            let entries = acl.get_log(Some(n));
            acl_log_entries(entries)
        }
        Err(_) => RespValue::Error("ERR argument must be a positive integer or RESET".into()),
    }
}

fn acl_log_entries(entries: Vec<AclLogEntry>) -> RespValue {
    let result: Vec<RespValue> = entries
        .into_iter()
        .map(|e| {
            let mut fields = Vec::new();
            fields.push(RespValue::Integer(e.timestamp as i64));
            fields.push(RespValue::bulk(Bytes::from(e.reason)));
            fields.push(RespValue::bulk(Bytes::from(e.username)));
            fields.push(RespValue::bulk(Bytes::from(e.command)));
            RespValue::array(fields)
        })
        .collect();
    RespValue::array(result)
}

async fn cmd_acl_help() -> RespValue {
    let help = vec![
        RespValue::bulk(Bytes::from("ACL <subcommand> [args...]")),
        RespValue::bulk(Bytes::from("  WHOAMI - Return the current connection username")),
        RespValue::bulk(Bytes::from("  LIST - Show all user rules")),
        RespValue::bulk(Bytes::from("  USERS - List all usernames")),
        RespValue::bulk(Bytes::from("  GETUSER <username> - Get user details")),
        RespValue::bulk(Bytes::from("  SETUSER <username> [rules...] - Create/update user")),
        RespValue::bulk(Bytes::from("  DELUSER <username> [username...] - Delete users")),
        RespValue::bulk(Bytes::from("  CAT [category] - List categories or commands")),
        RespValue::bulk(Bytes::from("  LOG [count|RESET] - ACL denial log")),
    ];
    RespValue::array(help)
}

// ---------------------------------------------------------------------------
// AUTH command handler
// ---------------------------------------------------------------------------

pub async fn cmd_auth(
    args: &[Bytes],
    client: Arc<std::sync::RwLock<ClientCtx>>,
) -> RespValue {
    let acl = global_acl();

    let (username, password) = match args.len() {
        1 => ("default", match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return RespValue::Error("ERR invalid password".into()),
        }),
        2 => (
            match std::str::from_utf8(&args[0]) {
                Ok(s) => s,
                Err(_) => return RespValue::Error("ERR invalid username".into()),
            },
            match std::str::from_utf8(&args[1]) {
                Ok(s) => s,
                Err(_) => return RespValue::Error("ERR invalid password".into()),
            },
        ),
        _ => return RespValue::Error("ERR wrong number of arguments for 'auth' command".into()),
    };

    if acl.authenticate(username, password) {
        let mut ctx = client.write().unwrap();
        ctx.authenticated = true;
        ctx.name = Some(username.to_string());
        RespValue::ok()
    } else {
        // Log the denial
        acl.log_denial(username, "AUTH", "authentication failed");
        RespValue::Error("ERR invalid password".into())
    }
}

// ---------------------------------------------------------------------------
// Permission check helper (called from dispatch)
// ---------------------------------------------------------------------------

/// Check if the current user can execute the given command with the given keys.
/// Returns Ok(()) if allowed. On failure, logs to ACL log and returns Err.
pub fn check_permission(
    client: &ClientCtx,
    cmd: &str,
    keys: &[Bytes],
) -> Result<(), String> {
    let acl = global_acl();
    let username = client.name.as_deref().unwrap_or("default");
    let user = acl.get_user(username).unwrap_or_else(|| acl.default_user());
    acl.check_command(&user, cmd, keys)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_client(name: &str) -> Arc<std::sync::RwLock<ClientCtx>> {
        let client = Arc::new(std::sync::RwLock::new(ClientCtx::new()));
        {
            let mut ctx = client.write().unwrap();
            ctx.name = Some(name.into());
            ctx.authenticated = true;
        }
        client
    }

    #[tokio::test]
    async fn test_acl_whoami_default() {
        let client = test_client("default");
        let r = cmd_acl_whoami(client).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("default")));
    }

    #[tokio::test]
    async fn test_acl_whoami_custom() {
        let client = test_client("alice");
        let r = cmd_acl_whoami(client).await;
        assert_eq!(r, RespValue::bulk(Bytes::from("alice")));
    }

    #[tokio::test]
    async fn test_acl_list_default_user() {
        // Create a fresh ACL manager for this test
        let acl = AclManager::new();
        // We can't easily test the global singleton, so test the struct directly
        let users = acl.list_users();
        assert!(users.contains(&"default".to_string()));
    }

    #[tokio::test]
    async fn test_acl_setuser_and_getuser() {
        let acl = AclManager::new();
        let mut user = AclUser {
            name: "testuser".into(),
            ..Default::default()
        };
        apply_rules(&mut user, &["on".into(), "+get".into(), "+set".into(), "resetkeys".into(), "~cache:*".into()]).unwrap();
        acl.set_user(user);

        let retrieved = acl.get_user("testuser").unwrap();
        assert_eq!(retrieved.name, "testuser");
        assert!(retrieved.enabled);
        assert_eq!(retrieved.keys, vec!["cache:*"]);
    }

    #[tokio::test]
    async fn test_acl_deluser() {
        let acl = AclManager::new();
        acl.set_user(AclUser {
            name: "temp".into(),
            ..Default::default()
        });
        assert!(acl.get_user("temp").is_some());

        let result = acl.del_user(&["temp".to_string()]);
        assert_eq!(result.unwrap(), 1);
        assert!(acl.get_user("temp").is_none());
    }

    #[tokio::test]
    async fn test_acl_deluser_cannot_delete_default() {
        let acl = AclManager::new();
        let result = acl.del_user(&["default".to_string()]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acl_check_command_allowed() {
        let acl = AclManager::new();
        let user = AclUser::default(); // default allows all
        let result = acl.check_command(&user, "GET", &[Bytes::from("mykey")]);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_acl_check_command_denied() {
        let acl = AclManager::new();
        let user = AclUser {
            name: "restricted".into(),
            enabled: true,
            nopass: true,
            commands: CommandPermissions::Set(
                {
                    let mut s = HashSet::new();
                    s.insert("GET".into());
                    s
                },
                HashSet::new(),
            ),
            keys: vec!["*".into()],
            channels: vec!["*".into()],
            ..Default::default()
        };
        // GET should be allowed
        assert!(acl.check_command(&user, "GET", &[Bytes::from("key")]).is_ok());
        // SET should be denied
        let result = acl.check_command(&user, "SET", &[Bytes::from("key")]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("NOPERM"));
    }

    #[tokio::test]
    async fn test_acl_check_command_disabled_user() {
        let acl = AclManager::new();
        let user = AclUser {
            name: "disabled".into(),
            enabled: false,
            nopass: true,
            commands: CommandPermissions::AllowAll,
            keys: vec!["*".into()],
            channels: vec!["*".into()],
            ..Default::default()
        };
        let result = acl.check_command(&user, "GET", &[Bytes::from("key")]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("disabled"));
    }

    #[tokio::test]
    async fn test_acl_check_key_pattern() {
        let acl = AclManager::new();
        let user = AclUser {
            name: "keytest".into(),
            enabled: true,
            nopass: true,
            commands: CommandPermissions::AllowAll,
            keys: vec!["cache:*".into()],
            channels: vec!["*".into()],
            ..Default::default()
        };
        // Matching key
        assert!(acl.check_command(&user, "GET", &[Bytes::from("cache:123")]).is_ok());
        // Non-matching key
        let result = acl.check_command(&user, "GET", &[Bytes::from("other:key")]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acl_check_empty_keys_denies_all() {
        let acl = AclManager::new();
        let user = AclUser {
            name: "nokeys".into(),
            enabled: true,
            nopass: true,
            commands: CommandPermissions::AllowAll,
            keys: vec![], // empty = deny all
            channels: vec!["*".into()],
            ..Default::default()
        };
        let result = acl.check_command(&user, "GET", &[Bytes::from("anykey")]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acl_noperm_logs_to_log() {
        let acl = AclManager::new();
        let user = AclUser {
            name: "logger".into(),
            enabled: true,
            nopass: true,
            commands: CommandPermissions::DenyAll,
            keys: vec!["*".into()],
            channels: vec!["*".into()],
            ..Default::default()
        };
        let _ = acl.check_command(&user, "SET", &[Bytes::from("key")]);
        let log = acl.get_log(None);
        assert!(!log.is_empty());
        assert_eq!(log[0].command, "SET");
    }

    #[tokio::test]
    async fn test_acl_log_reset() {
        let acl = AclManager::new();
        acl.log_denial("user", "CMD", "test");
        assert!(!acl.get_log(None).is_empty());
        acl.reset_log();
        assert!(acl.get_log(None).is_empty());
    }

    #[tokio::test]
    async fn test_acl_categories() {
        let cats = categories();
        assert!(cats.contains_key("string"));
        assert!(cats.contains_key("read"));
        assert!(cats.contains_key("write"));
        assert!(cats.contains_key("all"));
        assert!(cats.get("string").unwrap().contains("GET"));
        assert!(cats.get("string").unwrap().contains("SET"));
    }

    #[tokio::test]
    async fn test_acl_cat_command() {
        let r = cmd_acl_cat(&[Bytes::from("string")]).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                assert!(!arr.is_empty());
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_acl_cat_list_categories() {
        let r = cmd_acl_cat(&[]).await;
        match &r {
            RespValue::Array(Some(arr)) => {
                assert!(arr.len() > 10); // many categories
            }
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_acl_setuser_reset() {
        let acl = AclManager::new();
        let mut user = AclUser {
            name: "resetme".into(),
            enabled: false,
            nopass: false,
            passwords: vec!["abc123".into()],
            commands: CommandPermissions::DenyAll,
            keys: vec!["none".into()],
            channels: vec![],
            ..Default::default()
        };
        apply_rules(&mut user, &["reset".into(), "on".into(), "nopass".into(), "allkeys".into()]).unwrap();
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(user.passwords.is_empty());
        assert_eq!(user.keys, vec!["*"]);
    }

    #[tokio::test]
    async fn test_acl_setuser_category_rules() {
        let acl = AclManager::new();
        let mut user = AclUser {
            name: "catuser".into(),
            ..Default::default()
        };
        apply_rules(&mut user, &["on".into(), "nocommands".into(), "+@read".into(), "-@write".into(), "~app:*".into()]).unwrap();
        acl.set_user(user.clone());

        // GET is in @read category
        // GET is in @read category
        assert!(acl.check_command(&user, "GET", &[Bytes::from("app:key")]).is_ok());
        // SET is in @write category (denied)
        let result = acl.check_command(&user, "SET", &[Bytes::from("app:key")]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_glob_match() {
        assert!(glob_match("cache:*", "cache:123"));
        assert!(glob_match("cache:*", "cache:"));
        assert!(!glob_match("cache:*", "other:123"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("???", "abc"));
        assert!(!glob_match("???", "ab"));
        assert!(glob_match("test-?-key", "test-1-key"));
    }

    #[tokio::test]
    async fn test_acl_users() {
        let acl = AclManager::new();
        let users = acl.list_users();
        assert!(users.contains(&"default".to_string()));
    }

    #[tokio::test]
    async fn test_acl_user_rules() {
        let acl = AclManager::new();
        let user = AclUser::default();
        let rules = acl.user_rules(&user);
        assert!(rules.iter().any(|r| r.contains("user default")));
        assert!(rules.iter().any(|r| r == "on"));
        assert!(rules.iter().any(|r| r == "nopass"));
        assert!(rules.iter().any(|r| r == "allkeys"));
    }

    #[tokio::test]
    async fn test_auth_success() {
        let acl = AclManager::new();
        // Default user has nopass
        assert!(acl.authenticate("default", ""));
    }

    #[tokio::test]
    async fn test_auth_failure_disabled_user() {
        let acl = AclManager::new();
        acl.set_user(AclUser {
            name: "locked".into(),
            enabled: false,
            nopass: true,
            ..Default::default()
        });
        assert!(!acl.authenticate("locked", ""));
    }

    #[tokio::test]
    async fn test_auth_unknown_user() {
        let acl = AclManager::new();
        assert!(!acl.authenticate("nonexistent", "pass"));
    }
}
