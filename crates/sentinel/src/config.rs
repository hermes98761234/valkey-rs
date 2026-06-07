use crate::state::MasterInfo;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error at line {line}: {msg}")]
    Parse { line: usize, msg: String },
    #[error("Missing required parameter: {0}")]
    MissingParam(String),
}

/// Sentinel configuration parsed from sentinel.conf.
#[derive(Debug, Clone)]
pub struct SentinelConfig {
    /// Port the sentinel listens on.
    pub port: u16,
    /// Bind address.
    pub bind_addr: String,
    /// Monitored masters: name -> (addr, quorum, down_after_ms).
    pub monitors: Vec<MonitorConfig>,
}

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub quorum: u32,
    pub down_after_ms: u64,
    pub failover_timeout: u64,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            port: 26379,
            bind_addr: "127.0.0.1".to_string(),
            monitors: Vec::new(),
        }
    }
}

impl SentinelConfig {
    /// Parse a sentinel.conf file.
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse sentinel.conf content from a string.
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let mut config = SentinelConfig::default();
        let mut current_monitor: Option<MonitorConfig> = None;

        for (line_num, raw_line) in content.lines().enumerate() {
            let line_num = line_num + 1;
            let line = raw_line.trim();

            // Skip empty lines and comments.
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            match parts[0].to_lowercase().as_str() {
                "port" => {
                    if parts.len() < 2 {
                        return Err(ConfigError::Parse {
                            line: line_num,
                            msg: "port requires a value".into(),
                        });
                    }
                    config.port = parts[1].parse().map_err(|_| ConfigError::Parse {
                        line: line_num,
                        msg: format!("invalid port: {}", parts[1]),
                    })?;
                }

                "bind" => {
                    if parts.len() < 2 {
                        return Err(ConfigError::Parse {
                            line: line_num,
                            msg: "bind requires an address".into(),
                        });
                    }
                    config.bind_addr = parts[1].to_string();
                }

                "sentinel" => {
                    if parts.len() < 2 {
                        return Err(ConfigError::Parse {
                            line: line_num,
                            msg: "sentinel directive requires a subcommand".into(),
                        });
                    }
                    match parts[1].to_lowercase().as_str() {
                        "monitor" => {
                            // sentinel monitor <name> <ip> <port> <quorum>
                            if parts.len() < 6 {
                                return Err(ConfigError::Parse {
                                    line: line_num,
                                    msg: "sentinel monitor requires name ip port quorum".into(),
                                });
                            }
                            // Flush any previous monitor being built.
                            if let Some(m) = current_monitor.take() {
                                config.monitors.push(m);
                            }
                            let name = parts[2].to_string();
                            let host = parts[3].to_string();
                            let port: u16 = parts[4].parse().map_err(|_| ConfigError::Parse {
                                line: line_num,
                                msg: format!("invalid port: {}", parts[4]),
                            })?;
                            let quorum: u32 = parts[5].parse().map_err(|_| ConfigError::Parse {
                                line: line_num,
                                msg: format!("invalid quorum: {}", parts[5]),
                            })?;
                            current_monitor = Some(MonitorConfig {
                                name,
                                host,
                                port,
                                quorum,
                                down_after_ms: 30_000,
                                failover_timeout: 60_000,
                            });
                        }

                        "down-after-milliseconds" => {
                            if parts.len() < 4 {
                                return Err(ConfigError::Parse {
                                    line: line_num,
                                    msg: "sentinel down-after-milliseconds requires name and value"
                                        .into(),
                                });
                            }
                            let name = parts[2];
                            let ms: u64 = parts[3].parse().map_err(|_| ConfigError::Parse {
                                line: line_num,
                                msg: format!("invalid milliseconds: {}", parts[3]),
                            })?;
                            if let Some(ref mut m) = current_monitor {
                                if m.name == name {
                                    m.down_after_ms = ms;
                                }
                            }
                            // Also update existing monitor in the list.
                            for m in &mut config.monitors {
                                if m.name == name {
                                    m.down_after_ms = ms;
                                }
                            }
                        }

                        "failover-timeout" => {
                            if parts.len() < 4 {
                                return Err(ConfigError::Parse {
                                    line: line_num,
                                    msg: "sentinel failover-timeout requires name and value".into(),
                                });
                            }
                            let name = parts[2];
                            let ms: u64 = parts[3].parse().map_err(|_| ConfigError::Parse {
                                line: line_num,
                                msg: format!("invalid milliseconds: {}", parts[3]),
                            })?;
                            if let Some(ref mut m) = current_monitor {
                                if m.name == name {
                                    m.failover_timeout = ms;
                                }
                            }
                            for m in &mut config.monitors {
                                if m.name == name {
                                    m.failover_timeout = ms;
                                }
                            }
                        }

                        _ => {
                            warn!(
                                "line {}: unknown sentinel subcommand '{}'",
                                line_num, parts[1]
                            );
                        }
                    }
                }

                _ => {
                    // Unknown directive — skip with a warning.
                    warn!("line {}: unknown directive '{}'", line_num, parts[0]);
                }
            }
        }

        // Flush the last monitor being built.
        if let Some(m) = current_monitor {
            config.monitors.push(m);
        }

        info!(
            "Parsed sentinel config: port={}, monitors={}",
            config.port,
            config.monitors.len()
        );
        Ok(config)
    }

    /// Convert monitor configs into MasterInfo map.
    pub fn to_master_infos(&self) -> HashMap<String, MasterInfo> {
        let mut map = HashMap::new();
        for mc in &self.monitors {
            let addr: SocketAddr = format!("{}:{}", mc.host, mc.port)
                .parse()
                .unwrap_or_else(|_| "127.0.0.1:6379".parse().unwrap());
            let info = MasterInfo::new(mc.name.clone(), addr, mc.quorum, mc.down_after_ms);
            map.insert(mc.name.clone(), info);
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        let conf = r#"
port 26379
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
"#;
        let config = SentinelConfig::parse(conf).unwrap();
        assert_eq!(config.port, 26379);
        assert_eq!(config.monitors.len(), 1);
        let m = &config.monitors[0];
        assert_eq!(m.name, "mymaster");
        assert_eq!(m.host, "127.0.0.1");
        assert_eq!(m.port, 6379);
        assert_eq!(m.quorum, 2);
        assert_eq!(m.down_after_ms, 5000);
        assert_eq!(m.failover_timeout, 60000);
    }

    #[test]
    fn test_parse_multiple_monitors() {
        let conf = r#"
sentinel monitor master1 10.0.0.1 6379 2
sentinel monitor master2 10.0.0.2 6380 3
sentinel down-after-milliseconds master1 3000
sentinel down-after-milliseconds master2 10000
"#;
        let config = SentinelConfig::parse(conf).unwrap();
        assert_eq!(config.monitors.len(), 2);
        assert_eq!(config.monitors[0].name, "master1");
        assert_eq!(config.monitors[1].name, "master2");
        assert_eq!(config.monitors[1].quorum, 3);
    }

    #[test]
    fn test_default_port() {
        let conf = "sentinel monitor m 127.0.0.1 6379 1";
        let config = SentinelConfig::parse(conf).unwrap();
        assert_eq!(config.port, 26379);
    }
}
