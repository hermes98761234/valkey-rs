use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tracing::{info, warn};
use valkey_commands::dispatch;
use valkey_persistence::aof::{AofWriter, FsyncPolicy};
use valkey_proto::{RespDecoder, RespEncoder, RespValue};
use valkey_storage::Store;

/// Global AOF writer, set once at startup if appendonly is enabled.
static AOF_WRITER: OnceLock<Option<Arc<AofWriter>>> = OnceLock::new();

/// Check if AOF is enabled.
pub fn aof_enabled() -> bool {
    AOF_WRITER.get().map(|o| o.is_some()).unwrap_or(false)
}

/// Append a command to the AOF if enabled.
pub async fn aof_append(cmd: &[Bytes]) {
    if let Some(Some(writer)) = AOF_WRITER.get() {
        writer.append(cmd).await;
    }
}

/// Get a clone of the AOF writer if enabled.
pub fn aof_writer() -> Option<Arc<AofWriter>> {
    AOF_WRITER.get().and_then(|o| o.clone())
}

// ---------------------------------------------------------------------------
// ClientStream — abstracts over plain TCP and TLS connections
// ---------------------------------------------------------------------------

pub enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for ClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ClientStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ClientStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_flush(cx),
            ClientStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ClientStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// TLS config loading
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
pub enum TlsClientAuth {
    #[default]
    No,
    Yes,
    Optional,
}

pub struct TlsConfig {
    pub port: u16,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub ca_cert_file: Option<PathBuf>,
    pub auth_clients: TlsClientAuth,
}

fn load_tls_config(
    cert_file: &PathBuf,
    key_file: &PathBuf,
    ca_cert_file: Option<&PathBuf>,
    auth_clients: &TlsClientAuth,
) -> anyhow::Result<RustlsServerConfig> {
    let cert_file_reader = &mut std::io::BufReader::new(std::fs::File::open(cert_file)?);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(cert_file_reader)
        .collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {}", cert_file.display());
    }

    let key_file_reader = &mut std::io::BufReader::new(std::fs::File::open(key_file)?);
    let key = rustls_pemfile::private_key(key_file_reader)?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", key_file.display()))?;

    let builder = RustlsServerConfig::builder();

    let config = match auth_clients {
        TlsClientAuth::No => builder.with_no_client_auth(),
        TlsClientAuth::Yes | TlsClientAuth::Optional => {
            let ca_file = ca_cert_file
                .ok_or_else(|| anyhow::anyhow!("CA cert file required when tls-auth-clients is yes or optional"))?;
            let ca_file_reader = &mut std::io::BufReader::new(std::fs::File::open(ca_file)?);
            let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(ca_file_reader)
                .collect::<Result<Vec<_>, _>>()?;
            if ca_certs.is_empty() {
                anyhow::bail!("no CA certificates found in {}", ca_file.display());
            }
            let mut root_store = rustls::RootCertStore::empty();
            for cert in ca_certs {
                root_store.add(cert)?;
            }
            let verifier = if *auth_clients == TlsClientAuth::Optional {
                rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                    .allow_unauthenticated()
                    .build()?
            } else {
                rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()?
            };
            builder.with_client_cert_verifier(verifier)
        }
    };

    let config = config.with_single_cert(certs, key)?;
    Ok(config)
}

// ---------------------------------------------------------------------------
// Connection handler — generic over AsyncRead + AsyncWrite + Unpin
// ---------------------------------------------------------------------------

async fn handle_connection<S>(stream: S, store: Arc<Store>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (read_half, write_half) = tokio::io::split(stream);
    let mut framed_read = Framed::new(read_half, RespDecoder::default());
    let mut framed_write = Framed::new(write_half, RespEncoder);

    while let Some(result) = framed_read.next().await {
        let frame = result.map_err(|e| anyhow::anyhow!("decode error: {e}"))?;

        let cmd_bytes = match &frame {
            RespValue::Array(Some(arr)) => arr
                .iter()
                .map(|v| match v {
                    RespValue::BulkString(Some(b)) => b.clone(),
                    RespValue::BulkString(None) => Bytes::new(),
                    RespValue::SimpleString(s) => Bytes::from(s.clone()),
                    RespValue::Integer(i) => Bytes::from(i.to_string()),
                    _ => Bytes::new(),
                })
                .collect::<Vec<Bytes>>(),
            _ => {
                let resp = RespValue::Error("ERR expected array command".into());
                framed_write.send(resp).await?;
                continue;
            }
        };

        let response = dispatch(cmd_bytes.clone(), Arc::clone(&store)).await;

        // If this is a write command and it succeeded, append to AOF
        if aof_enabled() && is_write_command(&cmd_bytes) {
            if !matches!(response, RespValue::Error(_)) {
                aof_append(&cmd_bytes).await;
            }
        }

        framed_write.send(response).await?;
    }

    Ok(())
}

/// Check if a command is a write command that should be logged to AOF.
fn is_write_command(cmd: &[Bytes]) -> bool {
    if cmd.is_empty() {
        return false;
    }
    let name = match std::str::from_utf8(&cmd[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return false,
    };
    matches!(
        name.as_str(),
        "SET" | "SETEX" | "PSETEX" | "SETNX" | "GETSET" | "APPEND"
            | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT"
            | "DEL" | "UNLINK"
            | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
            | "RPUSH" | "LPUSH" | "RPOP" | "LPOP" | "LSET" | "LINSERT" | "LREM" | "LTRIM"
            | "HSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" | "HMSET"
            | "SADD" | "SREM" | "SPOP" | "SMOVE"
            | "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX"
            | "RENAME" | "RENAMENX"
            | "FLUSHDB" | "FLUSHALL"
            | "XADD" | "XDEL" | "XTRIM" | "XACK"
            | "MULTI" | "EXEC" | "DISCARD"
    )
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("valkey-rs listening on {addr}");

    let store = Store::new();

    // Load RDB snapshot if it exists
    let rdb_path = PathBuf::from("./dump.rdb");
    if rdb_path.exists() {
        info!("RDB file found at {}, loading...", rdb_path.display());
        if let Err(e) = valkey_persistence::rdb::load(&store, &rdb_path).await {
            warn!("RDB load failed: {e}");
        } else {
            info!("RDB loaded successfully");
        }
    }

    // Initialize AOF if configured
    let aof_dir = PathBuf::from(".");
    let aof_filename = "appendonly.aof";
    let aof_path = aof_dir.join(aof_filename);

    if aof_path.exists() {
        info!("AOF file found at {}, replaying...", aof_path.display());
        if let Err(e) = valkey_persistence::aof::replay(&store, &aof_path).await {
            warn!("AOF replay failed: {e}");
        }
    }

    let fsync = FsyncPolicy::EverySec;
    let writer = AofWriter::open(&aof_path, fsync).await?;
    let _ = AOF_WRITER.set(Some(Arc::new(writer)));
    info!("AOF enabled: {}", aof_path.display());

    // Auto-save background task
    let auto_store = Arc::clone(&store);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let dirty = auto_store.dirty_count();
            if dirty == 0 { continue; }
            let rdb_path = PathBuf::from("./dump.rdb");
            if let Err(e) = valkey_persistence::rdb::save(&auto_store, &rdb_path).await {
                warn!("auto-save failed: {e}");
            } else {
                auto_store.reset_dirty_count();
            }
        }
    });

    // TLS configuration (optional — set via env vars)
    let tls_config = if let (Some(port), Some(cert), Some(key)) = (
        std::env::var("TLS_PORT").ok().and_then(|p| p.parse::<u16>().ok()),
        std::env::var("TLS_CERT").ok().map(PathBuf::from),
        std::env::var("TLS_KEY").ok().map(PathBuf::from),
    ) {
        let ca_cert = std::env::var("TLS_CA_CERT").ok().map(PathBuf::from);
        let auth = match std::env::var("TLS_AUTH_CLIENTS").unwrap_or_else(|_| "no".into()).as_str() {
            "yes" => TlsClientAuth::Yes,
            "optional" => TlsClientAuth::Optional,
            _ => TlsClientAuth::No,
        };
        Some(TlsConfig {
            port,
            cert_file: cert,
            key_file: key,
            ca_cert_file: ca_cert,
            auth_clients: auth,
        })
    } else {
        None
    };

    // Spawn TLS listener if configured
    if let Some(tls_cfg) = tls_config {
        match load_tls_config(
            &tls_cfg.cert_file,
            &tls_cfg.key_file,
            tls_cfg.ca_cert_file.as_ref(),
            &tls_cfg.auth_clients,
        ) {
            Ok(tls_rustls_config) => {
                let tls_acceptor = TlsAcceptor::from(Arc::new(tls_rustls_config));
                match TcpListener::bind(format!("0.0.0.0:{}", tls_cfg.port)).await {
                    Ok(tls_listener) => {
                        info!("valkey-rs TLS listening on port {}", tls_cfg.port);
                        let store_tls = Arc::clone(&store);
                        tokio::spawn(async move {
                            loop {
                                match tls_listener.accept().await {
                                    Ok((stream, peer)) => {
                                        info!("TLS connection from {peer}");
                                        let acceptor = tls_acceptor.clone();
                                        let store = Arc::clone(&store_tls);
                                        tokio::spawn(async move {
                                            match acceptor.accept(stream).await {
                                                Ok(tls_stream) => {
                                                    let client_stream = ClientStream::Tls(Box::new(tls_stream));
                                                    if let Err(e) = handle_connection(client_stream, store).await {
                                                        tracing::warn!(%peer, error = %e, "TLS connection error");
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!(%peer, error = %e, "TLS handshake failed");
                                                }
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "TLS accept error");
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Failed to bind TLS listener on port {}: {}", tls_cfg.port, e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to load TLS config: {e}");
            }
        }
    }

    // Plain TCP accept loop
    loop {
        let (socket, peer) = listener.accept().await?;
        info!("connection from {peer}");
        let store = Arc::clone(&store);
        let client_stream = ClientStream::Plain(socket);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(client_stream, store).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}
