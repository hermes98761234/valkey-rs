use std::path::PathBuf;
use tracing::info;
use valkey_sentinel::config::SentinelConfig;
use valkey_sentinel::state::SentinelState;
use valkey_sentinel::SentinelServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();

    // Usage: valkey-sentinel [sentinel.conf]
    let config_path = args.get(1).map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from("sentinel.conf")
    });

    info!("Loading sentinel config from: {}", config_path.display());

    let config = if config_path.exists() {
        SentinelConfig::from_file(&config_path)?
    } else {
        info!("Config file not found, using defaults");
        SentinelConfig::default()
    };

    let port = config.port;
    let masters = config.to_master_infos();
    let state = SentinelState::with_masters(port, masters);

    info!(
        "Valkey Sentinel starting on port {} with {} master(s)",
        port,
        state.masters.read().await.len()
    );

    let server = SentinelServer::new(std::sync::Arc::new(state));
    server.run(config).await?;

    Ok(())
}
