use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("valkey-rs listening on {addr}");
    loop {
        let (socket, peer) = listener.accept().await?;
        info!("connection from {peer}");
        tokio::spawn(async move {
            drop(socket); // placeholder — proto crate will handle this
        });
    }
}
