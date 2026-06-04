use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::info;
use valkey_commands::dispatch;
use valkey_proto::{RespDecoder, RespEncoder, RespValue};
use valkey_storage::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("valkey-rs listening on {addr}");

    let store = Store::new();

    loop {
        let (socket, peer) = listener.accept().await?;
        info!("connection from {peer}");
        let store = Arc::clone(&store);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store).await {
                tracing::warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn handle_connection(
    socket: TcpStream,
    store: Arc<Store>,
) -> anyhow::Result<()> {
    let (read_half, write_half) = socket.into_split();
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

        let response = dispatch(cmd_bytes, Arc::clone(&store)).await;
        framed_write.send(response).await?;
    }

    Ok(())
}
