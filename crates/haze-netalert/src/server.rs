//! The network alert WebSocket listener. Each connection has a bounded
//! outbound queue drained by its own writer task, so a slow client only ever
//! delays itself.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_hdr_async_with_config, WebSocketStream};
use tracing::{debug, info};

use crate::hub::{Hub, Outbound};
use crate::protocol::WS_PATH;

/// Queued messages per client. An alert's audio is one entry, so this is
/// room for hundreds of alerts before a stalled client is dropped.
const CLIENT_QUEUE: usize = 256;
const MAX_CLIENT_MESSAGE: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const PING_INTERVAL: Duration = Duration::from_secs(30);

type Sink = SplitSink<WebSocketStream<TcpStream>, Message>;

pub async fn run(hub: Arc<Hub>, addr: String, max_clients: usize) -> Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind network alert listener {addr}"))?;
    info!("network alert WebSocket listening on ws://{addr}{WS_PATH}");
    loop {
        let (stream, peer) = listener.accept().await?;
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            if let Err(err) = serve(hub, stream, peer, max_clients).await {
                debug!(%peer, "network alert connection ended: {err:#}");
            }
        });
    }
}

async fn serve(
    hub: Arc<Hub>,
    stream: TcpStream,
    peer: SocketAddr,
    max_clients: usize,
) -> Result<()> {
    let _ = stream.set_nodelay(true);
    let full = hub.client_count() >= max_clients;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_CLIENT_MESSAGE))
        .max_frame_size(Some(MAX_CLIENT_MESSAGE));
    let handshake = accept_hdr_async_with_config(
        stream,
        |request: &Request, response: Response| {
            if request.uri().path() != WS_PATH {
                return Err(reject(StatusCode::NOT_FOUND, "not found"));
            }
            if full {
                return Err(reject(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "too many network alert clients",
                ));
            }
            Ok(response)
        },
        Some(config),
    );
    let socket = timeout(HANDSHAKE_TIMEOUT, handshake)
        .await
        .context("WebSocket handshake timed out")??;
    let (sink, mut source) = socket.split();
    let (tx, rx) = mpsc::channel(CLIENT_QUEUE);
    let id = hub.register(tx);
    info!(%peer, client = id, "network alert client connected");
    let writer = tokio::spawn(write_loop(sink, rx));

    let result = async {
        while let Some(message) = source.next().await {
            match message? {
                Message::Text(text) => hub.handle_client_text(id, text.as_str()),
                Message::Binary(_) => {
                    hub.send_error(id, "bad_request", "clients send JSON text frames only")
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Dropping the hub's sender lets the writer flush and close.
    hub.unregister(id);
    let _ = timeout(WRITE_TIMEOUT, writer).await;
    info!(%peer, client = id, "network alert client disconnected");
    result
}

fn reject(status: StatusCode, text: &str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(text.to_string()));
    *response.status_mut() = status;
    response
}

async fn write_loop(mut sink: Sink, mut rx: mpsc::Receiver<Outbound>) -> Result<()> {
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await;
    loop {
        tokio::select! {
            outbound = rx.recv() => match outbound {
                Some(Outbound::Text(text)) => send(&mut sink, Message::Text(text)).await?,
                Some(Outbound::Audio(stream)) => {
                    send(&mut sink, Message::Text(stream.start.clone())).await?;
                    for page in &stream.pages {
                        send(&mut sink, Message::Binary(page.clone())).await?;
                    }
                    send(&mut sink, Message::Text(stream.end.clone())).await?;
                }
                None => {
                    let close = CloseFrame { code: CloseCode::Again, reason: "disconnected by server".into() };
                    let _ = send(&mut sink, Message::Close(Some(close))).await;
                    return Ok(());
                }
            },
            _ = ping.tick() => send(&mut sink, Message::Ping(Bytes::new())).await?,
        }
    }
}

async fn send(sink: &mut Sink, message: Message) -> Result<()> {
    timeout(WRITE_TIMEOUT, sink.send(message))
        .await
        .context("client write timed out")??;
    Ok(())
}
