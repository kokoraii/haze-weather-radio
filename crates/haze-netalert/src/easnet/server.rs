//! EAS NET listeners: inbound peer messages and the tokenized audio server
//! that peers pull alert audio from.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{bail, Context, Result};
use serde_json::json;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use super::config::{AudioServerConfig, Config, PeerConfig, ReceivePolicy};
use super::message::{
    contains_end_marker, inbound_broadcast_payload, is_remote_control, message_matches, parse,
};
use super::EASNET_SOURCE;
use crate::bridge::Bridge;
use crate::util::{make_nonce_token, sanitize_header, unix_now_ms};

#[derive(Debug, Clone)]
struct TokenEntry {
    path: PathBuf,
    expires_at: SystemTime,
    peer_id: String,
    allowed_ips: Vec<IpAddr>,
    duration: Option<Duration>,
}

/// One-time-style audio URLs handed to peers in `EAS.AUDIO.STREAM.URL`.
#[derive(Clone, Default)]
pub struct AudioRegistry {
    tokens: Arc<RwLock<HashMap<String, TokenEntry>>>,
}

impl AudioRegistry {
    pub async fn register(
        &self,
        peer: &PeerConfig,
        path: PathBuf,
        ttl: Duration,
        duration: Option<Duration>,
    ) -> String {
        let token = make_nonce_token(&format!("{}:{}:{}", peer.id, path.display(), unix_now_ms()));
        let entry = TokenEntry {
            path,
            expires_at: SystemTime::now()
                .checked_add(ttl)
                .unwrap_or_else(SystemTime::now),
            peer_id: peer.id.clone(),
            allowed_ips: peer.allowed_ips.clone(),
            duration,
        };
        self.tokens.write().await.insert(token.clone(), entry);
        token
    }

    async fn take_valid(&self, token: &str, ip: IpAddr) -> Option<TokenEntry> {
        let mut tokens = self.tokens.write().await;
        let now = SystemTime::now();
        tokens.retain(|_, entry| entry.expires_at > now);
        let entry = tokens.get(token)?;
        if !entry.allowed_ips.is_empty() && !entry.allowed_ips.contains(&ip) {
            return None;
        }
        Some(entry.clone())
    }
}

pub async fn run_listener(config: Config, bridge: Bridge) -> Result<()> {
    let listener = TcpListener::bind(&config.listener.addr)
        .await
        .with_context(|| format!("failed to bind EAS NET listener {}", config.listener.addr))?;
    info!("EAS NET listener active on {}", config.listener.addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        let config = config.clone();
        let bridge = bridge.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_inbound(stream, addr, config, bridge).await {
                warn!(peer = %addr, "EAS NET inbound failed: {err:#}");
            }
        });
    }
}

async fn handle_inbound(
    mut stream: TcpStream,
    addr: SocketAddr,
    config: Config,
    bridge: Bridge,
) -> Result<()> {
    let Some(peer) = find_inbound_peer(&config.peers, addr.ip()).cloned() else {
        warn!(peer = %addr, "rejected EAS NET message from unknown peer");
        return Ok(());
    };
    let mut data = Vec::new();
    timeout(config.listener.read_timeout, async {
        let mut buf = [0u8; 4096];
        loop {
            let read = stream.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            data.extend_from_slice(&buf[..read]);
            if data.len() > config.listener.max_message_bytes {
                bail!("EAS NET message exceeded max size");
            }
            if contains_end_marker(&data) {
                break;
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("EAS NET read timed out")??;
    let message = parse(&String::from_utf8_lossy(&data))?;

    bridge
        .publish(json!({
            "type": "easnet.message.received",
            "source": EASNET_SOURCE,
            "subject": message.id(),
            "data": {
                "peer_id": peer.id,
                "remote_addr": addr.to_string(),
                "dedupe_key": message.dedupe_key(&peer.id),
                "body_hash": message.body_hash(),
                "fields": message.fields,
            }
        }))
        .await?;

    if is_remote_control(&message) {
        bridge
            .publish(json!({
                "type": "easnet.alert.rejected",
                "source": EASNET_SOURCE,
                "subject": message.id(),
                "data": {
                    "peer_id": peer.id,
                    "reason": "remote control messages are disabled",
                    "func": message.get("EAS.NET.FUNC"),
                    "type": message.get("EAS.ALERT_EVENT.TYPE"),
                }
            }))
            .await?;
        return Ok(());
    }

    if peer.receive_policy != ReceivePolicy::RelayIfMatched || !message_matches(&peer, &message) {
        bridge
            .publish(json!({
                "type": "easnet.alert.rejected",
                "source": EASNET_SOURCE,
                "subject": message.id(),
                "data": {
                    "peer_id": peer.id,
                    "reason": "archive only or filters did not match",
                }
            }))
            .await?;
        return Ok(());
    }

    let data = inbound_broadcast_payload(&peer, &message);
    for event_type in ["easnet.alert.accepted", "cap.alert.broadcast.requested"] {
        bridge
            .publish(json!({
                "type": event_type,
                "source": EASNET_SOURCE,
                "subject": message.id(),
                "data": data,
            }))
            .await?;
    }
    Ok(())
}

fn find_inbound_peer(peers: &[PeerConfig], ip: IpAddr) -> Option<&PeerConfig> {
    peers.iter().find(|peer| {
        peer.enabled
            && peer.direction.can_receive()
            && (peer.allowed_ips.is_empty() || peer.allowed_ips.contains(&ip))
    })
}

pub async fn run_audio_server(config: AudioServerConfig, registry: AudioRegistry) -> Result<()> {
    let listener = TcpListener::bind(&config.addr)
        .await
        .with_context(|| format!("failed to bind EAS NET audio server {}", config.addr))?;
    info!("EAS NET audio server active on {}", config.addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        let registry = registry.clone();
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_audio_request(stream, addr, registry, config).await {
                debug!(peer = %addr, "EAS NET audio request failed: {err:#}");
            }
        });
    }
}

async fn handle_audio_request(
    mut stream: TcpStream,
    addr: SocketAddr,
    registry: AudioRegistry,
    config: AudioServerConfig,
) -> Result<()> {
    let mut reader = BufReader::new(&mut stream);
    let mut first = String::new();
    reader.read_line(&mut first).await?;
    let mut headers = 0usize;
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).await?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        headers += read;
        if headers > 16 * 1024 {
            bail!("HTTP headers too large");
        }
    }
    let parts = first.split_whitespace().collect::<Vec<_>>();
    let token = parts
        .get(1)
        .and_then(|path| path.strip_prefix("/easnet/audio/"))
        .unwrap_or_default()
        .trim();
    if parts.first() != Some(&"GET") || token.is_empty() {
        write_http_status(&mut stream, 404, "Not Found").await?;
        return Ok(());
    }
    let Some(entry) = registry.take_valid(token, addr.ip()).await else {
        write_http_status(&mut stream, 403, "Forbidden").await?;
        return Ok(());
    };
    let metadata = fs::metadata(&entry.path).await?;
    if metadata.len() > config.max_audio_bytes {
        write_http_status(&mut stream, 413, "Payload Too Large").await?;
        return Ok(());
    }
    let body = fs::read(&entry.path).await?;
    let content_type = if entry
        .path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("ogg"))
    {
        "audio/ogg"
    } else {
        "audio/L16; rate=48000; channels=1"
    };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nConnection: close\r\nX-Haze-EASNET-Peer: {}\r\n\r\n",
        sanitize_header(&entry.peer_id)
    );
    stream.write_all(header.as_bytes()).await?;
    write_paced_audio_body(&mut stream, &body, entry.duration).await?;
    stream.flush().await?;
    Ok(())
}

/// Streams the file at roughly real time when its duration is known.
async fn write_paced_audio_body(
    stream: &mut TcpStream,
    body: &[u8],
    duration: Option<Duration>,
) -> Result<()> {
    let Some(duration) = duration.filter(|duration| duration.as_millis() >= 500) else {
        stream.write_all(body).await?;
        return Ok(());
    };
    let bytes_per_second = (body.len() as f64 / duration.as_secs_f64()).max(1.0);
    let chunk_len = ((bytes_per_second / 10.0).ceil() as usize).clamp(1024, 8192);
    let started = Instant::now();
    let total_len = body.len().max(1);
    let mut written = 0usize;
    for chunk in body.chunks(chunk_len) {
        stream.write_all(chunk).await?;
        written += chunk.len();
        let target = duration.mul_f64(written as f64 / total_len as f64);
        if let Some(wait) = target.checked_sub(started.elapsed()) {
            tokio::time::sleep(wait.min(Duration::from_millis(250))).await;
        }
    }
    Ok(())
}

async fn write_http_status(stream: &mut TcpStream, status: u16, text: &str) -> Result<()> {
    let body = format!("{status} {text}\n");
    let header = format!(
        "HTTP/1.1 {status} {text}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nCache-Control: no-store\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    Ok(())
}
