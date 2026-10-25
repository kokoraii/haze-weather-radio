#![recursion_limit = "256"]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use russh::{client, ChannelMsg, Disconnect};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG: &str = "config.yaml";
const DEFAULT_EASNET_XML: &str = "managed/configs/easnet.xml";
const DEFAULT_LISTEN: &str = "0.0.0.0:41800";
const DEFAULT_AUDIO_LISTEN: &str = "0.0.0.0:41801";
const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_AUDIO_BYTES: u64 = 25 * 1024 * 1024;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_AUDIO_TTL: Duration = Duration::from_secs(15 * 60);
const SAME_SAMPLE_RATE: u32 = 48_000;
const SAME_BIT_SAMPLES: usize = 92;
const SAME_PREAMBLE_BYTES: usize = 16;
const SAME_INTER_BURST_SAMPLES: usize = 48_000;
const SAME_BURST_LEAD_SAMPLES: usize = 4_800;
const SAME_PRE_ATTENTION_SAMPLES: usize = 48_000;
const SAME_ATTENTION_SAMPLES: usize = 384_000;
const SAME_EOM_LEAD_SAMPLES: usize = 48_000;
const SAME_EOM_TAIL_SAMPLES: usize = 38_400;
const PLAYLIST_POST_HEADER_SILENCE_SAMPLES: usize = 48_000;

#[derive(Debug, Parser)]
#[command(name = "haze-easnet")]
#[command(about = "DASDEC-style EAS NET send/receive service for Haze Weather Radio")]
struct Args {
    #[arg(long, default_value = DEFAULT_CONFIG)]
    config: PathBuf,
    #[arg(long, default_value = DEFAULT_EASNET_XML)]
    easnet: PathBuf,
    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: Option<String>,
    #[arg(long)]
    once: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RootXml {
    #[serde(rename = "@enabled", default)]
    enabled: String,
    #[serde(default)]
    listener: ListenerXml,
    #[serde(default)]
    audio_server: AudioServerXml,
    #[serde(default)]
    peers: PeersXml,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ListenerXml {
    #[serde(rename = "@enabled", default)]
    enabled: String,
    #[serde(rename = "@addr", default)]
    addr: String,
    #[serde(rename = "@max_message_bytes", default)]
    max_message_bytes: String,
    #[serde(rename = "@read_timeout", default)]
    read_timeout: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AudioServerXml {
    #[serde(rename = "@enabled", default)]
    enabled: String,
    #[serde(rename = "@addr", default)]
    addr: String,
    #[serde(rename = "@public_base_url", default)]
    public_base_url: String,
    #[serde(rename = "@token_ttl", default)]
    token_ttl: String,
    #[serde(rename = "@max_audio_bytes", default)]
    max_audio_bytes: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PeersXml {
    #[serde(rename = "peer", default)]
    peers: Vec<PeerXml>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PeerXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@enabled", default)]
    enabled: String,
    #[serde(rename = "@host", default)]
    host: String,
    #[serde(rename = "@port", default)]
    port: String,
    #[serde(rename = "@direction", default)]
    direction: String,
    #[serde(rename = "@receive_policy", default)]
    receive_policy: String,
    #[serde(rename = "@protocol", default)]
    protocol: String,
    #[serde(rename = "@user", default)]
    user: String,
    #[serde(rename = "@user_env", default)]
    user_env: String,
    #[serde(rename = "@password_env", default)]
    password_env: String,
    #[serde(rename = "@remote_command", default)]
    remote_command: String,
    #[serde(rename = "@data_protocol", default)]
    data_protocol: String,
    #[serde(default)]
    allow: AllowXml,
    #[serde(default)]
    filters: FiltersXml,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AllowXml {
    #[serde(rename = "ip", default)]
    ips: Vec<TextNode>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FiltersXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<TextNode>,
    #[serde(rename = "location", default)]
    locations: Vec<TextNode>,
    #[serde(rename = "severity", default)]
    severities: Vec<TextNode>,
    #[serde(rename = "event", default)]
    events: Vec<TextNode>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TextNode {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "$text", default)]
    value: String,
}

#[derive(Debug, Clone)]
struct Config {
    base_dir: PathBuf,
    enabled: bool,
    listener: ListenerConfig,
    audio: AudioServerConfig,
    peers: Vec<PeerConfig>,
}

#[derive(Debug, Clone)]
struct ListenerConfig {
    enabled: bool,
    addr: String,
    max_message_bytes: usize,
    read_timeout: Duration,
}

#[derive(Debug, Clone)]
struct AudioServerConfig {
    enabled: bool,
    addr: String,
    public_base_url: String,
    token_ttl: Duration,
    max_audio_bytes: u64,
}

#[derive(Debug, Clone)]
struct PeerConfig {
    id: String,
    enabled: bool,
    host: String,
    port: u16,
    protocol: PeerProtocol,
    user: String,
    user_env: String,
    password_env: String,
    remote_command: String,
    direction: Direction,
    receive_policy: ReceivePolicy,
    allowed_ips: Vec<IpAddr>,
    feeds: Vec<String>,
    locations: Vec<String>,
    severities: Vec<String>,
    events: Vec<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Direction {
    Send,
    Receive,
    SendReceive,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ReceivePolicy {
    ArchiveOnly,
    RelayIfMatched,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PeerProtocol {
    Tcp,
    SshStdin,
}

#[derive(Debug, Clone)]
struct EasNetMessage {
    fields: BTreeMap<String, String>,
    audio_upload: Option<AudioUpload>,
}

#[derive(Debug, Clone)]
struct AudioUpload {
    local_path: PathBuf,
    remote_name: String,
}

#[derive(Debug, Clone)]
struct TokenEntry {
    path: PathBuf,
    expires_at: SystemTime,
    peer_id: String,
    allowed_ips: Vec<IpAddr>,
    duration: Option<Duration>,
}

#[derive(Clone)]
struct AudioRegistry {
    tokens: Arc<RwLock<HashMap<String, TokenEntry>>>,
}

impl AudioRegistry {
    fn new() -> Self {
        Self {
            tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn register(
        &self,
        peer: &PeerConfig,
        path: PathBuf,
        ttl: Duration,
        duration: Option<Duration>,
    ) -> Result<String> {
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
        Ok(token)
    }

    async fn take_valid(&self, token: &str, ip: IpAddr) -> Option<TokenEntry> {
        let mut tokens = self.tokens.write().await;
        prune_tokens(&mut tokens);
        let entry = tokens.get(token)?;
        if !entry.allowed_ips.is_empty() && !entry.allowed_ips.contains(&ip) {
            return None;
        }
        Some(entry.clone())
    }
}

struct BridgeClient {
    writer: haze_bridge_client::BridgeWriter,
    reader: Mutex<haze_bridge_client::BridgeReader>,
}

impl BridgeClient {
    async fn connect(addr: &str) -> Result<Self> {
        let (reader, writer) =
            haze_bridge_client::connect(addr, &haze_bridge_client::Options::default(), None)
                .await?;
        Ok(Self {
            writer,
            reader: Mutex::new(reader),
        })
    }

    async fn publish(&self, event: Value) -> Result<()> {
        self.writer.send(&event).await
    }

    async fn read_event(&self) -> Result<Option<Value>> {
        let mut reader = self.reader.lock().await;
        Ok(reader.next_event().await?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("haze_easnet=info".parse()?))
        .with_target(false)
        .init();

    let args = Args::parse();
    let config = load_config(&args.config, &args.easnet)?;
    let loaded_env = load_local_env(&config.base_dir)?;
    if loaded_env > 0 {
        info!("loaded {loaded_env} EAS NET environment variable(s) from local .env");
    }
    if !config.enabled {
        info!("EAS NET service disabled by XML config");
        return Ok(());
    }

    let Some(bridge_addr) = args
        .bridge
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        bail!("--bridge/HAZE_HOST_BRIDGE_ADDR is required");
    };
    let bridge = Arc::new(BridgeClient::connect(bridge_addr).await?);
    let registry = AudioRegistry::new();

    bridge
        .publish(json!({
            "type": "easnet.status",
            "source": "haze-easnet",
            "data": {
                "status": "ready",
                "listener": config.listener.addr,
                "audio_server": config.audio.addr,
                "peers": config.peers.iter().filter(|peer| peer.enabled).count(),
            }
        }))
        .await?;

    if args.once {
        return Ok(());
    }

    let mut tasks = Vec::new();
    if config.listener.enabled {
        tasks.push(tokio::spawn(run_listener(
            config.clone(),
            Arc::clone(&bridge),
        )));
    }
    if config.audio.enabled {
        tasks.push(tokio::spawn(run_audio_server(
            config.audio.clone(),
            registry.clone(),
        )));
    }
    tasks.push(tokio::spawn(run_bridge_consumer(config, bridge, registry)));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("EAS NET service shutting down");
            Ok(())
        }
        result = async {
            for task in tasks {
                task.await??;
            }
            Ok::<(), anyhow::Error>(())
        } => result,
    }
}

fn load_config(config_path: &Path, easnet_path: &Path) -> Result<Config> {
    let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let path = resolve_path(base_dir, easnet_path);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read EAS NET config {}", path.display()))?;
    let raw = expand_env_vars(&raw);
    let root: RootXml = quick_xml::de::from_str(&raw).context("failed to parse EAS NET XML")?;
    let listener = ListenerConfig {
        enabled: xml_bool(&root.listener.enabled, true),
        addr: fallback(root.listener.addr, DEFAULT_LISTEN),
        max_message_bytes: root
            .listener
            .max_message_bytes
            .trim()
            .parse()
            .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES)
            .clamp(1024, 4 * 1024 * 1024),
        read_timeout: parse_duration(&root.listener.read_timeout).unwrap_or(DEFAULT_READ_TIMEOUT),
    };
    let audio = AudioServerConfig {
        enabled: xml_bool(&root.audio_server.enabled, true),
        addr: fallback(root.audio_server.addr, DEFAULT_AUDIO_LISTEN),
        public_base_url: root
            .audio_server
            .public_base_url
            .trim()
            .trim_end_matches('/')
            .to_string(),
        token_ttl: parse_duration(&root.audio_server.token_ttl).unwrap_or(DEFAULT_AUDIO_TTL),
        max_audio_bytes: root
            .audio_server
            .max_audio_bytes
            .trim()
            .parse()
            .unwrap_or(DEFAULT_MAX_AUDIO_BYTES)
            .clamp(1024, 512 * 1024 * 1024),
    };
    let peers = root
        .peers
        .peers
        .into_iter()
        .filter_map(|peer| peer_config(peer, base_dir))
        .collect::<Result<Vec<_>>>()?;
    Ok(Config {
        base_dir: base_dir.to_path_buf(),
        enabled: xml_bool(&root.enabled, true),
        listener,
        audio,
        peers,
    })
}

fn peer_config(raw: PeerXml, _base_dir: &Path) -> Option<Result<PeerConfig>> {
    let id = raw.id.trim().to_string();
    if id.is_empty() {
        return None;
    }
    let protocol = PeerProtocol::parse(&raw.protocol);
    let data_protocol = raw.data_protocol.trim();
    if !data_protocol.is_empty() && !data_protocol.eq_ignore_ascii_case("easnet") {
        return Some(Err(anyhow!(
            "EAS NET peer {id} uses unsupported data_protocol {data_protocol:?}"
        )));
    }
    let port = raw.port.trim().parse().unwrap_or(4098);
    let allowed_ips = raw
        .allow
        .ips
        .iter()
        .filter_map(|node| text_node_value(node).parse::<IpAddr>().ok())
        .collect::<Vec<_>>();
    Some(Ok(PeerConfig {
        id,
        enabled: xml_bool(&raw.enabled, false),
        host: raw.host.trim().to_string(),
        port,
        protocol,
        user: fallback(raw.user, "root"),
        user_env: raw.user_env.trim().to_string(),
        password_env: raw.password_env.trim().to_string(),
        remote_command: fallback(raw.remote_command, "cat > /tmp/EAS_NET_IN"),
        direction: Direction::parse(&raw.direction),
        receive_policy: ReceivePolicy::parse(&raw.receive_policy),
        allowed_ips,
        feeds: text_nodes(raw.filters.feeds),
        locations: text_nodes(raw.filters.locations),
        severities: text_nodes(raw.filters.severities),
        events: text_nodes(raw.filters.events),
    }))
}

async fn run_listener(config: Config, bridge: Arc<BridgeClient>) -> Result<()> {
    let listener = TcpListener::bind(&config.listener.addr)
        .await
        .with_context(|| format!("failed to bind EAS NET listener {}", config.listener.addr))?;
    info!("EAS NET listener active on {}", config.listener.addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        let config = config.clone();
        let bridge = Arc::clone(&bridge);
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
    bridge: Arc<BridgeClient>,
) -> Result<()> {
    let peer = match find_inbound_peer(&config.peers, addr.ip()) {
        Some(peer) => peer.clone(),
        None => {
            warn!(peer = %addr, "rejected EAS NET message from unknown peer");
            return Ok(());
        }
    };
    let mut data = Vec::new();
    let read_result = timeout(config.listener.read_timeout, async {
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
    .await;
    read_result.context("EAS NET read timed out")??;
    let raw = String::from_utf8_lossy(&data).to_string();
    let message = parse_easnet(&raw)?;
    let dedupe_key = message.dedupe_key(&peer.id);
    let body_hash = message.body_hash();

    bridge
        .publish(json!({
            "type": "easnet.message.received",
            "source": "haze-easnet",
            "subject": message.id(),
            "data": {
                "peer_id": peer.id,
                "remote_addr": addr.to_string(),
                "dedupe_key": dedupe_key,
                "body_hash": body_hash,
                "fields": message.fields,
            }
        }))
        .await?;

    if is_remote_control(&message) {
        bridge
            .publish(json!({
                "type": "easnet.alert.rejected",
                "source": "haze-easnet",
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

    let relay =
        peer.receive_policy == ReceivePolicy::RelayIfMatched && message_matches(&peer, &message);
    if !relay {
        bridge
            .publish(json!({
                "type": "easnet.alert.rejected",
                "source": "haze-easnet",
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
    bridge
        .publish(json!({
            "type": "easnet.alert.accepted",
            "source": "haze-easnet",
            "subject": message.id(),
            "data": data,
        }))
        .await?;
    bridge
        .publish(json!({
            "type": "cap.alert.broadcast.requested",
            "source": "haze-easnet",
            "subject": message.id(),
            "data": data,
        }))
        .await?;
    Ok(())
}

async fn run_bridge_consumer(
    config: Config,
    bridge: Arc<BridgeClient>,
    registry: AudioRegistry,
) -> Result<()> {
    loop {
        let Some(event) = bridge.read_event().await? else {
            bail!("host bridge closed");
        };
        if event.get("type").and_then(Value::as_str) != Some("cap.alert.audio.ready") {
            continue;
        }
        let data = event.get("data").cloned().unwrap_or_else(|| json!({}));
        for peer in config
            .peers
            .iter()
            .filter(|peer| peer.enabled && peer.direction.can_send())
        {
            if !event_matches_peer(peer, &data) {
                continue;
            }
            match outbound_message_for_event(&config, peer, &registry, &data).await {
                Ok(message) => {
                    if let Err(err) = send_to_peer(peer, &message).await {
                        warn!(
                            peer_id = %peer.id,
                            alert_id = %message.id(),
                            "EAS NET outbound send failed: {err:#}"
                        );
                        bridge
                            .publish(json!({
                                "type": "easnet.message.failed",
                                "source": "haze-easnet",
                                "subject": message.id(),
                                "data": {
                                    "peer_id": peer.id,
                                    "error": err.to_string(),
                                }
                            }))
                            .await?;
                    } else {
                        bridge
                            .publish(json!({
                                "type": "easnet.message.sent",
                                "source": "haze-easnet",
                                "subject": message.id(),
                                "data": {
                                    "peer_id": peer.id,
                                    "field_count": message.fields.len(),
                                    "audio_url_included": message.get("EAS.AUDIO.STREAM.URL").is_some(),
                                }
                            }))
                            .await?;
                    }
                }
                Err(err) => {
                    warn!(
                        peer_id = %peer.id,
                        "EAS NET outbound message build failed: {err:#}"
                    );
                    bridge
                        .publish(json!({
                            "type": "easnet.message.failed",
                            "source": "haze-easnet",
                            "data": {
                                "peer_id": peer.id,
                                "error": err.to_string(),
                            }
                        }))
                        .await?;
                }
            }
        }
    }
}

async fn outbound_message_for_event(
    config: &Config,
    peer: &PeerConfig,
    registry: &AudioRegistry,
    data: &Value,
) -> Result<EasNetMessage> {
    let object = data
        .as_object()
        .ok_or_else(|| anyhow!("event data is not an object"))?;
    let mut fields = BTreeMap::new();
    let alert_id = value_text(object.get("alert_id"))
        .or_else(|| value_text(object.get("id")))
        .or_else(|| value_text(object.get("queue_id")))
        .unwrap_or_else(|| format!("haze-{}", unix_now_ms()));
    fields.insert("EAS.NET.VERSION".to_string(), "1".to_string());
    fields.insert("EAS.NET.FUNC".to_string(), "EAS".to_string());
    fields.insert("EAS.ID".to_string(), alert_id.clone());
    fields.insert(
        "EAS.TYPE".to_string(),
        value_text(object.get("same_event"))
            .or_else(|| value_text(object.get("event")))
            .unwrap_or_else(|| "ADR".to_string()),
    );
    fields.insert(
        "EAS.ORG".to_string(),
        value_text(object.get("same_originator"))
            .or_else(|| value_text(object.get("originator")))
            .unwrap_or_else(|| "WXR".to_string()),
    );
    for (index, location) in value_list(
        object
            .get("same_locations")
            .or_else(|| object.get("locations")),
    )
    .into_iter()
    .enumerate()
    {
        fields.insert(format!("EAS.FIPS_{}", index + 1), location);
    }
    copy_field(object, &mut fields, "same_header", "EAS.HEADER");
    copy_field(object, &mut fields, "same_translation", "EAS.TRANSLATION");
    copy_field(object, &mut fields, "title", "EAS.HEADLINE");
    copy_field(object, &mut fields, "header", "EAS.HEADLINE");
    copy_field(object, &mut fields, "description", "EAS.DESCRIPTION");
    copy_field(object, &mut fields, "instruction", "EAS.INSTRUCTION");
    copy_field(object, &mut fields, "alert_text", "EAS.TEXT");
    copy_field(object, &mut fields, "severity", "EAS.SEVERITY");
    copy_field(object, &mut fields, "urgency", "EAS.URGENCY");
    copy_field(object, &mut fields, "certainty", "EAS.CERTAINTY");
    copy_field(object, &mut fields, "same_callsign", "EAS.STATION_ID");
    if let Some(sent) = value_text(
        object
            .get("alert_sent_at")
            .or_else(|| object.get("same_sent_at")),
    ) {
        fields.insert("EAS.START_TIME".to_string(), sent);
    }
    if let Some(expires) = value_text(
        object
            .get("alert_expires_at")
            .or_else(|| object.get("same_expires_at")),
    ) {
        fields.insert("EAS.END_TIME".to_string(), expires);
    }
    if let Some(duration) = value_text(object.get("same_duration")) {
        fields.insert("EAS.DURATION".to_string(), duration);
    }

    if config.audio.enabled {
        if let Some(audio_path) = value_text(object.get("audio_path")) {
            let path = resolve_path(&config.base_dir, Path::new(&audio_path));
            let metadata = fs::metadata(&path)
                .await
                .with_context(|| format!("EAS NET outbound audio missing: {}", path.display()))?;
            if metadata.len() <= config.audio.max_audio_bytes {
                let duration = outbound_audio_duration(object, &path, metadata.len());
                let source_path = path;
                if peer.protocol == PeerProtocol::SshStdin {
                    let upload_path =
                        prepare_outbound_audio_file(config, object, source_path).await?;
                    let extension = upload_path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .unwrap_or("wav");
                    let remote_name = easnet_audio_remote_name(&alert_id, extension);
                    fields.insert("EAS.AUDIO.FILE".to_string(), remote_name.clone());
                    fields.insert("EAS.AUDIO.FILE.ALERT".to_string(), remote_name.clone());
                    return Ok(EasNetMessage {
                        fields,
                        audio_upload: Some(AudioUpload {
                            local_path: upload_path,
                            remote_name,
                        }),
                    });
                }
                if !config.audio.public_base_url.is_empty() {
                    let path = prepare_outbound_audio(config, object, source_path).await?;
                    let token = registry
                        .register(peer, path.clone(), config.audio.token_ttl, duration)
                        .await?;
                    let url = format!("{}/easnet/audio/{}", config.audio.public_base_url, token);
                    fields.insert("EAS.AUDIO.STREAM.URL".to_string(), url);
                }
            }
        }
    }
    Ok(EasNetMessage {
        fields,
        audio_upload: None,
    })
}

async fn prepare_outbound_audio(
    config: &Config,
    object: &serde_json::Map<String, Value>,
    source_path: PathBuf,
) -> Result<PathBuf> {
    if source_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("ogg"))
    {
        return Ok(source_path);
    }
    let sample_rate = value_text(object.get("sample_rate"))
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(48_000)
        .clamp(8_000, 96_000);
    let channels = value_text(object.get("channels"))
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(1)
        .clamp(1, 8);
    let out_dir = config.base_dir.join("runtime").join("audio").join("easnet");
    fs::create_dir_all(&out_dir).await?;
    let out_path = out_dir.join(format!(
        "{}.ogg",
        make_nonce_token(&source_path.to_string_lossy())
    ));
    let input = source_path.clone();
    let output = out_path.clone();
    tokio::task::spawn_blocking(move || {
        encode_pcm_s16le_to_ogg_vorbis(&input, &output, sample_rate, channels)
    })
    .await
    .context("EAS NET ffmpeg audio conversion task failed")??;
    Ok(out_path)
}

async fn prepare_outbound_audio_file(
    config: &Config,
    object: &serde_json::Map<String, Value>,
    source_path: PathBuf,
) -> Result<PathBuf> {
    if source_path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("wav"))
    {
        return Ok(source_path);
    }
    let sample_rate = value_text(object.get("sample_rate"))
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(48_000)
        .clamp(8_000, 96_000);
    let channels = value_text(object.get("channels"))
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(1)
        .clamp(1, 8);
    let out_dir = config.base_dir.join("runtime").join("audio").join("easnet");
    fs::create_dir_all(&out_dir).await?;
    let out_path = out_dir.join(format!(
        "{}.wav",
        make_nonce_token(&source_path.to_string_lossy())
    ));
    let input = source_path.clone();
    let output = out_path.clone();
    let object = object.clone();
    tokio::task::spawn_blocking(move || {
        write_easnet_alert_wav(&input, &output, &object, sample_rate, channels)
    })
    .await
    .context("EAS NET WAV audio preparation task failed")??;
    Ok(out_path)
}

fn write_easnet_alert_wav(
    input_path: &Path,
    output_path: &Path,
    object: &serde_json::Map<String, Value>,
    sample_rate: u32,
    channels: u16,
) -> Result<()> {
    let bytes = std::fs::read(input_path)
        .with_context(|| format!("failed to read EAS NET PCM audio {}", input_path.display()))?;
    let bytes = easnet_alert_body_pcm(input_path, object, &bytes, sample_rate, channels)
        .unwrap_or_else(|| bytes);
    write_pcm_s16le_wav_bytes(&bytes, output_path, sample_rate, channels)
}

fn write_pcm_s16le_wav_bytes(
    bytes: &[u8],
    output_path: &Path,
    sample_rate: u32,
    channels: u16,
) -> Result<()> {
    let channels = channels.clamp(1, 8);
    let sample_rate = sample_rate.clamp(8_000, 96_000);
    let block_align = channels
        .checked_mul(2)
        .ok_or_else(|| anyhow!("invalid EAS NET WAV channel count"))?;
    let complete_len = bytes.len() - (bytes.len() % usize::from(block_align));
    if complete_len == 0 {
        bail!("EAS NET PCM audio is empty for {}", output_path.display());
    }
    let data_len = u32::try_from(complete_len).context("EAS NET WAV audio is too large")?;
    let riff_len = 36u32
        .checked_add(data_len)
        .ok_or_else(|| anyhow!("EAS NET WAV audio is too large"))?;
    let byte_rate = sample_rate
        .checked_mul(u32::from(block_align))
        .ok_or_else(|| anyhow!("invalid EAS NET WAV byte rate"))?;
    let mut out = Vec::with_capacity(44 + complete_len);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&riff_len.to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(&bytes[..complete_len]);
    std::fs::write(output_path, out).with_context(|| {
        format!(
            "failed to write EAS NET WAV audio {}",
            output_path.display()
        )
    })
}

fn easnet_alert_body_pcm(
    input_path: &Path,
    object: &serde_json::Map<String, Value>,
    bytes: &[u8],
    sample_rate: u32,
    channels: u16,
) -> Option<Vec<u8>> {
    if sample_rate != SAME_SAMPLE_RATE || channels == 0 {
        return None;
    }
    if !looks_like_haze_same_alert_audio(input_path, object) {
        return None;
    }
    let header = value_text(
        object
            .get("same_header")
            .or_else(|| object.get("EAS.HEADER"))
            .or_else(|| object.get("header")),
    )?;
    let lead_samples = same_header_attention_samples(&header, object)?
        .checked_add(PLAYLIST_POST_HEADER_SILENCE_SAMPLES)?;
    let tail_samples = same_eom_samples();
    let bytes_per_sample = usize::from(channels).checked_mul(2)?;
    let lead_bytes = lead_samples.checked_mul(bytes_per_sample)?;
    let tail_bytes = tail_samples.checked_mul(bytes_per_sample)?;
    let end = bytes.len().checked_sub(tail_bytes)?;
    if lead_bytes >= end {
        return None;
    }
    let body = bytes[lead_bytes..end].to_vec();
    if body.len() < bytes_per_sample * usize::try_from(sample_rate / 2).ok()? {
        return None;
    }
    Some(body)
}

fn looks_like_haze_same_alert_audio(
    input_path: &Path,
    object: &serde_json::Map<String, Value>,
) -> bool {
    let source = value_text(object.get("source"))
        .or_else(|| nested_text(object.get("alert_packet"), &["audio", "source"]))
        .unwrap_or_default();
    if source.contains("cap-same") {
        return true;
    }
    input_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with("_same.pcm16le") || name.contains("_same."))
}

fn same_header_attention_samples(
    header: &str,
    object: &serde_json::Map<String, Value>,
) -> Option<usize> {
    let burst = same_burst_samples(SAME_PREAMBLE_BYTES.checked_add(header.as_bytes().len())?);
    let mut samples = SAME_BURST_LEAD_SAMPLES.checked_add(triple_burst_samples(burst)?)?;
    if same_attention_enabled(object) {
        samples = samples
            .checked_add(SAME_PRE_ATTENTION_SAMPLES)?
            .checked_add(SAME_ATTENTION_SAMPLES)?;
    }
    Some(samples)
}

fn same_eom_samples() -> usize {
    let burst = same_burst_samples(SAME_PREAMBLE_BYTES + 4);
    SAME_EOM_LEAD_SAMPLES + triple_burst_samples(burst).unwrap_or(0) + SAME_EOM_TAIL_SAMPLES
}

fn same_burst_samples(byte_len: usize) -> usize {
    byte_len * 8 * SAME_BIT_SAMPLES
}

fn triple_burst_samples(burst_samples: usize) -> Option<usize> {
    burst_samples
        .checked_mul(3)?
        .checked_add(SAME_INTER_BURST_SAMPLES.checked_mul(2)?)
}

fn same_attention_enabled(object: &serde_json::Map<String, Value>) -> bool {
    let tone = value_text(object.get("same_tone").or_else(|| object.get("tone_type")))
        .or_else(|| nested_text(object.get("alert_packet"), &["same", "tone"]))
        .unwrap_or_else(|| "WXR".to_string());
    !matches!(
        tone.trim().to_ascii_uppercase().as_str(),
        "" | "NONE" | "NO" | "OFF" | "DISABLED"
    )
}

fn nested_text(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut current = value?;
    for key in path {
        current = current.get(*key)?;
    }
    value_text(Some(current))
}

fn outbound_audio_duration(
    object: &serde_json::Map<String, Value>,
    source_path: &Path,
    byte_len: u64,
) -> Option<Duration> {
    for key in ["duration_ms", "audio_duration_ms"] {
        if let Some(ms) = value_text(object.get(key)).and_then(|value| value.parse::<u64>().ok()) {
            if ms > 0 {
                return Some(Duration::from_millis(ms));
            }
        }
    }
    let extension = source_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default();
    if !extension.eq_ignore_ascii_case("pcm")
        && !extension.eq_ignore_ascii_case("s16")
        && !extension.eq_ignore_ascii_case("pcm16le")
    {
        return None;
    }
    let sample_rate = value_text(object.get("sample_rate"))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(48_000)
        .clamp(8_000, 96_000);
    let channels = value_text(object.get("channels"))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1)
        .clamp(1, 8);
    let bytes_per_second = sample_rate.saturating_mul(channels).saturating_mul(2);
    if bytes_per_second == 0 || byte_len == 0 {
        return None;
    }
    Some(Duration::from_secs_f64(
        byte_len as f64 / bytes_per_second as f64,
    ))
}

fn easnet_audio_remote_name(alert_id: &str, extension: &str) -> String {
    let mut safe = String::with_capacity(alert_id.len().min(96) + 4);
    for ch in alert_id.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            safe.push(ch);
        } else {
            safe.push('_');
        }
        if safe.len() >= 96 {
            break;
        }
    }
    let safe = safe.trim_matches('_');
    let extension = extension
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    let extension = if extension.is_empty() {
        "wav"
    } else {
        extension.as_str()
    };
    if safe.is_empty() {
        format!("haze_easnet_audio.{extension}")
    } else {
        format!("{safe}.{extension}")
    }
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn encode_pcm_s16le_to_ogg_vorbis(
    input_path: &Path,
    output_path: &Path,
    sample_rate: u32,
    channels: u16,
) -> Result<()> {
    let channels = channels.clamp(1, 8);
    let sample_rate = sample_rate.clamp(8_000, 96_000);
    let len = std::fs::metadata(input_path)
        .with_context(|| format!("failed to stat EAS NET PCM audio {}", input_path.display()))?
        .len();
    let block_align = u64::from(channels) * 2;
    if len < block_align {
        bail!("EAS NET PCM audio is empty: {}", input_path.display());
    }

    let mut failures = Vec::new();
    for codec in ["libvorbis", "vorbis"] {
        match run_ffmpeg_pcm_to_ogg(input_path, output_path, sample_rate, channels, codec) {
            Ok(()) => return Ok(()),
            Err(err) => {
                failures.push(format!("{codec}: {err:#}"));
                let _ = std::fs::remove_file(output_path);
            }
        }
    }
    bail!(
        "failed to encode EAS NET Ogg/Vorbis audio {}: {}",
        output_path.display(),
        failures.join("; ")
    )
}

fn run_ffmpeg_pcm_to_ogg(
    input_path: &Path,
    output_path: &Path,
    sample_rate: u32,
    channels: u16,
    codec: &str,
) -> Result<()> {
    let ffmpeg = env::var("HAZE_FFMPEG")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "ffmpeg".to_string());
    let output = Command::new(&ffmpeg)
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-y")
        .arg("-f")
        .arg("s16le")
        .arg("-ar")
        .arg(sample_rate.to_string())
        .arg("-ac")
        .arg(channels.to_string())
        .arg("-i")
        .arg(input_path)
        .arg("-vn")
        .arg("-c:a")
        .arg(codec)
        .arg("-b:a")
        .arg("128k")
        .arg("-f")
        .arg("ogg")
        .arg(output_path)
        .output()
        .with_context(|| format!("failed to run {ffmpeg} for EAS NET audio conversion"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if stderr.is_empty() {
        "no stderr output".to_string()
    } else {
        stderr
    };
    bail!("{ffmpeg} exited with {}: {detail}", output.status)
}

async fn send_to_peer(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    match peer.protocol {
        PeerProtocol::Tcp => send_to_tcp_peer(peer, message).await,
        PeerProtocol::SshStdin => send_to_ssh_stdin_peer(peer, message).await,
    }
}

async fn send_to_tcp_peer(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    if peer.host.trim().is_empty() {
        bail!("peer host is empty");
    }
    let addr = format!("{}:{}", peer.host, peer.port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect to EAS NET peer {addr}"))?;
    let raw = serialize_easnet(message);
    stream.write_all(raw.as_bytes()).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

async fn send_to_ssh_stdin_peer(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    timeout(
        Duration::from_secs(30),
        send_to_ssh_stdin_peer_native(peer, message),
    )
    .await
    .context("EAS NET native SSH sender timed out")?
}

async fn send_to_ssh_stdin_peer_native(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    if let Some(upload) = &message.audio_upload {
        let bytes = fs::read(&upload.local_path).await.with_context(|| {
            format!(
                "failed to read EAS NET upload {}",
                upload.local_path.display()
            )
        })?;
        let command = format!("cat > {}", shell_single_quote(&upload.remote_name));
        exec_native_ssh_stdin(peer, &command, bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to upload EAS NET alert audio {}",
                    upload.local_path.display()
                )
            })?;
    }
    exec_native_ssh_stdin(
        peer,
        peer.remote_command.trim(),
        serialize_easnet(message).into_bytes(),
    )
    .await
}

async fn exec_native_ssh_stdin(peer: &PeerConfig, command: &str, input: Vec<u8>) -> Result<()> {
    if peer.host.trim().is_empty() {
        bail!("peer host is empty");
    }
    let username = peer_ssh_user(peer);
    let password = peer_ssh_password(peer)?;
    let config = Arc::new(client::Config {
        inactivity_timeout: Some(DEFAULT_READ_TIMEOUT),
        ..Default::default()
    });
    let mut session = client::connect(config, (peer.host.trim(), peer.port), EasNetSshClient {})
        .await
        .with_context(|| format!("failed to connect to EAS NET SSH peer {}", peer.host))?;

    authenticate_native_ssh(&mut session, &username, &password).await?;

    let mut channel = session
        .channel_open_session()
        .await
        .context("failed to open EAS NET SSH session channel")?;
    channel
        .exec(true, command)
        .await
        .context("failed to exec EAS NET remote command")?;
    channel
        .data_bytes(input)
        .await
        .context("failed to write EAS NET message to native SSH channel")?;
    channel
        .eof()
        .await
        .context("failed to send native SSH EOF for EAS NET message")?;

    let mut exit_status = None;
    let mut stderr = String::new();
    while let Some(message) = channel.wait().await {
        match message {
            ChannelMsg::ExitStatus {
                exit_status: status,
            } => {
                exit_status = Some(status);
            }
            ChannelMsg::ExtendedData { data, .. } => {
                stderr.push_str(&String::from_utf8_lossy(&data));
            }
            _ => {}
        }
    }
    session
        .disconnect(Disconnect::ByApplication, "EAS NET send complete", "en")
        .await
        .ok();

    match exit_status {
        Some(0) => Ok(()),
        Some(status) => {
            let stderr = sanitize_ssh_error(&stderr);
            if stderr.is_empty() {
                bail!("EAS NET native SSH sender exited with status {status}");
            }
            bail!("EAS NET native SSH sender exited with status {status}: {stderr}");
        }
        None => bail!("EAS NET native SSH sender closed without an exit status"),
    }
}

async fn authenticate_native_ssh(
    session: &mut client::Handle<EasNetSshClient>,
    username: &str,
    password: &str,
) -> Result<()> {
    let password_auth = session
        .authenticate_password(username, password)
        .await
        .context("native SSH password authentication failed")?;
    if password_auth.success() {
        return Ok(());
    }
    match session
        .authenticate_keyboard_interactive_start(username.to_string(), None::<String>)
        .await
        .context("native SSH keyboard-interactive authentication failed")?
    {
        client::KeyboardInteractiveAuthResponse::Success => Ok(()),
        client::KeyboardInteractiveAuthResponse::InfoRequest { prompts, .. } => {
            let responses = prompts.iter().map(|_| password.to_string()).collect();
            match session
                .authenticate_keyboard_interactive_respond(responses)
                .await
                .context("native SSH keyboard-interactive response failed")?
            {
                client::KeyboardInteractiveAuthResponse::Success => Ok(()),
                _ => bail!("native SSH authentication was rejected"),
            }
        }
        _ => bail!("native SSH authentication was rejected"),
    }
}

fn peer_ssh_user(peer: &PeerConfig) -> String {
    if !peer.user_env.trim().is_empty() {
        if let Ok(value) = std::env::var(peer.user_env.trim()) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    fallback(peer.user.clone(), "root")
}

fn peer_ssh_password(peer: &PeerConfig) -> Result<String> {
    if peer.password_env.trim().is_empty() {
        bail!("EAS NET native SSH peer has no password_env configured");
    }
    let password = std::env::var(peer.password_env.trim())
        .with_context(|| format!("{} is not set", peer.password_env.trim()))?;
    let password = password.trim().to_string();
    if password.is_empty() {
        bail!("{} is empty", peer.password_env.trim());
    }
    Ok(password)
}

#[derive(Clone)]
struct EasNetSshClient;

impl client::Handler for EasNetSshClient {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

fn sanitize_ssh_error(raw: &str) -> String {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.to_ascii_lowercase().contains("password") {
                "[password-related SSH error redacted]".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

async fn run_audio_server(config: AudioServerConfig, registry: AudioRegistry) -> Result<()> {
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

fn parse_easnet(raw: &str) -> Result<EasNetMessage> {
    let mut in_body = false;
    let mut fields = BTreeMap::new();
    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.eq_ignore_ascii_case("#BEGIN") {
            in_body = true;
            continue;
        }
        if line.eq_ignore_ascii_case("#END") {
            break;
        }
        if !in_body || line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_uppercase();
        if key.is_empty() || key.len() > 128 {
            continue;
        }
        fields.insert(key, trim_value(value));
    }
    if fields.is_empty() {
        bail!("EAS NET message contained no fields");
    }
    Ok(EasNetMessage {
        fields,
        audio_upload: None,
    })
}

fn serialize_easnet(message: &EasNetMessage) -> String {
    let mut out = String::from("#BEGIN\n");
    for (key, value) in &message.fields {
        out.push_str(key);
        out.push('=');
        out.push_str(&value.replace(['\r', '\n'], " "));
        out.push('\n');
    }
    out.push_str("#END\n");
    out
}

fn inbound_broadcast_payload(peer: &PeerConfig, message: &EasNetMessage) -> Value {
    let id = message.id();
    let event = message
        .first_nonblank(&["EAS.TYPE", "EAS.ALERT_EVENT.TYPE"])
        .unwrap_or("EAS");
    let headline = message
        .first_nonblank(&["EAS.HEADLINE", "EAS.TEXT"])
        .unwrap_or(event);
    let description = message
        .first_nonblank(&["EAS.DESCRIPTION", "EAS.TEXT"])
        .unwrap_or("");
    let instruction = message.get("EAS.INSTRUCTION").unwrap_or("");
    let locations = message.locations();
    let alert_text = [description, instruction]
        .into_iter()
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let banner_text = message
        .first_nonblank(&["EAS.TEXT", "EAS.DESCRIPTION", "EAS.HEADLINE"])
        .unwrap_or(headline);
    let audio_url = message
        .first_nonblank(&["EAS.AUDIO.STREAM.URL", "EAS.AUDIO.URL"])
        .unwrap_or("");
    let feed_ids = if peer.feeds.is_empty() {
        vec!["*".to_string()]
    } else {
        peer.feeds.clone()
    };
    json!({
        "feed_id": feed_ids.first().cloned().unwrap_or_else(|| "*".to_string()),
        "feed_ids": feed_ids,
        "alert_id": id,
        "id": id,
        "package_id": "alerts",
        "title": headline,
        "headline": headline,
        "event": event,
        "message_type": "Alert",
        "sender": peer.id,
        "severity": message.get("EAS.SEVERITY").unwrap_or("Unknown"),
        "urgency": message.get("EAS.URGENCY").unwrap_or("Unknown"),
        "certainty": message.get("EAS.CERTAINTY").unwrap_or("Unknown"),
        "description": description,
        "instruction": instruction,
        "alert_text": alert_text,
        "banner_text": banner_text,
        "include_same": !locations.is_empty(),
        "same_event": event,
        "same_originator": message.get("EAS.ORG").unwrap_or("EAS"),
        "same_locations": locations,
        "same_duration": message.get("EAS.DURATION").unwrap_or("0015"),
        "same_callsign": message.get("EAS.STATION_ID").unwrap_or("EASNET"),
        "same_header": message.get("EAS.HEADER").unwrap_or(""),
        "same_translation": message.get("EAS.TRANSLATION").unwrap_or(""),
        "alert_sent_at": message.get("EAS.START_TIME").unwrap_or(""),
        "alert_expires_at": message.get("EAS.END_TIME").unwrap_or(""),
        "audio_url": audio_url,
        "audio_format": "external",
        "source": "easnet",
        "alert_packet": {
            "version": 1,
            "id": id,
            "source": "easnet",
            "feed_ids": feed_ids,
            "message_type": "Alert",
            "content": {
                "title": headline,
                "headline": headline,
                "event": event,
                "event_name": event,
                "severity": message.get("EAS.SEVERITY").unwrap_or("Unknown"),
                "urgency": message.get("EAS.URGENCY").unwrap_or("Unknown"),
                "certainty": message.get("EAS.CERTAINTY").unwrap_or("Unknown"),
                "description": description,
                "instruction": instruction
            },
            "timing": {
                "sent_at": message.get("EAS.START_TIME").unwrap_or(""),
                "expires_at": message.get("EAS.END_TIME").unwrap_or("")
            },
            "areas": {
                "codes": message.locations()
            },
            "same": {
                "include": !message.locations().is_empty(),
                "event": event,
                "originator": message.get("EAS.ORG").unwrap_or("EAS"),
                "locations": message.locations(),
                "duration": message.get("EAS.DURATION").unwrap_or("0015"),
                "callsign": message.get("EAS.STATION_ID").unwrap_or("EASNET"),
                "header": message.get("EAS.HEADER").unwrap_or(""),
                "translation": message.get("EAS.TRANSLATION").unwrap_or("")
            },
            "audio": {
                "url": audio_url,
                "source": "easnet"
            },
            "presentation": {
                "speech_text": alert_text,
                "banner_text": banner_text
            },
            "meta": {
                "easnet_peer": peer.id,
                "easnet_fields": message.fields
            }
        }
    })
}

fn find_inbound_peer(peers: &[PeerConfig], ip: IpAddr) -> Option<&PeerConfig> {
    peers.iter().find(|peer| {
        peer.enabled
            && peer.direction.can_receive()
            && (peer.allowed_ips.is_empty() || peer.allowed_ips.contains(&ip))
    })
}

fn message_matches(peer: &PeerConfig, message: &EasNetMessage) -> bool {
    set_matches(
        &peer.events,
        message
            .first_nonblank(&["EAS.TYPE", "EAS.ALERT_EVENT.TYPE"])
            .unwrap_or(""),
    ) && set_matches(&peer.severities, message.get("EAS.SEVERITY").unwrap_or(""))
        && locations_match(&peer.locations, &message.locations())
}

fn event_matches_peer(peer: &PeerConfig, data: &Value) -> bool {
    let Some(object) = data.as_object() else {
        return false;
    };
    set_matches(
        &peer.events,
        value_text(object.get("same_event"))
            .or_else(|| value_text(object.get("event")))
            .unwrap_or_default()
            .as_str(),
    ) && locations_match(
        &peer.locations,
        &value_list(
            object
                .get("same_locations")
                .or_else(|| object.get("locations")),
        ),
    ) && feed_matches(
        &peer.feeds,
        &value_list(object.get("feed_ids"))
            .into_iter()
            .chain(value_text(object.get("feed_id")))
            .collect::<Vec<_>>(),
    )
}

fn is_remote_control(message: &EasNetMessage) -> bool {
    let func = message
        .get("EAS.NET.FUNC")
        .unwrap_or("")
        .to_ascii_uppercase();
    let alert_type = message
        .get("EAS.ALERT_EVENT.TYPE")
        .unwrap_or("")
        .to_ascii_uppercase();
    let blocked = [
        "RUN_PRESET_RWT",
        "MANUAL_FORWARD",
        "MSG_ABORT",
        "MSG_LIVE_START",
        "MSG_LIVE_END",
        "EAS_LIVE_START",
        "EAS_LIVE_END",
        "STOP LIVE",
    ];
    blocked
        .iter()
        .any(|value| func == *value || alert_type == *value)
}

impl EasNetMessage {
    fn get(&self, key: &str) -> Option<&str> {
        self.fields
            .get(&key.to_ascii_uppercase())
            .map(String::as_str)
    }

    fn first_nonblank(&self, keys: &[&str]) -> Option<&str> {
        keys.iter()
            .filter_map(|key| self.get(key))
            .find(|value| !value.trim().is_empty())
    }

    fn id(&self) -> String {
        self.first_nonblank(&["EAS.ID", "EAS.HEADER"])
            .map(str::to_string)
            .unwrap_or_else(|| format!("easnet-{}", unix_now_ms()))
    }

    fn locations(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for (key, value) in &self.fields {
            if key == "EAS.FIPS" || key.starts_with("EAS.FIPS_") || key == "EAS.FIPS_" {
                for part in value.split([',', ';', ' ']) {
                    let part = part.trim();
                    if part.len() == 6
                        && part.chars().all(|ch| ch.is_ascii_digit())
                        && seen.insert(part.to_string())
                    {
                        out.push(part.to_string());
                    }
                }
            }
        }
        if out.is_empty() {
            if let Some(header) = self.get("EAS.HEADER") {
                out.extend(header_locations(header));
            }
        }
        out
    }

    fn dedupe_key(&self, peer_id: &str) -> String {
        make_token(&format!(
            "{}:{}:{}:{}:{}",
            peer_id,
            self.id(),
            self.get("EAS.HEADER").unwrap_or(""),
            self.get("EAS.START_TIME").unwrap_or(""),
            self.get("EAS.END_TIME").unwrap_or("")
        ))
    }

    fn body_hash(&self) -> String {
        make_token(&serialize_easnet(self))
    }
}

impl Direction {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "send" | "out" | "outbound" => Self::Send,
            "receive" | "recv" | "in" | "inbound" => Self::Receive,
            _ => Self::SendReceive,
        }
    }

    fn can_send(self) -> bool {
        matches!(self, Self::Send | Self::SendReceive)
    }

    fn can_receive(self) -> bool {
        matches!(self, Self::Receive | Self::SendReceive)
    }
}

impl ReceivePolicy {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "relay-if-matched" | "relay" | "broadcast" => Self::RelayIfMatched,
            _ => Self::ArchiveOnly,
        }
    }
}

impl PeerProtocol {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "ssh" | "ssh-stdin" | "stdin" | "scp" | "sshcp" => Self::SshStdin,
            _ => Self::Tcp,
        }
    }
}

fn contains_end_marker(data: &[u8]) -> bool {
    data.windows(4)
        .any(|window| window.eq_ignore_ascii_case(b"#END"))
}

fn header_locations(header: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for part in header.split('-') {
        let part = part
            .split_once('+')
            .map(|(location, _)| location)
            .unwrap_or(part)
            .trim();
        if part.len() == 6
            && part.chars().all(|ch| ch.is_ascii_digit())
            && seen.insert(part.to_string())
        {
            out.push(part.to_string());
        }
    }
    out
}

fn feed_matches(filters: &[String], feeds: &[String]) -> bool {
    if filters.is_empty() || filters.iter().any(|value| value == "*") {
        return true;
    }
    feeds.iter().any(|feed| {
        filters
            .iter()
            .any(|filter| filter.eq_ignore_ascii_case(feed))
    })
}

fn locations_match(filters: &[String], locations: &[String]) -> bool {
    if filters.is_empty() || filters.iter().any(|value| value == "*") {
        return true;
    }
    locations
        .iter()
        .any(|location| filters.iter().any(|filter| filter == location))
}

fn set_matches(filters: &[String], value: &str) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| filter == "*")
        || filters
            .iter()
            .any(|filter| filter.eq_ignore_ascii_case(value))
}

fn copy_field(
    object: &serde_json::Map<String, Value>,
    fields: &mut BTreeMap<String, String>,
    source: &str,
    target: &str,
) {
    if fields.contains_key(target) {
        return;
    }
    if let Some(value) = value_text(object.get(source)) {
        fields.insert(target.to_string(), value);
    }
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        }
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(|value| value_text(Some(value)))
            .collect(),
        Some(Value::String(value)) => value
            .split([',', ';'])
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn text_nodes(nodes: Vec<TextNode>) -> Vec<String> {
    nodes
        .iter()
        .map(text_node_value)
        .filter(|value| !value.is_empty())
        .collect()
}

fn text_node_value(node: &TextNode) -> String {
    if !node.id.trim().is_empty() {
        node.id.trim().to_string()
    } else {
        node.value.trim().to_string()
    }
}

fn trim_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].trim().to_string()
    } else {
        value.to_string()
    }
}

fn xml_bool(raw: &str, default: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" | "enabled" => true,
        "0" | "false" | "no" | "off" | "disabled" => false,
        _ => default,
    }
}

fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let (number, multiplier) = if let Some(number) = raw.strip_suffix("ms") {
        (number, 1)
    } else if let Some(number) = raw.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = raw.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = raw.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        (raw, 1_000)
    };
    number
        .trim()
        .parse::<u64>()
        .ok()
        .map(|value| Duration::from_millis(value.saturating_mul(multiplier)))
}

fn fallback(value: String, default: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    }
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn load_local_env(base_dir: &Path) -> Result<usize> {
    let path = base_dir.join(".env");
    if !path.is_file() {
        return Ok(0);
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read local .env {}", path.display()))?;
    let mut loaded = 0usize;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || std::env::var_os(key).is_some() {
            continue;
        }
        std::env::set_var(key, parse_env_value(value.trim()));
        loaded += 1;
    }
    Ok(loaded)
}

fn expand_env_vars(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for next in chars.by_ref() {
                if next == '}' {
                    break;
                }
                name.push(next);
            }
            out.push_str(&std::env::var(name).unwrap_or_default());
            continue;
        }
        let mut name = String::new();
        while let Some(next) = chars.peek().copied() {
            if next == '_' || next.is_ascii_alphanumeric() {
                name.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if name.is_empty() {
            out.push('$');
        } else {
            out.push_str(&std::env::var(name).unwrap_or_default());
        }
    }
    out
}

fn parse_env_value(raw: &str) -> String {
    let raw = raw.trim();
    if raw.len() >= 2 {
        let bytes = raw.as_bytes();
        if (bytes[0] == b'"' && bytes[raw.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[raw.len() - 1] == b'\'')
        {
            return raw[1..raw.len() - 1].to_string();
        }
    }
    raw.to_string()
}

fn make_token(input: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(input.as_bytes());
    let hash = digest.finalize();
    hash[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn make_nonce_token(input: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(input.as_bytes());
    digest.update(unix_now_ms().to_le_bytes());
    let hash = digest.finalize();
    hash[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn prune_tokens(tokens: &mut HashMap<String, TokenEntry>) {
    let now = SystemTime::now();
    tokens.retain(|_, entry| entry.expires_at > now);
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], "")
}

#[allow(dead_code)]
fn parse_rfc3339(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_serializes_easnet_message() {
        let raw =
            "#BEGIN\nEAS.NET.FUNC=EAS\nEAS.ID=test\nEAS.FIPS_1=065100\nEAS.TEXT=Hello\n#END\n";
        let message = parse_easnet(raw).unwrap();
        assert_eq!(message.get("EAS.ID"), Some("test"));
        assert_eq!(message.locations(), vec!["065100"]);
        let encoded = serialize_easnet(&message);
        assert!(encoded.starts_with("#BEGIN\n"));
        assert!(encoded.contains("EAS.TEXT=Hello\n"));
        assert!(encoded.ends_with("#END\n"));
    }

    #[test]
    fn extracts_locations_from_same_header() {
        let message = parse_easnet(
            "#BEGIN\nEAS.ID=x\nEAS.HEADER=ZCZC-WXR-SVR-065100-065200+0015-1841200-CAPALL  -\n#END\n",
        )
        .unwrap();
        assert_eq!(message.locations(), vec!["065100", "065200"]);
    }

    #[test]
    fn blocks_remote_control_messages() {
        let message =
            parse_easnet("#BEGIN\nEAS.NET.FUNC=RUN_PRESET_RWT\nEAS.ID=x\n#END\n").unwrap();
        assert!(is_remote_control(&message));
    }

    #[test]
    fn inbound_payload_carries_audio_url_and_same_fields() {
        let peer = PeerConfig {
            id: "dasdec".to_string(),
            enabled: true,
            host: "172.16.1.42".to_string(),
            port: 4098,
            protocol: PeerProtocol::Tcp,
            user: "root".to_string(),
            user_env: String::new(),
            password_env: String::new(),
            remote_command: "cat > /tmp/EAS_NET_IN".to_string(),
            direction: Direction::SendReceive,
            receive_policy: ReceivePolicy::RelayIfMatched,
            allowed_ips: vec![],
            feeds: vec!["CAP-IT-ALL".to_string()],
            locations: vec![],
            severities: vec![],
            events: vec![],
        };
        let message = parse_easnet(
            "#BEGIN\nEAS.ID=eas1\nEAS.TYPE=SVR\nEAS.ORG=WXR\nEAS.FIPS_1=065100\nEAS.AUDIO.STREAM.URL=http://device/audio.wav\nEAS.DESCRIPTION=Storm text.\n#END\n",
        )
        .unwrap();
        let payload = inbound_broadcast_payload(&peer, &message);
        assert_eq!(payload["feed_ids"][0], "CAP-IT-ALL");
        assert_eq!(payload["same_event"], "SVR");
        assert_eq!(payload["same_locations"][0], "065100");
        assert_eq!(payload["audio_url"], "http://device/audio.wav");
        assert_eq!(
            payload["alert_packet"]["audio"]["url"],
            "http://device/audio.wav"
        );
    }

    #[test]
    fn parses_duration_suffixes() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
    }

    #[test]
    fn ffmpeg_audio_encoder_writes_ogg_vorbis() {
        let dir = std::env::temp_dir().join(format!("haze-easnet-audio-{}", unix_now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("alert.pcm");
        let output = dir.join("alert.ogg");
        let mut pcm = Vec::new();
        for sample in 0..4800 {
            let phase = (sample as f32 / 48_000.0) * 440.0 * std::f32::consts::TAU;
            let value = (phase.sin() * f32::from(i16::MAX) * 0.25) as i16;
            pcm.extend_from_slice(&value.to_le_bytes());
        }
        std::fs::write(&input, pcm).unwrap();

        encode_pcm_s16le_to_ogg_vorbis(&input, &output, 48_000, 1).unwrap();
        let data = std::fs::read(&output).unwrap();
        assert!(data.starts_with(b"OggS"));
        assert!(data.len() > 64);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn dasdec_upload_strips_haze_same_wrapper() {
        let path = PathBuf::from("000_CAP-IT-ALL_alert_same.pcm16le");
        let header = "ZCZC-WXR-SVR-066311+0030-1832355-CAP/IT/A-";
        let mut object = serde_json::Map::new();
        object.insert("same_header".to_string(), Value::String(header.to_string()));
        object.insert(
            "alert_packet".to_string(),
            json!({"audio": {"source": "cap-same-tts"}}),
        );

        let lead = (same_header_attention_samples(header, &object).unwrap()
            + PLAYLIST_POST_HEADER_SILENCE_SAMPLES)
            * 2;
        let tail = same_eom_samples() * 2;
        let body = vec![0x55; 48_000 * 2];
        let mut pcm = vec![0x11; lead];
        pcm.extend_from_slice(&body);
        pcm.extend(std::iter::repeat_n(0x22, tail));

        let stripped = easnet_alert_body_pcm(&path, &object, &pcm, SAME_SAMPLE_RATE, 1).unwrap();
        assert_eq!(stripped, body);
    }
}
