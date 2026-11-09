//! `easnet.xml`: listener, audio server, and DASDEC-style peers.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use crate::util::{expand_env_vars, fallback, parse_duration, xml_bool};

const DEFAULT_LISTEN: &str = "0.0.0.0:41800";
const DEFAULT_AUDIO_LISTEN: &str = "0.0.0.0:41801";
const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_AUDIO_BYTES: u64 = 25 * 1024 * 1024;
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_AUDIO_TTL: Duration = Duration::from_secs(15 * 60);

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
pub struct Config {
    pub base_dir: PathBuf,
    pub enabled: bool,
    pub listener: ListenerConfig,
    pub audio: AudioServerConfig,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Clone)]
pub struct ListenerConfig {
    pub enabled: bool,
    pub addr: String,
    pub max_message_bytes: usize,
    pub read_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct AudioServerConfig {
    pub enabled: bool,
    pub addr: String,
    pub public_base_url: String,
    pub token_ttl: Duration,
    pub max_audio_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub id: String,
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub protocol: PeerProtocol,
    pub user: String,
    pub user_env: String,
    pub password_env: String,
    pub remote_command: String,
    pub direction: Direction,
    pub receive_policy: ReceivePolicy,
    pub allowed_ips: Vec<IpAddr>,
    pub feeds: Vec<String>,
    pub locations: Vec<String>,
    pub severities: Vec<String>,
    pub events: Vec<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Direction {
    Send,
    Receive,
    SendReceive,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReceivePolicy {
    ArchiveOnly,
    RelayIfMatched,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PeerProtocol {
    Tcp,
    SshStdin,
}

impl Direction {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "send" | "out" | "outbound" => Self::Send,
            "receive" | "recv" | "in" | "inbound" => Self::Receive,
            _ => Self::SendReceive,
        }
    }

    pub fn can_send(self) -> bool {
        matches!(self, Self::Send | Self::SendReceive)
    }

    pub fn can_receive(self) -> bool {
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

pub fn load(base_dir: &Path, path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
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
        .filter_map(peer_config)
        .collect::<Result<Vec<_>>>()?;
    Ok(Config {
        base_dir: base_dir.to_path_buf(),
        enabled: xml_bool(&root.enabled, true),
        listener,
        audio,
        peers,
    })
}

fn peer_config(raw: PeerXml) -> Option<Result<PeerConfig>> {
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
