//! Client registry and alert fan-out.
//!
//! Bridge events arrive here from the dispatcher in `main`. Matching and
//! queueing happen under short, non-async locks; every send is a `try_send` on
//! the client's bounded queue, so one slow client is dropped instead of
//! stalling the others. Audio is rendered and encoded off the dispatcher and
//! shared by every recipient.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use haze_cap::model::Alert;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{debug, warn};

use crate::audio::{encode, load_pcm, AudioStream, Pcm, OUTPUT_SAMPLE_RATE};
use crate::banner::BannerBoard;
use crate::bridge::{Bridge, SOURCE};
use crate::config::Settings;
use crate::facts::AlertFacts;
use crate::interest::Interest;
use crate::protocol::{
    parse_client_message, AlertMessage, AudioCaps, AudioPending, Delivery, Format, ServerMessage,
    SubscribeRequest, PROTOCOL_VERSION,
};
use crate::speech::Speech;
use crate::util::{resolve_path, value_list, value_text};

pub type ClientId = u64;

const DELIVERED_LIMIT: usize = 4096;
const CAP_CACHE_LIMIT: usize = 4096;

#[derive(Debug, Clone)]
pub enum Outbound {
    Text(Utf8Bytes),
    Audio(Arc<AudioStream>),
}

/// A received CAP alert, kept so feed airings and follow-ups can attach it.
#[derive(Debug)]
pub struct CapEntry {
    pub id: String,
    pub facts: AlertFacts,
    sent: String,
    /// Normalized CAP as JSON, without the raw XML.
    json: Value,
    raw_xml: String,
}

impl CapEntry {
    pub fn new(mut alert: Alert) -> Self {
        let raw_xml = std::mem::take(&mut alert.raw_xml);
        Self {
            id: alert.identifier.trim().to_string(),
            facts: AlertFacts::from_cap(&alert),
            sent: alert.sent.trim().to_string(),
            json: serde_json::to_value(&alert).expect("CAP alert serializes"),
            raw_xml,
        }
    }

    /// The alert as `cap.alert.received` carried it, for product render.
    pub fn alert_with_xml(&self) -> Value {
        let mut alert = self.json.clone();
        alert["raw_xml"] = json!(self.raw_xml);
        alert
    }

    /// The info language to voice: an exact or base-language match for the
    /// client's preference, else English, else the first info block.
    pub fn language_for(&self, preferred: Option<&str>) -> Option<String> {
        let languages = self
            .facts
            .infos
            .iter()
            .map(|info| {
                if info.language.is_empty() {
                    "en-US"
                } else {
                    info.language.as_str()
                }
            })
            .collect::<Vec<_>>();
        let base = |language: &str| {
            language
                .split('-')
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
        };
        preferred
            .and_then(|preferred| {
                languages
                    .iter()
                    .find(|language| language.eq_ignore_ascii_case(preferred))
                    .or_else(|| {
                        languages
                            .iter()
                            .find(|language| base(language) == base(preferred))
                    })
            })
            .or_else(|| languages.iter().find(|language| base(language) == "en"))
            .or_else(|| languages.first())
            .map(|language| language.to_string())
    }
}

#[derive(Default)]
struct CapCache {
    order: VecDeque<String>,
    entries: HashMap<String, Arc<CapEntry>>,
}

impl CapCache {
    fn insert(&mut self, entry: Arc<CapEntry>) {
        if self
            .entries
            .insert(entry.id.clone(), entry.clone())
            .is_none()
        {
            self.order.push_back(entry.id.clone());
        }
        while self.order.len() > CAP_CACHE_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

/// IDs and feed airings already sent to one client, bounded.
#[derive(Default)]
struct Delivered {
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl Delivered {
    fn contains(&self, key: &str) -> bool {
        self.seen.contains(key)
    }

    fn insert(&mut self, key: String) {
        if self.seen.insert(key.clone()) {
            self.order.push_back(key);
        }
        while self.order.len() > DELIVERED_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
    }

    /// `Some(follow_up)` when the alert should go out: a full match, or an
    /// update/cancellation of something this client already received.
    fn classify(&self, interest: &Interest, facts: &AlertFacts) -> Option<bool> {
        if interest.matches(facts) {
            return Some(false);
        }
        (facts.references.iter().any(|id| self.contains(id)) && interest.matches_message(facts))
            .then_some(true)
    }
}

struct AudioPrefs {
    language: Option<String>,
    timezone: Option<String>,
}

struct Subscription {
    interest: Option<Interest>,
    formats: Vec<Format>,
    audio: Option<AudioPrefs>,
    banner: Option<String>,
}

impl Subscription {
    fn wants(&self, format: Format) -> bool {
        self.formats.contains(&format)
    }
}

struct Client {
    tx: mpsc::Sender<Outbound>,
    subscription: Option<Subscription>,
    delivered: Delivered,
}

impl Client {
    /// False when the queue is full or closed; the caller then drops the client.
    fn push(&self, outbound: Outbound) -> bool {
        self.tx.try_send(outbound).is_ok()
    }
}

/// The shared parts of one alert message, rendered per format combination.
struct AlertBody<'a> {
    delivery: Delivery,
    alert_id: &'a str,
    feed_id: Option<&'a str>,
    status: &'a str,
    msg_type: &'a str,
    sent: &'a str,
    cap: Option<&'a CapEntry>,
    packet: Option<&'a Value>,
}

type BodyKey = (bool, bool, bool, Option<u32>);

impl AlertBody<'_> {
    fn render(&self, (json, cap, follow_up, stream): BodyKey) -> Utf8Bytes {
        ServerMessage::Alert(AlertMessage {
            delivery: self.delivery,
            alert_id: self.alert_id,
            feed_id: self.feed_id,
            status: self.status,
            msg_type: self.msg_type,
            sent: self.sent,
            follow_up,
            cap: self.cap.filter(|_| json).map(|entry| &entry.json),
            cap_xml: self.cap.filter(|_| cap).map(|entry| entry.raw_xml.as_str()),
            packet: self.packet.filter(|_| json),
            audio: stream.map(|stream| AudioPending {
                stream,
                status: "pending",
            }),
        })
        .to_text()
        .into()
    }
}

pub struct Hub {
    clients: Mutex<HashMap<ClientId, Client>>,
    caps: Mutex<CapCache>,
    banner: Mutex<BannerBoard>,
    next_client: AtomicU64,
    next_stream: AtomicU32,
    bridge: Bridge,
    speech: Speech,
    base_dir: PathBuf,
    default_timezone: String,
}

impl Hub {
    pub fn new(settings: &Settings, bridge: Bridge) -> Arc<Self> {
        Arc::new(Self {
            clients: Mutex::new(HashMap::new()),
            caps: Mutex::new(CapCache::default()),
            banner: Mutex::new(BannerBoard::new(settings.known_feed_ids())),
            next_client: AtomicU64::new(1),
            next_stream: AtomicU32::new(1),
            speech: Speech::new(
                bridge.clone(),
                settings.base_dir.join("runtime/audio/netalert"),
            ),
            bridge,
            base_dir: settings.base_dir.clone(),
            default_timezone: settings.default_timezone.clone(),
        })
    }

    pub fn client_count(&self) -> usize {
        self.lock_clients().len()
    }

    pub fn register(&self, tx: mpsc::Sender<Outbound>) -> ClientId {
        let id = self.next_client.fetch_add(1, Ordering::Relaxed);
        let hello = ServerMessage::Hello {
            protocol: PROTOCOL_VERSION,
            server: SOURCE,
            audio: AudioCaps {
                codec: "opus",
                container: "ogg",
                sample_rate: OUTPUT_SAMPLE_RATE,
            },
        };
        let client = Client {
            tx,
            subscription: None,
            delivered: Delivered::default(),
        };
        client.push(Outbound::Text(hello.to_text().into()));
        self.lock_clients().insert(id, client);
        id
    }

    pub fn unregister(&self, id: ClientId) {
        self.lock_clients().remove(&id);
    }

    pub fn handle_client_text(&self, id: ClientId, raw: &str) {
        match parse_client_message(raw).and_then(|request| self.subscribe(id, request)) {
            Ok(()) => {}
            Err(message) => self.send_error(id, "bad_request", &message),
        }
    }

    pub fn send_error(&self, id: ClientId, code: &'static str, message: &str) {
        let text = ServerMessage::Error { code, message }.to_text();
        self.send_to(&[id], Outbound::Text(text.into()));
    }

    fn subscribe(&self, id: ClientId, request: SubscribeRequest) -> Result<(), String> {
        let interest = request.interest.map(Interest::parse).transpose()?;
        let mut formats = Vec::new();
        for format in request
            .formats
            .unwrap_or_else(|| vec![Format::Json, Format::Cap])
        {
            if !formats.contains(&format) {
                formats.push(format);
            }
        }
        if formats.is_empty() {
            return Err("formats must name json, cap, or both".to_string());
        }
        let audio = request
            .audio
            .filter(|audio| audio.enabled)
            .map(|audio| AudioPrefs {
                language: audio
                    .language
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
                timezone: audio
                    .timezone
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            });
        let banner = request
            .banner
            .map(|banner| banner.feed.trim().to_string())
            .filter(|feed| !feed.is_empty());
        let subscription = Subscription {
            interest,
            formats,
            audio,
            banner,
        };
        let mut replies = vec![Outbound::Text(
            ServerMessage::Subscribed {
                interest: subscription.interest.as_ref(),
                formats: &subscription.formats,
                audio: subscription.audio.is_some(),
                banner: subscription.banner.as_deref(),
            }
            .to_text()
            .into(),
        )];
        if let Some(feed) = &subscription.banner {
            let payload = self.lock_banner().payload(feed, Utc::now()).to_value();
            replies.push(Outbound::Text(
                ServerMessage::Banner(&payload).to_text().into(),
            ));
        }
        let mut clients = self.lock_clients();
        let Some(client) = clients.get_mut(&id) else {
            return Ok(());
        };
        client.subscription = Some(subscription);
        if !replies.into_iter().all(|reply| client.push(reply)) {
            clients.remove(&id);
        }
        Ok(())
    }

    /// Routes one bridge event.
    pub async fn handle_event(self: &Arc<Self>, event: &Value) {
        match event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "cap.alert.received" => self.on_cap_received(event),
            "cap.alert.audio.ready" => self.on_feed_airing(event),
            "cap.alert.cancelled" => self.on_feed_cancelled(event),
            "alert.playout.started" | "alert.playout.completed" | "playout.interrupted" => {
                let changed = self.lock_banner().apply(event, Instant::now());
                if changed {
                    self.publish_banner_changes().await;
                }
            }
            _ => {}
        }
    }

    /// Expires stale on-air banners. Called on a timer.
    pub async fn tick(&self) {
        let changed = self.lock_banner().prune(Instant::now());
        if changed {
            self.publish_banner_changes().await;
        }
    }

    /// Publishes `banner.state.updated` and pushes `banner` messages for every
    /// feed whose payload changed (all feeds on the first call).
    pub async fn publish_banner_changes(&self) {
        let payloads = self.lock_banner().take_changed(Utc::now());
        for payload in payloads {
            let value = payload.to_value();
            let text: Utf8Bytes = ServerMessage::Banner(&value).to_text().into();
            self.send_where(Outbound::Text(text), |subscription| {
                subscription.banner.as_deref() == Some(payload.feed_id.as_str())
            });
            let event = json!({
                "type": "banner.state.updated",
                "source": SOURCE,
                "subject": payload.feed_id,
                "data": value,
            });
            if let Err(err) = self.bridge.publish(event).await {
                warn!(feed_id = %payload.feed_id, "banner state publish failed: {err:#}");
            }
        }
    }

    fn on_cap_received(self: &Arc<Self>, event: &Value) {
        let Some(alert) = event.pointer("/data/alert") else {
            return;
        };
        let alert = match serde_json::from_value::<Alert>(alert.clone()) {
            Ok(alert) if !alert.identifier.trim().is_empty() => alert,
            Ok(_) => return,
            Err(err) => {
                warn!("ignored malformed cap.alert.received: {err}");
                return;
            }
        };
        let entry = Arc::new(CapEntry::new(alert));
        self.lock_caps().insert(entry.clone());

        let body = AlertBody {
            delivery: Delivery::Cap,
            alert_id: &entry.id,
            feed_id: None,
            status: &entry.facts.status,
            msg_type: &entry.facts.msg_type,
            sent: &entry.sent,
            cap: Some(&entry),
            packet: None,
        };
        let mut texts: HashMap<BodyKey, Utf8Bytes> = HashMap::new();
        let mut audio_jobs: HashMap<(String, String), (u32, Vec<ClientId>)> = HashMap::new();
        {
            let mut clients = self.lock_clients();
            let mut dropped = Vec::new();
            for (id, client) in clients.iter_mut() {
                let Some(subscription) = &client.subscription else {
                    continue;
                };
                let Some(interest) = &subscription.interest else {
                    continue;
                };
                // Feed subscribers hear alerts when their feeds air them.
                if !interest.feeds.is_empty() || client.delivered.contains(&entry.id) {
                    continue;
                }
                let Some(follow_up) = client.delivered.classify(interest, &entry.facts) else {
                    continue;
                };
                let stream = subscription.audio.as_ref().and_then(|audio| {
                    let language = entry.language_for(audio.language.as_deref())?;
                    let timezone = audio
                        .timezone
                        .clone()
                        .unwrap_or_else(|| self.default_timezone.clone());
                    let job = audio_jobs
                        .entry((language, timezone))
                        .or_insert_with(|| (self.next_stream(), Vec::new()));
                    job.1.push(*id);
                    Some(job.0)
                });
                let key = (
                    subscription.wants(Format::Json),
                    subscription.wants(Format::Cap),
                    follow_up,
                    stream,
                );
                let text = texts.entry(key).or_insert_with(|| body.render(key)).clone();
                client.delivered.insert(entry.id.clone());
                if !client.push(Outbound::Text(text)) {
                    dropped.push(*id);
                }
            }
            drop_clients(&mut clients, dropped);
        }
        for ((language, timezone), (stream, recipients)) in audio_jobs {
            let hub = Arc::clone(self);
            let entry = entry.clone();
            tokio::spawn(async move {
                let pcm = hub.speech.render(&entry, &language, &timezone).await;
                hub.finish_audio(stream, &entry.id, None, Some(&language), pcm, &recipients)
                    .await;
            });
        }
    }

    fn on_feed_airing(self: &Arc<Self>, event: &Value) {
        let data = event.get("data").unwrap_or(&Value::Null);
        let Some(feed_id) =
            value_text(data.get("feed_id")).or_else(|| value_text(event.get("feed_id")))
        else {
            return;
        };
        let Some(alert_id) =
            value_text(data.get("alert_id")).or_else(|| value_text(event.get("subject")))
        else {
            return;
        };
        let airing = value_text(data.get("queue_id"))
            .or_else(|| value_text(data.get("manifest_id")))
            .unwrap_or_else(|| alert_id.clone());
        let packet = data
            .get("alert_packet")
            .filter(|packet| packet.is_object())
            .map(public_packet);
        let cap = self.lock_caps().entries.get(&alert_id).cloned();
        let facts = match (&cap, &packet) {
            (Some(cap), Some(packet)) => {
                let mut facts = cap.facts.clone();
                facts.add_packet_same(packet);
                facts
            }
            (Some(cap), None) => cap.facts.clone(),
            (None, Some(packet)) => AlertFacts::from_packet(packet),
            (None, None) => AlertFacts::from_packet(data),
        };
        let packet_text = |pointer: &str| {
            packet
                .as_ref()
                .and_then(|packet| value_text(packet.pointer(pointer)))
        };
        let msg_type = if facts.msg_type.is_empty() {
            packet_text("/message_type").unwrap_or_default()
        } else {
            facts.msg_type.clone()
        };
        let sent = cap
            .as_ref()
            .map(|cap| cap.sent.clone())
            .or_else(|| packet_text("/timing/sent_at"))
            .unwrap_or_default();
        let body = AlertBody {
            delivery: Delivery::Feed,
            alert_id: &alert_id,
            feed_id: Some(&feed_id),
            status: &facts.status,
            msg_type: &msg_type,
            sent: &sent,
            cap: cap.as_deref(),
            packet: packet.as_ref(),
        };

        let airing_key = format!("feed:{feed_id}:{airing}");
        let mut texts: HashMap<BodyKey, Utf8Bytes> = HashMap::new();
        let mut stream = None;
        let mut audio_recipients = Vec::new();
        {
            let mut clients = self.lock_clients();
            let mut dropped = Vec::new();
            for (id, client) in clients.iter_mut() {
                let Some(subscription) = &client.subscription else {
                    continue;
                };
                let Some(interest) = &subscription.interest else {
                    continue;
                };
                if !interest.wants_feed(&feed_id) || client.delivered.contains(&airing_key) {
                    continue;
                }
                let Some(follow_up) = client.delivered.classify(interest, &facts) else {
                    continue;
                };
                let client_stream = subscription.audio.as_ref().map(|_| {
                    audio_recipients.push(*id);
                    *stream.get_or_insert_with(|| self.next_stream())
                });
                let key = (
                    subscription.wants(Format::Json),
                    subscription.wants(Format::Cap),
                    follow_up,
                    client_stream,
                );
                let text = texts.entry(key).or_insert_with(|| body.render(key)).clone();
                client.delivered.insert(airing_key.clone());
                client.delivered.insert(alert_id.clone());
                if !client.push(Outbound::Text(text)) {
                    dropped.push(*id);
                }
            }
            drop_clients(&mut clients, dropped);
        }
        let Some(stream) = stream else { return };
        let path = value_text(data.get("audio_path"))
            .map(|path| resolve_path(&self.base_dir, path.as_ref()));
        let rate = value_text(data.get("sample_rate"))
            .and_then(|rate| rate.parse().ok())
            .unwrap_or(OUTPUT_SAMPLE_RATE);
        let channels = value_text(data.get("channels"))
            .and_then(|count| count.parse().ok())
            .unwrap_or(1);
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            let pcm = match path {
                Some(path) => tokio::task::spawn_blocking(move || load_pcm(&path, rate, channels))
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|result| result),
                None => Err(anyhow::anyhow!("airing has no rendered audio path")),
            };
            hub.finish_audio(
                stream,
                &alert_id,
                Some(&feed_id),
                None,
                pcm,
                &audio_recipients,
            )
            .await;
        });
    }

    fn on_feed_cancelled(&self, event: &Value) {
        let data = event.get("data").unwrap_or(&Value::Null);
        let Some(feed_id) =
            value_text(data.get("feed_id")).or_else(|| value_text(event.get("feed_id")))
        else {
            return;
        };
        let mut alert_ids = value_list(data.get("alert_ids"));
        if let Some(id) = value_text(data.get("alert_id")) {
            if !alert_ids.contains(&id) {
                alert_ids.push(id);
            }
        }
        if alert_ids.is_empty() {
            return;
        }
        let text: Utf8Bytes = ServerMessage::AlertCancelled {
            feed_id: &feed_id,
            alert_ids: &alert_ids,
        }
        .to_text()
        .into();
        let mut clients = self.lock_clients();
        let mut dropped = Vec::new();
        for (id, client) in clients.iter() {
            let wants = client
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.interest.as_ref())
                .is_some_and(|interest| {
                    interest.wants_feed(&feed_id)
                        && alert_ids
                            .iter()
                            .any(|alert_id| client.delivered.contains(alert_id))
                });
            if wants && !client.push(Outbound::Text(text.clone())) {
                dropped.push(*id);
            }
        }
        drop_clients(&mut clients, dropped);
    }

    async fn finish_audio(
        &self,
        stream: u32,
        alert_id: &str,
        feed_id: Option<&str>,
        language: Option<&str>,
        pcm: Result<Pcm>,
        recipients: &[ClientId],
    ) {
        let encoded = match pcm {
            Ok(pcm) => tokio::task::spawn_blocking(move || {
                encode(&pcm, stream).map(|encoded| (encoded, pcm.channels))
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result),
            Err(err) => Err(err),
        };
        let outbound = match encoded {
            Ok((encoded, channels)) => Outbound::Audio(Arc::new(AudioStream {
                start: ServerMessage::AudioStart {
                    stream,
                    alert_id,
                    feed_id,
                    language,
                    codec: "opus",
                    container: "ogg",
                    sample_rate: OUTPUT_SAMPLE_RATE,
                    channels,
                    duration_ms: encoded.duration_ms,
                }
                .to_text()
                .into(),
                pages: encoded.pages,
                end: ServerMessage::AudioEnd { stream }.to_text().into(),
            })),
            Err(err) => {
                // Clients only learn that audio failed; paths and service
                // errors stay in the log.
                warn!(
                    alert_id,
                    feed_id = feed_id.unwrap_or("-"),
                    "network alert audio failed: {err:#}"
                );
                Outbound::Text(
                    ServerMessage::AudioFailed {
                        stream,
                        alert_id,
                        error: "audio unavailable",
                    }
                    .to_text()
                    .into(),
                )
            }
        };
        debug!(
            alert_id,
            stream,
            recipients = recipients.len(),
            "network alert audio ready"
        );
        self.send_to(recipients, outbound);
    }

    fn send_to(&self, ids: &[ClientId], outbound: Outbound) {
        let mut clients = self.lock_clients();
        let mut dropped = Vec::new();
        for id in ids {
            if let Some(client) = clients.get(id) {
                if !client.push(outbound.clone()) {
                    dropped.push(*id);
                }
            }
        }
        drop_clients(&mut clients, dropped);
    }

    fn send_where(&self, outbound: Outbound, wants: impl Fn(&Subscription) -> bool) {
        let mut clients = self.lock_clients();
        let mut dropped = Vec::new();
        for (id, client) in clients.iter() {
            if client.subscription.as_ref().is_some_and(&wants) && !client.push(outbound.clone()) {
                dropped.push(*id);
            }
        }
        drop_clients(&mut clients, dropped);
    }

    fn next_stream(&self) -> u32 {
        self.next_stream.fetch_add(1, Ordering::Relaxed)
    }

    fn lock_clients(&self) -> std::sync::MutexGuard<'_, HashMap<ClientId, Client>> {
        self.clients.lock().expect("client registry poisoned")
    }

    fn lock_caps(&self) -> std::sync::MutexGuard<'_, CapCache> {
        self.caps.lock().expect("CAP cache poisoned")
    }

    fn lock_banner(&self) -> std::sync::MutexGuard<'_, BannerBoard> {
        self.banner.lock().expect("banner board poisoned")
    }
}

/// Removing a client drops its queue sender, which makes its writer close the
/// socket with "try again later".
fn drop_clients(clients: &mut HashMap<ClientId, Client>, dropped: Vec<ClientId>) {
    for id in dropped {
        if clients.remove(&id).is_some() {
            warn!(client = id, "dropped network alert client that fell behind");
        }
    }
}

/// The packet as clients see it: no local file paths or service metadata.
fn public_packet(packet: &Value) -> Value {
    let mut packet = packet.clone();
    if let Some(object) = packet.as_object_mut() {
        object.remove("meta");
        if let Some(audio) = object.get_mut("audio").and_then(Value::as_object_mut) {
            audio.remove("path");
        }
    }
    packet
}
