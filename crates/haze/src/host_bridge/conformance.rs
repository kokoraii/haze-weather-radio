//! Transport-independent conformance suite for the host event bridge.
//!
//! Each scenario below states one behavior the bridge must keep, using only the
//! small `Transport` and `Client` traits. The suite currently runs against the
//! JSON-lines transport through real loopback sockets. When a gRPC transport
//! exists (`docs/grpc-migration-plan.md`, Phase 2) it implements the same two
//! traits and `conformance_suite!` runs every scenario against it unchanged.

use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

use base64::Engine as _;
use haze_media_protocol::{
    encode_pcm_frame, pcm_frame_len_from_prefix, PcmFrame, PcmMediaKind, PCM_FRAME_MAGIC,
    PCM_FRAME_PREFIX_LEN,
};
use serde_json::{json, Value};

use super::HostBridge;

const SYNC_EVENT: &str = "conformance.sync";
const QUIET: Duration = Duration::from_millis(250);
const PATIENCE: Duration = Duration::from_secs(5);

/// What a client asks the broker for when it connects.
#[derive(Clone)]
struct Join {
    client_id: Option<&'static str>,
    receive_events: bool,
    subscriptions: &'static [&'static str],
    binary_pcm: bool,
    /// False connects as a legacy client that never sends `bridge.client`.
    register: bool,
}

impl Join {
    fn subscriber(client_id: &'static str, subscriptions: &'static [&'static str]) -> Self {
        Self {
            client_id: Some(client_id),
            receive_events: true,
            subscriptions,
            binary_pcm: false,
            register: true,
        }
    }

    fn anonymous(subscriptions: &'static [&'static str]) -> Self {
        Self {
            client_id: None,
            ..Self::subscriber("", subscriptions)
        }
    }

    fn publisher() -> Self {
        Self {
            client_id: None,
            receive_events: false,
            subscriptions: &[],
            binary_pcm: false,
            register: true,
        }
    }

    fn legacy() -> Self {
        Self {
            register: false,
            ..Self::publisher()
        }
    }

    fn binary_pcm(subscriptions: &'static [&'static str]) -> Self {
        Self {
            binary_pcm: true,
            ..Self::anonymous(subscriptions)
        }
    }
}

enum Delivery {
    Event(Value),
    Pcm(Vec<u8>),
}

trait Client {
    fn publish(&mut self, event: Value);
    fn publish_pcm(&mut self, frame: &[u8]);
    /// The next delivery, or `None` after `wait` without one or once the broker
    /// has closed the connection. Harness-internal sync events are never returned.
    fn recv(&mut self, wait: Duration) -> Option<Delivery>;
    /// Reads until the broker closes the connection or `limit` passes, and
    /// returns how many deliveries arrived and whether the broker hung up.
    fn drain_until_closed(&mut self, limit: Duration) -> (usize, bool);
    /// Ends the client's outbound stream, as closing a connection does, while
    /// leaving its receive side open so the broker's hang-up can be observed.
    fn close_send(&mut self);
}

trait Transport {
    /// Connects a client and returns only once the broker has applied its
    /// registration, so a following publish is routed to it.
    fn connect(&self, join: Join) -> Box<dyn Client>;
    /// Returns once the broker has routed everything `client` published so far.
    fn settle(&self, client: &mut dyn Client);
    /// An event the daemon itself consumes, such as `service.control`.
    fn host_event(&self, wait: Duration) -> Option<Value>;
    /// Publishes as the daemon host rather than as a client connection.
    fn host_publish(&self, event: Value);
}

fn events(client: &mut dyn Client, wait: Duration) -> Vec<Value> {
    let mut out = Vec::new();
    while let Some(delivery) = client.recv(wait) {
        if let Delivery::Event(value) = delivery {
            out.push(value);
        }
    }
    out
}

fn types(client: &mut dyn Client) -> Vec<String> {
    events(client, QUIET)
        .iter()
        .map(|event| event["type"].as_str().unwrap_or_default().to_string())
        .collect()
}

fn event(event_type: &str) -> Value {
    json!({ "type": event_type, "source": "conformance" })
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

mod scenarios {
    use super::*;

    /// Every member of an event, including ones the envelope does not model,
    /// reaches subscribers unchanged.
    pub(super) fn events_pass_through_unchanged(t: &dyn Transport) {
        let mut subscriber = t.connect(Join::subscriber("sub", &["cap.alert.received"]));
        let mut publisher = t.connect(Join::publisher());
        let sent = json!({
            "id": "cap-1",
            "type": "cap.alert.received",
            "source": "naads",
            "timestamp": "2026-10-25T12:00:00.123456789Z",
            "subject": "urn:parent",
            "reply_to": "somebody",
            "delivery_id": "cap-1",
            "feed_id": "feed-a",
            "data": { "sequence": 9007199254740993u64, "nested": { "list": [1, 2, { "k": null }] } },
        });
        publisher.publish(sent.clone());
        match subscriber.recv(PATIENCE) {
            Some(Delivery::Event(received)) => assert_eq!(received, sent),
            _ => panic!("subscriber did not receive the published event"),
        }
    }

    /// Exact types, `*`, `prefix*`, and an empty list select the right events.
    pub(super) fn subscriptions_match_exact_star_prefix_and_empty(t: &dyn Transport) {
        let mut exact = t.connect(Join::anonymous(&["a.b"]));
        let mut star = t.connect(Join::anonymous(&["*"]));
        let mut prefix = t.connect(Join::anonymous(&["a.*"]));
        let mut empty = t.connect(Join::anonymous(&[]));
        let mut other = t.connect(Join::anonymous(&["c.d"]));
        let mut publisher = t.connect(Join::publisher());
        for event_type in ["a.b", "a.c", "x.y"] {
            publisher.publish(event(event_type));
        }
        assert_eq!(types(&mut *exact), ["a.b"]);
        assert_eq!(types(&mut *star), ["a.b", "a.c", "x.y"]);
        assert_eq!(types(&mut *prefix), ["a.b", "a.c"]);
        assert_eq!(types(&mut *empty), ["a.b", "a.c", "x.y"]);
        assert!(types(&mut *other).is_empty());
    }

    /// A targeted event goes to the named client regardless of its
    /// subscriptions, and to nobody else.
    pub(super) fn targeted_delivery_reaches_only_the_named_client(t: &dyn Transport) {
        let mut named = t.connect(Join::subscriber("target-1", &["unrelated.*"]));
        let mut bystander = t.connect(Join::subscriber("bystander", &["*"]));
        let mut publisher = t.connect(Join::publisher());
        publisher.publish(json!({ "type": "x.y", "target": "target-1" }));
        assert_eq!(types(&mut *named), ["x.y"]);
        assert!(types(&mut *bystander).is_empty());
    }

    /// A publisher never receives its own event, even when it subscribes to it.
    pub(super) fn events_are_not_echoed_to_their_publisher(t: &dyn Transport) {
        let mut origin = t.connect(Join::subscriber("origin", &["*"]));
        let mut peer = t.connect(Join::subscriber("peer", &["*"]));
        origin.publish(event("echo.check"));
        assert_eq!(types(&mut *peer), ["echo.check"]);
        assert!(types(&mut *origin).is_empty());
    }

    /// `receive_events: false` clients are sent nothing.
    pub(super) fn publish_only_clients_receive_nothing(t: &dyn Transport) {
        let mut quiet = t.connect(Join::publisher());
        let mut sender = t.connect(Join::publisher());
        sender.publish(event("anything"));
        assert!(types(&mut *quiet).is_empty());
    }

    /// A client that never registers is activated by its first publish and, at
    /// the latest, after the handshake grace period.
    pub(super) fn unregistered_clients_activate_as_legacy_clients(t: &dyn Transport) {
        let mut publisher = t.connect(Join::publisher());

        let mut after_grace = t.connect(Join::legacy());
        std::thread::sleep(Duration::from_millis(900));
        publisher.publish(event("legacy.after_grace"));
        assert_eq!(types(&mut *after_grace), ["legacy.after_grace"]);

        let mut after_publish = t.connect(Join::legacy());
        after_publish.publish(event("legacy.hello"));
        std::thread::sleep(Duration::from_millis(50));
        publisher.publish(event("legacy.after_publish"));
        assert!(types(&mut *after_publish).contains(&"legacy.after_publish".to_string()));
    }

    /// A client that stops reading is dropped. It must not stall the router or
    /// the other clients.
    pub(super) fn slow_clients_are_dropped_without_blocking_the_router(t: &dyn Transport) {
        const EVENTS: usize = 2_000;
        let mut slow = t.connect(Join::subscriber("slow", &["flood.*"]));
        let mut publisher = t.connect(Join::publisher());
        let padding = "x".repeat(32 * 1024);
        for index in 0..EVENTS {
            publisher.publish(json!({
                "type": "flood.item",
                "source": "conformance",
                "data": { "index": index, "padding": padding },
            }));
        }

        // The router is still serving other clients.
        t.settle(&mut *publisher);
        let mut probe = t.connect(Join::anonymous(&["probe.*"]));
        publisher.publish(event("probe.alive"));
        assert_eq!(types(&mut *probe), ["probe.alive"]);

        // The slow client was disconnected instead of waited for: reading it to
        // the end terminates, and it saw only part of the flood.
        let (received, closed) = slow.drain_until_closed(Duration::from_secs(20));
        assert!(closed, "the broker kept a non-reading client connected");
        assert!(
            received < EVENTS,
            "the slow client received all {EVENTS} events, so nothing was dropped"
        );
    }

    /// Recent `cap.alert.received` events are replayed, in order, to a client
    /// that registers later with a matching subscription.
    pub(super) fn cap_alerts_replay_to_late_subscribers(t: &dyn Transport) {
        let mut publisher = t.connect(Join::publisher());
        for index in 0..3 {
            publisher.publish(json!({
                "type": "cap.alert.received", "source": "naads", "subject": format!("alert-{index}"),
            }));
        }
        publisher.publish(event("not.replayed"));
        t.settle(&mut *publisher);

        let mut late = t.connect(Join::subscriber("late", &["cap.alert.received"]));
        let replayed: Vec<String> = events(&mut *late, QUIET)
            .iter()
            .map(|value| value["subject"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(replayed, ["alert-0", "alert-1", "alert-2"]);

        let mut unrelated = t.connect(Join::subscriber("unrelated", &["weather.*"]));
        assert!(types(&mut *unrelated).is_empty());

        let mut everything = t.connect(Join::anonymous(&[]));
        let all = types(&mut *everything);
        assert_eq!(
            all.iter()
                .filter(|name| *name == "cap.alert.received")
                .count(),
            3
        );
    }

    /// `log_record` messages are consumed by the daemon and never routed.
    pub(super) fn log_records_are_consumed_not_republished(t: &dyn Transport) {
        let mut subscriber = t.connect(Join::subscriber("sub", &["*"]));
        let mut service = t.connect(Join::publisher());
        service.publish(json!({
            "type": "log_record", "level": "INFO", "logger": "conformance", "message": "hello",
        }));
        service.publish(event("after.log"));
        assert_eq!(types(&mut *subscriber), ["after.log"]);
    }

    /// The daemon consumes only service control and automation acknowledgements,
    /// and those still reach subscribers.
    pub(super) fn host_consumes_only_control_events(t: &dyn Transport) {
        let mut subscriber = t.connect(Join::subscriber("sub", &["*"]));
        let mut publisher = t.connect(Join::publisher());
        publisher.publish(event("cap.alert.received"));
        publisher.publish(json!({ "type": "service.control", "data": { "action": "restart" } }));
        publisher.publish(event("automation.dispatch.completed"));

        let first = t
            .host_event(PATIENCE)
            .expect("service.control reached the host");
        let second = t
            .host_event(PATIENCE)
            .expect("automation.dispatch.completed reached the host");
        assert_eq!(first["type"], "service.control");
        assert_eq!(second["type"], "automation.dispatch.completed");
        assert!(
            t.host_event(QUIET).is_none(),
            "the host consumed an event it should not"
        );
        assert_eq!(
            types(&mut *subscriber),
            [
                "cap.alert.received",
                "service.control",
                "automation.dispatch.completed"
            ]
        );
    }

    /// A client that closes is released at once, including a publish-only one
    /// the broker never writes to, and what it published first is still routed.
    pub(super) fn closed_clients_are_released_after_their_last_publish(t: &dyn Transport) {
        let mut subscriber = t.connect(Join::subscriber("sub", &["*"]));
        let mut publisher = t.connect(Join::publisher());
        publisher.publish(event("cap.alert.received"));
        publisher.close_send();

        let (received, closed) = publisher.drain_until_closed(PATIENCE);
        assert_eq!(received, 0, "a publish-only client received a delivery");
        assert!(closed, "the broker kept a closed client's connection open");
        assert_eq!(types(&mut *subscriber), ["cap.alert.received"]);
    }

    /// Events the daemon publishes itself reach subscribers.
    pub(super) fn host_published_events_reach_subscribers(t: &dyn Transport) {
        let mut subscriber = t.connect(Join::subscriber("sub", &["system.*"]));
        t.host_publish(event("system.shutdown"));
        assert_eq!(types(&mut *subscriber), ["system.shutdown"]);
    }

    /// PCM goes to opted-in clients as the compact frame and to everyone else
    /// as the legacy base64 JSON event.
    pub(super) fn pcm_frames_follow_the_binary_opt_in(t: &dyn Transport) {
        let mut binary = t.connect(Join::binary_pcm(&["playout.pcm"]));
        let mut legacy = t.connect(Join::anonymous(&["playout.pcm"]));
        let mut publisher = t.connect(Join::publisher());

        let samples = [1u8, 0, 2, 0, 3, 0, 4, 0];
        let mut frame = Vec::new();
        encode_pcm_frame(
            &mut frame,
            PcmFrame {
                feed_id: "feed-a",
                queue_id: Some("queue-1"),
                sample_rate: 48_000,
                channels: 1,
                duration_ms: 20,
                sequence: 7,
                pts_ns: 140_000_000,
                discontinuity: false,
                media_kind: PcmMediaKind::Alert,
                pcm: &samples,
            },
        )
        .expect("test PCM frame encodes");
        publisher.publish_pcm(&frame);

        match binary.recv(PATIENCE) {
            Some(Delivery::Pcm(received)) => assert_eq!(received, frame),
            _ => panic!("the binary client did not receive the compact frame"),
        }
        match legacy.recv(PATIENCE) {
            Some(Delivery::Event(received)) => {
                assert_eq!(received["type"], "playout.pcm");
                assert_eq!(received["data"]["feed_id"], "feed-a");
                assert_eq!(received["data"]["media_kind"], "alert");
                assert_eq!(received["data"]["sequence"], 7);
                assert_eq!(
                    received["data"]["pcm"],
                    base64::engine::general_purpose::STANDARD.encode(samples)
                );
            }
            _ => panic!("the legacy client did not receive a JSON PCM event"),
        }
    }
}

macro_rules! conformance_suite {
    ($name:ident, $transport:expr) => {
        mod $name {
            use super::*;

            conformance_suite!(@scenarios $transport;
                events_pass_through_unchanged,
                subscriptions_match_exact_star_prefix_and_empty,
                targeted_delivery_reaches_only_the_named_client,
                events_are_not_echoed_to_their_publisher,
                publish_only_clients_receive_nothing,
                unregistered_clients_activate_as_legacy_clients,
                slow_clients_are_dropped_without_blocking_the_router,
                closed_clients_are_released_after_their_last_publish,
                cap_alerts_replay_to_late_subscribers,
                log_records_are_consumed_not_republished,
                host_consumes_only_control_events,
                host_published_events_reach_subscribers,
                pcm_frames_follow_the_binary_opt_in,
            );
        }
    };
    (@scenarios $transport:expr; $($scenario:ident,)+) => {
        $(
            #[test]
            fn $scenario() {
                let transport = $transport;
                scenarios::$scenario(&transport);
            }
        )+
    };
}

conformance_suite!(json_lines, JsonLines::start());

// ---------------------------------------------------------------------------
// JSON-lines transport
// ---------------------------------------------------------------------------

/// A raw socket speaking the newline-delimited JSON and `HPCM` wire format.
struct Wire {
    stream: TcpStream,
    buffer: Vec<u8>,
    closed: bool,
}

impl Wire {
    fn open(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).expect("connect to the host bridge");
        stream.set_nodelay(true).ok();
        Self {
            stream,
            buffer: Vec::new(),
            closed: false,
        }
    }

    fn write_all(&mut self, bytes: &[u8]) {
        self.stream
            .write_all(bytes)
            .expect("write to the host bridge");
    }

    fn write_event(&mut self, event: &Value) {
        let mut raw = serde_json::to_vec(event).expect("event serializes");
        raw.push(b'\n');
        self.write_all(&raw);
    }

    fn take_frame(&mut self) -> Option<Delivery> {
        loop {
            let skip = self
                .buffer
                .iter()
                .take_while(|byte| byte.is_ascii_whitespace())
                .count();
            self.buffer.drain(..skip);
            if self.buffer.is_empty() {
                return None;
            }
            if self.buffer[0] == PCM_FRAME_MAGIC[0] {
                if self.buffer.len() < PCM_FRAME_PREFIX_LEN {
                    return None;
                }
                let length = pcm_frame_len_from_prefix(&self.buffer[..PCM_FRAME_PREFIX_LEN])
                    .expect("valid PCM frame prefix");
                if self.buffer.len() < length {
                    return None;
                }
                return Some(Delivery::Pcm(self.buffer.drain(..length).collect()));
            }
            let newline = self.buffer.iter().position(|byte| *byte == b'\n')?;
            let line: Vec<u8> = self.buffer.drain(..=newline).collect();
            if let Ok(value) = serde_json::from_slice::<Value>(&line) {
                return Some(Delivery::Event(value));
            }
        }
    }

    /// The next delivery, including sync events.
    fn next(&mut self, wait: Duration) -> Option<Delivery> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(delivery) = self.take_frame() {
                return Some(delivery);
            }
            if self.closed {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            self.stream.set_read_timeout(Some(remaining)).ok();
            let mut chunk = [0u8; 64 * 1024];
            match self.stream.read(&mut chunk) {
                Ok(0) => self.closed = true,
                Ok(read) => self.buffer.extend_from_slice(&chunk[..read]),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return None
                }
                Err(_) => self.closed = true,
            }
        }
    }
}

struct JsonLinesClient {
    wire: Wire,
}

impl Client for JsonLinesClient {
    fn publish(&mut self, event: Value) {
        self.wire.write_event(&event);
    }

    fn publish_pcm(&mut self, frame: &[u8]) {
        self.wire.write_all(frame);
    }

    fn recv(&mut self, wait: Duration) -> Option<Delivery> {
        let deadline = Instant::now() + wait;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.wire.next(remaining)? {
                Delivery::Event(value) if value["type"] == SYNC_EVENT => continue,
                other => return Some(other),
            }
        }
    }

    fn drain_until_closed(&mut self, limit: Duration) -> (usize, bool) {
        let deadline = Instant::now() + limit;
        let mut received = 0;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return (received, false);
            }
            match self.wire.next(remaining) {
                Some(Delivery::Event(value)) if value["type"] == SYNC_EVENT => {}
                Some(_) => received += 1,
                None if self.wire.closed => return (received, true),
                None => return (received, false),
            }
        }
    }

    fn close_send(&mut self) {
        self.wire
            .stream
            .shutdown(Shutdown::Write)
            .expect("close the client's send side");
    }
}

struct JsonLines {
    // Held so the broker's channels stay open for the life of the test.
    _bridge: HostBridge,
    addr: String,
    host_sender: Sender<Value>,
    host_events: Receiver<Value>,
    observer: RefCell<Wire>,
    next_token: RefCell<u64>,
}

impl JsonLines {
    fn start() -> Self {
        let mut bridge = HostBridge::start().expect("start the host bridge");
        let addr = bridge.addr().to_string();
        let host_sender = bridge.publisher();
        let host_events = bridge.take_events();

        let mut observer = Wire::open(&addr);
        observer.write_event(&registration(&Join::subscriber("observer", &[SYNC_EVENT])));
        // Registration has no acknowledgement, so ping from the host until the
        // observer proves it is registered.
        let deadline = Instant::now() + PATIENCE;
        loop {
            assert!(Instant::now() < deadline, "the observer never registered");
            host_sender
                .send(json!({ "type": SYNC_EVENT, "source": "bootstrap" }))
                .expect("host sender is open");
            if observer.next(Duration::from_millis(50)).is_some() {
                break;
            }
        }
        Self {
            _bridge: bridge,
            addr,
            host_sender,
            host_events,
            observer: RefCell::new(observer),
            next_token: RefCell::new(0),
        }
    }

    fn token(&self) -> String {
        let mut next = self.next_token.borrow_mut();
        *next += 1;
        format!("sync-{next}")
    }

    /// Messages on one connection are handled in order, so once the observer sees
    /// a sync event a client sent, everything that client sent before it, such as
    /// its registration or earlier events, has been routed.
    fn await_sync(&self, token: &str) {
        let deadline = Instant::now() + PATIENCE;
        let mut observer = self.observer.borrow_mut();
        while Instant::now() < deadline {
            if let Some(Delivery::Event(value)) = observer.next(Duration::from_millis(100)) {
                if value["source"] == token {
                    return;
                }
            }
        }
        panic!("the broker never routed sync event {token}");
    }
}

impl Transport for JsonLines {
    fn connect(&self, join: Join) -> Box<dyn Client> {
        let mut wire = Wire::open(&self.addr);
        if join.register {
            wire.write_event(&registration(&join));
            let token = self.token();
            wire.write_event(&json!({ "type": SYNC_EVENT, "source": token }));
            self.await_sync(&token);
        }
        Box::new(JsonLinesClient { wire })
    }

    fn settle(&self, client: &mut dyn Client) {
        let token = self.token();
        client.publish(json!({ "type": SYNC_EVENT, "source": token }));
        self.await_sync(&token);
    }

    fn host_event(&self, wait: Duration) -> Option<Value> {
        self.host_events.recv_timeout(wait).ok()
    }

    fn host_publish(&self, event: Value) {
        self.host_sender.send(event).expect("host sender is open");
    }
}

fn registration(join: &Join) -> Value {
    let mut data = json!({
        "receive_events": join.receive_events,
        "subscriptions": join.subscriptions,
    });
    if let Some(client_id) = join.client_id {
        data["client_id"] = json!(client_id);
    }
    if join.binary_pcm {
        data["binary_pcm"] = json!(true);
    }
    json!({ "type": "bridge.client", "data": data })
}
