use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::Value;
use tracing::{debug, error, info, warn};

const CAP_REPLAY_WINDOW: Duration = Duration::from_secs(15 * 60);
const CAP_REPLAY_LIMIT: usize = 512;

struct BridgeEnvelope {
    origin: Option<usize>,
    value: Value,
}

struct BridgeClientHandle {
    id: usize,
    sender: SyncSender<Vec<u8>>,
    receive_events: bool,
    client_id: Option<String>,
    subscriptions: Vec<String>,
}

enum ClientMessage {
    Event(Value),
    Configure {
        receive_events: bool,
        client_id: Option<String>,
        subscriptions: Vec<String>,
    },
    Consumed,
}

pub(crate) struct HostBridge {
    addr: String,
    sender: Sender<Value>,
    events: Option<Receiver<Value>>,
}

impl HostBridge {
    pub(crate) fn start() -> Result<Self> {
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).context("failed to bind host event bridge")?;
        let addr = listener
            .local_addr()
            .context("failed to inspect host event bridge address")?
            .to_string();
        let (sender, receiver) = mpsc::channel::<Value>();
        let (envelope_sender, envelope_receiver) = mpsc::channel::<BridgeEnvelope>();
        let (client_sender, client_receiver) = mpsc::channel::<BridgeClientHandle>();
        let (event_sender, event_receiver) = mpsc::channel::<Value>();

        thread::spawn({
            let envelope_sender = envelope_sender.clone();
            move || {
                for value in receiver {
                    if envelope_sender
                        .send(BridgeEnvelope {
                            origin: None,
                            value,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });

        thread::spawn(move || {
            let mut clients: Vec<BridgeClientHandle> = Vec::new();
            let mut cap_replay = VecDeque::<(Instant, Vec<u8>)>::new();
            loop {
                drain_new_clients(&client_receiver, &mut clients, &mut cap_replay);
                match envelope_receiver.recv() {
                    Ok(envelope) => {
                        drain_new_clients(&client_receiver, &mut clients, &mut cap_replay);
                        match handle_client_message(envelope.value.clone()) {
                            ClientMessage::Event(message) => {
                                let _ = event_sender.send(message.clone());
                                let Ok(mut raw) = serde_json::to_vec(&message) else {
                                    continue;
                                };
                                raw.push(b'\n');
                                if replayable_event(&message) {
                                    cap_replay.push_back((Instant::now(), raw.clone()));
                                    prune_replay(&mut cap_replay);
                                    while cap_replay.len() > CAP_REPLAY_LIMIT {
                                        cap_replay.pop_front();
                                    }
                                }
                                deliver_event(&mut clients, &envelope, &message, &raw);
                            }
                            ClientMessage::Configure {
                                receive_events,
                                client_id,
                                subscriptions,
                            } => {
                                if let Some(origin) = envelope.origin {
                                    for client in &mut clients {
                                        if client.id == origin {
                                            client.receive_events = receive_events;
                                            client.client_id = client_id;
                                            client.subscriptions = subscriptions;
                                            break;
                                        }
                                    }
                                }
                            }
                            ClientMessage::Consumed => {}
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        thread::spawn({
            let addr_for_log = addr.clone();
            let accept_publisher = envelope_sender.clone();
            move || {
                info!("event bridge listening on {addr_for_log}");
                let mut next_client_id = 1usize;
                for accepted in listener.incoming() {
                    match accepted {
                        Ok(stream) => {
                            let client_id = next_client_id;
                            next_client_id = next_client_id.saturating_add(1);
                            let peer = stream
                                .peer_addr()
                                .map(|addr| addr.to_string())
                                .unwrap_or_default();
                            debug!(peer, "host bridge client connected");
                            match stream.try_clone() {
                                Ok(writer) => {
                                    let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(256);
                                    if client_sender
                                        .send(BridgeClientHandle {
                                            id: client_id,
                                            sender: tx,
                                            receive_events: true,
                                            client_id: None,
                                            subscriptions: Vec::new(),
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                    spawn_client_writer(writer, rx);
                                }
                                Err(err) => warn!("failed to clone host bridge stream: {err}"),
                            }
                            spawn_client_reader(stream, accept_publisher.clone(), client_id);
                        }
                        Err(err) => {
                            warn!("host event bridge accept failed: {err}");
                            continue;
                        }
                    }
                }
            }
        });

        Ok(Self {
            addr,
            sender,
            events: Some(event_receiver),
        })
    }

    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }

    pub(crate) fn publisher(&self) -> Sender<Value> {
        self.sender.clone()
    }

    pub(crate) fn take_events(&mut self) -> Receiver<Value> {
        self.events
            .take()
            .expect("host bridge events can only be taken once")
    }
}

fn drain_new_clients(
    client_receiver: &Receiver<BridgeClientHandle>,
    clients: &mut Vec<BridgeClientHandle>,
    cap_replay: &mut VecDeque<(Instant, Vec<u8>)>,
) {
    while let Ok(client) = client_receiver.try_recv() {
        prune_replay(cap_replay);
        if client.receive_events
            && subscription_matches(&client.subscriptions, "cap.alert.received")
        {
            for (_, raw) in cap_replay.iter() {
                let _ = client.sender.try_send(raw.clone());
            }
        }
        clients.push(client);
    }
}

fn spawn_client_reader<R>(reader: R, publisher: Sender<BridgeEnvelope>, client_id: usize)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines().map_while(std::result::Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(value) => {
                    let _ = publisher.send(BridgeEnvelope {
                        origin: Some(client_id),
                        value,
                    });
                }
                Err(err) => warn!("host bridge received invalid JSON: {err}"),
            }
        }
    });
}

fn spawn_client_writer(mut writer: TcpStream, receiver: Receiver<Vec<u8>>) {
    thread::spawn(move || {
        for raw in receiver {
            if writer.write_all(&raw).is_err() {
                let _ = writer.shutdown(Shutdown::Both);
                return;
            }
        }
        let _ = writer.shutdown(Shutdown::Both);
    });
}

fn handle_client_message(value: Value) -> ClientMessage {
    let msg_type = value.get("type").and_then(Value::as_str).unwrap_or("");
    if msg_type == "bridge.client" {
        let receive_events = value
            .get("data")
            .and_then(|data| data.get("receive_events"))
            .and_then(Value::as_bool)
            .or_else(|| value.get("receive_events").and_then(Value::as_bool))
            .unwrap_or(true);
        let client_id = value
            .get("data")
            .and_then(|data| data.get("client_id"))
            .and_then(Value::as_str)
            .or_else(|| value.get("client_id").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let subscriptions = value
            .get("data")
            .and_then(|data| data.get("subscriptions"))
            .or_else(|| value.get("subscriptions"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        return ClientMessage::Configure {
            receive_events,
            client_id,
            subscriptions,
        };
    }
    if msg_type == "log_record" {
        let level = value
            .get("level")
            .and_then(Value::as_str)
            .unwrap_or("INFO")
            .to_ascii_uppercase();
        let logger = value
            .get("logger")
            .and_then(Value::as_str)
            .unwrap_or("haze");
        let message = value.get("message").and_then(Value::as_str).unwrap_or("");
        match level.as_str() {
            "ERROR" | "CRITICAL" => error!("[{}] {message}", logger_label(logger)),
            "WARNING" | "WARN" => warn!("[{}] {message}", logger_label(logger)),
            "DEBUG" => debug!("[{}] {message}", logger_label(logger)),
            _ => info!("[{}] {message}", logger_label(logger)),
        }
        return ClientMessage::Consumed;
    }
    ClientMessage::Event(value)
}

fn deliver_event(
    clients: &mut Vec<BridgeClientHandle>,
    envelope: &BridgeEnvelope,
    message: &Value,
    raw: &[u8],
) {
    let event_type = message
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let target = message
        .get("target")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let mut delivered = false;
    clients.retain(|client| {
        if envelope.origin.is_some_and(|id| id == client.id) || !client.receive_events {
            return true;
        }
        if target.is_some_and(|target| client.client_id.as_deref() != Some(target)) {
            return true;
        }
        if target.is_none() && !subscription_matches(&client.subscriptions, event_type) {
            return true;
        }
        match client.sender.try_send(raw.to_vec()) {
            Ok(()) => {
                delivered = true;
                true
            }
            Err(TrySendError::Full(_)) => {
                warn!(client_id = ?client.client_id, "dropped slow host bridge client");
                false
            }
            Err(TrySendError::Disconnected(_)) => false,
        }
    });
    if let Some(target) = target.filter(|_| !delivered) {
        warn!(target, event_type, "host bridge target is unavailable");
    }
}

fn subscription_matches(subscriptions: &[String], event_type: &str) -> bool {
    subscriptions.is_empty()
        || subscriptions.iter().any(|subscription| {
            subscription == "*"
                || subscription == event_type
                || subscription
                    .strip_suffix('*')
                    .is_some_and(|prefix| event_type.starts_with(prefix))
        })
}

fn logger_label(logger: &str) -> &str {
    logger
        .strip_prefix("module.")
        .or_else(|| logger.strip_prefix("haze."))
        .unwrap_or(logger)
}

fn replayable_event(value: &Value) -> bool {
    matches!(
        value.get("type").and_then(Value::as_str).unwrap_or(""),
        "cap.alert.received"
    )
}

fn prune_replay(replay: &mut VecDeque<(Instant, Vec<u8>)>) {
    let now = Instant::now();
    while replay
        .front()
        .is_some_and(|(inserted_at, _)| now.duration_since(*inserted_at) > CAP_REPLAY_WINDOW)
    {
        replay.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn log_records_are_consumed_by_logger() {
        let result = handle_client_message(json!({
            "type": "log_record",
            "level": "INFO",
            "logger": "test",
            "message": "hello",
        }));

        assert!(matches!(result, ClientMessage::Consumed));
    }

    #[test]
    fn service_events_are_republished() {
        let event = json!({
            "type": "cap.alert.received",
            "source": "go-cap",
            "subject": "abc",
        });
        let result = handle_client_message(event.clone());

        match result {
            ClientMessage::Event(value) => assert_eq!(value, event),
            _ => panic!("service event was not republished"),
        }
    }

    #[test]
    fn clients_can_disable_event_receives() {
        let result = handle_client_message(json!({
            "type": "bridge.client",
            "data": {
                "receive_events": false,
            },
        }));

        assert!(matches!(
            result,
            ClientMessage::Configure {
                receive_events: false,
                ..
            }
        ));
    }

    #[test]
    fn client_identity_and_subscriptions_are_parsed() {
        let result = handle_client_message(json!({
            "type": "bridge.client",
            "data": {
                "client_id": "haze-location",
                "subscriptions": ["location.query.request"],
            },
        }));
        match result {
            ClientMessage::Configure {
                client_id,
                subscriptions,
                ..
            } => {
                assert_eq!(client_id.as_deref(), Some("haze-location"));
                assert_eq!(subscriptions, vec!["location.query.request"]);
            }
            _ => panic!("expected client configuration"),
        }
    }

    #[test]
    fn subscriptions_support_exact_and_prefix_matches() {
        assert!(subscription_matches(&[], "anything"));
        assert!(subscription_matches(
            &["location.query.request".to_string()],
            "location.query.request"
        ));
        assert!(subscription_matches(
            &["location.*".to_string()],
            "location.catalog.ready"
        ));
        assert!(!subscription_matches(
            &["cap.alert.received".to_string()],
            "location.query.request"
        ));
    }

    #[test]
    fn targeted_events_only_reach_the_named_client() {
        let (location_sender, location_receiver) = mpsc::sync_channel(1);
        let (requester_sender, requester_receiver) = mpsc::sync_channel(1);
        let mut clients = vec![
            BridgeClientHandle {
                id: 1,
                sender: location_sender,
                receive_events: true,
                client_id: Some("haze-location".to_string()),
                subscriptions: vec!["location.*".to_string()],
            },
            BridgeClientHandle {
                id: 2,
                sender: requester_sender,
                receive_events: true,
                client_id: Some("haze-ivr-1".to_string()),
                subscriptions: vec!["location.query.completed".to_string()],
            },
        ];
        let message = json!({
            "type": "location.query.completed",
            "target": "haze-ivr-1",
        });
        let envelope = BridgeEnvelope {
            origin: Some(3),
            value: message.clone(),
        };

        deliver_event(&mut clients, &envelope, &message, b"response\n");

        assert!(location_receiver.try_recv().is_err());
        assert_eq!(
            requester_receiver.try_recv().expect("targeted response"),
            b"response\n"
        );
    }
}
