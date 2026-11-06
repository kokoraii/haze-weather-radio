//! Managed services that run on another machine.
//!
//! A service whose configured `host` is not an address of this machine is not
//! launched here. The daemon dials the remote instance at `host:port`, sends
//! `link.hello` with the shared `HAZE_REMOTE_TOKEN` and the settings a local
//! launch would have passed, waits for `link.ready`, and then relays the
//! connection into the host bridge as one more bridge client. A
//! [`LinkAdapter`] carries what the shared disk would otherwise carry, such as
//! TTS audio files. See docs/remote-services.md.

use std::env;
use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Shutdown, TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

pub(crate) const REMOTE_TOKEN_ENV: &str = "HAZE_REMOTE_TOKEN";
const BRIDGE_ADDR_ENV: &str = "HAZE_HOST_BRIDGE_ADDR";
const LINK_PROTOCOL: u64 = 1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(30);
const STOP_POLL: Duration = Duration::from_millis(100);
/// Largest line accepted from a remote service. Inline audio for a long alert
/// is tens of megabytes of base64.
const MAX_REMOTE_LINE: usize = 64 * 1024 * 1024;
const MAX_BRIDGE_LINE: usize = 16 * 1024 * 1024;

/// True when `host` names this machine: empty, `localhost`, a loopback or
/// unspecified address, or a name or address assigned to a local interface.
pub(crate) fn is_local_host(host: &str) -> bool {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() || host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<IpAddr>() {
        Ok(ip) => is_local_ip(ip),
        Err(_) => (host, 0)
            .to_socket_addrs()
            .map(|mut addrs| addrs.any(|addr| is_local_ip(addr.ip())))
            .unwrap_or(false),
    }
}

fn is_local_ip(ip: IpAddr) -> bool {
    // Binding only succeeds for an address one of this machine's interfaces owns.
    ip.is_loopback() || ip.is_unspecified() || UdpSocket::bind((ip, 0)).is_ok()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemoteEndpoint {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for RemoteEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Service-specific work a link does on top of relaying bridge lines.
pub(crate) trait LinkAdapter: Send + Sync {
    /// Settings sent in `link.hello`. Read on every connect, so edits apply
    /// after the service is restarted.
    fn settings(&self) -> Result<Value>;
    /// Prepares a bridge event before it goes to the remote service.
    fn outbound(&self, event: &mut Value);
    /// Prepares a remote event before it goes to the bridge.
    fn inbound(&self, event: Value) -> Value;
    /// Forgets per-connection state once a connection ends.
    fn reset(&self);
}

/// Connection state the supervisor reports in the managed service status.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LinkStatus {
    pub connected: bool,
    pub last_error: Option<String>,
    pub connects: u64,
    /// Bumped on every change, so status is only rewritten when it moved.
    pub revision: u64,
}

struct Shared {
    stop: AtomicBool,
    status: Mutex<LinkStatus>,
    /// Clones of the live sockets, shut down to interrupt blocking I/O on stop.
    sockets: Mutex<Vec<TcpStream>>,
}

impl Shared {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    fn update(&self, change: impl FnOnce(&mut LinkStatus)) {
        let mut status = self.status.lock().expect("link status lock");
        change(&mut status);
        status.revision += 1;
    }

    fn track(&self, socket: &TcpStream) -> io::Result<()> {
        let clone = socket.try_clone()?;
        self.sockets.lock().expect("link sockets lock").push(clone);
        Ok(())
    }

    fn close_sockets(&self) {
        for socket in self.sockets.lock().expect("link sockets lock").drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

/// What a link needs from the daemon environment.
pub(crate) struct LinkEnv {
    /// The shared secret both ends read from `HAZE_REMOTE_TOKEN`.
    pub token: Option<String>,
    /// The local host bridge the link joins.
    pub bridge_addr: Option<String>,
}

impl LinkEnv {
    pub(crate) fn from_process() -> Self {
        let read = |key: &str| {
            env::var(key)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Self {
            token: read(REMOTE_TOKEN_ENV),
            bridge_addr: read(BRIDGE_ADDR_ENV),
        }
    }
}

/// A running link to one remote service. Dropping it disconnects. The worker
/// thread is not joined, so a connect in progress never stalls the
/// supervisor; it notices the stop and exits on its own.
pub(crate) struct RemoteLink {
    shared: Arc<Shared>,
}

impl RemoteLink {
    /// Starts dialing `endpoint` in the background, reconnecting with backoff.
    /// `adapter` must be fresh for this link, since its state is per link.
    pub(crate) fn start(
        label: &'static str,
        endpoint: RemoteEndpoint,
        adapter: Arc<dyn LinkAdapter>,
        link_env: LinkEnv,
    ) -> Self {
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            status: Mutex::new(LinkStatus::default()),
            sockets: Mutex::new(Vec::new()),
        });
        thread::spawn({
            let shared = Arc::clone(&shared);
            move || run(label, &endpoint, adapter.as_ref(), &link_env, &shared)
        });
        Self { shared }
    }

    pub(crate) fn status(&self) -> LinkStatus {
        self.shared.status.lock().expect("link status lock").clone()
    }
}

impl Drop for RemoteLink {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        self.shared.close_sockets();
    }
}

fn run(
    label: &str,
    endpoint: &RemoteEndpoint,
    adapter: &dyn LinkAdapter,
    link_env: &LinkEnv,
    shared: &Shared,
) {
    let mut failures = 0u32;
    while !shared.stopped() {
        let result = connect(endpoint, adapter, link_env, shared).and_then(|session| {
            failures = 0;
            info!("[{label}] connected to remote {endpoint}");
            shared.update(|status| {
                status.connected = true;
                status.last_error = None;
                status.connects += 1;
            });
            let result = relay(session, adapter);
            adapter.reset();
            result
        });
        shared.close_sockets();
        if shared.stopped() {
            break;
        }
        let error = match result {
            Ok(()) => "remote closed the link".to_string(),
            Err(err) => format!("{err:#}"),
        };
        failures = failures.saturating_add(1);
        let (was_connected, repeated) = {
            let status = shared.status.lock().expect("link status lock");
            (
                status.connected,
                status.last_error.as_deref() == Some(error.as_str()),
            )
        };
        // A remote that stays down logs once, not on every retry.
        if was_connected || !repeated {
            warn!("[{label}] remote {endpoint}: {error}");
        }
        shared.update(|status| {
            status.connected = false;
            status.last_error = Some(error);
        });
        sleep_unless_stopped(shared, retry_delay(failures));
    }
    shared.update(|status| status.connected = false);
}

fn retry_delay(failures: u32) -> Duration {
    MIN_RETRY
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1)))
        .min(MAX_RETRY)
}

fn sleep_unless_stopped(shared: &Shared, delay: Duration) {
    let mut remaining = delay;
    while !shared.stopped() && !remaining.is_zero() {
        let step = remaining.min(STOP_POLL);
        thread::sleep(step);
        remaining -= step;
    }
}

struct Session {
    remote: TcpStream,
    remote_reader: BufReader<TcpStream>,
    bridge: TcpStream,
}

/// Dials the remote service, authenticates, and joins the host bridge.
fn connect(
    endpoint: &RemoteEndpoint,
    adapter: &dyn LinkAdapter,
    link_env: &LinkEnv,
    shared: &Shared,
) -> Result<Session> {
    let token = link_env
        .token
        .as_deref()
        .ok_or_else(|| anyhow!("{REMOTE_TOKEN_ENV} is not set in .env"))?;
    let bridge_addr = link_env
        .bridge_addr
        .as_deref()
        .ok_or_else(|| anyhow!("{BRIDGE_ADDR_ENV} is not set"))?;
    let settings = adapter.settings()?;

    let addr = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .with_context(|| format!("cannot resolve {endpoint}"))?
        .next()
        .ok_or_else(|| anyhow!("{endpoint} has no address"))?;
    let mut remote = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
        .with_context(|| format!("cannot connect to {endpoint}"))?;
    shared.track(&remote)?;
    if shared.stopped() {
        bail!("link stopped");
    }
    let _ = remote.set_nodelay(true);
    write_line(
        &mut remote,
        &json!({
            "type": "link.hello",
            "data": {"protocol": LINK_PROTOCOL, "token": token, "settings": settings},
        }),
    )?;

    remote.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    let mut remote_reader = BufReader::new(remote.try_clone()?);
    let reply = read_json_line(&mut remote_reader, MAX_REMOTE_LINE)
        .context("no link.ready from the remote service")?
        .ok_or_else(|| anyhow!("remote closed the link during the handshake"))?;
    match reply.get("type").and_then(Value::as_str) {
        Some("link.ready") => {}
        Some("link.refused") => {
            let reason = reply
                .pointer("/data/error")
                .and_then(Value::as_str)
                .unwrap_or("no reason given");
            bail!("remote refused the link: {reason}");
        }
        other => bail!(
            "expected link.ready, got {}",
            other.unwrap_or("an untyped message")
        ),
    }
    remote.set_read_timeout(None)?;

    let bridge = TcpStream::connect(bridge_addr).context("cannot join the host bridge")?;
    shared.track(&bridge)?;
    Ok(Session {
        remote,
        remote_reader,
        bridge,
    })
}

/// Relays lines both ways until either side closes.
fn relay(session: Session, adapter: &dyn LinkAdapter) -> Result<()> {
    let Session {
        remote,
        mut remote_reader,
        bridge,
    } = session;
    thread::scope(|scope| {
        let to_remote = scope.spawn(|| -> Result<()> {
            let mut reader = BufReader::new(bridge.try_clone()?);
            let mut writer = remote.try_clone()?;
            let result = pump(&mut reader, &mut writer, MAX_BRIDGE_LINE, |mut event| {
                adapter.outbound(&mut event);
                event
            });
            let _ = remote.shutdown(Shutdown::Both);
            result.context("bridge to remote")
        });
        let mut writer = bridge.try_clone()?;
        let result = pump(&mut remote_reader, &mut writer, MAX_REMOTE_LINE, |event| {
            adapter.inbound(event)
        });
        let _ = bridge.shutdown(Shutdown::Both);
        let _ = remote.shutdown(Shutdown::Both);
        let forwarded = to_remote.join().expect("link relay thread panicked");
        result.context("remote to bridge")?;
        forwarded
    })
}

/// Copies JSON lines from `reader` to `writer` through `transform`.
fn pump<R: Read>(
    reader: &mut BufReader<R>,
    writer: &mut TcpStream,
    limit: usize,
    mut transform: impl FnMut(Value) -> Value,
) -> Result<()> {
    while let Some(event) = read_json_line(reader, limit)? {
        write_line(writer, &transform(event))?;
    }
    Ok(())
}

/// Reads the next JSON line, skipping blank and unparseable lines. `None` is a
/// clean end of stream.
fn read_json_line<R: Read>(reader: &mut BufReader<R>, limit: usize) -> Result<Option<Value>> {
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader
            .by_ref()
            .take(limit as u64 + 1)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            return Ok(None);
        }
        if line.len() > limit {
            bail!("line exceeds {limit} bytes");
        }
        let text = line.trim_ascii();
        if text.is_empty() {
            continue;
        }
        match serde_json::from_slice(text) {
            Ok(value) => return Ok(Some(value)),
            Err(err) => debug!("skipping a non-JSON link line: {err}"),
        }
    }
}

fn write_line(writer: &mut TcpStream, value: &Value) -> Result<()> {
    let mut raw = serde_json::to_vec(value)?;
    raw.push(b'\n');
    writer.write_all(&raw)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::time::Instant;

    /// Marks events in both directions so the test can see the adapter ran.
    struct MarkingAdapter;

    impl LinkAdapter for MarkingAdapter {
        fn settings(&self) -> Result<Value> {
            Ok(json!({"readers_xml": "<Readers/>"}))
        }

        fn outbound(&self, event: &mut Value) {
            event["to_remote"] = json!(true);
        }

        fn inbound(&self, mut event: Value) -> Value {
            event["from_remote"] = json!(true);
            event
        }

        fn reset(&self) {}
    }

    fn read_value(reader: &mut BufReader<TcpStream>) -> Value {
        read_json_line(reader, MAX_REMOTE_LINE)
            .expect("read line")
            .expect("line before close")
    }

    fn write_value(stream: &mut TcpStream, value: Value) {
        write_line(stream, &value).expect("write line");
    }

    fn start_link(remote: &TcpListener, bridge: &TcpListener, token: &str) -> RemoteLink {
        let endpoint = RemoteEndpoint {
            host: "127.0.0.1".to_string(),
            port: remote.local_addr().unwrap().port(),
        };
        RemoteLink::start(
            "test",
            endpoint,
            Arc::new(MarkingAdapter),
            LinkEnv {
                token: Some(token.to_string()),
                bridge_addr: Some(bridge.local_addr().unwrap().to_string()),
            },
        )
    }

    fn wait_for(link: &RemoteLink, done: impl Fn(&LinkStatus) -> bool) -> LinkStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = link.status();
            if done(&status) {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "link status never settled: {status:?}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn relays_both_ways_after_the_handshake() {
        let remote = TcpListener::bind("127.0.0.1:0").unwrap();
        let bridge = TcpListener::bind("127.0.0.1:0").unwrap();
        let link = start_link(&remote, &bridge, "secret");

        let (mut remote_conn, _) = remote.accept().unwrap();
        let mut remote_reader = BufReader::new(remote_conn.try_clone().unwrap());
        let hello = read_value(&mut remote_reader);
        assert_eq!(hello["type"], "link.hello");
        assert_eq!(hello["data"]["token"], "secret");
        assert_eq!(hello["data"]["settings"]["readers_xml"], "<Readers/>");
        write_value(&mut remote_conn, json!({"type": "link.ready"}));
        write_value(
            &mut remote_conn,
            json!({"type": "bridge.client", "data": {"client_id": "haze-tts"}}),
        );

        let (mut bridge_conn, _) = bridge.accept().unwrap();
        let mut bridge_reader = BufReader::new(bridge_conn.try_clone().unwrap());
        let registration = read_value(&mut bridge_reader);
        assert_eq!(registration["type"], "bridge.client");
        assert_eq!(registration["from_remote"], true);
        wait_for(&link, |status| status.connected);

        write_value(&mut bridge_conn, json!({"type": "tts.synthesize"}));
        let request = read_value(&mut remote_reader);
        assert_eq!(request["to_remote"], true);
        write_value(&mut remote_conn, json!({"type": "tts.synthesized"}));
        let reply = read_value(&mut bridge_reader);
        assert_eq!(reply["from_remote"], true);

        drop(link);
        let closed = read_json_line(&mut remote_reader, MAX_REMOTE_LINE).unwrap_or(None);
        assert!(
            closed.is_none(),
            "the remote connection must close when the link stops"
        );
    }

    #[test]
    fn reports_a_refused_handshake() {
        let remote = TcpListener::bind("127.0.0.1:0").unwrap();
        let bridge = TcpListener::bind("127.0.0.1:0").unwrap();
        let link = start_link(&remote, &bridge, "wrong");

        let (mut remote_conn, _) = remote.accept().unwrap();
        let mut remote_reader = BufReader::new(remote_conn.try_clone().unwrap());
        read_value(&mut remote_reader);
        write_value(
            &mut remote_conn,
            json!({"type": "link.refused", "data": {"error": "invalid token"}}),
        );
        let status = wait_for(&link, |status| status.last_error.is_some());
        assert!(!status.connected);
        assert_eq!(
            status.last_error.as_deref(),
            Some("remote refused the link: invalid token")
        );
    }

    #[test]
    fn local_hosts_are_this_machine() {
        assert!(is_local_host(""));
        assert!(is_local_host("localhost"));
        assert!(is_local_host("127.0.0.1"));
        assert!(is_local_host("[::1]"));
        assert!(is_local_host("0.0.0.0"));
        // TEST-NET-1 is never assigned to a real interface.
        assert!(!is_local_host("192.0.2.10"));
    }

    #[test]
    fn retry_delay_backs_off_to_the_cap() {
        assert_eq!(retry_delay(1), MIN_RETRY);
        assert_eq!(retry_delay(2), MIN_RETRY * 2);
        assert_eq!(retry_delay(10), MAX_RETRY);
    }

    #[test]
    fn endpoints_bracket_ipv6_hosts() {
        let endpoint = RemoteEndpoint {
            host: "fe80::1".to_string(),
            port: 41810,
        };
        assert_eq!(endpoint.to_string(), "[fe80::1]:41810");
    }
}
