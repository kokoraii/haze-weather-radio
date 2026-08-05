use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::de::{self, Unexpected, Visitor};
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::ServiceHostConfig;

const DEFAULT_SETTINGS_FILE: &str = "runtime/state/daemonSettings.json";
const STATUS_FILE: &str = "runtime/state/goServiceRuntime.json";
const EMBEDDED_BIN_DIR: &str = "bin";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const MIN_RESTART_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RESTART_BACKOFF: Duration = Duration::from_secs(30);

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/go_assets.rs"));
}

#[derive(Debug, Default, Deserialize)]
struct RootConfig {
    services: Option<ServicesConfig>,
    webpanel: Option<WebPanelConfig>,
    cap: Option<CapConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct ServicesConfig {
    go: Option<GoServicesConfig>,
    rust: Option<RustServicesConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustServicesConfig {
    location: Option<RustLocationConfig>,
    media: Option<RustMediaConfig>,
    playout: Option<RustPlayoutConfig>,
    cgen: Option<RustCgenConfig>,
    cap_ingest: Option<RustCapIngestConfig>,
    easnet: Option<RustEasNetConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustLocationConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    config: Option<PathBuf>,
    scheduler: Option<ServiceSchedulerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustMediaConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    addr: Option<String>,
    listen: Option<String>,
    backend: Option<String>,
    scheduler: Option<ServiceSchedulerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustPlayoutConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    alert_poll: Option<String>,
    scheduler: Option<ServiceSchedulerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustCgenConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    config: Option<PathBuf>,
    scheduler: Option<ServiceSchedulerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustCapIngestConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    source_id: Option<String>,
    source: Option<String>,
    url: Option<String>,
    fallback_url: Option<String>,
    mode: Option<String>,
    archive_url: Option<String>,
    fallback_archive_url: Option<String>,
    interval: Option<String>,
    timeout: Option<String>,
    user_agent: Option<String>,
    shadow: Option<bool>,
    startup_seed: Option<bool>,
    concurrency: Option<usize>,
    scheduler: Option<ServiceSchedulerConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustEasNetConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    config: Option<PathBuf>,
    scheduler: Option<ServiceSchedulerConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ServiceSchedulerConfig {
    priority: Option<String>,
    windows_priority: Option<String>,
    nice: Option<i32>,
}

#[derive(Debug, Default, Deserialize)]
struct GoServicesConfig {
    enabled: Option<bool>,
    web_gateway: Option<WebGatewayConfig>,
    data_ingest: Option<DataIngestConfig>,
    tts: Option<TtsConfig>,
    product_render: Option<ProductRenderConfig>,
    playlist: Option<PlaylistConfig>,
    webhook: Option<WebhookConfig>,
    asr: Option<AsrConfig>,
    ivr: Option<IvrConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct WebGatewayConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WebPanelConfig {
    host: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    port: Option<u16>,
    public_port: Option<WebPanelPublicPortConfig>,
    public: Option<WebPanelSurfaceConfig>,
    admin: Option<WebPanelSurfaceConfig>,
    tls: Option<WebPanelTlsConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct WebPanelSurfaceConfig {
    enabled: Option<bool>,
    host: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    port: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct WebPanelPublicPortConfig {
    enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    http_port: Option<u16>,
    #[serde(default, deserialize_with = "deserialize_optional_port")]
    https_port: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct WebPanelTlsConfig {
    enabled: Option<bool>,
    #[serde(default)]
    domains: Vec<String>,
}

fn deserialize_optional_port<'de, D>(deserializer: D) -> std::result::Result<Option<u16>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct PortVisitor;

    impl<'de> Visitor<'de> for PortVisitor {
        type Value = Option<u16>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a TCP/UDP port number or numeric string")
        }

        fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D2>(self, deserializer: D2) -> std::result::Result<Self::Value, D2::Error>
        where
            D2: serde::Deserializer<'de>,
        {
            deserialize_optional_port(deserializer)
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            u16::try_from(value)
                .map(Some)
                .map_err(|_| E::invalid_value(Unexpected::Unsigned(value), &self))
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            u16::try_from(value)
                .map(Some)
                .map_err(|_| E::invalid_value(Unexpected::Signed(value), &self))
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            let value = value.trim();
            if value.is_empty() {
                return Ok(None);
            }
            value
                .parse::<u16>()
                .map(Some)
                .map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
        }

        fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(PortVisitor)
}

#[derive(Debug, Default, Deserialize)]
struct CapConfig {
    nws_cap: Option<NwsCapConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct NwsCapConfig {
    enabled: Option<bool>,
    sources: Option<Vec<NwsCapSourceConfig>>,
}

#[derive(Debug, Default, Deserialize)]
struct NwsCapSourceConfig {
    id: Option<String>,
    url: Option<String>,
    queries: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
struct DataIngestConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    interval: Option<String>,
    timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct TtsConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    readers: Option<String>,
    dictionary: Option<String>,
    provider: Option<String>,
    language: Option<String>,
    timezone: Option<String>,
    out_dir: Option<String>,
    cache_dir: Option<String>,
    cache_max_bytes: Option<u64>,
    cache_max_entries: Option<usize>,
    timeout: Option<String>,
    workers: Option<usize>,
    piper_voices_dir: Option<String>,
    kokoro_model_dir: Option<String>,
    kokoro_runtime_provider: Option<String>,
    kokoro_threads: Option<usize>,
    kokoro_speed: Option<f32>,
    kokoro_length_scale: Option<f32>,
    speakyapi_url: Option<String>,
    runtime_idle_timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ProductRenderConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    refresh: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PlaylistConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    tick: Option<String>,
    lookahead: Option<String>,
    out_dir: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WebhookConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    webhooks: Option<String>,
    timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct AsrConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct IvrConfig {
    enabled: Option<bool>,
    executable: Option<PathBuf>,
    http: Option<IvrHttpConfig>,
    sip: Option<IvrSipConfig>,
    cache: Option<IvrCacheConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct IvrHttpConfig {
    addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct IvrSipConfig {
    listen: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct IvrCacheConfig {
    dir: Option<String>,
}

#[derive(Clone, Debug)]
struct ServiceSpec {
    id: &'static str,
    kind: &'static str,
    binary: &'static str,
    configured_executable: Option<PathBuf>,
    args: Vec<String>,
    scheduler: ProcessScheduler,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ProcessScheduler {
    windows_priority: WindowsPriorityClass,
    unix_nice: Option<i32>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum WindowsPriorityClass {
    #[default]
    Normal,
    AboveNormal,
    High,
    Realtime,
}

#[derive(Debug)]
struct ManagedService {
    spec: ServiceSpec,
    child: Option<Child>,
    desired: DesiredState,
    restart_count: u64,
    next_restart: Option<Instant>,
}

/// Owns managed service child processes for the lifetime of the daemon.
pub(crate) struct GoServiceSupervisor {
    host: ServiceHostConfig,
    services: Vec<ManagedService>,
    status_path: PathBuf,
    statuses: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DesiredState {
    Running,
    Stopped,
}

impl GoServiceSupervisor {
    /// Starts enabled managed services from the loaded daemon configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the main YAML configuration cannot be read or
    /// parsed. Individual optional service spawn failures are recorded in the
    /// runtime status file and do not abort the Haze daemon.
    pub(crate) fn start(host: &ServiceHostConfig) -> Result<Self> {
        let root = load_config_with_overlay(&host.config_path, &host.app_dir, &host.runtime_dir)?;
        let status_path = crate::runtime_dir::resolve_configured_runtime_path(
            &host.app_dir,
            &host.runtime_dir,
            Path::new(STATUS_FILE),
        );
        if let Some(parent) = status_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create managed service status directory {}",
                    parent.display()
                )
            })?;
        }
        stop_previous_managed_services(&status_path);

        let specs = service_specs(&root, host);
        let mut supervisor = Self {
            host: host.clone(),
            services: specs
                .into_iter()
                .map(|spec| ManagedService {
                    spec,
                    child: None,
                    desired: DesiredState::Running,
                    restart_count: 0,
                    next_restart: None,
                })
                .collect(),
            status_path,
            statuses: BTreeMap::new(),
        };

        for index in 0..supervisor.services.len() {
            supervisor.start_service(index, "startup");
        }
        supervisor.write_status();
        Ok(supervisor)
    }

    fn start_service(&mut self, index: usize, reason: &str) -> bool {
        let Some(service) = self.services.get(index) else {
            return false;
        };
        if service.child.is_some() || service.desired != DesiredState::Running {
            return false;
        }
        let spec = service.spec.clone();
        let Some(executable) = resolve_executable(
            &self.host,
            spec.configured_executable.as_deref(),
            spec.binary,
        )
        .or_else(|| {
            extract_embedded_executable(&self.host, spec.binary)
                .ok()
                .flatten()
        }) else {
            let error = format!("managed service binary not found: {}", spec.binary);
            warn!("[{}] {error}", service_label(spec.id));
            if let Some(service) = self.services.get_mut(index) {
                service.restart_count = service.restart_count.saturating_add(1);
                service.next_restart =
                    Some(Instant::now() + restart_backoff(service.restart_count));
            }
            self.set_status(
                spec.id,
                spec.kind,
                "missing",
                DesiredState::Running,
                None,
                Some(error),
            );
            return true;
        };

        let mut command = managed_command(&executable, spec.scheduler);
        configure_managed_process(&mut command, spec.scheduler);
        command
            .args(&spec.args)
            .current_dir(&self.host.runtime_dir)
            .env("HAZE_PARENT_PID", std::process::id().to_string())
            .env("HAZE_MANAGED_SERVICE_ID", spec.id)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_managed_service_env(&mut command, &spec);

        match command.spawn() {
            Ok(mut child) => {
                let pid = child.id();
                if let Some(stdout) = child.stdout.take() {
                    pipe_service_output(spec.id, "stdout", stdout, false);
                }
                if let Some(stderr) = child.stderr.take() {
                    pipe_service_output(spec.id, "stderr", stderr, true);
                }
                apply_managed_process_priority(pid, spec.scheduler, service_label(spec.id));
                info!("started {} (pid {pid})", service_label(spec.id));
                debug!(
                    "[{}] executable {}",
                    service_label(spec.id),
                    executable.display()
                );
                let restart_count = self.services[index].restart_count;
                self.statuses.insert(
                    spec.id.to_string(),
                    json!({
                        "id": spec.id,
                        "kind": spec.kind,
                        "status": "running",
                        "desired": "running",
                        "pid": pid,
                        "executable": executable,
                        "args": spec.args,
                        "embedded": embedded_binary(spec.binary).is_some(),
                        "restart_count": restart_count,
                        "start_reason": reason,
                        "scheduler": spec.scheduler.status_value(),
                        "started_at_unix": unix_now(),
                    }),
                );
                if let Some(service) = self.services.get_mut(index) {
                    service.child = Some(child);
                    service.next_restart = None;
                }
                true
            }
            Err(err) => {
                let detail = format!("failed to start {}: {err}", service_label(spec.id));
                warn!("{detail}");
                self.set_status(
                    spec.id,
                    spec.kind,
                    "failed",
                    DesiredState::Running,
                    None,
                    Some(detail),
                );
                if let Some(service) = self.services.get_mut(index) {
                    service.restart_count = service.restart_count.saturating_add(1);
                    service.next_restart =
                        Some(Instant::now() + restart_backoff(service.restart_count));
                }
                true
            }
        }
    }

    fn set_status(
        &mut self,
        id: &str,
        kind: &str,
        status: &str,
        desired: DesiredState,
        pid: Option<u32>,
        last_error: Option<String>,
    ) {
        self.statuses.insert(
            id.to_string(),
            json!({
                "id": id,
                "kind": kind,
                "status": status,
                "desired": desired_status(desired),
                "pid": pid,
                "last_error": last_error,
                "updated_at_unix": unix_now(),
            }),
        );
    }

    pub(crate) fn poll_children(&mut self) {
        let mut changed = false;
        for index in 0..self.services.len() {
            let spec = self.services[index].spec.clone();
            let desired = self.services[index].desired;
            let restart_count = self.services[index].restart_count;
            let Some(child) = self.services[index].child.as_mut() else {
                continue;
            };
            match child.try_wait() {
                Ok(Some(status)) => {
                    let pid = child.id();
                    self.services[index].child = None;
                    let state = if desired == DesiredState::Running {
                        "restarting"
                    } else if status.success() {
                        "stopped"
                    } else {
                        "failed"
                    };
                    let detail = format!("{} exited with {status}", service_label(spec.id));
                    if desired == DesiredState::Running {
                        warn!(
                            "[{}] exited with {status}; scheduling restart",
                            service_label(spec.id)
                        );
                        self.services[index].restart_count =
                            self.services[index].restart_count.saturating_add(1);
                        self.services[index].next_restart = Some(
                            Instant::now() + restart_backoff(self.services[index].restart_count),
                        );
                    } else {
                        info!("[{}] exited with {status}", service_label(spec.id));
                    }
                    self.statuses.insert(
                        spec.id.to_string(),
                        json!({
                            "id": spec.id,
                            "kind": spec.kind,
                            "status": state,
                            "desired": desired_status(desired),
                            "pid": pid,
                            "restart_count": self.services[index].restart_count,
                            "last_error": if desired == DesiredState::Running || !status.success() { Some(detail) } else { None::<String> },
                            "exited_at_unix": unix_now(),
                        }),
                    );
                    changed = true;
                }
                Ok(None) => {}
                Err(err) => {
                    let pid = child.id();
                    let detail = format!("failed to inspect {}: {err}", service_label(spec.id));
                    warn!(
                        "[{}] failed to inspect service: {err}",
                        service_label(spec.id)
                    );
                    self.statuses.insert(
                        spec.id.to_string(),
                        json!({
                            "id": spec.id,
                            "kind": spec.kind,
                            "status": "unknown",
                            "desired": desired_status(desired),
                            "pid": pid,
                            "restart_count": restart_count,
                            "last_error": detail,
                            "updated_at_unix": unix_now(),
                        }),
                    );
                    changed = true;
                }
            }
        }
        let now = Instant::now();
        for index in 0..self.services.len() {
            if self.services[index].child.is_none()
                && self.services[index].desired == DesiredState::Running
                && self.services[index]
                    .next_restart
                    .is_none_or(|deadline| deadline <= now)
            {
                changed |= self.start_service(index, "auto_restart");
            }
        }
        if changed {
            self.write_status();
        }
    }

    pub(crate) fn handle_control_event(&mut self, event: &Value) -> bool {
        if event.get("type").and_then(Value::as_str) != Some("service.control") {
            return false;
        }
        let data = event.get("data").and_then(Value::as_object);
        let service_id = data
            .and_then(|data| data.get("service_id").or_else(|| data.get("id")))
            .and_then(Value::as_str)
            .or_else(|| event.get("subject").and_then(Value::as_str))
            .unwrap_or_default()
            .trim();
        let action = data
            .and_then(|data| data.get("action"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if service_id.is_empty() || action.is_empty() {
            return false;
        }
        let Some(index) = self
            .services
            .iter()
            .position(|service| service.spec.id == service_id)
        else {
            warn!("service control requested unknown service {service_id}");
            return false;
        };
        let changed = match action.as_str() {
            "start" => self.start_controlled(index),
            "stop" => self.stop_controlled(index),
            "restart" => self.restart_controlled(index),
            _ => {
                warn!("service control requested unsupported action {action}");
                false
            }
        };
        if changed {
            self.write_status();
        }
        changed
    }

    fn start_controlled(&mut self, index: usize) -> bool {
        self.services[index].desired = DesiredState::Running;
        self.services[index].next_restart = None;
        if self.services[index].child.is_some() {
            return false;
        }
        self.start_service(index, "operator_start")
    }

    fn stop_controlled(&mut self, index: usize) -> bool {
        self.services[index].desired = DesiredState::Stopped;
        self.services[index].next_restart = None;
        self.stop_service(index, "stopped", "operator_stop")
    }

    fn restart_controlled(&mut self, index: usize) -> bool {
        self.services[index].desired = DesiredState::Running;
        self.services[index].next_restart = None;
        let stopped = self.stop_service(index, "restarting", "operator_restart");
        let started = self.start_service(index, "operator_restart");
        stopped || started
    }

    pub(crate) fn restart_all(&mut self) -> usize {
        let indexes = self
            .services
            .iter()
            .enumerate()
            .filter_map(|(index, service)| {
                (service.desired == DesiredState::Running).then_some(index)
            })
            .collect::<Vec<_>>();
        if indexes.is_empty() {
            return 0;
        }

        info!("restarting {} managed services", indexes.len());
        for index in indexes.iter().copied() {
            self.services[index].next_restart = None;
            self.stop_service(index, "restarting", "operator_restart_all");
        }
        self.write_status();
        for index in indexes.iter().copied() {
            self.start_service(index, "operator_restart_all");
        }
        self.write_status();
        indexes.len()
    }

    fn stop_service(&mut self, index: usize, final_status: &str, reason: &str) -> bool {
        let spec = self.services[index].spec.clone();
        let Some(mut child) = self.services[index].child.take() else {
            self.statuses.insert(
                spec.id.to_string(),
                json!({
                    "id": spec.id,
                    "kind": spec.kind,
                    "status": if final_status == "restarting" { "restarting" } else { "stopped" },
                    "desired": desired_status(self.services[index].desired),
                    "pid": None::<u32>,
                    "restart_count": self.services[index].restart_count,
                    "control_reason": reason,
                    "updated_at_unix": unix_now(),
                }),
            );
            return true;
        };
        info!("stopping {} ({reason})", service_label(spec.id));
        if let Err(err) = terminate_child_tree(&mut child) {
            warn!("[{}] failed to stop service: {err}", service_label(spec.id));
        }
        let status = wait_for_child_exit(&mut child, SHUTDOWN_GRACE);
        self.statuses.insert(
            spec.id.to_string(),
            json!({
                "id": spec.id,
                "kind": spec.kind,
                "status": final_status,
                "desired": desired_status(self.services[index].desired),
                "pid": child.id(),
                "exit_status": status.map(|status| status.to_string()),
                "restart_count": self.services[index].restart_count,
                "control_reason": reason,
                "stopped_at_unix": unix_now(),
            }),
        );
        true
    }

    pub(crate) fn shutdown(&mut self) {
        self.poll_children();
        let mut changed = false;
        for index in 0..self.services.len() {
            self.services[index].desired = DesiredState::Stopped;
            let spec = self.services[index].spec.clone();
            let Some(mut child) = self.services[index].child.take() else {
                continue;
            };
            match child.try_wait() {
                Ok(Some(status)) => {
                    info!(
                        "[{}] exited during shutdown with {status}",
                        service_label(spec.id)
                    );
                    self.statuses.insert(
                        spec.id.to_string(),
                        json!({
                            "id": spec.id,
                            "kind": spec.kind,
                            "status": if status.success() { "exited" } else { "failed" },
                            "desired": "stopped",
                            "pid": child.id(),
                            "restart_count": self.services[index].restart_count,
                            "exited_at_unix": unix_now(),
                        }),
                    );
                }
                Ok(None) => {
                    info!("stopping {}", service_label(spec.id));
                    if let Err(err) = terminate_child_tree(&mut child) {
                        warn!("[{}] failed to stop service: {err}", service_label(spec.id));
                    }
                    let status = wait_for_child_exit(&mut child, SHUTDOWN_GRACE);
                    self.statuses.insert(
                        spec.id.to_string(),
                        json!({
                            "id": spec.id,
                            "kind": spec.kind,
                            "status": "stopped",
                            "desired": "stopped",
                            "pid": child.id(),
                            "exit_status": status.map(|status| status.to_string()),
                            "restart_count": self.services[index].restart_count,
                            "stopped_at_unix": unix_now(),
                        }),
                    );
                }
                Err(err) => {
                    warn!(
                        "[{}] failed to inspect service: {err}",
                        service_label(spec.id)
                    );
                    self.statuses.insert(
                        spec.id.to_string(),
                        json!({
                            "id": spec.id,
                            "kind": spec.kind,
                            "status": "unknown",
                            "desired": "stopped",
                            "pid": child.id(),
                            "restart_count": self.services[index].restart_count,
                            "last_error": format!("failed to inspect managed service: {err}"),
                            "updated_at_unix": unix_now(),
                        }),
                    );
                }
            }
            changed = true;
        }
        if changed {
            self.write_status();
        }
    }

    fn write_status(&self) {
        let payload = json!({
            "updated_at_unix": unix_now(),
            "services": self.statuses,
        });
        if let Ok(raw) = serde_json::to_vec_pretty(&payload) {
            let tmp = self.status_path.with_extension("json.tmp");
            if fs::write(&tmp, raw).is_ok() {
                let _ = fs::rename(tmp, &self.status_path);
            }
        }
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
            Ok(None) => {
                let _ = child.kill();
                return child.wait().ok();
            }
            Err(_) => return None,
        }
    }
}

fn desired_status(desired: DesiredState) -> &'static str {
    match desired {
        DesiredState::Running => "running",
        DesiredState::Stopped => "stopped",
    }
}

fn restart_backoff(restart_count: u64) -> Duration {
    let exponent = restart_count.saturating_sub(1).min(5);
    let seconds = MIN_RESTART_BACKOFF.as_secs().saturating_mul(1 << exponent);
    Duration::from_secs(seconds).min(MAX_RESTART_BACKOFF)
}

impl ProcessScheduler {
    fn from_config(config: Option<&ServiceSchedulerConfig>, fallback: Self) -> Self {
        let Some(config) = config else {
            return fallback;
        };
        let mut scheduler = fallback;
        let priority = config
            .windows_priority
            .as_deref()
            .or(config.priority.as_deref())
            .and_then(parse_windows_priority)
            .unwrap_or(scheduler.windows_priority);
        scheduler.windows_priority = priority;
        if let Some(nice) = config.nice {
            scheduler.unix_nice = Some(nice.clamp(-20, 19));
        }
        scheduler
    }

    fn cgen_default() -> Self {
        Self {
            windows_priority: WindowsPriorityClass::AboveNormal,
            unix_nice: Some(-2),
        }
    }

    fn media_default() -> Self {
        Self {
            windows_priority: WindowsPriorityClass::High,
            unix_nice: Some(-10),
        }
    }

    fn status_value(self) -> Value {
        json!({
            "windows_priority": self.windows_priority.as_str(),
            "unix_nice": self.unix_nice,
        })
    }
}

impl WindowsPriorityClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::AboveNormal => "above_normal",
            Self::High => "high",
            Self::Realtime => "realtime",
        }
    }
}

fn parse_windows_priority(value: &str) -> Option<WindowsPriorityClass> {
    match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "" | "normal" => Some(WindowsPriorityClass::Normal),
        "above_normal" | "above" => Some(WindowsPriorityClass::AboveNormal),
        "high" => Some(WindowsPriorityClass::High),
        "realtime" | "real_time" | "rt" => Some(WindowsPriorityClass::Realtime),
        _ => None,
    }
}

impl Drop for GoServiceSupervisor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn service_specs(root: &RootConfig, host: &ServiceHostConfig) -> Vec<ServiceSpec> {
    let rust_location = root
        .services
        .as_ref()
        .and_then(|services| services.rust.as_ref())
        .and_then(|services| services.location.as_ref());
    let rust_playout = root
        .services
        .as_ref()
        .and_then(|services| services.rust.as_ref())
        .and_then(|services| services.playout.as_ref());
    let rust_media = root
        .services
        .as_ref()
        .and_then(|services| services.rust.as_ref())
        .and_then(|services| services.media.as_ref());
    let rust_cgen = root
        .services
        .as_ref()
        .and_then(|services| services.rust.as_ref())
        .and_then(|services| services.cgen.as_ref());
    let rust_cap = root
        .services
        .as_ref()
        .and_then(|services| services.rust.as_ref())
        .and_then(|services| services.cap_ingest.as_ref());
    let rust_easnet = root
        .services
        .as_ref()
        .and_then(|services| services.rust.as_ref())
        .and_then(|services| services.easnet.as_ref());
    let mut specs = Vec::new();
    let mut deferred_cap_specs = Vec::new();

    if let Some(location) = rust_location.filter(|location| location.enabled.unwrap_or(false)) {
        specs.push(ServiceSpec {
            id: "svc:location",
            kind: "managed",
            binary: executable_name("haze-location"),
            configured_executable: location.executable.clone(),
            args: vec![
                "--config".to_string(),
                host.config_path.to_string_lossy().into_owned(),
                "--locations".to_string(),
                location
                    .config
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("managed/configs/locations.xml"))
                    .to_string_lossy()
                    .into_owned(),
                "--bridge".to_string(),
                std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
            ],
            scheduler: ProcessScheduler::from_config(
                location.scheduler.as_ref(),
                ProcessScheduler::default(),
            ),
        });
    }

    if let Some(go) = root
        .services
        .as_ref()
        .and_then(|services| services.go.as_ref())
        .filter(|go| go.enabled.unwrap_or(false))
    {
        if let Some(web) = &go.web_gateway {
            if web.enabled.unwrap_or(false) {
                specs.extend(web_gateway_specs(web, root.webpanel.as_ref(), host));
            }
        }

        if let Some(data_ingest) = &go.data_ingest {
            if data_ingest.enabled.unwrap_or(false) {
                let mut args = vec![
                    "--config".to_string(),
                    host.config_path.to_string_lossy().into_owned(),
                ];
                if let Some(interval) = data_ingest
                    .interval
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--interval".to_string(), interval.to_string()]);
                }
                if let Some(timeout) = data_ingest
                    .timeout
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--timeout".to_string(), timeout.to_string()]);
                }
                specs.push(ServiceSpec {
                    id: "svc:data_ingest",
                    kind: "managed",
                    binary: executable_name("haze-data-ingest"),
                    configured_executable: data_ingest.executable.clone(),
                    args,
                    scheduler: ProcessScheduler::default(),
                });
            }
        }

        if let Some(tts) = &go.tts {
            if tts.enabled.unwrap_or(false) {
                let mut args = vec![
                    "--service".to_string(),
                    "--bridge".to_string(),
                    std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
                    "--readers".to_string(),
                    tts.readers
                        .clone()
                        .unwrap_or_else(|| "managed/configs/readers.xml".to_string()),
                    "--dictionary".to_string(),
                    tts.dictionary
                        .clone()
                        .unwrap_or_else(|| "managed/dictionary.json".to_string()),
                    "--provider".to_string(),
                    tts.provider.clone().unwrap_or_else(|| "auto".to_string()),
                    "--lang".to_string(),
                    tts.language.clone().unwrap_or_else(|| "en-CA".to_string()),
                    "--timezone".to_string(),
                    tts.timezone.clone().unwrap_or_else(|| "Local".to_string()),
                    "--out-dir".to_string(),
                    tts.out_dir
                        .clone()
                        .unwrap_or_else(|| "runtime/audio/tts".to_string()),
                    "--timeout".to_string(),
                    tts.timeout.clone().unwrap_or_else(|| "60s".to_string()),
                ];
                if let Some(voices_dir) = tts
                    .piper_voices_dir
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--piper-voices-dir".to_string(), voices_dir.to_string()]);
                }
                let cache_dir = tts
                    .cache_dir
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("runtime/cache/tts");
                let cache_dir = crate::runtime_dir::resolve_configured_runtime_path(
                    &host.app_dir,
                    &host.runtime_dir,
                    Path::new(cache_dir),
                );
                args.extend([
                    "--cache-dir".to_string(),
                    cache_dir.to_string_lossy().into_owned(),
                ]);
                if let Some(max_bytes) = tts.cache_max_bytes {
                    args.extend(["--cache-max-bytes".to_string(), max_bytes.to_string()]);
                }
                if let Some(max_entries) = tts.cache_max_entries {
                    args.extend(["--cache-max-entries".to_string(), max_entries.to_string()]);
                }
                if let Some(workers) = tts.workers {
                    args.extend(["--workers".to_string(), workers.max(1).to_string()]);
                }
                if let Some(model_dir) = tts
                    .kokoro_model_dir
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--kokoro-model-dir".to_string(), model_dir.to_string()]);
                }
                if let Some(provider) = tts
                    .kokoro_runtime_provider
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend([
                        "--kokoro-runtime-provider".to_string(),
                        provider.to_string(),
                    ]);
                }
                if let Some(threads) = tts.kokoro_threads {
                    args.extend(["--kokoro-threads".to_string(), threads.to_string()]);
                }
                if let Some(speed) = tts.kokoro_speed {
                    args.extend(["--kokoro-speed".to_string(), speed.to_string()]);
                }
                if let Some(length_scale) = tts.kokoro_length_scale {
                    args.extend([
                        "--kokoro-length-scale".to_string(),
                        length_scale.to_string(),
                    ]);
                }
                if let Some(url) = tts
                    .speakyapi_url
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--speakyapi-url".to_string(), url.to_string()]);
                }
                if let Some(timeout) = tts
                    .runtime_idle_timeout
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--runtime-idle-timeout".to_string(), timeout.to_string()]);
                }
                specs.push(ServiceSpec {
                    id: "aux:tts",
                    kind: "managed",
                    binary: executable_name("haze-tts"),
                    configured_executable: tts.executable.clone(),
                    args,
                    scheduler: ProcessScheduler::default(),
                });
            }
        }

        if let Some(product_render) = &go.product_render {
            if product_render.enabled.unwrap_or(false) {
                let mut args = vec![
                    "--config".to_string(),
                    host.config_path.to_string_lossy().into_owned(),
                    "--bridge".to_string(),
                    std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
                ];
                if let Some(refresh) = product_render
                    .refresh
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--refresh".to_string(), refresh.to_string()]);
                }
                specs.push(ServiceSpec {
                    id: "svc:product_render",
                    kind: "managed",
                    binary: executable_name("haze-product-render"),
                    configured_executable: product_render.executable.clone(),
                    args,
                    scheduler: ProcessScheduler::default(),
                });
            }
        }

        if let Some(playlist) = &go.playlist {
            if playlist.enabled.unwrap_or(false) {
                let mut args = vec![
                    "--config".to_string(),
                    host.config_path.to_string_lossy().into_owned(),
                    "--bridge".to_string(),
                    std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
                    "--out-dir".to_string(),
                    playlist
                        .out_dir
                        .clone()
                        .unwrap_or_else(|| "runtime/audio/playlist".to_string()),
                ];
                if let Some(tick) = playlist
                    .tick
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--tick".to_string(), tick.to_string()]);
                }
                if let Some(lookahead) = playlist
                    .lookahead
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--lookahead".to_string(), lookahead.to_string()]);
                }
                specs.push(ServiceSpec {
                    id: "svc:playlist",
                    kind: "managed",
                    binary: executable_name("haze-playlist"),
                    configured_executable: playlist.executable.clone(),
                    args,
                    scheduler: ProcessScheduler::default(),
                });
            }
        }

        if let Some(webhook) = &go.webhook {
            if webhook.enabled.unwrap_or(false) {
                let mut args = vec![
                    "--config".to_string(),
                    host.config_path.to_string_lossy().into_owned(),
                    "--bridge".to_string(),
                    std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
                    "--webhooks".to_string(),
                    webhook
                        .webhooks
                        .clone()
                        .unwrap_or_else(|| "managed/configs/webhooks.xml".to_string()),
                ];
                if let Some(timeout) = webhook
                    .timeout
                    .as_ref()
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--timeout".to_string(), timeout.to_string()]);
                }
                specs.push(ServiceSpec {
                    id: "svc:webhook",
                    kind: "managed",
                    binary: executable_name("haze-webhook"),
                    configured_executable: webhook.executable.clone(),
                    args,
                    scheduler: ProcessScheduler::default(),
                });
            }
        }

        if let Some(asr) = &go.asr {
            if asr.enabled.unwrap_or(false) {
                specs.push(ServiceSpec {
                    id: "svc:asr",
                    kind: "managed",
                    binary: executable_name("haze-asr"),
                    configured_executable: asr.executable.clone(),
                    args: vec![
                        "--config".to_string(),
                        host.config_path.to_string_lossy().into_owned(),
                        "--bridge".to_string(),
                        std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
                    ],
                    scheduler: ProcessScheduler::default(),
                });
            }
        }

        if let Some(ivr) = &go.ivr {
            if ivr.enabled.unwrap_or(false) {
                let mut args = vec![
                    "--config".to_string(),
                    host.config_path.to_string_lossy().into_owned(),
                    "--bridge".to_string(),
                    std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
                    "--media-bridge".to_string(),
                    std::env::var("HAZE_MEDIA_BRIDGE_ADDR").unwrap_or_default(),
                ];
                if let Some(addr) = ivr
                    .http
                    .as_ref()
                    .and_then(|http| http.addr.as_ref())
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--http-addr".to_string(), addr.to_string()]);
                }
                if let Some(addr) = ivr
                    .sip
                    .as_ref()
                    .and_then(|sip| sip.listen.as_ref())
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--sip-addr".to_string(), addr.to_string()]);
                }
                if let Some(dir) = ivr
                    .cache
                    .as_ref()
                    .and_then(|cache| cache.dir.as_ref())
                    .filter(|value| !value.trim().is_empty())
                {
                    args.extend(["--cache-dir".to_string(), dir.to_string()]);
                }
                specs.push(ServiceSpec {
                    id: "svc:ivr",
                    kind: "managed",
                    binary: executable_name("haze-ivr"),
                    configured_executable: ivr.executable.clone(),
                    args,
                    scheduler: ProcessScheduler::default(),
                });
            }
        }
    }

    if let Some(media) = rust_media.filter(|media| media.enabled.unwrap_or(false)) {
        let mut args = vec![
            "--config".to_string(),
            host.config_path.to_string_lossy().into_owned(),
            "--bridge".to_string(),
            std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
            "--media-bridge".to_string(),
            std::env::var("HAZE_MEDIA_BRIDGE_ADDR").unwrap_or_default(),
        ];
        if let Some(addr) = media
            .listen
            .as_ref()
            .or(media.addr.as_ref())
            .filter(|value| !value.trim().is_empty())
        {
            args.extend(["--listen".to_string(), addr.to_string()]);
        }
        if let Some(backend) = media
            .backend
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            args.extend(["--backend".to_string(), backend.to_string()]);
        }
        specs.push(ServiceSpec {
            id: "svc:media",
            kind: "managed",
            binary: executable_name("haze-media"),
            configured_executable: media.executable.clone(),
            args,
            scheduler: ProcessScheduler::from_config(
                media.scheduler.as_ref(),
                ProcessScheduler::media_default(),
            ),
        });
    }

    if let Some(playout) = rust_playout.filter(|playout| playout.enabled.unwrap_or(false)) {
        let mut args = vec![
            "--config".to_string(),
            host.config_path.to_string_lossy().into_owned(),
            "--bridge".to_string(),
            std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
        ];
        if let Some(alert_poll) = playout
            .alert_poll
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            args.extend(["--alert-poll".to_string(), alert_poll.to_string()]);
        }
        specs.push(ServiceSpec {
            id: "svc:playout",
            kind: "managed",
            binary: executable_name("haze-playout-rs"),
            configured_executable: playout.executable.clone(),
            args,
            scheduler: ProcessScheduler::from_config(
                playout.scheduler.as_ref(),
                ProcessScheduler::media_default(),
            ),
        });
    }
    if let Some(cgen) = rust_cgen.filter(|cgen| cgen.enabled.unwrap_or(false)) {
        let args = vec![
            "--config".to_string(),
            host.config_path.to_string_lossy().into_owned(),
            "--cgen".to_string(),
            cgen.config
                .clone()
                .unwrap_or_else(|| PathBuf::from("managed/configs/cgen.xml"))
                .to_string_lossy()
                .into_owned(),
            "--bridge".to_string(),
            std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
        ];
        specs.push(ServiceSpec {
            id: "svc:cgen",
            kind: "managed",
            binary: executable_name("haze-cgen"),
            configured_executable: cgen.executable.clone(),
            args,
            scheduler: ProcessScheduler::from_config(
                cgen.scheduler.as_ref(),
                ProcessScheduler::cgen_default(),
            ),
        });
    }
    if let Some(cap) = rust_cap.filter(|cap| cap.enabled.unwrap_or(false)) {
        deferred_cap_specs.extend(rust_cap_ingest_specs(cap, root));
    }
    if let Some(easnet) = rust_easnet.filter(|easnet| easnet.enabled.unwrap_or(false)) {
        specs.push(ServiceSpec {
            id: "svc:easnet",
            kind: "managed",
            binary: executable_name("haze-easnet"),
            configured_executable: easnet.executable.clone(),
            args: vec![
                "--config".to_string(),
                host.config_path.to_string_lossy().into_owned(),
                "--easnet".to_string(),
                easnet
                    .config
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("managed/configs/easnet.xml"))
                    .to_string_lossy()
                    .into_owned(),
                "--bridge".to_string(),
                std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
            ],
            scheduler: ProcessScheduler::from_config(
                easnet.scheduler.as_ref(),
                ProcessScheduler::default(),
            ),
        });
    }
    specs.extend(deferred_cap_specs);
    specs
}

fn rust_cap_ingest_specs(cap: &RustCapIngestConfig, root: &RootConfig) -> Vec<ServiceSpec> {
    let mut specs = Vec::new();
    specs.push(rust_cap_ingest_spec(
        "svc:cap_ingest",
        cap,
        cap.source_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("rust-cap"),
        cap.source
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("naads"),
        cap.url.as_deref(),
        cap.fallback_url.as_deref(),
    ));

    if root
        .cap
        .as_ref()
        .and_then(|cap| cap.nws_cap.as_ref())
        .and_then(|nws| nws.enabled)
        .unwrap_or(false)
    {
        let source = root
            .cap
            .as_ref()
            .and_then(|cap| cap.nws_cap.as_ref())
            .and_then(|nws| nws.sources.as_ref())
            .and_then(|sources| sources.first());
        let url = source
            .and_then(|source| source.url.as_ref())
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| "https://api.weather.gov/alerts/active.atom".to_string());
        let url = if url.to_ascii_lowercase().contains(".atom") {
            url
        } else {
            append_query_params(
                &url,
                source
                    .and_then(|source| source.queries.as_ref())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            )
        };
        let source_id = source
            .and_then(|source| source.id.as_ref())
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| "nws_api".to_string());
        specs.push(rust_cap_ingest_spec(
            "svc:nws_cap_ingest",
            cap,
            &source_id,
            "nws",
            Some(&url),
            None,
        ));
    }

    specs
}

fn rust_cap_ingest_spec(
    id: &'static str,
    cap: &RustCapIngestConfig,
    source_id: &str,
    source: &str,
    url: Option<&str>,
    fallback_url: Option<&str>,
) -> ServiceSpec {
    let mode = if source.eq_ignore_ascii_case("nws") {
        "atom".to_string()
    } else {
        cap.mode.clone().unwrap_or_else(|| "auto".to_string())
    };
    let mut args = vec![
        "--source-id".to_string(),
        source_id.to_string(),
        "--source".to_string(),
        source.to_string(),
        "--mode".to_string(),
        mode,
        "--interval".to_string(),
        cap.interval.clone().unwrap_or_else(|| "5s".to_string()),
        "--timeout".to_string(),
        cap.timeout.clone().unwrap_or_else(|| "15s".to_string()),
        "--startup-seed".to_string(),
        cap.startup_seed.unwrap_or(true).to_string(),
        "--concurrency".to_string(),
        cap.concurrency.unwrap_or(8).max(1).to_string(),
        "--bridge".to_string(),
        std::env::var("HAZE_HOST_BRIDGE_ADDR").unwrap_or_default(),
    ];
    if let Some(user_agent) = cap
        .user_agent
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.extend(["--user-agent".to_string(), user_agent.to_string()]);
    }
    if let Some(url) = url.filter(|value| !value.trim().is_empty()) {
        args.extend(["--url".to_string(), url.to_string()]);
    }
    if let Some(url) = fallback_url.filter(|value| !value.trim().is_empty()) {
        args.extend(["--fallback-url".to_string(), url.to_string()]);
    }
    if let Some(url) = cap
        .archive_url
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.extend(["--archive-url".to_string(), url.to_string()]);
    }
    if let Some(url) = cap
        .fallback_archive_url
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        args.extend(["--fallback-archive-url".to_string(), url.to_string()]);
    }
    if cap.shadow.unwrap_or(false) {
        args.push("--shadow".to_string());
    }
    ServiceSpec {
        id,
        kind: "managed",
        binary: executable_name("haze-cap-ingest"),
        configured_executable: cap.executable.clone(),
        args,
        scheduler: ProcessScheduler::from_config(
            cap.scheduler.as_ref(),
            ProcessScheduler::default(),
        ),
    }
}

fn web_gateway_specs(
    web: &WebGatewayConfig,
    webpanel: Option<&WebPanelConfig>,
    host: &ServiceHostConfig,
) -> Vec<ServiceSpec> {
    let mut specs = Vec::new();
    let Some(panel) = webpanel else {
        specs.push(web_gateway_spec(
            "go:web_gateway",
            "combined",
            web.addr
                .clone()
                .unwrap_or_else(|| "0.0.0.0:6444".to_string()),
            web.executable.clone(),
            host,
        ));
        return specs;
    };

    let public_enabled = panel
        .public
        .as_ref()
        .and_then(|surface| surface.enabled)
        .unwrap_or(true);
    let admin_enabled = panel
        .admin
        .as_ref()
        .and_then(|surface| surface.enabled)
        .unwrap_or(true);
    let public_addr = webpanel_addr(
        panel.public.as_ref(),
        panel.host.as_deref().unwrap_or("0.0.0.0"),
        Some(6444),
    );
    let admin_addr = webpanel_addr(
        panel.admin.as_ref(),
        panel.host.as_deref().unwrap_or("0.0.0.0"),
        panel.port.or(Some(6444)),
    );
    if public_enabled && admin_enabled && public_addr == admin_addr {
        specs.push(web_gateway_spec(
            "go:web_gateway",
            "combined",
            public_addr,
            web.executable.clone(),
            host,
        ));
    } else {
        if public_enabled {
            specs.push(web_gateway_spec(
                "go:web_public",
                "public",
                public_addr,
                web.executable.clone(),
                host,
            ));
        }
        if admin_enabled {
            specs.push(web_gateway_spec(
                "go:web_admin",
                "admin",
                admin_addr,
                web.executable.clone(),
                host,
            ));
        }
    }

    if specs.is_empty() {
        specs.push(web_gateway_spec(
            "go:web_gateway",
            "combined",
            web.addr
                .clone()
                .unwrap_or_else(|| "0.0.0.0:6444".to_string()),
            web.executable.clone(),
            host,
        ));
    }
    specs
}

fn append_query_params(url: &str, queries: &[String]) -> String {
    let mut out = url.trim().to_string();
    for query in queries {
        let query = query.trim();
        if query.is_empty() {
            continue;
        }
        if out.contains('?') {
            out.push('&');
        } else {
            out.push('?');
        }
        out.push_str(query);
    }
    out
}

fn web_gateway_spec(
    id: &'static str,
    surface: &str,
    addr: String,
    configured_executable: Option<PathBuf>,
    host: &ServiceHostConfig,
) -> ServiceSpec {
    ServiceSpec {
        id,
        kind: "managed",
        binary: executable_name("haze-web"),
        configured_executable,
        args: vec![
            "--addr".to_string(),
            addr,
            "--surface".to_string(),
            surface.to_string(),
            "--webroot".to_string(),
            host.app_dir.join("webroot").to_string_lossy().into_owned(),
            "--config".to_string(),
            host.config_path.to_string_lossy().into_owned(),
        ],
        scheduler: ProcessScheduler::default(),
    }
}

fn webpanel_addr(
    surface: Option<&WebPanelSurfaceConfig>,
    fallback_host: &str,
    fallback_port: Option<u16>,
) -> String {
    let host = surface
        .and_then(|surface| surface.host.as_deref())
        .filter(|host| !host.trim().is_empty())
        .unwrap_or(fallback_host)
        .trim();
    let port = surface
        .and_then(|surface| surface.port)
        .or(fallback_port)
        .unwrap_or(6444);
    format!("{host}:{port}")
}

pub(crate) fn public_panel_url(host: &ServiceHostConfig) -> Result<String> {
    for name in ["HAZE_PUBLIC_BASE_URL", "WEB_BASE_URL", "WEB_HOSTNAME"] {
        if let Ok(value) = env::var(name) {
            if let Some(url) = normalize_public_base_url(&value) {
                return Ok(url);
            }
        }
    }

    let root = load_config_with_overlay(&host.config_path, &host.app_dir, &host.runtime_dir)?;
    Ok(public_panel_url_from_config(&root))
}

fn public_panel_url_from_config(root: &RootConfig) -> String {
    let Some(panel) = root.webpanel.as_ref() else {
        let configured_addr = root
            .services
            .as_ref()
            .and_then(|services| services.go.as_ref())
            .and_then(|services| services.web_gateway.as_ref())
            .and_then(|web| web.addr.as_deref())
            .unwrap_or("0.0.0.0:6444");
        let (host, port) = split_listen_addr(configured_addr, 6444);
        return format_panel_url("http", &browser_host(&host), port);
    };

    let tls_enabled = panel
        .tls
        .as_ref()
        .and_then(|tls| tls.enabled)
        .unwrap_or(false);
    let public_port = panel.public_port.as_ref();
    let use_public_port = public_port
        .and_then(|public_port| public_port.enabled)
        .unwrap_or(false);
    let port = if use_public_port {
        if tls_enabled {
            public_port
                .and_then(|public_port| public_port.https_port)
                .unwrap_or(443)
        } else {
            public_port
                .and_then(|public_port| public_port.http_port)
                .unwrap_or(80)
        }
    } else {
        panel
            .public
            .as_ref()
            .and_then(|public| public.port)
            .or(panel.port)
            .unwrap_or(6444)
    };
    let configured_host = panel
        .public
        .as_ref()
        .and_then(|public| public.host.as_deref())
        .or(panel.host.as_deref())
        .unwrap_or("0.0.0.0");
    let tls_domain = tls_enabled.then(|| {
        panel
            .tls
            .as_ref()
            .and_then(|tls| tls.domains.iter().find(|domain| !domain.trim().is_empty()))
            .map(String::as_str)
    });
    let public_host = tls_domain.flatten().unwrap_or(configured_host);
    let scheme = if tls_enabled { "https" } else { "http" };
    format_panel_url(scheme, &browser_host(public_host), port)
}

fn normalize_public_base_url(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if value
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme != "http" && scheme != "https")
    {
        return None;
    }
    let candidate = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else {
        format!("https://{value}")
    };
    let (_, remainder) = candidate.split_once("://")?;
    let authority = remainder.split('/').next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') || authority.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some(candidate.trim_end_matches('/').to_string())
}

fn browser_host(raw: &str) -> String {
    let host = raw.trim().trim_start_matches('[').trim_end_matches(']');
    match host {
        "" | "0.0.0.0" => "127.0.0.1".to_string(),
        "::" => "::1".to_string(),
        _ => host.to_string(),
    }
}

fn split_listen_addr(raw: &str, default_port: u16) -> (String, u16) {
    if let Ok(addr) = raw.trim().parse::<SocketAddr>() {
        return (addr.ip().to_string(), addr.port());
    }
    let Some((host, port)) = raw.trim().rsplit_once(':') else {
        return (raw.trim().to_string(), default_port);
    };
    match port.parse::<u16>() {
        Ok(port) => (host.trim_matches(['[', ']']).to_string(), port),
        Err(_) => (raw.trim().to_string(), default_port),
    }
}

fn format_panel_url(scheme: &str, host: &str, port: u16) -> String {
    let formatted_host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let default_port = (scheme == "http" && port == 80) || (scheme == "https" && port == 443);
    if default_port {
        format!("{scheme}://{formatted_host}")
    } else {
        format!("{scheme}://{formatted_host}:{port}")
    }
}

fn executable_name(base: &'static str) -> &'static str {
    #[cfg(windows)]
    {
        match base {
            "haze-web" => "haze-web.exe",
            "haze-data-ingest" => "haze-data-ingest.exe",
            "haze-cap-ingest" => "haze-cap-ingest.exe",
            "haze-easnet" => "haze-easnet.exe",
            "haze-tts" => "haze-tts.exe",
            "haze-product-render" => "haze-product-render.exe",
            "haze-playlist" => "haze-playlist.exe",
            "haze-webhook" => "haze-webhook.exe",
            "haze-location" => "haze-location.exe",
            "haze-asr" => "haze-asr.exe",
            "haze-ivr" => "haze-ivr.exe",
            "haze-playout-rs" => "haze-playout-rs.exe",
            "haze-cgen" => "haze-cgen.exe",
            "haze-media" => "haze-media.exe",
            _ => base,
        }
    }
    #[cfg(not(windows))]
    {
        base
    }
}

fn resolve_executable(
    host: &ServiceHostConfig,
    configured: Option<&Path>,
    binary: &str,
) -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path) = configured {
        candidates.push(resolve_path(&host.runtime_dir, path));
        candidates.push(resolve_path(&host.app_dir, path));
    }
    candidates.extend([
        host.app_dir.join("bin").join(binary),
        host.runtime_dir.join("bin").join(binary),
        host.app_dir.join(binary),
        host.runtime_dir.join(binary),
        host.app_dir.join("services").join(binary),
        host.runtime_dir.join("services").join(binary),
        host.app_dir.join("go-services").join(binary),
        host.runtime_dir.join("go-services").join(binary),
    ]);

    candidates
        .into_iter()
        .map(|candidate| dunce::simplified(&candidate).to_path_buf())
        .find(|candidate| candidate.is_file())
}

fn embedded_binary(binary: &str) -> Option<&'static [u8]> {
    embedded::EMBEDDED_GO_BINARIES
        .iter()
        .find_map(|(name, bytes)| (*name == binary).then_some(*bytes))
}

fn extract_embedded_executable(host: &ServiceHostConfig, binary: &str) -> Result<Option<PathBuf>> {
    let Some(bytes) = embedded_binary(binary) else {
        return Ok(None);
    };
    if bytes.is_empty() {
        return Ok(None);
    }

    let bin_dir = host.runtime_dir.join(EMBEDDED_BIN_DIR);
    fs::create_dir_all(&bin_dir).with_context(|| {
        format!(
            "failed to create service binary directory {}",
            bin_dir.display()
        )
    })?;
    extract_embedded_support_libraries(&bin_dir)?;
    let target = bin_dir.join(binary);
    let should_write = fs::read(&target)
        .map(|existing| existing.as_slice() != bytes)
        .unwrap_or(true);
    if should_write {
        fs::write(&target, bytes)
            .with_context(|| format!("failed to extract service binary {}", target.display()))?;
        make_executable(&target)?;
    }
    Ok(Some(target))
}

fn extract_embedded_support_libraries(bin_dir: &Path) -> Result<()> {
    for name in embedded_support_library_names() {
        let Some(bytes) = embedded_binary(name) else {
            continue;
        };
        if bytes.is_empty() {
            continue;
        }
        let target = bin_dir.join(name);
        let should_write = fs::read(&target)
            .map(|existing| existing.as_slice() != bytes)
            .unwrap_or(true);
        if should_write {
            fs::write(&target, bytes).with_context(|| {
                format!("failed to extract support library {}", target.display())
            })?;
        }
    }
    Ok(())
}

fn embedded_support_library_names() -> &'static [&'static str] {
    #[cfg(windows)]
    {
        &[
            "libopus-0.dll",
            "libopusfile-0.dll",
            "libogg-0.dll",
            "sherpa-onnx-c-api.dll",
            "sherpa-onnx-cxx-api.dll",
        ]
    }
    #[cfg(not(windows))]
    {
        &[]
    }
}

fn stop_previous_managed_services(status_path: &Path) {
    let Ok(raw) = fs::read_to_string(status_path) else {
        return;
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return;
    };
    let Some(services) = payload
        .get("services")
        .and_then(serde_json::Value::as_object)
    else {
        return;
    };
    for (id, service) in services {
        let status = service
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !matches!(status, "running" | "unknown") {
            continue;
        }
        let pid = service.get("pid").and_then(serde_json::Value::as_u64);
        let executable = service
            .get("executable")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let Some(pid) = pid.and_then(|pid| u32::try_from(pid).ok()) else {
            continue;
        };
        if pid == std::process::id() || !is_known_managed_executable(executable) {
            continue;
        }
        match terminate_pid_tree(pid) {
            Ok(()) => info!(
                "stopped stale {} from previous run (pid {pid})",
                service_label(id)
            ),
            Err(err) => warn!(
                "failed to stop stale {} from previous run (pid {pid}): {err}",
                service_label(id)
            ),
        }
    }
}

fn is_known_managed_executable(path: &str) -> bool {
    let Some(file_name) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let file_name = file_name.to_ascii_lowercase();
    [
        executable_name("haze-web"),
        executable_name("haze-data-ingest"),
        executable_name("haze-cap-ingest"),
        executable_name("haze-location"),
        executable_name("haze-tts"),
        executable_name("haze-product-render"),
        executable_name("haze-playlist"),
        executable_name("haze-webhook"),
        executable_name("haze-asr"),
        executable_name("haze-ivr"),
        executable_name("haze-playout-rs"),
        executable_name("haze-cgen"),
        executable_name("haze-media"),
        executable_name("haze-easnet"),
    ]
    .into_iter()
    .any(|known| known.eq_ignore_ascii_case(&file_name))
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

#[cfg(windows)]
fn managed_command(executable: &Path, _scheduler: ProcessScheduler) -> Command {
    Command::new(executable)
}

#[cfg(not(windows))]
fn managed_command(executable: &Path, scheduler: ProcessScheduler) -> Command {
    if let Some(nice) = scheduler.unix_nice {
        let mut command = Command::new("nice");
        command.args(["-n", &nice.to_string()]);
        command.arg(executable);
        command
    } else {
        Command::new(executable)
    }
}

#[cfg(windows)]
fn configure_managed_process(command: &mut Command, scheduler: ProcessScheduler) {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const ABOVE_NORMAL_PRIORITY_CLASS: u32 = 0x0000_8000;
    const HIGH_PRIORITY_CLASS: u32 = 0x0000_0080;
    const REALTIME_PRIORITY_CLASS: u32 = 0x0000_0100;
    let priority_flag = match scheduler.windows_priority {
        WindowsPriorityClass::Normal => 0,
        WindowsPriorityClass::AboveNormal => ABOVE_NORMAL_PRIORITY_CLASS,
        WindowsPriorityClass::High => HIGH_PRIORITY_CLASS,
        WindowsPriorityClass::Realtime => REALTIME_PRIORITY_CLASS,
    };
    command.creation_flags(CREATE_NEW_PROCESS_GROUP | priority_flag);
}

#[cfg(not(windows))]
fn configure_managed_process(_command: &mut Command, _scheduler: ProcessScheduler) {}

fn configure_managed_service_env(command: &mut Command, spec: &ServiceSpec) {
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    if spec.id == "svc:cgen" {
        if let Some(value) =
            cgen_system_library_path(std::env::var_os("LD_LIBRARY_PATH").as_deref())
        {
            command.env("LD_LIBRARY_PATH", value);
        }
    }
    if !is_go_service_binary(spec.binary) {
        return;
    }
    env_if_unset(command, "MALLOC_ARENA_MAX", "2");
    env_if_unset(command, "GOGC", go_gc_percent(spec.id));
    if let Some(limit) = go_memory_limit(spec.id) {
        env_if_unset(command, "GOMEMLIMIT", limit);
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn cgen_system_library_path(existing: Option<&std::ffi::OsStr>) -> Option<std::ffi::OsString> {
    let mut paths = Vec::<PathBuf>::new();
    for candidate in cgen_system_library_directories() {
        let path = PathBuf::from(candidate);
        if path.is_dir() && !paths.contains(&path) {
            paths.push(path);
        }
    }
    if let Some(existing) = existing {
        for path in std::env::split_paths(existing) {
            if !path.as_os_str().is_empty() && !paths.contains(&path) {
                paths.push(path);
            }
        }
    }
    (!paths.is_empty())
        .then(|| std::env::join_paths(paths).ok())
        .flatten()
}

#[cfg(target_os = "linux")]
fn cgen_system_library_directories() -> &'static [&'static str] {
    &[
        "/lib64",
        "/usr/lib64",
        "/lib/x86_64-linux-gnu",
        "/usr/lib/x86_64-linux-gnu",
        "/lib/aarch64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
        "/lib/arm-linux-gnueabihf",
        "/usr/lib/arm-linux-gnueabihf",
        "/lib",
        "/usr/lib",
    ]
}

#[cfg(target_os = "freebsd")]
fn cgen_system_library_directories() -> &'static [&'static str] {
    &["/lib", "/usr/lib", "/usr/local/lib"]
}

fn env_if_unset(command: &mut Command, key: &str, value: &str) {
    if std::env::var_os(key).is_none() {
        command.env(key, value);
    }
}

fn is_go_service_binary(binary: &str) -> bool {
    let binary = binary.strip_suffix(".exe").unwrap_or(binary);
    matches!(
        binary,
        "haze-web"
            | "haze-data-ingest"
            | "haze-tts"
            | "haze-product-render"
            | "haze-playlist"
            | "haze-webhook"
            | "haze-asr"
            | "haze-ivr"
    )
}

fn go_memory_limit(id: &str) -> Option<&'static str> {
    match id {
        "aux:tts" => Some("384MiB"),
        "svc:ivr" => Some("192MiB"),
        "go:web_gateway" | "go:web_public" | "go:web_admin" => Some("192MiB"),
        "svc:data_ingest" | "svc:product_render" | "svc:playlist" | "svc:webhook" | "svc:asr" => {
            Some("128MiB")
        }
        _ => Some("128MiB"),
    }
}

fn go_gc_percent(id: &str) -> &'static str {
    match id {
        "aux:tts" => "50",
        _ => "75",
    }
}

#[cfg(windows)]
fn apply_managed_process_priority(pid: u32, scheduler: ProcessScheduler, label: &str) {
    use windows_sys::Win32::Foundation::{CloseHandle, GetLastError};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, SetPriorityClass, ABOVE_NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS,
        PROCESS_SET_INFORMATION, REALTIME_PRIORITY_CLASS,
    };

    let priority = match scheduler.windows_priority {
        WindowsPriorityClass::Normal => return,
        WindowsPriorityClass::AboveNormal => ABOVE_NORMAL_PRIORITY_CLASS,
        WindowsPriorityClass::High => HIGH_PRIORITY_CLASS,
        WindowsPriorityClass::Realtime => REALTIME_PRIORITY_CLASS,
    };
    unsafe {
        let handle = OpenProcess(PROCESS_SET_INFORMATION, 0, pid);
        if handle.is_null() {
            warn!(
                "[{label}] failed to open process for priority update: win32 error {}",
                GetLastError()
            );
            return;
        }
        if SetPriorityClass(handle, priority) == 0 {
            warn!(
                "[{label}] failed to apply {} priority: win32 error {}",
                scheduler.windows_priority.as_str(),
                GetLastError()
            );
        }
        let _ = CloseHandle(handle);
    }
}

#[cfg(not(windows))]
fn apply_managed_process_priority(_pid: u32, _scheduler: ProcessScheduler, _label: &str) {}

#[cfg(windows)]
fn terminate_child_tree(child: &mut Child) -> std::io::Result<()> {
    terminate_pid_tree(child.id()).or_else(|_| child.kill())
}

#[cfg(not(windows))]
fn terminate_child_tree(child: &mut Child) -> std::io::Result<()> {
    child.kill()
}

#[cfg(windows)]
fn terminate_pid_tree(pid: u32) -> std::io::Result<()> {
    let status = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() || status.code() == Some(128) {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "taskkill exited with {status}"
        )))
    }
}

#[cfg(not(windows))]
fn terminate_pid_tree(pid: u32) -> std::io::Result<()> {
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("kill exited with {status}"),
        ))
    }
}

fn load_config_with_overlay(path: &Path, app_dir: &Path, runtime_dir: &Path) -> Result<RootConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read daemon config {}", path.display()))?;
    let raw = expand_env_vars(&raw);
    let mut value: serde_yaml::Value = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse daemon config {}", path.display()))?;

    let settings_file = value
        .get("daemon_settings_file")
        .and_then(serde_yaml::Value::as_str)
        .filter(|raw| !raw.trim().is_empty())
        .unwrap_or(DEFAULT_SETTINGS_FILE);
    let settings_path = crate::runtime_dir::resolve_configured_runtime_path(
        app_dir,
        runtime_dir,
        Path::new(settings_file),
    );
    if settings_path.is_file() {
        let overlay_raw = fs::read_to_string(&settings_path).with_context(|| {
            format!(
                "failed to read daemon settings overlay {}",
                settings_path.display()
            )
        })?;
        let overlay_json: serde_json::Value =
            serde_json::from_str(&overlay_raw).with_context(|| {
                format!(
                    "failed to parse daemon settings overlay {}",
                    settings_path.display()
                )
            })?;
        let overlay_yaml = serde_yaml::to_value(overlay_json)
            .context("failed to convert daemon settings overlay")?;
        merge_yaml(&mut value, overlay_yaml);
    }

    serde_yaml::from_value(value).context("failed to decode daemon service configuration")
}

fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    match (base, overlay) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_yaml(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value;
        }
    }
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

fn pipe_service_output<R>(
    service_id: &'static str,
    stream: &'static str,
    reader: R,
    warn_lines: bool,
) where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines().map_while(std::result::Result::ok) {
            log_service_line(service_id, stream, warn_lines, &line);
        }
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceLogLevel {
    Info,
    Warn,
    Error,
}

fn log_service_line(service_id: &str, stream: &str, warn_lines: bool, raw: &str) {
    let Some((mut level, line)) = normalize_service_log_line(raw, warn_lines) else {
        return;
    };
    if stream == "stderr" && level == ServiceLogLevel::Info {
        level = ServiceLogLevel::Warn;
    }
    let label = service_label(service_id);
    match level {
        ServiceLogLevel::Info => info!("[{label}] {line}"),
        ServiceLogLevel::Warn => warn!("[{label}] {line}"),
        ServiceLogLevel::Error => error!("[{label}] {line}"),
    }
}

fn normalize_service_log_line(raw: &str, warn_lines: bool) -> Option<(ServiceLogLevel, String)> {
    let cleaned = strip_ansi(raw);
    let mut line = cleaned.trim();
    if line.is_empty() {
        return None;
    }

    let mut level = if warn_lines {
        ServiceLogLevel::Warn
    } else {
        ServiceLogLevel::Info
    };

    line = strip_go_log_timestamp(line);
    line = strip_iso_log_timestamp(line);
    if let Some((parsed_level, rest)) = strip_log_level(line) {
        level = parsed_level;
        line = rest;
    }
    line = strip_rust_target_prefix(line);
    line = strip_redundant_service_prefix(line);
    if let Some((parsed_level, rest)) = strip_log_level(line) {
        level = parsed_level;
        line = rest;
    }

    let line = line.trim();
    (!line.is_empty()).then(|| (level, line.to_string()))
}

fn service_label(service_id: &str) -> &str {
    match service_id {
        "go:web_gateway" => "Web panel",
        "go:web_public" => "Public web",
        "go:web_admin" => "Admin web",
        "svc:data_ingest" => "Data ingest",
        "svc:cap_ingest" => "CAP ingest",
        "svc:nws_cap_ingest" => "NWS CAP ingest",
        "aux:tts" => "TTS",
        "svc:product_render" => "Product render",
        "svc:playlist" => "Playlist",
        "svc:webhook" => "Webhook",
        "svc:asr" => "Speech recognition",
        "svc:ivr" => "IVR",
        "svc:playout" => "Playout",
        "svc:cgen" => "CG renderer",
        "svc:easnet" => "EAS NET",
        _ => service_id,
    }
}

fn strip_ansi(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for code in chars.by_ref() {
                if ('@'..='~').contains(&code) {
                    break;
                }
            }
            continue;
        }
        output.push(ch);
    }
    output
}

fn strip_go_log_timestamp(line: &str) -> &str {
    let bytes = line.as_bytes();
    if bytes.len() >= 20
        && digits(bytes, 0..4)
        && bytes[4] == b'/'
        && digits(bytes, 5..7)
        && bytes[7] == b'/'
        && digits(bytes, 8..10)
        && bytes[10] == b' '
        && digits(bytes, 11..13)
        && bytes[13] == b':'
        && digits(bytes, 14..16)
        && bytes[16] == b':'
        && digits(bytes, 17..19)
        && bytes[19].is_ascii_whitespace()
    {
        line[20..].trim_start()
    } else {
        line
    }
}

fn strip_iso_log_timestamp(line: &str) -> &str {
    let bytes = line.as_bytes();
    if bytes.len() < 20
        || !digits(bytes, 0..4)
        || bytes[4] != b'-'
        || !digits(bytes, 5..7)
        || bytes[7] != b'-'
        || !digits(bytes, 8..10)
        || bytes[10] != b'T'
        || !digits(bytes, 11..13)
        || bytes[13] != b':'
        || !digits(bytes, 14..16)
        || bytes[16] != b':'
        || !digits(bytes, 17..19)
    {
        return line;
    }
    let Some(end) = line.find(char::is_whitespace) else {
        return line;
    };
    line[end..].trim_start()
}

fn digits(bytes: &[u8], range: std::ops::Range<usize>) -> bool {
    range
        .filter_map(|index| bytes.get(index))
        .all(u8::is_ascii_digit)
}

fn strip_log_level(line: &str) -> Option<(ServiceLogLevel, &str)> {
    const LEVELS: [(&str, ServiceLogLevel); 6] = [
        ("ERROR", ServiceLogLevel::Error),
        ("CRITICAL", ServiceLogLevel::Error),
        ("WARNING", ServiceLogLevel::Warn),
        ("WARN", ServiceLogLevel::Warn),
        ("INFO", ServiceLogLevel::Info),
        ("DEBUG", ServiceLogLevel::Info),
    ];

    let trimmed = line.trim_start();
    for (prefix, level) in LEVELS {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest
                .strip_prefix(':')
                .or_else(|| rest.strip_prefix(' '))
                .or_else(|| rest.strip_prefix('\t'))?;
            return Some((level, rest.trim_start()));
        }
    }
    None
}

fn strip_rust_target_prefix(line: &str) -> &str {
    let Some((target, rest)) = line.split_once(": ") else {
        return line;
    };
    if target.len() > 96 {
        return line;
    }
    let rust_like_target = target.contains("::") || target.starts_with("haze_") || target == "haze";
    if rust_like_target
        && target
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '-'))
    {
        rest.trim_start()
    } else {
        line
    }
}

fn strip_redundant_service_prefix(line: &str) -> &str {
    const PREFIXES: [&str; 16] = [
        "haze-web: ",
        "haze-web ",
        "haze-data-ingest: ",
        "haze-data-ingest ",
        "haze-cap-ingest: ",
        "haze-cap-ingest ",
        "haze-tts: ",
        "haze-tts ",
        "haze-product-render: ",
        "haze-product-render ",
        "haze-playlist: ",
        "haze-playlist ",
        "haze-asr: ",
        "haze-asr ",
        "haze-ivr: ",
        "haze-ivr ",
    ];
    PREFIXES
        .iter()
        .find_map(|prefix| line.strip_prefix(prefix))
        .unwrap_or(line)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_host() -> ServiceHostConfig {
        ServiceHostConfig {
            app_dir: PathBuf::from("app"),
            runtime_dir: PathBuf::from("runtime"),
            config_path: PathBuf::from("config.yaml"),
        }
    }

    #[test]
    fn location_is_a_known_managed_executable() {
        assert!(is_known_managed_executable(executable_name(
            "haze-location"
        )));
    }

    #[cfg(windows)]
    #[test]
    fn location_uses_the_windows_executable_suffix() {
        assert_eq!(executable_name("haze-location"), "haze-location.exe");
    }

    #[test]
    fn web_gateway_specs_combines_public_and_admin_on_same_port() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    web_gateway:
      enabled: true
      addr: 0.0.0.0:6444
webpanel:
  host: 0.0.0.0
  port: 6444
  public:
    enabled: true
    host: 0.0.0.0
    port: 6444
  admin:
    enabled: true
    host: 0.0.0.0
    port: 6444
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        let web_specs: Vec<&ServiceSpec> = specs
            .iter()
            .filter(|spec| spec.binary == executable_name("haze-web"))
            .collect();
        assert_eq!(web_specs.len(), 1);
        assert_eq!(web_specs[0].id, "go:web_gateway");
        assert!(web_specs[0].args.contains(&"0.0.0.0:6444".to_string()));
        assert!(web_specs[0].args.contains(&"combined".to_string()));
    }

    #[test]
    fn web_gateway_specs_accepts_env_expanded_string_ports() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    web_gateway:
      enabled: true
webpanel:
  host: 0.0.0.0
  port: "6444"
  public:
    enabled: true
    port: "6444"
  admin:
    enabled: true
    port: "6444"
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());
        let web_specs: Vec<&ServiceSpec> = specs
            .iter()
            .filter(|spec| spec.binary == executable_name("haze-web"))
            .collect();

        assert_eq!(web_specs.len(), 1);
        assert_eq!(web_specs[0].id, "go:web_gateway");
        assert!(web_specs[0].args.contains(&"0.0.0.0:6444".to_string()));
        assert!(web_specs[0].args.contains(&"combined".to_string()));
    }

    #[test]
    fn web_gateway_specs_fall_back_to_combined_legacy_addr_without_webpanel() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    web_gateway:
      enabled: true
      addr: 127.0.0.1:9999
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "go:web_gateway");
        assert!(specs[0].args.contains(&"127.0.0.1:9999".to_string()));
        assert!(specs[0].args.contains(&"combined".to_string()));
    }

    #[test]
    fn public_panel_url_uses_loopback_for_a_wildcard_listener() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
webpanel:
  host: 0.0.0.0
  port: 18080
  public:
    enabled: true
    host: 0.0.0.0
    port: 18080
  tls:
    enabled: false
"#,
        )
        .expect("config");

        assert_eq!(
            public_panel_url_from_config(&root),
            "http://127.0.0.1:18080"
        );
    }

    #[test]
    fn public_panel_url_uses_tls_domain_and_public_https_port() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
webpanel:
  public_port:
    enabled: true
    https_port: 443
  public:
    enabled: true
    host: 0.0.0.0
    port: 6444
  tls:
    enabled: true
    domains: [weather.example]
"#,
        )
        .expect("config");

        assert_eq!(
            public_panel_url_from_config(&root),
            "https://weather.example"
        );
    }

    #[test]
    fn public_panel_url_supports_legacy_ipv6_listener() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    web_gateway:
      enabled: true
      addr: "[::]:7444"
"#,
        )
        .expect("config");

        assert_eq!(public_panel_url_from_config(&root), "http://[::1]:7444");
    }

    #[test]
    fn configured_public_base_url_rejects_credentials_and_non_http_schemes() {
        assert_eq!(
            normalize_public_base_url("radio.example"),
            Some("https://radio.example".to_string())
        );
        assert_eq!(
            normalize_public_base_url("https://user:pass@example.com"),
            None
        );
        assert_eq!(normalize_public_base_url("ftp://example.com"), None);
    }

    #[test]
    fn ivr_service_uses_separate_pcm_media_bridge() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    ivr:
      enabled: true
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "svc:ivr");
        assert_eq!(specs[0].binary, executable_name("haze-ivr"));
        assert!(specs[0].args.contains(&"--bridge".to_string()));
        assert!(specs[0].args.contains(&"--media-bridge".to_string()));
    }

    #[test]
    fn asr_service_uses_host_event_bridge() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    asr:
      enabled: true
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "svc:asr");
        assert_eq!(specs[0].binary, executable_name("haze-asr"));
        assert!(specs[0].args.contains(&"--bridge".to_string()));
    }

    #[test]
    fn tts_service_resolves_configured_persistent_cache_dir() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    tts:
      enabled: true
      cache_dir: runtime/cache/tts-custom
      cache_max_bytes: 123456
      cache_max_entries: 321
      workers: 6
"#,
        )
        .expect("config");

        let host = ServiceHostConfig {
            app_dir: PathBuf::from("/srv/haze-weather-radio"),
            runtime_dir: PathBuf::from("/srv/haze-weather-radio/runtime"),
            config_path: PathBuf::from("/srv/haze-weather-radio/config.yaml"),
        };
        let specs = service_specs(&root, &host);
        let tts = specs
            .iter()
            .find(|spec| spec.binary == executable_name("haze-tts"))
            .expect("tts service spec");

        assert!(tts.args.windows(2).any(|args| {
            args == [
                "--cache-dir".to_string(),
                host.runtime_dir
                    .join("cache/tts-custom")
                    .to_string_lossy()
                    .into_owned(),
            ]
        }));
        assert!(tts
            .args
            .windows(2)
            .any(|args| { args == ["--cache-max-bytes".to_string(), "123456".to_string()] }));
        assert!(tts
            .args
            .windows(2)
            .any(|args| { args == ["--cache-max-entries".to_string(), "321".to_string()] }));
        assert!(tts
            .args
            .windows(2)
            .any(|args| { args == ["--workers".to_string(), "6".to_string()] }));
    }

    #[test]
    fn tts_service_resolves_default_persistent_cache_dir_in_legacy_layout() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  go:
    enabled: true
    tts:
      enabled: true
"#,
        )
        .expect("config");
        let host = ServiceHostConfig {
            app_dir: PathBuf::from("/srv/haze-weather-radio"),
            runtime_dir: PathBuf::from("/srv/haze-weather-radio"),
            config_path: PathBuf::from("/srv/haze-weather-radio/config.yaml"),
        };

        let specs = service_specs(&root, &host);
        let tts = specs
            .iter()
            .find(|spec| spec.binary == executable_name("haze-tts"))
            .expect("tts service spec");

        assert!(tts.args.windows(2).any(|args| {
            args == [
                "--cache-dir".to_string(),
                host.runtime_dir
                    .join("runtime/cache/tts")
                    .to_string_lossy()
                    .into_owned(),
            ]
        }));
    }

    #[test]
    fn rust_playout_is_the_only_playout_service() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  rust:
    playout:
      enabled: true
      alert_poll: 250ms
  go:
    enabled: true
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "svc:playout");
        assert_eq!(specs[0].binary, executable_name("haze-playout-rs"));
        assert!(specs[0].args.contains(&"250ms".to_string()));
    }

    #[test]
    fn rust_cgen_service_uses_managed_xml_config() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  rust:
    cgen:
      enabled: true
      config: managed/configs/cgen.xml
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "svc:cgen");
        assert_eq!(specs[0].binary, executable_name("haze-cgen"));
        assert!(specs[0].args.contains(&"--cgen".to_string()));
        assert!(specs[0]
            .args
            .contains(&"managed/configs/cgen.xml".to_string()));
        assert_eq!(
            specs[0].args,
            vec![
                "--config".to_string(),
                test_host().config_path.to_string_lossy().into_owned(),
                "--cgen".to_string(),
                "managed/configs/cgen.xml".to_string(),
                "--bridge".to_string(),
                String::new(),
            ]
        );
    }

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    #[test]
    fn cgen_prefers_host_gpu_libraries_before_portable_runtime_libraries() {
        let portable_bin = PathBuf::from("/portable/haze/bin");
        let portable_plugins = portable_bin.join("gstreamer-1.0");
        let existing = std::env::join_paths([portable_bin.clone(), portable_plugins.clone()])
            .expect("portable library path");

        let configured = cgen_system_library_path(Some(&existing))
            .expect("CGEN library path contains system and portable entries");
        let paths = std::env::split_paths(&configured).collect::<Vec<_>>();
        let portable_index = paths
            .iter()
            .position(|path| path == &portable_bin)
            .expect("portable bin retained");

        assert!(paths[..portable_index]
            .iter()
            .any(|path| cgen_system_library_directories()
                .iter()
                .any(|candidate| path == Path::new(candidate))));
        assert!(paths.contains(&portable_plugins));
    }

    #[test]
    fn rust_media_service_uses_separate_pcm_media_bridge() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  rust:
    media:
      enabled: true
      addr: 127.0.0.1:8097
      backend: auto
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "svc:media");
        assert_eq!(specs[0].binary, executable_name("haze-media"));
        assert!(specs[0].args.contains(&"--bridge".to_string()));
        assert!(specs[0].args.contains(&"--media-bridge".to_string()));
        assert!(specs[0].args.contains(&"127.0.0.1:8097".to_string()));
    }

    #[test]
    fn rust_cap_service_uses_native_binary_and_tcp_mode() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  rust:
    cap_ingest:
      enabled: true
      shadow: false
      mode: tcp
      url: tcp://streaming1.naad-adna.pelmorex.com:8080
      fallback_url: tcp://streaming2.naad-adna.pelmorex.com:8080
      interval: 5s
      startup_seed: true
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        let rust = specs
            .iter()
            .find(|spec| spec.id == "svc:cap_ingest")
            .expect("rust cap spec");
        assert_eq!(rust.binary, executable_name("haze-cap-ingest"));
        assert!(!rust.args.contains(&"--shadow".to_string()));
        assert!(rust.args.contains(&"tcp".to_string()));
        assert!(rust.args.contains(&"5s".to_string()));
    }

    #[test]
    fn rust_easnet_service_uses_managed_xml_config() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  rust:
    easnet:
      enabled: true
      config: managed/configs/easnet.xml
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "svc:easnet");
        assert_eq!(specs[0].binary, executable_name("haze-easnet"));
        assert!(specs[0].args.contains(&"--easnet".to_string()));
        assert!(specs[0]
            .args
            .contains(&"managed/configs/easnet.xml".to_string()));
    }

    #[test]
    fn nws_cap_atom_source_does_not_append_query_filters() {
        let root: RootConfig = serde_yaml::from_str(
            r#"
services:
  rust:
    cap_ingest:
      enabled: true
      mode: tcp
      interval: 30s
      timeout: 15s
cap:
  nws_cap:
    enabled: true
    sources:
      - id: nws_api
        url: https://api.weather.gov/alerts/active.atom
        queries:
          - severity=extreme,severe,moderate
"#,
        )
        .expect("config");

        let specs = service_specs(&root, &test_host());
        let nws = specs
            .iter()
            .find(|spec| spec.id == "svc:nws_cap_ingest")
            .expect("nws cap spec");

        let url_index = nws
            .args
            .iter()
            .position(|arg| arg == "--url")
            .expect("url arg")
            + 1;
        let mode_index = nws
            .args
            .iter()
            .position(|arg| arg == "--mode")
            .expect("mode arg")
            + 1;
        assert_eq!(nws.args[mode_index], "atom");
        assert_eq!(
            nws.args[url_index],
            "https://api.weather.gov/alerts/active.atom"
        );
    }

    #[test]
    fn service_log_normalizer_strips_go_timestamp_and_binary_name() {
        let (level, line) = normalize_service_log_line(
            "2026/06/16 21:13:04 haze-web combined listening on 0.0.0.0:6444",
            false,
        )
        .expect("normalized line");

        assert_eq!(level, ServiceLogLevel::Info);
        assert_eq!(line, "combined listening on 0.0.0.0:6444");
    }

    #[test]
    fn service_log_normalizer_strips_ansi_rust_prefix() {
        let (level, line) = normalize_service_log_line(
            "\u{1b}[2m2026-06-16T01:35:27.612989Z\u{1b}[0m \u{1b}[33mWARN\u{1b}[0m haze_playout_rs::sinks: UDP sink warning",
            false,
        )
        .expect("normalized line");

        assert_eq!(level, ServiceLogLevel::Warn);
        assert_eq!(line, "UDP sink warning");
    }

    #[test]
    fn service_labels_are_operator_readable() {
        assert_eq!(service_label("go:web_gateway"), "Web panel");
        assert_eq!(service_label("svc:product_render"), "Product render");
        assert_eq!(service_label("svc:cgen"), "CG renderer");
        assert_eq!(service_label("svc:cap_ingest"), "CAP ingest");
        assert_eq!(service_label("unknown-service"), "unknown-service");
    }
}
