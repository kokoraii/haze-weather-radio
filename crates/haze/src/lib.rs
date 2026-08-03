mod daemon_services;
mod go_services;
mod host_bridge;
mod runtime_dir;
mod same_cli;
#[allow(dead_code)]
mod same_core;
mod signals;
#[cfg(any(windows, target_os = "linux"))]
mod tray;
#[cfg(windows)]
mod windows_service_host;

use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Local;
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use tracing::{info, Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

const DAEMON_LOCK_FILE: &str = "runtime/state/haze.pid";

/// Command line options for the Haze daemon host.
#[derive(Debug, Parser)]
#[command(name = "haze", version, about = "Haze Weather Radio host")]
pub struct DaemonArgs {
    #[command(subcommand)]
    pub command: Option<DaemonCommand>,

    /// Path to the Haze YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    pub config: PathBuf,

    /// Runtime directory containing config, logs, data, audio, and managed state.
    #[arg(long)]
    pub workdir: Option<PathBuf>,

    /// Override the Haze log level.
    #[arg(short = 'l', long)]
    pub log_level: Option<String>,

    /// Run under the Windows Service Control Manager.
    #[cfg(windows)]
    #[arg(long, hide = true)]
    pub service: bool,

    /// Windows service name used when registering with the Service Control Manager.
    #[cfg(windows)]
    #[arg(long, hide = true, default_value = "HazeWeatherRadio")]
    pub service_name: String,

    /// Initialize the host runtime and exit without starting long-running services.
    #[arg(long = "host-smoke", alias = "runtime-smoke")]
    pub host_smoke: bool,
}

#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    /// SAME tools.
    Same {
        #[command(subcommand)]
        command: same_cli::SameCommand,
    },
}

#[cfg(windows)]
pub fn run_windows_service(args: DaemonArgs) -> Result<()> {
    windows_service_host::run(args)
}

#[derive(Debug, Clone)]
pub(crate) struct ServiceHostConfig {
    pub app_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub config_path: PathBuf,
}

/// Run the Haze daemon host.
///
/// # Errors
///
/// Returns an error when the working directory cannot be selected, the bundled
/// runtime cannot be initialized, or Haze exits with an exception.
pub fn run(args: DaemonArgs) -> Result<()> {
    if let Some(command) = args.command {
        return match command {
            DaemonCommand::Same { command } => same_cli::run(command),
        };
    }

    let layout = runtime_dir::resolve(args.workdir.as_deref())?;
    let config_path = normalize_config_path(&layout.runtime_dir, &args.config);
    let host_exe = env::current_exe().context("failed to resolve Haze host executable")?;

    env::set_current_dir(&layout.runtime_dir).with_context(|| {
        format!(
            "failed to enter Haze runtime directory {}",
            layout.runtime_dir.display()
        )
    })?;
    load_dotenv_for_runtime(&layout.runtime_dir)?;
    env::set_var("CONFIG_PATH", &config_path);
    env::set_var("HAZE_HOST_RUNTIME", "haze");
    env::set_var("HAZE_HOST_EXE", &host_exe);
    env::set_var("HAZE_APP_DIR", &layout.app_dir);
    env::set_var("HAZE_RUNTIME_DIR", &layout.runtime_dir);
    if let Some(level) = args.log_level.as_deref() {
        env::set_var("LOG_LEVEL", level.to_uppercase());
    }

    let _instance_guard = if args.host_smoke {
        None
    } else {
        Some(acquire_runtime_instance_lock(
            &layout.app_dir,
            &layout.runtime_dir,
        )?)
    };
    let _log_guard = init_tracing(args.log_level.as_deref(), &layout.runtime_dir);
    let mut host_bridge = host_bridge::HostBridge::start()?;
    env::set_var("HAZE_HOST_BRIDGE_ADDR", host_bridge.addr());
    env::set_var("HAZE_LOG_BRIDGE_ADDR", host_bridge.addr());
    let service_events = host_bridge.take_events();
    let mut media_bridge = host_bridge::HostBridge::start()?;
    env::set_var("HAZE_MEDIA_BRIDGE_ADDR", media_bridge.addr());
    let media_events = media_bridge.take_events();
    thread::spawn(move || while media_events.recv().is_ok() {});

    let host = ServiceHostConfig {
        app_dir: layout.app_dir,
        runtime_dir: layout.runtime_dir,
        config_path,
    };

    info!(
        "starting Haze services: config={}, app={}, runtime={}",
        host.config_path.display(),
        host.app_dir.display(),
        host.runtime_dir.display()
    );

    if args.host_smoke {
        info!("host smoke check completed");
        return Ok(());
    }
    #[cfg(windows)]
    {
        if !args.service {
            signals::install_shutdown_handler()?;
        }
    }
    #[cfg(not(windows))]
    signals::install_shutdown_handler()?;
    let mut daemon_services = daemon_services::DaemonServices::start(
        &host,
        host_bridge.publisher(),
        media_bridge.publisher(),
    )?;
    let mut go_services = go_services::GoServiceSupervisor::start(&host)?;
    #[cfg(windows)]
    let desktop_tray = if args.service {
        None
    } else {
        tray::TrayController::start()
    };
    #[cfg(target_os = "linux")]
    let desktop_tray = tray::TrayController::start();
    wait_for_shutdown(
        &host,
        &mut daemon_services,
        &mut go_services,
        host_bridge.publisher(),
        service_events,
        #[cfg(any(windows, target_os = "linux"))]
        desktop_tray.as_ref(),
    )
}

fn wait_for_shutdown(
    host: &ServiceHostConfig,
    daemon_services: &mut daemon_services::DaemonServices,
    go_services: &mut go_services::GoServiceSupervisor,
    publisher: Sender<Value>,
    service_events: Receiver<Value>,
    #[cfg(any(windows, target_os = "linux"))] desktop_tray: Option<&tray::TrayController>,
) -> Result<()> {
    info!("Haze host services running");
    while !signals::shutdown_requested() {
        #[cfg(any(windows, target_os = "linux"))]
        if let Some(desktop_tray) = desktop_tray {
            while let Some(command) = desktop_tray.try_recv() {
                match command {
                    tray::TrayCommand::OpenPublicPanel => {
                        match go_services::public_panel_url(host) {
                            Ok(url) => match tray::open_public_panel(&url) {
                                Ok(()) => info!("opened public panel at {url}"),
                                Err(err) => tracing::warn!("{err:#}"),
                            },
                            Err(err) => tracing::warn!(
                                "failed to resolve the public panel address: {err:#}"
                            ),
                        }
                    }
                    tray::TrayCommand::RestartAllServices => {
                        let _ = publisher.send(json!({
                            "type": "service.restart_all.started",
                            "source": "haze-tray",
                        }));
                        let restarted = go_services.restart_all();
                        let _ = publisher.send(json!({
                            "type": "service.restart_all.completed",
                            "source": "haze-tray",
                            "data": { "service_count": restarted },
                        }));
                        info!("desktop tray restarted {restarted} managed services");
                    }
                    tray::TrayCommand::Exit => {
                        info!("desktop tray requested Haze shutdown");
                        signals::request_shutdown();
                        break;
                    }
                }
            }
        }
        if signals::shutdown_requested() {
            break;
        }
        while let Ok(event) = service_events.try_recv() {
            if go_services.handle_control_event(&event) {
                go_services.poll_children();
            }
        }
        go_services.poll_children();
        thread::sleep(Duration::from_millis(250));
    }
    info!("shutdown requested; stopping managed services");
    let _ = publisher.send(json!({
        "type": "system.shutdown",
        "source": "haze",
    }));
    daemon_services.shutdown();
    go_services.shutdown();
    Ok(())
}

fn init_tracing(log_level: Option<&str>, runtime_dir: &Path) -> Option<WorkerGuard> {
    let fallback = log_level.unwrap_or("info");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback));
    let log_dir = runtime_dir.join("logs");
    migrate_legacy_log_names(&log_dir);
    let file_appender = daily_log_appender(&log_dir)?;
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let console_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_target(false)
        .event_format(ConsoleEventFormatter);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_timer(LocalSystemTime)
        .with_writer(file_writer);

    match tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .try_init()
    {
        Ok(()) => Some(guard),
        Err(_) => None,
    }
}

fn daily_log_appender(log_dir: &Path) -> Option<tracing_appender::rolling::RollingFileAppender> {
    tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("haze")
        .filename_suffix("log")
        .build(log_dir)
        .ok()
}

fn migrate_legacy_log_names(log_dir: &Path) {
    let Ok(entries) = fs::read_dir(log_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(date) = name.strip_prefix("haze.log.") else {
            continue;
        };
        if chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err() {
            continue;
        }
        let destination = log_dir.join(format!("haze.{date}.log"));
        if !destination.exists() {
            let _ = fs::rename(entry.path(), destination);
        }
    }
}

struct LocalSystemTime;

impl FormatTime for LocalSystemTime {
    fn format_time(&self, writer: &mut Writer<'_>) -> fmt::Result {
        write!(writer, "{}", Local::now().format("%Y-%m-%d %-I:%M:%S %p"))
    }
}

struct BlueLocalSystemTime;

impl FormatTime for BlueLocalSystemTime {
    fn format_time(&self, writer: &mut Writer<'_>) -> fmt::Result {
        write!(
            writer,
            "\x1b[34m{}\x1b[0m",
            Local::now().format("%Y-%m-%d %-I:%M:%S %p")
        )
    }
}

struct ConsoleEventFormatter;

impl<S, N> FormatEvent<S, N> for ConsoleEventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let level = event.metadata().level();

        BlueLocalSystemTime.format_time(&mut writer)?;
        write!(writer, "  {level:<5} ")?;

        if let Some(style) = console_level_style(level) {
            write!(writer, "{style}")?;
        }

        ctx.format_fields(writer.by_ref(), event)?;

        if console_level_style(level).is_some() {
            write!(writer, "\x1b[0m")?;
        }

        writeln!(writer)
    }
}

fn console_level_style(level: &Level) -> Option<&'static str> {
    match *level {
        Level::INFO => Some("\x1b[90m"),
        Level::WARN => Some("\x1b[33m"),
        Level::ERROR => Some("\x1b[1;31m"),
        _ => None,
    }
}

fn normalize_config_path(workdir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        dunce::simplified(path).to_path_buf()
    } else {
        dunce::simplified(&workdir.join(path)).to_path_buf()
    }
}

struct RuntimeInstanceGuard {
    path: PathBuf,
    pid: u32,
}

impl Drop for RuntimeInstanceGuard {
    fn drop(&mut self) {
        if read_lock_pid(&self.path) == Some(self.pid) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn acquire_runtime_instance_lock(
    app_dir: &Path,
    runtime_dir: &Path,
) -> Result<RuntimeInstanceGuard> {
    let path = runtime_dir::resolve_configured_runtime_path(
        app_dir,
        runtime_dir,
        Path::new(DAEMON_LOCK_FILE),
    );
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create Haze instance lock directory {}",
                parent.display()
            )
        })?;
    }

    let pid = std::process::id();
    for _ in 0..2 {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                writeln!(file, "{pid}").with_context(|| {
                    format!("failed to write Haze instance lock {}", path.display())
                })?;
                file.flush().with_context(|| {
                    format!("failed to flush Haze instance lock {}", path.display())
                })?;
                return Ok(RuntimeInstanceGuard { path, pid });
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                if let Some(existing_pid) = read_lock_pid(&path) {
                    if process_owns_runtime(existing_pid, runtime_dir) {
                        anyhow::bail!(
                            "another Haze host is already running for runtime {} (pid {}). Stop it before starting another instance.",
                            runtime_dir.display(),
                            existing_pid
                        );
                    }
                }
                fs::remove_file(&path).with_context(|| {
                    format!(
                        "failed to remove stale Haze instance lock {}",
                        path.display()
                    )
                })?;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to acquire Haze instance lock {}", path.display())
                });
            }
        }
    }

    anyhow::bail!(
        "failed to acquire Haze instance lock {}; another process is starting",
        path.display()
    )
}

fn read_lock_pid(path: &Path) -> Option<u32> {
    let raw = fs::read_to_string(path).ok()?;
    parse_lock_pid(&raw)
}

fn parse_lock_pid(raw: &str) -> Option<u32> {
    raw.lines().next()?.trim().parse().ok()
}

fn load_dotenv_for_runtime(runtime_dir: &Path) -> Result<usize> {
    let path = runtime_dir.join(".env");
    if !path.is_file() {
        let example_path = runtime_dir.join(".env.example");
        if example_path.is_file() {
            fs::copy(&example_path, &path).with_context(|| {
                format!(
                    "failed to create {} from {}",
                    path.display(),
                    example_path.display()
                )
            })?;
            eprintln!(
                "WARN .env file not found: created {} from {}",
                path.display(),
                example_path.display()
            );
        } else {
            eprintln!(
                "WARN .env file not found and no .env.example is available: {}",
                path.display()
            );
            return Ok(0);
        }
    }

    let raw = fs::read_to_string(&path)
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
        if key.is_empty() || env::var_os(key).is_some() {
            continue;
        }
        env::set_var(key, parse_env_value(value));
        loaded += 1;
    }
    Ok(loaded)
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

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, FALSE, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code = 0;
        let queried = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        queried != FALSE && exit_code == STILL_ACTIVE as u32
    }
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(unix)]
fn process_owns_runtime(pid: u32, runtime_dir: &Path) -> bool {
    if pid == std::process::id() {
        return true;
    }
    if !process_alive(pid) {
        return false;
    }

    let cmdline = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(cmdline) => cmdline,
        Err(_) => return true,
    };
    let args = parse_proc_cmdline(&cmdline);
    if !args.first().is_some_and(|arg| command_is_haze(arg)) {
        return false;
    }

    let runtime = dunce::simplified(runtime_dir).to_string_lossy();
    args.iter().any(|arg| {
        *arg == runtime.as_ref()
            || arg.strip_prefix("--workdir=").is_some_and(|value| {
                dunce::simplified(Path::new(value)) == dunce::simplified(runtime_dir)
            })
    }) || args.windows(2).any(|pair| {
        pair[0] == "--workdir"
            && dunce::simplified(Path::new(pair[1])) == dunce::simplified(runtime_dir)
    })
}

#[cfg(unix)]
fn command_is_haze(command: &str) -> bool {
    Path::new(command)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "haze")
}

#[cfg(unix)]
fn parse_proc_cmdline(raw: &[u8]) -> Vec<&str> {
    raw.split(|byte| *byte == 0)
        .filter_map(|arg| std::str::from_utf8(arg).ok())
        .filter(|arg| !arg.is_empty())
        .collect()
}

#[cfg(windows)]
fn process_owns_runtime(pid: u32, _runtime_dir: &Path) -> bool {
    process_alive(pid)
}

#[cfg(not(any(unix, windows)))]
fn process_owns_runtime(pid: u32, _runtime_dir: &Path) -> bool {
    process_alive(pid)
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_config_is_resolved_against_workdir() {
        let workdir = PathBuf::from("haze-root");
        let config = normalize_config_path(&workdir, Path::new("config.yaml"));

        assert_eq!(config, PathBuf::from("haze-root").join("config.yaml"));
    }

    #[test]
    fn absolute_config_is_preserved() {
        let absolute = std::env::current_dir()
            .expect("test process should have a current directory")
            .join("prod.yaml");
        let config = normalize_config_path(Path::new("."), &absolute);

        assert_eq!(config, absolute);
    }

    #[test]
    fn lock_pid_parser_reads_first_line() {
        assert_eq!(parse_lock_pid("1234\nextra"), Some(1234));
        assert_eq!(parse_lock_pid(" nope "), None);
        assert_eq!(parse_lock_pid(""), None);
    }

    #[cfg(windows)]
    #[test]
    fn windows_process_liveness_rejects_an_exited_process_with_an_open_handle() {
        let mut child = std::process::Command::new("cmd.exe")
            .args(["/d", "/c", "exit", "0"])
            .spawn()
            .expect("short-lived child should start");
        let pid = child.id();
        child.wait().expect("short-lived child should exit");

        assert!(!process_alive(pid));
        assert!(process_alive(std::process::id()));
    }

    #[test]
    fn daily_logs_keep_log_as_the_filename_extension() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut appender = daily_log_appender(temp.path()).expect("daily log appender");
        writeln!(appender, "test log line").expect("write log line");
        let names = fs::read_dir(temp.path())
            .expect("read logs")
            .flatten()
            .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
            .collect::<Vec<_>>();

        assert!(names
            .iter()
            .any(|name| name.starts_with("haze.") && name.ends_with(".log")));
        assert!(!names.iter().any(|name| name.starts_with("haze.log.")));
    }

    #[test]
    fn legacy_dated_logs_are_migrated_without_overwriting() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::write(temp.path().join("haze.log.2026-07-15"), "legacy").expect("legacy log");
        fs::write(temp.path().join("haze.log.2026-07-16"), "do not replace").expect("old log");
        fs::write(temp.path().join("haze.2026-07-16.log"), "current").expect("new log");
        fs::write(temp.path().join("haze.log.not-a-date"), "keep").expect("unrelated log");

        migrate_legacy_log_names(temp.path());

        assert_eq!(
            fs::read_to_string(temp.path().join("haze.2026-07-15.log")).expect("migrated log"),
            "legacy"
        );
        assert!(!temp.path().join("haze.log.2026-07-15").exists());
        assert_eq!(
            fs::read_to_string(temp.path().join("haze.2026-07-16.log")).expect("new log"),
            "current"
        );
        assert!(temp.path().join("haze.log.2026-07-16").exists());
        assert!(temp.path().join("haze.log.not-a-date").exists());
    }

    #[test]
    fn runtime_instance_lock_blocks_second_host() {
        let temp = tempfile::tempdir().expect("tempdir");
        let first = acquire_runtime_instance_lock(temp.path(), temp.path()).expect("first lock");

        assert!(acquire_runtime_instance_lock(temp.path(), temp.path()).is_err());

        drop(first);
        assert!(acquire_runtime_instance_lock(temp.path(), temp.path()).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_instance_lock_ignores_reused_non_haze_pid() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg("sleep 30")
            .spawn()
            .expect("spawn non-haze child");
        let lock_path = temp.path().join(DAEMON_LOCK_FILE);
        fs::create_dir_all(lock_path.parent().expect("lock parent")).expect("lock dir");
        fs::write(&lock_path, child.id().to_string()).expect("write stale lock");

        let guard = acquire_runtime_instance_lock(temp.path(), temp.path())
            .expect("lock replaces reused pid");

        assert_eq!(read_lock_pid(&lock_path), Some(std::process::id()));
        drop(guard);
        child.kill().ok();
        child.wait().ok();
    }
}
