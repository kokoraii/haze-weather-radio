mod bridge;
mod config;
mod engine;
mod sinks;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(windows)]
use windows_sys::Win32::Media::{timeBeginPeriod, timeEndPeriod};

#[cfg(not(windows))]
use std::process::Command;

#[derive(Debug, Parser)]
#[command(
    name = "haze-playout",
    version,
    about = "Rust playout engine for Haze Weather Radio"
)]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: PathBuf,

    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: String,

    #[arg(long, env = "HAZE_MEDIA_BRIDGE_ADDR", default_value = "")]
    media_bridge: String,

    #[arg(long, default_value = "500ms")]
    alert_poll: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing();
    let _timer_resolution = TimerResolutionGuard::request();
    let options = engine::Options {
        config_path: args.config,
        bridge_addr: args.bridge,
        media_bridge_addr: args.media_bridge,
        alert_poll: parse_duration_ms(&args.alert_poll, Duration::from_millis(500)),
    };
    match parent_pid() {
        Some(pid) => {
            tokio::select! {
                result = engine::run(options) => result,
                () = wait_for_parent_exit(pid) => {
                    tracing::info!("haze parent process {pid} exited; shutting down playout");
                    Ok(())
                }
            }
        }
        None => engine::run(options).await,
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .try_init();
    let media = haze_media::backend_status();
    tracing::info!(
        backend = media.name,
        available = media.available,
        fallback = media.fallback,
        version = media.version.as_deref().unwrap_or("n/a"),
        "media backend ready"
    );
}

struct TimerResolutionGuard;

impl TimerResolutionGuard {
    fn request() -> Self {
        request_timer_resolution();
        Self
    }
}

impl Drop for TimerResolutionGuard {
    fn drop(&mut self) {
        release_timer_resolution();
    }
}

#[cfg(windows)]
fn request_timer_resolution() {
    let result = unsafe { timeBeginPeriod(1) };
    if result == 0 {
        tracing::info!("haze-playout requested 1 ms Windows timer resolution");
    } else {
        tracing::warn!("haze-playout failed to request 1 ms Windows timer resolution: {result}");
    }
}

#[cfg(not(windows))]
fn request_timer_resolution() {}

#[cfg(windows)]
fn release_timer_resolution() {
    unsafe {
        timeEndPeriod(1);
    }
}

#[cfg(not(windows))]
fn release_timer_resolution() {}

fn parse_duration_ms(raw: &str, fallback: Duration) -> Duration {
    let value = raw.trim();
    if value.is_empty() {
        return fallback;
    }
    if let Some(ms) = value.strip_suffix("ms") {
        return ms
            .trim()
            .parse::<u64>()
            .map(Duration::from_millis)
            .unwrap_or(fallback);
    }
    if let Some(seconds) = value.strip_suffix('s') {
        return seconds
            .trim()
            .parse::<u64>()
            .map(Duration::from_secs)
            .unwrap_or(fallback);
    }
    value
        .parse::<u64>()
        .map(Duration::from_millis)
        .unwrap_or(fallback)
}

fn parent_pid() -> Option<u32> {
    std::env::var("HAZE_PARENT_PID")
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|pid| *pid > 0)
}

async fn wait_for_parent_exit(pid: u32) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        if !parent_alive(pid) {
            return;
        }
    }
}

#[cfg(windows)]
fn parent_alive(pid: u32) -> bool {
    use std::ffi::c_void;

    const SYNCHRONIZE: u32 = 0x0010_0000;
    const WAIT_TIMEOUT: u32 = 0x0000_0102;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenProcess(dwDesiredAccess: u32, bInheritHandle: i32, dwProcessId: u32) -> *mut c_void;
        fn WaitForSingleObject(hHandle: *mut c_void, dwMilliseconds: u32) -> u32;
        fn CloseHandle(hObject: *mut c_void) -> i32;
    }

    // Avoid spawning tasklist once a second from the realtime playout process.
    let handle = unsafe { OpenProcess(SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        return false;
    }
    let result = unsafe { WaitForSingleObject(handle, 0) };
    let _ = unsafe { CloseHandle(handle) };
    result == WAIT_TIMEOUT
}

#[cfg(not(windows))]
fn parent_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
