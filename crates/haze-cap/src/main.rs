use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use haze_cap::bridge::EventPublisher;
use haze_cap::poller::{default_atom_urls, Poller, SourceConfig, SourceKind};
use haze_cap::stream::{default_archive_urls, default_stream_urls, NaadsTcpIngest, StreamConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "haze-cap-ingest")]
#[command(about = "Rust CAP Atom ingest for Haze Weather Radio")]
struct Args {
    #[arg(long, default_value = "rust-cap")]
    source_id: String,
    #[arg(long, default_value = "naads")]
    source: String,
    #[arg(long)]
    url: Option<String>,
    #[arg(long)]
    fallback_url: Option<String>,
    #[arg(long, default_value = "auto")]
    mode: String,
    #[arg(long)]
    archive_url: Option<String>,
    #[arg(long)]
    fallback_archive_url: Option<String>,
    #[arg(long, default_value = "5s")]
    interval: String,
    #[arg(long, default_value = "15s")]
    timeout: String,
    #[arg(long, default_value = "haze-weather-radio-rust-cap-ingest/0.1")]
    user_agent: String,
    #[arg(long, default_value_t = false)]
    shadow: bool,
    #[arg(long, default_value = "true")]
    startup_seed: String,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("haze_cap=info".parse()?))
        .with_target(false)
        .init();

    let args = Args::parse();
    let source = SourceKind::from_raw(&args.source);
    let mode = IngestMode::from_raw(&args.mode)?;
    let mut urls = Vec::new();
    if let Some(url) = args.url.as_deref().filter(|value| !value.trim().is_empty()) {
        urls.push(url.trim().to_string());
    }
    if let Some(raw) = args
        .fallback_url
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        urls.extend(split_urls(raw));
    }
    let interval = parse_duration(&args.interval).context("invalid --interval")?;
    let timeout = parse_duration(&args.timeout).context("invalid --timeout")?;
    let startup_seed = parse_bool(&args.startup_seed).context("invalid --startup-seed")?;
    let publisher = EventPublisher::new(args.bridge, args.source_id.clone());
    let replayed = publisher.replay_pending().await?;
    if replayed > 0 {
        info!(
            replayed,
            "replayed pending CAP deliveries from durable journal"
        );
    }

    let run_tcp = matches!(mode, IngestMode::Tcp)
        || matches!(mode, IngestMode::Auto) && source == SourceKind::Naads;
    if run_tcp {
        if urls.is_empty() {
            urls = default_stream_urls();
        }
        let mut archive_urls = Vec::new();
        if let Some(raw) = args
            .archive_url
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            archive_urls.extend(split_urls(raw));
        }
        if let Some(raw) = args
            .fallback_archive_url
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            archive_urls.extend(split_urls(raw));
        }
        if archive_urls.is_empty() {
            archive_urls = default_archive_urls();
        }
        info!(
            source_id = args.source_id,
            source = source.as_str(),
            mode = "tcp",
            shadow = args.shadow,
            urls = ?urls,
            archive_urls = ?archive_urls,
            "starting Rust NAADS TCP ingest"
        );
        let ingest = NaadsTcpIngest::new(
            StreamConfig {
                source_id: args.source_id,
                stream_urls: urls,
                archive_urls,
                timeout,
                user_agent: args.user_agent,
                shadow: args.shadow,
                startup_seed,
            },
            publisher,
        )?;
        return tokio::select! {
            result = ingest.run() => result,
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for ctrl-c")?;
                info!("Rust NAADS TCP ingest shutting down");
                Ok(())
            }
        };
    }

    if urls.is_empty() {
        urls = default_atom_urls(&source);
    }
    if urls.is_empty() {
        return Err(anyhow!("no Atom URL configured for source {}", args.source));
    }

    info!(
        source_id = args.source_id,
        source = source.as_str(),
        mode = "atom",
        shadow = args.shadow,
        interval_ms = interval.as_millis(),
        concurrency = args.concurrency.max(1),
        urls = ?urls,
        "starting Rust CAP ingest"
    );

    let config = SourceConfig {
        id: args.source_id,
        source,
        urls,
        interval,
        timeout,
        user_agent: args.user_agent,
        shadow: args.shadow,
        startup_seed,
        concurrency: args.concurrency.max(1),
    };
    let mut poller = Poller::new(config, publisher)?;

    tokio::select! {
        result = poller.run(args.once) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for ctrl-c")?;
            info!("Rust CAP ingest shutting down");
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IngestMode {
    Auto,
    Tcp,
    Atom,
}

impl IngestMode {
    fn from_raw(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "tcp" | "stream" | "streaming" => Ok(Self::Tcp),
            "atom" | "poll" | "polling" | "georss" | "rss" => Ok(Self::Atom),
            value => Err(anyhow!("unsupported ingest mode {value:?}")),
        }
    }
}

fn split_urls(raw: &str) -> Vec<String> {
    raw.split([',', ';'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        value => Err(anyhow!("expected boolean, got {value:?}")),
    }
}

fn parse_duration(raw: &str) -> Result<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow!("duration is empty"));
    }
    let (number, scale) = if let Some(number) = raw.strip_suffix("ms") {
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
    let value: u64 = number
        .trim()
        .parse()
        .with_context(|| format!("invalid duration number {number:?}"))?;
    Ok(Duration::from_millis(value.saturating_mul(scale)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_haze_duration_strings() {
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
    }
}
