use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use haze_location::catalog::CatalogManager;
use haze_location::config;
use haze_location::engine::QueryEngine;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "haze-location")]
#[command(about = "Offline canonical location resolver for Haze Weather Radio")]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: PathBuf,
    #[arg(long)]
    locations: Option<PathBuf>,
    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("haze_location=info".parse()?))
        .with_target(false)
        .init();

    let args = Args::parse();
    if args.bridge.trim().is_empty() {
        anyhow::bail!("--bridge or HAZE_HOST_BRIDGE_ADDR is required");
    }
    let service_config = config::load(&args.config, args.locations.as_deref())?;
    let manager = CatalogManager::load(&service_config)?;
    let engine = QueryEngine::start(manager.clone(), &service_config);
    let shutdown = CancellationToken::new();
    let bridge_shutdown = shutdown.clone();

    info!(
        mode = service_config.rollout_mode,
        workers = service_config.workers,
        queue_size = service_config.queue_size,
        "starting location resolver"
    );
    tokio::select! {
        result = haze_location::bridge::run(
            &args.bridge,
            args.config,
            args.locations,
            manager,
            engine,
            bridge_shutdown,
        ) => result,
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for ctrl-c")?;
            shutdown.cancel();
            info!("location resolver shutting down");
            Ok(())
        }
    }
}
