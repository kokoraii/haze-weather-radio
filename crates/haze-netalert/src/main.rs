//! haze-netalert: network alert delivery for Haze.
//!
//! - A WebSocket listener where clients subscribe with an interest list and
//!   receive matching alerts as CAP JSON and/or CAP XML, plus Ogg/Opus audio
//!   (see `docs/netalert.md`).
//! - On-air banner state per feed, published as `banner.state.updated` for
//!   CGEN and pushed to `banner.html` over the same socket.
//! - The legacy EAS NET bridge, off unless enabled in `config.yaml`.

// The EAS NET inbound broadcast payload is one large `json!` literal.
#![recursion_limit = "256"]

mod audio;
mod banner;
mod bridge;
mod config;
mod easnet;
mod facts;
mod geo;
mod hub;
mod interest;
mod protocol;
mod server;
mod speech;
mod util;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::Parser;
use haze_bridge_client::{spawn_event_pump, Options, Registration};
use serde_json::{json, Value};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::bridge::{Bridge, CLIENT_ID, SOURCE, SUBSCRIPTIONS};
use crate::config::Settings;
use crate::easnet::EasNet;
use crate::hub::Hub;

const BRIDGE_EVENT_BUFFER: usize = 1024;
const BANNER_TICK: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
#[command(name = "haze-netalert")]
#[command(
    about = "Network alert WebSocket, banner state, and EAS NET service for Haze Weather Radio"
)]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: PathBuf,
    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("haze_netalert=info".parse()?))
        .with_target(false)
        .init();

    let args = Args::parse();
    let settings = Settings::load(&args.config)?;
    let loaded_env = util::load_local_env(&settings.base_dir)?;
    if loaded_env > 0 {
        info!("loaded {loaded_env} environment variable(s) from local .env");
    }
    let bridge_addr = args.bridge.trim();
    if bridge_addr.is_empty() {
        bail!("--bridge/HAZE_HOST_BRIDGE_ADDR is required");
    }
    let registration = Registration::subscriber(CLIENT_ID, SUBSCRIPTIONS).with_source(SOURCE);
    let (reader, writer) =
        haze_bridge_client::connect(bridge_addr, &Options::default(), Some(&registration)).await?;
    let (mut events, _pump) = spawn_event_pump(reader, BRIDGE_EVENT_BUFFER);
    let bridge = Bridge::new(writer);

    let hub = Hub::new(&settings, bridge.clone());
    let easnet = match &settings.easnet {
        Some(xml) => EasNet::start(&settings.base_dir, xml, bridge.clone()).await?,
        None => None,
    };
    bridge
        .publish(json!({
            "type": "netalert.status",
            "source": SOURCE,
            "data": {"status": "ready", "listener": settings.addr, "easnet": easnet.is_some()}
        }))
        .await?;
    hub.publish_banner_changes().await;

    let server = tokio::spawn(server::run(
        hub.clone(),
        settings.addr.clone(),
        settings.max_clients,
    ));
    let ticker = tokio::spawn({
        let hub = hub.clone();
        async move {
            let mut interval = tokio::time::interval(BANNER_TICK);
            loop {
                interval.tick().await;
                hub.tick().await;
            }
        }
    });

    let dispatch = async {
        while let Some(event) = events.recv().await {
            if bridge.resolve(&event) {
                continue;
            }
            match event.get("type").and_then(Value::as_str) {
                Some("system.shutdown") => return Ok(()),
                Some("cap.alert.audio.ready") => {
                    if let Some(easnet) = &easnet {
                        easnet.offer(&event);
                    }
                }
                _ => {}
            }
            hub.handle_event(&event).await;
        }
        bail!("host bridge closed")
    };

    let result = tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        result = dispatch => result,
        result = server => result?,
    };
    ticker.abort();
    info!("network alert service shutting down");
    result
}
