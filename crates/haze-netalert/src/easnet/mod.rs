//! The legacy DASDEC-style EAS NET bridge, formerly the haze-easnet service.
//!
//! It only runs when `services.rust.netalert.easnet.enabled` is set in
//! `config.yaml` and `easnet.xml` does not disable itself. A failure here is
//! logged and never takes the network alert service down with it.

mod config;
mod message;
mod outbound;
mod server;
mod transport;

use std::future::Future;
use std::path::Path;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::bridge::Bridge;
use server::AudioRegistry;

/// EAS NET events keep the retired service's source name so existing
/// consumers and log searches still recognise them.
pub const EASNET_SOURCE: &str = "haze-easnet";
const AIRING_QUEUE: usize = 64;

pub struct EasNet {
    airings: mpsc::Sender<Value>,
}

impl EasNet {
    /// Starts the listeners and outbound sender. `None` when the XML disables EAS NET.
    pub async fn start(base_dir: &Path, xml: &Path, bridge: Bridge) -> Result<Option<Self>> {
        let config = config::load(base_dir, xml)?;
        if !config.enabled {
            info!("EAS NET disabled by {}", xml.display());
            return Ok(None);
        }
        let registry = AudioRegistry::default();
        if config.listener.enabled {
            spawn_logged(
                "EAS NET listener",
                server::run_listener(config.clone(), bridge.clone()),
            );
        }
        if config.audio.enabled {
            spawn_logged(
                "EAS NET audio server",
                server::run_audio_server(config.audio.clone(), registry.clone()),
            );
        }
        bridge
            .publish(json!({
                "type": "easnet.status",
                "source": EASNET_SOURCE,
                "data": {
                    "status": "ready",
                    "listener": config.listener.addr,
                    "audio_server": config.audio.addr,
                    "peers": config.peers.iter().filter(|peer| peer.enabled).count(),
                }
            }))
            .await?;
        let (airings, rx) = mpsc::channel(AIRING_QUEUE);
        spawn_logged(
            "EAS NET sender",
            outbound::run(config, bridge, registry, rx),
        );
        Ok(Some(Self { airings }))
    }

    /// Queues a `cap.alert.audio.ready` airing for peers.
    pub fn offer(&self, event: &Value) {
        if self.airings.try_send(event.clone()).is_err() {
            warn!("EAS NET sender is backed up; dropped an airing");
        }
    }
}

fn spawn_logged(what: &'static str, task: impl Future<Output = Result<()>> + Send + 'static) {
    tokio::spawn(async move {
        if let Err(err) = task.await {
            error!("{what} stopped: {err:#}");
        }
    });
}
