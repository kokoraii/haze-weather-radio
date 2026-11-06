//! Where each managed service runs, from its `host` and `port` settings.
//!
//! A host that names this machine (blank, `localhost`, a loopback address, or
//! an address of a local interface) launches the service here as before. Any
//! other host makes the daemon connect to a remote instance at `host:port`
//! (see remote_service.rs). Only services with a remote mode can do that; the
//! rest are refused with a clear status instead of silently running locally.

use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;

use crate::remote_service::{is_local_host, LinkAdapter, RemoteEndpoint};
use crate::remote_tts::{TtsLink, TtsLinkSettings};

/// The `host` and `port` keys every service section accepts.
#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PlacementConfig {
    pub host: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::go_services::deserialize_optional_port"
    )]
    pub port: Option<u16>,
}

/// A service with a remote mode, and what its link needs.
#[derive(Clone)]
pub(crate) enum RemoteService {
    Tts(TtsLinkSettings),
}

impl RemoteService {
    /// A fresh adapter for one link. Links never share adapter state.
    pub(crate) fn adapter(&self, runtime_dir: &Path) -> Arc<dyn LinkAdapter> {
        match self {
            Self::Tts(settings) => {
                Arc::new(TtsLink::new(runtime_dir.to_path_buf(), settings.clone()))
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum Placement {
    Local,
    Remote {
        endpoint: RemoteEndpoint,
        service: RemoteService,
    },
    /// Configured for a remote host it cannot use. The reason is shown in status.
    Refused(String),
}

/// Decides where a service runs. `remote` is the service's remote mode, if it
/// has one.
pub(crate) fn resolve(
    config: Option<&PlacementConfig>,
    label: &str,
    remote: Option<RemoteService>,
) -> Placement {
    let host = config
        .and_then(|config| config.host.as_deref())
        .map(str::trim)
        .unwrap_or_default();
    if is_local_host(host) {
        return Placement::Local;
    }
    let Some(port) = config
        .and_then(|config| config.port)
        .filter(|port| *port != 0)
    else {
        return Placement::Refused(format!("remote host {host} needs a port"));
    };
    let Some(service) = remote else {
        return Placement::Refused(format!("{label} cannot run on a remote host yet"));
    };
    Placement::Remote {
        endpoint: RemoteEndpoint {
            host: host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_string(),
            port,
        },
        service,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn config(host: &str, port: Option<u16>) -> PlacementConfig {
        PlacementConfig {
            host: Some(host.to_string()),
            port,
        }
    }

    fn tts() -> RemoteService {
        RemoteService::Tts(TtsLinkSettings {
            readers: PathBuf::from("managed/configs/readers.xml"),
            dictionary: PathBuf::from("managed/dictionary.json"),
            out_dir: PathBuf::from("runtime/audio/tts"),
            provider: "auto".to_string(),
            language: "en-CA".to_string(),
            timezone: "Local".to_string(),
            timeout: "60s".to_string(),
        })
    }

    #[test]
    fn local_or_missing_hosts_launch_here() {
        assert!(matches!(
            resolve(None, "TTS", Some(tts())),
            Placement::Local
        ));
        assert!(matches!(
            resolve(Some(&config("127.0.0.1", Some(41810))), "TTS", Some(tts())),
            Placement::Local
        ));
        assert!(matches!(
            resolve(Some(&config("localhost", None)), "Playlist", None),
            Placement::Local
        ));
    }

    #[test]
    fn remote_hosts_connect_when_the_service_supports_it() {
        let placement = resolve(Some(&config("192.0.2.22", Some(41810))), "TTS", Some(tts()));
        let Placement::Remote { endpoint, .. } = placement else {
            panic!("expected a remote placement");
        };
        assert_eq!(endpoint.to_string(), "192.0.2.22:41810");
    }

    #[test]
    fn remote_hosts_are_refused_without_a_port_or_a_remote_mode() {
        let Placement::Refused(reason) =
            resolve(Some(&config("192.0.2.22", None)), "TTS", Some(tts()))
        else {
            panic!("expected a refusal");
        };
        assert!(reason.contains("needs a port"), "{reason}");
        let Placement::Refused(reason) =
            resolve(Some(&config("192.0.2.22", Some(9000))), "Playlist", None)
        else {
            panic!("expected a refusal");
        };
        assert_eq!(reason, "Playlist cannot run on a remote host yet");
    }
}
