//! Sending EAS NET messages to peers over raw TCP or SSH stdin.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use russh::{client, ChannelMsg, Disconnect};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::config::{PeerConfig, PeerProtocol, DEFAULT_READ_TIMEOUT};
use super::message::{serialize, EasNetMessage};
use crate::util::fallback;

pub async fn send_to_peer(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    match peer.protocol {
        PeerProtocol::Tcp => send_to_tcp_peer(peer, message).await,
        PeerProtocol::SshStdin => timeout(
            Duration::from_secs(30),
            send_to_ssh_stdin_peer(peer, message),
        )
        .await
        .context("EAS NET native SSH sender timed out")?,
    }
}

async fn send_to_tcp_peer(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    if peer.host.trim().is_empty() {
        bail!("peer host is empty");
    }
    let addr = format!("{}:{}", peer.host, peer.port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect to EAS NET peer {addr}"))?;
    stream.write_all(serialize(message).as_bytes()).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

async fn send_to_ssh_stdin_peer(peer: &PeerConfig, message: &EasNetMessage) -> Result<()> {
    if let Some(upload) = &message.audio_upload {
        let bytes = fs::read(&upload.local_path).await.with_context(|| {
            format!(
                "failed to read EAS NET upload {}",
                upload.local_path.display()
            )
        })?;
        let command = format!("cat > {}", shell_single_quote(&upload.remote_name));
        exec_native_ssh_stdin(peer, &command, bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to upload EAS NET alert audio {}",
                    upload.local_path.display()
                )
            })?;
    }
    exec_native_ssh_stdin(
        peer,
        peer.remote_command.trim(),
        serialize(message).into_bytes(),
    )
    .await
}

async fn exec_native_ssh_stdin(peer: &PeerConfig, command: &str, input: Vec<u8>) -> Result<()> {
    if peer.host.trim().is_empty() {
        bail!("peer host is empty");
    }
    let username = peer_ssh_user(peer);
    let password = peer_ssh_password(peer)?;
    let config = Arc::new(client::Config {
        inactivity_timeout: Some(DEFAULT_READ_TIMEOUT),
        ..Default::default()
    });
    let mut session = client::connect(config, (peer.host.trim(), peer.port), EasNetSshClient {})
        .await
        .with_context(|| format!("failed to connect to EAS NET SSH peer {}", peer.host))?;

    authenticate_native_ssh(&mut session, &username, &password).await?;

    let mut channel = session
        .channel_open_session()
        .await
        .context("failed to open EAS NET SSH session channel")?;
    channel
        .exec(true, command)
        .await
        .context("failed to exec EAS NET remote command")?;
    channel
        .data_bytes(input)
        .await
        .context("failed to write EAS NET message to native SSH channel")?;
    channel
        .eof()
        .await
        .context("failed to send native SSH EOF for EAS NET message")?;

    let mut exit_status = None;
    let mut stderr = String::new();
    while let Some(message) = channel.wait().await {
        match message {
            ChannelMsg::ExitStatus {
                exit_status: status,
            } => {
                exit_status = Some(status);
            }
            ChannelMsg::ExtendedData { data, .. } => {
                stderr.push_str(&String::from_utf8_lossy(&data));
            }
            _ => {}
        }
    }
    session
        .disconnect(Disconnect::ByApplication, "EAS NET send complete", "en")
        .await
        .ok();

    match exit_status {
        Some(0) => Ok(()),
        Some(status) => {
            let stderr = sanitize_ssh_error(&stderr);
            if stderr.is_empty() {
                bail!("EAS NET native SSH sender exited with status {status}");
            }
            bail!("EAS NET native SSH sender exited with status {status}: {stderr}");
        }
        None => bail!("EAS NET native SSH sender closed without an exit status"),
    }
}

async fn authenticate_native_ssh(
    session: &mut client::Handle<EasNetSshClient>,
    username: &str,
    password: &str,
) -> Result<()> {
    let password_auth = session
        .authenticate_password(username, password)
        .await
        .context("native SSH password authentication failed")?;
    if password_auth.success() {
        return Ok(());
    }
    match session
        .authenticate_keyboard_interactive_start(username.to_string(), None::<String>)
        .await
        .context("native SSH keyboard-interactive authentication failed")?
    {
        client::KeyboardInteractiveAuthResponse::Success => Ok(()),
        client::KeyboardInteractiveAuthResponse::InfoRequest { prompts, .. } => {
            let responses = prompts.iter().map(|_| password.to_string()).collect();
            match session
                .authenticate_keyboard_interactive_respond(responses)
                .await
                .context("native SSH keyboard-interactive response failed")?
            {
                client::KeyboardInteractiveAuthResponse::Success => Ok(()),
                _ => bail!("native SSH authentication was rejected"),
            }
        }
        _ => bail!("native SSH authentication was rejected"),
    }
}

fn peer_ssh_user(peer: &PeerConfig) -> String {
    if !peer.user_env.trim().is_empty() {
        if let Ok(value) = std::env::var(peer.user_env.trim()) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    fallback(peer.user.clone(), "root")
}

fn peer_ssh_password(peer: &PeerConfig) -> Result<String> {
    if peer.password_env.trim().is_empty() {
        bail!("EAS NET native SSH peer has no password_env configured");
    }
    let password = std::env::var(peer.password_env.trim())
        .with_context(|| format!("{} is not set", peer.password_env.trim()))?;
    let password = password.trim().to_string();
    if password.is_empty() {
        bail!("{} is empty", peer.password_env.trim());
    }
    Ok(password)
}

#[derive(Clone)]
struct EasNetSshClient;

impl client::Handler for EasNetSshClient {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn sanitize_ssh_error(raw: &str) -> String {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.to_ascii_lowercase().contains("password") {
                "[password-related SSH error redacted]".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}
