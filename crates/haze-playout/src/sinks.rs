use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use rtrb::{Producer, RingBuffer};

use crate::config::{resolve_path, FeedConfig, LoadedConfig, OutputNodeConfig};
use haze_media::{pcm16_samples, read_i16};

const ENCODER_WORKER_QUEUE_CAPACITY: usize = 16;
const AUDIO_DEVICE_MAX_BUFFER_MS: usize = 120;
const AUDIO_DEVICE_MAX_PENDING_MS: usize = 60;
const OPUS_BITRATE_KBPS: u32 = 32;

pub(crate) trait Sink: Send {
    fn name(&self) -> &str;
    fn write(&mut self, pcm: &[u8]) -> Result<()>;
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn sinks_for_feed(cfg: &LoadedConfig, feed: &FeedConfig) -> Vec<Box<dyn Sink>> {
    let mut sinks: Vec<Box<dyn Sink>> = Vec::new();
    if feed.output.webrtc.is_enabled() {
        tracing::info!(
            feed_id = feed.id,
            "playout WebRTC sink enabled through shared media bridge"
        );
    }
    if feed.output.udp.is_enabled() {
        push_sink(&mut sinks, udp_sink(cfg, feed, &feed.output.udp));
    }
    if feed.output.rtp.is_enabled() {
        push_sink(&mut sinks, rtp_sink(cfg, feed, &feed.output.rtp));
    }
    if feed.output.file.is_enabled() {
        push_sink(&mut sinks, file_sink(cfg, feed, &feed.output.file));
    }
    if feed.output.audio_device.is_enabled() {
        push_sink(
            &mut sinks,
            audio_device_sink(cfg, feed, &feed.output.audio_device),
        );
    }
    for (label, node) in [
        ("icecast", &feed.output.icecast),
        ("rtmp", &feed.output.rtmp),
        ("srt", &feed.output.srt),
        ("rtsp", &feed.output.rtsp),
    ] {
        if node.is_enabled() {
            tracing::warn!(
                feed_id = feed.id,
                sink = label,
                "sink requires the GStreamer backend and is not started by this first Rust playout pass"
            );
        }
    }
    sinks
}

fn push_sink(sinks: &mut Vec<Box<dyn Sink>>, sink: Result<Option<Box<dyn Sink>>>) {
    match sink {
        Ok(Some(sink)) => sinks.push(sink),
        Ok(None) => {}
        Err(err) => tracing::warn!("playout sink setup failed: {err}"),
    }
}

fn udp_sink(
    cfg: &LoadedConfig,
    feed: &FeedConfig,
    node: &OutputNodeConfig,
) -> Result<Option<Box<dyn Sink>>> {
    let format = node.format.trim().to_ascii_lowercase();
    let codec = normalize_codec(&node.acodec);
    if is_raw_pcm(&format, &codec) {
        let address = socket_addr(node, 8898)?;
        let sink = RawUdpSink::new(format!("udp:{address}"), address)?;
        return Ok(Some(Box::new(sink)));
    }
    if is_rtp_g711(&format, &codec) {
        let address = socket_addr(node, 8898)?;
        let sink = RtpG711Sink::new(
            format!("udp-rtp:{address}"),
            address,
            g711_kind(&codec),
            cfg.root.playout.sample_rate,
            cfg.root.playout.channels,
        )?;
        return Ok(Some(Box::new(sink)));
    }
    tracing::info!(
        feed_id = feed.id,
        format,
        codec,
        "playout UDP encoded sink using external encoder backend"
    );
    let address = socket_addr(node, 8898)?;
    let bitrate_kbps = encoded_bitrate_kbps(&node.bitrate_kbps, &codec);
    let sink = EncoderUdpSink::new(
        format!("udp-encoded:{address}"),
        address,
        format,
        codec,
        bitrate_kbps,
        fallback_text(&cfg.root.services.rust.playout.ffmpeg, "ffmpeg"),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    )?;
    Ok(Some(Box::new(sink)))
}

fn rtp_sink(
    cfg: &LoadedConfig,
    feed: &FeedConfig,
    node: &OutputNodeConfig,
) -> Result<Option<Box<dyn Sink>>> {
    let codec = normalize_codec(&node.acodec);
    if !matches!(codec.as_str(), "pcmu" | "mulaw" | "pcma" | "alaw") {
        tracing::warn!(
            feed_id = feed.id,
            codec,
            "RTP sink currently supports PCMU/PCMA natively; other codecs require GStreamer"
        );
        return Ok(None);
    }
    let address = socket_addr(node, 8899)?;
    let sink = RtpG711Sink::new(
        format!("rtp:{address}"),
        address,
        g711_kind(&codec),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    )?;
    Ok(Some(Box::new(sink)))
}

fn file_sink(
    cfg: &LoadedConfig,
    feed: &FeedConfig,
    node: &OutputNodeConfig,
) -> Result<Option<Box<dyn Sink>>> {
    let configured = if node.path.trim().is_empty() {
        format!("runtime/audio/playout/{}-capture.wav", feed.id)
    } else {
        node.path.trim().to_string()
    };
    let path = resolve_path(&cfg.base_dir, &configured);
    let sink = WavFileSink::new(
        path,
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    )?;
    Ok(Some(Box::new(sink)))
}

fn audio_device_sink(
    cfg: &LoadedConfig,
    feed: &FeedConfig,
    node: &OutputNodeConfig,
) -> Result<Option<Box<dyn Sink>>> {
    let sink = AudioDeviceSink::new(
        format!("audio_device:{}", feed.id),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
        node.device.trim(),
    )?;
    Ok(Some(Box::new(sink)))
}

struct RawUdpSink {
    name: String,
    socket: UdpSocket,
    address: SocketAddr,
}

impl RawUdpSink {
    fn new(name: String, address: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to bind UDP sink socket")?;
        socket
            .connect(address)
            .with_context(|| format!("failed to connect UDP sink to {address}"))?;
        tracing::info!("playout UDP raw sink started: {address}");
        Ok(Self {
            name,
            socket,
            address,
        })
    }
}

impl Sink for RawUdpSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn write(&mut self, pcm: &[u8]) -> Result<()> {
        self.socket
            .send(pcm)
            .with_context(|| format!("failed to write UDP sink {}", self.address))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum G711Kind {
    MuLaw,
    ALaw,
}

struct RtpG711Sink {
    name: String,
    socket: UdpSocket,
    address: SocketAddr,
    kind: G711Kind,
    sequence: u16,
    timestamp: u32,
    ssrc: u32,
    source_rate: u32,
    source_channels: u16,
}

impl RtpG711Sink {
    fn new(
        name: String,
        address: SocketAddr,
        kind: G711Kind,
        source_rate: u32,
        source_channels: u16,
    ) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").context("failed to bind RTP sink socket")?;
        socket
            .connect(address)
            .with_context(|| format!("failed to connect RTP sink to {address}"))?;
        tracing::info!("playout RTP G.711 sink started: {address}");
        Ok(Self {
            name,
            socket,
            address,
            kind,
            sequence: 0,
            timestamp: 0,
            ssrc: randomish_ssrc(),
            source_rate,
            source_channels: source_channels.max(1),
        })
    }
}

impl Sink for RtpG711Sink {
    fn name(&self) -> &str {
        &self.name
    }

    fn write(&mut self, pcm: &[u8]) -> Result<()> {
        let payload = pcm16_to_g711(
            pcm,
            self.source_rate,
            self.source_channels,
            8_000,
            self.kind,
        );
        let mut packet = Vec::with_capacity(12 + payload.len());
        packet.push(0x80);
        packet.push(match self.kind {
            G711Kind::MuLaw => 0,
            G711Kind::ALaw => 8,
        });
        packet.extend_from_slice(&self.sequence.to_be_bytes());
        packet.extend_from_slice(&self.timestamp.to_be_bytes());
        packet.extend_from_slice(&self.ssrc.to_be_bytes());
        packet.extend_from_slice(&payload);
        self.socket
            .send(&packet)
            .with_context(|| format!("failed to write RTP sink {}", self.address))?;
        self.sequence = self.sequence.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(payload.len() as u32);
        Ok(())
    }
}

struct WavFileSink {
    name: String,
    writer: Option<hound::WavWriter<BufWriter<File>>>,
}

struct EncoderUdpSink {
    name: String,
    tx: Option<SyncSender<Vec<u8>>>,
    worker: Option<JoinHandle<()>>,
    dropped_chunks: u64,
}

struct EncoderWorker {
    address: SocketAddr,
    format: String,
    codec: String,
    bitrate_kbps: u32,
    encoder_path: String,
    sample_rate: u32,
    channels: u16,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
}

impl EncoderUdpSink {
    #[allow(clippy::too_many_arguments)]
    fn new(
        name: String,
        address: SocketAddr,
        format: String,
        codec: String,
        bitrate_kbps: u32,
        encoder_path: String,
        sample_rate: u32,
        channels: u16,
    ) -> Result<Self> {
        let mut worker = EncoderWorker {
            address,
            format: normalize_container_format(&format),
            codec: normalize_encoder_codec(&codec),
            bitrate_kbps: bitrate_kbps.max(8),
            encoder_path,
            sample_rate,
            channels: channels.max(1),
            child: None,
            stdin: None,
        };
        worker.start()?;
        tracing::info!(
            address = %worker.address,
            format = worker.format,
            codec = worker.codec,
            "playout UDP encoded sink started"
        );
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(ENCODER_WORKER_QUEUE_CAPACITY);
        let worker_name = name.clone();
        let worker = thread::spawn(move || worker.run(rx, worker_name));
        Ok(Self {
            name,
            tx: Some(tx),
            worker: Some(worker),
            dropped_chunks: 0,
        })
    }
}

impl EncoderWorker {
    fn run(mut self, rx: Receiver<Vec<u8>>, sink_name: String) {
        for pcm in rx {
            if let Err(err) = self.write_blocking(&pcm) {
                tracing::warn!(
                    sink = sink_name.as_str(),
                    "encoder worker write failed: {err}"
                );
            }
        }
        self.stop();
    }

    fn start(&mut self) -> Result<()> {
        self.stop();
        let url = format!("udp://{}?pkt_size=1316&buffer_size=65536", self.address);
        let sample_rate = self.sample_rate.to_string();
        let channels = self.channels.to_string();
        let bitrate = format!("{}k", self.bitrate_kbps);
        let mut command = Command::new(&self.encoder_path);
        command
            .args([
                "-hide_banner",
                "-nostats",
                "-loglevel",
                "warning",
                "-f",
                "s16le",
                "-ar",
                &sample_rate,
                "-ac",
                &channels,
                "-i",
                "pipe:0",
                "-vn",
                "-c:a",
                &self.codec,
                "-b:a",
                &bitrate,
                "-f",
                &self.format,
                "-flush_packets",
                "1",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
                &url,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        configure_encoder_process(&mut command);
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start external encoder {}", self.encoder_path))?;
        if let Some(stderr) = child.stderr.take() {
            spawn_encoder_stderr_logger(
                stderr,
                self.address,
                self.format.clone(),
                self.codec.clone(),
            );
        }
        self.stdin = child.stdin.take();
        self.child = Some(child);
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.flush();
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn write_blocking(&mut self, pcm: &[u8]) -> Result<()> {
        if self.stdin.is_none() {
            self.start()?;
        }
        let Some(stdin) = self.stdin.as_mut() else {
            anyhow::bail!("external encoder stdin is unavailable");
        };
        if let Err(err) = stdin.write_all(pcm) {
            self.start()?;
            let Some(stdin) = self.stdin.as_mut() else {
                anyhow::bail!("external encoder restart left stdin unavailable");
            };
            stdin
                .write_all(pcm)
                .with_context(|| format!("external encoder write failed after restart: {err}"))?;
        }
        Ok(())
    }
}

impl Sink for EncoderUdpSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn write(&mut self, pcm: &[u8]) -> Result<()> {
        let Some(tx) = self.tx.as_ref() else {
            anyhow::bail!("external encoder worker is stopped");
        };
        match tx.try_send(pcm.to_vec()) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.dropped_chunks = self.dropped_chunks.saturating_add(1);
                if self.dropped_chunks == 1 || self.dropped_chunks.is_multiple_of(250) {
                    tracing::warn!(
                        sink = self.name.as_str(),
                        dropped_chunks = self.dropped_chunks,
                        "external encoder is behind; dropping stale transmitter audio"
                    );
                }
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => {
                anyhow::bail!("external encoder worker stopped")
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.tx.take();
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::warn!(
                    sink = self.name.as_str(),
                    "external encoder worker panicked"
                );
            }
        }
        Ok(())
    }
}

impl WavFileSink {
    fn new(path: PathBuf, sample_rate: u32, channels: u16) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let spec = hound::WavSpec {
            channels,
            sample_rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let writer = hound::WavWriter::create(&path, spec)
            .with_context(|| format!("failed to create WAV sink {}", path.display()))?;
        Ok(Self {
            name: format!("file:{}", path.display()),
            writer: Some(writer),
        })
    }
}

impl Sink for WavFileSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn write(&mut self, pcm: &[u8]) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            for sample in pcm16_samples(pcm) {
                writer.write_sample(sample)?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.finalize()?;
        }
        Ok(())
    }
}

struct AudioDeviceSink {
    name: String,
    producer: Producer<i16>,
    _stream: cpal::Stream,
    samples_per_second: usize,
    dropped_samples: u64,
}

impl AudioDeviceSink {
    fn new(name: String, sample_rate: u32, channels: u16, device_name: &str) -> Result<Self> {
        let host = cpal::default_host();
        let device = if device_name.is_empty() {
            host.default_output_device()
                .context("no default audio output device")?
        } else {
            host.output_devices()?
                .find(|device| {
                    device
                        .description()
                        .map(|description| {
                            description
                                .name()
                                .to_ascii_lowercase()
                                .contains(&device_name.to_ascii_lowercase())
                        })
                        .unwrap_or(false)
                })
                .with_context(|| format!("audio output device not found: {device_name}"))?
        };
        let config = cpal::StreamConfig {
            channels,
            sample_rate,
            buffer_size: cpal::BufferSize::Default,
        };
        let samples_per_second = device_samples_per_second(sample_rate, channels);
        let (producer, mut consumer) =
            RingBuffer::<i16>::new(device_queue_capacity_samples(sample_rate, channels));
        let max_pending_samples =
            samples_for_duration(samples_per_second, AUDIO_DEVICE_MAX_PENDING_MS);
        let stream = device.build_output_stream(
            config,
            move |output: &mut [i16], _| {
                let stale_samples = device_stale_samples_to_drop(
                    consumer.slots(),
                    output.len(),
                    max_pending_samples,
                );
                for _ in 0..stale_samples {
                    if consumer.pop().is_err() {
                        break;
                    }
                }
                let (_, remaining) = consumer.pop_partial_slice(output);
                remaining.fill(0);
            },
            move |err| tracing::warn!("audio device sink error: {err}"),
            None,
        )?;
        stream.play()?;
        tracing::info!("playout audio device sink started");
        Ok(Self {
            name,
            producer,
            _stream: stream,
            samples_per_second,
            dropped_samples: 0,
        })
    }
}

impl Sink for AudioDeviceSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn write(&mut self, pcm: &[u8]) -> Result<()> {
        let available = self.producer.slots();
        let sample_count = pcm.len() / 2;
        let pushed = available.min(sample_count);
        for bytes in pcm.chunks_exact(2).take(pushed) {
            if self.producer.push(read_i16(bytes)).is_err() {
                break;
            }
        }
        let dropped = sample_count.saturating_sub(pushed);
        if dropped > 0 {
            let previous = self.dropped_samples;
            self.dropped_samples = self.dropped_samples.saturating_add(dropped as u64);
            if previous == 0
                || previous / (self.samples_per_second as u64)
                    < self.dropped_samples / (self.samples_per_second as u64)
            {
                tracing::warn!(
                    sink = self.name,
                    dropped_ms = dropped.saturating_mul(1000) / self.samples_per_second,
                    total_dropped_ms =
                        self.dropped_samples.saturating_mul(1000) / self.samples_per_second as u64,
                    "audio device queue is full; dropping stale playout audio"
                );
            }
        }
        Ok(())
    }
}

fn device_samples_per_second(sample_rate: u32, channels: u16) -> usize {
    usize::try_from(sample_rate)
        .unwrap_or(usize::MAX)
        .saturating_mul(usize::from(channels.max(1)))
        .max(1)
}

fn samples_for_duration(samples_per_second: usize, duration_ms: usize) -> usize {
    samples_per_second.saturating_mul(duration_ms) / 1000
}

fn device_queue_capacity_samples(sample_rate: u32, channels: u16) -> usize {
    samples_for_duration(
        device_samples_per_second(sample_rate, channels),
        AUDIO_DEVICE_MAX_BUFFER_MS,
    )
    .max(1)
}

fn device_stale_samples_to_drop(
    pending_samples: usize,
    output_samples: usize,
    max_pending_samples: usize,
) -> usize {
    pending_samples.saturating_sub(max_pending_samples.saturating_add(output_samples))
}

fn socket_addr(node: &OutputNodeConfig, default_port: u16) -> Result<SocketAddr> {
    let host = if node.ip.trim().is_empty() {
        "127.0.0.1"
    } else {
        node.ip.trim()
    };
    let port = if node.port.trim().is_empty() {
        default_port
    } else {
        node.port
            .trim()
            .parse::<u16>()
            .with_context(|| format!("invalid output port {}", node.port))?
    };
    format!("{host}:{port}")
        .to_socket_addrs()
        .with_context(|| format!("invalid socket address {host}:{port}"))?
        .next()
        .with_context(|| format!("socket address did not resolve: {host}:{port}"))
}

fn is_raw_pcm(format: &str, codec: &str) -> bool {
    matches!(format, "" | "raw" | "s16le" | "pcm16le" | "pcm_s16le")
        || matches!(codec, "pcm" | "pcm16le" | "pcm_s16le")
}

fn is_rtp_g711(format: &str, codec: &str) -> bool {
    matches!(format, "rtp" | "rtp_pcmu" | "rtp_pcma")
        && matches!(codec, "pcmu" | "mulaw" | "pcma" | "alaw")
}

fn normalize_codec(codec: &str) -> String {
    match codec.trim().to_ascii_lowercase().as_str() {
        "" | "opus" | "libopus" => "opus".to_string(),
        "pcm" | "pcm_s16le" | "s16le" => "pcm_s16le".to_string(),
        "ulaw" | "mulaw" | "pcmu" => "pcmu".to_string(),
        "alaw" | "pcma" => "pcma".to_string(),
        other => other.to_string(),
    }
}

fn g711_kind(codec: &str) -> G711Kind {
    match codec {
        "pcma" | "alaw" => G711Kind::ALaw,
        _ => G711Kind::MuLaw,
    }
}

fn normalize_encoder_codec(codec: &str) -> String {
    match codec.trim().to_ascii_lowercase().as_str() {
        "" | "opus" | "libopus" => "libopus".to_string(),
        "aac" => "aac".to_string(),
        "mp3" | "libmp3lame" => "libmp3lame".to_string(),
        "pcm" | "pcm_s16le" | "s16le" => "pcm_s16le".to_string(),
        other => other.to_string(),
    }
}

fn normalize_container_format(format: &str) -> String {
    match format.trim().to_ascii_lowercase().as_str() {
        "" | "mpegts" | "mpeg-ts" | "ts" | "rtp_mpegts" => "mpegts".to_string(),
        other => other.to_string(),
    }
}

fn int_text(raw: &str, fallback: u32) -> u32 {
    raw.trim().parse::<u32>().unwrap_or(fallback)
}

fn encoded_bitrate_kbps(raw: &str, codec: &str) -> u32 {
    let fallback = if normalize_codec(codec) == "opus" {
        OPUS_BITRATE_KBPS
    } else {
        32
    };
    int_text(raw, fallback)
}

fn fallback_text(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

#[cfg(windows)]
fn configure_encoder_process(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn configure_encoder_process(_command: &mut Command) {}

fn spawn_encoder_stderr_logger<R>(reader: R, address: SocketAddr, format: String, codec: String)
where
    R: std::io::Read + Send + 'static,
{
    thread::spawn(move || {
        for line in BufReader::new(reader)
            .lines()
            .map_while(std::result::Result::ok)
        {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            tracing::warn!(
                address = %address,
                format = format.as_str(),
                codec = codec.as_str(),
                "UDP encoder: {line}"
            );
        }
    });
}

fn pcm16_to_g711(
    data: &[u8],
    in_rate: u32,
    in_channels: u16,
    out_rate: u32,
    kind: G711Kind,
) -> Vec<u8> {
    let in_channels = usize::from(in_channels.max(1));
    let source_frames = data.len() / (in_channels * 2);
    if source_frames == 0 {
        return vec![
            match kind {
                G711Kind::MuLaw => 0xff,
                G711Kind::ALaw => 0xd5,
            };
            160
        ];
    }
    let out_frames = ((source_frames as f64 * f64::from(out_rate) / f64::from(in_rate.max(1)))
        .round() as usize)
        .max(1);
    let mut out = Vec::with_capacity(out_frames);
    for frame in 0..out_frames {
        let source = (frame as u64 * u64::from(in_rate.max(1)) / u64::from(out_rate)) as usize;
        let source = source.min(source_frames - 1);
        let mut mixed = 0i32;
        for channel in 0..in_channels {
            let offset = (source * in_channels + channel) * 2;
            mixed += i32::from(read_i16(&data[offset..offset + 2]));
        }
        let sample = (mixed / in_channels as i32) as i16;
        out.push(match kind {
            G711Kind::MuLaw => linear_to_mulaw(sample),
            G711Kind::ALaw => linear_to_alaw(sample),
        });
    }
    out
}

fn linear_to_mulaw(sample: i16) -> u8 {
    const BIAS: i32 = 0x84;
    const CLIP: i32 = 32635;
    let mut pcm = i32::from(sample);
    let sign = if pcm < 0 {
        pcm = -pcm;
        0x80
    } else {
        0
    };
    pcm = pcm.min(CLIP) + BIAS;
    let mut exponent = 7;
    let mut mask = 0x4000;
    while exponent > 0 && pcm & mask == 0 {
        exponent -= 1;
        mask >>= 1;
    }
    let mantissa = (pcm >> (exponent + 3)) & 0x0f;
    !(sign | (exponent << 4) | mantissa) as u8
}

fn linear_to_alaw(sample: i16) -> u8 {
    let mut pcm = i32::from(sample);
    let mask = if pcm >= 0 { 0xd5 } else { 0x55 };
    if pcm < 0 {
        pcm = -pcm - 1;
    }
    let encoded = if pcm < 256 {
        pcm >> 4
    } else {
        let mut exponent = 7;
        let mut exp_mask = 0x4000;
        while exponent > 0 && pcm & exp_mask == 0 {
            exponent -= 1;
            exp_mask >>= 1;
        }
        ((exponent << 4) | ((pcm >> (exponent + 3)) & 0x0f)) & 0x7f
    };
    (encoded as u8) ^ mask
}

fn randomish_ssrc() -> u32 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    (now as u32) ^ ((now >> 32) as u32) ^ std::process::id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::hint::black_box;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    #[test]
    fn mulaw_silence_is_ff() {
        assert_eq!(linear_to_mulaw(0), 0xff);
    }

    #[test]
    fn rtp_payload_resamples_to_8khz() {
        let pcm = vec![0; 1_920];
        let payload = pcm16_to_g711(&pcm, 48_000, 1, 8_000, G711Kind::MuLaw);
        assert_eq!(payload.len(), 160);
    }

    #[test]
    fn unsupported_encoded_udp_is_skipped() {
        let node = OutputNodeConfig {
            enabled: Some("true".to_string()),
            format: "mpegts".to_string(),
            acodec: "libopus".to_string(),
            ..OutputNodeConfig::default()
        };
        assert!(!is_raw_pcm(&node.format, &normalize_codec(&node.acodec)));
    }

    #[test]
    fn opus_encoded_output_defaults_to_32_kbps() {
        assert_eq!(encoded_bitrate_kbps("", "libopus"), 32);
        assert_eq!(encoded_bitrate_kbps("32", "libopus"), 32);
        assert_eq!(encoded_bitrate_kbps("", "aac"), 32);
    }

    #[test]
    fn audio_device_queue_is_bounded_to_120ms() {
        assert_eq!(device_queue_capacity_samples(48_000, 1), 5_760);
        assert_eq!(device_queue_capacity_samples(48_000, 2), 11_520);
    }

    #[test]
    fn audio_device_callback_drops_stale_backlog_before_filling_output() {
        assert_eq!(device_stale_samples_to_drop(3_840, 960, 2_880), 0);
        assert_eq!(device_stale_samples_to_drop(7_680, 960, 2_880), 3_840);
    }

    #[test]
    fn audio_device_callback_keeps_recent_samples_after_a_stall() {
        let (mut producer, mut consumer) = RingBuffer::new(8);
        for sample in 1..=6 {
            producer.push(sample).expect("ring buffer space");
        }

        let stale = device_stale_samples_to_drop(consumer.slots(), 2, 2);
        for _ in 0..stale {
            consumer.pop().expect("stale sample");
        }
        let mut output = [0; 2];
        let (_, remaining) = consumer.pop_partial_slice(&mut output);
        remaining.fill(0);

        assert_eq!(output, [3, 4]);
        assert_eq!(consumer.slots(), 2);
    }

    #[test]
    #[ignore = "microbenchmark, run with --ignored"]
    fn bench_mutex_device_callback_queue() {
        let buffer = Arc::new(Mutex::new(VecDeque::with_capacity(96_000)));
        let producer = Arc::clone(&buffer);
        let chunk = vec![0x5a; 1_920];
        let start = Instant::now();

        for _ in 0..500 {
            producer.lock().expect("producer lock").extend(&chunk);
            let mut pending = buffer.lock().expect("callback lock");
            for _ in 0..960 {
                black_box(pending.pop_front().unwrap_or(0));
                black_box(pending.pop_front().unwrap_or(0));
            }
        }

        eprintln!(
            "mutex device callback queue: {} us for 500 x 20 ms blocks",
            start.elapsed().as_micros()
        );
    }

    #[test]
    #[ignore = "microbenchmark, run with --ignored"]
    fn bench_spsc_device_callback_queue() {
        let (mut producer, mut consumer) = RingBuffer::<i16>::new(96_000);
        let chunk = vec![0x5a; 1_920];
        let mut output = vec![0i16; 960];
        let start = Instant::now();

        for _ in 0..500 {
            for bytes in chunk.chunks_exact(2).take(producer.slots()) {
                producer.push(read_i16(bytes)).expect("ring buffer space");
            }
            let (_, remaining) = consumer.pop_partial_slice(&mut output);
            remaining.fill(0);
            black_box(&output);
        }

        eprintln!(
            "SPSC device callback queue: {} us for 500 x 20 ms blocks",
            start.elapsed().as_micros()
        );
    }
}
