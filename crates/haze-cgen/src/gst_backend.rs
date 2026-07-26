use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::num::{NonZeroU16, NonZeroU32};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use anyhow::{bail, Result};
use serde_json::{json, Value};
use tokio::sync::watch;
use tokio::time::sleep;
use tokio::time::Duration;
use tracing::{info, warn};

use crate::architecture::{
    AudioRoutingSpec, AudioTopologyMode, AudioTrackId, ChannelLayout, DeinterlaceAlgorithm,
    DeinterlaceBackend, DeinterlaceCadence, DeinterlaceParity, DeinterlaceSpec, FeedId, GainDb,
    MixMatrix, MpegTsPid, ResolvedProgramMapSpec,
};
use crate::audio_routing::AudioGainController;
use crate::config::{EncoderCodecSettings, FeedConfig};
use crate::gst_output_sink::{CpuBgrxFrameHandle, GstOutputSinkFactory};
use crate::input_program_map::{InputProgramMapSnapshot, MpegTsPsiParser, ProgramMappingMode};
use crate::media_pcm::{MediaPcmSubscription, PcmChunk};
use crate::output_workers::{
    AudioPacket, AudioPayload, OutputFanout, OutputWorkerConfig, VideoFrame,
};
use crate::program_mapping::{
    build_gstreamer_program_map, redacted_resolved_program_map_status,
    PlannedAudioTrackAssociation, PlannedTrackAssociations, PlannedVideoTrackAssociation,
    ValidatedGstProgramMap,
};
use crate::source_caps::{
    CapsRational, CapsWriter, LastKnownCapsStore, SourceCaps, SourceFieldOrder, SourceScanMode,
};
use crate::state::RuntimeState;
use crate::wgpu_renderer::{OverlayRenderState, WgpuFrameRenderer};

use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app as gst_app;

const DEFAULT_UDP_BUFFER_BYTES: u32 = 4 * 1024 * 1024;
const GST_BUS_POLL_INTERVAL_MS: u64 = 10;
const OUTPUT_FANOUT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MPEG_TS_VIDEO_CAPS: &str =
    "video/x-h264;video/x-h265;video/mpeg;video/x-vp8;video/x-vp9;image/jpeg";
const MPEG_TS_AUDIO_CAPS: &str = "audio/mpeg;audio/x-ac3;audio/x-eac3;audio/x-opus;audio/x-dts;audio/x-private1-ac3;audio/x-private1-lpcm;audio/x-lpcm";

#[derive(Debug)]
enum PipelineRunExit {
    Clean,
    ReconfigureCaps(SourceCaps),
    ReconfigureProgramMap(InputProgramMapSnapshot),
}

pub(crate) async fn run_supervised(
    feed: FeedConfig,
    state_rx: watch::Receiver<RuntimeState>,
    mut shutdown_rx: watch::Receiver<bool>,
    base_dir: PathBuf,
    status_tx: Option<watch::Sender<Option<Value>>>,
    media_pcm: MediaPcmSubscription,
) -> Result<()> {
    configure_portable_gstreamer_paths();
    gst::init().context("failed to initialize GStreamer")?;
    let restart = restart_backoff_bounds(&feed);
    let mut restart_delay = restart.initial;
    let mut effective_caps_override = None;
    let mut effective_program_map_override = None;
    loop {
        if *shutdown_rx.borrow() {
            info!(feed_id = %feed.id, "gstreamer cgen supervisor shutting down");
            return Ok(());
        }
        let feed_once = feed.clone();
        let state_once = state_rx.clone();
        let shutdown_once = shutdown_rx.clone();
        let base_once = base_dir.clone();
        let status_once = status_tx.clone();
        let media_pcm_once = media_pcm.clone();
        let caps_once = effective_caps_override.clone();
        let program_map_once = effective_program_map_override.clone();
        let output_fanout = spawn_output_fanout(&feed, effective_program_map_override.as_ref())?;
        let output_fanout_once = output_fanout.clone();
        let result = tokio::task::spawn_blocking(move || {
            run_pipeline_once(
                feed_once,
                state_once,
                shutdown_once,
                base_once,
                status_once,
                media_pcm_once,
                caps_once,
                program_map_once,
                output_fanout_once,
            )
        })
        .await
        .context("gstreamer cgen worker panicked")?;
        if let Some(fanout) = output_fanout.as_deref() {
            if tokio::time::timeout(OUTPUT_FANOUT_SHUTDOWN_TIMEOUT, fanout.shutdown())
                .await
                .is_err()
            {
                warn!(
                    feed_id = %feed.id,
                    timeout_ms = OUTPUT_FANOUT_SHUTDOWN_TIMEOUT.as_millis(),
                    "timed out while stopping isolated CGEN output workers"
                );
            }
        }
        match result {
            Ok(PipelineRunExit::Clean) => {
                info!(feed_id = %feed.id, "gstreamer cgen pipeline exited cleanly");
                restart_delay = restart.initial;
            }
            Ok(PipelineRunExit::ReconfigureCaps(caps)) => {
                info!(
                    feed_id = %feed.id,
                    width = caps.width(),
                    height = caps.height(),
                    frame_rate_numerator = caps.frame_rate().numerator(),
                    frame_rate_denominator = caps.frame_rate().denominator(),
                    "rebuilding cgen resources for changed source caps"
                );
                effective_caps_override = Some(caps);
                restart_delay = restart.initial;
                tokio::task::yield_now().await;
                continue;
            }
            Ok(PipelineRunExit::ReconfigureProgramMap(program_map)) => {
                info!(
                    feed_id = %feed.id,
                    transport_stream_id = program_map.transport_stream_id,
                    program_number = program_map.selected_program.program_number.get(),
                    pmt_pid = program_map.selected_program.pmt_pid,
                    "rebuilding cgen outputs for changed input program map"
                );
                effective_program_map_override = Some(program_map);
                restart_delay = restart.initial;
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => warn!(feed_id = %feed.id, "gstreamer cgen pipeline failed: {err:#}"),
        }
        tokio::select! {
            _ = sleep(restart_delay) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    info!(feed_id = %feed.id, "gstreamer cgen supervisor shutdown requested");
                    return Ok(());
                }
            }
        }
        restart_delay = (restart_delay * 2).min(restart.max);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestartBackoff {
    initial: Duration,
    max: Duration,
}

fn restart_backoff_bounds(feed: &FeedConfig) -> RestartBackoff {
    let initial_ms = feed.sync.reconnect_initial_ms.clamp(100, 60_000);
    let max_ms = feed
        .sync
        .reconnect_max_ms
        .max(initial_ms)
        .clamp(initial_ms, 300_000);
    RestartBackoff {
        initial: Duration::from_millis(u64::from(initial_ms)),
        max: Duration::from_millis(u64::from(max_ms)),
    }
}

fn spawn_output_fanout(
    feed: &FeedConfig,
    input_program_map: Option<&InputProgramMapSnapshot>,
) -> Result<Option<Arc<OutputFanout>>> {
    if !feed.has_explicit_outputs() {
        return Ok(None);
    }
    let pipeline_spec = feed.pipeline_spec()?;
    let program_map = effective_resolved_program_map(feed, input_program_map)?;
    let mut worker_config = OutputWorkerConfig::default();
    worker_config.initial_backoff =
        Duration::from_millis(u64::from(feed.sync.reconnect_initial_ms.clamp(100, 60_000)));
    worker_config.maximum_backoff = Duration::from_millis(u64::from(
        feed.sync
            .reconnect_max_ms
            .max(feed.sync.reconnect_initial_ms)
            .clamp(feed.sync.reconnect_initial_ms.max(100), 300_000),
    ));
    let factory = Arc::new(GstOutputSinkFactory::new(program_map));
    let fanout = OutputFanout::spawn(pipeline_spec.outputs, factory, worker_config)
        .context("failed to start isolated CGEN output workers")?;
    Ok(Some(Arc::new(fanout)))
}

fn effective_resolved_program_map(
    feed: &FeedConfig,
    input_program_map: Option<&InputProgramMapSnapshot>,
) -> Result<Option<ResolvedProgramMapSpec>> {
    let pipeline_spec = feed.pipeline_spec()?;
    let Some(template) = pipeline_spec.program_map.as_ref() else {
        return Ok(None);
    };
    let resolved = if feed.program_mapping_mode()? == ProgramMappingMode::Source {
        input_program_map
            .map(|input| input.resolve_program_map(template))
            .unwrap_or_else(|| template.resolve())
    } else {
        template.resolve()
    }?;
    Ok(Some(resolved))
}

#[derive(Debug)]
struct InputProgramMapRuntimeState {
    parser: MpegTsPsiParser,
    detected: Option<InputProgramMapSnapshot>,
}

#[derive(Debug, Clone)]
struct InputProgramMapRuntime {
    mode: ProgramMappingMode,
    effective: Option<InputProgramMapSnapshot>,
    reconfigure_enabled: bool,
    monitor_installed: bool,
    state: Arc<Mutex<InputProgramMapRuntimeState>>,
}

impl InputProgramMapRuntime {
    fn install(
        pipeline: &gst::Pipeline,
        feed: &FeedConfig,
        effective: Option<InputProgramMapSnapshot>,
    ) -> Result<Self> {
        let mode = feed.program_mapping_mode()?;
        let reconfigure_enabled = mode == ProgramMappingMode::Source
            && feed
                .pipeline_spec()?
                .outputs
                .iter()
                .any(|output| output.enabled && output.destination.is_mpeg_ts());
        let state = Arc::new(Mutex::new(InputProgramMapRuntimeState {
            parser: MpegTsPsiParser::default(),
            detected: None,
        }));
        let Some(monitor) = pipeline.by_name("program_ts_monitor") else {
            return Ok(Self {
                mode,
                effective,
                reconfigure_enabled,
                monitor_installed: false,
                state,
            });
        };
        let pad = monitor
            .static_pad("src")
            .context("GStreamer CGEN program transport monitor missing source pad")?;
        let state_for_probe = Arc::clone(&state);
        pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
            let Some(buffer) = info.buffer() else {
                return gst::PadProbeReturn::Ok;
            };
            let Ok(map) = buffer.map_readable() else {
                return gst::PadProbeReturn::Ok;
            };
            if let Ok(mut state) = state_for_probe.lock() {
                if let Some(snapshot) = state.parser.push(map.as_slice()) {
                    state.detected = Some(snapshot);
                }
            }
            gst::PadProbeReturn::Ok
        })
        .context("failed to install MPEG-TS PAT/PMT probe")?;
        Ok(Self {
            mode,
            effective,
            reconfigure_enabled,
            monitor_installed: true,
            state,
        })
    }

    fn reconfigure_request(&self) -> Option<InputProgramMapSnapshot> {
        if !self.reconfigure_enabled {
            return None;
        }
        let detected = self
            .state
            .lock()
            .ok()
            .and_then(|state| state.detected.clone())?;
        (self.effective.as_ref() != Some(&detected)).then_some(detected)
    }

    fn status_value(&self) -> Value {
        let detected = self
            .state
            .lock()
            .ok()
            .and_then(|state| state.detected.clone());
        json!({
            "mode": self.mode.as_str(),
            "monitor_installed": self.monitor_installed,
            "detected": detected.as_ref().map(InputProgramMapSnapshot::status_value),
            "effective": self.effective.as_ref().map(InputProgramMapSnapshot::status_value),
            "effective_origin": if self.mode == ProgramMappingMode::Source && self.effective.is_some() {
                "input"
            } else if self.mode == ProgramMappingMode::Source {
                "automatic_fallback"
            } else {
                self.mode.as_str()
            },
            "reconfigure_enabled": self.reconfigure_enabled,
            "reconfigure_required": self.reconfigure_request().is_some(),
        })
    }
}

fn output_worker_status_value(fanout: Option<&OutputFanout>) -> Value {
    match fanout {
        Some(fanout) => json!({
            "mode": "isolated_workers",
            "outputs": fanout
                .statuses()
                .iter()
                .map(crate::output_workers::OutputStatusSnapshot::status_value)
                .collect::<Vec<_>>(),
        }),
        None => json!({
            "mode": "legacy_single_sink",
            "outputs": [],
        }),
    }
}

fn sync_status_value(feed: &FeedConfig) -> Value {
    json!({
        "hard_reset_ms": feed.sync.hard_reset_ms,
        "max_audio_frames_per_video": feed.sync.max_audio_frames_per_video,
        "source_buffer_ms": feed.sync.source_buffer_ms,
        "reconnect_initial_ms": feed.sync.reconnect_initial_ms,
        "reconnect_max_ms": feed.sync.reconnect_max_ms,
        "status_interval_ms": feed.sync.status_interval_ms,
    })
}

pub(crate) fn gstreamer_catalog_json() -> Result<Value> {
    configure_portable_gstreamer_paths();
    gst::init().context("failed to initialize GStreamer")?;
    let ancillary = crate::ancillary::AncillaryBackendCapabilities::current_gstreamer();
    Ok(json!({
        "formats": gstreamer_muxer_catalog(),
        "video_codecs": gstreamer_encoder_catalog(gst::ElementFactoryType::VIDEO_ENCODER, "video"),
        "audio_codecs": gstreamer_encoder_catalog(gst::ElementFactoryType::AUDIO_ENCODER, "audio"),
        "video_decoders": gstreamer_decoder_catalog(),
        "deinterlacers": gstreamer_deinterlacer_catalog(),
        "input_devices": gstreamer_input_device_catalog()?,
        "input_backends": ["v4l2", "directshow"],
        "capabilities": {
            "audio_topologies": {
                "force_layout": true,
                "preserve_native_tracks": false,
            },
            "ancillary": ancillary.status_value(),
        },
        "gstreamer": {
            "source": "haze-cgen-registry",
            "runtime": "gstreamer-rs",
        },
    }))
}

fn gstreamer_deinterlacer_catalog() -> Vec<Value> {
    let available = |elements: &[&str]| {
        elements
            .iter()
            .all(|element| gst::ElementFactory::find(*element).is_some())
    };
    let entry = |id: &str,
                 label: &str,
                 algorithm: &str,
                 backend: &str,
                 hardware: bool,
                 elements: &[&str],
                 unavailable_reason: &str,
                 parity_control: bool| {
        let is_available = available(elements);
        json!({
            "id": id,
            "label": label,
            "algorithm": algorithm,
            "backend": backend,
            "hardware": hardware,
            "available": is_available,
            "elements": elements,
            "cadences": ["field", "frame"],
            "parity_control": parity_control,
            "reason": if is_available { "" } else { unavailable_reason },
        })
    };
    vec![
        entry(
            "yadif_software",
            "YADIF, software",
            "yadif",
            "software",
            false,
            &["deinterlace"],
            "The GStreamer YADIF element is not installed.",
            true,
        ),
        entry(
            "motion_adaptive_software",
            "Motion adaptive, software",
            "motion_adaptive",
            "software",
            false,
            &["deinterlace"],
            "The GStreamer motion-adaptive deinterlacer is not installed.",
            true,
        ),
        entry(
            "motion_adaptive_vaapi",
            "Motion adaptive, VA-API",
            "motion_adaptive",
            "vaapi",
            true,
            &["vadeinterlace"],
            "VA-API video processing is not available.",
            false,
        ),
        entry(
            "motion_adaptive_quicksync",
            "Motion adaptive, Intel Quick Sync",
            "motion_adaptive",
            "quicksync",
            true,
            &["msdkvpp"],
            "Intel Media SDK video processing is not available.",
            false,
        ),
        entry(
            "motion_adaptive_d3d11",
            "Motion compensated, Direct3D 11",
            "motion_adaptive",
            "d3d11",
            true,
            &["d3d11deinterlace"],
            "Direct3D 11 video processing is not available.",
            false,
        ),
        entry(
            "motion_adaptive_opengl",
            "Motion adaptive, OpenGL",
            "motion_adaptive",
            "opengl",
            true,
            &["glupload", "gldeinterlace", "gldownload"],
            "The GStreamer OpenGL deinterlacer is not available.",
            false,
        ),
        json!({
            "id": "bwdif_software",
            "label": "BWDIF, software",
            "algorithm": "bwdif",
            "backend": "software",
            "hardware": false,
            "available": false,
            "elements": [],
            "cadences": ["field", "frame"],
            "parity_control": true,
            "reason": "FFmpeg BWDIF is installed but no in-process GStreamer libavfilter bridge is available.",
        }),
        json!({
            "id": "yadif_cuda",
            "label": "YADIF, NVIDIA CUDA",
            "algorithm": "yadif",
            "backend": "cuda",
            "hardware": true,
            "available": false,
            "elements": [],
            "cadences": ["field", "frame"],
            "parity_control": false,
            "reason": "No in-process GStreamer YADIF CUDA element is available.",
        }),
        json!({
            "id": "bwdif_cuda",
            "label": "BWDIF, NVIDIA CUDA",
            "algorithm": "bwdif",
            "backend": "cuda",
            "hardware": true,
            "available": false,
            "elements": [],
            "cadences": ["field", "frame"],
            "parity_control": false,
            "reason": "No in-process GStreamer BWDIF CUDA element is available.",
        }),
        json!({
            "id": "bwdif_vulkan",
            "label": "BWDIF, Vulkan",
            "algorithm": "bwdif",
            "backend": "vulkan",
            "hardware": true,
            "available": false,
            "elements": [],
            "cadences": ["field", "frame"],
            "parity_control": false,
            "reason": "No in-process GStreamer BWDIF Vulkan element is available.",
        }),
    ]
}

pub(crate) fn preflight_feed(feed: &FeedConfig, base_dir: &Path) -> Result<Value> {
    configure_portable_gstreamer_paths();
    gst::init().context("failed to initialize GStreamer")?;

    let pipeline_spec = feed.pipeline_spec()?;
    let audio_layout = forced_audio_output_layout(&pipeline_spec.audio)?;
    let plan = GstPipelinePlan::from_feed(feed)?;
    validate_gstreamer_elements(&plan.required_elements)?;
    let element =
        gst::parse::launch(&plan.description).context("failed to parse GStreamer CGEN pipeline")?;
    let pipeline = element
        .downcast::<gst::Pipeline>()
        .map_err(|_| anyhow::anyhow!("GStreamer CGEN description did not produce a pipeline"))?;
    apply_mpegts_program_map(&pipeline, &plan)?;
    install_audio_rendition_matrices(&pipeline, &plan, audio_layout)?;
    pipeline
        .set_state(gst::State::Null)
        .context("failed to release preflight GStreamer pipeline")?;

    let resolved_program_map = pipeline_spec
        .program_map
        .as_ref()
        .map(crate::architecture::ProgramMapSpec::resolve)
        .transpose()?;
    let output_factory = GstOutputSinkFactory::new(resolved_program_map);
    let outputs = pipeline_spec
        .outputs
        .iter()
        .filter(|output| output.enabled)
        .map(|output| {
            output_factory.preflight(output).map(|report| {
                json!({
                    "output_id": report.output_id,
                    "destination": report.destination_kind,
                    "required_elements": report.required_elements,
                })
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let managed_font_dir = base_dir.join("managed").join("cgen").join("fonts");
    let renderer = WgpuFrameRenderer::new(
        format!("{}-preflight", feed.id),
        feed.video.width,
        feed.video.height,
        feed.video.interlaced,
        &managed_font_dir,
    )?;
    let graphics = renderer.status_value();

    Ok(json!({
        "feed_id": feed.id,
        "input_kind": pipeline_spec.input.kind(),
        "audio_layout": audio_layout.as_str(),
        "pipeline": plan.status_value(),
        "outputs": outputs,
        "graphics": graphics,
    }))
}

fn gstreamer_input_device_catalog() -> Result<Vec<Value>> {
    let monitor = gst::DeviceMonitor::new();
    monitor.set_show_all_devices(false);
    monitor
        .add_filter(Some("Video/Source"), None)
        .context("failed to add GStreamer video device filter")?;
    monitor
        .start()
        .context("failed to start GStreamer device discovery")?;
    let mut devices = monitor
        .devices()
        .into_iter()
        .map(|device| {
            let properties = device.properties();
            let property = |keys: &[&str]| {
                properties.as_ref().and_then(|properties| {
                    keys.iter().find_map(|key| {
                        properties
                            .get::<String>(*key)
                            .ok()
                            .map(|value| value.trim().to_string())
                            .filter(|value| !value.is_empty())
                    })
                })
            };
            let display_name = device.display_name().to_string();
            let persistent_id = property(&[
                "device.path",
                "device.id",
                "device.unique-id",
                "device.name",
            ])
            .unwrap_or_else(|| display_name.clone());
            let element = device
                .create_element(None)
                .ok()
                .and_then(|element| element.factory())
                .map(|factory| factory.name().to_string())
                .unwrap_or_default();
            let backend = property(&["device.api"])
                .or_else(|| element.contains("v4l2").then_some("v4l2".to_string()))
                .or_else(|| {
                    (element.contains("dshow") || element.contains("directshow"))
                        .then_some("directshow".to_string())
                })
                .unwrap_or_else(|| "gstreamer".to_string());
            let caps = device
                .caps()
                .map(|caps| caps.to_string())
                .unwrap_or_default();
            json!({
                "id": persistent_id,
                "label": display_name,
                "backend": backend,
                "element": element,
                "class": device.device_class().to_string(),
                "caps": caps.chars().take(4096).collect::<String>(),
            })
        })
        .collect::<Vec<_>>();
    monitor.stop();
    devices.sort_by(|left, right| {
        left.get("label")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .cmp(
                &right
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
            )
    });
    Ok(devices)
}

fn configure_portable_gstreamer_paths() {
    let Ok(exe_path) = env::current_exe() else {
        return;
    };
    let Some(exe_dir) = exe_path.parent() else {
        return;
    };
    let plugin_dir = exe_dir.join("gstreamer-1.0");
    if !plugin_dir.is_dir() {
        return;
    }
    prepend_env_path("GST_PLUGIN_PATH", &plugin_dir);
    prepend_env_path("GST_PLUGIN_SYSTEM_PATH_1_0", &plugin_dir);
    prepend_env_path("GST_PLUGIN_PATH_1_0", &plugin_dir);

    let scanner = plugin_dir.join(if cfg!(windows) {
        "gst-plugin-scanner.exe"
    } else {
        "gst-plugin-scanner"
    });
    if scanner.is_file() {
        env::set_var("GST_PLUGIN_SCANNER", scanner);
    }
}

fn prepend_env_path(name: &str, path: &Path) {
    let mut paths = env::var_os(name)
        .map(|raw| env::split_paths(&raw).collect::<Vec<_>>())
        .unwrap_or_default();
    if paths.iter().any(|existing| existing == path) {
        return;
    }
    paths.insert(0, path.to_path_buf());
    match env::join_paths(paths) {
        Ok(joined) => env::set_var(name, joined),
        Err(err) => warn!("failed to set {name} for GStreamer runtime: {err}"),
    }
}

fn gstreamer_muxer_catalog() -> Vec<Value> {
    let mut entries = Vec::new();
    for factory in
        gst::ElementFactory::factories_with_type(gst::ElementFactoryType::MUXER, gst::Rank::NONE)
    {
        let element = factory.name().to_string();
        if let Some((id, label)) = format_catalog_entry(
            element.as_str(),
            factory.longname(),
            factory.klass(),
            factory.description(),
        ) {
            let source = factory
                .plugin_name()
                .map(|name| name.to_string())
                .unwrap_or_else(|| "gstreamer".to_string());
            entries.push(json!({
                "id": id,
                "label": label,
                "kind": "container",
                "element": element,
                "source": source,
            }));
        }
    }
    sort_catalog_values(entries)
}

fn gstreamer_encoder_catalog(
    factory_type: gst::ElementFactoryType,
    media_kind: &str,
) -> Vec<Value> {
    let mut entries = Vec::new();
    for factory in gst::ElementFactory::factories_with_type(factory_type, gst::Rank::NONE) {
        let element = factory.name().to_string();
        let label = encoder_catalog_label(
            element.as_str(),
            factory.longname(),
            factory.klass(),
            factory.description(),
        );
        let source = factory
            .plugin_name()
            .map(|name| name.to_string())
            .unwrap_or_else(|| "gstreamer".to_string());
        entries.push(json!({
            "id": element,
            "label": label,
            "kind": media_kind,
            "element": element,
            "source": source,
        }));
    }
    sort_catalog_values(entries)
}

fn gstreamer_decoder_catalog() -> Vec<Value> {
    let mut entries = Vec::new();
    for factory in
        gst::ElementFactory::factories_with_type(gst::ElementFactoryType::DECODER, gst::Rank::NONE)
    {
        let element = factory.name().to_string();
        let lower = catalog_lower(
            element.as_str(),
            factory.longname(),
            factory.klass(),
            factory.description(),
        );
        if !factory_outputs_media_type(&factory, "video/")
            || !looks_like_video_decoder(element.as_str(), lower.as_str())
        {
            continue;
        }
        let label = encoder_catalog_label(
            element.as_str(),
            factory.longname(),
            factory.klass(),
            factory.description(),
        );
        let source = factory
            .plugin_name()
            .map(|name| name.to_string())
            .unwrap_or_else(|| "gstreamer".to_string());
        entries.push(json!({
            "id": element,
            "label": label,
            "kind": "video",
            "element": element,
            "source": source,
        }));
    }
    sort_catalog_values(entries)
}

fn factory_outputs_media_type(factory: &gst::ElementFactory, media_type_prefix: &str) -> bool {
    factory.static_pad_templates().iter().any(|template| {
        template.direction() == gst::PadDirection::Src
            && template
                .caps()
                .iter()
                .any(|structure| structure.name().as_str().starts_with(media_type_prefix))
    })
}

fn looks_like_video_decoder(element: &str, lower: &str) -> bool {
    let element = element.to_ascii_lowercase();
    element.contains("h264")
        || element.contains("avc")
        || element.contains("h265")
        || element.contains("hevc")
        || element.contains("mpeg2")
        || element.contains("av1")
        || element.contains("vp9")
        || element.contains("vp8")
        || lower.contains("h.264")
        || contains_catalog_token(lower, "h264")
        || contains_catalog_token(lower, "avc")
        || lower.contains("h.265")
        || contains_catalog_token(lower, "h265")
        || contains_catalog_token(lower, "hevc")
        || lower.contains("mpeg-2")
        || contains_catalog_token(lower, "mpeg2")
        || contains_catalog_token(lower, "av1")
        || contains_catalog_token(lower, "vp9")
        || contains_catalog_token(lower, "vp8")
}

fn sort_catalog_values(mut entries: Vec<Value>) -> Vec<Value> {
    entries.sort_by(|left, right| {
        let left_label = left
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let right_label = right
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        left_label.cmp(&right_label)
    });
    entries
}

fn format_catalog_entry(
    element: &str,
    longname: &str,
    klass: &str,
    description: &str,
) -> Option<(&'static str, &'static str)> {
    let lower = catalog_lower(element, longname, klass, description);
    match () {
        _ if lower.contains("mpeg-ts")
            || lower.contains("mpeg transport")
            || element == "mpegtsmux" =>
        {
            Some(("mpegts", "MPEG-TS"))
        }
        _ if lower.contains("quicktime") || lower.contains("mp4") => Some(("mp4", "MPEG-4 / MP4")),
        _ if lower.contains("matroska") => Some(("matroska", "Matroska")),
        _ if lower.contains("webm") => Some(("webm", "WebM")),
        _ if lower.contains("flv") => Some(("flv", "FLV")),
        _ if lower.contains("wav") => Some(("wav", "WAV")),
        _ if lower.contains("ogg") => Some(("ogg", "Ogg")),
        _ if lower.contains("avi") => Some(("avi", "AVI")),
        _ if lower.contains("mxf") => Some(("mxf", "MXF")),
        _ => None,
    }
}

fn encoder_catalog_label(element: &str, longname: &str, klass: &str, description: &str) -> String {
    let lower = catalog_lower(element, longname, klass, description);
    let codec = catalog_codec_name(lower.as_str()).unwrap_or(longname.trim());
    let implementation = catalog_implementation_name(element, lower.as_str());
    if implementation.is_empty() {
        format!("{codec} ({element})")
    } else {
        format!("{codec} - {implementation} ({element})")
    }
}

fn catalog_lower(element: &str, longname: &str, klass: &str, description: &str) -> String {
    format!("{element} {longname} {klass} {description}").to_ascii_lowercase()
}

fn contains_catalog_token(haystack: &str, token: &str) -> bool {
    haystack.match_indices(token).any(|(start, matched)| {
        let end = start + matched.len();
        let starts_at_boundary = start == 0
            || haystack
                .as_bytes()
                .get(start - 1)
                .is_some_and(|byte| !byte.is_ascii_alphanumeric());
        let ends_at_boundary = end == haystack.len()
            || haystack
                .as_bytes()
                .get(end)
                .is_some_and(|byte| !byte.is_ascii_alphanumeric());
        starts_at_boundary && ends_at_boundary
    })
}

fn catalog_codec_name(lower: &str) -> Option<&'static str> {
    match () {
        _ if lower.contains("e-ac-3")
            || contains_catalog_token(lower, "eac3")
            || lower.contains("enhanced ac-3") =>
        {
            Some("E-AC-3")
        }
        _ if lower.contains("ac-3") || contains_catalog_token(lower, "ac3") => Some("AC-3"),
        _ if lower.contains("h.265")
            || contains_catalog_token(lower, "h265")
            || contains_catalog_token(lower, "hevc") =>
        {
            Some("H.265 / HEVC")
        }
        _ if lower.contains("h.264")
            || contains_catalog_token(lower, "h264")
            || contains_catalog_token(lower, "avc") =>
        {
            Some("H.264 / AVC")
        }
        _ if contains_catalog_token(lower, "aac") => Some("AAC"),
        _ if contains_catalog_token(lower, "opus") => Some("Opus"),
        _ if contains_catalog_token(lower, "vorbis") => Some("Vorbis"),
        _ if contains_catalog_token(lower, "flac") => Some("FLAC"),
        _ if contains_catalog_token(lower, "mp3") || lower.contains("mpeg layer 3") => Some("MP3"),
        _ if contains_catalog_token(lower, "mp2") || lower.contains("mpeg layer ii") => {
            Some("MPEG Layer II Audio")
        }
        _ if contains_catalog_token(lower, "alaw") || lower.contains("a-law") => Some("A-law"),
        _ if contains_catalog_token(lower, "mulaw") || lower.contains("mu-law") => Some("mu-law"),
        _ if contains_catalog_token(lower, "speex") => Some("Speex"),
        _ if lower.contains("mpeg-2") || contains_catalog_token(lower, "mpeg2") => {
            Some("MPEG-2 Video")
        }
        _ if lower.contains("mpeg-4") || contains_catalog_token(lower, "mpeg4") => {
            Some("MPEG-4 Part 2")
        }
        _ if contains_catalog_token(lower, "av1") => Some("AV1"),
        _ if contains_catalog_token(lower, "vp9") => Some("VP9"),
        _ if contains_catalog_token(lower, "vp8") => Some("VP8"),
        _ if contains_catalog_token(lower, "theora") => Some("Theora"),
        _ if contains_catalog_token(lower, "jpeg") => Some("Motion JPEG"),
        _ if contains_catalog_token(lower, "png") => Some("PNG"),
        _ if contains_catalog_token(lower, "webp") => Some("WebP"),
        _ => None,
    }
}

fn catalog_implementation_name(element: &str, lower: &str) -> &'static str {
    match element {
        "x264enc" => "x264 software",
        "x265enc" => "x265 software",
        "openh264enc" => "OpenH264",
        "amfh264enc" | "amfh265enc" | "amfav1enc" => "AMD AMF",
        "nvh264enc" | "nvh265enc" | "nvav1enc" => "NVIDIA NVENC",
        "qsvh264enc" | "qsvh265enc" | "qsvmpeg2enc" | "qsvav1enc" => "Intel Quick Sync",
        "svtav1enc" => "SVT-AV1",
        "aomav1enc" | "av1enc" => "AOM",
        "vp8enc" | "vp9enc" => "libvpx",
        "opusenc" => "Opus",
        "vorbisenc" => "Vorbis",
        "flacenc" => "FLAC",
        "faac" => "FAAC",
        "fdkaacenc" => "fdk-aac",
        "mfaacenc" => "Media Foundation",
        "lamemp3enc" => "LAME",
        "twolame" => "TwoLAME",
        _ if element.starts_with("avenc_") => "libav",
        _ if lower.contains("hardware") => "hardware",
        _ => "",
    }
}

fn run_pipeline_once(
    mut feed: FeedConfig,
    mut state_rx: watch::Receiver<RuntimeState>,
    mut shutdown_rx: watch::Receiver<bool>,
    base_dir: PathBuf,
    status_tx: Option<watch::Sender<Option<Value>>>,
    media_pcm: MediaPcmSubscription,
    effective_caps_override: Option<SourceCaps>,
    effective_program_map_override: Option<InputProgramMapSnapshot>,
    output_fanout: Option<Arc<OutputFanout>>,
) -> Result<PipelineRunExit> {
    let caps_runtime = prepare_source_caps_runtime(&mut feed, &base_dir, effective_caps_override)?;
    let audio_routing_spec = feed.audio_routing_spec()?;
    let audio_output_layout = forced_audio_output_layout(&audio_routing_spec)?;
    let program_mapping_mode = feed.program_mapping_mode()?;
    let plan = GstPipelinePlan::from_feed_with_program_map(
        &feed,
        effective_program_map_override.as_ref(),
    )?;
    info!(
        feed_id = %feed.id,
        input = %feed.redacted_program_input_url(),
        output = %feed.redacted_program_output_url(),
        routine_audio = feed.priority_input.routine_audio_enabled(),
        priority_audio = feed.priority_input.priority_audio_enabled(),
        mute_standby_routine = feed.audio.mute_standby_routine,
        pipeline = %crate::config::redacted_pipeline_description(&feed, &plan.description),
        "starting gstreamer cgen pipeline"
    );
    publish_status(
        &status_tx,
        &feed,
        &state_rx,
        false,
        json!({
            "media_backend": "gstreamer-rs",
            "input_connected": false,
            "input_video_connected": false,
            "input_audio_connected": false,
            "output_active": false,
            "pipeline_description": crate::config::redacted_pipeline_description(&feed, &plan.description),
            "output_ladder": plan.status_value(),
            "output_workers": output_worker_status_value(output_fanout.as_deref()),
            "detected_caps": caps_runtime.detected_caps(),
            "effective_caps": caps_runtime.effective_caps(),
            "source_caps": caps_runtime.status_value(),
            "input_program_map": {
                "mode": program_mapping_mode.as_str(),
                "detected": Value::Null,
                "effective": effective_program_map_override.as_ref()
                    .map(InputProgramMapSnapshot::status_value),
                "effective_origin": if program_mapping_mode == ProgramMappingMode::Source
                    && effective_program_map_override.is_some() {
                    "input"
                } else if program_mapping_mode == ProgramMappingMode::Source {
                    "automatic_fallback"
                } else {
                    program_mapping_mode.as_str()
                },
            },
            "sync": sync_status_value(&feed),
        }),
    );
    if let Err(err) = validate_gstreamer_elements(&plan.required_elements) {
        publish_status(
            &status_tx,
            &feed,
            &state_rx,
            false,
            json!({
                "media_backend": "gstreamer-rs",
                "input_connected": false,
                "output_active": false,
                "last_error": err.to_string(),
                "required_elements": plan.required_elements,
                "detected_caps": caps_runtime.detected_caps(),
                "effective_caps": caps_runtime.effective_caps(),
                "source_caps": caps_runtime.status_value(),
                "sync": sync_status_value(&feed),
            }),
        );
        return Err(err);
    }

    let element =
        gst::parse::launch(&plan.description).context("failed to parse GStreamer CGEN pipeline")?;
    let pipeline = element
        .downcast::<gst::Pipeline>()
        .map_err(|_| anyhow::anyhow!("GStreamer CGEN description did not produce a pipeline"))?;
    let input_program_map_runtime =
        InputProgramMapRuntime::install(&pipeline, &feed, effective_program_map_override.clone())?;
    apply_mpegts_program_map(&pipeline, &plan)?;
    install_audio_rendition_matrices(&pipeline, &plan, audio_output_layout)?;
    if plan.uses_output_fanout() {
        let fanout = output_fanout
            .as_ref()
            .context("isolated CGEN output plan is missing its output workers")?;
        install_output_fanout_sinks(
            &pipeline,
            Arc::clone(fanout),
            feed.video.width,
            feed.video.height,
            audio_output_layout,
        )?;
    } else if output_fanout.is_some() {
        bail!("legacy CGEN pipeline received isolated output workers");
    }
    let priority_appsrc = priority_audio_appsrc(&pipeline)?;
    let input_health = Arc::new(Mutex::new(InputHealth::default()));
    install_program_video_probe(&pipeline, Arc::clone(&input_health), caps_runtime.clone())?;
    let audio_mix =
        install_audio_mix_runtime(&pipeline, Arc::clone(&input_health), audio_routing_spec)?;
    let mut priority_feeder = PriorityAudioFeeder::new(priority_appsrc, media_pcm);
    let mut text_overlay = TextOverlayController::default();
    let preview_pixel_aspect_ratio = caps_runtime.effective_caps().pixel_aspect_ratio();
    let wgpu_compositors = match install_wgpu_overlay_probes(
        &pipeline,
        &feed,
        &plan,
        &base_dir,
        text_overlay.shared_state(),
        preview_pixel_aspect_ratio,
    ) {
        Ok(compositors) => compositors,
        Err(err) => {
            publish_status(
                &status_tx,
                &feed,
                &state_rx,
                false,
                json!({
                    "media_backend": "gstreamer-rs",
                    "graphics_backend": "wgpu",
                    "input_connected": false,
                    "output_active": false,
                    "fatal": true,
                    "last_error": err.to_string(),
                    "detected_caps": caps_runtime.detected_caps(),
                    "effective_caps": caps_runtime.effective_caps(),
                    "source_caps": caps_runtime.status_value(),
                    "sync": sync_status_value(&feed),
                }),
            );
            return Err(err);
        }
    };
    let (initial_audio_mode, initial_video_mode) = {
        let state = state_rx.borrow();
        let video_connected = input_video_connected(&input_health, &feed);
        let audio_connected = input_audio_connected(&input_health, &feed);
        let feeder_status = priority_audio_for_feed(&feed, &state).map_or(
            PriorityAudioSourceStatus {
                status: "idle",
                queue_id: None,
                error: None,
            },
            |audio| PriorityAudioSourceStatus {
                status: "waiting",
                queue_id: Some(audio.queue_id.clone()),
                error: None,
            },
        );
        let video_mode = desired_video_selector_mode(&feed, &state, video_connected);
        (
            desired_audio_selector_mode_with_status(&feed, audio_connected, &feeder_status),
            video_mode,
        )
    };
    let mut current_audio_mode = initial_audio_mode;
    let mut current_video_mode = initial_video_mode;
    audio_mix.set_mode(initial_audio_mode)?;
    set_video_selector_mode(&pipeline, initial_video_mode)?;
    let mut diagnostics = PipelineDiagnostics::default();
    pipeline
        .set_state(gst::State::Playing)
        .context("failed to set GStreamer CGEN pipeline to Playing")?;
    {
        let state = state_rx.borrow();
        let feeder_status = priority_feeder.update(priority_audio_for_feed(&feed, &state));
        text_overlay.sync(
            &pipeline,
            &feed,
            &state,
            current_video_mode.no_signal(),
            feeder_status.is_live_priority(),
        )?;
    }
    publish_status(
        &status_tx,
        &feed,
        &state_rx,
        current_video_mode.no_signal(),
        json!({
            "media_backend": "gstreamer-rs",
            "input_connected": false,
            "input_video_connected": false,
            "input_audio_connected": false,
            "output_active": true,
            "routine_audio_enabled": feed.priority_input.routine_audio_enabled(),
            "priority_audio_enabled": feed.priority_input.priority_audio_enabled(),
            "priority_input_format": feed.priority_input.format,
            "mute_standby_routine": feed.audio.mute_standby_routine,
            "audio_selector": initial_audio_mode.as_str(),
            "audio_mix": audio_mix.status_value(),
            "video_selector": initial_video_mode.as_str(),
            "priority_audio_pipeline": priority_feeder.active_status().status,
            "priority_audio_media_bridge": priority_feeder.media_status_value(),
            "audio_drops": priority_feeder.media_pcm.dropped_chunks(),
            "text_overlay": text_overlay.status_value(),
            "graphics_renderer": wgpu_compositors.status_value(),
            "input_health": input_health_status(&input_health, &feed),
            "detected_caps": caps_runtime.detected_caps(),
            "effective_caps": caps_runtime.effective_caps(),
            "source_caps": caps_runtime.status_value(),
            "input_program_map": input_program_map_runtime.status_value(),
            "pipeline_diagnostics": diagnostics.status_value(),
            "output_ladder": plan.status_value(),
            "output_workers": output_worker_status_value(output_fanout.as_deref()),
            "sync": sync_status_value(&feed),
        }),
    );

    let bus = pipeline
        .bus()
        .context("GStreamer CGEN pipeline has no bus")?;
    let mut next_status = Instant::now();
    loop {
        if *shutdown_rx.borrow() {
            pipeline
                .set_state(gst::State::Null)
                .context("failed to stop GStreamer CGEN pipeline during shutdown")?;
            return Ok(PipelineRunExit::Clean);
        }
        while let Some(message) =
            bus.timed_pop(gst::ClockTime::from_mseconds(GST_BUS_POLL_INTERVAL_MS))
        {
            use gst::MessageView;
            match message.view() {
                MessageView::Eos(..) => {
                    mark_input_timeout(&input_health);
                    let _ = set_video_selector_mode(&pipeline, VideoSelectorMode::Standby);
                    publish_status(
                        &status_tx,
                        &feed,
                        &state_rx,
                        true,
                        json!({
                            "media_backend": "gstreamer-rs",
                            "input_connected": false,
                            "input_video_connected": false,
                            "output_active": true,
                            "video_selector": "standby",
                            "visual_lifecycle": "standby",
                            "input_unhealthy_reason": "eos",
                            "detected_caps": caps_runtime.detected_caps(),
                            "effective_caps": caps_runtime.effective_caps(),
                            "source_caps": caps_runtime.status_value(),
                            "input_health": input_health_status(&input_health, &feed),
                        }),
                    );
                    pipeline
                        .set_state(gst::State::Null)
                        .context("failed to stop GStreamer CGEN pipeline")?;
                    return Ok(PipelineRunExit::Clean);
                }
                MessageView::Error(err) => {
                    let src = err
                        .src()
                        .map(|src| src.path_string())
                        .unwrap_or_else(|| "unknown".into());
                    let debug = err
                        .debug()
                        .map(|debug| debug.to_string())
                        .unwrap_or_default();
                    mark_input_timeout(&input_health);
                    let _ = pipeline.set_state(gst::State::Null);
                    let error_text =
                        format!("GStreamer CGEN error from {src}: {} {debug}", err.error());
                    publish_status(
                        &status_tx,
                        &feed,
                        &state_rx,
                        current_video_mode.no_signal(),
                        json!({
                            "media_backend": "gstreamer-rs",
                            "graphics_backend": plan.graphics_backend,
                            "fatal": false,
                            "input_connected": false,
                            "input_video_connected": false,
                            "output_active": false,
                            "input_unhealthy_reason": "decoder_or_pipeline_error",
                            "last_error": error_text,
                            "detected_caps": caps_runtime.detected_caps(),
                            "effective_caps": caps_runtime.effective_caps(),
                            "source_caps": caps_runtime.status_value(),
                            "pipeline_diagnostics": diagnostics.status_value(),
                            "output_ladder": plan.status_value(),
                            "output_workers": output_worker_status_value(output_fanout.as_deref()),
                            "sync": sync_status_value(&feed),
                        }),
                    );
                    bail!("{}", error_text);
                }
                MessageView::Warning(warning) => {
                    diagnostics.record_warning(message_src_path(&message), warning);
                }
                MessageView::StateChanged(state) => {
                    if message
                        .src()
                        .is_some_and(|src| src == pipeline.upcast_ref::<gst::Object>())
                    {
                        publish_status(
                            &status_tx,
                            &feed,
                            &state_rx,
                            current_video_mode.no_signal(),
                            json!({
                                "media_backend": "gstreamer-rs",
                                "gst_state": format!("{:?}", state.current()),
                                "detected_caps": caps_runtime.detected_caps(),
                                "effective_caps": caps_runtime.effective_caps(),
                                "source_caps": caps_runtime.status_value(),
                            }),
                        );
                    }
                }
                MessageView::Element(element) => {
                    if element
                        .structure()
                        .is_some_and(|structure| structure.name() == "GstUDPSrcTimeout")
                    {
                        mark_input_timeout(&input_health);
                    }
                }
                MessageView::Latency(..) => match pipeline.recalculate_latency() {
                    Ok(()) => diagnostics.record_latency_recalculation(None),
                    Err(err) => diagnostics.record_latency_recalculation(Some(err.to_string())),
                },
                MessageView::Qos(qos) => {
                    diagnostics.record_qos(message_src_path(&message), qos);
                }
                _ => {}
            }
        }
        if let Some(changed_program_map) = input_program_map_runtime.reconfigure_request() {
            mark_input_timeout(&input_health);
            set_video_selector_mode(&pipeline, VideoSelectorMode::Standby)?;
            publish_status(
                &status_tx,
                &feed,
                &state_rx,
                true,
                json!({
                    "media_backend": "gstreamer-rs",
                    "graphics_backend": plan.graphics_backend,
                    "input_connected": false,
                    "input_video_connected": false,
                    "output_active": true,
                    "video_selector": "standby",
                    "visual_lifecycle": "standby",
                    "reconfigure_state": "rebuilding",
                    "reconfigure_reason": "input_program_map_changed",
                    "detected_caps": caps_runtime.detected_caps(),
                    "effective_caps": caps_runtime.effective_caps(),
                    "source_caps": caps_runtime.status_value(),
                    "input_program_map": input_program_map_runtime.status_value(),
                    "output_ladder": plan.status_value(),
                    "output_workers": output_worker_status_value(output_fanout.as_deref()),
                    "sync": sync_status_value(&feed),
                }),
            );
            pipeline.set_state(gst::State::Null).context(
                "failed to stop GStreamer CGEN pipeline for program map reconfiguration",
            )?;
            return Ok(PipelineRunExit::ReconfigureProgramMap(changed_program_map));
        }
        if let Some(changed_caps) = caps_runtime.reconfigure_request() {
            mark_input_timeout(&input_health);
            set_video_selector_mode(&pipeline, VideoSelectorMode::Standby)?;
            publish_status(
                &status_tx,
                &feed,
                &state_rx,
                true,
                json!({
                    "media_backend": "gstreamer-rs",
                    "graphics_backend": plan.graphics_backend,
                    "input_connected": false,
                    "input_video_connected": false,
                    "output_active": true,
                    "video_selector": "standby",
                    "visual_lifecycle": "standby",
                    "reconfigure_state": "rebuilding",
                    "reconfigure_reason": "source_caps_changed",
                    "detected_caps": caps_runtime.detected_caps(),
                    "effective_caps": caps_runtime.effective_caps(),
                    "source_caps": caps_runtime.status_value(),
                    "input_program_map": input_program_map_runtime.status_value(),
                    "output_ladder": plan.status_value(),
                    "output_workers": output_worker_status_value(output_fanout.as_deref()),
                    "sync": sync_status_value(&feed),
                }),
            );
            pipeline
                .set_state(gst::State::Null)
                .context("failed to stop GStreamer CGEN pipeline for caps reconfiguration")?;
            return Ok(PipelineRunExit::ReconfigureCaps(changed_caps));
        }
        {
            let state = state_rx.borrow();
            let video_connected = input_video_connected(&input_health, &feed);
            let video_mode = desired_video_selector_mode(&feed, &state, video_connected);
            if current_video_mode != video_mode {
                set_video_selector_mode(&pipeline, video_mode)?;
                current_video_mode = video_mode;
            }
            let priority_audio = priority_audio_for_feed(&feed, &state);
            let audio_connected = input_audio_connected(&input_health, &feed);
            let lifecycle_status = priority_audio.map_or(
                PriorityAudioSourceStatus {
                    status: "idle",
                    queue_id: None,
                    error: None,
                },
                |audio| PriorityAudioSourceStatus {
                    status: "waiting",
                    queue_id: Some(audio.queue_id.clone()),
                    error: None,
                },
            );
            let audio_mode =
                desired_audio_selector_mode_with_status(&feed, audio_connected, &lifecycle_status);
            if current_audio_mode != audio_mode {
                audio_mix.set_mode(audio_mode)?;
                current_audio_mode = audio_mode;
            }
            let feeder_status = priority_feeder.update(priority_audio);
            text_overlay.sync(
                &pipeline,
                &feed,
                &state,
                video_mode.no_signal(),
                feeder_status.is_live_priority(),
            )?;
        }
        if shutdown_rx.has_changed().unwrap_or(false) && *shutdown_rx.borrow_and_update() {
            pipeline
                .set_state(gst::State::Null)
                .context("failed to stop GStreamer CGEN pipeline during shutdown")?;
            return Ok(PipelineRunExit::Clean);
        }
        if state_rx.has_changed().unwrap_or(false) || Instant::now() >= next_status {
            let state = state_rx.borrow_and_update();
            let video_connected = input_video_connected(&input_health, &feed);
            let audio_connected = input_audio_connected(&input_health, &feed);
            let priority_audio = priority_audio_for_feed(&feed, &state);
            let lifecycle_status = priority_audio.map_or(
                PriorityAudioSourceStatus {
                    status: "idle",
                    queue_id: None,
                    error: None,
                },
                |audio| PriorityAudioSourceStatus {
                    status: "waiting",
                    queue_id: Some(audio.queue_id.clone()),
                    error: None,
                },
            );
            let audio_mode =
                desired_audio_selector_mode_with_status(&feed, audio_connected, &lifecycle_status);
            if current_audio_mode != audio_mode {
                audio_mix.set_mode(audio_mode)?;
                current_audio_mode = audio_mode;
            }
            let feeder_status = priority_feeder.update(priority_audio);
            let video_mode = desired_video_selector_mode(&feed, &state, video_connected);
            let priority_active = priority_audio_active(&feed, &state);
            let visual_lifecycle = if state
                .banner_for(configured_priority_feed_id(&feed))
                .is_some()
            {
                "banner"
            } else if video_mode.no_signal() {
                "standby"
            } else {
                "release"
            };
            drop(state);
            let media_pcm_status = priority_feeder.media_status_value();
            let audio_drops = priority_feeder.media_pcm.dropped_chunks();
            if current_video_mode != video_mode {
                set_video_selector_mode(&pipeline, video_mode)?;
                current_video_mode = video_mode;
            }
            publish_status(
                &status_tx,
                &feed,
                &state_rx,
                video_mode.no_signal(),
                json!({
                    "media_backend": "gstreamer-rs",
                    "output_active": true,
                    "routine_audio_enabled": feed.priority_input.routine_audio_enabled(),
                    "priority_audio_enabled": feed.priority_input.priority_audio_enabled(),
                    "mute_standby_routine": feed.audio.mute_standby_routine,
                    "audio_selector": audio_mode.as_str(),
                    "audio_mix": audio_mix.status_value(),
                    "video_selector": video_mode.as_str(),
                    "audio_lifecycle": audio_mode.status_text(priority_active),
                    "priority_audio_active": priority_active,
                    "priority_audio_pipeline": feeder_status.status,
                    "priority_audio_queue_id": feeder_status.queue_id,
                    "priority_audio_error": feeder_status.error,
                    "priority_audio_media_bridge": media_pcm_status,
                    "audio_drops": audio_drops,
                    "text_overlay": text_overlay.status_value(),
                    "graphics_renderer": wgpu_compositors.status_value(),
                    "visual_lifecycle": visual_lifecycle,
                    "input_connected": video_connected && audio_connected,
                    "input_video_connected": video_connected,
                    "input_audio_connected": audio_connected,
                    "input_health": input_health_status(&input_health, &feed),
                    "detected_caps": caps_runtime.detected_caps(),
                    "effective_caps": caps_runtime.effective_caps(),
                    "source_caps": caps_runtime.status_value(),
                    "input_program_map": input_program_map_runtime.status_value(),
                    "pipeline_diagnostics": diagnostics.status_value(),
                    "output_ladder": plan.status_value(),
                    "output_workers": output_worker_status_value(output_fanout.as_deref()),
                    "sync": sync_status_value(&feed),
                }),
            );
            next_status = Instant::now()
                + Duration::from_millis(u64::from(feed.sync.status_interval_ms.max(100)));
        }
    }
}

type TextOverlayRenderState = OverlayRenderState;

#[derive(Debug, Default)]
struct TextOverlayController {
    key: Option<String>,
    started_at: Option<Instant>,
    pass_index: u32,
    completed_passes: u32,
    audio_was_live: bool,
    post_audio_repeats_started: u32,
    retaining_cleared_ticker: bool,
    last: Option<TextOverlayRenderState>,
    shared: Arc<Mutex<Option<TextOverlayRenderState>>>,
}

impl TextOverlayController {
    fn sync(
        &mut self,
        _pipeline: &gst::Pipeline,
        feed: &FeedConfig,
        state: &RuntimeState,
        no_signal: bool,
        audio_live: bool,
    ) -> Result<()> {
        let next = self.next_state(feed, state, no_signal, audio_live);
        if self.last.as_ref() == Some(&next) {
            return Ok(());
        }
        if let Ok(mut guard) = self.shared.lock() {
            *guard = Some(next.clone());
        }
        self.last = Some(next);
        Ok(())
    }

    fn next_state(
        &mut self,
        feed: &FeedConfig,
        state: &RuntimeState,
        no_signal: bool,
        audio_live: bool,
    ) -> TextOverlayRenderState {
        self.update_audio_lifecycle(audio_live);
        let snapshot = crate::graphics::presentation_snapshot(feed, state, no_signal);
        let text = snapshot.overlay_text.trim().to_string();
        let font_desc = format!("{} {}", snapshot.font, snapshot.font_size.max(16));
        let ypos = overlay_ypos(feed, snapshot.ticker_y);
        if text.is_empty() {
            if snapshot.visual_mode == "release" {
                if let Some(last) = self.last.clone() {
                    if last.visual_mode == "ticker_alert" {
                        if !ticker_state_done(&last) {
                            self.retaining_cleared_ticker = true;
                            return last;
                        }
                        if self.should_repeat_ticker(feed, audio_live) {
                            let next = self.restart_ticker_pass(last);
                            self.retaining_cleared_ticker = true;
                            return next;
                        }
                    }
                }
            }
            self.reset_ticker_lifecycle();
            return TextOverlayRenderState {
                source_text: text.clone(),
                visual_id: String::new(),
                visual_mode: snapshot.visual_mode,
                font_family: snapshot.font,
                font_weight: snapshot.font_weight,
                font_size: snapshot.font_size.max(16),
                clock_text: snapshot.clock_text,
                clock_x: snapshot.clock_x,
                clock_y: snapshot.clock_y,
                clock_font_size: snapshot.clock_font_size,
                clock_color: snapshot.clock_color,
                banner_height: snapshot.ticker_height,
                speed_px_per_frame: snapshot.ticker_speed_px_per_frame.max(1),
                frame_width: feed.video.width.max(1),
                draw_fps: render_draw_fps(feed),
                text_width_px: 0.0,
                started_at: Instant::now(),
                text,
                font_desc,
                ypos,
                x_absolute: 1.0,
                silent: true,
                gradient: snapshot.ticker_gradient,
                sunny_cat: snapshot.sunny_cat,
            };
        }

        let visual_id = if snapshot.visual_id.trim().is_empty() {
            text.clone()
        } else {
            snapshot.visual_id.clone()
        };
        let source_text = snapshot.overlay_text.trim().to_string();
        let key = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}",
            snapshot.visual_mode,
            visual_id,
            snapshot.font,
            snapshot.font_weight,
            snapshot.font_size,
            snapshot.ticker_height,
            snapshot.ticker_speed_px_per_frame,
            feed.banner.scroll_repeat_mode,
            source_text
        );
        if self.retaining_cleared_ticker && snapshot.visual_mode == "ticker_alert" {
            self.start_ticker_lifecycle(key);
        } else if self.key.as_deref() != Some(key.as_str()) {
            self.start_ticker_lifecycle(key);
        } else if let Some(last) = self.last.clone() {
            if snapshot.visual_mode == "ticker_alert" && ticker_state_done(&last) {
                if self.should_repeat_ticker(feed, audio_live) {
                    return self.restart_ticker_pass(last);
                }
                return self.silent_state(feed, snapshot, text, font_desc, ypos);
            }
        }
        let font_size = snapshot.font_size.max(16);
        let started_at = self.started_at.unwrap_or_else(Instant::now);
        let (text, x_absolute, silent) = if snapshot.visual_mode == "ticker_alert" {
            let (x_absolute, _) = ticker_x_absolute(feed, &snapshot, &source_text, started_at);
            (source_text.clone(), x_absolute, false)
        } else {
            ticker_overlay_window(feed, &snapshot, &source_text, started_at)
        };
        TextOverlayRenderState {
            text_width_px: estimated_text_width_px(&source_text, font_size),
            visual_id: self.render_visual_id(&visual_id),
            source_text,
            visual_mode: snapshot.visual_mode,
            font_family: snapshot.font,
            font_weight: snapshot.font_weight,
            font_size,
            clock_text: snapshot.clock_text,
            clock_x: snapshot.clock_x,
            clock_y: snapshot.clock_y,
            clock_font_size: snapshot.clock_font_size,
            clock_color: snapshot.clock_color,
            banner_height: snapshot.ticker_height,
            speed_px_per_frame: snapshot.ticker_speed_px_per_frame.max(1),
            frame_width: feed.video.width.max(1),
            draw_fps: render_draw_fps(feed),
            started_at,
            text,
            font_desc,
            ypos,
            x_absolute,
            silent,
            gradient: snapshot.ticker_gradient,
            sunny_cat: snapshot.sunny_cat,
        }
    }

    fn update_audio_lifecycle(&mut self, audio_live: bool) {
        if audio_live {
            self.audio_was_live = true;
            self.post_audio_repeats_started = 0;
        } else if self.audio_was_live {
            self.audio_was_live = false;
            self.post_audio_repeats_started = 0;
        }
    }

    fn start_ticker_lifecycle(&mut self, key: String) {
        self.key = Some(key);
        self.started_at = Some(Instant::now());
        self.pass_index = 0;
        self.completed_passes = 0;
        self.post_audio_repeats_started = 0;
        self.retaining_cleared_ticker = false;
    }

    fn reset_ticker_lifecycle(&mut self) {
        self.key = None;
        self.started_at = None;
        self.pass_index = 0;
        self.completed_passes = 0;
        self.post_audio_repeats_started = 0;
        self.retaining_cleared_ticker = false;
    }

    fn restart_ticker_pass(&mut self, mut state: TextOverlayRenderState) -> TextOverlayRenderState {
        self.completed_passes = self.completed_passes.saturating_add(1);
        self.pass_index = self.pass_index.saturating_add(1);
        let now = Instant::now();
        self.started_at = Some(now);
        state.started_at = now;
        state.x_absolute = 1.0;
        state.silent = false;
        state.text = state.source_text.clone();
        state.visual_id = self.render_visual_id(&base_visual_id(&state.visual_id));
        state
    }

    fn should_repeat_ticker(&mut self, feed: &FeedConfig, audio_live: bool) -> bool {
        if fixed_scroll_repeats_enabled(feed) {
            let total = feed.banner.fixed_repeats.max(1);
            return self.completed_passes.saturating_add(1) < total;
        }
        if audio_live {
            return true;
        }
        if self.post_audio_repeats_started < feed.banner.after_eom_repeats {
            self.post_audio_repeats_started = self.post_audio_repeats_started.saturating_add(1);
            return true;
        }
        false
    }

    fn render_visual_id(&self, visual_id: &str) -> String {
        if self.pass_index == 0 {
            visual_id.to_string()
        } else {
            format!("{}#pass{}", visual_id, self.pass_index.saturating_add(1))
        }
    }

    fn silent_state(
        &mut self,
        feed: &FeedConfig,
        snapshot: crate::graphics::PresentationSnapshot,
        text: String,
        font_desc: String,
        ypos: f64,
    ) -> TextOverlayRenderState {
        self.reset_ticker_lifecycle();
        TextOverlayRenderState {
            source_text: text.clone(),
            visual_id: String::new(),
            visual_mode: snapshot.visual_mode,
            font_family: snapshot.font,
            font_weight: snapshot.font_weight,
            font_size: snapshot.font_size.max(16),
            clock_text: snapshot.clock_text,
            clock_x: snapshot.clock_x,
            clock_y: snapshot.clock_y,
            clock_font_size: snapshot.clock_font_size,
            clock_color: snapshot.clock_color,
            banner_height: snapshot.ticker_height,
            speed_px_per_frame: snapshot.ticker_speed_px_per_frame.max(1),
            frame_width: feed.video.width.max(1),
            draw_fps: render_draw_fps(feed),
            text_width_px: 0.0,
            started_at: Instant::now(),
            text,
            font_desc,
            ypos,
            x_absolute: 1.0,
            silent: true,
            gradient: snapshot.ticker_gradient,
            sunny_cat: snapshot.sunny_cat,
        }
    }

    fn status_value(&self) -> Value {
        if let Some(state) = &self.last {
            json!({
                "render_text_len": state.text.chars().count(),
                "render_text_preview": state.text.chars().take(160).collect::<String>(),
                "x_absolute": state.x_absolute,
                "ypos": state.ypos,
                "silent": state.silent,
                "sunny_cat": state.sunny_cat,
                "font_desc": state.font_desc,
                "font_weight": state.font_weight,
                "clock_text": state.clock_text,
                "clock_x": state.clock_x,
                "clock_y": state.clock_y,
                "clock_font_size": state.clock_font_size,
                "pass_index": self.pass_index,
                "completed_passes": self.completed_passes,
                "audio_was_live": self.audio_was_live,
                "post_audio_repeats_started": self.post_audio_repeats_started,
            })
        } else {
            json!({
                "render_text_len": 0,
                "render_text_preview": "",
                "x_absolute": 1.0,
                "ypos": 0.08,
                "silent": true,
                "sunny_cat": false,
                "font_desc": "",
                "clock_text": "",
                "clock_x": 48,
                "clock_y": 48,
                "clock_font_size": 30,
                "pass_index": self.pass_index,
                "completed_passes": self.completed_passes,
            })
        }
    }

    fn shared_state(&self) -> Arc<Mutex<Option<TextOverlayRenderState>>> {
        Arc::clone(&self.shared)
    }
}

struct WgpuCompositorSet {
    backend: &'static str,
    renderers: Vec<Arc<Mutex<WgpuFrameRenderer>>>,
    preview_statuses: Vec<Arc<FramePreviewStatus>>,
    _probe_ids: Vec<gst::PadProbeId>,
}

impl WgpuCompositorSet {
    fn passthrough(backend: &'static str) -> Self {
        Self {
            backend,
            renderers: Vec::new(),
            preview_statuses: Vec::new(),
            _probe_ids: Vec::new(),
        }
    }

    fn status_value(&self) -> Value {
        json!({
            "graphics_backend": self.backend,
            "renditions": self.renderers.iter().filter_map(|renderer| {
                renderer.lock().ok().map(|renderer| renderer.status_value())
            }).collect::<Vec<_>>(),
            "frame_previews": self.preview_statuses.iter().map(|status| {
                status.status_value()
            }).collect::<Vec<_>>(),
        })
    }
}

fn install_wgpu_overlay_probes(
    pipeline: &gst::Pipeline,
    feed: &FeedConfig,
    plan: &GstPipelinePlan,
    base_dir: &Path,
    state: Arc<Mutex<Option<TextOverlayRenderState>>>,
    fallback_pixel_aspect_ratio: CapsRational,
) -> Result<WgpuCompositorSet> {
    let mut renderers = Vec::new();
    let mut preview_statuses = Vec::new();
    let mut probe_ids = Vec::new();
    for (index, video) in plan.videos.iter().enumerate() {
        let overlay_name = gst_element_name("cgen_overlay", &video.id, index);
        let overlay = pipeline
            .by_name(&overlay_name)
            .with_context(|| format!("GStreamer CGEN pipeline is missing {overlay_name}"))?;
        let src_pad = overlay
            .static_pad("src")
            .with_context(|| format!("GStreamer CGEN overlay {overlay_name} has no src pad"))?;
        let managed_font_dir = base_dir.join("managed").join("fonts");
        let renderer = Arc::new(Mutex::new(WgpuFrameRenderer::new(
            video.id.clone(),
            video.width,
            video.height,
            video.interlaced,
            &managed_font_dir,
        )?));
        let renderer_for_probe = Arc::clone(&renderer);
        let state_for_probe = Arc::clone(&state);
        let preview_writer = if index == 0 {
            let (preview, status) = FramePreviewDispatcher::spawn(
                base_dir
                    .join("runtime")
                    .join("cgen")
                    .join(format!("{}.preview.jpg", safe_preview_id(&feed.id))),
                video.width,
                video.height,
                &feed.id,
            );
            preview_statuses.push(status);
            preview
        } else {
            None
        };
        let preview_for_probe = preview_writer;
        let feed_id = feed.id.clone();
        let overlay_for_probe = overlay_name.clone();
        let probe_id = src_pad
            .add_probe(gst::PadProbeType::BUFFER, move |pad, info| {
                let state = state_for_probe.lock().ok().and_then(|guard| guard.clone());
                let overlay_active = overlay_render_state_needs_frame_mutation(state.as_ref());
                let preview_due = preview_for_probe
                    .as_ref()
                    .is_some_and(FramePreviewDispatcher::claim_due);
                let preview_pixel_aspect_ratio = preview_due.then(|| {
                    preview_pixel_aspect_ratio_from_pad(pad, fallback_pixel_aspect_ratio)
                });
                if !overlay_active && !preview_due {
                    return gst::PadProbeReturn::Ok;
                }

                if overlay_active {
                    let Some(buffer) = info.buffer_mut() else {
                        return gst::PadProbeReturn::Ok;
                    };
                    let frame_pts_ns = buffer.pts().map(|pts| pts.nseconds());
                    let buffer = buffer.make_mut();
                    match buffer.map_writable() {
                        Ok(mut map) => {
                            if let Ok(mut renderer) = renderer_for_probe.lock() {
                                if let Err(err) = renderer.composite_bgrx(map.as_mut_slice(), frame_pts_ns, state.as_ref()) {
                                    warn!(feed_id = %feed_id, overlay = %overlay_for_probe, "wgpu compositor failed: {err:#}");
                                }
                            }
                            if preview_due {
                                if let (Some(preview), Some(pixel_aspect_ratio)) =
                                    (&preview_for_probe, preview_pixel_aspect_ratio)
                                {
                                    preview.submit(map.as_slice(), pixel_aspect_ratio);
                                }
                            }
                        }
                        Err(err) => {
                            warn!(feed_id = %feed_id, overlay = %overlay_for_probe, "failed to map video buffer for wgpu compositor: {err}");
                        }
                    }
                } else if preview_due {
                    let Some(buffer) = info.buffer() else {
                        return gst::PadProbeReturn::Ok;
                    };
                    match buffer.map_readable() {
                        Ok(map) => {
                            if let (Some(preview), Some(pixel_aspect_ratio)) =
                                (&preview_for_probe, preview_pixel_aspect_ratio)
                            {
                                preview.submit(map.as_slice(), pixel_aspect_ratio);
                            }
                        }
                        Err(err) => {
                            warn!(feed_id = %feed_id, overlay = %overlay_for_probe, "failed to map video buffer for cgen preview: {err}");
                        }
                    }
                }
                gst::PadProbeReturn::Ok
            })
            .with_context(|| format!("failed to add WGPU probe to {overlay_name}"))?;
        renderers.push(renderer);
        probe_ids.push(probe_id);
    }
    Ok(WgpuCompositorSet {
        backend: "wgpu",
        renderers,
        preview_statuses,
        _probe_ids: probe_ids,
    })
}

fn overlay_render_state_needs_frame_mutation(state: Option<&TextOverlayRenderState>) -> bool {
    state.is_some_and(|state| {
        state.sunny_cat
            || (!state.silent && !state.text.trim().is_empty())
            || !state.clock_text.trim().is_empty()
    })
}

const FRAME_PREVIEW_INTERVAL: Duration = Duration::from_millis(100);
const MAX_PREVIEW_WIDTH: usize = 960;
const MAX_PREVIEW_HEIGHT: usize = 720;
const PREVIEW_JPEG_QUALITY: u8 = 90;

#[cfg(test)]
fn square_pixel_aspect_ratio() -> CapsRational {
    CapsRational::new(1, 1).expect("square pixel aspect ratio is valid")
}

fn preview_pixel_aspect_ratio_from_pad(pad: &gst::Pad, fallback: CapsRational) -> CapsRational {
    pad.current_caps()
        .and_then(|caps| {
            caps.structure(0)?
                .get::<gst::Fraction>("pixel-aspect-ratio")
                .ok()
        })
        .and_then(|fraction| {
            let numerator = u32::try_from(fraction.numer()).ok()?;
            let denominator = u32::try_from(fraction.denom()).ok()?;
            CapsRational::new(numerator, denominator).ok()
        })
        .unwrap_or(fallback)
}

struct FramePreviewStatus {
    feed_id: String,
    worker_alive: AtomicBool,
    submitted_frames: AtomicU64,
    written_frames: AtomicU64,
    pending_frames: AtomicU64,
    queue_dropped_frames: AtomicU64,
    invalid_frames: AtomicU64,
    write_failures: AtomicU64,
    disconnected_submissions: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl FramePreviewStatus {
    fn new(feed_id: &str) -> Self {
        Self {
            feed_id: feed_id.to_string(),
            worker_alive: AtomicBool::new(false),
            submitted_frames: AtomicU64::new(0),
            written_frames: AtomicU64::new(0),
            pending_frames: AtomicU64::new(0),
            queue_dropped_frames: AtomicU64::new(0),
            invalid_frames: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            disconnected_submissions: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: Option<String>) {
        let mut last_error = self
            .last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *last_error = error;
    }

    fn status_value(&self) -> Value {
        let last_error = self
            .last_error
            .lock()
            .map(|value| value.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone());
        json!({
            "feed_id": self.feed_id,
            "worker_alive": self.worker_alive.load(Ordering::Relaxed),
            "queue_capacity": 1,
            "submitted_frames": self.submitted_frames.load(Ordering::Relaxed),
            "written_frames": self.written_frames.load(Ordering::Relaxed),
            "pending_frames": self.pending_frames.load(Ordering::Relaxed),
            "queue_dropped_frames": self.queue_dropped_frames.load(Ordering::Relaxed),
            "invalid_frames": self.invalid_frames.load(Ordering::Relaxed),
            "write_failures": self.write_failures.load(Ordering::Relaxed),
            "disconnected_submissions": self.disconnected_submissions.load(Ordering::Relaxed),
            "last_error": last_error,
            "interval_ms": FRAME_PREVIEW_INTERVAL.as_millis(),
            "max_width": MAX_PREVIEW_WIDTH,
            "max_height": MAX_PREVIEW_HEIGHT,
            "jpeg_quality": PREVIEW_JPEG_QUALITY,
            "resampler": "bilinear",
        })
    }
}

struct FramePreview {
    bgrx: Vec<u8>,
    pixel_aspect_ratio: CapsRational,
}

struct FramePreviewDispatcher {
    tx: std::sync::mpsc::SyncSender<FramePreview>,
    source_width: u32,
    source_height: u32,
    last_submit: Mutex<Option<Instant>>,
    status: Arc<FramePreviewStatus>,
}

impl FramePreviewDispatcher {
    fn spawn(
        path: PathBuf,
        source_width: u32,
        source_height: u32,
        feed_id: &str,
    ) -> (Option<Self>, Arc<FramePreviewStatus>) {
        let status = Arc::new(FramePreviewStatus::new(feed_id));
        let (tx, rx) = std::sync::mpsc::sync_channel::<FramePreview>(1);
        let status_for_worker = Arc::clone(&status);
        let mut safe_id = safe_preview_id(feed_id);
        safe_id.truncate(40);
        let thread_name = format!("cgen-preview-{safe_id}");
        status.worker_alive.store(true, Ordering::Relaxed);
        let spawn = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                while let Ok(frame) = rx.recv() {
                    let outcome = write_bgrx_preview_jpeg(
                        &path,
                        &frame.bgrx,
                        usize::try_from(source_width).unwrap_or(0),
                        usize::try_from(source_height).unwrap_or(0),
                        frame.pixel_aspect_ratio,
                    );
                    status_for_worker
                        .pending_frames
                        .fetch_sub(1, Ordering::Relaxed);
                    match outcome {
                        Ok(()) => {
                            status_for_worker
                                .written_frames
                                .fetch_add(1, Ordering::Relaxed);
                            status_for_worker.set_last_error(None);
                        }
                        Err(err) => {
                            status_for_worker
                                .write_failures
                                .fetch_add(1, Ordering::Relaxed);
                            status_for_worker.set_last_error(Some(err.to_string()));
                        }
                    }
                }
                status_for_worker
                    .worker_alive
                    .store(false, Ordering::Relaxed);
            });
        if let Err(err) = spawn {
            status.worker_alive.store(false, Ordering::Relaxed);
            status.set_last_error(Some(format!("failed to start frame preview worker: {err}")));
            warn!(feed_id, "failed to start cgen frame preview worker: {err}");
            return (None, status);
        }
        (
            Some(Self {
                tx,
                source_width,
                source_height,
                last_submit: Mutex::new(None),
                status: Arc::clone(&status),
            }),
            status,
        )
    }

    fn claim_due(&self) -> bool {
        self.claim_due_at(Instant::now())
    }

    fn claim_due_at(&self, now: Instant) -> bool {
        let mut last_submit = self
            .last_submit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let due = last_submit
            .map(|last| now.saturating_duration_since(last) >= FRAME_PREVIEW_INTERVAL)
            .unwrap_or(true);
        if due {
            *last_submit = Some(now);
        }
        due
    }

    fn submit(&self, frame: &[u8], pixel_aspect_ratio: CapsRational) {
        let expected = usize::try_from(self.source_width)
            .unwrap_or(0)
            .saturating_mul(usize::try_from(self.source_height).unwrap_or(0))
            .saturating_mul(4);
        if expected == 0 || frame.len() < expected {
            self.status.invalid_frames.fetch_add(1, Ordering::Relaxed);
            self.status.set_last_error(Some(format!(
                "short preview frame: got {} bytes, expected {expected}",
                frame.len()
            )));
            return;
        }
        let preview = FramePreview {
            bgrx: frame[..expected].to_vec(),
            pixel_aspect_ratio,
        };
        self.status.pending_frames.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(preview) {
            Ok(()) => {
                self.status.submitted_frames.fetch_add(1, Ordering::Relaxed);
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                self.status.pending_frames.fetch_sub(1, Ordering::Relaxed);
                self.status
                    .queue_dropped_frames
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                self.status.pending_frames.fetch_sub(1, Ordering::Relaxed);
                self.status
                    .disconnected_submissions
                    .fetch_add(1, Ordering::Relaxed);
                self.status
                    .set_last_error(Some("frame preview worker disconnected".to_string()));
            }
        }
    }
}

#[cfg(test)]
impl FramePreviewDispatcher {
    fn test_without_worker(
        source_width: u32,
        source_height: u32,
    ) -> (Self, std::sync::mpsc::Receiver<FramePreview>) {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let status = Arc::new(FramePreviewStatus::new("test"));
        (
            Self {
                tx,
                source_width,
                source_height,
                last_submit: Mutex::new(None),
                status,
            },
            rx,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct LinearSample {
    lower: usize,
    upper: usize,
    upper_weight: f64,
}

fn linear_samples(source_length: usize, output_length: usize) -> Vec<LinearSample> {
    let scale = source_length as f64 / output_length as f64;
    (0..output_length)
        .map(|output_index| {
            let position = ((output_index as f64 + 0.5) * scale - 0.5)
                .clamp(0.0, source_length.saturating_sub(1) as f64);
            let lower = position.floor() as usize;
            LinearSample {
                lower,
                upper: lower.saturating_add(1).min(source_length - 1),
                upper_weight: position - lower as f64,
            }
        })
        .collect()
}

fn preview_output_dimensions(
    width: usize,
    height: usize,
    pixel_aspect_ratio: CapsRational,
) -> Result<(usize, usize)> {
    if width == 0 || height == 0 {
        bail!("preview dimensions must be non-zero");
    }
    let pixel_ratio =
        f64::from(pixel_aspect_ratio.numerator()) / f64::from(pixel_aspect_ratio.denominator());
    let normalized_width = width as f64 * pixel_ratio.max(1.0);
    let normalized_height = height as f64 * pixel_ratio.recip().max(1.0);
    let fit_scale = (MAX_PREVIEW_WIDTH as f64 / normalized_width)
        .min(MAX_PREVIEW_HEIGHT as f64 / normalized_height)
        .min(1.0);
    let output_width = (normalized_width * fit_scale)
        .round()
        .clamp(1.0, MAX_PREVIEW_WIDTH as f64) as usize;
    let output_height = (normalized_height * fit_scale)
        .round()
        .clamp(1.0, MAX_PREVIEW_HEIGHT as f64) as usize;
    Ok((output_width, output_height))
}

fn resample_bgrx_to_rgb(
    frame: &[u8],
    width: usize,
    height: usize,
    output_width: usize,
    output_height: usize,
) -> Result<Vec<u8>> {
    if width == 0 || height == 0 || output_width == 0 || output_height == 0 {
        bail!("preview source and output dimensions must be non-zero");
    }
    let expected = width
        .checked_mul(height)
        .and_then(|pixels| pixels.checked_mul(4))
        .context("preview source dimensions exceed addressable memory")?;
    if frame.len() < expected {
        bail!(
            "short preview frame: got {} bytes, expected {expected}",
            frame.len()
        );
    }
    let output_len = output_width
        .checked_mul(output_height)
        .and_then(|pixels| pixels.checked_mul(3))
        .context("preview output dimensions exceed addressable memory")?;
    let mut rgb = vec![0u8; output_len];
    if output_width == width && output_height == height {
        for (source, output) in frame[..expected]
            .chunks_exact(4)
            .zip(rgb.chunks_exact_mut(3))
        {
            output.copy_from_slice(&[source[2], source[1], source[0]]);
        }
        return Ok(rgb);
    }

    let vertical = linear_samples(height, output_height);
    if output_width == width {
        for (output_y, vertical_sample) in vertical.iter().copied().enumerate() {
            for output_x in 0..output_width {
                let top = (vertical_sample.lower * width + output_x) * 4;
                let bottom = (vertical_sample.upper * width + output_x) * 4;
                let output = (output_y * output_width + output_x) * 3;
                for (output_channel, source_channel) in [2, 1, 0].into_iter().enumerate() {
                    rgb[output + output_channel] = interpolate_byte(
                        frame[top + source_channel],
                        frame[bottom + source_channel],
                        vertical_sample.upper_weight,
                    );
                }
            }
        }
        return Ok(rgb);
    }

    let horizontal = linear_samples(width, output_width);
    if output_height == height {
        for output_y in 0..output_height {
            for (output_x, horizontal_sample) in horizontal.iter().copied().enumerate() {
                let left = (output_y * width + horizontal_sample.lower) * 4;
                let right = (output_y * width + horizontal_sample.upper) * 4;
                let output = (output_y * output_width + output_x) * 3;
                for (output_channel, source_channel) in [2, 1, 0].into_iter().enumerate() {
                    rgb[output + output_channel] = interpolate_byte(
                        frame[left + source_channel],
                        frame[right + source_channel],
                        horizontal_sample.upper_weight,
                    );
                }
            }
        }
        return Ok(rgb);
    }

    for (output_y, vertical_sample) in vertical.iter().copied().enumerate() {
        for (output_x, horizontal_sample) in horizontal.iter().copied().enumerate() {
            let top_left = (vertical_sample.lower * width + horizontal_sample.lower) * 4;
            let top_right = (vertical_sample.lower * width + horizontal_sample.upper) * 4;
            let bottom_left = (vertical_sample.upper * width + horizontal_sample.lower) * 4;
            let bottom_right = (vertical_sample.upper * width + horizontal_sample.upper) * 4;
            let output = (output_y * output_width + output_x) * 3;
            for (output_channel, source_channel) in [2, 1, 0].into_iter().enumerate() {
                let top = f64::from(frame[top_left + source_channel])
                    + (f64::from(frame[top_right + source_channel])
                        - f64::from(frame[top_left + source_channel]))
                        * horizontal_sample.upper_weight;
                let bottom = f64::from(frame[bottom_left + source_channel])
                    + (f64::from(frame[bottom_right + source_channel])
                        - f64::from(frame[bottom_left + source_channel]))
                        * horizontal_sample.upper_weight;
                let value = top + (bottom - top) * vertical_sample.upper_weight;
                rgb[output + output_channel] = value.round().clamp(0.0, 255.0) as u8;
            }
        }
    }
    Ok(rgb)
}

fn interpolate_byte(lower: u8, upper: u8, upper_weight: f64) -> u8 {
    let lower = f64::from(lower);
    let value = lower + (f64::from(upper) - lower) * upper_weight;
    value.round().clamp(0.0, 255.0) as u8
}

fn write_bgrx_preview_jpeg(
    path: &Path,
    frame: &[u8],
    width: usize,
    height: usize,
    pixel_aspect_ratio: CapsRational,
) -> Result<()> {
    let (out_width, out_height) = preview_output_dimensions(width, height, pixel_aspect_ratio)?;
    let rgb = resample_bgrx_to_rgb(frame, width, height, out_width, out_height)?;
    let mut jpeg = Vec::with_capacity(rgb.len().saturating_div(3));
    let encoder = jpeg_encoder::Encoder::new(&mut jpeg, PREVIEW_JPEG_QUALITY);
    encoder
        .encode(
            &rgb,
            out_width as u16,
            out_height as u16,
            jpeg_encoder::ColorType::Rgb,
        )
        .context("failed to encode cgen preview jpeg")?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("failed to create cgen preview directory")?;
    }
    crate::atomic_file::write_ephemeral(path, &jpeg)
        .context("failed to replace cgen preview file")?;
    Ok(())
}

fn safe_preview_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn ticker_state_done(state: &TextOverlayRenderState) -> bool {
    let frame_width = f64::from(state.frame_width.max(1));
    let pixels_per_second = crate::graphics::ticker_speed_px_per_second(state.speed_px_per_frame);
    let elapsed = state.started_at.elapsed().as_secs_f64();
    let x_px = frame_width + 1.0 - (elapsed * pixels_per_second);
    let text_width_px =
        ticker_required_text_width_px(&state.source_text, state.font_size, state.text_width_px);
    x_px < -ticker_exit_width_px(text_width_px, frame_width)
}

fn fixed_scroll_repeats_enabled(feed: &FeedConfig) -> bool {
    matches!(
        feed.banner
            .scroll_repeat_mode
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "fixed" | "fixed_repeats" | "count" | "count_only"
    )
}

fn base_visual_id(value: &str) -> String {
    value
        .split_once("#pass")
        .map(|(base, _)| base)
        .unwrap_or(value)
        .to_string()
}

#[cfg(test)]
fn text_overlay_state(feed: &FeedConfig, state: &RuntimeState) -> TextOverlayRenderState {
    TextOverlayController::default().next_state(feed, state, false, false)
}

fn ticker_x_absolute(
    feed: &FeedConfig,
    snapshot: &crate::graphics::PresentationSnapshot,
    text: &str,
    started_at: Instant,
) -> (f64, bool) {
    if snapshot.visual_mode == "fullscreen_alert" || snapshot.visual_mode == "overlay" {
        return (0.5, false);
    }
    let frame_width = f64::from(feed.video.width.max(1));
    let text_width = ticker_required_text_width_px(
        text,
        snapshot.font_size,
        estimated_text_width_px(text, snapshot.font_size),
    );
    let pixels_per_second =
        crate::graphics::ticker_speed_px_per_second(snapshot.ticker_speed_px_per_frame);
    let elapsed = ticker_elapsed_seconds(feed, started_at);
    let x_px = frame_width + 1.0 - (elapsed * pixels_per_second);
    let done = x_px < -ticker_exit_width_px(text_width, frame_width);
    if done {
        return (-1.0, true);
    }
    (x_px / frame_width, done)
}

fn ticker_exit_width_px(text_width_px: f64, frame_width_px: f64) -> f64 {
    let tail_padding = (frame_width_px.max(1.0) * 0.15).clamp(96.0, 480.0);
    text_width_px.max(1.0) + tail_padding
}

fn ticker_required_text_width_px(text: &str, font_size: u32, configured_width_px: f64) -> f64 {
    let chars = text.chars().count().max(1) as f64;
    configured_width_px
        .max(estimated_text_width_px(text, font_size))
        .max(chars * f64::from(font_size.max(16)) * 0.92)
        .max(1.0)
}

fn ticker_elapsed_seconds(feed: &FeedConfig, started_at: Instant) -> f64 {
    let _ = feed;
    started_at.elapsed().as_secs_f64()
}

fn ticker_overlay_window(
    feed: &FeedConfig,
    snapshot: &crate::graphics::PresentationSnapshot,
    text: &str,
    started_at: Instant,
) -> (String, f64, bool) {
    let (x_absolute, done) = ticker_x_absolute(feed, snapshot, text, started_at);
    if done {
        return (String::new(), -1.0, true);
    }
    if snapshot.visual_mode == "fullscreen_alert" || snapshot.visual_mode == "overlay" {
        return (text.to_string(), x_absolute, false);
    }

    let frame_width = f64::from(feed.video.width.max(1));
    let char_count = text.chars().count();
    if char_count == 0 {
        return (String::new(), 1.0, true);
    }
    let text_width = estimated_text_width_px(text, snapshot.font_size);
    let char_width = (text_width / char_count as f64).max(8.0);
    let x_px = x_absolute * frame_width;
    let first_visible_px = (-x_px).max(0.0);
    let first_char = ((first_visible_px / char_width).floor() as usize).saturating_sub(4);
    let char_offset_px = first_char as f64 * char_width;
    let window_left_px = x_px + char_offset_px;
    let visible_width_px = (frame_width - window_left_px).max(frame_width);
    let max_chars = ((visible_width_px / char_width).ceil() as usize)
        .saturating_add(24)
        .min(char_count.saturating_sub(first_char));
    let window_text = text.chars().skip(first_char).take(max_chars).collect();
    (window_text, window_left_px / frame_width, false)
}

fn estimated_text_width_px(text: &str, font_size: u32) -> f64 {
    let font_size = f64::from(font_size.max(16));
    let mut units: f64 = 0.0;
    for ch in text.chars() {
        units += if ch.is_whitespace() {
            0.38
        } else if ch.is_ascii_punctuation() {
            0.42
        } else if ch.is_ascii_lowercase() {
            0.58
        } else if ch.is_ascii_digit() {
            0.60
        } else {
            0.74
        };
    }
    units.max(1.0) * font_size * 1.12
}

fn feed_fps(feed: &FeedConfig) -> f64 {
    let fps = feed.video.fps.trim();
    if fps.is_empty() || fps.eq_ignore_ascii_case("source") {
        return 30.0;
    }
    if let Some((num, den)) = fps.split_once('/') {
        let num = num.trim().parse::<f64>().unwrap_or(30.0);
        let den = den.trim().parse::<f64>().unwrap_or(1.0);
        return if den <= 0.0 { 30.0 } else { num / den };
    }
    fps.parse::<f64>().unwrap_or(30.0).max(1.0)
}

fn render_draw_fps(feed: &FeedConfig) -> f64 {
    feed_fps(feed)
}

fn overlay_ypos(feed: &FeedConfig, ticker_y: i32) -> f64 {
    if feed.video.height == 0 {
        return 0.08;
    }
    (f64::from(ticker_y.max(0)) / f64::from(feed.video.height)).clamp(0.0, 1.0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AudioSelectorMode {
    Silence,
    Program,
    Priority,
}

const AUDIO_MIX_SAMPLE_RATE: u32 = 48_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AudioBus {
    Program,
    Alert,
}

impl AudioBus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Program => "program",
            Self::Alert => "alert",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct AudioMatrixRuntimeState {
    source_layout: Option<ChannelLayout>,
    ready: bool,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct AudioMixRuntime {
    spec: AudioRoutingSpec,
    output_layout: ChannelLayout,
    gains: Arc<Mutex<AudioGainController>>,
    program_matrix: Arc<Mutex<AudioMatrixRuntimeState>>,
    alert_matrix: Arc<Mutex<AudioMatrixRuntimeState>>,
}

impl AudioMixRuntime {
    fn set_mode(&self, mode: AudioSelectorMode) -> Result<()> {
        let (program, alert) = match mode {
            AudioSelectorMode::Silence => (GainDb::Muted, GainDb::Muted),
            AudioSelectorMode::Program => (self.spec.idle_program_gain, GainDb::Muted),
            AudioSelectorMode::Priority => (self.spec.alert_program_gain, self.spec.alert_gain),
        };
        let mut gains = self
            .gains
            .lock()
            .map_err(|_| anyhow::anyhow!("CGEN audio gain controller lock poisoned"))?;
        if mode == AudioSelectorMode::Priority {
            gains.prime_alert(GainDb::Muted);
        }
        gains.set_targets(
            program,
            alert,
            AUDIO_MIX_SAMPLE_RATE,
            self.spec.transition_ms,
        );
        Ok(())
    }

    fn status_value(&self) -> Value {
        let gains = self.gains.lock().ok().map(|gains| gains.status());
        json!({
            "engine": "audiomixer",
            "topology": "force_layout",
            "output_layout": channel_layout_name(self.output_layout),
            "sample_rate": AUDIO_MIX_SAMPLE_RATE,
            "transition_ms": self.spec.transition_ms,
            "program_matrix": audio_matrix_status_value(&self.program_matrix),
            "alert_matrix": audio_matrix_status_value(&self.alert_matrix),
            "program_gain_linear": gains.map(|status| status.program_current),
            "program_gain_target_linear": gains.map(|status| status.program_target),
            "alert_gain_linear": gains.map(|status| status.alert_current),
            "alert_gain_target_linear": gains.map(|status| status.alert_target),
        })
    }
}

fn audio_matrix_status_value(state: &Arc<Mutex<AudioMatrixRuntimeState>>) -> Value {
    let Ok(state) = state.lock() else {
        return json!({
            "ready": false,
            "error": "audio matrix state lock poisoned",
        });
    };
    json!({
        "ready": state.ready,
        "source_layout": state.source_layout.map(channel_layout_name),
        "error": state.error,
    })
}

fn channel_layout_name(layout: ChannelLayout) -> &'static str {
    match layout {
        ChannelLayout::Mono => "mono",
        ChannelLayout::Stereo => "stereo",
        ChannelLayout::Surround51 => "5.1",
    }
}

fn channel_mask(layout: ChannelLayout) -> u64 {
    match layout {
        ChannelLayout::Mono => 0x04,
        ChannelLayout::Stereo => 0x03,
        ChannelLayout::Surround51 => 0x3f,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoSelectorMode {
    Program,
    Standby,
    Black,
    Smpte,
}

impl VideoSelectorMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Program => "program",
            Self::Standby => "standby",
            Self::Black => "black",
            Self::Smpte => "smpte",
        }
    }

    fn pad_name(self) -> &'static str {
        match self {
            Self::Program => "sink_0",
            Self::Standby | Self::Black => "sink_1",
            Self::Smpte => "sink_2",
        }
    }

    fn no_signal(self) -> bool {
        matches!(self, Self::Standby)
    }
}

impl AudioSelectorMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Silence => "silence",
            Self::Program => "program",
            Self::Priority => "priority",
        }
    }

    fn status_text(self, priority_active: bool) -> &'static str {
        match (self, priority_active) {
            (Self::Priority, true) => "priority",
            (Self::Silence, true) => "priority_silence",
            (Self::Silence, false) => "standby_silence",
            (Self::Program, _) => "program",
            (Self::Priority, false) => "priority",
        }
    }
}

#[derive(Debug, Clone)]
struct SourceCapsRuntime {
    inner: Arc<Mutex<SourceCapsRuntimeState>>,
    writer: Option<CapsWriter>,
}

#[derive(Debug, Clone)]
struct SourceCapsRuntimeState {
    detected: Option<SourceCaps>,
    effective: SourceCaps,
    last_known: Option<SourceCaps>,
    effective_origin: &'static str,
    reconfigure: Option<SourceCaps>,
    detection_error: Option<String>,
    persistence_error: Option<String>,
}

impl SourceCapsRuntime {
    fn new(
        effective: SourceCaps,
        last_known: Option<SourceCaps>,
        effective_origin: &'static str,
        writer: Option<CapsWriter>,
        persistence_error: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SourceCapsRuntimeState {
                detected: None,
                effective,
                last_known,
                effective_origin,
                reconfigure: None,
                detection_error: None,
                persistence_error,
            })),
            writer,
        }
    }

    fn observe(&self, caps: SourceCaps) {
        let should_persist = {
            let Ok(mut state) = self.inner.lock() else {
                return;
            };
            state.detection_error = None;
            if state.detected.as_ref() != Some(&caps) {
                state.detected = Some(caps.clone());
                if caps == state.effective {
                    state.effective_origin = "detected";
                    state.reconfigure = None;
                } else {
                    state.reconfigure = Some(caps.clone());
                }
            }
            state.last_known.as_ref() != Some(&caps)
        };
        if should_persist {
            if let Some(writer) = &self.writer {
                writer.enqueue(&caps);
            }
        }
    }

    fn record_detection_error(&self, error: impl Into<String>) {
        if let Ok(mut state) = self.inner.lock() {
            state.detection_error = Some(error.into());
        }
    }

    fn reconfigure_request(&self) -> Option<SourceCaps> {
        self.inner
            .lock()
            .ok()
            .and_then(|state| state.reconfigure.clone())
    }

    fn detected_caps(&self) -> Option<SourceCaps> {
        self.inner
            .lock()
            .ok()
            .and_then(|state| state.detected.clone())
    }

    fn effective_caps(&self) -> SourceCaps {
        self.inner
            .lock()
            .map(|state| state.effective.clone())
            .unwrap_or_else(|_| SourceCaps::fallback())
    }

    fn status_value(&self) -> Value {
        let writer = self.writer.as_ref().map(CapsWriter::status);
        match self.inner.lock() {
            Ok(state) => {
                let last_known_good = writer
                    .as_ref()
                    .and_then(|status| status.last_written.clone())
                    .or_else(|| state.last_known.clone());
                json!({
                    "detected": state.detected,
                    "effective": state.effective,
                    "last_known_good": last_known_good,
                    "effective_origin": state.effective_origin,
                    "reconfigure_required": state.reconfigure.is_some(),
                    "reconfigure_target": state.reconfigure,
                    "detection_error": state.detection_error,
                    "persistence_error": state.persistence_error,
                    "persistence": writer.map(|status| json!({
                        "queued": status.queued,
                        "dropped": status.dropped,
                        "written": status.written,
                        "last_error": status.last_error,
                    })),
                })
            }
            Err(_) => json!({
                "detected": null,
                "effective": SourceCaps::fallback(),
                "effective_origin": "fallback",
                "reconfigure_required": false,
                "last_error": "source caps status lock poisoned",
            }),
        }
    }
}

fn prepare_source_caps_runtime(
    feed: &mut FeedConfig,
    base_dir: &Path,
    effective_override: Option<SourceCaps>,
) -> Result<SourceCapsRuntime> {
    let feed_id = FeedId::parse(&feed.id).context("invalid CGEN feed ID for caps state")?;
    let (last_known, writer, persistence_error) = match LastKnownCapsStore::new(base_dir, &feed_id)
    {
        Ok(store) => {
            let (last_known, load_error) = match store.load() {
                Ok(caps) => (caps, None),
                Err(err) => (None, Some(err.to_string())),
            };
            match CapsWriter::spawn(store) {
                Ok(writer) => (last_known, Some(writer), load_error),
                Err(err) => {
                    let error = match load_error {
                        Some(load_error) => format!("{load_error}; {err}"),
                        None => err.to_string(),
                    };
                    (last_known, None, Some(error))
                }
            }
        }
        Err(err) => (None, None, Some(err.to_string())),
    };

    let input_is_dummy = matches!(
        feed.program_input
            .input_type
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "dummy" | "none" | "no_input"
    );
    let (effective, origin) = if let Some(caps) = effective_override {
        (caps, "reconfigure")
    } else if input_is_dummy {
        (configured_dummy_caps(feed)?, "dummy")
    } else if let Some(caps) = last_known.clone() {
        (caps, "last_known_good")
    } else {
        (SourceCaps::fallback(), "fallback")
    };
    let deinterlace = feed.deinterlace_spec()?;
    apply_effective_source_caps(feed, &effective, deinterlace)?;
    Ok(SourceCapsRuntime::new(
        effective,
        last_known,
        origin,
        writer,
        persistence_error,
    ))
}

fn configured_dummy_caps(feed: &FeedConfig) -> Result<SourceCaps> {
    let width = if feed.program_input.width > 0 {
        feed.program_input.width
    } else {
        feed.video.width
    };
    let height = if feed.program_input.height > 0 {
        feed.program_input.height
    } else {
        feed.video.height
    };
    let frame_rate = if feed.program_input.fps.trim().is_empty() {
        feed.video.fps.as_str()
    } else {
        feed.program_input.fps.as_str()
    };
    let (frame_rate_numerator, frame_rate_denominator) =
        parse_positive_fraction(frame_rate).unwrap_or((30_000, 1_001));
    let interlaced = feed.program_input.interlaced || feed.video.interlaced;
    let (scan_mode, field_order) = if interlaced {
        let field_order = match feed
            .program_input
            .field_order
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "bff" | "bottom" | "bottom_first" | "bottom-field-first" => {
                SourceFieldOrder::BottomFieldFirst
            }
            _ => SourceFieldOrder::TopFieldFirst,
        };
        (SourceScanMode::Interlaced, field_order)
    } else {
        (SourceScanMode::Progressive, SourceFieldOrder::NotApplicable)
    };
    SourceCaps::new(
        width,
        height,
        frame_rate_numerator,
        frame_rate_denominator,
        scan_mode,
        field_order,
        1,
        1,
        "unknown",
    )
    .context("invalid configured dummy source caps")
}

fn apply_effective_source_caps(
    feed: &mut FeedConfig,
    caps: &SourceCaps,
    deinterlace: DeinterlaceSpec,
) -> Result<()> {
    feed.video.width = caps.width();
    feed.video.height = caps.height();
    let source_is_interlaced = caps.scan_mode() == SourceScanMode::Interlaced;
    let frame_rate_numerator =
        if source_is_interlaced && deinterlace.cadence == DeinterlaceCadence::Field {
            caps.frame_rate()
                .numerator()
                .checked_mul(2)
                .context("field-rate deinterlacing frame rate exceeds the supported range")?
        } else {
            caps.frame_rate().numerator()
        };
    feed.video.fps = format!(
        "{}/{}",
        frame_rate_numerator,
        caps.frame_rate().denominator()
    );
    feed.video.interlaced = false;
    feed.video.field_order = match caps.field_order() {
        SourceFieldOrder::BottomFieldFirst => "bff",
        SourceFieldOrder::NotApplicable
        | SourceFieldOrder::TopFieldFirst
        | SourceFieldOrder::Unknown => "tff",
    }
    .to_string();
    for rendition in &mut feed.ladder.videos {
        if rendition.enabled.trim().eq_ignore_ascii_case("auto") {
            rendition.width = feed.video.width;
            rendition.height = feed.video.height;
            rendition.fps.clone_from(&feed.video.fps);
            rendition.interlaced = feed.video.interlaced;
            rendition.field_order.clone_from(&feed.video.field_order);
        }
    }
    Ok(())
}

fn parse_positive_fraction(raw: &str) -> Option<(u32, u32)> {
    let raw = raw.trim();
    let (numerator, denominator) = raw.split_once('/').unwrap_or((raw, "1"));
    let numerator = numerator.trim().parse::<u32>().ok()?;
    let denominator = denominator.trim().parse::<u32>().ok()?;
    (numerator > 0 && denominator > 0).then_some((numerator, denominator))
}

#[derive(Debug, Clone, Copy)]
struct InputHealth {
    last_video_frame: Option<Instant>,
    last_audio_frame: Option<Instant>,
    video_timed_out: bool,
    audio_timed_out: bool,
}

impl Default for InputHealth {
    fn default() -> Self {
        Self {
            last_video_frame: None,
            last_audio_frame: None,
            video_timed_out: true,
            audio_timed_out: true,
        }
    }
}

impl InputHealth {
    fn mark_video_frame(&mut self) {
        self.last_video_frame = Some(Instant::now());
        self.video_timed_out = false;
    }

    fn mark_audio_frame(&mut self) {
        self.last_audio_frame = Some(Instant::now());
        self.audio_timed_out = false;
    }

    fn mark_timeout(&mut self) {
        self.video_timed_out = true;
        self.audio_timed_out = true;
    }

    fn video_connected(self, _feed: &FeedConfig) -> bool {
        !self.video_timed_out
            && self
                .last_video_frame
                .is_some_and(|frame| frame.elapsed() <= Duration::from_secs(2))
    }

    fn audio_connected(self, feed: &FeedConfig) -> bool {
        self.stream_connected(self.last_audio_frame, self.audio_timed_out, feed)
    }

    fn stream_connected(
        self,
        last_frame: Option<Instant>,
        timed_out: bool,
        feed: &FeedConfig,
    ) -> bool {
        if timed_out {
            return false;
        }
        let Some(last_frame) = last_frame else {
            return false;
        };
        let timeout = Duration::from_millis(u64::from(
            feed.sync
                .hard_reset_ms
                .max(feed.sync.source_buffer_ms)
                .clamp(100, 10_000),
        ));
        last_frame.elapsed() <= timeout
    }
}

fn input_video_connected(input_health: &Arc<Mutex<InputHealth>>, feed: &FeedConfig) -> bool {
    input_health
        .lock()
        .map(|health| health.video_connected(feed))
        .unwrap_or(false)
}

fn input_audio_connected(input_health: &Arc<Mutex<InputHealth>>, feed: &FeedConfig) -> bool {
    input_health
        .lock()
        .map(|health| health.audio_connected(feed))
        .unwrap_or(false)
}

fn input_health_status(input_health: &Arc<Mutex<InputHealth>>, feed: &FeedConfig) -> Value {
    match input_health.lock() {
        Ok(health) => {
            let video_age_ms = health
                .last_video_frame
                .map(|instant| u64::try_from(instant.elapsed().as_millis()).unwrap_or(u64::MAX));
            let audio_age_ms = health
                .last_audio_frame
                .map(|instant| u64::try_from(instant.elapsed().as_millis()).unwrap_or(u64::MAX));
            let video_connected = health.video_connected(feed);
            let audio_connected = health.audio_connected(feed);
            let unhealthy_reason = if health.video_timed_out {
                Some("source_timeout")
            } else if !video_connected {
                Some("no_decoded_video_for_2s")
            } else {
                None
            };
            json!({
                "connected": video_connected && audio_connected,
                "video_connected": video_connected,
                "audio_connected": audio_connected,
                "video_timed_out": health.video_timed_out,
                "audio_timed_out": health.audio_timed_out,
                "video_stale": !video_connected,
                "video_timeout_ms": 2000,
                "unhealthy_reason": unhealthy_reason,
                "last_program_frame_age_ms": video_age_ms,
                "last_video_frame_age_ms": video_age_ms,
                "last_audio_frame_age_ms": audio_age_ms,
            })
        }
        Err(_) => json!({
            "connected": false,
            "video_connected": false,
            "audio_connected": false,
            "video_timed_out": true,
            "audio_timed_out": true,
            "last_error": "input health lock poisoned",
        }),
    }
}

fn mark_input_timeout(input_health: &Arc<Mutex<InputHealth>>) {
    if let Ok(mut health) = input_health.lock() {
        health.mark_timeout();
    }
}

fn install_program_video_probe(
    pipeline: &gst::Pipeline,
    input_health: Arc<Mutex<InputHealth>>,
    caps_runtime: SourceCapsRuntime,
) -> Result<()> {
    let monitor = pipeline
        .by_name("program_video_monitor")
        .context("GStreamer CGEN pipeline is missing program_video_monitor")?;
    let pad = monitor
        .static_pad("src")
        .context("GStreamer CGEN program video monitor missing source pad")?;
    pad.add_probe(
        gst::PadProbeType::BUFFER | gst::PadProbeType::EVENT_DOWNSTREAM,
        move |_pad, info| {
            if info.buffer().is_some() {
                if let Ok(mut health) = input_health.lock() {
                    health.mark_video_frame();
                }
            }
            if let Some(event) = info.event() {
                if let gst::EventView::Caps(caps_event) = event.view() {
                    match source_caps_from_gstreamer(caps_event.caps()) {
                        Ok(caps) => caps_runtime.observe(caps),
                        Err(err) => caps_runtime.record_detection_error(err.to_string()),
                    }
                }
            }
            gst::PadProbeReturn::Ok
        },
    );
    Ok(())
}

fn source_caps_from_gstreamer(caps: &gst::CapsRef) -> Result<SourceCaps> {
    let structure = caps
        .structure(0)
        .context("decoded video caps contain no structures")?;
    let width = structure
        .get::<i32>("width")
        .context("decoded video caps are missing fixed width")?;
    let height = structure
        .get::<i32>("height")
        .context("decoded video caps are missing fixed height")?;
    let width = u32::try_from(width).context("decoded video caps width is invalid")?;
    let height = u32::try_from(height).context("decoded video caps height is invalid")?;
    let frame_rate = structure
        .get::<gst::Fraction>("framerate")
        .context("decoded video caps are missing fixed framerate")?;
    let frame_rate_numerator =
        u32::try_from(frame_rate.numer()).context("decoded video framerate is invalid")?;
    let frame_rate_denominator =
        u32::try_from(frame_rate.denom()).context("decoded video framerate is invalid")?;
    let pixel_aspect_ratio = structure
        .get::<gst::Fraction>("pixel-aspect-ratio")
        .unwrap_or_else(|_| gst::Fraction::new(1, 1));
    let pixel_aspect_ratio_numerator = u32::try_from(pixel_aspect_ratio.numer())
        .context("decoded video pixel aspect ratio is invalid")?;
    let pixel_aspect_ratio_denominator = u32::try_from(pixel_aspect_ratio.denom())
        .context("decoded video pixel aspect ratio is invalid")?;
    let interlace_mode = structure
        .get::<String>("interlace-mode")
        .unwrap_or_else(|_| "progressive".to_string())
        .to_ascii_lowercase();
    let (scan_mode, field_order) = if interlace_mode == "progressive" {
        (SourceScanMode::Progressive, SourceFieldOrder::NotApplicable)
    } else {
        let field_order = match structure
            .get::<String>("field-order")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "top-field-first" | "top_field_first" | "tff" => SourceFieldOrder::TopFieldFirst,
            "bottom-field-first" | "bottom_field_first" | "bff" => {
                SourceFieldOrder::BottomFieldFirst
            }
            _ => SourceFieldOrder::Unknown,
        };
        (SourceScanMode::Interlaced, field_order)
    };
    let colorimetry = structure
        .get::<String>("colorimetry")
        .unwrap_or_else(|_| "unknown".to_string());
    SourceCaps::new(
        width,
        height,
        frame_rate_numerator,
        frame_rate_denominator,
        scan_mode,
        field_order,
        pixel_aspect_ratio_numerator,
        pixel_aspect_ratio_denominator,
        colorimetry,
    )
    .context("decoded video caps failed validation")
}

fn install_audio_mix_runtime(
    pipeline: &gst::Pipeline,
    input_health: Arc<Mutex<InputHealth>>,
    spec: AudioRoutingSpec,
) -> Result<AudioMixRuntime> {
    let AudioTopologyMode::ForceLayout(output_layout) = spec.topology else {
        bail!(
            "preserve-native audio routing cannot run through the decoded single-track CGEN mixer"
        );
    };
    let gains = Arc::new(Mutex::new(AudioGainController::new(&spec)));
    let program_matrix = install_audio_matrix_probe(
        pipeline,
        "program_audio_monitor",
        "program_audio_matrix",
        AudioBus::Program,
        output_layout,
        Some(input_health),
    )?;
    let alert_matrix = install_audio_matrix_probe(
        pipeline,
        "alert_audio_monitor",
        "alert_audio_matrix",
        AudioBus::Alert,
        output_layout,
        None,
    )?;
    install_audio_gain_probe(
        pipeline,
        "program_audio_gain",
        AudioBus::Program,
        output_layout.channels(),
        Arc::clone(&gains),
    )?;
    install_audio_gain_probe(
        pipeline,
        "alert_audio_gain",
        AudioBus::Alert,
        output_layout.channels(),
        Arc::clone(&gains),
    )?;
    Ok(AudioMixRuntime {
        spec,
        output_layout,
        gains,
        program_matrix,
        alert_matrix,
    })
}

fn install_audio_matrix_probe(
    pipeline: &gst::Pipeline,
    monitor_name: &'static str,
    converter_name: &'static str,
    bus: AudioBus,
    output_layout: ChannelLayout,
    input_health: Option<Arc<Mutex<InputHealth>>>,
) -> Result<Arc<Mutex<AudioMatrixRuntimeState>>> {
    let monitor = pipeline
        .by_name(monitor_name)
        .with_context(|| format!("GStreamer CGEN pipeline is missing {monitor_name}"))?;
    let converter = pipeline
        .by_name(converter_name)
        .with_context(|| format!("GStreamer CGEN pipeline is missing {converter_name}"))?;
    converter.find_property("mix-matrix").with_context(|| {
        format!("GStreamer element {converter_name} has no mix-matrix property")
    })?;
    let pad = monitor.static_pad("src").with_context(|| {
        format!("GStreamer CGEN audio monitor {monitor_name} has no source pad")
    })?;
    let state = Arc::new(Mutex::new(AudioMatrixRuntimeState::default()));
    let state_for_probe = Arc::clone(&state);
    pad.add_probe(
        gst::PadProbeType::BUFFER | gst::PadProbeType::EVENT_DOWNSTREAM,
        move |_pad, info| {
            if info.buffer().is_some() {
                if let Some(input_health) = &input_health {
                    if let Ok(mut health) = input_health.lock() {
                        health.mark_audio_frame();
                    }
                }
                let ready = state_for_probe
                    .lock()
                    .map(|state| state.ready)
                    .unwrap_or(false);
                return if ready {
                    gst::PadProbeReturn::Ok
                } else {
                    gst::PadProbeReturn::Drop
                };
            }
            let Some(event) = info.event() else {
                return gst::PadProbeReturn::Ok;
            };
            let gst::EventView::Caps(caps_event) = event.view() else {
                return gst::PadProbeReturn::Ok;
            };
            let result = audio_matrix_from_caps(caps_event.caps(), bus, output_layout).and_then(
                |(source_layout, matrix)| {
                    set_gstreamer_mix_matrix(&converter, &matrix)?;
                    Ok(source_layout)
                },
            );
            if let Ok(mut state) = state_for_probe.lock() {
                match result {
                    Ok(source_layout) => {
                        state.source_layout = Some(source_layout);
                        state.ready = true;
                        state.error = None;
                    }
                    Err(err) => {
                        warn!(
                            audio_bus = bus.as_str(),
                            "CGEN audio matrix rejected caps: {err:#}"
                        );
                        state.source_layout = None;
                        state.ready = false;
                        state.error = Some(err.to_string());
                    }
                }
            }
            gst::PadProbeReturn::Ok
        },
    )
    .with_context(|| format!("failed to install {bus:?} audio matrix probe"))?;
    Ok(state)
}

fn audio_matrix_from_caps(
    caps: &gst::CapsRef,
    bus: AudioBus,
    output_layout: ChannelLayout,
) -> Result<(ChannelLayout, MixMatrix)> {
    let structure = caps
        .structure(0)
        .context("decoded audio caps contain no structures")?;
    let format = structure
        .get::<String>("format")
        .context("decoded audio caps are missing a fixed sample format")?;
    if format != "F32LE" {
        bail!("decoded audio sample format {format} is not F32LE");
    }
    let rate = structure
        .get::<i32>("rate")
        .context("decoded audio caps are missing a fixed sample rate")?;
    if rate != i32::try_from(AUDIO_MIX_SAMPLE_RATE).unwrap_or(48_000) {
        bail!("decoded audio sample rate {rate} is not {AUDIO_MIX_SAMPLE_RATE}");
    }
    let channels = structure
        .get::<i32>("channels")
        .context("decoded audio caps are missing a fixed channel count")?;
    let source_layout = channel_layout_from_count(channels)?;
    let matrix = match bus {
        AudioBus::Program => MixMatrix::for_program(source_layout, output_layout)?,
        AudioBus::Alert => alert_mix_matrix(source_layout, output_layout)?,
    };
    Ok((source_layout, matrix))
}

fn channel_layout_from_count(channels: i32) -> Result<ChannelLayout> {
    match channels {
        1 => Ok(ChannelLayout::Mono),
        2 => Ok(ChannelLayout::Stereo),
        6 => Ok(ChannelLayout::Surround51),
        _ => bail!(
            "unsupported decoded audio channel count {channels}; CGEN supports mono, stereo, or 5.1"
        ),
    }
}

fn alert_mix_matrix(
    source_layout: ChannelLayout,
    output_layout: ChannelLayout,
) -> Result<MixMatrix> {
    let source_to_mono = MixMatrix::for_program(source_layout, ChannelLayout::Mono)?;
    let destination_channels = usize::from(output_layout.channels());
    let mut coefficients = Vec::with_capacity(
        usize::from(source_layout.channels()).saturating_mul(destination_channels),
    );
    for coefficient in source_to_mono.coefficients {
        coefficients.extend(std::iter::repeat(coefficient).take(destination_channels));
    }
    Ok(MixMatrix::new(
        source_layout.channels(),
        output_layout.channels(),
        coefficients,
    )?)
}

fn gstreamer_mix_matrix_rows(matrix: &MixMatrix) -> Vec<Vec<f32>> {
    let source_channels = usize::from(matrix.source_channels);
    let destination_channels = usize::from(matrix.destination_channels);
    (0..destination_channels)
        .map(|destination| {
            (0..source_channels)
                .map(|source| matrix.coefficients[source * destination_channels + destination])
                .collect()
        })
        .collect()
}

fn set_gstreamer_mix_matrix(converter: &gst::Element, matrix: &MixMatrix) -> Result<()> {
    if converter.find_property("mix-matrix").is_none() {
        bail!("audio converter does not expose mix-matrix");
    }
    let value = gst::Array::new(
        gstreamer_mix_matrix_rows(matrix)
            .into_iter()
            .map(|row| gst::Array::new(row)),
    );
    converter.set_property("mix-matrix", &value);
    Ok(())
}

fn install_audio_rendition_matrices(
    pipeline: &gst::Pipeline,
    plan: &GstPipelinePlan,
    source_layout: ChannelLayout,
) -> Result<()> {
    if plan.uses_output_fanout() {
        return Ok(());
    }
    for (index, audio) in plan.audios.iter().enumerate() {
        let converter_name = audio_rendition_matrix_name(audio, index);
        let converter = pipeline
            .by_name(&converter_name)
            .with_context(|| format!("GStreamer CGEN pipeline is missing {converter_name}"))?;
        let destination_layout = channel_layout_from_count(i32::from(audio.channels))?;
        let matrix = MixMatrix::for_program(source_layout, destination_layout)?;
        set_gstreamer_mix_matrix(&converter, &matrix).with_context(|| {
            format!(
                "failed to configure audio rendition {} from {} to {}",
                audio.id,
                channel_layout_name(source_layout),
                channel_layout_name(destination_layout)
            )
        })?;
    }
    Ok(())
}

fn install_audio_gain_probe(
    pipeline: &gst::Pipeline,
    gain_name: &'static str,
    bus: AudioBus,
    channels: u16,
    gains: Arc<Mutex<AudioGainController>>,
) -> Result<()> {
    let gain = pipeline
        .by_name(gain_name)
        .with_context(|| format!("GStreamer CGEN pipeline is missing {gain_name}"))?;
    let pad = gain
        .static_pad("src")
        .with_context(|| format!("GStreamer CGEN audio gain {gain_name} has no source pad"))?;
    pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
        let Some(buffer) = info.buffer_mut() else {
            return gst::PadProbeReturn::Ok;
        };
        let buffer = buffer.make_mut();
        let Ok(mut map) = buffer.map_writable() else {
            warn!(
                audio_bus = bus.as_str(),
                "failed to map CGEN audio buffer for gain ramp"
            );
            return gst::PadProbeReturn::Drop;
        };
        let Ok(mut gains) = gains.lock() else {
            return gst::PadProbeReturn::Drop;
        };
        match apply_audio_gain_f32le(map.as_mut_slice(), channels, bus, &mut gains) {
            Ok(()) => gst::PadProbeReturn::Ok,
            Err(err) => {
                warn!(
                    audio_bus = bus.as_str(),
                    "dropping malformed CGEN audio buffer: {err:#}"
                );
                gst::PadProbeReturn::Drop
            }
        }
    })
    .with_context(|| format!("failed to install {bus:?} audio gain probe"))?;
    Ok(())
}

fn apply_audio_gain_f32le(
    bytes: &mut [u8],
    channels: u16,
    bus: AudioBus,
    gains: &mut AudioGainController,
) -> Result<()> {
    let frame_bytes = usize::from(channels).saturating_mul(std::mem::size_of::<f32>());
    if frame_bytes == 0 || bytes.len() % frame_bytes != 0 {
        bail!("audio buffer is not aligned to {channels} F32LE channels");
    }
    for frame in bytes.chunks_exact_mut(frame_bytes) {
        let gain = match bus {
            AudioBus::Program => gains.next_program_gain(),
            AudioBus::Alert => gains.next_alert_gain(),
        };
        for sample in frame.chunks_exact_mut(std::mem::size_of::<f32>()) {
            let mut encoded = [0_u8; std::mem::size_of::<f32>()];
            encoded.copy_from_slice(sample);
            let scaled = f32::from_le_bytes(encoded) * gain;
            sample.copy_from_slice(&scaled.to_le_bytes());
        }
    }
    Ok(())
}

fn desired_video_selector_mode(
    feed: &FeedConfig,
    state: &RuntimeState,
    input_connected: bool,
) -> VideoSelectorMode {
    let control = state.control_for(feed.id.as_str());
    let mode = control
        .and_then(|control| control.string_field("mode"))
        .unwrap_or(feed.state.mode.as_str());
    let smpte_bars = control
        .and_then(|control| control.bool_field("smpte_bars"))
        .unwrap_or(feed.state.smpte_bars);
    if smpte_bars || mode.eq_ignore_ascii_case("smpte") {
        VideoSelectorMode::Smpte
    } else if mode.eq_ignore_ascii_case("black") {
        VideoSelectorMode::Black
    } else if mode.eq_ignore_ascii_case("standby") || !input_connected {
        VideoSelectorMode::Standby
    } else {
        VideoSelectorMode::Program
    }
}

#[cfg(test)]
fn desired_audio_selector_mode(
    feed: &FeedConfig,
    state: &RuntimeState,
    input_connected: bool,
) -> AudioSelectorMode {
    let status = if let Some(audio) = priority_audio_for_feed(feed, state) {
        PriorityAudioSourceStatus {
            status: "waiting",
            queue_id: Some(audio.queue_id.clone()),
            error: None,
        }
    } else {
        PriorityAudioSourceStatus {
            status: "idle",
            queue_id: None,
            error: None,
        }
    };
    desired_audio_selector_mode_with_status(feed, input_connected, &status)
}

fn desired_audio_selector_mode_with_status(
    feed: &FeedConfig,
    input_connected: bool,
    feeder_status: &PriorityAudioSourceStatus,
) -> AudioSelectorMode {
    if feeder_status.queue_id.is_some() {
        AudioSelectorMode::Priority
    } else if !feed.priority_input.routine_audio_enabled() {
        AudioSelectorMode::Silence
    } else if !input_connected || mute_standby_routine_applies(feed) {
        AudioSelectorMode::Silence
    } else {
        AudioSelectorMode::Program
    }
}

fn mute_standby_routine_applies(feed: &FeedConfig) -> bool {
    feed.audio.mute_standby_routine && !feed.audio.idle.eq_ignore_ascii_case("source")
}

fn priority_audio_active(feed: &FeedConfig, state: &RuntimeState) -> bool {
    if !feed.priority_input.priority_audio_enabled() {
        return false;
    }
    let feed_id = configured_priority_feed_id(feed);
    state.priority_audio_for(feed_id).is_some()
}

fn priority_audio_for_feed<'a>(
    feed: &FeedConfig,
    state: &'a RuntimeState,
) -> Option<&'a crate::state::PriorityAudio> {
    if !feed.priority_input.priority_audio_enabled() {
        return None;
    }
    state.priority_audio_for(configured_priority_feed_id(feed))
}

fn configured_priority_feed_id(feed: &FeedConfig) -> &str {
    let configured = if feed.alert.feed_id.trim().is_empty() {
        feed.priority_input.feed_id.trim()
    } else {
        feed.alert.feed_id.trim()
    };
    if configured.is_empty() || configured == "*" {
        feed.id.as_str()
    } else {
        configured
    }
}

fn set_video_selector_mode(pipeline: &gst::Pipeline, mode: VideoSelectorMode) -> Result<()> {
    let selector = pipeline
        .by_name("video_selector")
        .context("GStreamer CGEN pipeline is missing video_selector")?;
    let pad = selector
        .static_pad(mode.pad_name())
        .with_context(|| format!("GStreamer CGEN video selector missing {}", mode.pad_name()))?;
    selector.set_property("active-pad", &pad);
    Ok(())
}

fn priority_audio_appsrc(pipeline: &gst::Pipeline) -> Result<gst_app::AppSrc> {
    pipeline
        .by_name("priority_audio_src")
        .context("GStreamer CGEN pipeline is missing priority_audio_src")?
        .downcast::<gst_app::AppSrc>()
        .map_err(|_| anyhow::anyhow!("priority_audio_src is not an appsrc"))
}

fn apply_mpegts_program_map(pipeline: &gst::Pipeline, plan: &GstPipelinePlan) -> Result<()> {
    if plan.mux_kind != MuxKind::MpegTs {
        return Ok(());
    }
    let mux = pipeline
        .by_name("mux")
        .context("GStreamer CGEN pipeline is missing mpegts mux")?;
    let program_map = plan
        .program_map
        .as_ref()
        .context("MPEG-TS CGEN plan is missing a validated program map")?;
    let structure = program_map
        .structure()
        .parse::<gst::Structure>()
        .with_context(|| "failed to parse validated MPEG-TS program map")?;
    mux.set_property("prog-map", &structure);
    Ok(())
}

fn install_output_fanout_sinks(
    pipeline: &gst::Pipeline,
    fanout: Arc<OutputFanout>,
    width: u32,
    height: u32,
    audio_layout: ChannelLayout,
) -> Result<()> {
    let width = NonZeroU32::new(width).context("master output width must be non-zero")?;
    let height = NonZeroU32::new(height).context("master output height must be non-zero")?;
    let audio_channels = NonZeroU16::new(audio_layout.channels())
        .context("master output audio layout must have channels")?;
    let video_sink = pipeline
        .by_name("master_video_sink")
        .context("isolated CGEN output pipeline is missing master video appsink")?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow::anyhow!("master video sink is not an appsink"))?;
    let audio_sink = pipeline
        .by_name("master_audio_sink")
        .context("isolated CGEN output pipeline is missing master audio appsink")?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow::anyhow!("master audio sink is not an appsink"))?;

    let video_fanout = Arc::clone(&fanout);
    let video_sequence = Arc::new(AtomicU64::new(0));
    let video_sequence_for_callback = Arc::clone(&video_sequence);
    video_sink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let Some(buffer) = sample.buffer() else {
                    return Err(gst::FlowError::Error);
                };
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let bytes = map.as_slice();
                let rows = usize::try_from(height.get()).map_err(|_| gst::FlowError::Error)?;
                if rows == 0 || bytes.len() % rows != 0 {
                    return Err(gst::FlowError::Error);
                }
                let stride =
                    u32::try_from(bytes.len() / rows).map_err(|_| gst::FlowError::Error)?;
                let stride = NonZeroU32::new(stride).ok_or(gst::FlowError::Error)?;
                let pixels = Arc::<[u8]>::from(bytes);
                let surface = CpuBgrxFrameHandle::new(width, height, stride, pixels)
                    .map_err(|_| gst::FlowError::Error)?;
                let sequence = video_sequence_for_callback.fetch_add(1, Ordering::Relaxed) + 1;
                let frame = VideoFrame {
                    sequence,
                    pts_ns: buffer.pts().map(|pts| pts.nseconds()).unwrap_or(0),
                    duration_ns: buffer
                        .duration()
                        .map(|duration| duration.nseconds())
                        .filter(|duration| *duration > 0)
                        .unwrap_or(33_366_667),
                    discontinuity: buffer.flags().contains(gst::BufferFlags::DISCONT),
                    width,
                    height,
                    surface: Arc::new(surface),
                };
                let _ = video_fanout.publish_video(Arc::new(frame));
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );

    let audio_fanout = fanout;
    let audio_sequence = Arc::new(AtomicU64::new(0));
    let audio_sequence_for_callback = Arc::clone(&audio_sequence);
    audio_sink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let Some(buffer) = sample.buffer() else {
                    return Err(gst::FlowError::Error);
                };
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let bytes = map.as_slice();
                if bytes.len() % std::mem::size_of::<f32>() != 0 {
                    return Err(gst::FlowError::Error);
                }
                let samples = bytes
                    .chunks_exact(std::mem::size_of::<f32>())
                    .map(|sample| f32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]]))
                    .collect::<Vec<_>>();
                let frames = u64::try_from(samples.len() / usize::from(audio_channels.get()))
                    .map_err(|_| gst::FlowError::Error)?;
                let duration_ns = buffer
                    .duration()
                    .map(|duration| duration.nseconds())
                    .filter(|duration| *duration > 0)
                    .unwrap_or_else(|| frames.saturating_mul(1_000_000_000) / 48_000);
                let sequence = audio_sequence_for_callback.fetch_add(1, Ordering::Relaxed) + 1;
                let packet = AudioPacket {
                    sequence,
                    pts_ns: buffer.pts().map(|pts| pts.nseconds()).unwrap_or(0),
                    duration_ns,
                    discontinuity: buffer.flags().contains(gst::BufferFlags::DISCONT),
                    sample_rate: NonZeroU32::new(48_000).expect("constant sample rate is non-zero"),
                    channels: audio_channels,
                    payload: AudioPayload::InterleavedF32(Arc::from(samples)),
                };
                let _ = audio_fanout.publish_audio(Arc::new(packet));
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PriorityAudioSourceStatus {
    status: &'static str,
    queue_id: Option<String>,
    error: Option<String>,
}

impl PriorityAudioSourceStatus {
    fn is_live_priority(&self) -> bool {
        self.status == "active"
    }
}

struct PriorityAudioFeeder {
    appsrc: gst_app::AppSrc,
    media_pcm: MediaPcmSubscription,
    active_queue_id: Option<String>,
    last_sequence: Option<u64>,
    last_source_pts_ns: Option<u64>,
    source_pts_origin_ns: Option<u64>,
    running_pts_origin: Option<gst::ClockTime>,
    current_caps: Option<(u32, u16)>,
    last_chunk_at: Option<Instant>,
    error: Option<String>,
}

impl PriorityAudioFeeder {
    fn new(appsrc: gst_app::AppSrc, media_pcm: MediaPcmSubscription) -> Self {
        appsrc.set_format(gst::Format::Time);
        appsrc.set_is_live(true);
        Self {
            appsrc,
            media_pcm,
            active_queue_id: None,
            last_sequence: None,
            last_source_pts_ns: None,
            source_pts_origin_ns: None,
            running_pts_origin: None,
            current_caps: None,
            last_chunk_at: None,
            error: None,
        }
    }

    fn update(&mut self, audio: Option<&crate::state::PriorityAudio>) -> PriorityAudioSourceStatus {
        let Some(audio) = audio else {
            self.reset();
            return PriorityAudioSourceStatus {
                status: "idle",
                queue_id: None,
                error: None,
            };
        };
        if self.active_queue_id.as_deref() != Some(audio.queue_id.as_str()) {
            self.reset();
            self.active_queue_id = Some(audio.queue_id.clone());
        }
        for chunk in self.media_pcm.drain_correlated(&audio.queue_id) {
            if let Err(err) = self.push_chunk(chunk) {
                if self.error.is_none() {
                    warn!(
                        feed_id = %self.media_pcm.feed_id(),
                        queue_id = %audio.queue_id,
                        "correlated priority audio appsrc failed: {err:#}"
                    );
                }
                self.error = Some(err.to_string());
                self.media_pcm.record_appsrc_error();
            }
        }
        self.active_status()
    }

    fn active_status(&self) -> PriorityAudioSourceStatus {
        let Some(queue_id) = self.active_queue_id.as_ref() else {
            return PriorityAudioSourceStatus {
                status: "idle",
                queue_id: None,
                error: None,
            };
        };
        let status = if self.error.is_some() {
            "error"
        } else if self
            .last_chunk_at
            .is_some_and(|at| at.elapsed() <= Duration::from_millis(750))
        {
            "active"
        } else {
            "waiting"
        };
        PriorityAudioSourceStatus {
            status,
            queue_id: Some(queue_id.clone()),
            error: self.error.clone(),
        }
    }

    fn push_chunk(&mut self, chunk: PcmChunk) -> Result<()> {
        debug_assert_eq!(chunk.media_kind, crate::media_pcm::PcmMediaKind::Alert);
        let source_layout = channel_layout_from_count(i32::from(chunk.channels))?;
        if chunk.channel_layout != channel_layout_name(source_layout) {
            bail!(
                "correlated priority PCM layout {} is ambiguous for {} channels; expected {}",
                chunk.channel_layout,
                chunk.channels,
                channel_layout_name(source_layout)
            );
        }
        let mut discontinuity = chunk.discontinuity;
        if let Some(previous) = self.last_sequence {
            if chunk.sequence <= previous {
                self.media_pcm.record_duplicate_or_reordered();
                return Ok(());
            }
            if chunk.sequence != previous.saturating_add(1) {
                discontinuity = true;
                self.media_pcm.record_sequence_gap();
            }
        }
        if self
            .last_source_pts_ns
            .is_some_and(|previous| chunk.pts_ns < previous)
        {
            discontinuity = true;
        }
        let next_caps = (chunk.sample_rate, chunk.channels);
        if self.current_caps != Some(next_caps) {
            let caps = gst::Caps::builder("audio/x-raw")
                .field("format", "S16LE")
                .field("rate", i32::try_from(chunk.sample_rate).unwrap_or(48_000))
                .field("channels", i32::from(chunk.channels))
                .field(
                    "channel-mask",
                    gst::Bitmask::new(channel_mask(source_layout)),
                )
                .field("layout", "interleaved")
                .build();
            self.appsrc.set_caps(Some(&caps));
            self.current_caps = Some(next_caps);
            discontinuity = true;
        }
        if discontinuity || self.source_pts_origin_ns.is_none() {
            self.source_pts_origin_ns = Some(chunk.pts_ns);
            self.running_pts_origin = self
                .appsrc
                .current_running_time()
                .or(Some(gst::ClockTime::ZERO));
            self.media_pcm.record_discontinuity();
        }
        let source_origin = self.source_pts_origin_ns.unwrap_or(chunk.pts_ns);
        let running_origin = self.running_pts_origin.unwrap_or(gst::ClockTime::ZERO);
        let pts = running_origin.saturating_add(gst::ClockTime::from_nseconds(
            chunk.pts_ns.saturating_sub(source_origin),
        ));
        let mut buffer = gst::Buffer::from_mut_slice(chunk.pcm.as_ref().to_vec());
        {
            let buffer = buffer.make_mut();
            buffer.set_pts(pts);
            buffer.set_duration(gst::ClockTime::from_mseconds(u64::from(chunk.duration_ms)));
            buffer.set_offset(chunk.sequence);
            buffer.set_offset_end(chunk.sequence.saturating_add(1));
            if discontinuity {
                buffer.set_flags(gst::BufferFlags::DISCONT);
            }
        }
        self.appsrc
            .push_buffer(buffer)
            .map_err(|err| anyhow::anyhow!("failed to push correlated PCM to appsrc: {err:?}"))?;
        self.last_sequence = Some(chunk.sequence);
        self.last_source_pts_ns = Some(chunk.pts_ns);
        self.last_chunk_at = Some(Instant::now());
        self.error = None;
        Ok(())
    }

    fn reset(&mut self) {
        self.active_queue_id = None;
        self.last_sequence = None;
        self.last_source_pts_ns = None;
        self.source_pts_origin_ns = None;
        self.running_pts_origin = None;
        self.current_caps = None;
        self.last_chunk_at = None;
        self.error = None;
    }

    fn media_status_value(&self) -> Value {
        let mut status = self.media_pcm.status_value();
        if let Some(object) = status.as_object_mut() {
            object.insert(
                "active_queue_id".to_string(),
                self.active_queue_id
                    .as_ref()
                    .map_or(Value::Null, |value| Value::String(value.clone())),
            );
            object.insert(
                "last_sequence".to_string(),
                self.last_sequence
                    .map_or(Value::Null, |value| Value::from(value)),
            );
            object.insert(
                "last_source_pts_ns".to_string(),
                self.last_source_pts_ns
                    .map_or(Value::Null, |value| Value::from(value)),
            );
        }
        status
    }
}

fn publish_status(
    status_tx: &Option<watch::Sender<Option<Value>>>,
    feed: &FeedConfig,
    state_rx: &watch::Receiver<RuntimeState>,
    no_signal: bool,
    mut data: Value,
) {
    let compositor = crate::graphics::compositor_status(feed, &state_rx.borrow(), no_signal);
    if let (Some(target), Some(source)) = (data.as_object_mut(), compositor.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    if let Some(target) = data.as_object_mut() {
        target.insert("feed_id".to_string(), Value::String(feed.id.clone()));
    }
    crate::config::redact_feed_endpoint_status(feed, &mut data);
    if let Some(tx) = status_tx {
        tx.send_replace(Some(data));
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
struct PipelineDiagnostics {
    warning_count: u64,
    qos_count: u64,
    latency_recalculation_count: u64,
    last_warning: Option<String>,
    last_warning_source: Option<String>,
    last_qos: Option<QosSnapshot>,
    last_latency_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct QosSnapshot {
    source: String,
    live: bool,
    jitter_ns: i64,
    proportion: f64,
    quality: i32,
    processed: String,
    dropped: String,
}

impl PipelineDiagnostics {
    fn record_warning(&mut self, source: String, warning: &gst::message::Warning) {
        self.warning_count = self.warning_count.saturating_add(1);
        let debug = warning
            .debug()
            .map(|debug| debug.to_string())
            .unwrap_or_default();
        self.last_warning = Some(if debug.is_empty() {
            warning.error().to_string()
        } else {
            format!("{} {debug}", warning.error())
        });
        self.last_warning_source = Some(source);
    }

    fn record_latency_recalculation(&mut self, error: Option<String>) {
        self.latency_recalculation_count = self.latency_recalculation_count.saturating_add(1);
        self.last_latency_error = error;
    }

    fn record_qos(&mut self, source: String, qos: &gst::message::Qos) {
        self.qos_count = self.qos_count.saturating_add(1);
        let (jitter_ns, proportion, quality) = qos.values();
        self.last_qos = Some(QosSnapshot {
            source,
            live: qos.live(),
            jitter_ns,
            proportion,
            quality,
            processed: qos.processed().to_string(),
            dropped: qos.dropped().to_string(),
        });
    }

    fn status_value(&self) -> Value {
        json!({
            "warning_count": self.warning_count,
            "qos_count": self.qos_count,
            "latency_recalculation_count": self.latency_recalculation_count,
            "last_warning": self.last_warning,
            "last_warning_source": self.last_warning_source,
            "last_latency_error": self.last_latency_error,
            "last_qos": self.last_qos.as_ref().map(QosSnapshot::status_value),
        })
    }
}

impl QosSnapshot {
    fn status_value(&self) -> Value {
        json!({
            "source": self.source,
            "live": self.live,
            "jitter_ns": self.jitter_ns,
            "proportion": self.proportion,
            "quality": self.quality,
            "processed": self.processed,
            "dropped": self.dropped,
        })
    }
}

fn message_src_path(message: &gst::Message) -> String {
    message
        .src()
        .map(|src| src.path_string())
        .unwrap_or_else(|| "unknown".into())
        .to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GstDeinterlacePlan {
    requested: DeinterlaceSpec,
    effective_backend: DeinterlaceBackend,
    implementation: &'static str,
    fragment: String,
    required_elements: Vec<String>,
}

impl GstDeinterlacePlan {
    fn from_spec(spec: DeinterlaceSpec) -> Result<Self> {
        let backend = match (spec.algorithm, spec.backend) {
            (DeinterlaceAlgorithm::Yadif, DeinterlaceBackend::Auto) => DeinterlaceBackend::Software,
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::Auto) => {
                Self::best_motion_adaptive_backend()
            }
            (DeinterlaceAlgorithm::Bwdif, DeinterlaceBackend::Auto) => {
                bail!(
                    "BWDIF requires an in-process GStreamer libavfilter bridge; select YADIF software on this runtime"
                )
            }
            (_, backend) => backend,
        };
        let software_fragment = |method: &str| {
            let fields = match (spec.cadence, spec.parity) {
                (DeinterlaceCadence::Field, _) => "all",
                (DeinterlaceCadence::Frame, DeinterlaceParity::TopFirst) => "top",
                (DeinterlaceCadence::Frame, DeinterlaceParity::BottomFirst) => "bottom",
                (DeinterlaceCadence::Frame, DeinterlaceParity::Auto) => "auto",
            };
            format!(
                "deinterlace method={method} mode=auto-strict fields={fields} locking=passive tff={}",
                spec.parity.as_str()
            )
        };
        let plan = match (spec.algorithm, backend) {
            (DeinterlaceAlgorithm::Yadif, DeinterlaceBackend::Software) => Self {
                requested: spec,
                effective_backend: backend,
                implementation: "gstreamer_deinterlace_yadif",
                fragment: software_fragment("yadif"),
                required_elements: vec!["deinterlace".to_string()],
            },
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::Software) => Self {
                requested: spec,
                effective_backend: backend,
                implementation: "gstreamer_deinterlace_greedyh",
                fragment: software_fragment("greedyh"),
                required_elements: vec!["deinterlace".to_string()],
            },
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::Vaapi) => {
                Self::require_auto_parity(spec, backend)?;
                Self::require_elements(&["vadeinterlace"], spec, backend)?;
                Self {
                    requested: spec,
                    effective_backend: backend,
                    implementation: "gstreamer_vaapi_adaptive",
                    fragment:
                        "videoconvert ! video/x-raw,format=NV12 ! vadeinterlace method=adaptive"
                            .to_string(),
                    required_elements: vec![
                        "videoconvert".to_string(),
                        "vadeinterlace".to_string(),
                    ],
                }
            }
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::QuickSync) => {
                Self::require_auto_parity(spec, backend)?;
                Self::require_elements(&["msdkvpp"], spec, backend)?;
                Self {
                    requested: spec,
                    effective_backend: backend,
                    implementation: "gstreamer_msdk_advanced",
                    fragment: "videoconvert ! video/x-raw,format=NV12 ! msdkvpp deinterlace-mode=auto deinterlace-method=advanced".to_string(),
                    required_elements: vec!["videoconvert".to_string(), "msdkvpp".to_string()],
                }
            }
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::D3d11) => {
                Self::require_auto_parity(spec, backend)?;
                Self::require_elements(&["d3d11deinterlace"], spec, backend)?;
                Self {
                    requested: spec,
                    effective_backend: backend,
                    implementation: "gstreamer_d3d11_best",
                    fragment: "d3d11deinterlace qos=true".to_string(),
                    required_elements: vec!["d3d11deinterlace".to_string()],
                }
            }
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::OpenGl) => {
                Self::require_auto_parity(spec, backend)?;
                Self::require_elements(
                    &["glupload", "gldeinterlace", "gldownload"],
                    spec,
                    backend,
                )?;
                Self {
                    requested: spec,
                    effective_backend: backend,
                    implementation: "gstreamer_opengl_greedyh",
                    fragment:
                        "glupload ! gldeinterlace method=greedyh ! gldownload".to_string(),
                    required_elements: vec![
                        "glupload".to_string(),
                        "gldeinterlace".to_string(),
                        "gldownload".to_string(),
                    ],
                }
            }
            (DeinterlaceAlgorithm::Bwdif, _) => bail!(
                "BWDIF with backend {} is unavailable because the native GStreamer pipeline has no in-process BWDIF filter",
                backend.as_str()
            ),
            (DeinterlaceAlgorithm::Yadif, DeinterlaceBackend::Cuda) => bail!(
                "YADIF CUDA is unavailable because the native GStreamer pipeline has no in-process YADIF CUDA filter"
            ),
            (DeinterlaceAlgorithm::Yadif, _) => bail!(
                "YADIF is unavailable on backend {}; select software",
                backend.as_str()
            ),
            (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::Cuda)
            | (DeinterlaceAlgorithm::MotionAdaptive, DeinterlaceBackend::Vulkan) => bail!(
                "motion-adaptive deinterlacing is unavailable on backend {}",
                backend.as_str()
            ),
            (_, DeinterlaceBackend::Auto) => unreachable!("auto backend is resolved above"),
        };
        Ok(plan)
    }

    fn best_motion_adaptive_backend() -> DeinterlaceBackend {
        [
            ("d3d11deinterlace", DeinterlaceBackend::D3d11),
            ("msdkvpp", DeinterlaceBackend::QuickSync),
            ("vadeinterlace", DeinterlaceBackend::Vaapi),
            ("gldeinterlace", DeinterlaceBackend::OpenGl),
        ]
        .into_iter()
        .find_map(|(element, backend)| {
            gst::ElementFactory::find(element)
                .is_some()
                .then_some(backend)
        })
        .unwrap_or(DeinterlaceBackend::Software)
    }

    fn require_auto_parity(spec: DeinterlaceSpec, backend: DeinterlaceBackend) -> Result<()> {
        if spec.parity == DeinterlaceParity::Auto {
            return Ok(());
        }
        bail!(
            "backend {} reads field order from video metadata and requires deinterlace_parity=auto",
            backend.as_str()
        )
    }

    fn require_elements(
        elements: &[&str],
        spec: DeinterlaceSpec,
        backend: DeinterlaceBackend,
    ) -> Result<()> {
        let missing = elements
            .iter()
            .filter(|element| gst::ElementFactory::find(**element).is_none())
            .copied()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        bail!(
            "{} deinterlacing on backend {} is unavailable; missing GStreamer element(s): {}",
            spec.algorithm.as_str(),
            backend.as_str(),
            missing.join(", ")
        )
    }

    fn status_value(&self) -> Value {
        json!({
            "algorithm": self.requested.algorithm.as_str(),
            "requested_backend": self.requested.backend.as_str(),
            "effective_backend": self.effective_backend.as_str(),
            "cadence": self.requested.cadence.as_str(),
            "parity": self.requested.parity.as_str(),
            "implementation": self.implementation,
            "field_rate_output": self.requested.cadence == DeinterlaceCadence::Field,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GstPipelinePlan {
    description: String,
    videos: Vec<PlannedVideoRendition>,
    audios: Vec<PlannedAudioRendition>,
    program_map: Option<ValidatedGstProgramMap>,
    requested_program_map: Option<ResolvedProgramMapSpec>,
    mux_kind: MuxKind,
    required_elements: Vec<String>,
    deinterlace: GstDeinterlacePlan,
    graphics_backend: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedVideoRendition {
    id: String,
    program: i32,
    video_pid: i32,
    pmt_pid: i32,
    width: u32,
    height: u32,
    fps: String,
    interlaced: bool,
    field_order: String,
    standard: String,
    codec: String,
    bitrate_kbps: Option<u32>,
    encoder: EncoderCodecSettings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedAudioRendition {
    id: String,
    program: i32,
    audio_pid: i32,
    channels: u16,
    codec: String,
    bitrate_bps: i64,
    language: String,
    encoder: EncoderCodecSettings,
}

impl GstPipelinePlan {
    fn uses_output_fanout(&self) -> bool {
        self.mux_kind == MuxKind::Fanout
    }

    fn from_feed(feed: &FeedConfig) -> Result<Self> {
        Self::from_feed_with_program_map(feed, None)
    }

    fn from_feed_with_program_map(
        feed: &FeedConfig,
        input_program_map: Option<&InputProgramMapSnapshot>,
    ) -> Result<Self> {
        let audio_routing = feed.audio_routing_spec()?;
        let audio_output_layout = forced_audio_output_layout(&audio_routing)?;
        let deinterlace = GstDeinterlacePlan::from_spec(feed.deinterlace_spec()?)?;
        let source = InputSourceFragment::from_feed(feed)?;
        let resolved_output = feed.resolved_program_output_url();
        let sink = if feed.has_explicit_outputs() {
            SinkFragment::fanout()
        } else {
            SinkFragment::from_url(&resolved_output, feed)?
        };
        let mut videos = feed.enabled_video_renditions(feed.video.width, feed.video.height);
        let mut audios = feed.enabled_audio_renditions();
        if sink.mux_kind == MuxKind::Flv {
            videos.truncate(1);
            audios.truncate(1);
        }
        let resolved_program_map = match sink.mux_kind {
            MuxKind::MpegTs => Some(
                effective_resolved_program_map(feed, input_program_map)?
                    .context("MPEG-TS CGEN output requires a program map")?,
            ),
            MuxKind::Fanout => effective_resolved_program_map(feed, input_program_map)?,
            MuxKind::Flv => None,
        };
        if feed.program_mapping_mode()? == ProgramMappingMode::Source {
            if let (Some(input), Some(video)) = (input_program_map, videos.first_mut()) {
                video.program = Some(i32::from(input.selected_program.program_number.get()));
            }
        }
        let (planned_videos, planned_audios) = if sink.mux_kind == MuxKind::Fanout {
            planned_master_tracks(feed, &videos, audio_output_layout)?
        } else if let Some(resolved) = &resolved_program_map {
            planned_mpegts_tracks(feed, &videos, &audios, resolved)?
        } else {
            let planned_videos = videos
                .iter()
                .enumerate()
                .map(|(index, video)| PlannedVideoRendition::from_config(feed, video, index))
                .collect::<Vec<_>>();
            let planned_audios = planned_videos
                .iter()
                .flat_map(|video| {
                    audios.iter().enumerate().map(|(audio_index, audio)| {
                        PlannedAudioRendition::from_config(
                            feed,
                            audio,
                            video.program,
                            audio_pid_for(video.video_pid, audio_index),
                        )
                    })
                })
                .collect::<Vec<_>>();
            (planned_videos, planned_audios)
        };
        let video_input = source.video_branch();
        let audio_input = source.audio_branch();
        let queue = queue_fragment(feed, QueueLeak::None);
        let video_live_queue = queue_fragment(feed, QueueLeak::Downstream);
        let audio_live_queue = queue_fragment(feed, QueueLeak::Downstream);
        let priority_max_bytes = priority_appsrc_max_bytes(feed);
        let priority_max_latency = queue_time_ns(feed);
        let audio_mix_caps = audio_mix_caps_fragment(audio_output_layout);
        let standby_caps = source_video_caps_fragment(feed);
        let (video_branches, audio_branches, output_tail) = if sink.mux_kind == MuxKind::Fanout {
            (
                format!(
                    "video_tee. ! {video_live_queue} ! videoconvert ! videoscale method=lanczos add-borders=true ! videorate ! {standby_caps},format=BGRx ! identity name=cgen_overlay_master silent=true ! appsink name=master_video_sink emit-signals=false sync=false max-buffers=1 drop=true"
                ),
                format!(
                    "audio_tee. ! {audio_live_queue} ! audioconvert ! audioresample ! {audio_mix_caps} ! appsink name=master_audio_sink emit-signals=false sync=false max-buffers=32 drop=true"
                ),
                String::new(),
            )
        } else {
            let video_branches = videos
                .iter()
                .enumerate()
                .map(|(index, video)| {
                    video_rendition_branch(
                        feed,
                        video,
                        &planned_videos[index],
                        index,
                        sink.mux_kind,
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            let audio_branches = planned_audios
                .iter()
                .enumerate()
                .map(|(index, audio)| audio_rendition_branch(feed, audio, index, sink.mux_kind))
                .collect::<Vec<_>>()
                .join(" ");
            let mux = mux_fragment(feed, sink.mux_kind);
            let output_tail = format!("{mux} ! {queue} ! {}", sink.description);
            (video_branches, audio_branches, output_tail)
        };
        let program_video_input = format!(
            "{video_input} ! identity name=program_video_monitor silent=true ! {} ! {video_live_queue} ! videoconvert ! videoscale method=lanczos add-borders=true ! videorate ! {standby_caps} ! video_selector.sink_0",
            deinterlace.fragment.as_str()
        );
        let description = format!(
            "{} \
             {program_video_input} \
             videotestsrc name=standby_video_src pattern=black is-live=true do-timestamp=true ! {standby_caps} ! {queue} ! video_selector.sink_1 \
             videotestsrc name=smpte_video_src pattern=smpte is-live=true do-timestamp=true ! {standby_caps} ! {queue} ! video_selector.sink_2 \
             input-selector name=video_selector sync-streams=true sync-mode=clock cache-buffers=false drop-backwards=true ! {queue} ! tee name=video_tee \
             audiomixer name=audio_mixer ignore-inactive-pads=true latency={priority_max_latency} ! {queue} ! audioconvert ! audioresample ! audiorate ! {audio_mix_caps} ! tee name=audio_tee \
             audiotestsrc wave=silence is-live=true do-timestamp=true ! {audio_mix_caps} ! {queue} ! audio_mixer. \
             {audio_input} ! {audio_live_queue} ! audioconvert ! audioresample ! audio/x-raw,format=F32LE,rate=48000,layout=interleaved ! identity name=program_audio_monitor silent=true ! audioconvert name=program_audio_matrix ! {audio_mix_caps} ! identity name=program_audio_gain silent=true ! {queue} ! audio_mixer. \
             appsrc name=priority_audio_src is-live=true block=false leaky-type=downstream stream-type=stream format=time do-timestamp=true min-latency=0 max-latency={priority_max_latency} max-bytes={priority_max_bytes} ! {queue} ! audioconvert ! audioresample ! audio/x-raw,format=F32LE,rate=48000,layout=interleaved ! identity name=alert_audio_monitor silent=true ! audioconvert name=alert_audio_matrix ! {audio_mix_caps} ! identity name=alert_audio_gain silent=true ! {queue} ! audio_mixer. \
             {video_branches} \
             {audio_branches} \
             {output_tail}"
            , source.description
        );
        let program_map = if sink.mux_kind == MuxKind::MpegTs {
            resolved_program_map
                .as_ref()
                .map(|resolved| {
                    validated_program_map_for_plan(resolved, &planned_videos, &planned_audios)
                })
                .transpose()?
        } else {
            None
        };
        let required_elements = required_elements(
            &source,
            &sink,
            &planned_videos,
            &planned_audios,
            &deinterlace,
        );
        Ok(Self {
            description,
            videos: planned_videos,
            audios: planned_audios,
            program_map,
            requested_program_map: resolved_program_map,
            mux_kind: sink.mux_kind,
            required_elements,
            deinterlace,
            graphics_backend: "wgpu",
        })
    }

    fn status_value(&self) -> Value {
        json!({
            "videos": self.videos.iter().map(PlannedVideoRendition::status_value).collect::<Vec<_>>(),
            "audios": self.audios.iter().map(PlannedAudioRendition::status_value).collect::<Vec<_>>(),
            "video_count": self.videos.len(),
            "audio_count": self.audios.len(),
            "program_map": self.program_map.as_ref()
                .map(ValidatedGstProgramMap::redacted_status_value)
                .or_else(|| self.requested_program_map.as_ref().map(redacted_resolved_program_map_status))
                .unwrap_or(Value::Null),
            "output_mode": if self.uses_output_fanout() {
                "isolated_workers"
            } else {
                "legacy_single_sink"
            },
            "required_elements": self.required_elements,
            "deinterlace": self.deinterlace.status_value(),
            "video_memory": "SystemMemory",
            "graphics_backend": self.graphics_backend,
        })
    }
}

fn forced_audio_output_layout(spec: &AudioRoutingSpec) -> Result<ChannelLayout> {
    match spec.topology {
        AudioTopologyMode::ForceLayout(layout) => Ok(layout),
        AudioTopologyMode::PreserveNativeTracks => bail!(
            "preserve-native audio routing is unavailable in the current decoded single-track CGEN runtime; select force_layout until track codec, language, layout, and ordering retention can be guaranteed"
        ),
    }
}

fn validate_gstreamer_elements(elements: &[String]) -> Result<()> {
    let missing = elements
        .iter()
        .filter(|name| gst::ElementFactory::find(name.as_str()).is_none())
        .cloned()
        .collect::<Vec<String>>();
    if missing.is_empty() {
        return Ok(());
    }
    bail!(
        "missing required GStreamer element(s): {}",
        missing.join(", ")
    )
}

fn required_elements(
    source: &InputSourceFragment,
    sink: &SinkFragment,
    videos: &[PlannedVideoRendition],
    audios: &[PlannedAudioRendition],
    deinterlace: &GstDeinterlacePlan,
) -> Vec<String> {
    let mut elements = BTreeSet::from([
        "appsrc".to_string(),
        "audioconvert".to_string(),
        "audiomixer".to_string(),
        "audiorate".to_string(),
        "audioresample".to_string(),
        "audiotestsrc".to_string(),
        "input-selector".to_string(),
        "queue".to_string(),
        "tee".to_string(),
    ]);
    elements.extend([
        "identity".to_string(),
        "videoconvert".to_string(),
        "videorate".to_string(),
        "videoscale".to_string(),
        "videotestsrc".to_string(),
    ]);
    elements.extend(deinterlace.required_elements.iter().cloned());
    elements.insert(sink.mux_kind.required_element().to_string());
    elements.extend(source.required_elements.iter().cloned());
    elements.extend(sink.required_elements.iter().cloned());
    if sink.mux_kind == MuxKind::Fanout {
        return elements.into_iter().collect();
    }
    elements.extend(
        videos
            .iter()
            .map(|video| video_encoder_element(video.codec.as_str()).to_string()),
    );
    if videos.iter().any(|video| video.interlaced) {
        elements.insert("interlace".to_string());
    }
    elements.extend(
        audios
            .iter()
            .map(|audio| audio_encoder_element(audio.codec.as_str()).to_string()),
    );
    elements.into_iter().collect()
}

impl PlannedVideoRendition {
    fn from_config(
        feed: &FeedConfig,
        video: &crate::config::VideoRenditionConfig,
        index: usize,
    ) -> Self {
        let codec = video.codec_name(feed.output().vcodec.as_str()).to_string();
        let encoder = if feed.encoder.video.applies_to(codec.as_str()) {
            feed.encoder.video.clone()
        } else {
            EncoderCodecSettings::default()
        };
        Self {
            id: video.id.clone(),
            program: video.program_number(index),
            video_pid: video.video_pid(index),
            pmt_pid: video.pmt_pid(index),
            width: video.width,
            height: video.height,
            fps: video.frame_rate_text(feed.video.fps.as_str()).to_string(),
            interlaced: video.interlaced,
            field_order: video.field_order.clone(),
            standard: video.standard.clone(),
            codec,
            bitrate_kbps: encoder
                .bitrate_or(video.bitrate_kbps.or(feed.output().video_bitrate_kbps)),
            encoder,
        }
    }

    fn status_value(&self) -> Value {
        json!({
            "id": self.id,
            "program": self.program,
            "video_pid": self.video_pid,
            "pmt_pid": self.pmt_pid,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "interlaced": self.interlaced,
            "field_order": self.field_order,
            "standard": self.standard,
            "codec": self.codec,
            "bitrate_kbps": self.bitrate_kbps,
        })
    }
}

impl PlannedAudioRendition {
    fn from_config(
        feed: &FeedConfig,
        audio: &crate::config::AudioRenditionConfig,
        program: i32,
        audio_pid: i32,
    ) -> Self {
        let codec = audio.codec_name(feed.output().acodec.as_str()).to_string();
        let encoder = if feed.encoder.audio.applies_to(codec.as_str()) {
            feed.encoder.audio.clone()
        } else {
            EncoderCodecSettings::default()
        };
        let fallback_bitrate_kbps =
            u32::try_from(audio.bitrate_bps().saturating_div(1_000)).unwrap_or(192);
        Self {
            id: audio.id.clone(),
            program,
            audio_pid,
            channels: audio.channels(),
            codec,
            bitrate_bps: i64::from(
                encoder
                    .bitrate_or(Some(fallback_bitrate_kbps))
                    .unwrap_or(fallback_bitrate_kbps),
            ) * 1_000,
            language: audio.language.clone(),
            encoder,
        }
    }

    fn status_value(&self) -> Value {
        json!({
            "id": self.id,
            "program": self.program,
            "audio_pid": self.audio_pid,
            "channels": self.channels,
            "codec": self.codec,
            "bitrate_bps": self.bitrate_bps,
            "language": self.language,
        })
    }
}

fn video_rendition_branch(
    feed: &FeedConfig,
    video: &crate::config::VideoRenditionConfig,
    planned: &PlannedVideoRendition,
    index: usize,
    mux_kind: MuxKind,
) -> String {
    let caps = video_caps_fragment(feed, video);
    let overlay_name = gst_element_name("cgen_overlay", &video.id, index);
    let encoder = video_encoder_fragment(
        planned.codec.as_str(),
        planned.bitrate_kbps,
        video.interlaced,
        video.field_order.as_str(),
        &planned.encoder,
    );
    let interlace = if video.interlaced {
        if top_field_first(video.field_order.as_str()) {
            " ! interlace field-pattern=1:1 top-field-first=true"
        } else {
            " ! interlace field-pattern=1:1 top-field-first=false"
        }
    } else {
        ""
    };
    let queue = queue_fragment(feed, QueueLeak::None);
    let leaky_queue = queue_fragment(feed, QueueLeak::Downstream);
    let mux_pad = mux_kind.video_sink_pad(planned);
    let encoder_format = video_encoder_raw_format(planned.codec.as_str());
    format!(
        "video_tee. ! {leaky_queue} ! videoconvert ! videoscale method=lanczos add-borders=true ! videorate ! {caps},format=BGRx ! identity name={overlay_name} silent=true ! videoconvert ! video/x-raw,format={encoder_format}{interlace} ! {queue} ! {encoder} ! {queue} ! {mux_pad}"
    )
}

fn audio_rendition_branch(
    feed: &FeedConfig,
    audio: &PlannedAudioRendition,
    index: usize,
    mux_kind: MuxKind,
) -> String {
    let name = gst_element_name(
        "audio",
        &format!("{}_p{}_pid{}", audio.id, audio.program, audio.audio_pid),
        index,
    );
    let matrix_name = audio_rendition_matrix_name(audio, index);
    let encoder =
        audio_encoder_fragment_bps(audio.codec.as_str(), audio.bitrate_bps, &audio.encoder);
    let queue = queue_fragment(feed, QueueLeak::None);
    let leaky_queue = queue_fragment(feed, QueueLeak::Downstream);
    let mux_pad = mux_kind.audio_sink_pad(audio);
    format!(
        "audio_tee. ! {leaky_queue} ! audioconvert name={matrix_name} ! audioresample ! audio/x-raw,rate=48000,channels={},layout=interleaved ! {queue} ! {encoder} ! {queue} name={name}_encoded ! {mux_pad}",
        audio.channels
    )
}

fn audio_rendition_matrix_name(audio: &PlannedAudioRendition, index: usize) -> String {
    gst_element_name(
        "audio_matrix",
        &format!("{}_p{}_pid{}", audio.id, audio.program, audio.audio_pid),
        index,
    )
}

fn audio_pid_for(video_pid: i32, audio_index: usize) -> i32 {
    video_pid + 1 + i32::try_from(audio_index).unwrap_or(0)
}

fn planned_mpegts_tracks(
    feed: &FeedConfig,
    videos: &[crate::config::VideoRenditionConfig],
    audios: &[crate::config::AudioRenditionConfig],
    resolved: &ResolvedProgramMapSpec,
) -> Result<(Vec<PlannedVideoRendition>, Vec<PlannedAudioRendition>)> {
    let mut planned_videos = Vec::with_capacity(videos.len());
    for (index, video) in videos.iter().enumerate() {
        let configured_program = planned_program_number(video.program_number(index), "video")?;
        let program = resolved
            .programs
            .iter()
            .find(|program| program.program_number == configured_program)
            .with_context(|| {
                format!(
                    "enabled video rendition {:?} targets program {} that is absent from the program map",
                    video.id,
                    configured_program
                )
            })?;
        let video_pid = program.video_pid.with_context(|| {
            format!(
                "enabled video rendition {:?} targets audio-only program {}",
                video.id, configured_program
            )
        })?;
        let mut planned = PlannedVideoRendition::from_config(feed, video, index);
        planned.program = i32::from(program.program_number.get());
        planned.video_pid = i32::from(video_pid.get());
        planned.pmt_pid = i32::from(program.pmt_pid.get());
        planned_videos.push(planned);
    }

    let mut planned_audios = Vec::new();
    for program in &resolved.programs {
        for (track_id, pid) in &program.audio {
            let audio = audios
                .iter()
                .find(|audio| audio.id.eq_ignore_ascii_case(track_id.as_str()))
                .with_context(|| {
                    format!(
                        "program {} maps audio track {:?}, but no matching enabled rendition exists",
                        program.program_number, track_id
                    )
                })?;
            planned_audios.push(PlannedAudioRendition::from_config(
                feed,
                audio,
                i32::from(program.program_number.get()),
                i32::from(pid.get()),
            ));
        }
    }
    Ok((planned_videos, planned_audios))
}

fn planned_master_tracks(
    feed: &FeedConfig,
    videos: &[crate::config::VideoRenditionConfig],
    audio_layout: ChannelLayout,
) -> Result<(Vec<PlannedVideoRendition>, Vec<PlannedAudioRendition>)> {
    let source_video = videos
        .first()
        .context("isolated output workers require one enabled master video rendition")?;
    let mut master_video = PlannedVideoRendition::from_config(feed, source_video, 0);
    master_video.id = "master".to_string();
    master_video.width = feed.video.width;
    master_video.height = feed.video.height;
    master_video.fps = feed.video.fps.clone();
    master_video.interlaced = feed.video.interlaced;
    master_video.field_order = feed.video.field_order.clone();
    master_video.standard = feed.video.standard.clone();
    let master_audio = PlannedAudioRendition {
        id: "master".to_string(),
        program: 0,
        audio_pid: 0,
        channels: audio_layout.channels(),
        codec: "raw_f32".to_string(),
        bitrate_bps: 0,
        language: "und".to_string(),
        encoder: EncoderCodecSettings::default(),
    };
    Ok((vec![master_video], vec![master_audio]))
}

fn validated_program_map_for_plan(
    resolved: &ResolvedProgramMapSpec,
    videos: &[PlannedVideoRendition],
    audios: &[PlannedAudioRendition],
) -> Result<ValidatedGstProgramMap> {
    let video = videos
        .iter()
        .map(|track| {
            Ok(PlannedVideoTrackAssociation {
                program_number: planned_program_number(track.program, "video")?,
                sink_pid: planned_mpegts_pid(track.video_pid, "video")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let audio = audios
        .iter()
        .map(|track| {
            Ok(PlannedAudioTrackAssociation {
                program_number: planned_program_number(track.program, "audio")?,
                track_id: AudioTrackId::parse(&track.id)
                    .with_context(|| format!("invalid audio track ID {:?}", track.id))?,
                sink_pid: planned_mpegts_pid(track.audio_pid, "audio")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    build_gstreamer_program_map(resolved, &PlannedTrackAssociations { video, audio })
        .context("MPEG-TS program map does not match planned encoder tracks")
}

fn planned_program_number(value: i32, kind: &str) -> Result<NonZeroU16> {
    let value = u16::try_from(value)
        .with_context(|| format!("planned {kind} program number {value} is outside u16"))?;
    NonZeroU16::new(value).with_context(|| format!("planned {kind} program number is zero"))
}

fn planned_mpegts_pid(value: i32, kind: &str) -> Result<MpegTsPid> {
    let value = u16::try_from(value)
        .with_context(|| format!("planned {kind} MPEG-TS PID {value} is outside u16"))?;
    MpegTsPid::new(value).with_context(|| format!("planned {kind} MPEG-TS PID is invalid"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueLeak {
    None,
    Downstream,
}

fn queue_time_ns(feed: &FeedConfig) -> u64 {
    u64::from(feed.sync.source_buffer_ms.clamp(40, 5_000)) * 1_000_000
}

const GST_QUEUE_MAX_BUFFERS: u32 = 4;
const GST_QUEUE_MIN_BYTES: u64 = 1 * 1024 * 1024;

fn queue_fragment(feed: &FeedConfig, leak: QueueLeak) -> String {
    queue_fragment_with_name(feed, leak, None)
}

fn queue_fragment_named(feed: &FeedConfig, leak: QueueLeak, name: &str) -> String {
    queue_fragment_with_name(feed, leak, Some(name))
}

fn queue_fragment_with_name(feed: &FeedConfig, leak: QueueLeak, name: Option<&str>) -> String {
    let leaky = match leak {
        QueueLeak::None => "",
        QueueLeak::Downstream => " leaky=downstream",
    };
    let name = name
        .map(|value| format!(" name={value}"))
        .unwrap_or_default();
    format!(
        "queue{name} max-size-time={} max-size-buffers={} max-size-bytes={} flush-on-eos=true{leaky}",
        queue_time_ns(feed),
        GST_QUEUE_MAX_BUFFERS,
        queue_max_bytes(feed)
    )
}

fn queue_max_bytes(feed: &FeedConfig) -> u64 {
    raw_bgrx_frame_bytes(feed.video.width, feed.video.height).max(GST_QUEUE_MIN_BYTES)
}

fn raw_bgrx_frame_bytes(width: u32, height: u32) -> u64 {
    u64::from(width)
        .saturating_mul(u64::from(height))
        .saturating_mul(4)
}

fn priority_appsrc_max_bytes(feed: &FeedConfig) -> u64 {
    let channels = feed
        .enabled_audio_renditions()
        .iter()
        .map(|audio| u64::from(audio.channels()))
        .max()
        .unwrap_or(2)
        .max(2);
    let bytes_per_ms = 48_000_u64.saturating_mul(channels).saturating_mul(2) / 1_000;
    bytes_per_ms
        .saturating_mul(u64::from(feed.sync.source_buffer_ms.clamp(40, 5_000)))
        .max(48_000)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MuxKind {
    MpegTs,
    Flv,
    Fanout,
}

impl MuxKind {
    fn required_element(self) -> &'static str {
        match self {
            Self::MpegTs => "mpegtsmux",
            Self::Flv => "flvmux",
            Self::Fanout => "appsink",
        }
    }

    fn video_sink_pad(self, video: &PlannedVideoRendition) -> String {
        match self {
            Self::MpegTs => format!("mux.sink_{}", video.video_pid),
            Self::Flv => "mux.video".to_string(),
            Self::Fanout => unreachable!("fanout uses raw master-frame appsinks"),
        }
    }

    fn audio_sink_pad(self, audio: &PlannedAudioRendition) -> String {
        match self {
            Self::MpegTs => format!("mux.sink_{}", audio.audio_pid),
            Self::Flv => "mux.audio".to_string(),
            Self::Fanout => unreachable!("fanout uses raw master-audio appsinks"),
        }
    }
}

fn mux_fragment(feed: &FeedConfig, kind: MuxKind) -> String {
    let latency_ns = queue_time_ns(feed) / 2;
    match kind {
        MuxKind::MpegTs => format!(
            "mpegtsmux name=mux alignment=7 latency={latency_ns} pat-interval=4500 pmt-interval=4500 si-interval=4500 pcr-interval=1800"
        ),
        MuxKind::Flv => "flvmux name=mux streamable=true latency=0".to_string(),
        MuxKind::Fanout => String::new(),
    }
}

fn gst_element_name(prefix: &str, id: &str, index: usize) -> String {
    let mut name = String::with_capacity(prefix.len() + id.len() + 8);
    name.push_str(prefix);
    name.push('_');
    if id.trim().is_empty() {
        name.push_str(&index.to_string());
        return name;
    }
    for ch in id.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            name.push(ch);
        } else {
            name.push('_');
        }
    }
    name
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputSourceFragment {
    description: String,
    video_pad: &'static str,
    audio_pad: &'static str,
    video_decode_chain: String,
    audio_decode_chain: String,
    required_elements: Vec<String>,
}

impl InputSourceFragment {
    fn from_feed(feed: &FeedConfig) -> Result<Self> {
        match feed
            .program_input
            .input_type
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "dummy" | "none" | "no_input" => Self::from_dummy(feed),
            "device" | "v4l2" | "directshow" | "dshow" => Self::from_device(feed),
            _ => Self::from_stream(feed),
        }
    }

    fn from_dummy(feed: &FeedConfig) -> Result<Self> {
        let foreground = gst_solid_color(feed.program_input.background.as_str());
        Ok(Self {
            description: format!(
                "videotestsrc name=video_src pattern=solid-color foreground-color={foreground} is-live=true do-timestamp=true \
                 audiotestsrc name=audio_src wave=silence is-live=true do-timestamp=true"
            ),
            video_pad: "video_src.",
            audio_pad: "audio_src.",
            video_decode_chain: format!(" ! {}", source_video_caps_fragment(feed)),
            audio_decode_chain: String::new(),
            required_elements: vec!["audiotestsrc".to_string(), "videotestsrc".to_string()],
        })
    }

    fn from_device(feed: &FeedConfig) -> Result<Self> {
        let device_id = feed.program_input.device_id.trim();
        if device_id.is_empty() {
            bail!("gstreamer cgen device input requires a persistent device id");
        }
        let backend = feed
            .program_input
            .device_backend
            .trim()
            .to_ascii_lowercase();
        let (video_source, device_property) = match backend.as_str() {
            "directshow" | "dshow" => ("dshowvideosrc", "device-name"),
            "v4l2" => ("v4l2src", "device"),
            "" if cfg!(windows) => ("dshowvideosrc", "device-name"),
            "" => ("v4l2src", "device"),
            _ => bail!("unsupported gstreamer device backend"),
        };
        Ok(Self {
            description: format!(
                "{video_source} name=video_src {device_property}={} do-timestamp=true \
                 autoaudiosrc name=audio_src",
                gst_quote(device_id)
            ),
            video_pad: "video_src.",
            audio_pad: "audio_src.",
            video_decode_chain: " ! decodebin".to_string(),
            audio_decode_chain: String::new(),
            required_elements: vec![
                video_source.to_string(),
                "autoaudiosrc".to_string(),
                "decodebin".to_string(),
            ],
        })
    }

    fn from_stream(feed: &FeedConfig) -> Result<Self> {
        let resolved_input = feed.resolved_program_input_url();
        let url = resolved_input.as_str();
        let url = url.trim();
        if url.is_empty() {
            bail!("gstreamer cgen input url is empty");
        }
        let required_decoder = feed.program_input.hardware_decoder().map(str::to_string);
        if let Some(endpoint) = UdpEndpoint::parse(url) {
            let mut required_elements = vec![
                "parsebin".to_string(),
                "tsdemux".to_string(),
                "udpsrc".to_string(),
                "decodebin".to_string(),
            ];
            if let Some(decoder) = required_decoder.as_ref() {
                required_elements.push(decoder.clone());
            }
            required_elements.push("capsfilter".to_string());
            required_elements.push("identity".to_string());
            let video_decode_chain = if let Some(decoder) = required_decoder.as_deref() {
                format!(
                    " ! capsfilter caps={} ! parsebin ! {decoder}",
                    gst_quote(MPEG_TS_VIDEO_CAPS)
                )
            } else {
                format!(
                    " ! capsfilter caps={} ! parsebin ! decodebin",
                    gst_quote(MPEG_TS_VIDEO_CAPS)
                )
            };
            let audio_decode_chain = format!(
                " ! capsfilter caps={} ! parsebin ! decodebin",
                gst_quote(MPEG_TS_AUDIO_CAPS)
            );
            return Ok(Self {
                description: format!(
                    "udpsrc address={} port={} auto-multicast=true reuse={} buffer-size={} retrieve-sender-address=false ! identity name=program_ts_monitor silent=true ! tsdemux name=src",
                    gst_quote(&endpoint.host),
                    endpoint.port,
                    endpoint.reuse,
                    endpoint.buffer_size.unwrap_or(DEFAULT_UDP_BUFFER_BYTES)
                ),
                video_pad: "src.",
                audio_pad: "src.",
                video_decode_chain,
                audio_decode_chain,
                required_elements,
            });
        }
        Ok(Self {
            description: format!("uridecodebin uri={} name=src", gst_quote(url)),
            video_pad: "src.",
            audio_pad: "src.",
            video_decode_chain: String::new(),
            audio_decode_chain: String::new(),
            required_elements: vec!["uridecodebin".to_string()],
        })
    }

    fn video_branch(&self) -> String {
        self.branch("video")
    }

    fn audio_branch(&self) -> String {
        self.branch("audio")
    }

    fn branch(&self, kind: &str) -> String {
        let caps = if kind == "video" {
            " ! video/x-raw"
        } else {
            " ! audio/x-raw"
        };
        let decode_chain = if kind == "video" {
            self.video_decode_chain.as_str()
        } else {
            self.audio_decode_chain.as_str()
        };
        let pad = if kind == "video" {
            self.video_pad
        } else {
            self.audio_pad
        };
        format!("{pad}{decode_chain}{caps}")
    }
}

fn gst_solid_color(value: &str) -> String {
    let trimmed = value.trim().trim_start_matches('#');
    let (rgb, alpha) = match trimmed.len() {
        6 if trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) => (trimmed, "FF"),
        8 if trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) => (&trimmed[..6], &trimmed[6..]),
        _ => ("000000", "FF"),
    };
    format!("0x{alpha}{rgb}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SinkFragment {
    description: String,
    required_elements: Vec<String>,
    mux_kind: MuxKind,
}

impl SinkFragment {
    fn fanout() -> Self {
        Self {
            description: String::new(),
            required_elements: vec!["appsink".to_string()],
            mux_kind: MuxKind::Fanout,
        }
    }

    fn from_url(url: &str, feed: &FeedConfig) -> Result<Self> {
        let url = url.trim();
        if url.is_empty() {
            bail!("gstreamer cgen output url is empty");
        }
        let max_lateness_ns = queue_time_ns(feed);
        if let Some(endpoint) = UdpEndpoint::parse(url) {
            return Ok(Self {
                description: format!(
                    "udpsink host={} port={} auto-multicast=true buffer-size={} sync=true async=false qos=true max-lateness={max_lateness_ns}",
                    gst_quote(&endpoint.host),
                    endpoint.port,
                    endpoint.buffer_size.unwrap_or(DEFAULT_UDP_BUFFER_BYTES)
                ),
                required_elements: vec!["udpsink".to_string()],
                mux_kind: MuxKind::MpegTs,
            });
        }
        if url.to_ascii_lowercase().starts_with("rtmp://") {
            let sink_element = rtmp_sink_element();
            return Ok(Self {
                description: format!(
                    "{sink_element} location={} sync=true async=false qos=true max-lateness={max_lateness_ns}",
                    gst_quote(url)
                ),
                required_elements: vec![sink_element],
                mux_kind: MuxKind::Flv,
            });
        }
        return Ok(Self {
            description: format!("filesink location={} sync=true async=false", gst_quote(url)),
            required_elements: vec!["filesink".to_string()],
            mux_kind: MuxKind::MpegTs,
        });
    }
}

fn rtmp_sink_element() -> String {
    if gst::ElementFactory::find("rtmp2sink").is_some() {
        "rtmp2sink".to_string()
    } else {
        "rtmpsink".to_string()
    }
}

fn video_caps_fragment(feed: &FeedConfig, video: &crate::config::VideoRenditionConfig) -> String {
    let caps = format!("video/x-raw,width={},height={}", video.width, video.height);
    let Some(output_framerate) = gst_fraction(video.frame_rate_text(feed.video.fps.as_str()))
    else {
        return caps;
    };
    let framerate = rendition_framerate_for_caps(output_framerate.as_str(), video.interlaced);
    format!("{caps},framerate={framerate}")
}

fn source_video_caps_fragment(feed: &FeedConfig) -> String {
    let caps = format!(
        "video/x-raw,width={},height={}",
        feed.video.width, feed.video.height
    );
    let Some(output_framerate) = gst_fraction(feed.video.fps.as_str()) else {
        return caps;
    };
    format!("{caps},framerate={output_framerate}")
}

fn audio_mix_caps_fragment(layout: ChannelLayout) -> String {
    format!(
        "audio/x-raw,format=F32LE,rate={AUDIO_MIX_SAMPLE_RATE},channels={},layout=interleaved",
        layout.channels()
    )
}

fn rendition_framerate_for_caps(framerate: &str, interlaced: bool) -> String {
    if interlaced {
        doubled_gst_fraction(framerate).unwrap_or_else(|| framerate.to_string())
    } else {
        framerate.to_string()
    }
}

fn video_encoder_fragment(
    codec: &str,
    bitrate_kbps: Option<u32>,
    interlaced: bool,
    field_order: &str,
    settings: &EncoderCodecSettings,
) -> String {
    let bitrate = settings
        .bitrate_or(bitrate_kbps)
        .unwrap_or(12_000)
        .saturating_mul(1_000);
    match codec.trim().to_ascii_lowercase().as_str() {
        "mpeg2video" | "mpeg2" | "avenc_mpeg2video" => {
            let gop = settings.gop.unwrap_or(15).clamp(1, 600);
            let interlace_flags = if interlaced {
                let field_order = if top_field_first(field_order) {
                    "tt"
                } else {
                    "bb"
                };
                format!(
                    " flags=+ildct+ilme alternate-scan=true field-order={field_order} seq-disp-ext=always"
                )
            } else {
                " field-order=progressive".to_string()
            };
            format!("avenc_mpeg2video bitrate={bitrate} gop-size={gop} qos=true{interlace_flags}")
        }
        "h264" | "avc" | "x264" | "libx264" | "x264enc" => {
            let preset = video_encoder_preset(settings.preset.as_str(), "ultrafast");
            let tune = video_encoder_tune(settings.tune.as_str(), "zerolatency");
            let keyint = settings
                .gop
                .map(|value| format!(" key-int-max={}", value.clamp(1, 600)))
                .unwrap_or_default();
            let bframes = settings
                .bframes
                .map(|value| format!(" bframes={}", value.min(16)))
                .unwrap_or_default();
            format!(
                "x264enc tune={tune} speed-preset={preset} bitrate={} qos=true{keyint}{bframes}",
                bitrate / 1_000
            )
        }
        "avenc_h264" | "h264_libav" => format!("avenc_h264 bitrate={bitrate}"),
        "h264_amf" | "amf_h264" | "amfh264enc" => {
            format!("amfh264enc bitrate={} qos=true", bitrate / 1_000)
        }
        "h264_nvenc" | "nvenc_h264" | "nvh264enc" => {
            format!("nvh264enc bitrate={}", bitrate / 1_000)
        }
        "h264_qsv" | "qsv_h264" | "qsvh264enc" => {
            format!("qsvh264enc bitrate={}", bitrate / 1_000)
        }
        "mpeg2_qsv" | "qsv_mpeg2" | "qsvmpeg2enc" => {
            format!("qsvmpeg2enc bitrate={}", bitrate / 1_000)
        }
        "openh264" | "h264_openh264" | "openh264enc" => format!("openh264enc bitrate={bitrate}"),
        "h265" | "hevc" | "x265" | "libx265" | "x265enc" => {
            let preset = video_encoder_preset(settings.preset.as_str(), "ultrafast");
            let tune = video_encoder_tune(settings.tune.as_str(), "zerolatency");
            let mut options = vec![
                format!("bframes={}", settings.bframes.unwrap_or(0).min(16)),
                "rc-lookahead=0".to_string(),
                "frame-threads=1".to_string(),
                "pools=none".to_string(),
            ];
            if let Some(gop) = settings.gop {
                options.push(format!("keyint={}", gop.clamp(1, 600)));
            }
            format!(
                "x265enc speed-preset={preset} tune={tune} bitrate={} option-string=\"{}\"",
                bitrate / 1_000,
                options.join(":")
            )
        }
        "h265_amf" | "hevc_amf" | "amf_h265" | "amf_hevc" | "amfh265enc" => {
            format!("amfh265enc bitrate={}", bitrate / 1_000)
        }
        "h265_nvenc" | "hevc_nvenc" | "nvenc_h265" | "nvenc_hevc" | "nvh265enc" => {
            format!("nvh265enc bitrate={}", bitrate / 1_000)
        }
        "h265_qsv" | "hevc_qsv" | "qsv_h265" | "qsv_hevc" | "qsvh265enc" => {
            format!("qsvh265enc bitrate={}", bitrate / 1_000)
        }
        "hevc_libav" | "h265_libav" | "avenc_hevc" => format!("avenc_hevc bitrate={bitrate}"),
        "mpeg4" | "mpeg4video" | "avenc_mpeg4" => format!("avenc_mpeg4 bitrate={bitrate}"),
        actual if actual.starts_with("avenc_") => format!("{actual} bitrate={bitrate}"),
        actual if actual.ends_with("enc") => generic_video_encoder_fragment(actual, bitrate),
        _ => format!(
            "x264enc tune=zerolatency speed-preset=ultrafast bitrate={} qos=true",
            bitrate / 1_000
        ),
    }
}

fn generic_video_encoder_fragment(element: &str, bitrate: u32) -> String {
    match element {
        name if name.starts_with("amf") || name.starts_with("qsv") || name.starts_with("nv") => {
            format!("{name} bitrate={}", bitrate / 1_000)
        }
        name if name.starts_with("mf") || name.starts_with("d3d12") => {
            format!("{name} bitrate={}", bitrate / 1_000)
        }
        name => name.to_string(),
    }
}

fn video_encoder_preset(value: &str, fallback: &'static str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "ultrafast" | "superfast" | "veryfast" | "faster" | "fast" | "medium" | "slow"
        | "slower" | "veryslow" => value.trim().to_ascii_lowercase(),
        _ => fallback.to_string(),
    }
}

fn video_encoder_tune(value: &str, fallback: &'static str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "zerolatency" | "film" | "animation" | "grain" | "stillimage" | "fastdecode" | "psnr"
        | "ssim" => value.trim().to_ascii_lowercase(),
        _ => fallback.to_string(),
    }
}

fn top_field_first(field_order: &str) -> bool {
    !field_order.eq_ignore_ascii_case("bff")
        && !field_order.eq_ignore_ascii_case("bb")
        && !field_order.eq_ignore_ascii_case("bottom-field-first")
}

fn doubled_gst_fraction(value: &str) -> Option<String> {
    let (num, den) = value.split_once('/')?;
    let num = num.trim().parse::<u32>().ok()?;
    let den = den.trim().parse::<u32>().ok()?;
    Some(format!("{}/{}", num.saturating_mul(2), den.max(1)))
}

fn video_encoder_element(codec: &str) -> String {
    match codec.trim().to_ascii_lowercase().as_str() {
        "mpeg2video" | "mpeg2" | "avenc_mpeg2video" => "avenc_mpeg2video".to_string(),
        "h264" | "avc" | "x264" | "libx264" | "x264enc" => "x264enc".to_string(),
        "avenc_h264" | "h264_libav" => "avenc_h264".to_string(),
        "h264_amf" | "amf_h264" | "amfh264enc" => "amfh264enc".to_string(),
        "h264_nvenc" | "nvenc_h264" | "nvh264enc" => "nvh264enc".to_string(),
        "h264_qsv" | "qsv_h264" | "qsvh264enc" => "qsvh264enc".to_string(),
        "mpeg2_qsv" | "qsv_mpeg2" | "qsvmpeg2enc" => "qsvmpeg2enc".to_string(),
        "openh264" | "h264_openh264" | "openh264enc" => "openh264enc".to_string(),
        "h265" | "hevc" | "x265" | "libx265" | "x265enc" => "x265enc".to_string(),
        "h265_amf" | "hevc_amf" | "amf_h265" | "amf_hevc" | "amfh265enc" => {
            "amfh265enc".to_string()
        }
        "h265_nvenc" | "hevc_nvenc" | "nvenc_h265" | "nvenc_hevc" | "nvh265enc" => {
            "nvh265enc".to_string()
        }
        "h265_qsv" | "hevc_qsv" | "qsv_h265" | "qsv_hevc" | "qsvh265enc" => {
            "qsvh265enc".to_string()
        }
        "hevc_libav" | "h265_libav" | "avenc_hevc" => "avenc_hevc".to_string(),
        "mpeg4" | "mpeg4video" | "avenc_mpeg4" => "avenc_mpeg4".to_string(),
        actual if actual.starts_with("avenc_") || actual.ends_with("enc") => actual.to_string(),
        _ => "x264enc".to_string(),
    }
}

fn video_encoder_raw_format(codec: &str) -> &'static str {
    if video_encoder_element(codec) == "nvh264enc" {
        "NV12"
    } else {
        "I420"
    }
}

fn audio_encoder_fragment_bps(
    codec: &str,
    bitrate_bps: i64,
    settings: &EncoderCodecSettings,
) -> String {
    let bitrate = bitrate_bps.max(1);
    let bitrate = settings
        .bitrate_kbps
        .map(|kbps| i64::from(kbps.max(1)) * 1_000)
        .unwrap_or(bitrate);
    match codec.trim().to_ascii_lowercase().as_str() {
        "ac3" | "avenc_ac3" => format!("avenc_ac3 bitrate={bitrate}"),
        "eac3" | "e-ac3" | "e_ac3" | "enhanced_ac3" | "enhanced-ac3" | "avenc_eac3" => {
            format!("avenc_eac3 bitrate={bitrate}")
        }
        "mp2" | "mpeg_layer_2" | "mpeg-layer-2" | "avenc_mp2" => {
            format!("avenc_mp2 bitrate={bitrate}")
        }
        "mp3" | "lamemp3enc" => {
            format!("lamemp3enc target=bitrate bitrate={}", bitrate / 1_000)
        }
        "avenc_mp3" => format!("avenc_mp3 bitrate={bitrate}"),
        "opus" | "opusenc" => format!("opusenc bitrate={bitrate}"),
        "vorbis" | "vorbisenc" => format!("vorbisenc bitrate={bitrate}"),
        "flac" | "flacenc" => "flacenc".to_string(),
        "avenc_flac" => "avenc_flac".to_string(),
        "avenc_aac" | "aac" => format!("avenc_aac bitrate={bitrate}"),
        "faac" => format!("faac bitrate={bitrate}"),
        actual if actual.starts_with("avenc_") => format!("{actual} bitrate={bitrate}"),
        "fdkaacenc" | "mfaacenc" => format!("{} bitrate={bitrate}", codec.trim()),
        actual if actual.ends_with("enc") => actual.to_string(),
        _ => format!("avenc_aac bitrate={bitrate}"),
    }
}

fn audio_encoder_element(codec: &str) -> String {
    match codec.trim().to_ascii_lowercase().as_str() {
        "ac3" | "avenc_ac3" => "avenc_ac3".to_string(),
        "eac3" | "e-ac3" | "e_ac3" | "enhanced_ac3" | "enhanced-ac3" | "avenc_eac3" => {
            "avenc_eac3".to_string()
        }
        "mp2" | "mpeg_layer_2" | "mpeg-layer-2" | "avenc_mp2" => "avenc_mp2".to_string(),
        "mp3" | "lamemp3enc" => "lamemp3enc".to_string(),
        "avenc_mp3" => "avenc_mp3".to_string(),
        "opus" | "opusenc" => "opusenc".to_string(),
        "vorbis" | "vorbisenc" => "vorbisenc".to_string(),
        "flac" | "flacenc" => "flacenc".to_string(),
        "avenc_flac" => "avenc_flac".to_string(),
        "aac" | "avenc_aac" => "avenc_aac".to_string(),
        "faac" => "faac".to_string(),
        actual if actual.starts_with("avenc_") || actual.ends_with("enc") => actual.to_string(),
        _ => "avenc_aac".to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UdpEndpoint {
    host: String,
    port: u16,
    buffer_size: Option<u32>,
    reuse: bool,
}

impl UdpEndpoint {
    fn parse(url: &str) -> Option<Self> {
        let url = url.trim();
        if !url.get(..6)?.eq_ignore_ascii_case("udp://") {
            return None;
        }
        let rest = &url[6..];
        let authority = rest.split(['/', '?']).next().unwrap_or(rest);
        let (host, port) = authority.rsplit_once(':')?;
        let port = port.parse::<u16>().ok()?;
        let query = rest.split_once('?').map(|(_, query)| query).unwrap_or("");
        Some(Self {
            host: host.trim_matches(['[', ']']).to_string(),
            port,
            buffer_size: udp_query_u32(query, &["buffer_size", "buffer-size", "fifo_size"])
                .map(|value| value.max(1316)),
            reuse: udp_query_bool(query, "reuse").unwrap_or(true),
        })
    }
}

fn udp_query_u32(query: &str, keys: &[&str]) -> Option<u32> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        keys.iter()
            .any(|candidate| key.eq_ignore_ascii_case(candidate))
            .then(|| value.parse::<u32>().ok())
            .flatten()
    })
}

fn udp_query_bool(query: &str, key: &str) -> Option<bool> {
    query.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        candidate.eq_ignore_ascii_case(key).then(|| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
    })
}

fn gst_fraction(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("source") {
        return None;
    }
    if value.contains('/') {
        return Some(value.to_string());
    }
    value.parse::<u32>().ok().map(|fps| format!("{fps}/1"))
}

fn gst_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AudioConfig, AudioRenditionConfig, BannerConfig, ClockConfig, EncoderCodecSettings,
        EndpointConfig, FeedConfig, GraphicsConfig, LadderConfig, OutputConfig,
        PriorityInputConfig, StandbyConfig, StateConfig, SyncConfig, TextConfig, VideoConfig,
        VideoRenditionConfig,
    };

    #[test]
    fn catalog_codec_tokens_do_not_match_inside_unrelated_codec_names() {
        assert!(!contains_catalog_token("avdec_atrac3", "ac3"));
        assert!(!contains_catalog_token("avenc_wmav1", "av1"));
        assert!(contains_catalog_token("codec/encoder/video av1", "av1"));
        assert_eq!(catalog_codec_name("libav atrac3 audio decoder"), None);
        assert_eq!(
            catalog_codec_name("windows media audio 1 avenc_wmav1"),
            None
        );
        assert_eq!(catalog_codec_name("av1 video decoder"), Some("AV1"));
    }

    #[test]
    fn video_decoder_catalog_excludes_audio_decoder_factories() {
        gst::init().expect("GStreamer test initialization");
        let catalog = gstreamer_decoder_catalog();
        assert!(!catalog.iter().any(|entry| entry["id"] == "avdec_aac"));

        if let Some(factory) = gst::ElementFactory::find("avdec_aac") {
            assert!(!factory_outputs_media_type(&factory, "video/"));
        }
        if gst::ElementFactory::find("avdec_h264").is_some() {
            assert!(catalog.iter().any(|entry| entry["id"] == "avdec_h264"));
        }
    }

    #[test]
    fn preserve_native_fails_closed_until_track_retention_is_guaranteed() {
        let mut feed = test_feed();
        feed.audio.topology = "preserve_native_tracks".to_string();

        let error = GstPipelinePlan::from_feed(&feed)
            .expect_err("single-track decoded runtime must reject preserve-native mode");

        assert!(error.to_string().contains("preserve-native audio routing"));
        assert!(error.to_string().contains("track codec, language, layout"));
    }

    #[test]
    fn forced_audio_layout_is_shared_by_runtime_and_plan_validation() {
        let spec = AudioRoutingSpec {
            topology: AudioTopologyMode::ForceLayout(ChannelLayout::Surround51),
            ..AudioRoutingSpec::default()
        };
        assert_eq!(
            forced_audio_output_layout(&spec).expect("forced layout"),
            ChannelLayout::Surround51
        );

        let preserve = AudioRoutingSpec::default();
        let error = forced_audio_output_layout(&preserve)
            .expect_err("decoded single-track runtime cannot preserve native tracks");
        assert!(error.to_string().contains("preserve-native audio routing"));
    }

    #[test]
    fn forced_layout_configures_each_legacy_rendition_matrix() {
        gst::init().expect("GStreamer test initialization");
        let mut feed = test_feed();
        feed.audio.force_layout = "surround_51".to_string();
        feed.ladder.audios[1].enabled = "true".to_string();

        let plan = GstPipelinePlan::from_feed(&feed).expect("mixed rendition plan");
        let element = gst::parse::launch(&plan.description).expect("parse pipeline");
        let pipeline = element.downcast::<gst::Pipeline>().expect("pipeline");
        install_audio_rendition_matrices(&pipeline, &plan, ChannelLayout::Surround51)
            .expect("rendition matrices");

        assert!(pipeline.by_name("audio_matrix_stereo_p1_pid257").is_some());
        assert!(pipeline
            .by_name("audio_matrix_surround_51_p1_pid258")
            .is_some());
        pipeline.set_state(gst::State::Null).expect("pipeline stop");
    }

    #[test]
    fn explicit_outputs_use_one_master_compositor_fanout() {
        let mut feed = test_feed();
        feed.outputs.outputs = vec![OutputConfig {
            id: "primary".to_string(),
            enabled: true,
            destination: "rtmp".to_string(),
            url: "rtmp://example.invalid/live".to_string(),
            video_codec: "h264".to_string(),
            video_bitrate_kbps: 4_000,
            gop_frames: 60,
            audio_codec: "aac".to_string(),
            audio_bitrate_kbps: 192,
            sample_rate: 48_000,
            ..Default::default()
        }];

        let plan = GstPipelinePlan::from_feed(&feed).expect("fanout plan");

        assert!(plan.uses_output_fanout());
        assert_eq!(plan.videos.len(), 1);
        assert_eq!(plan.videos[0].id, "master");
        assert_eq!(plan.audios.len(), 1);
        assert!(plan
            .description
            .contains("identity name=cgen_overlay_master"));
        assert!(plan.description.contains("appsink name=master_video_sink"));
        assert!(plan.description.contains("appsink name=master_audio_sink"));
        assert!(!plan.description.contains("mpegtsmux name=mux"));
        assert!(plan.required_elements.contains(&"appsink".to_string()));
        assert!(!plan.required_elements.contains(&"x264enc".to_string()));
    }

    #[test]
    fn gstreamer_matrix_rows_transpose_source_major_coefficients() {
        let matrix = MixMatrix::for_program(ChannelLayout::Stereo, ChannelLayout::Surround51)
            .expect("matrix");

        let rows = gstreamer_mix_matrix_rows(&matrix);

        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0], vec![1.0, 0.0]);
        assert_eq!(rows[1], vec![0.0, 1.0]);
        assert_eq!(rows[3], vec![0.0, 0.0]);
    }

    #[test]
    fn multichannel_alert_matrix_omits_source_lfe_and_replicates_mono_sum() {
        let matrix = alert_mix_matrix(ChannelLayout::Surround51, ChannelLayout::Stereo)
            .expect("alert matrix");

        assert_eq!(matrix.coefficients[6], 0.0);
        assert_eq!(matrix.coefficients[7], 0.0);
        for destination in 0..2 {
            let sum = (0..6)
                .map(|source| matrix.coefficients[source * 2 + destination])
                .sum::<f32>();
            assert!((sum - 1.0).abs() < 0.000_001);
        }
    }

    #[test]
    fn f32le_alert_gain_ramps_over_twenty_milliseconds() {
        let spec = AudioRoutingSpec {
            topology: AudioTopologyMode::ForceLayout(ChannelLayout::Stereo),
            ..AudioRoutingSpec::default()
        };
        let mut gains = AudioGainController::new(&spec);
        gains.prime_alert(GainDb::Muted);
        gains.set_targets(GainDb::Muted, GainDb::Db(0.0), AUDIO_MIX_SAMPLE_RATE, 20);
        let mut bytes = vec![0_u8; 961 * 2 * std::mem::size_of::<f32>()];
        for sample in bytes.chunks_exact_mut(std::mem::size_of::<f32>()) {
            sample.copy_from_slice(&1.0_f32.to_le_bytes());
        }

        apply_audio_gain_f32le(&mut bytes, 2, AudioBus::Alert, &mut gains).expect("gain ramp");

        let left = bytes
            .chunks_exact(2 * std::mem::size_of::<f32>())
            .map(|frame| f32::from_le_bytes(frame[..4].try_into().expect("sample")))
            .collect::<Vec<_>>();
        assert_eq!(left[0], 0.0);
        assert!((left[960] - 1.0).abs() < 0.000_001);
        assert!(left
            .windows(2)
            .all(|window| window[1] >= window[0] && window[1] - window[0] <= 0.001_1));
    }

    #[test]
    fn unsupported_channel_count_is_rejected_before_default_rematrixing() {
        gst::init().expect("GStreamer initialization");
        let caps = gst::Caps::builder("audio/x-raw")
            .field("format", "F32LE")
            .field("rate", 48_000i32)
            .field("channels", 4i32)
            .field("layout", "interleaved")
            .build();

        let error = audio_matrix_from_caps(caps.as_ref(), AudioBus::Program, ChannelLayout::Stereo)
            .expect_err("four-channel input is not a supported deterministic layout");

        assert!(error.to_string().contains("channel count 4"));
    }

    #[test]
    fn parses_udp_sink_url() {
        let endpoint = UdpEndpoint::parse("udp://239.0.0.2:9001?pkt_size=1316&buffer_size=1048576")
            .expect("udp endpoint");
        assert_eq!(endpoint.host, "239.0.0.2");
        assert_eq!(endpoint.port, 9001);
        assert_eq!(endpoint.buffer_size, Some(1_048_576));
        assert!(endpoint.reuse);
    }

    #[test]
    fn dummy_input_builds_live_video_and_silent_audio_sources() {
        let mut feed = test_feed();
        feed.program_input.input_type = "dummy".to_string();
        feed.program_input.url.clear();
        feed.program_input.background = "#10203080".to_string();

        let source = InputSourceFragment::from_feed(&feed).expect("dummy input");

        assert!(source.description.contains("videotestsrc name=video_src"));
        assert!(source.description.contains("foreground-color=0x80102030"));
        assert!(source.description.contains("audiotestsrc name=audio_src"));
        assert_eq!(source.video_pad, "video_src.");
        assert_eq!(source.audio_pad, "audio_src.");
    }

    #[test]
    fn device_input_uses_validated_backend_and_quotes_persistent_id() {
        let mut feed = test_feed();
        feed.program_input.input_type = "device".to_string();
        feed.program_input.url.clear();
        feed.program_input.device_backend = "v4l2".to_string();
        feed.program_input.device_id = "/dev/video0".to_string();

        let source = InputSourceFragment::from_feed(&feed).expect("device input");

        assert!(source
            .description
            .contains("v4l2src name=video_src device=\"/dev/video0\""));
        assert!(source.description.contains("autoaudiosrc name=audio_src"));
        assert!(source.required_elements.contains(&"v4l2src".to_string()));
    }

    #[test]
    fn gstreamer_plan_uses_ts_demux_and_mpegts_mux() {
        let feed = test_feed();
        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");
        assert!(plan.description.contains("udpsrc address="));
        assert!(plan.description.contains("retrieve-sender-address=false"));
        assert!(plan.description.contains("buffer-size=2000000"));
        assert!(!plan.description.contains("timeout="));
        assert!(plan
            .description
            .contains("identity name=program_ts_monitor silent=true"));
        assert!(plan.description.contains("tsdemux name=src"));
        assert!(plan.description.contains(
            "src. ! capsfilter caps=\"video/x-h264;video/x-h265;video/mpeg;video/x-vp8;video/x-vp9;image/jpeg\" ! parsebin ! decodebin"
        ));
        assert!(plan.description.contains(
            "src. ! capsfilter caps=\"audio/mpeg;audio/x-ac3;audio/x-eac3;audio/x-opus;audio/x-dts;audio/x-private1-ac3;audio/x-private1-lpcm;audio/x-lpcm\" ! parsebin ! decodebin"
        ));
        assert!(plan.description.contains("tee name=video_tee"));
        assert!(plan.description.contains("tee name=audio_tee"));
        assert!(plan
            .description
            .contains("input-selector name=video_selector"));
        assert!(plan
            .description
            .contains("identity name=program_video_monitor silent=true"));
        assert!(plan.description.contains(
            "identity name=program_video_monitor silent=true ! deinterlace method=yadif mode=auto-strict fields=all locking=passive tff=auto"
        ));
        assert!(
            plan.description
                .matches("videoscale method=lanczos add-borders=true")
                .count()
                >= 2
        );
        assert!(plan.required_elements.contains(&"deinterlace".to_string()));
        assert_eq!(
            plan.status_value()["deinterlace"]["implementation"],
            "gstreamer_deinterlace_yadif"
        );
        assert!(plan.description.contains(
            "input-selector name=video_selector sync-streams=true sync-mode=clock cache-buffers=false drop-backwards=true"
        ));
        assert!(plan.description.contains("video_selector.sink_0"));
        assert!(plan
            .description
            .contains("videotestsrc name=standby_video_src pattern=black"));
        assert!(plan.description.contains("video_selector.sink_1"));
        assert!(plan
            .description
            .contains("videotestsrc name=smpte_video_src pattern=smpte"));
        assert!(plan.description.contains("video_selector.sink_2"));
        assert!(plan
            .description
            .contains("identity name=cgen_overlay_hd silent=true"));
        assert!(!plan.description.contains("name=audio_selector"));
        assert!(plan
            .description
            .contains("audiomixer name=audio_mixer ignore-inactive-pads=true latency=240000000"));
        assert!(plan.description.contains("audiotestsrc wave=silence"));
        assert!(plan.description.contains("appsrc name=priority_audio_src"));
        assert!(plan.description.contains(
            "appsrc name=priority_audio_src is-live=true block=false leaky-type=downstream stream-type=stream format=time do-timestamp=true min-latency=0 max-latency=240000000"
        ));
        assert!(plan
            .description
            .contains("identity name=program_audio_monitor silent=true"));
        assert!(plan
            .description
            .contains("audioconvert name=program_audio_matrix"));
        assert!(plan
            .description
            .contains("identity name=program_audio_gain silent=true"));
        assert!(plan
            .description
            .contains("identity name=alert_audio_monitor silent=true"));
        assert!(plan
            .description
            .contains("audioconvert name=alert_audio_matrix"));
        assert!(plan
            .description
            .contains("identity name=alert_audio_gain silent=true"));
        assert!(plan.description.contains("video_tee."));
        assert!(plan.description.contains("audio_tee."));
        assert!(plan.description.contains(
            "audio_tee. ! queue max-size-time=240000000 max-size-buffers=4 max-size-bytes=8294400 flush-on-eos=true leaky=downstream ! audioconvert"
        ));
        assert!(plan.description.contains("mux.sink_256"));
        assert!(plan.description.contains("mux.sink_257"));
        assert!(plan.description.contains("mpegtsmux name=mux"));
        assert!(plan.description.contains("max-size-time=240000000"));
        assert!(plan.description.contains("flush-on-eos=true"));
        assert!(plan.description.contains("audiorate"));
        assert!(plan
            .description
            .contains("audio/x-raw,format=F32LE,rate=48000,channels=2,layout=interleaved"));
        assert!(plan.description.contains("latency=120000000"));
        assert!(plan.description.contains("pat-interval=4500"));
        assert!(plan.description.contains("pcr-interval=1800"));
        assert!(plan.description.contains("max-bytes=48000"));
        assert!(!plan.description.contains("max-size-time=300000000"));
        assert!(plan
            .description
            .contains("udpsink host=\"239.0.0.2\" port=9001"));
        assert!(plan.description.contains("qos=true max-lateness=240000000"));
        assert!(plan
            .description
            .contains("avenc_mpeg2video bitrate=12000000 gop-size=15 qos=true flags=+ildct+ilme alternate-scan=true field-order=tt seq-disp-ext=always"));
        assert!(plan.description.contains("videorate ! video/x-raw,width=1920,height=1080,framerate=60000/1001,format=BGRx ! identity name=cgen_overlay_hd silent=true"));
        assert!(plan.description.contains(
            "videoconvert ! video/x-raw,format=I420 ! interlace field-pattern=1:1 top-field-first=true"
        ));
        assert!(plan.required_elements.contains(&"identity".to_string()));
        assert!(plan.required_elements.contains(&"audiomixer".to_string()));
        assert!(!plan.description.contains("interlace-mode=interleaved"));
        assert!(plan.description.contains("buffer-size=4194304"));
        assert!(plan.required_elements.contains(&"udpsrc".to_string()));
        assert!(plan.required_elements.contains(&"parsebin".to_string()));
        assert!(plan.required_elements.contains(&"tsdemux".to_string()));
        assert!(plan.required_elements.contains(&"decodebin".to_string()));
        assert!(plan.required_elements.contains(&"interlace".to_string()));
        assert!(plan.required_elements.contains(&"mpegtsmux".to_string()));
        assert!(plan
            .required_elements
            .contains(&"avenc_mpeg2video".to_string()));
        assert!(plan.required_elements.contains(&"avenc_ac3".to_string()));
        assert!(plan.required_elements.contains(&"videotestsrc".to_string()));
        assert!(plan.required_elements.contains(&"udpsink".to_string()));
    }

    #[test]
    fn yadif_frame_cadence_uses_the_selected_field_and_parity() {
        let mut feed = test_feed();
        feed.program_input.deinterlace_cadence = "frame".to_string();
        feed.program_input.deinterlace_parity = "bff".to_string();

        let plan = GstPipelinePlan::from_feed(&feed).expect("YADIF plan");

        assert!(plan.description.contains(
            "deinterlace method=yadif mode=auto-strict fields=bottom locking=passive tff=bff"
        ));
        assert_eq!(
            plan.status_value()["deinterlace"]["field_rate_output"],
            false
        );
    }

    #[test]
    fn unavailable_bwdif_is_rejected_instead_of_falling_back() {
        let mut feed = test_feed();
        feed.program_input.deinterlace_algorithm = "bwdif".to_string();
        feed.program_input.deinterlace_backend = "software".to_string();

        let error = GstPipelinePlan::from_feed(&feed)
            .expect_err("BWDIF must not silently fall back to another algorithm");

        assert!(error.to_string().contains("no in-process BWDIF filter"));
    }

    #[test]
    fn field_rate_deinterlacing_normalizes_interlaced_caps_to_progressive() {
        let mut feed = test_feed();
        let caps = SourceCaps::new(
            720,
            480,
            30_000,
            1_001,
            SourceScanMode::Interlaced,
            SourceFieldOrder::TopFieldFirst,
            8,
            9,
            "bt601",
        )
        .expect("source caps");

        apply_effective_source_caps(&mut feed, &caps, DeinterlaceSpec::default())
            .expect("effective caps");

        assert_eq!(feed.video.width, 720);
        assert_eq!(feed.video.height, 480);
        assert_eq!(feed.video.fps, "60000/1001");
        assert!(!feed.video.interlaced);
        let automatic = feed
            .ladder
            .videos
            .iter()
            .find(|rendition| rendition.enabled.eq_ignore_ascii_case("auto"))
            .expect("automatic rendition");
        assert_eq!(automatic.fps, "60000/1001");
        assert!(!automatic.interlaced);
    }

    #[test]
    fn source_program_map_follows_detected_input_topology() {
        use crate::input_program_map::{InputElementaryStream, InputProgram, InputStreamKind};

        let mut feed = test_feed();
        feed.program_mapping.mode = "source".to_string();
        let input = InputProgramMapSnapshot {
            transport_stream_id: 42,
            pat_program_count: 1,
            selected_program: InputProgram {
                program_number: NonZeroU16::new(7).expect("non-zero program"),
                pmt_pid: 0x0123,
                pcr_pid: 0x0200,
                streams: vec![
                    InputElementaryStream {
                        pid: 0x0200,
                        stream_type: 0x02,
                        kind: InputStreamKind::Video,
                        codec: "mpeg2video",
                        language: None,
                    },
                    InputElementaryStream {
                        pid: 0x0201,
                        stream_type: 0x81,
                        kind: InputStreamKind::Audio,
                        codec: "ac3",
                        language: Some("eng".to_string()),
                    },
                    InputElementaryStream {
                        pid: 0x10c0,
                        stream_type: 0x86,
                        kind: InputStreamKind::Scte35,
                        codec: "scte35",
                        language: None,
                    },
                ],
            },
        };

        let plan = GstPipelinePlan::from_feed_with_program_map(&feed, Some(&input)).expect("plan");
        let status = plan.status_value();
        assert_eq!(status["program_map"]["transport_stream_id"], 42);
        assert_eq!(
            status["program_map"]["prog_map"],
            "program_map,sink_512=(int)7,sink_513=(int)7,PMT_7=(uint)291,PCR_7=(int)512"
        );
        assert_eq!(status["videos"][0]["program"], 7);
        assert_eq!(status["videos"][0]["video_pid"], 0x0200);
        assert_eq!(status["audios"][0]["audio_pid"], 0x0201);
    }

    #[test]
    fn gstreamer_plan_uses_selected_stream_video_decoder() {
        let mut feed = test_feed();
        feed.program_input.hardware_decoder_enabled = "true".to_string();
        feed.program_input.hardware_decoder = "nvh264dec".to_string();

        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(plan
            .description
            .contains("! parsebin ! nvh264dec ! video/x-raw"));
        assert!(plan
            .description
            .contains("! parsebin ! decodebin ! audio/x-raw"));
        assert!(plan.required_elements.contains(&"nvh264dec".to_string()));
    }

    #[test]
    fn gstreamer_plan_uses_nv12_before_nvh264enc_on_system_memory_path() {
        let mut feed = test_feed();
        feed.program_output.vcodec = "nvh264enc".to_string();
        feed.video.fps = "60000/1001".to_string();
        feed.video.interlaced = false;
        feed.ladder.videos[0].vcodec = "nvh264enc".to_string();
        feed.ladder.videos[0].fps = "60000/1001".to_string();
        feed.ladder.videos[0].interlaced = false;

        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(plan
            .description
            .contains("videoconvert ! video/x-raw,format=NV12"));
        assert!(plan.description.contains("! nvh264enc bitrate="));
        assert!(!plan.description.contains("video/x-raw,format=I420 ! queue"));
    }

    #[test]
    fn generic_queue_max_bytes_tracks_one_output_frame() {
        let feed = test_feed();

        assert_eq!(raw_bgrx_frame_bytes(1920, 1080), 8_294_400);
        assert_eq!(queue_max_bytes(&feed), 8_294_400);
    }

    #[test]
    fn gstreamer_plan_uses_sync_buffer_for_queues_and_mux_latency() {
        let mut feed = test_feed();
        feed.sync.source_buffer_ms = 640;

        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(plan.description.contains("max-size-time=640000000"));
        assert!(plan.description.contains(
            "! parsebin ! decodebin ! audio/x-raw ! queue max-size-time=640000000 max-size-buffers=4 max-size-bytes=8294400 flush-on-eos=true leaky=downstream ! audioconvert"
        ));
        assert!(plan.description.contains("latency=320000000"));
        assert!(plan.description.contains("max-bytes=122880"));
    }

    #[test]
    fn source_fps_omits_forced_ntsc_framerate_caps() {
        let mut feed = test_feed();
        feed.video.fps = "source".to_string();
        feed.video.interlaced = false;
        feed.ladder.videos[0].fps = "source".to_string();
        feed.ladder.videos[0].interlaced = false;

        assert_eq!(
            source_video_caps_fragment(&feed),
            "video/x-raw,width=1920,height=1080"
        );
        assert_eq!(
            video_caps_fragment(&feed, &feed.ladder.videos[0]),
            "video/x-raw,width=1920,height=1080"
        );
        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(!plan.description.contains("framerate=30000/1001"));
        assert!(!plan.description.contains("framerate=60000/1001"));
    }

    #[test]
    fn gstreamer_plan_fans_out_enabled_ladder_renditions() {
        let mut feed = test_feed();
        feed.ladder.videos = vec![
            VideoRenditionConfig {
                id: "hd".to_string(),
                enabled: "true".to_string(),
                width: 1920,
                height: 1080,
                fps: "30000/1001".to_string(),
                interlaced: true,
                field_order: "tff".to_string(),
                standard: "atsc".to_string(),
                vcodec: "mpeg2video".to_string(),
                bitrate_kbps: Some(14_000),
                program: Some(1),
                video_pid: Some(0x100),
                pmt_pid: Some(0x1000),
            },
            VideoRenditionConfig {
                id: "p720".to_string(),
                enabled: "true".to_string(),
                width: 1280,
                height: 720,
                fps: "60000/1001".to_string(),
                interlaced: false,
                field_order: "progressive".to_string(),
                standard: "atsc".to_string(),
                vcodec: "h264".to_string(),
                bitrate_kbps: Some(6_000),
                program: Some(2),
                video_pid: Some(0x120),
                pmt_pid: Some(0x1001),
            },
        ];
        feed.ladder.audios = vec![
            AudioRenditionConfig {
                id: "secondary_stereo".to_string(),
                enabled: "true".to_string(),
                channels: 2,
                acodec: "ac3".to_string(),
                bitrate_kbps: Some(384),
                language: "eng".to_string(),
                program: Some(1),
                audio_pid: Some(0x102),
                pmt_pid: Some(0x1000),
            },
            AudioRenditionConfig {
                id: "stereo".to_string(),
                enabled: "true".to_string(),
                channels: 2,
                acodec: "ac3".to_string(),
                bitrate_kbps: Some(192),
                language: "eng".to_string(),
                program: Some(1),
                audio_pid: Some(0x101),
                pmt_pid: Some(0x1000),
            },
        ];

        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(plan
            .description
            .contains("video/x-raw,width=1920,height=1080"));
        assert!(plan
            .description
            .contains("video/x-raw,width=1280,height=720"));
        assert!(plan
            .description
            .contains("identity name=cgen_overlay_hd silent=true"));
        assert!(plan
            .description
            .contains("identity name=cgen_overlay_p720 silent=true"));
        assert!(plan
            .description
            .contains("x264enc tune=zerolatency speed-preset=ultrafast bitrate=6000 qos=true"));
        assert!(plan
            .description
            .contains("audio/x-raw,rate=48000,channels=2,layout=interleaved"));

        let status = plan.status_value();
        assert_eq!(status["video_count"], 2);
        assert_eq!(status["audio_count"], 4);
        assert_eq!(status["program_map"]["transport_stream_id"], 1);
        assert_eq!(
            status["program_map"]["prog_map"],
            "program_map,sink_256=(int)1,sink_257=(int)1,sink_258=(int)1,sink_259=(int)2,sink_260=(int)2,sink_261=(int)2,PMT_1=(uint)4096,PCR_1=(int)256,PMT_2=(uint)4097,PCR_2=(int)259"
        );
        assert_eq!(status["videos"][0]["program"], 1);
        assert_eq!(status["videos"][0]["video_pid"], 0x100);
        assert_eq!(status["videos"][0]["pmt_pid"], 0x1000);
        assert_eq!(status["videos"][1]["program"], 2);
        assert_eq!(status["audios"][0]["program"], 1);
        assert_eq!(status["audios"][0]["audio_pid"], 0x101);
        assert_eq!(status["audios"][0]["channels"], 2);
        assert_eq!(status["audios"][0]["bitrate_bps"], 384_000);
        assert_eq!(status["audios"][1]["language"], "eng");
    }

    #[test]
    fn gstreamer_plan_uses_selected_hevc_and_eac3_encoders() {
        let mut feed = test_feed();
        feed.program_output.vcodec = "h265".to_string();
        feed.program_output.acodec = "e-ac3".to_string();
        for video in &mut feed.ladder.videos {
            video.vcodec = "h265".to_string();
        }
        for audio in &mut feed.ladder.audios {
            audio.acodec = "e-ac3".to_string();
        }

        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(
            plan.description
                .contains("x265enc speed-preset=ultrafast tune=zerolatency bitrate=12000"),
            "HEVC selection should not fall back to x264: {}",
            plan.description
        );
        assert!(plan.description.contains("avenc_eac3 bitrate=192000"));
        assert!(plan.required_elements.contains(&"x265enc".to_string()));
        assert!(plan.required_elements.contains(&"avenc_eac3".to_string()));
        assert!(!plan.required_elements.contains(&"x264enc".to_string()));
    }

    #[test]
    fn gstreamer_plan_applies_companion_encoder_settings() {
        let mut feed = test_feed();
        feed.program_output.vcodec = "x264enc".to_string();
        feed.program_output.acodec = "avenc_ac3".to_string();
        feed.ladder.videos[0].vcodec = "x264enc".to_string();
        feed.ladder.audios[0].acodec = "avenc_ac3".to_string();
        feed.encoder.video = EncoderCodecSettings {
            codec: "x264enc".to_string(),
            bitrate_kbps: Some(7_000),
            gop: Some(30),
            bframes: Some(2),
            preset: "veryfast".to_string(),
            tune: "zerolatency".to_string(),
            ..Default::default()
        };
        feed.encoder.audio = EncoderCodecSettings {
            codec: "avenc_ac3".to_string(),
            bitrate_kbps: Some(256),
            ..Default::default()
        };

        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");

        assert!(plan.description.contains(
            "x264enc tune=zerolatency speed-preset=veryfast bitrate=7000 qos=true key-int-max=30 bframes=2"
        ));
        assert!(plan.description.contains("avenc_ac3 bitrate=256000"));
    }

    #[test]
    fn gstreamer_plan_uses_uridecodebin_for_non_udp_inputs() {
        let mut feed = test_feed();
        feed.program_input.url = "http://172.16.1.31:8866/live?channel_id=10798".to_string();
        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");
        assert!(plan.description.contains("uridecodebin uri="));
        assert!(!plan.description.contains("souphttpsrc"));
        assert!(!plan.description.contains("tsdemux name=src"));
        assert!(plan.required_elements.contains(&"uridecodebin".to_string()));
        assert!(!plan.required_elements.contains(&"udpsrc".to_string()));
        assert!(!plan.required_elements.contains(&"tsdemux".to_string()));
    }

    #[test]
    fn gstreamer_preflight_reports_missing_elements() {
        gst::init().expect("gst init");
        let err =
            validate_gstreamer_elements(&["definitely_missing_haze_cgen_element".to_string()])
                .expect_err("missing element should fail");

        assert!(err
            .to_string()
            .contains("definitely_missing_haze_cgen_element"));
    }

    #[test]
    fn restart_backoff_uses_xml_bounds() {
        let mut feed = test_feed();
        feed.sync.reconnect_initial_ms = 2_000;
        feed.sync.reconnect_max_ms = 5_000;

        let backoff = restart_backoff_bounds(&feed);

        assert_eq!(backoff.initial, Duration::from_millis(2_000));
        assert_eq!(backoff.max, Duration::from_millis(5_000));
    }
    #[test]
    fn sync_status_reports_runtime_tuning() {
        let mut feed = test_feed();
        feed.sync.source_buffer_ms = 360;

        let status = sync_status_value(&feed);

        assert_eq!(status["source_buffer_ms"], 360);
        assert_eq!(status["reconnect_initial_ms"], 500);
        assert_eq!(status["status_interval_ms"], 750);
    }

    #[test]
    fn graphics_fatal_error_rejects_cairo_fallback() {
        let message = crate::wgpu_renderer::fatal_error_message();

        assert!(message.contains("HAZE CGEN FATAL GRAPHICS ERROR"));
        assert!(message.contains("WGPU"));
        assert!(message.contains("Cairo fallback is disabled"));
    }

    #[test]
    fn pipeline_diagnostics_status_reports_counts_and_latency_errors() {
        let mut diagnostics = PipelineDiagnostics::default();
        diagnostics.record_latency_recalculation(None);
        diagnostics.record_latency_recalculation(Some("latency query failed".to_string()));

        let status = diagnostics.status_value();

        assert_eq!(status["warning_count"], 0);
        assert_eq!(status["qos_count"], 0);
        assert_eq!(status["latency_recalculation_count"], 2);
        assert_eq!(status["last_latency_error"], "latency query failed");
        assert!(status["last_qos"].is_null());
    }

    #[test]
    fn gstreamer_plan_parses_and_audio_mix_modes_are_settable() {
        gst::init().expect("gst init");
        let feed = test_feed();
        let plan = GstPipelinePlan::from_feed(&feed).expect("plan");
        let element = gst::parse::launch(&plan.description).expect("parse launch");
        let pipeline = element.downcast::<gst::Pipeline>().expect("pipeline");

        let audio_mix = install_audio_mix_runtime(
            &pipeline,
            Arc::new(Mutex::new(InputHealth::default())),
            feed.audio_routing_spec().expect("audio routing"),
        )
        .expect("audio mixer probes");
        audio_mix
            .set_mode(AudioSelectorMode::Silence)
            .expect("silence mix");
        audio_mix
            .set_mode(AudioSelectorMode::Program)
            .expect("program mix");
        audio_mix
            .set_mode(AudioSelectorMode::Priority)
            .expect("priority mix");
        set_video_selector_mode(&pipeline, VideoSelectorMode::Program).expect("program video pad");
        set_video_selector_mode(&pipeline, VideoSelectorMode::Standby).expect("standby video pad");
        set_video_selector_mode(&pipeline, VideoSelectorMode::Smpte).expect("smpte video pad");
        apply_mpegts_program_map(&pipeline, &plan).expect("program map");
        priority_audio_appsrc(&pipeline).expect("priority appsrc");
        pipeline
            .by_name("cgen_overlay_hd")
            .expect("graphics handoff");
        pipeline.set_state(gst::State::Null).expect("pipeline stop");
    }

    #[test]
    fn routine_and_priority_audio_source_are_independent_flags() {
        let mut input = PriorityInputConfig {
            audio_source: "both".to_string(),
            ..Default::default()
        };
        assert!(input.priority_audio_enabled());
        assert!(input.routine_audio_enabled());
        input.audio_source = "routine".to_string();
        assert!(!input.priority_audio_enabled());
        assert!(input.routine_audio_enabled());
        input.audio_source = "priority".to_string();
        assert!(input.priority_audio_enabled());
        assert!(!input.routine_audio_enabled());
    }
    #[test]
    fn standby_mute_does_not_mute_external_source_audio() {
        let feed = test_feed();
        let state = RuntimeState::default();
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Program
        );
    }

    #[test]
    fn standby_mute_can_mute_routine_idle_audio() {
        let mut feed = test_feed();
        feed.audio.idle = "routine".to_string();
        let state = RuntimeState::default();
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Silence
        );
    }

    #[test]
    fn program_audio_can_be_selected_when_standby_mute_is_disabled() {
        let mut feed = test_feed();
        feed.audio.mute_standby_routine = false;
        let state = RuntimeState::default();
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Program
        );
    }

    #[test]
    fn video_lifecycle_is_independent_from_alert_audio() {
        let mut feed = test_feed();
        let mut state = RuntimeState::default();

        assert_eq!(
            desired_video_selector_mode(&feed, &state, true),
            VideoSelectorMode::Program
        );
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "q1",
            "data": {
                "queue_id": "q1",
                "audio_path": "runtime/audio/alerts/q1.pcm16le",
                "duration_ms": 60000
            }
        })));
        assert_eq!(
            desired_video_selector_mode(&feed, &state, true),
            VideoSelectorMode::Program
        );
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Priority
        );

        feed.state.mode = "standby".to_string();
        assert_eq!(
            desired_video_selector_mode(&feed, &state, true),
            VideoSelectorMode::Standby
        );
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Priority
        );
        feed.state.smpte_bars = true;
        assert_eq!(
            desired_video_selector_mode(&feed, &state, true),
            VideoSelectorMode::Smpte
        );
    }

    #[test]
    fn live_cgen_control_can_switch_video_selector_without_static_config_change() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        assert_eq!(
            desired_video_selector_mode(&feed, &state, true),
            VideoSelectorMode::Program
        );
        assert!(state.apply_event(&json!({
            "type": "cgen.control",
            "subject": feed.id.clone(),
            "data": {
                "feed_id": feed.id.clone(),
                "action": "smpte_bars",
                "enabled": true,
                "smpte_bars": true
            }
        })));
        assert_eq!(
            desired_video_selector_mode(&feed, &state, true),
            VideoSelectorMode::Smpte
        );
    }

    #[test]
    fn input_health_drives_standby_without_touching_priority_audio() {
        let mut feed = test_feed();
        feed.audio.mute_standby_routine = false;
        let mut state = RuntimeState::default();

        assert_eq!(
            desired_video_selector_mode(&feed, &state, false),
            VideoSelectorMode::Standby
        );
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, false),
            AudioSelectorMode::Silence
        );

        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "q1",
            "data": {
                "queue_id": "q1",
                "audio_path": "runtime/audio/alerts/q1.pcm16le",
                "duration_ms": 60000
            }
        })));
        assert_eq!(
            desired_video_selector_mode(&feed, &state, false),
            VideoSelectorMode::Standby
        );
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, false),
            AudioSelectorMode::Priority
        );
    }

    #[test]
    fn input_health_marks_frames_and_timeouts() {
        let feed = test_feed();
        let mut health = InputHealth::default();

        assert!(!health.video_connected(&feed));
        assert!(!health.audio_connected(&feed));
        health.mark_video_frame();
        assert!(health.video_connected(&feed));
        assert!(!health.audio_connected(&feed));
        health.mark_audio_frame();
        assert!(health.video_connected(&feed));
        assert!(health.audio_connected(&feed));
        health.mark_timeout();
        assert!(!health.video_connected(&feed));
        assert!(!health.audio_connected(&feed));
    }

    #[test]
    fn decoded_video_is_unhealthy_after_exact_two_second_window() {
        let feed = test_feed();
        let mut health = InputHealth::default();
        health.last_video_frame = Some(Instant::now() - Duration::from_millis(2_001));
        health.video_timed_out = false;

        assert!(!health.video_connected(&feed));
        let status = input_health_status(&Arc::new(Mutex::new(health)), &feed);
        assert_eq!(status["video_timeout_ms"], 2_000);
        assert_eq!(status["unhealthy_reason"], "no_decoded_video_for_2s");
    }

    #[test]
    fn parses_complete_progressive_gstreamer_caps() {
        gst::init().expect("GStreamer initialization");
        let caps = gst::Caps::builder("video/x-raw")
            .field("width", 1920i32)
            .field("height", 1080i32)
            .field("framerate", gst::Fraction::new(60_000, 1_001))
            .field("interlace-mode", "progressive")
            .field("pixel-aspect-ratio", gst::Fraction::new(1, 1))
            .field("colorimetry", "bt709")
            .build();

        let parsed = source_caps_from_gstreamer(caps.as_ref()).expect("source caps");

        assert_eq!(parsed.width(), 1920);
        assert_eq!(parsed.height(), 1080);
        assert_eq!(parsed.frame_rate().numerator(), 60_000);
        assert_eq!(parsed.frame_rate().denominator(), 1_001);
        assert_eq!(parsed.scan_mode(), SourceScanMode::Progressive);
        assert_eq!(parsed.field_order(), SourceFieldOrder::NotApplicable);
        assert_eq!(parsed.pixel_aspect_ratio().numerator(), 1);
        assert_eq!(parsed.colorimetry(), "bt709");
    }

    #[test]
    fn parses_interlaced_field_order_and_non_square_pixels() {
        gst::init().expect("GStreamer initialization");
        let caps = gst::Caps::builder("video/x-raw")
            .field("width", 720i32)
            .field("height", 480i32)
            .field("framerate", gst::Fraction::new(30_000, 1_001))
            .field("interlace-mode", "interleaved")
            .field("field-order", "bottom-field-first")
            .field("pixel-aspect-ratio", gst::Fraction::new(8, 9))
            .field("colorimetry", "bt601")
            .build();

        let parsed = source_caps_from_gstreamer(caps.as_ref()).expect("source caps");

        assert_eq!(parsed.scan_mode(), SourceScanMode::Interlaced);
        assert_eq!(parsed.field_order(), SourceFieldOrder::BottomFieldFirst);
        assert_eq!(parsed.pixel_aspect_ratio().numerator(), 8);
        assert_eq!(parsed.pixel_aspect_ratio().denominator(), 9);
        assert_eq!(parsed.colorimetry(), "bt601");
    }

    #[test]
    fn caps_change_requests_reconfigure_without_changing_effective_caps_in_place() {
        let effective = SourceCaps::fallback();
        let detected = SourceCaps::new(
            1280,
            720,
            60_000,
            1_001,
            SourceScanMode::Progressive,
            SourceFieldOrder::NotApplicable,
            1,
            1,
            "bt709",
        )
        .unwrap();
        let runtime = SourceCapsRuntime::new(effective.clone(), None, "fallback", None, None);

        runtime.observe(detected.clone());

        assert_eq!(runtime.effective_caps(), effective);
        assert_eq!(runtime.detected_caps(), Some(detected.clone()));
        assert_eq!(runtime.reconfigure_request(), Some(detected));
        assert_eq!(runtime.status_value()["reconfigure_required"], true);
    }

    #[test]
    fn last_known_caps_seed_the_next_pipeline_generation() {
        let directory = tempfile::tempdir().unwrap();
        let feed_id = FeedId::parse("CAP-IT-ALL").unwrap();
        let store = LastKnownCapsStore::new(directory.path(), &feed_id).unwrap();
        let last_known = SourceCaps::new(
            1280,
            720,
            60_000,
            1_001,
            SourceScanMode::Progressive,
            SourceFieldOrder::NotApplicable,
            1,
            1,
            "bt709",
        )
        .unwrap();
        store.save(&last_known).unwrap();
        let mut feed = test_feed();

        let runtime =
            prepare_source_caps_runtime(&mut feed, directory.path(), None).expect("caps runtime");

        assert_eq!(runtime.effective_caps(), last_known);
        assert_eq!(feed.video.width, 1280);
        assert_eq!(feed.video.height, 720);
        assert_eq!(feed.video.fps, "60000/1001");
        assert_eq!(
            runtime.status_value()["effective_origin"],
            "last_known_good"
        );
    }

    #[test]
    fn missing_caps_state_uses_safe_fallback_until_detection() {
        let directory = tempfile::tempdir().unwrap();
        let mut feed = test_feed();

        let runtime =
            prepare_source_caps_runtime(&mut feed, directory.path(), None).expect("caps runtime");

        assert_eq!(runtime.effective_caps(), SourceCaps::fallback());
        assert_eq!(feed.video.width, 720);
        assert_eq!(feed.video.height, 480);
        assert_eq!(feed.video.fps, "30000/1001");
        assert_eq!(runtime.status_value()["effective_origin"], "fallback");
        let enabled = feed.enabled_video_renditions(feed.video.width, feed.video.height);
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].id, "hd");
        assert_eq!(enabled[0].width, 720);
        assert_eq!(enabled[0].height, 480);
        let plan = GstPipelinePlan::from_feed(&feed)
            .expect("fallback caps must retain a bootstrap video rendition");
        assert_eq!(plan.videos.len(), 1);
        assert_eq!(plan.videos[0].width, 720);
        assert_eq!(plan.videos[0].height, 480);
    }

    #[test]
    fn source_audio_health_is_independent_from_video_health() {
        let mut feed = test_feed();
        feed.audio.mute_standby_routine = false;
        let state = RuntimeState::default();
        let mut health = InputHealth::default();

        health.mark_video_frame();
        assert!(health.video_connected(&feed));
        assert!(!health.audio_connected(&feed));
        assert_eq!(
            desired_video_selector_mode(&feed, &state, health.video_connected(&feed)),
            VideoSelectorMode::Program
        );
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, health.audio_connected(&feed)),
            AudioSelectorMode::Silence
        );

        health.mark_audio_frame();
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, health.audio_connected(&feed)),
            AudioSelectorMode::Program
        );
    }

    #[test]
    fn active_priority_owns_mix_while_waiting_for_correlated_bridge_pcm() {
        let mut feed = test_feed();
        feed.audio.mute_standby_routine = false;
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "q1",
            "data": {
                "queue_id": "q1",
                "audio_path": "runtime/audio/alerts/q1.pcm16le",
                "duration_ms": 60000
            }
        })));
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Priority
        );
        assert!(priority_audio_active(&feed, &state));
    }

    #[test]
    fn priority_audio_feeder_status_reports_live_chunks_and_errors() {
        gst::init().expect("gst init");
        let appsrc = gst::ElementFactory::make("appsrc")
            .build()
            .expect("appsrc")
            .downcast::<gst_app::AppSrc>()
            .expect("appsrc type");
        let media = crate::media_pcm::MediaPcmHub::new().subscribe("CAP-IT-ALL");
        let mut feeder = PriorityAudioFeeder::new(appsrc, media);
        feeder.active_queue_id = Some("q1".to_string());
        feeder.last_chunk_at = Some(Instant::now());

        assert_eq!(feeder.active_status().status, "active");
        feeder.error = Some("push failed".to_string());
        let status = feeder.active_status();
        assert_eq!(status.status, "error");
        assert_eq!(status.error.as_deref(), Some("push failed"));
    }

    #[test]
    fn priority_audio_feeder_pushes_only_correlated_bridge_pcm() {
        use base64::Engine as _;

        gst::init().expect("gst init");
        let element = gst::parse::launch(
            "appsrc name=priority_audio_src is-live=true block=false format=time ! fakesink sync=false",
        )
        .expect("parse appsrc pipeline");
        let pipeline = element.downcast::<gst::Pipeline>().expect("pipeline");
        let appsrc = priority_audio_appsrc(&pipeline).expect("priority appsrc");
        let hub = crate::media_pcm::MediaPcmHub::new();
        let media = hub.subscribe("CAP-IT-ALL");
        let mut feeder = PriorityAudioFeeder::new(appsrc, media);
        let pcm = base64::engine::general_purpose::STANDARD.encode(vec![0u8; 1_920]);
        hub.ingest_raw(
            &serde_json::to_vec(&json!({
                "type": "playout.pcm",
                "feed_id": "CAP-IT-ALL",
                "queue_id": "q1",
                "data": {
                    "feed_id": "CAP-IT-ALL",
                    "queue_id": "q1",
                    "sequence": 9,
                    "pts_ns": 180_000_000,
                    "discontinuity": true,
                    "sample_rate": 48_000,
                    "channels": 1,
                    "channel_layout": "mono",
                    "duration_ms": 20,
                    "media_kind": "alert",
                    "pcm": pcm,
                }
            }))
            .expect("PCM event"),
        );
        let audio = crate::state::PriorityAudio {
            queue_id: "q1".to_string(),
            audio_path: None,
            duration_ms: Some(20),
            sample_rate: 48_000,
            channels: 1,
            banner_text: None,
            background_color: None,
            priority: None,
            presentation: Default::default(),
            started_at: chrono::Utc::now(),
        };
        pipeline
            .set_state(gst::State::Playing)
            .expect("play pipeline");
        let status = feeder.update(Some(&audio));
        assert_eq!(status.status, "active");
        assert_eq!(status.queue_id.as_deref(), Some("q1"));
        assert_eq!(feeder.last_sequence, Some(9));
        pipeline.set_state(gst::State::Null).expect("stop pipeline");
    }

    #[test]
    fn completed_priority_audio_restores_program_audio() {
        let feed = test_feed();
        let status = PriorityAudioSourceStatus {
            status: "idle",
            queue_id: None,
            error: None,
        };

        assert_eq!(
            desired_audio_selector_mode_with_status(&feed, true, &status),
            AudioSelectorMode::Program
        );
    }

    #[test]
    fn waiting_for_correlated_pcm_uses_configured_alert_mix() {
        let feed = test_feed();
        let status = PriorityAudioSourceStatus {
            status: "waiting",
            queue_id: Some("q1".to_string()),
            error: None,
        };

        assert_eq!(
            desired_audio_selector_mode_with_status(&feed, true, &status),
            AudioSelectorMode::Priority
        );
    }

    #[test]
    fn priority_only_input_silences_when_no_priority_audio_is_live() {
        let mut feed = test_feed();
        feed.audio.mute_standby_routine = false;
        feed.priority_input.audio_source = "priority".to_string();
        let status = PriorityAudioSourceStatus {
            status: "idle",
            queue_id: None,
            error: None,
        };

        assert_eq!(
            desired_audio_selector_mode_with_status(&feed, true, &status),
            AudioSelectorMode::Silence
        );
    }

    #[test]
    fn priority_audio_without_bridge_pcm_keeps_canonical_alert_mix_active() {
        let mut feed = test_feed();
        feed.audio.mute_standby_routine = false;
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "q1",
            "data": {
                "queue_id": "q1",
                "duration_ms": 60000
            }
        })));
        assert_eq!(
            desired_audio_selector_mode(&feed, &state, true),
            AudioSelectorMode::Priority
        );
        assert!(priority_audio_active(&feed, &state));
    }

    #[test]
    fn priority_audio_waits_for_bridge_until_state_clears() {
        gst::init().expect("gst init");
        let appsrc = gst::ElementFactory::make("appsrc")
            .build()
            .expect("appsrc")
            .downcast::<gst_app::AppSrc>()
            .expect("appsrc type");
        let media = crate::media_pcm::MediaPcmHub::new().subscribe("CAP-IT-ALL");
        let mut feeder = PriorityAudioFeeder::new(appsrc, media);
        let audio = crate::state::PriorityAudio {
            queue_id: "q-missing".to_string(),
            audio_path: None,
            duration_ms: Some(60_000),
            sample_rate: 48_000,
            channels: 2,
            banner_text: None,
            background_color: None,
            priority: None,
            presentation: Default::default(),
            started_at: chrono::Utc::now(),
        };

        let first = feeder.update(Some(&audio));
        assert_eq!(first.status, "waiting");
        let held = feeder.update(Some(&audio));
        assert_eq!(held.status, "waiting");
        let cleared = feeder.update(None);
        assert_eq!(cleared.status, "idle");
    }
    #[test]
    fn text_overlay_state_tracks_banner_presentation() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        let idle = text_overlay_state(&feed, &state);
        assert!(idle.silent);

        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Native CGEN crawl"}]
            }
        })));
        let active = text_overlay_state(&feed, &state);
        assert!(!active.silent);
        assert_eq!(active.text, "Native CGEN crawl");
        assert!(active.font_desc.starts_with("Arial "));
        assert!((active.ypos - 0.08).abs() < 0.01);
        assert!(active.x_absolute > 1.0);
    }

    #[test]
    fn text_overlay_state_uses_canonical_priority_audio_text_when_banner_is_delayed() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "q-alert",
            "data": {
                "queue_id": "q-alert",
                "audio_path": "runtime/audio/alerts/q-alert.pcm16le",
                "duration_ms": 60000,
                "banner_text": "Priority alert crawl"
            }
        })));

        let active = text_overlay_state(&feed, &state);
        assert!(!active.silent);
        assert_eq!(active.text, "Priority alert crawl");
    }

    #[test]
    fn ticker_visual_finishes_after_banner_clears() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        let mut controller = TextOverlayController::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Native CGEN crawl should finish"}]
            }
        })));
        let active = controller.next_state(&feed, &state, false, true);
        assert!(!active.silent);
        assert_eq!(active.visual_id, "crawl-1");
        controller.last = Some(active);

        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": false,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": []
            }
        })));
        let retained = controller.next_state(&feed, &state, false, true);
        assert!(!retained.silent);
        assert_eq!(retained.source_text, "Native CGEN crawl should finish");
        assert_eq!(retained.visual_id, "crawl-1");
    }

    #[test]
    fn reissued_same_banner_restarts_after_retained_clear() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        let mut controller = TextOverlayController::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Same crawl should restart"}]
            }
        })));
        let mut active = controller.next_state(&feed, &state, false, true);
        let old_start = Instant::now() - Duration::from_secs(12);
        controller.started_at = Some(old_start);
        active.started_at = old_start;
        controller.last = Some(active);

        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": false,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": []
            }
        })));
        let retained = controller.next_state(&feed, &state, false, true);
        assert!(!retained.silent);
        controller.last = Some(retained);

        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Same crawl should restart"}]
            }
        })));
        let reissued = controller.next_state(&feed, &state, false, true);

        assert!(!reissued.silent);
        assert!(reissued.x_absolute > 1.0);
        assert_eq!(reissued.visual_id, "crawl-1");
    }

    #[test]
    fn operator_visual_override_clears_stale_ticker() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        let mut controller = TextOverlayController::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Native CGEN crawl should not linger"}]
            }
        })));
        let active = controller.next_state(&feed, &state, false, true);
        controller.last = Some(active);
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": false,
                "signature": "crawl-1",
                "feed_id": "CAP-IT-ALL",
                "alerts": []
            }
        })));
        assert!(state.apply_event(&json!({
            "type": "cgen.control",
            "subject": "CAP-IT-ALL",
            "data": {
                "feed_id": "CAP-IT-ALL",
                "action": "smpte_bars",
                "smpte_bars": true
            }
        })));

        let cleared = controller.next_state(&feed, &state, false, true);

        assert!(cleared.silent);
        assert_eq!(cleared.visual_mode, "smpte");
        assert_eq!(cleared.text, "");
    }

    #[test]
    fn ticker_repeats_while_priority_audio_is_live() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        let mut controller = TextOverlayController::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"queue_id": "q1", "message": "Repeat crawl"}]
            }
        })));

        let mut active = controller.next_state(&feed, &state, false, true);
        active.started_at = Instant::now() - Duration::from_secs(120);
        controller.last = Some(active);
        let repeated = controller.next_state(&feed, &state, false, true);

        assert!(!repeated.silent);
        assert!(repeated.visual_id.ends_with("#pass2"));
        assert_eq!(repeated.source_text, "Repeat crawl");
    }

    #[test]
    fn ticker_runs_configured_extra_passes_after_audio_release() {
        let mut feed = test_feed();
        feed.banner.after_eom_repeats = 1;
        let mut state = RuntimeState::default();
        let mut controller = TextOverlayController::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"queue_id": "q1", "message": "Post EOM crawl"}]
            }
        })));

        let mut active = controller.next_state(&feed, &state, false, true);
        active.started_at = Instant::now() - Duration::from_secs(120);
        controller.last = Some(active);
        let post_eom = controller.next_state(&feed, &state, false, false);

        assert!(!post_eom.silent);
        assert!(post_eom.visual_id.ends_with("#pass2"));
    }

    #[test]
    fn fixed_ticker_repeats_ignore_audio_lifetime() {
        let mut feed = test_feed();
        feed.banner.scroll_repeat_mode = "fixed".to_string();
        feed.banner.fixed_repeats = 1;
        let mut state = RuntimeState::default();
        let mut controller = TextOverlayController::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"queue_id": "q1", "message": "One pass only"}]
            }
        })));

        let mut active = controller.next_state(&feed, &state, false, true);
        active.started_at = Instant::now() - Duration::from_secs(120);
        controller.last = Some(active);
        let cleared = controller.next_state(&feed, &state, false, true);

        assert!(cleared.silent);
    }

    #[test]
    fn ticker_overlay_clears_after_exiting_left_edge() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Short crawl"}]
            }
        })));
        let snapshot = crate::graphics::presentation_snapshot(&feed, &state, false);
        let (start_x, start_silent) =
            ticker_x_absolute(&feed, &snapshot, &snapshot.overlay_text, Instant::now());
        let (end_x, end_silent) = ticker_x_absolute(
            &feed,
            &snapshot,
            &snapshot.overlay_text,
            Instant::now() - Duration::from_secs(30),
        );

        assert!(start_x > 1.0);
        assert!(!start_silent);
        assert!(end_x < 0.0);
        assert!(end_silent);
    }

    #[test]
    fn ticker_scroll_speed_ignores_output_fps() {
        let mut feed = test_feed();
        feed.video.fps = "60000/1001".to_string();
        feed.banner.scroll_speed = 12;
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Wallclock crawl"}]
            }
        })));
        let snapshot = crate::graphics::presentation_snapshot(&feed, &state, false);

        let (x_absolute, silent) = ticker_x_absolute(
            &feed,
            &snapshot,
            &snapshot.overlay_text,
            Instant::now() - Duration::from_secs(1),
        );

        let x_px = x_absolute * f64::from(feed.video.width);
        let expected = f64::from(feed.video.width) + 1.0 - (12.0 * 30.0);
        assert!(!silent);
        assert!((x_px - expected).abs() < 2.0);
    }

    #[test]
    fn ticker_done_state_is_stable_after_full_exit() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Long enough crawl text to fully exit"}]
            }
        })));
        let snapshot = crate::graphics::presentation_snapshot(&feed, &state, false);
        let (x1, silent1) = ticker_x_absolute(
            &feed,
            &snapshot,
            &snapshot.overlay_text,
            Instant::now() - Duration::from_secs(120),
        );
        let (x2, silent2) = ticker_x_absolute(
            &feed,
            &snapshot,
            &snapshot.overlay_text,
            Instant::now() - Duration::from_secs(180),
        );

        assert_eq!((x1, silent1), (-1.0, true));
        assert_eq!((x2, silent2), (-1.0, true));
    }

    #[test]
    fn publish_status_uses_no_signal_compositor_state() {
        let mut feed = test_feed();
        feed.standby.mode = "banner".to_string();
        feed.standby.text = "Standby Details Channel".to_string();
        let (_state_tx, state_rx) = watch::channel(RuntimeState::default());
        let (status_tx, status_rx) = watch::channel(None);

        publish_status(
            &Some(status_tx),
            &feed,
            &state_rx,
            true,
            json!({"media_backend": "test"}),
        );

        let status = status_rx.borrow().clone().expect("status payload");
        assert_eq!(status["no_signal"], true);
        assert_eq!(status["visual_mode"], "banner");
        assert_eq!(status["overlay_text"], "Standby Details Channel");
    }

    #[test]
    fn publish_status_keeps_the_latest_snapshot_without_queue_backpressure() {
        let feed = test_feed();
        let (_state_tx, state_rx) = watch::channel(RuntimeState::default());
        let (status_tx, status_rx) = watch::channel(None);

        publish_status(
            &Some(status_tx.clone()),
            &feed,
            &state_rx,
            true,
            json!({"sequence": 1}),
        );
        publish_status(
            &Some(status_tx),
            &feed,
            &state_rx,
            false,
            json!({"sequence": 2}),
        );

        let status = status_rx.borrow().clone().expect("latest status payload");
        assert_eq!(status["sequence"], 2);
        assert_eq!(status["no_signal"], false);
    }

    #[test]
    fn idle_overlay_state_does_not_require_frame_mutation() {
        let feed = test_feed();
        let state = RuntimeState::default();
        let idle = text_overlay_state(&feed, &state);

        assert!(!overlay_render_state_needs_frame_mutation(None));
        assert!(!overlay_render_state_needs_frame_mutation(Some(&idle)));

        let mut clock = idle.clone();
        clock.clock_text = "12:34".to_string();
        assert!(overlay_render_state_needs_frame_mutation(Some(&clock)));
    }

    #[test]
    fn preview_dispatcher_due_check_rate_limits_attempts() {
        let (dispatcher, _rx) = FramePreviewDispatcher::test_without_worker(1, 1);
        let now = Instant::now();

        assert!(dispatcher.claim_due_at(now));
        assert!(!dispatcher.claim_due_at(now + FRAME_PREVIEW_INTERVAL / 2));
        assert!(dispatcher.claim_due_at(now + FRAME_PREVIEW_INTERVAL));
    }

    #[test]
    fn preview_dispatcher_drops_when_its_single_frame_queue_is_full() {
        let (dispatcher, rx) = FramePreviewDispatcher::test_without_worker(1, 1);
        let frame = [0_u8; 4];
        let pixel_aspect_ratio = square_pixel_aspect_ratio();

        dispatcher.submit(&frame, pixel_aspect_ratio);
        dispatcher.submit(&frame, pixel_aspect_ratio);

        assert_eq!(
            dispatcher.status.submitted_frames.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            dispatcher
                .status
                .queue_dropped_frames
                .load(Ordering::Relaxed),
            1
        );
        let queued = rx.try_recv().expect("queued preview");
        assert_eq!(queued.bgrx, frame);
        assert_eq!(queued.pixel_aspect_ratio, pixel_aspect_ratio);
    }

    #[test]
    fn preview_dimensions_preserve_non_square_source_pixels() {
        let ntsc_4x3 = CapsRational::new(8, 9).expect("NTSC pixel aspect ratio");
        let ntsc_16x9 = CapsRational::new(32, 27).expect("anamorphic pixel aspect ratio");

        assert_eq!(
            preview_output_dimensions(720, 480, ntsc_4x3).expect("4:3 preview"),
            (720, 540)
        );
        assert_eq!(
            preview_output_dimensions(720, 480, ntsc_16x9).expect("16:9 preview"),
            (853, 480)
        );
        assert_eq!(
            preview_output_dimensions(1920, 1080, square_pixel_aspect_ratio()).expect("HD preview"),
            (960, 540)
        );
    }

    #[test]
    fn preview_resampler_interpolates_instead_of_repeating_nearest_pixels() {
        let frame = [
            0_u8, 0, 0, 255, 255, 255, 255, 255, 0, 0, 0, 255, 255, 255, 255, 255,
        ];

        let rgb = resample_bgrx_to_rgb(&frame, 2, 2, 3, 3).expect("resampled preview");

        assert_eq!(&rgb[..3], &[0, 0, 0]);
        assert_eq!(&rgb[12..15], &[128, 128, 128]);
        assert_eq!(&rgb[24..27], &[255, 255, 255]);
    }

    #[test]
    fn preview_resampler_fast_paths_preserve_interpolation_and_channel_order() {
        let horizontal = [0_u8, 0, 255, 255, 255, 0, 0, 255];
        let vertical = [255_u8, 0, 0, 255, 0, 0, 255, 255];

        let horizontal_rgb =
            resample_bgrx_to_rgb(&horizontal, 2, 1, 3, 1).expect("horizontal resample");
        let vertical_rgb = resample_bgrx_to_rgb(&vertical, 1, 2, 1, 3).expect("vertical resample");
        let direct_rgb = resample_bgrx_to_rgb(&horizontal, 2, 1, 2, 1).expect("direct conversion");

        assert_eq!(horizontal_rgb, [255, 0, 0, 128, 0, 128, 0, 0, 255]);
        assert_eq!(vertical_rgb, [0, 0, 255, 128, 0, 128, 255, 0, 0]);
        assert_eq!(direct_rgb, [255, 0, 0, 0, 0, 255]);
    }

    #[test]
    fn preview_writer_replaces_an_existing_jpeg() {
        let directory = tempfile::tempdir().expect("temporary preview directory");
        let path = directory.path().join("feed.preview.jpg");
        let black = vec![0_u8; 2 * 2 * 4];
        let white = vec![255_u8; 2 * 2 * 4];

        write_bgrx_preview_jpeg(&path, &black, 2, 2, square_pixel_aspect_ratio())
            .expect("first preview");
        write_bgrx_preview_jpeg(&path, &white, 2, 2, square_pixel_aspect_ratio())
            .expect("replacement preview");

        let jpeg = std::fs::read(path).expect("preview jpeg");
        assert!(jpeg.starts_with(&[0xff, 0xd8]));
        assert!(jpeg.ends_with(&[0xff, 0xd9]));
    }

    fn test_feed() -> FeedConfig {
        FeedConfig {
            id: "CAP-IT-ALL".to_string(),
            name: "CAP CGEN".to_string(),
            enabled: true,
            program_input: EndpointConfig {
                url: "udp://172.16.1.30:9098?fifo_size=2000000&overrun_nonfatal=1&reuse=1"
                    .to_string(),
                format: "mpegts".to_string(),
                ..Default::default()
            },
            priority_input: PriorityInputConfig {
                feed_id: "*".to_string(),
                audio_source: "both".to_string(),
                format: "priority-audio".to_string(),
            },
            program_output: EndpointConfig {
                url: "udp://239.0.0.2:9001?pkt_size=1316".to_string(),
                format: "mpegts".to_string(),
                vcodec: "mpeg2video".to_string(),
                acodec: "ac3".to_string(),
                video_bitrate_kbps: Some(12_000),
                audio_bitrate_kbps: Some(192),
                ..Default::default()
            },
            program: Default::default(),
            priority: Default::default(),
            media: Default::default(),
            presentation: Default::default(),
            video: VideoConfig {
                width: 1920,
                height: 1080,
                fps: "30000/1001".to_string(),
                interlaced: true,
                field_order: "tff".to_string(),
                standard: "atsc".to_string(),
            },
            audio: AudioConfig {
                idle: "source".to_string(),
                alert_mode: "replace".to_string(),
                mute_standby_routine: true,
                ..Default::default()
            },
            ladder: LadderConfig {
                videos: vec![VideoRenditionConfig {
                    id: "hd".to_string(),
                    enabled: "auto".to_string(),
                    width: 1920,
                    height: 1080,
                    fps: "30000/1001".to_string(),
                    interlaced: true,
                    field_order: "tff".to_string(),
                    standard: "atsc".to_string(),
                    vcodec: "mpeg2video".to_string(),
                    bitrate_kbps: Some(12_000),
                    program: Some(1),
                    video_pid: Some(0x100),
                    pmt_pid: Some(0x1000),
                }],
                audios: vec![
                    AudioRenditionConfig {
                        id: "stereo".to_string(),
                        enabled: "true".to_string(),
                        channels: 2,
                        acodec: "ac3".to_string(),
                        bitrate_kbps: Some(192),
                        language: "eng".to_string(),
                        program: Some(1),
                        audio_pid: Some(0x101),
                        pmt_pid: Some(0x1000),
                    },
                    AudioRenditionConfig {
                        id: "surround_51".to_string(),
                        enabled: "false".to_string(),
                        channels: 6,
                        acodec: "ac3".to_string(),
                        bitrate_kbps: Some(384),
                        language: "eng".to_string(),
                        program: Some(1),
                        audio_pid: Some(0x102),
                        pmt_pid: Some(0x1000),
                    },
                ],
            },
            banner: BannerConfig::default(),
            graphics: GraphicsConfig::default(),
            clock: ClockConfig::default(),
            text: TextConfig::default(),
            state: StateConfig::default(),
            standby: StandbyConfig::default(),
            sync: SyncConfig::default(),
            alert: Default::default(),
            ancillary: Default::default(),
            compositor: Default::default(),
            program_mapping: Default::default(),
            outputs: Default::default(),
            encoder: Default::default(),
            output_encoders: Default::default(),
        }
    }
}
