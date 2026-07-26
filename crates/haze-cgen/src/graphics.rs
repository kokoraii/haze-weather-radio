use serde_json::{json, Value};

use crate::config::FeedConfig;
use crate::state::{
    BannerPayload, CgenControlOverride, PriorityAudio, RuntimeState, SerializedAlert,
};
use crate::sunny_cat;

const TICKER_SPEED_BASE_FPS: f64 = 30.0;

pub(crate) fn ticker_speed_px_per_second(scroll_speed: u32) -> f64 {
    f64::from(scroll_speed.max(1)) * TICKER_SPEED_BASE_FPS
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PresentationSnapshot {
    pub(crate) visual_mode: String,
    pub(crate) overlay_active: bool,
    pub(crate) overlay_text: String,
    pub(crate) ticker_color: String,
    pub(crate) ticker_gradient: [String; 3],
    pub(crate) ticker_y: i32,
    pub(crate) ticker_height: u32,
    pub(crate) ticker_speed_px_per_frame: u32,
    pub(crate) font: String,
    pub(crate) font_weight: String,
    pub(crate) font_size: u32,
    pub(crate) clock_text: String,
    pub(crate) clock_enabled: bool,
    pub(crate) clock_x: i32,
    pub(crate) clock_y: i32,
    pub(crate) clock_font_size: u32,
    pub(crate) clock_color: String,
    pub(crate) state_mode: String,
    pub(crate) smpte_bars: bool,
    pub(crate) visual_id: String,
    pub(crate) active_alert_queue_id: Option<String>,
    pub(crate) sunny_cat: bool,
    pub(crate) sunny_cat_available: bool,
}

pub(crate) fn presentation_snapshot(
    feed: &FeedConfig,
    state: &RuntimeState,
    no_signal: bool,
) -> PresentationSnapshot {
    let effective = EffectivePresentation::new(feed, state);
    let priority_feed_id = priority_feed_id(feed);
    let banner_feed_id = banner_feed_id(feed, priority_feed_id);
    let banner = state.banner_for(banner_feed_id);
    let audio = state.priority_audio_for(priority_feed_id);
    let has_visual_input = banner.is_some() || audio.is_some();
    let overlay_text = if has_visual_input {
        overlay_text(&effective, banner, audio)
    } else if no_signal && effective.standby_mode.eq_ignore_ascii_case("banner") {
        effective.standby_text.trim().to_string()
    } else if effective.manual_overlay_enabled() && effective.text_enabled {
        effective.text_content.trim().to_string()
    } else {
        String::new()
    };
    let clock_text = effective.clock_text();
    let ticker_color = ticker_color(banner, audio).unwrap_or_else(|| "#111827".to_string());
    let gradient = ticker_gradient(banner, audio, &ticker_color);
    let sunny_cat_available = sunny_cat::available();
    let sunny_cat =
        effective.manual_overlay_enabled() && effective.sunny_cat && sunny_cat_available;
    PresentationSnapshot {
        visual_mode: visual_mode(&effective, has_visual_input, no_signal).to_string(),
        overlay_active: !overlay_text.trim().is_empty()
            || !clock_text.trim().is_empty()
            || effective.smpte_bars
            || sunny_cat,
        overlay_text,
        ticker_color,
        ticker_gradient: [
            rgb_to_hex(gradient.top),
            rgb_to_hex(gradient.middle),
            rgb_to_hex(gradient.bottom),
        ],
        ticker_y: ticker_y(
            &effective,
            i32::try_from(feed.video.height).unwrap_or(1080),
            no_signal,
        ),
        ticker_height: effective.ticker_height.max(48),
        ticker_speed_px_per_frame: effective.scroll_speed.max(1),
        font: effective.font.clone(),
        font_weight: effective.font_weight.clone(),
        font_size: effective.font_size.max(16),
        clock_text,
        clock_enabled: effective.manual_overlay_enabled() && effective.clock_enabled,
        clock_x: effective.clock_x,
        clock_y: effective.clock_y,
        clock_font_size: effective.clock_font_size.max(12),
        clock_color: effective.clock_color.clone(),
        state_mode: effective.state_mode.clone(),
        smpte_bars: effective.smpte_bars,
        visual_id: banner
            .map(banner_visual_id)
            .or_else(|| audio.map(|audio| audio.queue_id.clone()))
            .unwrap_or_default(),
        active_alert_queue_id: audio.map(|audio| audio.queue_id.clone()),
        sunny_cat,
        sunny_cat_available,
    }
}

pub(crate) fn compositor_status(feed: &FeedConfig, state: &RuntimeState, no_signal: bool) -> Value {
    let snapshot = presentation_snapshot(feed, state, no_signal);
    json!({
        "visual_mode": snapshot.visual_mode,
        "state_mode": snapshot.state_mode,
        "smpte_bars": snapshot.smpte_bars,
        "sunny_cat": snapshot.sunny_cat,
        "sunny_cat_available": snapshot.sunny_cat_available,
        "overlay_active": snapshot.overlay_active,
        "overlay_text": snapshot.overlay_text,
        "ticker_color": snapshot.ticker_color,
        "ticker_gradient": snapshot.ticker_gradient,
        "ticker_y": snapshot.ticker_y,
        "ticker_height": snapshot.ticker_height,
        "ticker_speed_px_per_frame": snapshot.ticker_speed_px_per_frame,
        "scroll_repeat_mode": feed.banner.scroll_repeat_mode,
        "after_eom_repeats": feed.banner.after_eom_repeats,
        "fixed_repeats": feed.banner.fixed_repeats,
        "font": snapshot.font,
        "font_weight": snapshot.font_weight,
        "font_size": snapshot.font_size,
        "visual_id": snapshot.visual_id,
        "clock_enabled": snapshot.clock_enabled,
        "clock_text": snapshot.clock_text,
        "clock_x": snapshot.clock_x,
        "clock_y": snapshot.clock_y,
        "clock_font_size": snapshot.clock_font_size,
        "clock_color": snapshot.clock_color,
        "text_enabled": !snapshot.overlay_text.trim().is_empty(),
        "no_signal": no_signal,
        "active_alert_queue_id": snapshot.active_alert_queue_id,
    })
}

#[derive(Debug, Clone)]
struct EffectivePresentation {
    state_mode: String,
    smpte_bars: bool,
    sunny_cat: bool,
    banner_mode: String,
    ticker_height: u32,
    scroll_speed: u32,
    font: String,
    font_weight: String,
    font_size: u32,
    text_enabled: bool,
    text_content: String,
    clock_enabled: bool,
    clock_format: String,
    clock_x: i32,
    clock_y: i32,
    clock_font_size: u32,
    clock_color: String,
    standby_mode: String,
    standby_text: String,
    standby_y_percent: u32,
}

impl EffectivePresentation {
    fn new(feed: &FeedConfig, state: &RuntimeState) -> Self {
        let control = state.control_for(feed.id.as_str());
        let font = string_override(control, "font")
            .or_else(|| non_empty_ref(&feed.banner.font).map(str::to_string))
            .or_else(|| non_empty_ref(&feed.graphics.font).map(str::to_string))
            .unwrap_or_else(|| "Arial".to_string());
        let font_weight = string_override(control, "font_weight")
            .or_else(|| non_empty_ref(&feed.banner.font_weight).map(str::to_string))
            .or_else(|| non_empty_ref(&feed.graphics.font_weight).map(str::to_string))
            .unwrap_or_else(|| "regular".to_string());
        let font_size = u32_override(control, "font_size")
            .unwrap_or_else(|| feed.banner.font_size.max(feed.graphics.font_size))
            .max(16);
        Self {
            state_mode: string_override(control, "mode").unwrap_or_else(|| feed.state.mode.clone()),
            smpte_bars: bool_override(control, "smpte_bars").unwrap_or(feed.state.smpte_bars),
            sunny_cat: bool_override(control, "sunny_cat").unwrap_or(feed.state.sunny_cat),
            banner_mode: string_override(control, "banner_mode").unwrap_or_else(|| {
                if feed
                    .compositor
                    .alert_scene_id
                    .eq_ignore_ascii_case("Fullscreen_Takeover")
                {
                    "fullscreen".to_string()
                } else if feed
                    .compositor
                    .alert_scene_id
                    .eq_ignore_ascii_case("Standard_Crawl")
                {
                    "ticker".to_string()
                } else {
                    feed.banner.mode.clone()
                }
            }),
            ticker_height: u32_override(control, "ticker_height")
                .or_else(|| u32_override(control, "banner_height"))
                .unwrap_or_else(|| feed.banner.ticker_height.max(feed.graphics.banner_height)),
            scroll_speed: u32_override(control, "scroll_speed").unwrap_or(feed.banner.scroll_speed),
            font,
            font_weight,
            font_size,
            text_enabled: bool_override(control, "text_enabled").unwrap_or(feed.text.enabled),
            text_content: string_override(control, "text")
                .unwrap_or_else(|| feed.text.content.clone()),
            clock_enabled: bool_override(control, "clock_enabled").unwrap_or(feed.clock.enabled),
            clock_format: string_override(control, "clock_format")
                .unwrap_or_else(|| feed.clock.format.clone()),
            clock_x: i32_override(control, "clock_x").unwrap_or(feed.clock.x),
            clock_y: i32_override(control, "clock_y").unwrap_or(feed.clock.y),
            clock_font_size: u32_override(control, "clock_font_size")
                .unwrap_or(feed.clock.font_size),
            clock_color: string_override(control, "clock_color")
                .unwrap_or_else(|| feed.clock.color.clone()),
            standby_mode: string_override(control, "standby_mode")
                .unwrap_or_else(|| feed.standby.mode.clone()),
            standby_text: string_override(control, "standby_text")
                .unwrap_or_else(|| feed.standby.text.clone()),
            standby_y_percent: u32_override(control, "standby_y_percent")
                .unwrap_or(feed.standby.y_percent),
        }
    }

    fn manual_overlay_enabled(&self) -> bool {
        self.state_mode.eq_ignore_ascii_case("overlay")
            || self.state_mode.eq_ignore_ascii_case("smpte")
    }

    fn clock_text(&self) -> String {
        if !self.manual_overlay_enabled() || !self.clock_enabled {
            return String::new();
        }
        let layout = if self.clock_format.trim().is_empty() {
            "%b %d %H:%M:%S".to_string()
        } else {
            chrono_clock_layout(self.clock_format.trim())
        };
        chrono::Local::now().format(&layout).to_string()
    }
}

fn priority_feed_id(feed: &FeedConfig) -> &str {
    if !feed.alert.feed_id.trim().is_empty() {
        feed.alert.feed_id.as_str()
    } else if feed.priority_input.feed_id.trim().is_empty()
        || feed.priority_input.feed_id.trim() == "*"
    {
        feed.id.as_str()
    } else {
        feed.priority_input.feed_id.as_str()
    }
}

fn banner_feed_id<'a>(feed: &'a FeedConfig, priority_feed_id: &'a str) -> &'a str {
    let _ = feed;
    priority_feed_id
}

fn banner_visual_id(banner: &BannerPayload) -> String {
    if let Some(signature) = non_empty_ref(&banner.signature) {
        return signature.to_string();
    }
    let mut parts = Vec::new();
    for alert in &banner.alerts {
        for key in [
            "queue_id",
            "identifier",
            "id",
            "alert_id",
            "event",
            "message",
        ] {
            if let Some(text) = alert.fields.get(key).and_then(Value::as_str) {
                let text = text.trim();
                if !text.is_empty() {
                    parts.push(text.to_string());
                    break;
                }
            }
        }
    }
    if !parts.is_empty() {
        return parts.join("|");
    }
    String::new()
}

fn visual_mode(effective: &EffectivePresentation, has_visual_input: bool, no_signal: bool) -> &str {
    if effective.smpte_bars || effective.state_mode.eq_ignore_ascii_case("smpte") {
        "smpte"
    } else if has_visual_input {
        if effective.banner_mode.eq_ignore_ascii_case("fullscreen") {
            "fullscreen_alert"
        } else {
            "ticker_alert"
        }
    } else if no_signal {
        effective.standby_mode.trim()
    } else if effective.manual_overlay_enabled()
        && (effective.text_enabled || effective.clock_enabled || effective.sunny_cat)
    {
        "overlay"
    } else {
        "release"
    }
}

fn overlay_text(
    effective: &EffectivePresentation,
    banner: Option<&BannerPayload>,
    audio: Option<&PriorityAudio>,
) -> String {
    let mut parts = Vec::new();
    if let Some(text) = audio.and_then(|audio| audio.banner_text.as_deref()) {
        push_text_part(&mut parts, text);
    }
    if let Some(audio) = audio {
        push_text_part(&mut parts, &audio.presentation.full_text);
        if parts.is_empty() {
            push_text_part(&mut parts, &audio.presentation.organization);
            push_text_part(&mut parts, &audio.presentation.action);
            push_text_part(&mut parts, &audio.presentation.event);
        }
    }
    if let Some(banner) = banner {
        for alert in &banner.alerts {
            if let Some(text) = alert_text(alert) {
                push_text_part(&mut parts, text);
            }
        }
    }
    if effective.manual_overlay_enabled() && effective.text_enabled {
        push_text_part(&mut parts, &effective.text_content);
    }
    parts.join("     ").chars().take(1800).collect()
}

fn alert_text(alert: &SerializedAlert) -> Option<&str> {
    for key in [
        "banner_text",
        "message",
        "scroll_text",
        "headline",
        "title",
        "event_name",
        "event",
        "description",
        "identifier",
    ] {
        if let Some(text) = alert.fields.get(key).and_then(Value::as_str) {
            let text = text.trim();
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn push_text_part(parts: &mut Vec<String>, text: &str) {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if !text.is_empty() && !parts.iter().any(|part| part == &text) {
        parts.push(text);
    }
}

fn ticker_y(effective: &EffectivePresentation, frame_height: i32, no_signal: bool) -> i32 {
    if no_signal {
        let percent = effective.standby_y_percent.min(100) as f32 / 100.0;
        return ((frame_height.max(1) as f32) * percent).round() as i32;
    }
    ((frame_height.max(1) as f32) * 0.08).round() as i32
}

fn string_override(control: Option<&CgenControlOverride>, key: &str) -> Option<String> {
    control
        .and_then(|control| control.string_field(key))
        .map(str::to_string)
}

fn bool_override(control: Option<&CgenControlOverride>, key: &str) -> Option<bool> {
    control.and_then(|control| control.bool_field(key))
}

fn u32_override(control: Option<&CgenControlOverride>, key: &str) -> Option<u32> {
    control.and_then(|control| control.u32_field(key))
}

fn i32_override(control: Option<&CgenControlOverride>, key: &str) -> Option<i32> {
    control.and_then(|control| control.i32_field(key))
}

fn chrono_clock_layout(layout: &str) -> String {
    let mut out = layout.to_string();
    for (from, to) in [
        ("January", "%B"),
        ("Jan", "%b"),
        ("Monday", "%A"),
        ("Mon", "%a"),
        ("2006", "%Y"),
        ("06", "%y"),
        ("15", "%H"),
        ("03", "%I"),
        ("04", "%M"),
        ("05", "%S"),
        ("PM", "%p"),
        ("pm", "%P"),
        ("MST", "%Z"),
        ("02", "%d"),
    ] {
        out = out.replace(from, to);
    }
    out
}

#[cfg(test)]
fn ticker_text_x(frame_width: i32, _text_width: i32, frame_index: u64, scroll_speed: u32) -> i32 {
    let start_x = frame_width.saturating_add(1).max(1);
    let speed = scroll_speed.max(1) as i64;
    let scroll = (frame_index as i64).saturating_mul(speed);
    i64::from(start_x).saturating_sub(scroll) as i32
}

#[cfg(test)]
fn ticker_text_x_field(
    frame_width: i32,
    text_width: i32,
    field_index: u64,
    scroll_speed: u32,
) -> i32 {
    let start_x = i128::from(frame_width.saturating_add(1).max(1));
    let scroll = i128::from(field_index).saturating_mul(i128::from(scroll_speed.max(1))) / 2;
    let min_x = -i128::from(text_width.max(1));
    let value = start_x.saturating_sub(scroll).max(min_x);
    i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
}

#[cfg(test)]
fn ticker_scroll_frames(frame_width: i32, text_width: i32, scroll_speed: u32) -> u64 {
    let start_x = frame_width.saturating_add(1).max(1);
    let travel = start_x.saturating_add(text_width.max(1)).saturating_add(1);
    let speed = scroll_speed.max(1);
    u64::from((travel as u32).div_ceil(speed))
}

#[cfg(test)]
fn field_parities(feed: &FeedConfig) -> [i32; 2] {
    if feed.video.field_order.eq_ignore_ascii_case("bff")
        || feed.video.field_order.eq_ignore_ascii_case("bottom")
        || feed.video.field_order.eq_ignore_ascii_case("bottom_first")
    {
        [1, 0]
    } else {
        [0, 1]
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TickerGradient {
    top: [f32; 3],
    middle: [f32; 3],
    bottom: [f32; 3],
}

fn ticker_gradient(
    banner: Option<&BannerPayload>,
    audio: Option<&PriorityAudio>,
    base_color: &str,
) -> TickerGradient {
    if let Some(colors) = banner_gradient_colors(banner) {
        return colors;
    }
    if let Some(color) = audio
        .and_then(|audio| audio.background_color.as_deref())
        .and_then(non_empty_ref)
    {
        let rgb = rgb_from_hex_loose(color);
        return TickerGradient {
            top: rgb,
            middle: rgb,
            bottom: rgb,
        };
    }
    let middle = rgb_from_hex_loose(base_color);
    let edge = darken_rgb(middle, 0.62);
    TickerGradient {
        top: edge,
        middle,
        bottom: edge,
    }
}

fn ticker_color<'a>(
    banner: Option<&'a BannerPayload>,
    audio: Option<&'a PriorityAudio>,
) -> Option<String> {
    banner
        .and_then(|banner| non_empty_ref(&banner.primary_color))
        .or_else(|| {
            audio
                .and_then(|audio| audio.background_color.as_deref())
                .and_then(non_empty_ref)
        })
        .map(ToString::to_string)
}

fn banner_gradient_colors(banner: Option<&BannerPayload>) -> Option<TickerGradient> {
    let stops = &banner?.primary_gradient;
    let colors = stops
        .iter()
        .filter_map(|stop| non_empty_ref(stop))
        .map(rgb_from_hex_loose)
        .collect::<Vec<_>>();
    match colors.as_slice() {
        [] => None,
        [only] => Some(TickerGradient {
            top: *only,
            middle: *only,
            bottom: *only,
        }),
        [first, .., last] => Some(TickerGradient {
            top: *last,
            middle: *first,
            bottom: *last,
        }),
    }
}

fn rgb_from_hex_loose(value: &str) -> [f32; 3] {
    let value = value.trim().trim_start_matches('#');
    if value.len() != 6 {
        return [0.5, 0.5, 0.5];
    }
    let r = u8::from_str_radix(&value[0..2], 16).unwrap_or(128) as f32 / 255.0;
    let g = u8::from_str_radix(&value[2..4], 16).unwrap_or(128) as f32 / 255.0;
    let b = u8::from_str_radix(&value[4..6], 16).unwrap_or(128) as f32 / 255.0;
    [r, g, b]
}

fn darken_rgb(rgb: [f32; 3], amount: f32) -> [f32; 3] {
    [
        (rgb[0] * amount).clamp(0.0, 1.0),
        (rgb[1] * amount).clamp(0.0, 1.0),
        (rgb[2] * amount).clamp(0.0, 1.0),
    ]
}

fn rgb_to_hex(rgb: [f32; 3]) -> String {
    let r = (rgb[0].clamp(0.0, 1.0) * 255.0).round() as u8;
    let g = (rgb[1].clamp(0.0, 1.0) * 255.0).round() as u8;
    let b = (rgb[2].clamp(0.0, 1.0) * 255.0).round() as u8;
    format!("#{r:02x}{g:02x}{b:02x}")
}

#[cfg(test)]
fn yuv_from_hex(value: &str) -> (u8, u8, u8) {
    let value = value.trim().trim_start_matches('#');
    if value.len() != 6 {
        return (128, 128, 128);
    }
    let r = u8::from_str_radix(&value[0..2], 16).unwrap_or(128) as f32;
    let g = u8::from_str_radix(&value[2..4], 16).unwrap_or(128) as f32;
    let b = u8::from_str_radix(&value[4..6], 16).unwrap_or(128) as f32;
    let y = (0.257 * r + 0.504 * g + 0.098 * b + 16.0)
        .round()
        .clamp(16.0, 235.0) as u8;
    let u = (-0.148 * r - 0.291 * g + 0.439 * b + 128.0)
        .round()
        .clamp(16.0, 240.0) as u8;
    let v = (0.439 * r - 0.368 * g - 0.071 * b + 128.0)
        .round()
        .clamp(16.0, 240.0) as u8;
    (y, u, v)
}

fn non_empty_ref(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AudioConfig, BannerConfig, ClockConfig, EndpointConfig, GraphicsConfig, LadderConfig,
        PriorityInputConfig, StandbyConfig, StateConfig, SyncConfig, TextConfig, VideoConfig,
    };

    #[test]
    fn overlay_text_prefers_priority_audio_then_banner() {
        let mut feed = test_feed();
        feed.state.mode = "overlay".to_string();
        feed.text.enabled = true;
        feed.text.content = "Manual".to_string();
        let audio = PriorityAudio {
            queue_id: "q".to_string(),
            audio_path: None,
            duration_ms: None,
            sample_rate: 48_000,
            channels: 2,
            banner_text: Some("Audio banner".to_string()),
            background_color: None,
            priority: None,
            presentation: Default::default(),
            started_at: chrono::Utc::now(),
        };
        let banner = BannerPayload {
            active: true,
            alerts: vec![SerializedAlert {
                fields: [(
                    "message".to_string(),
                    Value::String("Visual banner".to_string()),
                )]
                .into_iter()
                .collect(),
            }],
            ..Default::default()
        };
        let state = RuntimeState::default();
        let effective = EffectivePresentation::new(&feed, &state);
        let text = overlay_text(&effective, Some(&banner), Some(&audio));
        assert!(text.starts_with("Audio banner"));
        assert!(text.contains("Visual banner"));
        assert!(text.contains("Manual"));
    }

    #[test]
    fn yuv_color_conversion_handles_hex() {
        let white = yuv_from_hex("#ffffff");
        let orange = yuv_from_hex("#b45309");
        assert!(white.0 > orange.0);
        assert_ne!(orange.1, 128);
    }

    #[test]
    fn ticker_scroll_starts_off_right_and_exits_left() {
        assert_eq!(ticker_text_x(1920, 320, 0, 4), 1921);
        let exit_frame = (1921 + 320) / 4;
        assert!(ticker_text_x(1920, 320, exit_frame as u64, 4) <= -319);
        assert_eq!(ticker_scroll_frames(1920, 320, 4), 561);
    }

    #[test]
    fn ticker_scroll_fields_advance_half_a_frame_apart() {
        assert_eq!(ticker_text_x_field(1920, 320, 0, 8), 1921);
        assert_eq!(ticker_text_x_field(1920, 320, 1, 8), 1917);
        assert_eq!(ticker_text_x_field(1920, 320, 2, 8), 1913);
    }

    #[test]
    fn interlaced_field_parities_follow_field_order() {
        let mut feed = test_feed();
        feed.video.interlaced = true;
        feed.video.field_order = "tff".to_string();
        assert_eq!(field_parities(&feed), [0, 1]);
        feed.video.field_order = "bff".to_string();
        assert_eq!(field_parities(&feed), [1, 0]);
    }

    #[test]
    fn default_ticker_y_is_eight_percent_down() {
        let feed = test_feed();
        let state = RuntimeState::default();
        let effective = EffectivePresentation::new(&feed, &state);
        assert_eq!(ticker_y(&effective, 1080, false), 86);
    }

    #[test]
    fn no_signal_banner_uses_standby_text_and_position() {
        let feed = test_feed();
        let state = RuntimeState::default();

        let snapshot = presentation_snapshot(&feed, &state, true);

        assert_eq!(snapshot.visual_mode, "banner");
        assert_eq!(snapshot.overlay_text, "Emergency Alert Details Channel");
        assert_eq!(snapshot.ticker_y, 72);
    }

    #[test]
    fn html_banner_gradient_order_is_preserved() {
        let banner = BannerPayload {
            primary_gradient: vec!["#ff0000".to_string(), "#220000".to_string()],
            ..Default::default()
        };
        let gradient = banner_gradient_colors(Some(&banner)).expect("gradient");
        assert_eq!(gradient.top, rgb_from_hex_loose("#220000"));
        assert_eq!(gradient.middle, rgb_from_hex_loose("#ff0000"));
        assert_eq!(gradient.bottom, rgb_from_hex_loose("#220000"));
    }

    #[test]
    fn presentation_snapshot_separates_audio_and_visual_sources() {
        let feed = test_feed();
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP",
            "data": {
                "active": true,
                "feed_id": "CAP",
                "primary_color": "#b91c1c",
                "primary_gradient": ["#b91c1c", "#7f1d1d"],
                "alerts": [{"message": "Visual only"}]
            }
        })));
        let snapshot = presentation_snapshot(&feed, &state, false);
        assert_eq!(snapshot.visual_mode, "ticker_alert");
        assert!(snapshot.overlay_active);
        assert_eq!(snapshot.overlay_text, "Visual only");
        assert_eq!(snapshot.active_alert_queue_id, None);
    }

    fn test_feed() -> FeedConfig {
        FeedConfig {
            id: "CAP".to_string(),
            name: "CAP CGEN".to_string(),
            enabled: true,
            program_input: EndpointConfig {
                url: "udp://239.0.0.1:9000".to_string(),
                format: "mpegts".to_string(),
                ..Default::default()
            },
            priority_input: PriorityInputConfig {
                feed_id: "CAP".to_string(),
                ..Default::default()
            },
            program_output: EndpointConfig {
                url: "udp://239.0.0.2:9001".to_string(),
                format: "mpegts".to_string(),
                ..Default::default()
            },
            program: Default::default(),
            priority: Default::default(),
            media: Default::default(),
            presentation: Default::default(),
            video: VideoConfig {
                width: 1280,
                height: 720,
                fps: "source".to_string(),
                interlaced: false,
                field_order: "tff".to_string(),
                standard: String::new(),
            },
            audio: AudioConfig::default(),
            ladder: LadderConfig::default(),
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
