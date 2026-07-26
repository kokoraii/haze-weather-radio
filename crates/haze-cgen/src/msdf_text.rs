//! Additive MSDF text preparation for the scene compositor.
//!
//! The legacy glyphon renderer remains the active renderer for the compatibility
//! release. This module owns the scene renderer's immutable text layout, bounded
//! native MSDF generation, atlas lifetime rules, and shader contract so the new
//! path can be exercised in shadow preview before cutover.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use std::time::Duration;

use thiserror::Error;

use crate::scene::{RgbaColor, ScrollDirection, TextScroll, TextStyle};

pub(crate) const DEFAULT_GLYPH_SIZE: u32 = 64;
pub(crate) const DEFAULT_DISTANCE_RANGE_PX: f32 = 8.0;

/// The fragment shader samples a non-sRGB texture. RGB stores the three signed
/// distance channels and alpha is unused by the distance calculation.
pub(crate) const ATLAS_TEXTURE_FORMAT: &str = "rgba8unorm-linear-rgb";

/// A stable glyph cache key. Font identity is derived from the font bytes and
/// face index, while weight keeps variable-font outlines distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct GlyphKey {
    pub(crate) font_fingerprint: u64,
    pub(crate) face_index: u32,
    pub(crate) glyph_id: u16,
    pub(crate) weight: u16,
}

/// Full atlas plane bounds in em units, relative to a glyph baseline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GlyphPlaneBounds {
    pub(crate) left: f32,
    pub(crate) bottom: f32,
    pub(crate) right: f32,
    pub(crate) top: f32,
}

impl GlyphPlaneBounds {
    fn is_valid(self) -> bool {
        [self.left, self.bottom, self.right, self.top]
            .into_iter()
            .all(f32::is_finite)
            && self.left < self.right
            && self.bottom < self.top
    }
}

/// CPU output ready for upload to an atlas page.
#[derive(Clone, PartialEq)]
pub(crate) struct GeneratedGlyph {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) rgba: Arc<[u8]>,
    pub(crate) plane_bounds: GlyphPlaneBounds,
    pub(crate) distance_range_px: f32,
}

impl fmt::Debug for GeneratedGlyph {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedGlyph")
            .field("width", &self.width)
            .field("height", &self.height)
            .field("rgba_bytes", &self.rgba.len())
            .field("plane_bounds", &self.plane_bounds)
            .field("distance_range_px", &self.distance_range_px)
            .finish()
    }
}

impl GeneratedGlyph {
    fn validate(&self, slot_size: u32) -> Result<(), AtlasError> {
        let expected_len = usize::try_from(self.width)
            .ok()
            .and_then(|width| {
                usize::try_from(self.height)
                    .ok()
                    .and_then(|height| width.checked_mul(height))
            })
            .and_then(|pixels| pixels.checked_mul(4))
            .ok_or(AtlasError::InvalidGeneratedGlyph)?;
        if self.width == 0
            || self.height == 0
            || self.width > slot_size
            || self.height > slot_size
            || self.rgba.len() != expected_len
            || !self.plane_bounds.is_valid()
            || !self.distance_range_px.is_finite()
            || self.distance_range_px <= 0.0
        {
            return Err(AtlasError::InvalidGeneratedGlyph);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct AtlasSlot {
    pub(crate) page: u16,
    pub(crate) x: u32,
    pub(crate) y: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AtlasGlyph {
    pub(crate) key: GlyphKey,
    pub(crate) generation: u64,
    pub(crate) slot: AtlasSlot,
    pub(crate) uv_rect: [f32; 4],
    pub(crate) plane_bounds: GlyphPlaneBounds,
    pub(crate) distance_range_px: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AtlasReservation {
    key: GlyphKey,
    generation: u64,
    slot: AtlasSlot,
}

impl AtlasReservation {
    pub(crate) fn key(self) -> GlyphKey {
        self.key
    }

    pub(crate) fn slot(self) -> AtlasSlot {
        self.slot
    }
}

#[derive(Clone, PartialEq)]
pub(crate) struct AtlasUpload {
    pub(crate) glyph: AtlasGlyph,
    pub(crate) rgba: Arc<[u8]>,
    pub(crate) width: u32,
    pub(crate) height: u32,
}

impl fmt::Debug for AtlasUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AtlasUpload")
            .field("glyph", &self.glyph)
            .field("rgba_bytes", &self.rgba.len())
            .field("width", &self.width)
            .field("height", &self.height)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AtlasRequest {
    Ready(AtlasGlyph),
    Pending,
    Generate(AtlasReservation),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum AtlasError {
    #[error("atlas dimensions must be non-zero and contain at least one glyph slot")]
    InvalidDimensions,
    #[error("all atlas slots are pending or referenced by in-flight GPU work")]
    AllSlotsInFlight,
    #[error("the atlas reservation is stale")]
    StaleReservation,
    #[error("the generated glyph does not fit its atlas slot")]
    InvalidGeneratedGlyph,
}

#[derive(Debug, Clone)]
enum EntryState {
    Pending,
    Ready(AtlasGlyph),
}

#[derive(Debug, Clone)]
struct AtlasEntry {
    generation: u64,
    slot: AtlasSlot,
    last_used: u64,
    in_flight: u32,
    state: EntryState,
}

/// Tokens held until the GPU submission that references their glyphs retires.
/// Generation checks make late completion safe after a cache key is reused.
#[derive(Debug)]
#[must_use = "retire the ticket after the matching GPU submission completes"]
pub(crate) struct FrameAtlasTicket {
    references: Vec<(GlyphKey, u64)>,
}

/// Fixed-slot LRU metadata for one or more linear-RGB atlas pages.
///
/// Pending generation reservations and glyphs referenced by submitted frames
/// cannot be selected for eviction. The owner calls [`Self::retire_frame`]
/// after the matching GPU submission completes.
#[derive(Debug)]
pub(crate) struct GlyphAtlasCache {
    page_size: u32,
    slot_size: u32,
    entries: HashMap<GlyphKey, AtlasEntry>,
    free_slots: Vec<AtlasSlot>,
    tick: u64,
    next_generation: u64,
}

impl GlyphAtlasCache {
    pub(crate) fn new(
        page_size: u32,
        slot_size: u32,
        maximum_pages: u16,
    ) -> Result<Self, AtlasError> {
        const MAX_PAGE_SIZE: u32 = 16_384;
        const MAX_PAGES: u16 = 256;
        const MAX_SLOTS: usize = 1_048_576;
        if page_size == 0
            || page_size > MAX_PAGE_SIZE
            || slot_size < 8
            || maximum_pages == 0
            || maximum_pages > MAX_PAGES
        {
            return Err(AtlasError::InvalidDimensions);
        }
        let slots_per_axis = page_size / slot_size;
        if slots_per_axis == 0 {
            return Err(AtlasError::InvalidDimensions);
        }
        let page_slot_count = slots_per_axis
            .checked_mul(slots_per_axis)
            .ok_or(AtlasError::InvalidDimensions)?;
        let total_slots = usize::try_from(page_slot_count)
            .ok()
            .and_then(|count| count.checked_mul(usize::from(maximum_pages)))
            .ok_or(AtlasError::InvalidDimensions)?;
        if total_slots > MAX_SLOTS {
            return Err(AtlasError::InvalidDimensions);
        }
        let mut free_slots = Vec::with_capacity(total_slots);
        for page in (0..maximum_pages).rev() {
            for index in (0..page_slot_count).rev() {
                free_slots.push(AtlasSlot {
                    page,
                    x: (index % slots_per_axis) * slot_size,
                    y: (index / slots_per_axis) * slot_size,
                });
            }
        }
        Ok(Self {
            page_size,
            slot_size,
            entries: HashMap::with_capacity(total_slots),
            free_slots,
            tick: 0,
            next_generation: 1,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.entries.len() + self.free_slots.len()
    }

    pub(crate) fn request(&mut self, key: GlyphKey) -> Result<AtlasRequest, AtlasError> {
        self.tick = self.tick.wrapping_add(1);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.last_used = self.tick;
            return Ok(match &entry.state {
                EntryState::Pending => AtlasRequest::Pending,
                EntryState::Ready(glyph) => AtlasRequest::Ready(glyph.clone()),
            });
        }

        let slot = if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let eviction_key = self
                .entries
                .iter()
                .filter(|(_, entry)| {
                    entry.in_flight == 0 && matches!(&entry.state, EntryState::Ready(_))
                })
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| *key)
                .ok_or(AtlasError::AllSlotsInFlight)?;
            self.entries
                .remove(&eviction_key)
                .map(|entry| entry.slot)
                .ok_or(AtlasError::AllSlotsInFlight)?
        };

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        self.entries.insert(
            key,
            AtlasEntry {
                generation,
                slot,
                last_used: self.tick,
                in_flight: 0,
                state: EntryState::Pending,
            },
        );
        Ok(AtlasRequest::Generate(AtlasReservation {
            key,
            generation,
            slot,
        }))
    }

    pub(crate) fn commit(
        &mut self,
        reservation: AtlasReservation,
        generated: GeneratedGlyph,
    ) -> Result<AtlasUpload, AtlasError> {
        generated.validate(self.slot_size)?;
        let entry = self
            .entries
            .get_mut(&reservation.key)
            .filter(|entry| {
                entry.generation == reservation.generation
                    && entry.slot == reservation.slot
                    && matches!(&entry.state, EntryState::Pending)
            })
            .ok_or(AtlasError::StaleReservation)?;
        let page = self.page_size as f32;
        let glyph = AtlasGlyph {
            key: reservation.key,
            generation: reservation.generation,
            slot: reservation.slot,
            uv_rect: [
                reservation.slot.x as f32 / page,
                reservation.slot.y as f32 / page,
                (reservation.slot.x + generated.width) as f32 / page,
                (reservation.slot.y + generated.height) as f32 / page,
            ],
            plane_bounds: generated.plane_bounds,
            distance_range_px: generated.distance_range_px,
        };
        entry.state = EntryState::Ready(glyph.clone());
        Ok(AtlasUpload {
            glyph,
            rgba: generated.rgba,
            width: generated.width,
            height: generated.height,
        })
    }

    pub(crate) fn cancel(&mut self, reservation: AtlasReservation) -> Result<(), AtlasError> {
        let matches = self.entries.get(&reservation.key).is_some_and(|entry| {
            entry.generation == reservation.generation
                && entry.slot == reservation.slot
                && matches!(&entry.state, EntryState::Pending)
        });
        if !matches {
            return Err(AtlasError::StaleReservation);
        }
        if let Some(entry) = self.entries.remove(&reservation.key) {
            self.free_slots.push(entry.slot);
        }
        Ok(())
    }

    pub(crate) fn ready(&mut self, key: GlyphKey) -> Option<AtlasGlyph> {
        self.tick = self.tick.wrapping_add(1);
        let entry = self.entries.get_mut(&key)?;
        entry.last_used = self.tick;
        match &entry.state {
            EntryState::Ready(glyph) => Some(glyph.clone()),
            EntryState::Pending => None,
        }
    }

    pub(crate) fn begin_frame(
        &mut self,
        keys: impl IntoIterator<Item = GlyphKey>,
    ) -> FrameAtlasTicket {
        let mut unique = HashSet::new();
        let mut references = Vec::new();
        for key in keys {
            if !unique.insert(key) {
                continue;
            }
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            if matches!(&entry.state, EntryState::Ready(_)) {
                entry.in_flight = entry.in_flight.saturating_add(1);
                references.push((key, entry.generation));
            }
        }
        FrameAtlasTicket { references }
    }

    pub(crate) fn retire_frame(&mut self, ticket: FrameAtlasTicket) {
        for (key, generation) in ticket.references {
            if let Some(entry) = self
                .entries
                .get_mut(&key)
                .filter(|entry| entry.generation == generation)
            {
                entry.in_flight = entry.in_flight.saturating_sub(1);
            }
        }
    }
}

/// A frame-relative scrolling description. It is copied into each immutable
/// glyph instance and evaluated in the vertex shader from monotonic time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ScrollMotion {
    pub(crate) velocity: [f32; 2],
    pub(crate) cycle_distance: f32,
    pub(crate) repeat: bool,
    pub(crate) start_seconds: f32,
}

impl ScrollMotion {
    pub(crate) const NONE: Self = Self {
        velocity: [0.0, 0.0],
        cycle_distance: 0.0,
        repeat: false,
        start_seconds: 0.0,
    };

    pub(crate) fn from_scene(
        scroll: TextScroll,
        content_extent: [f32; 2],
        start_seconds: f32,
    ) -> Self {
        if !scroll.enabled || scroll.pixels_per_second <= 0.0 {
            return Self::NONE;
        }
        let (velocity, extent) = match scroll.direction {
            ScrollDirection::Left => ([-scroll.pixels_per_second, 0.0], content_extent[0]),
            ScrollDirection::Right => ([scroll.pixels_per_second, 0.0], content_extent[0]),
            ScrollDirection::Up => ([0.0, -scroll.pixels_per_second], content_extent[1]),
            ScrollDirection::Down => ([0.0, scroll.pixels_per_second], content_extent[1]),
        };
        Self {
            velocity,
            cycle_distance: (extent + scroll.gap).max(1.0),
            repeat: scroll.repeat,
            start_seconds: start_seconds.max(0.0),
        }
    }

    #[cfg(test)]
    fn offset_at(self, elapsed: Duration) -> [f32; 2] {
        let elapsed_seconds = (elapsed.as_secs_f64() as f32 - self.start_seconds).max(0.0);
        let speed = self.velocity[0].abs() + self.velocity[1].abs();
        if speed == 0.0 {
            return [0.0, 0.0];
        }
        let raw_distance = elapsed_seconds * speed;
        let distance = if self.repeat {
            raw_distance % self.cycle_distance.max(1.0)
        } else {
            raw_distance.min(self.cycle_distance.max(1.0))
        };
        self.velocity.map(|velocity| {
            if velocity == 0.0 {
                0.0
            } else {
                velocity.signum() * distance
            }
        })
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "gpu-wgpu", derive(bytemuck::Pod, bytemuck::Zeroable))]
pub(crate) struct MsdfFrameUniform {
    /// Width, height, monotonic seconds since renderer creation, reserved.
    pub(crate) viewport_time: [f32; 4],
}

#[derive(Debug, Clone)]
pub(crate) struct MonotonicFrameClock {
    origin: Instant,
}

impl MonotonicFrameClock {
    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    pub(crate) fn uniform(&self, width: u32, height: u32) -> MsdfFrameUniform {
        self.uniform_at(width, height, Instant::now())
    }

    fn uniform_at(&self, width: u32, height: u32, now: Instant) -> MsdfFrameUniform {
        MsdfFrameUniform {
            viewport_time: [
                width.max(1) as f32,
                height.max(1) as f32,
                now.saturating_duration_since(self.origin).as_secs_f64() as f32,
                0.0,
            ],
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "gpu-wgpu", derive(bytemuck::Pod, bytemuck::Zeroable))]
pub(crate) struct GpuGlyphInstance {
    pub(crate) rect: [f32; 4],
    pub(crate) uv_rect: [f32; 4],
    pub(crate) fill_color: [f32; 4],
    pub(crate) outline_color: [f32; 4],
    pub(crate) shadow_color: [f32; 4],
    pub(crate) clip_rect: [f32; 4],
    /// Distance range, outline width, shadow blur, pass kind (0 shadow, 1 main).
    pub(crate) style_flags: [f32; 4],
    /// Scroll velocity X/Y, scroll start seconds, atlas page.
    pub(crate) motion_page: [f32; 4],
    /// Scroll cycle distance, repeat flag, reserved, reserved.
    pub(crate) scroll_flags: [f32; 4],
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SceneTextDrawList {
    pub(crate) instances: Arc<[GpuGlyphInstance]>,
    pub(crate) shadow_range: Range<u32>,
    pub(crate) outline_fill_range: Range<u32>,
    pub(crate) atlas_keys: Arc<[GlyphKey]>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct TextPaint {
    fill: [f32; 4],
    outline: [f32; 4],
    outline_width: f32,
    shadow: [f32; 4],
    shadow_offset: [f32; 2],
    shadow_blur: f32,
}

impl TextPaint {
    pub(crate) fn from_scene(style: &TextStyle, node_opacity: f32) -> Self {
        let opacity = node_opacity.clamp(0.0, 1.0);
        let mut fill = color_to_linear(style.color);
        fill[3] *= opacity;
        let (mut outline, outline_width) = style
            .outline
            .map(|outline| (color_to_linear(outline.color), outline.width))
            .unwrap_or(([0.0; 4], 0.0));
        outline[3] *= opacity;
        let (mut shadow, shadow_offset, shadow_blur) = style
            .shadow
            .map(|shadow| {
                (
                    color_to_linear(shadow.color),
                    [shadow.x, shadow.y],
                    shadow.blur,
                )
            })
            .unwrap_or(([0.0; 4], [0.0; 2], 0.0));
        shadow[3] *= opacity;
        Self {
            fill,
            outline,
            outline_width,
            shadow,
            shadow_offset,
            shadow_blur,
        }
    }
}

#[derive(Clone)]
pub(crate) struct FontFaceBlob {
    pub(crate) fingerprint: u64,
    pub(crate) face_index: u32,
    pub(crate) data: Arc<[u8]>,
}

impl fmt::Debug for FontFaceBlob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FontFaceBlob")
            .field("fingerprint", &format_args!("{:016x}", self.fingerprint))
            .field("face_index", &self.face_index)
            .field("bytes", &self.data.len())
            .finish()
    }
}

impl fmt::Display for FontFaceBlob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "font {:016x} face {} ({} bytes)",
            self.fingerprint,
            self.face_index,
            self.data.len()
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ShapedGlyph {
    pub(crate) key: GlyphKey,
    pub(crate) font: Arc<FontFaceBlob>,
    pub(crate) origin: [f32; 2],
    pub(crate) font_size: f32,
}

#[derive(Debug, Clone)]
pub(crate) struct ShapedText {
    pub(crate) glyphs: Arc<[ShapedGlyph]>,
    pub(crate) content_extent: [f32; 2],
    pub(crate) missing_glyphs: usize,
}

impl ShapedText {
    pub(crate) fn raster_requests(&self) -> Vec<GlyphRasterRequest> {
        let mut seen = HashSet::new();
        self.glyphs
            .iter()
            .filter(|glyph| seen.insert(glyph.key))
            .map(|glyph| GlyphRasterRequest {
                key: glyph.key,
                font: Arc::clone(&glyph.font),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GlyphRasterRequest {
    pub(crate) key: GlyphKey,
    pub(crate) font: Arc<FontFaceBlob>,
}

/// Builds immutable GPU instances. Shadow instances are deliberately first so
/// one upload can be drawn as two ordered ranges without re-sorting each frame.
pub(crate) fn build_draw_list(
    shaped: &ShapedText,
    atlas: &mut GlyphAtlasCache,
    paint: TextPaint,
    clip_rect: [f32; 4],
    scroll: ScrollMotion,
) -> SceneTextDrawList {
    let mut shadows = Vec::new();
    let mut mains = Vec::new();
    let mut atlas_keys = Vec::new();
    for shaped_glyph in shaped.glyphs.iter() {
        let Some(atlas_glyph) = atlas.ready(shaped_glyph.key) else {
            continue;
        };
        let bounds = atlas_glyph.plane_bounds;
        let rect = [
            shaped_glyph.origin[0] + bounds.left * shaped_glyph.font_size,
            shaped_glyph.origin[1] - bounds.top * shaped_glyph.font_size,
            (bounds.right - bounds.left) * shaped_glyph.font_size,
            (bounds.top - bounds.bottom) * shaped_glyph.font_size,
        ];
        let shared = |rect: [f32; 4], pass_kind: f32| GpuGlyphInstance {
            rect,
            uv_rect: atlas_glyph.uv_rect,
            fill_color: paint.fill,
            outline_color: paint.outline,
            shadow_color: paint.shadow,
            clip_rect,
            style_flags: [
                atlas_glyph.distance_range_px,
                paint.outline_width,
                paint.shadow_blur,
                pass_kind,
            ],
            motion_page: [
                scroll.velocity[0],
                scroll.velocity[1],
                scroll.start_seconds,
                f32::from(atlas_glyph.slot.page),
            ],
            scroll_flags: [
                scroll.cycle_distance,
                if scroll.repeat { 1.0 } else { 0.0 },
                0.0,
                0.0,
            ],
        };
        if paint.shadow[3] > 0.0 {
            shadows.push(shared(
                [
                    rect[0] + paint.shadow_offset[0],
                    rect[1] + paint.shadow_offset[1],
                    rect[2],
                    rect[3],
                ],
                0.0,
            ));
        }
        mains.push(shared(rect, 1.0));
        atlas_keys.push(shaped_glyph.key);
    }
    let shadow_count = u32::try_from(shadows.len()).unwrap_or(u32::MAX);
    shadows.extend(mains);
    let total = u32::try_from(shadows.len()).unwrap_or(u32::MAX);
    SceneTextDrawList {
        instances: Arc::from(shadows),
        shadow_range: 0..shadow_count,
        outline_fill_range: shadow_count..total,
        atlas_keys: Arc::from(atlas_keys),
    }
}

fn color_to_linear(color: RgbaColor) -> [f32; 4] {
    let convert = |channel: u8| {
        let value = f32::from(channel) / 255.0;
        if value <= 0.040_45 {
            value / 12.92
        } else {
            ((value + 0.055) / 1.055).powf(2.4)
        }
    };
    [
        convert(color.red),
        convert(color.green),
        convert(color.blue),
        f32::from(color.alpha) / 255.0,
    ]
}

fn font_fingerprint(data: &[u8], face_index: u32) -> u64 {
    // FNV-1a is deterministic and only identifies an in-process cache entry.
    // It is not used as an integrity or security hash.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in face_index
        .to_le_bytes()
        .into_iter()
        .chain(data.iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// WGSL contract for the additive scene text path. Instances are submitted in
/// two ranges: all shadows first, then the combined outline and fill pass.
pub(crate) const MSDF_TEXT_SHADER: &str = r#"
struct FrameUniform {
    viewport_time: vec4<f32>,
};

struct GlyphInstance {
    rect: vec4<f32>,
    uv_rect: vec4<f32>,
    fill_color: vec4<f32>,
    outline_color: vec4<f32>,
    shadow_color: vec4<f32>,
    clip_rect: vec4<f32>,
    style_flags: vec4<f32>,
    motion_page: vec4<f32>,
    scroll_flags: vec4<f32>,
};

@group(0) @binding(0) var<uniform> frame: FrameUniform;
@group(0) @binding(1) var<storage, read> glyphs: array<GlyphInstance>;
@group(0) @binding(2) var atlas_texture: texture_2d_array<f32>;
@group(0) @binding(3) var atlas_sampler: sampler;

struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
    @location(1) @interpolate(flat) page: i32,
    @location(2) fill_color: vec4<f32>,
    @location(3) outline_color: vec4<f32>,
    @location(4) shadow_color: vec4<f32>,
    @location(5) clip_rect: vec4<f32>,
    @location(6) style_flags: vec4<f32>,
};

fn scroll_offset(glyph: GlyphInstance) -> vec2<f32> {
    let velocity = glyph.motion_page.xy;
    let speed = abs(velocity.x) + abs(velocity.y);
    if speed <= 0.0 {
        return vec2<f32>(0.0);
    }
    let elapsed = max(frame.viewport_time.z - glyph.motion_page.z, 0.0);
    let cycle = max(glyph.scroll_flags.x, 1.0);
    let raw_distance = elapsed * speed;
    var distance = min(raw_distance, cycle);
    if glyph.scroll_flags.y >= 0.5 {
        distance = raw_distance - floor(raw_distance / cycle) * cycle;
    }
    return sign(velocity) * distance;
}

@vertex
fn vertex_main(
    @builtin(vertex_index) vertex_index: u32,
    @builtin(instance_index) instance_index: u32,
) -> VertexOutput {
    let corners = array<vec2<f32>, 6>(
        vec2<f32>(0.0, 0.0), vec2<f32>(1.0, 0.0), vec2<f32>(0.0, 1.0),
        vec2<f32>(0.0, 1.0), vec2<f32>(1.0, 0.0), vec2<f32>(1.0, 1.0),
    );
    let glyph = glyphs[instance_index];
    let corner = corners[vertex_index];
    let pixel = glyph.rect.xy + corner * glyph.rect.zw + scroll_offset(glyph);
    let clip = vec2<f32>(
        pixel.x / frame.viewport_time.x * 2.0 - 1.0,
        1.0 - pixel.y / frame.viewport_time.y * 2.0,
    );
    var output: VertexOutput;
    output.position = vec4<f32>(clip, 0.0, 1.0);
    output.uv = mix(glyph.uv_rect.xy, glyph.uv_rect.zw, corner);
    output.page = i32(glyph.motion_page.w);
    output.fill_color = glyph.fill_color;
    output.outline_color = glyph.outline_color;
    output.shadow_color = glyph.shadow_color;
    output.clip_rect = glyph.clip_rect;
    output.style_flags = glyph.style_flags;
    return output;
}

fn median3(value: vec3<f32>) -> f32 {
    return max(min(value.r, value.g), min(max(value.r, value.g), value.b));
}

@fragment
fn fragment_main(input: VertexOutput) -> @location(0) vec4<f32> {
    let pixel = input.position.xy;
    let sample_rgb = textureSample(atlas_texture, atlas_sampler, input.uv, input.page).rgb;
    let signed_distance = median3(sample_rgb) - 0.5;
    let atlas_size = vec2<f32>(textureDimensions(atlas_texture, 0));
    let unit_range = vec2<f32>(max(input.style_flags.x, 0.00001)) / atlas_size;
    let screen_tex_size = vec2<f32>(1.0) / max(fwidth(input.uv), vec2<f32>(0.00001));
    let screen_px_range = max(0.5 * dot(unit_range, screen_tex_size), 1.0);
    let screen_distance = signed_distance * screen_px_range;
    let clip_max = input.clip_rect.xy + input.clip_rect.zw;
    if pixel.x < input.clip_rect.x || pixel.y < input.clip_rect.y ||
       pixel.x >= clip_max.x || pixel.y >= clip_max.y {
        discard;
    }
    let fill_alpha = smoothstep(-0.5, 0.5, screen_distance);
    let outline_alpha = smoothstep(
        -0.5,
        0.5,
        screen_distance + max(input.style_flags.y, 0.0),
    );

    var color: vec4<f32>;
    if input.style_flags.w < 0.5 {
        let blur = max(input.style_flags.z, 0.0);
        let shadow_alpha = smoothstep(-0.5 - blur, 0.5 + blur, screen_distance);
        color = vec4<f32>(input.shadow_color.rgb, input.shadow_color.a * shadow_alpha);
    } else {
        let outline_mix = max(outline_alpha - fill_alpha, 0.0);
        let alpha = input.fill_color.a * fill_alpha + input.outline_color.a * outline_mix;
        let rgb = input.fill_color.rgb * input.fill_color.a * fill_alpha +
                  input.outline_color.rgb * input.outline_color.a * outline_mix;
        color = vec4<f32>(rgb / max(alpha, 0.00001), alpha);
    }
    // The render target uses premultiplied-alpha blending in linear space.
    return vec4<f32>(color.rgb * color.a, color.a);
}
"#;

#[cfg(feature = "gpu-wgpu")]
pub(crate) fn atlas_wgpu_texture_format() -> wgpu::TextureFormat {
    wgpu::TextureFormat::Rgba8Unorm
}

#[cfg(feature = "gpu-wgpu")]
pub(crate) fn create_msdf_atlas_texture(
    device: &wgpu::Device,
    page_size: u32,
    page_count: u16,
) -> wgpu::Texture {
    device.create_texture(&wgpu::TextureDescriptor {
        label: Some("haze-cgen-msdf-linear-atlas"),
        size: wgpu::Extent3d {
            width: page_size.max(1),
            height: page_size.max(1),
            depth_or_array_layers: u32::from(page_count.max(1)),
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: atlas_wgpu_texture_format(),
        usage: wgpu::TextureUsages::COPY_DST | wgpu::TextureUsages::TEXTURE_BINDING,
        view_formats: &[],
    })
}

/// Uploads one generated tile directly to its array layer. Queue writes do not
/// require the 256-byte row padding used by staging-buffer copies.
#[cfg(feature = "gpu-wgpu")]
pub(crate) fn upload_msdf_atlas_tile(
    queue: &wgpu::Queue,
    texture: &wgpu::Texture,
    upload: &AtlasUpload,
) {
    queue.write_texture(
        wgpu::TexelCopyTextureInfo {
            texture,
            mip_level: 0,
            origin: wgpu::Origin3d {
                x: upload.glyph.slot.x,
                y: upload.glyph.slot.y,
                z: u32::from(upload.glyph.slot.page),
            },
            aspect: wgpu::TextureAspect::All,
        },
        upload.rgba.as_ref(),
        wgpu::TexelCopyBufferLayout {
            offset: 0,
            bytes_per_row: Some(upload.width.saturating_mul(4)),
            rows_per_image: Some(upload.height),
        },
        wgpu::Extent3d {
            width: upload.width,
            height: upload.height,
            depth_or_array_layers: 1,
        },
    );
}

#[cfg(feature = "gpu-wgpu")]
pub(crate) fn premultiplied_blend_state() -> wgpu::BlendState {
    let component = wgpu::BlendComponent {
        src_factor: wgpu::BlendFactor::One,
        dst_factor: wgpu::BlendFactor::OneMinusSrcAlpha,
        operation: wgpu::BlendOperation::Add,
    };
    wgpu::BlendState {
        color: component,
        alpha: component,
    }
}

/// Records the two ordered instance ranges after the caller binds the MSDF
/// pipeline, frame uniform, instance storage, and linear atlas resources.
#[cfg(feature = "gpu-wgpu")]
pub(crate) fn record_msdf_draws(
    render_pass: &mut wgpu::RenderPass<'_>,
    draw_list: &SceneTextDrawList,
) {
    if !draw_list.shadow_range.is_empty() {
        render_pass.draw(0..6, draw_list.shadow_range.clone());
    }
    if !draw_list.outline_fill_range.is_empty() {
        render_pass.draw(0..6, draw_list.outline_fill_range.clone());
    }
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Clone)]
pub(crate) struct TextShapeRequest {
    pub(crate) text: String,
    pub(crate) bounds: crate::scene::ResolvedRect,
    pub(crate) font_family: String,
    pub(crate) weight: u16,
    pub(crate) font_size: f32,
    pub(crate) horizontal: crate::scene::HorizontalAlignment,
    pub(crate) vertical: crate::scene::VerticalAlignment,
    pub(crate) auto_wrap: bool,
}

#[cfg(feature = "gpu-wgpu")]
impl TextShapeRequest {
    pub(crate) fn from_scene(
        node: &crate::scene::TextNode,
        text: impl Into<String>,
        bounds: crate::scene::ResolvedRect,
    ) -> Self {
        Self {
            text: text.into(),
            bounds,
            font_family: node.style.font_family.clone(),
            weight: node.style.weight,
            font_size: node.style.size,
            horizontal: node.layout.horizontal,
            vertical: node.layout.vertical,
            auto_wrap: node.layout.auto_wrap,
        }
    }
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Error)]
pub(crate) enum TextShapeError {
    #[error("text bounds and font metrics must be finite and positive")]
    InvalidMetrics,
    #[error("cosmic-text selected a font that is no longer available")]
    MissingFont,
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Clone, Copy)]
struct RawShapedGlyph {
    font_id: cosmic_text::fontdb::ID,
    glyph_id: u16,
    weight: u16,
    origin: [f32; 2],
    font_size: f32,
}

/// Uses cosmic-text for selection, fallback, shaping, wrapping, justification,
/// and glyph positioning. It deliberately does not use cosmic-text's rasterizer.
#[cfg(feature = "gpu-wgpu")]
pub(crate) struct MsdfTextShaper {
    font_system: cosmic_text::FontSystem,
    font_blobs: HashMap<cosmic_text::fontdb::ID, Arc<FontFaceBlob>>,
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Clone)]
pub(crate) struct PreparedSceneText {
    pub(crate) shaped: ShapedText,
    paint: TextPaint,
    clip_rect: [f32; 4],
    scroll: ScrollMotion,
}

#[cfg(feature = "gpu-wgpu")]
impl PreparedSceneText {
    pub(crate) fn draw_list(&self, atlas: &mut GlyphAtlasCache) -> SceneTextDrawList {
        build_draw_list(&self.shaped, atlas, self.paint, self.clip_rect, self.scroll)
    }
}

#[cfg(feature = "gpu-wgpu")]
impl fmt::Debug for MsdfTextShaper {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MsdfTextShaper")
            .field("cached_font_blobs", &self.font_blobs.len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "gpu-wgpu")]
impl MsdfTextShaper {
    pub(crate) fn new(managed_font_directory: &std::path::Path) -> Self {
        let mut font_system = cosmic_text::FontSystem::new();
        load_managed_fonts_bounded(&mut font_system, managed_font_directory);
        Self {
            font_system,
            font_blobs: HashMap::new(),
        }
    }

    pub(crate) fn shape(
        &mut self,
        request: &TextShapeRequest,
    ) -> Result<ShapedText, TextShapeError> {
        let bounds = request.bounds;
        if [
            bounds.x,
            bounds.y,
            bounds.width,
            bounds.height,
            request.font_size,
        ]
        .into_iter()
        .all(f32::is_finite)
            && bounds.width >= 0.0
            && bounds.height >= 0.0
            && request.font_size > 0.0
        {
            // Validated below.
        } else {
            return Err(TextShapeError::InvalidMetrics);
        }

        let metrics = cosmic_text::Metrics::relative(request.font_size, 1.2);
        let mut buffer = cosmic_text::Buffer::new(&mut self.font_system, metrics);
        buffer.set_size(
            &mut self.font_system,
            Some(bounds.width),
            Some(bounds.height),
        );
        buffer.set_wrap(
            &mut self.font_system,
            if request.auto_wrap {
                cosmic_text::Wrap::WordOrGlyph
            } else {
                cosmic_text::Wrap::None
            },
        );
        let family = if request.font_family.trim().is_empty() {
            cosmic_text::Family::SansSerif
        } else {
            cosmic_text::Family::Name(request.font_family.trim())
        };
        let attrs = cosmic_text::Attrs::new()
            .family(family)
            .weight(cosmic_text::Weight(request.weight));
        let alignment = Some(match request.horizontal {
            crate::scene::HorizontalAlignment::Left => cosmic_text::Align::Left,
            crate::scene::HorizontalAlignment::Right => cosmic_text::Align::Right,
            crate::scene::HorizontalAlignment::Center => cosmic_text::Align::Center,
            crate::scene::HorizontalAlignment::Justify => cosmic_text::Align::Justified,
        });
        buffer.set_text(
            &mut self.font_system,
            &request.text,
            &attrs,
            cosmic_text::Shaping::Advanced,
            alignment,
        );
        buffer.shape_until_scroll(&mut self.font_system, true);

        let mut content_width = 0.0_f32;
        let mut content_height = 0.0_f32;
        let mut raw = Vec::new();
        let mut missing_glyphs = 0_usize;
        for run in buffer.layout_runs() {
            content_width = content_width.max(run.line_w);
            content_height = content_height.max(run.line_top + run.line_height);
            for glyph in run.glyphs {
                if glyph.glyph_id == 0 {
                    missing_glyphs += 1;
                    continue;
                }
                raw.push(RawShapedGlyph {
                    font_id: glyph.font_id,
                    glyph_id: glyph.glyph_id,
                    weight: glyph.font_weight.0,
                    origin: [
                        bounds.x + glyph.x + glyph.font_size * glyph.x_offset,
                        bounds.y + run.line_y + glyph.y - glyph.font_size * glyph.y_offset,
                    ],
                    font_size: glyph.font_size,
                });
            }
        }
        drop(buffer);

        let vertical_offset = match request.vertical {
            crate::scene::VerticalAlignment::Top => 0.0,
            crate::scene::VerticalAlignment::Center => {
                ((bounds.height - content_height) * 0.5).max(0.0)
            }
            crate::scene::VerticalAlignment::Bottom => (bounds.height - content_height).max(0.0),
        };

        let mut glyphs = Vec::with_capacity(raw.len());
        for raw_glyph in raw {
            let font = self.font_blob(raw_glyph.font_id, raw_glyph.weight)?;
            glyphs.push(ShapedGlyph {
                key: GlyphKey {
                    font_fingerprint: font.fingerprint,
                    face_index: font.face_index,
                    glyph_id: raw_glyph.glyph_id,
                    weight: raw_glyph.weight,
                },
                font,
                origin: [raw_glyph.origin[0], raw_glyph.origin[1] + vertical_offset],
                font_size: raw_glyph.font_size,
            });
        }
        Ok(ShapedText {
            glyphs: Arc::from(glyphs),
            content_extent: [content_width, content_height],
            missing_glyphs,
        })
    }

    /// Converts a text node from the hierarchy resolver into immutable shaping
    /// and paint data. Callers supply already-bound text and the monotonic scene
    /// activation time, so frame rendering never reaches back into broker state.
    pub(crate) fn prepare_resolved_node(
        &mut self,
        node: &crate::scene_layout::ResolvedSceneNode<'_>,
        bound_text: impl Into<String>,
        scroll_start_seconds: f32,
    ) -> Result<Option<PreparedSceneText>, TextShapeError> {
        let crate::scene::NodeKind::Text(text_node) = node.kind else {
            return Ok(None);
        };
        let rect = crate::scene::ResolvedRect {
            x: node.rect.x,
            y: node.rect.y,
            width: node.rect.width,
            height: node.rect.height,
        };
        let shaped = self.shape(&TextShapeRequest::from_scene(text_node, bound_text, rect))?;
        let inherited_clip = node.clip.unwrap_or(crate::scene_layout::PixelRect {
            x: 0.0,
            y: 0.0,
            width: f32::MAX / 4.0,
            height: f32::MAX / 4.0,
        });
        let clip = if text_node.layout.clip {
            intersect_pixel_rect(inherited_clip, node.rect).unwrap_or(
                crate::scene_layout::PixelRect {
                    x: node.rect.x,
                    y: node.rect.y,
                    width: 0.0,
                    height: 0.0,
                },
            )
        } else {
            inherited_clip
        };
        let scroll = ScrollMotion::from_scene(
            text_node.scroll,
            shaped.content_extent,
            scroll_start_seconds,
        );
        Ok(Some(PreparedSceneText {
            shaped,
            paint: TextPaint::from_scene(&text_node.style, node.opacity),
            clip_rect: [clip.x, clip.y, clip.width, clip.height],
            scroll,
        }))
    }

    fn font_blob(
        &mut self,
        id: cosmic_text::fontdb::ID,
        weight: u16,
    ) -> Result<Arc<FontFaceBlob>, TextShapeError> {
        if let Some(font) = self.font_blobs.get(&id) {
            return Ok(Arc::clone(font));
        }
        let face_index = self
            .font_system
            .db()
            .face(id)
            .map(|face| face.index)
            .ok_or(TextShapeError::MissingFont)?;
        let font = self
            .font_system
            .get_font(id, cosmic_text::Weight(weight))
            .ok_or(TextShapeError::MissingFont)?;
        let data = Arc::<[u8]>::from(font.data());
        let blob = Arc::new(FontFaceBlob {
            fingerprint: font_fingerprint(&data, face_index),
            face_index,
            data,
        });
        self.font_blobs.insert(id, Arc::clone(&blob));
        Ok(blob)
    }
}

/// The compatibility release prepares active alert scenes on a bounded worker
/// without replacing glyphon's live draw path. Keeping the prepared nodes here
/// makes the shadow result directly reusable when the MSDF render pipeline is
/// enabled in a later release.
#[cfg(feature = "gpu-wgpu")]
#[derive(Debug)]
struct ShadowPreparedScene {
    scene_id: String,
    resolved_node_count: usize,
    prepared_text: Arc<[PreparedSceneText]>,
    glyph_count: usize,
    missing_glyph_count: usize,
    unique_raster_request_count: usize,
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug)]
struct ShadowSceneRequest {
    key: u64,
    kind: crate::scene::ProtectedSceneKind,
    width: u32,
    height: u32,
    bound_text: String,
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug)]
struct ShadowSceneResult {
    key: u64,
    result: Result<Arc<ShadowPreparedScene>, String>,
}

/// Nonblocking bridge from the live frame callback into scene text shaping.
/// There can be at most one outstanding request and one completed result. A
/// scene change while work is in flight is coalesced and retried on the next
/// frame after the old result is collected.
#[cfg(feature = "gpu-wgpu")]
pub(crate) struct SceneTextShadowPreview {
    request_tx: Option<std::sync::mpsc::SyncSender<ShadowSceneRequest>>,
    result_rx: std::sync::mpsc::Receiver<ShadowSceneResult>,
    worker_available: bool,
    active_key: Option<u64>,
    pending_key: Option<u64>,
    last_finished_key: Option<u64>,
    latest_key: Option<u64>,
    latest: Option<Arc<ShadowPreparedScene>>,
    submitted_requests: u64,
    completed_requests: u64,
    coalesced_requests: u64,
    full_queue_drops: u64,
    last_error: Option<String>,
}

#[cfg(feature = "gpu-wgpu")]
impl fmt::Debug for SceneTextShadowPreview {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SceneTextShadowPreview")
            .field("worker_available", &self.worker_available)
            .field("active_key", &self.active_key)
            .field("pending_key", &self.pending_key)
            .field("submitted_requests", &self.submitted_requests)
            .field("completed_requests", &self.completed_requests)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "gpu-wgpu")]
impl SceneTextShadowPreview {
    const QUEUE_CAPACITY: usize = 1;
    const MAX_BOUND_TEXT_CHARS: usize = 4_096;

    pub(crate) fn spawn(managed_font_directory: &std::path::Path) -> Self {
        let (request_tx, request_rx) =
            std::sync::mpsc::sync_channel::<ShadowSceneRequest>(Self::QUEUE_CAPACITY);
        let (result_tx, result_rx) =
            std::sync::mpsc::sync_channel::<ShadowSceneResult>(Self::QUEUE_CAPACITY);
        let font_directory = managed_font_directory.to_path_buf();
        let spawned = std::thread::Builder::new()
            .name("haze-cgen-msdf-shadow".to_string())
            .spawn(move || {
                let mut shaper = MsdfTextShaper::new(&font_directory);
                while let Ok(request) = request_rx.recv() {
                    let key = request.key;
                    let result = prepare_shadow_scene(&mut shaper, request)
                        .map(Arc::new)
                        .map_err(|error| error.to_string());
                    if result_tx.send(ShadowSceneResult { key, result }).is_err() {
                        break;
                    }
                }
            });
        let (request_tx, worker_available, last_error) = match spawned {
            Ok(_worker) => (Some(request_tx), true, None),
            Err(error) => (
                None,
                false,
                Some(format!("MSDF shadow worker could not start: {error}")),
            ),
        };
        Self {
            request_tx,
            result_rx,
            worker_available,
            active_key: None,
            pending_key: None,
            last_finished_key: None,
            latest_key: None,
            latest: None,
            submitted_requests: 0,
            completed_requests: 0,
            coalesced_requests: 0,
            full_queue_drops: 0,
            last_error,
        }
    }

    pub(crate) fn observe_alert_scene(
        &mut self,
        kind: crate::scene::ProtectedSceneKind,
        width: u32,
        height: u32,
        visual_id: &str,
        bound_text: &str,
    ) {
        self.poll_results();
        let bounded_text: String = bound_text
            .chars()
            .take(Self::MAX_BOUND_TEXT_CHARS)
            .collect();
        let key = shadow_request_key(kind, width, height, visual_id, &bounded_text);
        self.active_key = Some(key);
        if self.pending_key == Some(key) || self.last_finished_key == Some(key) {
            return;
        }
        if self.pending_key.is_some() {
            self.coalesced_requests = self.coalesced_requests.saturating_add(1);
            return;
        }
        let Some(request_tx) = self.request_tx.clone() else {
            self.worker_available = false;
            return;
        };
        let request = ShadowSceneRequest {
            key,
            kind,
            width: width.max(1),
            height: height.max(1),
            bound_text: bounded_text,
        };
        match request_tx.try_send(request) {
            Ok(()) => {
                self.pending_key = Some(key);
                self.submitted_requests = self.submitted_requests.saturating_add(1);
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                self.full_queue_drops = self.full_queue_drops.saturating_add(1);
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                self.worker_available = false;
                self.request_tx = None;
                self.last_error = Some("MSDF shadow worker stopped".to_string());
            }
        }
    }

    pub(crate) fn set_inactive(&mut self) {
        self.poll_results();
        self.active_key = None;
    }

    pub(crate) fn poll_results(&mut self) {
        loop {
            match self.result_rx.try_recv() {
                Ok(result) => {
                    if self.pending_key == Some(result.key) {
                        self.pending_key = None;
                    }
                    self.completed_requests = self.completed_requests.saturating_add(1);
                    self.last_finished_key = Some(result.key);
                    match result.result {
                        Ok(prepared) => {
                            self.latest_key = Some(result.key);
                            self.latest = Some(prepared);
                            self.last_error = None;
                        }
                        Err(error) => {
                            self.last_error = Some(error);
                        }
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.worker_available = false;
                    self.request_tx = None;
                    if self.pending_key.take().is_some() && self.last_error.is_none() {
                        self.last_error = Some("MSDF shadow worker stopped".to_string());
                    }
                    break;
                }
            }
        }
    }

    pub(crate) fn status_value(&self) -> serde_json::Value {
        let latest_matches_active = self.active_key.is_some() && self.latest_key == self.active_key;
        serde_json::json!({
            "mode": "prepare_only",
            "binding_source": "legacy_overlay_projection",
            "worker_available": self.worker_available,
            "queue_capacity": Self::QUEUE_CAPACITY,
            "active_request": self.active_key.is_some(),
            "pending": self.pending_key.is_some(),
            "latest_matches_active": latest_matches_active,
            "submitted_requests": self.submitted_requests,
            "completed_requests": self.completed_requests,
            "coalesced_requests": self.coalesced_requests,
            "full_queue_drops": self.full_queue_drops,
            "prepared_scene_id": self.latest.as_ref().map(|scene| scene.scene_id.as_str()),
            "resolved_node_count": self.latest.as_ref().map(|scene| scene.resolved_node_count),
            "prepared_text_node_count": self.latest.as_ref().map(|scene| scene.prepared_text.len()),
            "glyph_count": self.latest.as_ref().map(|scene| scene.glyph_count),
            "missing_glyph_count": self.latest.as_ref().map(|scene| scene.missing_glyph_count),
            "unique_raster_request_count": self.latest.as_ref().map(|scene| scene.unique_raster_request_count),
            "last_error": self.last_error.as_deref(),
        })
    }
}

#[cfg(feature = "gpu-wgpu")]
fn prepare_shadow_scene(
    shaper: &mut MsdfTextShaper,
    request: ShadowSceneRequest,
) -> Result<ShadowPreparedScene, TextShapeError> {
    let scene = crate::scene::protected_default_scene(request.kind);
    let layout =
        crate::scene_layout::ResolvedSceneLayout::resolve(&scene, request.width, request.height)
            .map_err(|_| TextShapeError::InvalidMetrics)?;
    let mut prepared_text = Vec::new();
    let mut glyph_count = 0_usize;
    let mut missing_glyph_count = 0_usize;
    let mut raster_keys = HashSet::new();
    for node in layout.drawable_nodes() {
        let crate::scene::NodeKind::Text(text_node) = node.kind else {
            continue;
        };
        let text = match &text_node.source {
            crate::scene::TextSource::Static(text) => text.as_str(),
            crate::scene::TextSource::Binding(_) => request.bound_text.as_str(),
        };
        let Some(prepared) = shaper.prepare_resolved_node(node, text, 0.0)? else {
            continue;
        };
        glyph_count = glyph_count.saturating_add(prepared.shaped.glyphs.len());
        missing_glyph_count = missing_glyph_count.saturating_add(prepared.shaped.missing_glyphs);
        raster_keys.extend(
            prepared
                .shaped
                .raster_requests()
                .into_iter()
                .map(|glyph| glyph.key),
        );
        prepared_text.push(prepared);
    }
    Ok(ShadowPreparedScene {
        scene_id: scene.id.as_str().to_string(),
        resolved_node_count: layout.nodes.len(),
        prepared_text: Arc::from(prepared_text),
        glyph_count,
        missing_glyph_count,
        unique_raster_request_count: raster_keys.len(),
    })
}

#[cfg(feature = "gpu-wgpu")]
fn shadow_request_key(
    kind: crate::scene::ProtectedSceneKind,
    width: u32,
    height: u32,
    visual_id: &str,
    bound_text: &str,
) -> u64 {
    let kind_byte = match kind {
        crate::scene::ProtectedSceneKind::ProgramPassthrough => 0_u8,
        crate::scene::ProtectedSceneKind::StandardCrawl => 1,
        crate::scene::ProtectedSceneKind::FullscreenTakeover => 2,
        crate::scene::ProtectedSceneKind::Standby => 3,
    };
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in [kind_byte]
        .into_iter()
        .chain(width.to_le_bytes())
        .chain(height.to_le_bytes())
        .chain(visual_id.as_bytes().iter().copied())
        .chain([0])
        .chain(bound_text.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(feature = "gpu-wgpu")]
fn intersect_pixel_rect(
    first: crate::scene_layout::PixelRect,
    second: crate::scene_layout::PixelRect,
) -> Option<crate::scene_layout::PixelRect> {
    let left = first.x.max(second.x);
    let top = first.y.max(second.y);
    let right = (first.x + first.width).min(second.x + second.width);
    let bottom = (first.y + first.height).min(second.y + second.height);
    (right > left && bottom > top).then_some(crate::scene_layout::PixelRect {
        x: left,
        y: top,
        width: right - left,
        height: bottom - top,
    })
}

#[cfg(feature = "gpu-wgpu")]
fn load_managed_fonts_bounded(
    font_system: &mut cosmic_text::FontSystem,
    managed_font_directory: &std::path::Path,
) {
    const MAX_MANAGED_FONTS: usize = 256;
    const MAX_FONT_BYTES: u64 = 64 * 1024 * 1024;
    let mut pending = vec![managed_font_directory.to_path_buf()];
    let mut loaded = 0_usize;
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            if loaded >= MAX_MANAGED_FONTS {
                return;
            }
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            let path = entry.path();
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }
            let supported = path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| {
                    matches!(
                        extension.to_ascii_lowercase().as_str(),
                        "ttf" | "ttc" | "otf" | "otc"
                    )
                });
            let within_limit = entry
                .metadata()
                .is_ok_and(|metadata| metadata.is_file() && metadata.len() <= MAX_FONT_BYTES);
            if supported && within_limit && font_system.db_mut().load_font_file(&path).is_ok() {
                loaded += 1;
            }
        }
    }
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GlyphRasterConfig {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) distance_range_px: f32,
}

#[cfg(feature = "gpu-wgpu")]
impl Default for GlyphRasterConfig {
    fn default() -> Self {
        Self {
            width: DEFAULT_GLYPH_SIZE,
            height: DEFAULT_GLYPH_SIZE,
            distance_range_px: DEFAULT_DISTANCE_RANGE_PX,
        }
    }
}

#[cfg(feature = "gpu-wgpu")]
impl GlyphRasterConfig {
    fn validate(self) -> Result<Self, GlyphRasterError> {
        if self.width == 0
            || self.height == 0
            || self.width > 512
            || self.height > 512
            || !self.distance_range_px.is_finite()
            || self.distance_range_px <= 0.0
            || self.distance_range_px * 2.0 >= self.width.min(self.height) as f32
        {
            return Err(GlyphRasterError::InvalidConfiguration);
        }
        Ok(self)
    }
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum GlyphRasterError {
    #[error("the glyph raster configuration is invalid")]
    InvalidConfiguration,
    #[error("the font face could not be parsed")]
    InvalidFont,
    #[error("the glyph has no vector outline")]
    MissingOutline,
    #[error("msdfgen rejected the glyph outline")]
    InvalidShape,
    #[error("the glyph cannot be framed in the configured atlas tile")]
    CannotFrame,
    #[error("the glyph worker panicked")]
    WorkerPanicked,
}

#[cfg(feature = "gpu-wgpu")]
pub(crate) fn rasterize_glyph(
    request: &GlyphRasterRequest,
    config: GlyphRasterConfig,
) -> Result<GeneratedGlyph, GlyphRasterError> {
    use msdfgen::{Bitmap, FillRule, FontExt, MsdfGeneratorConfig, Range, Rgb};

    let config = config.validate()?;
    let mut face = ttf_parser::Face::parse(&request.font.data, request.font.face_index)
        .map_err(|_| GlyphRasterError::InvalidFont)?;
    let _ = face.set_variation(
        ttf_parser::Tag::from_bytes(b"wght"),
        f32::from(request.key.weight),
    );
    let glyph_id = ttf_parser::GlyphId(request.key.glyph_id);
    let mut shape = face
        .glyph_shape(glyph_id)
        .ok_or(GlyphRasterError::MissingOutline)?;
    shape.normalize();
    if !shape.validate() {
        return Err(GlyphRasterError::InvalidShape);
    }
    let framing = shape
        .get_bound()
        .autoframe(
            config.width,
            config.height,
            Range::Px(f64::from(config.distance_range_px)),
            None,
        )
        .ok_or(GlyphRasterError::CannotFrame)?;
    if framing.scale.x <= 0.0 || framing.scale.y <= 0.0 {
        return Err(GlyphRasterError::CannotFrame);
    }
    shape.edge_coloring_simple(3.0, u64::from(request.key.glyph_id));
    let generator = MsdfGeneratorConfig::default();
    let mut bitmap = Bitmap::<Rgb<f32>>::new(config.width, config.height);
    shape.generate_msdf(&mut bitmap, &framing, &generator);
    shape.correct_sign(&mut bitmap, &framing, FillRule::default());
    shape.correct_msdf_error(&mut bitmap, &framing, &generator);
    bitmap.flip_y();

    let mut rgba = Vec::with_capacity(
        usize::try_from(config.width)
            .ok()
            .and_then(|width| {
                usize::try_from(config.height)
                    .ok()
                    .and_then(|height| width.checked_mul(height))
            })
            .and_then(|pixels| pixels.checked_mul(4))
            .ok_or(GlyphRasterError::InvalidConfiguration)?,
    );
    for pixel in bitmap.pixels() {
        rgba.extend_from_slice(&[
            float_channel(pixel.r),
            float_channel(pixel.g),
            float_channel(pixel.b),
            255,
        ]);
    }
    let units_per_em = f64::from(face.units_per_em()).max(1.0);
    let plane_bounds = GlyphPlaneBounds {
        left: (-framing.translate.x / units_per_em) as f32,
        bottom: (-framing.translate.y / units_per_em) as f32,
        right: ((f64::from(config.width) / framing.scale.x - framing.translate.x) / units_per_em)
            as f32,
        top: ((f64::from(config.height) / framing.scale.y - framing.translate.y) / units_per_em)
            as f32,
    };
    if !plane_bounds.is_valid() {
        return Err(GlyphRasterError::CannotFrame);
    }
    Ok(GeneratedGlyph {
        width: config.width,
        height: config.height,
        rgba: Arc::from(rgba),
        plane_bounds,
        distance_range_px: config.distance_range_px,
    })
}

#[cfg(feature = "gpu-wgpu")]
fn float_channel(value: f32) -> u8 {
    if value.is_finite() {
        (value.clamp(0.0, 1.0) * 255.0).round() as u8
    } else {
        0
    }
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug)]
pub(crate) struct GlyphRasterResult {
    pub(crate) key: GlyphKey,
    pub(crate) result: Result<GeneratedGlyph, GlyphRasterError>,
}

#[cfg(feature = "gpu-wgpu")]
#[derive(Debug, Error)]
pub(crate) enum GlyphWorkerError {
    #[error("glyph worker capacity and parallelism must be non-zero")]
    InvalidCapacity,
    #[error("the bounded glyph work queue is full")]
    QueueFull,
    #[error("the glyph worker has stopped")]
    Stopped,
}

/// Bounded CPU work queue backed by a fixed number of `spawn_blocking` jobs.
/// The supervisor never dequeues more work than the configured parallelism.
#[cfg(feature = "gpu-wgpu")]
pub(crate) struct GlyphAtlasWorker {
    request_tx: tokio::sync::mpsc::Sender<GlyphRasterRequest>,
    result_rx: tokio::sync::mpsc::Receiver<GlyphRasterResult>,
    supervisor: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "gpu-wgpu")]
impl fmt::Debug for GlyphAtlasWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GlyphAtlasWorker")
            .field("request_capacity", &self.request_tx.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "gpu-wgpu")]
impl GlyphAtlasWorker {
    pub(crate) fn spawn(
        config: GlyphRasterConfig,
        queue_capacity: usize,
        parallelism: usize,
    ) -> Result<Self, GlyphWorkerError> {
        if queue_capacity == 0 || parallelism == 0 || config.validate().is_err() {
            return Err(GlyphWorkerError::InvalidCapacity);
        }
        let (request_tx, mut request_rx) =
            tokio::sync::mpsc::channel::<GlyphRasterRequest>(queue_capacity);
        let (result_tx, result_rx) =
            tokio::sync::mpsc::channel::<GlyphRasterResult>(queue_capacity);
        let supervisor = tokio::spawn(async move {
            let mut accepting = true;
            let mut jobs = tokio::task::JoinSet::new();
            loop {
                if !accepting && jobs.is_empty() {
                    break;
                }
                tokio::select! {
                    joined = jobs.join_next(), if !jobs.is_empty() => {
                        if let Some(Ok(result)) = joined {
                            if result_tx.send(result).await.is_err() {
                                break;
                            }
                        }
                    }
                    request = request_rx.recv(), if accepting && jobs.len() < parallelism => {
                        match request {
                            Some(request) => {
                                jobs.spawn_blocking(move || {
                                    let key = request.key;
                                    let result = std::panic::catch_unwind(
                                        std::panic::AssertUnwindSafe(|| rasterize_glyph(&request, config)),
                                    )
                                    .unwrap_or(Err(GlyphRasterError::WorkerPanicked));
                                    GlyphRasterResult { key, result }
                                });
                            }
                            None => accepting = false,
                        }
                    }
                }
            }
        });
        Ok(Self {
            request_tx,
            result_rx,
            supervisor: Some(supervisor),
        })
    }

    pub(crate) fn enqueue(&self, request: GlyphRasterRequest) -> Result<(), GlyphWorkerError> {
        self.request_tx
            .try_send(request)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => GlyphWorkerError::QueueFull,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => GlyphWorkerError::Stopped,
            })
    }

    pub(crate) async fn enqueue_wait(
        &self,
        request: GlyphRasterRequest,
    ) -> Result<(), GlyphWorkerError> {
        self.request_tx
            .send(request)
            .await
            .map_err(|_| GlyphWorkerError::Stopped)
    }

    pub(crate) fn try_result(&mut self) -> Option<GlyphRasterResult> {
        self.result_rx.try_recv().ok()
    }

    pub(crate) async fn result(&mut self) -> Option<GlyphRasterResult> {
        self.result_rx.recv().await
    }
}

#[cfg(feature = "gpu-wgpu")]
impl Drop for GlyphAtlasWorker {
    fn drop(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scene::{TextShadow, TextStyle};

    fn key(value: u16) -> GlyphKey {
        GlyphKey {
            font_fingerprint: 7,
            face_index: 0,
            glyph_id: value,
            weight: 400,
        }
    }

    fn generated() -> GeneratedGlyph {
        GeneratedGlyph {
            width: 64,
            height: 64,
            rgba: Arc::from(vec![128_u8; 64 * 64 * 4]),
            plane_bounds: GlyphPlaneBounds {
                left: -0.1,
                bottom: -0.2,
                right: 0.9,
                top: 1.0,
            },
            distance_range_px: 8.0,
        }
    }

    fn commit(cache: &mut GlyphAtlasCache, glyph_key: GlyphKey) -> AtlasGlyph {
        let AtlasRequest::Generate(reservation) = cache.request(glyph_key).expect("reserve") else {
            panic!("new glyph should reserve a slot");
        };
        cache
            .commit(reservation, generated())
            .expect("commit")
            .glyph
    }

    #[test]
    fn pending_and_in_flight_slots_cannot_be_evicted() {
        let mut cache = GlyphAtlasCache::new(64, 64, 1).expect("cache");
        let AtlasRequest::Generate(pending) = cache.request(key(1)).expect("reserve") else {
            panic!("new glyph should reserve a slot");
        };
        assert_eq!(cache.request(key(2)), Err(AtlasError::AllSlotsInFlight));

        cache.commit(pending, generated()).expect("commit");
        let ticket = cache.begin_frame([key(1), key(1)]);
        assert_eq!(cache.request(key(2)), Err(AtlasError::AllSlotsInFlight));
        cache.retire_frame(ticket);
        assert!(matches!(
            cache.request(key(2)),
            Ok(AtlasRequest::Generate(_))
        ));
    }

    #[test]
    fn late_generation_cannot_overwrite_a_reused_slot() {
        let mut cache = GlyphAtlasCache::new(64, 64, 1).expect("cache");
        let AtlasRequest::Generate(old) = cache.request(key(1)).expect("reserve") else {
            panic!("new glyph should reserve a slot");
        };
        cache.cancel(old).expect("cancel");
        let AtlasRequest::Generate(new) = cache.request(key(1)).expect("reserve again") else {
            panic!("new glyph should reserve a slot");
        };
        assert_eq!(
            cache.commit(old, generated()),
            Err(AtlasError::StaleReservation)
        );
        assert!(cache.commit(new, generated()).is_ok());
    }

    #[test]
    fn least_recently_used_ready_slot_is_recycled() {
        let mut cache = GlyphAtlasCache::new(128, 64, 1).expect("cache");
        let first = commit(&mut cache, key(1));
        let second = commit(&mut cache, key(2));
        commit(&mut cache, key(3));
        commit(&mut cache, key(4));
        assert_eq!(cache.capacity(), 4);
        assert!(cache.ready(key(1)).is_some());

        let AtlasRequest::Generate(replacement) = cache.request(key(5)).expect("evict") else {
            panic!("the fifth glyph should recycle one slot");
        };
        assert_eq!(replacement.slot(), second.slot);
        assert_ne!(replacement.slot(), first.slot);
        assert!(cache.ready(key(2)).is_none());
    }

    #[test]
    fn scrolling_uses_monotonic_elapsed_time_and_wraps_without_relayout() {
        let motion = ScrollMotion {
            velocity: [-50.0, 0.0],
            cycle_distance: 120.0,
            repeat: true,
            start_seconds: 1.0,
        };
        assert_eq!(motion.offset_at(Duration::from_millis(500)), [0.0, 0.0]);
        assert_eq!(motion.offset_at(Duration::from_secs(2)), [-50.0, 0.0]);
        assert_eq!(motion.offset_at(Duration::from_secs(4)), [-30.0, 0.0]);
    }

    #[test]
    fn frame_clock_never_moves_backwards() {
        let clock = MonotonicFrameClock {
            origin: Instant::now(),
        };
        let first = clock.uniform_at(1920, 1080, clock.origin + Duration::from_millis(20));
        let second = clock.uniform_at(1920, 1080, clock.origin + Duration::from_millis(40));
        assert!(second.viewport_time[2] >= first.viewport_time[2]);
        assert_eq!(&second.viewport_time[..2], &[1920.0, 1080.0]);
    }

    #[test]
    fn draw_list_places_every_shadow_before_outline_and_fill() {
        let mut cache = GlyphAtlasCache::new(64, 64, 1).expect("cache");
        commit(&mut cache, key(1));
        let font = Arc::new(FontFaceBlob {
            fingerprint: 7,
            face_index: 0,
            data: Arc::<[u8]>::from([]),
        });
        let shaped = ShapedText {
            glyphs: Arc::from([ShapedGlyph {
                key: key(1),
                font,
                origin: [10.0, 20.0],
                font_size: 30.0,
            }]),
            content_extent: [30.0, 30.0],
            missing_glyphs: 0,
        };
        let mut style = TextStyle::default();
        style.shadow = Some(TextShadow {
            x: 2.0,
            y: 3.0,
            blur: 1.0,
            color: RgbaColor::new(0, 0, 0, 128),
        });
        let draw_list = build_draw_list(
            &shaped,
            &mut cache,
            TextPaint::from_scene(&style, 1.0),
            [0.0, 0.0, 1920.0, 1080.0],
            ScrollMotion::NONE,
        );
        assert_eq!(draw_list.shadow_range, 0..1);
        assert_eq!(draw_list.outline_fill_range, 1..2);
        assert_eq!(draw_list.instances[0].style_flags[3], 0.0);
        assert_eq!(draw_list.instances[1].style_flags[3], 1.0);
    }

    #[test]
    fn shader_uses_msdf_derivatives_clipping_and_premultiplied_output() {
        assert!(MSDF_TEXT_SHADER.contains("median3(sample_rgb)"));
        assert!(MSDF_TEXT_SHADER.contains("fwidth(input.uv)"));
        assert!(MSDF_TEXT_SHADER.contains("texture_2d_array"));
        assert!(MSDF_TEXT_SHADER.contains("discard;"));
        assert!(MSDF_TEXT_SHADER.contains("color.rgb * color.a"));
        assert_eq!(ATLAS_TEXTURE_FORMAT, "rgba8unorm-linear-rgb");
        #[cfg(feature = "gpu-wgpu")]
        {
            assert_eq!(atlas_wgpu_texture_format(), wgpu::TextureFormat::Rgba8Unorm);
            let blend = premultiplied_blend_state();
            assert_eq!(blend.color.src_factor, wgpu::BlendFactor::One);
            assert_eq!(blend.color.dst_factor, wgpu::BlendFactor::OneMinusSrcAlpha);
        }
    }

    #[test]
    fn scene_colors_are_converted_to_linear_space() {
        let linear = color_to_linear(RgbaColor::new(128, 255, 0, 128));
        assert!((linear[0] - 0.215_86).abs() < 0.000_1);
        assert_eq!(linear[1], 1.0);
        assert_eq!(linear[2], 0.0);
        assert!((linear[3] - 0.501_96).abs() < 0.000_1);
    }

    #[cfg(feature = "gpu-wgpu")]
    fn test_raster_request() -> GlyphRasterRequest {
        const FONT: &[u8] =
            include_bytes!("../../../bundle/managed/fonts/HelveticaNeueLTPro-Md.otf");
        let face = ttf_parser::Face::parse(FONT, 0).expect("test font");
        let glyph_id = face.glyph_index('A').expect("A glyph");
        let font = Arc::new(FontFaceBlob {
            fingerprint: font_fingerprint(FONT, 0),
            face_index: 0,
            data: Arc::from(FONT),
        });
        GlyphRasterRequest {
            key: GlyphKey {
                font_fingerprint: font.fingerprint,
                face_index: 0,
                glyph_id: glyph_id.0,
                weight: 500,
            },
            font,
        }
    }

    #[cfg(feature = "gpu-wgpu")]
    fn isolated_test_shaper() -> MsdfTextShaper {
        const FONT: &[u8] =
            include_bytes!("../../../bundle/managed/fonts/HelveticaNeueLTPro-Md.otf");
        let mut database = cosmic_text::fontdb::Database::new();
        database.load_font_data(FONT.to_vec());
        let family = database
            .faces()
            .next()
            .and_then(|face| face.families.first())
            .map(|(family, _)| family.clone())
            .expect("test font family");
        database.set_sans_serif_family(family);
        MsdfTextShaper {
            font_system: cosmic_text::FontSystem::new_with_locale_and_db(
                "en-US".to_string(),
                database,
            ),
            font_blobs: HashMap::new(),
        }
    }

    #[cfg(feature = "gpu-wgpu")]
    #[test]
    fn cosmic_text_shapes_non_ascii_and_reports_missing_glyphs() {
        let mut shaper = isolated_test_shaper();
        let request = TextShapeRequest {
            text: "Alerte météo, café".to_string(),
            bounds: crate::scene::ResolvedRect {
                x: 10.0,
                y: 20.0,
                width: 640.0,
                height: 160.0,
            },
            font_family: String::new(),
            weight: 500,
            font_size: 42.0,
            horizontal: crate::scene::HorizontalAlignment::Justify,
            vertical: crate::scene::VerticalAlignment::Center,
            auto_wrap: true,
        };
        let shaped = shaper.shape(&request).expect("shape non-ASCII text");
        assert!(!shaped.glyphs.is_empty());
        assert_eq!(shaped.missing_glyphs, 0);
        assert!(shaped.content_extent[0] > 0.0);

        let missing = shaper
            .shape(&TextShapeRequest {
                text: "\u{10ffff}".to_string(),
                ..request
            })
            .expect("shape missing glyph");
        assert!(missing.missing_glyphs > 0 || missing.glyphs.is_empty());
    }

    #[cfg(feature = "gpu-wgpu")]
    #[test]
    fn resolved_scene_layout_drives_text_bounds_clipping_and_opacity() {
        let scene =
            crate::scene::protected_default_scene(crate::scene::ProtectedSceneKind::StandardCrawl);
        let layout = crate::scene_layout::ResolvedSceneLayout::resolve(&scene, 720, 480)
            .expect("scene layout");
        let text = layout
            .drawable_nodes()
            .find(|node| node.id.as_str() == "crawl_text")
            .expect("crawl text");
        let mut shaper = isolated_test_shaper();
        let prepared = shaper
            .prepare_resolved_node(text, "Severe thunderstorm warning", 2.5)
            .expect("prepare text")
            .expect("text node");
        assert!(!prepared.shaped.glyphs.is_empty());
        assert_eq!(prepared.clip_rect, [0.0, 360.0, 720.0, 120.0]);
        assert_eq!(prepared.paint.fill[3], 1.0);
        assert_eq!(prepared.scroll.start_seconds, 2.5);
    }

    #[cfg(feature = "gpu-wgpu")]
    #[test]
    fn shadow_preview_prepares_an_immutable_resolved_scene() {
        let mut shaper = isolated_test_shaper();
        let prepared = prepare_shadow_scene(
            &mut shaper,
            ShadowSceneRequest {
                key: 1,
                kind: crate::scene::ProtectedSceneKind::StandardCrawl,
                width: 720,
                height: 480,
                bound_text: "Severe thunderstorm warning".to_string(),
            },
        )
        .expect("shadow preparation");
        assert_eq!(prepared.scene_id, crate::scene::STANDARD_CRAWL_ID);
        assert_eq!(prepared.resolved_node_count, 4);
        assert_eq!(prepared.prepared_text.len(), 1);
        assert!(prepared.glyph_count > 0);
        assert!(prepared.unique_raster_request_count > 0);
    }

    #[cfg(feature = "gpu-wgpu")]
    #[test]
    fn statically_linked_msdfgen_produces_a_linear_rgb_tile() {
        let request = test_raster_request();
        let glyph = rasterize_glyph(&request, GlyphRasterConfig::default()).expect("MSDF glyph");
        assert_eq!((glyph.width, glyph.height), (64, 64));
        assert_eq!(glyph.rgba.len(), 64 * 64 * 4);
        assert!(glyph.rgba.chunks_exact(4).all(|pixel| pixel[3] == 255));
        assert!(glyph.rgba.iter().any(|channel| *channel != 0));
    }

    #[cfg(feature = "gpu-wgpu")]
    #[tokio::test]
    async fn bounded_worker_returns_native_msdf_work() {
        assert!(matches!(
            GlyphAtlasWorker::spawn(GlyphRasterConfig::default(), 0, 1),
            Err(GlyphWorkerError::InvalidCapacity)
        ));
        let request = test_raster_request();
        let expected_key = request.key;
        let mut worker =
            GlyphAtlasWorker::spawn(GlyphRasterConfig::default(), 1, 1).expect("worker");
        worker.enqueue(request).expect("enqueue");
        let result = tokio::time::timeout(Duration::from_secs(10), worker.result())
            .await
            .expect("worker timeout")
            .expect("worker stopped");
        assert_eq!(result.key, expected_key);
        assert!(result.result.is_ok());
    }
}
