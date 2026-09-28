use std::f32::consts::TAU;

use thiserror::Error;

pub const SAMPLE_RATE: u32 = 48_000;
pub const MARK_FREQ_HZ: f32 = 6250.0 / 3.0;
pub const SPACE_FREQ_HZ: f32 = 3125.0 / 2.0;
pub const BIT_DURATION_S: f32 = 6.0 / 3125.0;

const BIT_SAMPLE_NUMERATOR: u128 = SAMPLE_RATE as u128 * 6;
const BIT_SAMPLE_DENOMINATOR: u128 = 3125;
const PREAMBLE_BYTE: u8 = 0xAB;
const PREAMBLE_LEN: usize = 16;
const MAX_OUTPUT_PEAK: f32 = 0.2737;
const INTER_BURST_S: f32 = 1.0;
const BURST_LEAD_S: f32 = 0.100;
const PRE_ATTN_S: f32 = 1.0;
const EOM_LEAD_S: f32 = 1.0;
const EOM_TAIL_S: f32 = 0.8;
const ATTN_DEFAULT_S: f32 = 8.0;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SameError {
    #[error("originator must be exactly 3 ASCII characters")]
    InvalidOriginator,
    #[error("event must be exactly 3 ASCII characters")]
    InvalidEvent,
    #[error("duration must be exactly 4 ASCII digits")]
    InvalidDuration,
    #[error("at least one location is required")]
    MissingLocation,
    #[error("location code must be 6 ASCII digits")]
    InvalidLocation,
    #[error("issue time must be JJJHHMM ASCII digits")]
    InvalidIssueTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SameHeader {
    originator: String,
    event: String,
    locations: Vec<String>,
    duration: String,
    callsign: String,
    issue_time: String,
}

impl SameHeader {
    /// Create a validated SAME header.
    ///
    /// # Errors
    ///
    /// Returns an error if any field violates SAME header formatting.
    pub fn new(
        originator: impl Into<String>,
        event: impl Into<String>,
        locations: impl IntoIterator<Item = impl Into<String>>,
        duration: impl Into<String>,
        callsign: impl Into<String>,
        issue_time: impl Into<String>,
    ) -> Result<Self, SameError> {
        let originator = originator.into();
        let event = event.into();
        let locations: Vec<String> = locations.into_iter().map(Into::into).collect();
        let duration = duration.into();
        let callsign = callsign.into();
        let issue_time = issue_time.into();

        validate_exact_ascii(&originator, 3)
            .then_some(())
            .ok_or(SameError::InvalidOriginator)?;
        validate_exact_ascii(&event, 3)
            .then_some(())
            .ok_or(SameError::InvalidEvent)?;
        validate_digits(&duration, 4)
            .then_some(())
            .ok_or(SameError::InvalidDuration)?;
        if locations.is_empty() {
            return Err(SameError::MissingLocation);
        }
        for location in &locations {
            validate_digits(location, 6)
                .then_some(())
                .ok_or(SameError::InvalidLocation)?;
        }
        validate_digits(&issue_time, 7)
            .then_some(())
            .ok_or(SameError::InvalidIssueTime)?;

        Ok(Self {
            originator,
            event,
            locations,
            duration,
            callsign,
            issue_time,
        })
    }

    #[must_use]
    pub fn encoded(&self) -> String {
        let location = self.locations.join("-");
        let callsign = fixed_callsign(&self.callsign);
        format!(
            "ZCZC-{}-{}-{}+{}-{}-{}-",
            self.originator, self.event, location, self.duration, self.issue_time, callsign
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToneType {
    Wxr,
    Eas,
    EggTimer,
    Quebec,
}

#[derive(Debug, Clone)]
pub struct SameAudio {
    pub sample_rate: u32,
    pub samples: Vec<f32>,
}

impl SameAudio {
    #[must_use]
    pub fn to_pcm16le(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.samples.len() * 2);
        for sample in &self.samples {
            let clamped = sample.clamp(-1.0, 1.0);
            let value = (clamped * i16::MAX as f32) as i16;
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }
}

#[must_use]
pub fn generate_same_header_sequence(
    header: &SameHeader,
    tone_type: Option<ToneType>,
) -> SameAudio {
    let attention = tone_type.map(|tone| attention_tone(tone, ATTN_DEFAULT_S));
    let mut audio =
        generate_same_header_attention_sequence_with_attention(header, attention.as_deref());
    audio.samples.extend(eom_sequence().samples);
    audio
}

#[must_use]
pub fn generate_same_header_sequence_with_attention(
    header: &SameHeader,
    attention: Option<&[f32]>,
) -> SameAudio {
    let mut audio = generate_same_header_attention_sequence_with_attention(header, attention);
    audio.samples.extend(eom_sequence().samples);
    audio
}

#[must_use]
pub fn generate_same_header_attention_sequence(
    header: &SameHeader,
    tone_type: Option<ToneType>,
) -> SameAudio {
    let attention = tone_type.map(|tone| attention_tone(tone, ATTN_DEFAULT_S));
    generate_same_header_attention_sequence_with_attention(header, attention.as_deref())
}

#[must_use]
pub fn generate_same_header_attention_sequence_with_attention(
    header: &SameHeader,
    attention: Option<&[f32]>,
) -> SameAudio {
    let mut samples = Vec::new();
    samples.extend(silence(BURST_LEAD_S));
    samples.extend(triple_burst(&header_burst(header)));

    if let Some(attention) = attention {
        samples.extend(silence(PRE_ATTN_S));
        extend_limited(&mut samples, attention, MAX_OUTPUT_PEAK);
    }

    SameAudio {
        sample_rate: SAMPLE_RATE,
        samples,
    }
}

#[must_use]
pub fn eom_sequence() -> SameAudio {
    let mut samples = Vec::new();
    samples.extend(silence(EOM_LEAD_S));
    samples.extend(triple_burst(&eom_burst()));
    samples.extend(silence(EOM_TAIL_S));
    SameAudio {
        sample_rate: SAMPLE_RATE,
        samples,
    }
}

#[must_use]
pub fn header_burst(header: &SameHeader) -> Vec<f32> {
    let mut data = Vec::with_capacity(PREAMBLE_LEN + header.encoded().len());
    data.extend(std::iter::repeat_n(PREAMBLE_BYTE, PREAMBLE_LEN));
    data.extend(header.encoded().as_bytes());
    afsk_encode(&data)
}

#[must_use]
pub fn eom_burst() -> Vec<f32> {
    let mut data = Vec::with_capacity(PREAMBLE_LEN + 4);
    data.extend(std::iter::repeat_n(PREAMBLE_BYTE, PREAMBLE_LEN));
    data.extend(b"NNNN");
    afsk_encode(&data)
}

#[must_use]
pub fn afsk_encode(data: &[u8]) -> Vec<f32> {
    let bit_count = data.len().saturating_mul(8);
    let mut samples = Vec::with_capacity(samples_for_bits(bit_count));
    let mut phase = 0.0f32;

    for (byte_index, byte) in data.iter().enumerate() {
        for bit_index in 0..8 {
            let bit = (byte >> bit_index) & 1;
            let frequency = if bit == 1 {
                MARK_FREQ_HZ
            } else {
                SPACE_FREQ_HZ
            };
            let absolute_bit = (byte_index * 8) + bit_index;
            let sample_count = bit_sample_count(absolute_bit);
            extend_sine_with_phase(&mut samples, frequency, sample_count, &mut phase);
        }
    }

    debug_assert_eq!(samples.len(), samples_for_bits(bit_count));
    samples
}

#[must_use]
pub fn attention_tone(tone_type: ToneType, duration_s: f32) -> Vec<f32> {
    let mut tone = match tone_type {
        ToneType::Wxr => sine_duration(1050.0, duration_s),
        ToneType::Quebec => mix_tones(&[1050.0, 650.0], duration_s),
        ToneType::Eas => {
            let mut tone = mix_tones(&[853.0, 960.0], duration_s);
            apply_fade_in_place(&mut tone, 0.006);
            apply_fade_out_place(&mut tone, 0.006);
            tone
        }
        ToneType::EggTimer => egg_timer_tone(duration_s),
    };
    limit_peak_in_place(&mut tone, MAX_OUTPUT_PEAK);
    tone
}

fn egg_timer_tone(duration_s: f32) -> Vec<f32> {
    let total = (duration_s.max(0.0) * SAMPLE_RATE as f32).round() as usize;
    let tone_len = (SAMPLE_RATE as f32 * 0.070).round() as usize;
    let inter_len = (SAMPLE_RATE as f32 * 0.055).round() as usize;
    let gap_len = (SAMPLE_RATE as f32 * 0.500).round() as usize;
    let burst_period = tone_len + inter_len;
    let cycle_len = (burst_period * 4) + gap_len;
    let mut samples = vec![0.0; total];
    for (index, sample) in samples.iter_mut().enumerate().take(total) {
        let pos = index % cycle_len;
        if pos / burst_period < 4 && pos % burst_period < tone_len {
            *sample = (TAU * 2055.0 * index as f32 / SAMPLE_RATE as f32).sin() / 1.40;
        }
    }
    for cycle_start in (0..total).step_by(cycle_len.max(1)) {
        for burst in 0..4 {
            let start = cycle_start + (burst * burst_period);
            let end = (start + tone_len).min(total);
            if start < end {
                apply_fade_in_place(&mut samples[start..end], 0.006);
                apply_fade_out_place(&mut samples[start..end], 0.006);
            }
        }
    }
    samples
}

fn triple_burst(burst: &[f32]) -> Vec<f32> {
    let gap = silence(INTER_BURST_S);
    let mut samples = Vec::with_capacity((burst.len() * 3) + (gap.len() * 2));
    samples.extend_from_slice(burst);
    samples.extend_from_slice(&gap);
    samples.extend_from_slice(burst);
    samples.extend_from_slice(&gap);
    samples.extend_from_slice(burst);
    samples
}

fn sine_duration(freq_hz: f32, duration_s: f32) -> Vec<f32> {
    let sample_count = (duration_s.max(0.0) * SAMPLE_RATE as f32).round() as usize;
    sine(freq_hz, sample_count)
}

fn sine(freq_hz: f32, sample_count: usize) -> Vec<f32> {
    (0..sample_count)
        .map(|index| (TAU * freq_hz * index as f32 / SAMPLE_RATE as f32).sin())
        .collect()
}

fn extend_sine_with_phase(
    samples: &mut Vec<f32>,
    freq_hz: f32,
    sample_count: usize,
    phase: &mut f32,
) {
    let phase_step = TAU * freq_hz / SAMPLE_RATE as f32;
    for _ in 0..sample_count {
        samples.push(phase.sin() * MAX_OUTPUT_PEAK);
        *phase = (*phase + phase_step).rem_euclid(TAU);
    }
}

fn mix_tones(freqs: &[f32], duration_s: f32) -> Vec<f32> {
    let sample_count = (duration_s.max(0.0) * SAMPLE_RATE as f32).round() as usize;
    mix_tones_for_samples(freqs, sample_count)
}

fn mix_tones_for_samples(freqs: &[f32], sample_count: usize) -> Vec<f32> {
    let divisor = freqs.len().max(1) as f32;
    (0..sample_count)
        .map(|index| {
            freqs
                .iter()
                .map(|freq| (TAU * *freq * index as f32 / SAMPLE_RATE as f32).sin())
                .sum::<f32>()
                / divisor
        })
        .collect()
}

fn apply_fade_in_place(samples: &mut [f32], duration_s: f32) {
    let fade_samples = ((SAMPLE_RATE as f32 * duration_s).round() as usize).min(samples.len());
    if fade_samples == 0 {
        return;
    }
    let denom = (fade_samples - 1).max(1) as f32;
    for (index, sample) in samples.iter_mut().take(fade_samples).enumerate() {
        *sample *= index as f32 / denom;
    }
}

fn apply_fade_out_place(samples: &mut [f32], duration_s: f32) {
    let fade_samples = ((SAMPLE_RATE as f32 * duration_s).round() as usize).min(samples.len());
    if fade_samples == 0 {
        return;
    }
    let denom = (fade_samples - 1).max(1) as f32;
    let start = samples.len() - fade_samples;
    for index in 0..fade_samples {
        samples[start + index] *= 1.0 - (index as f32 / denom);
    }
}

fn extend_limited(output: &mut Vec<f32>, samples: &[f32], max_peak: f32) {
    output.reserve(samples.len());
    let peak = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite())
        .map(f32::abs)
        .fold(0.0, f32::max);
    let gain = if peak > max_peak {
        max_peak / peak
    } else {
        1.0
    };
    output.extend(samples.iter().map(|sample| {
        if sample.is_finite() {
            (*sample * gain).clamp(-max_peak, max_peak)
        } else {
            0.0
        }
    }));
}

fn limit_peak_in_place(samples: &mut [f32], max_peak: f32) {
    let peak = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite())
        .map(f32::abs)
        .fold(0.0, f32::max);
    let gain = if peak > max_peak {
        max_peak / peak
    } else {
        1.0
    };
    for sample in samples {
        if sample.is_finite() {
            *sample = (*sample * gain).clamp(-max_peak, max_peak);
        } else {
            *sample = 0.0;
        }
    }
}

fn bit_sample_count(bit_index: usize) -> usize {
    samples_for_bits(bit_index + 1) - samples_for_bits(bit_index)
}

fn samples_for_bits(bit_count: usize) -> usize {
    let numerator = bit_count as u128 * BIT_SAMPLE_NUMERATOR;
    ((numerator + (BIT_SAMPLE_DENOMINATOR / 2)) / BIT_SAMPLE_DENOMINATOR) as usize
}

fn silence(duration_s: f32) -> Vec<f32> {
    vec![0.0; (duration_s.max(0.0) * SAMPLE_RATE as f32).round() as usize]
}

fn fixed_callsign(value: &str) -> String {
    let normalized = value.replace('-', "/");
    let mut output: String = normalized.chars().take(8).collect();
    while output.len() < 8 {
        output.push(' ');
    }
    output
}

fn validate_exact_ascii(value: &str, len: usize) -> bool {
    value.len() == len && value.is_ascii()
}

fn validate_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_header_string() {
        let header = SameHeader::new("WXR", "RWT", ["065100"], "0015", "EC-GC-CA", "1661200")
            .expect("valid header");

        assert_eq!(
            header.encoded(),
            "ZCZC-WXR-RWT-065100+0015-1661200-EC/GC/CA-"
        );
    }

    #[test]
    fn rejects_bad_location() {
        let err = SameHeader::new("WXR", "RWT", ["ABC"], "0015", "EC", "1661200")
            .expect_err("bad location should fail");

        assert_eq!(err, SameError::InvalidLocation);
    }

    #[test]
    fn afsk_generates_lsb_first_samples() {
        let samples = afsk_encode(&[0b0000_0001]);

        assert_eq!(samples.len(), samples_for_bits(8));
        assert!(samples[1].abs() > 0.0);
    }

    #[test]
    fn afsk_uses_exact_average_baud_timing() {
        let samples = afsk_encode(&[0xAB, 0xCD, 0xEF, 0x12]);

        assert_eq!(samples.len(), samples_for_bits(32));
        assert_eq!(samples_for_bits(25), 2_304);
        assert_eq!(samples_for_bits(50), 4_608);
    }

    #[test]
    fn afsk_output_stays_below_full_scale() {
        let samples = afsk_encode(&[0xAB, 0xCD, 0xEF, 0x12]);

        assert_peak_at_most(&samples, MAX_OUTPUT_PEAK);
    }

    #[test]
    fn eom_sequence_produces_pcm() {
        let audio = eom_sequence();
        let pcm = audio.to_pcm16le();

        assert_eq!(audio.sample_rate, SAMPLE_RATE);
        assert_eq!(pcm.len(), audio.samples.len() * 2);
    }

    #[test]
    fn attention_tones_include_quebec_and_restored_egg_timer() {
        let duration_s = 1.0;
        let quebec = attention_tone(ToneType::Quebec, duration_s);
        let egg_timer = attention_tone(ToneType::EggTimer, duration_s);

        assert_eq!(quebec.len(), SAMPLE_RATE as usize);
        assert_eq!(egg_timer.len(), SAMPLE_RATE as usize);
        assert!(quebec.iter().any(|sample| sample.abs() > 0.0));
        assert!(egg_timer.iter().any(|sample| sample.abs() > 0.0));
        assert_eq!(egg_timer[0], 0.0);
        let silence_index = (SAMPLE_RATE as f32 * 0.080).round() as usize;
        assert!(egg_timer[silence_index].abs() < 0.001);
    }

    #[test]
    fn builtin_attention_tones_stay_below_full_scale() {
        for tone in [
            ToneType::Wxr,
            ToneType::Eas,
            ToneType::EggTimer,
            ToneType::Quebec,
        ] {
            let samples = attention_tone(tone, 1.0);

            assert_peak_at_most(&samples, MAX_OUTPUT_PEAK);
        }
    }

    #[test]
    fn same_sequence_uses_reduced_attention_amplitude() {
        let header = SameHeader::new("WXR", "RWT", ["065100"], "0015", "EC-GC-CA", "1661200")
            .expect("valid header");
        let audio = generate_same_header_attention_sequence(&header, Some(ToneType::Wxr));

        let peak = audio
            .samples
            .iter()
            .map(|sample| sample.abs())
            .fold(0.0, f32::max);
        assert!(peak <= MAX_OUTPUT_PEAK + 0.001);
    }

    #[test]
    fn custom_attention_is_limited_in_header_sequence() {
        let header = SameHeader::new("WXR", "RWT", ["065100"], "0015", "EC-GC-CA", "1661200")
            .expect("valid header");
        let attention = [2.0, -2.0, f32::NAN, 0.5];

        let audio =
            generate_same_header_attention_sequence_with_attention(&header, Some(&attention));

        assert!(audio.samples.iter().all(|sample| sample.is_finite()));
        assert_peak_at_most(&audio.samples, MAX_OUTPUT_PEAK);
    }

    fn assert_peak_at_most(samples: &[f32], max_peak: f32) {
        let peak = samples
            .iter()
            .map(|sample| sample.abs())
            .fold(0.0, f32::max);

        assert!(peak <= max_peak + f32::EPSILON);
    }
}
