//! Small helpers shared by the network alert hub and the EAS NET module.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Trimmed text for a JSON string, number, or bool. Blank strings are `None`.
pub fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        }
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// A JSON array of scalars, or a comma/semicolon separated string, as text.
pub fn value_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(|value| value_text(Some(value)))
            .collect(),
        Some(Value::String(value)) => value
            .split([',', ';'])
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

/// Text at a nested object path such as `["audio", "source"]`.
pub fn nested_text(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut current = value?;
    for key in path {
        current = current.get(*key)?;
    }
    value_text(Some(current))
}

pub fn xml_bool(raw: &str, default: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" | "enabled" => true,
        "0" | "false" | "no" | "off" | "disabled" => false,
        _ => default,
    }
}

/// Parses `250ms`, `10s`, `2m`, or `1h`. A bare number is seconds.
pub fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let (number, multiplier) = if let Some(number) = raw.strip_suffix("ms") {
        (number, 1)
    } else if let Some(number) = raw.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = raw.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = raw.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        (raw, 1_000)
    };
    number
        .trim()
        .parse::<u64>()
        .ok()
        .map(|value| Duration::from_millis(value.saturating_mul(multiplier)))
}

pub fn fallback(value: String, default: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    }
}

pub fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

/// Loads `KEY=value` lines from `<base>/.env` without overriding variables the
/// process already has. Returns how many were set.
pub fn load_local_env(base_dir: &Path) -> Result<usize> {
    let path = base_dir.join(".env");
    if !path.is_file() {
        return Ok(0);
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read local .env {}", path.display()))?;
    let mut loaded = 0usize;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || std::env::var_os(key).is_some() {
            continue;
        }
        std::env::set_var(key, parse_env_value(value.trim()));
        loaded += 1;
    }
    Ok(loaded)
}

/// Expands `$NAME` and `${NAME}` references. Unset variables expand to "".
pub fn expand_env_vars(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for next in chars.by_ref() {
                if next == '}' {
                    break;
                }
                name.push(next);
            }
            out.push_str(&std::env::var(name).unwrap_or_default());
            continue;
        }
        let mut name = String::new();
        while let Some(next) = chars.peek().copied() {
            if next == '_' || next.is_ascii_alphanumeric() {
                name.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if name.is_empty() {
            out.push('$');
        } else {
            out.push_str(&std::env::var(name).unwrap_or_default());
        }
    }
    out
}

fn parse_env_value(raw: &str) -> String {
    let raw = raw.trim();
    if raw.len() >= 2 {
        let bytes = raw.as_bytes();
        if (bytes[0] == b'"' && bytes[raw.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[raw.len() - 1] == b'\'')
        {
            return raw[1..raw.len() - 1].to_string();
        }
    }
    raw.to_string()
}

/// A stable 32-hex-digit SHA-256 prefix of `input`.
pub fn make_token(input: &str) -> String {
    hex_prefix(Sha256::digest(input.as_bytes()).as_slice())
}

/// Like [`make_token`], salted with the current time so repeats differ.
pub fn make_nonce_token(input: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(input.as_bytes());
    digest.update(unix_now_ms().to_le_bytes());
    hex_prefix(digest.finalize().as_slice())
}

fn hex_prefix(hash: &[u8]) -> String {
    hash[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

pub fn sanitize_header(value: &str) -> String {
    value.replace(['\r', '\n'], "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duration_suffixes() {
        assert_eq!(parse_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
    }
}
