//! Shared normalization rules for identifiers, station modes, and names.

use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

use crate::contract::StationMode;

#[must_use]
pub fn normalize_identifier(scheme: &str, value: &str) -> String {
    let scheme = scheme.trim().to_ascii_lowercase().replace('-', "_");
    let value = value.trim();
    match scheme.as_str() {
        "postal" | "postal_code" | "zip" | "zcta" => value
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .flat_map(char::to_uppercase)
            .collect(),
        "entity" | "canonical" => value.trim().to_ascii_lowercase(),
        _ => value
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .flat_map(char::to_uppercase)
            .collect(),
    }
}

#[must_use]
pub fn normalize_name(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_space = true;
    for ch in value.nfkd().filter(|ch| !is_combining_mark(*ch)) {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                normalized.push(lower);
            }
            previous_space = false;
        } else if !previous_space {
            normalized.push(' ');
            previous_space = true;
        }
    }
    normalized.trim().to_string()
}

#[must_use]
pub fn t9_digits(value: &str) -> String {
    normalize_name(value)
        .chars()
        .filter_map(|character| match character {
            'a'..='c' => Some('2'),
            'd'..='f' => Some('3'),
            'g'..='i' => Some('4'),
            'j'..='l' => Some('5'),
            'm'..='o' => Some('6'),
            'p'..='s' => Some('7'),
            't'..='v' => Some('8'),
            'w'..='z' => Some('9'),
            '0'..='9' => Some(character),
            _ => None,
        })
        .collect()
}

#[must_use]
pub fn locality_stem(value: &str) -> String {
    const SUFFIXES: &[&str] = &[
        "airport",
        "international",
        "intl",
        "station",
        "rcs",
        "awos",
        "asos",
        "weather",
        "climate",
        "auto",
        "automatic",
        "manual",
        "manned",
    ];
    let first = value.split(['/', '|']).next().unwrap_or(value);
    normalize_name(first)
        .split_whitespace()
        .filter(|token| !SUFFIXES.contains(token))
        .collect::<Vec<_>>()
        .join(" ")
}

#[must_use]
pub fn normalize_station_mode(value: &str) -> Option<StationMode> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    if normalized.starts_with("auto") {
        Some(StationMode::Auto)
    } else if matches!(normalized.as_str(), "man" | "manual" | "manned") {
        Some(StationMode::Manual)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_normalization_preserves_leading_zeroes() {
        assert_eq!(normalize_identifier("same", " 001001 "), "001001");
        assert_eq!(normalize_identifier("postal", " s7k 1a1 "), "S7K1A1");
    }

    #[test]
    fn names_are_unicode_and_punctuation_insensitive() {
        assert_eq!(normalize_name("Montréal-Trudeau"), "montreal trudeau");
    }

    #[test]
    fn station_facility_suffixes_do_not_change_locality_stem() {
        assert_eq!(locality_stem("Saskatoon Airport"), "saskatoon");
        assert_eq!(locality_stem("SASKATOON RCS"), "saskatoon");
        assert_eq!(
            locality_stem("Saskatoon/John G. Diefenbaker Intl"),
            "saskatoon"
        );
    }

    #[test]
    fn t9_normalization_supports_dtmf_search() {
        assert_eq!(t9_digits("Saskatoon"), "727528666");
        assert_eq!(t9_digits("727-52"), "72752");
    }
}
