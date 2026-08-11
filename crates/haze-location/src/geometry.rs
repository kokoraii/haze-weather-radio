//! Minimal WKB and spherical-distance helpers used by point queries.

use std::f64::consts::PI;

use serde_json::{json, Value};
use thiserror::Error;

const EARTH_RADIUS_M: f64 = 6_371_008.8;

#[derive(Debug, Error)]
pub enum GeometryError {
    #[error("truncated WKB geometry")]
    Truncated,
    #[error("unsupported WKB geometry type {0}")]
    Unsupported(u32),
    #[error("invalid WKB byte order")]
    ByteOrder,
    #[error("WKB geometry has trailing bytes")]
    Trailing,
    #[error("WKB geometry contains a non-finite coordinate")]
    NonFinite,
    #[error("WKB coordinate is outside WGS84 bounds")]
    CoordinateBounds,
    #[error("WKB polygon ring is invalid")]
    InvalidRing,
    #[error("WKB multipolygon contains a non-polygon member")]
    InvalidMultiPolygonMember,
}

#[must_use]
pub fn valid_point(latitude: f64, longitude: f64) -> bool {
    latitude.is_finite()
        && longitude.is_finite()
        && (-90.0..=90.0).contains(&latitude)
        && (-180.0..=180.0).contains(&longitude)
}

#[must_use]
pub fn haversine_m(latitude_a: f64, longitude_a: f64, latitude_b: f64, longitude_b: f64) -> f64 {
    let radians = |degrees: f64| degrees * PI / 180.0;
    let lat_a = radians(latitude_a);
    let lat_b = radians(latitude_b);
    let delta_lat = lat_b - lat_a;
    let delta_lon = radians(longitude_b - longitude_a);
    let sin_lat = (delta_lat / 2.0).sin();
    let sin_lon = (delta_lon / 2.0).sin();
    let value = sin_lat * sin_lat + lat_a.cos() * lat_b.cos() * sin_lon * sin_lon;
    2.0 * EARTH_RADIUS_M * value.sqrt().asin()
}

#[must_use]
pub fn bounding_box(latitude: f64, longitude: f64, radius_km: f64) -> [f64; 4] {
    let latitude_delta = radius_km / 110.574;
    let longitude_scale = latitude.to_radians().cos().abs().max(0.01);
    let longitude_delta = radius_km / (111.320 * longitude_scale);
    [
        (longitude - longitude_delta).max(-180.0),
        (latitude - latitude_delta).max(-90.0),
        (longitude + longitude_delta).min(180.0),
        (latitude + latitude_delta).min(90.0),
    ]
}

pub fn contains_wkb(bytes: &[u8], longitude: f64, latitude: f64) -> Result<bool, GeometryError> {
    let mut cursor = Cursor::new(bytes);
    contains_geometry(&mut cursor, longitude, latitude)
}

pub fn wkb_to_geojson(bytes: &[u8]) -> Result<Value, GeometryError> {
    let mut cursor = Cursor::new(bytes);
    let geometry = decode_geometry(&mut cursor)?;
    if cursor.remaining() != 0 {
        return Err(GeometryError::Trailing);
    }
    Ok(geometry)
}

fn decode_geometry(cursor: &mut Cursor<'_>) -> Result<Value, GeometryError> {
    let little_endian = match cursor.read_u8()? {
        0 => false,
        1 => true,
        _ => return Err(GeometryError::ByteOrder),
    };
    let raw_type = cursor.read_u32(little_endian)?;
    match raw_type & 0x0000_00ff {
        1 => {
            let coordinate = read_coordinate(cursor, little_endian)?;
            Ok(json!({"type": "Point", "coordinates": coordinate}))
        }
        3 => {
            let coordinates = decode_polygon(cursor, little_endian)?;
            Ok(json!({"type": "Polygon", "coordinates": coordinates}))
        }
        6 => {
            let count = cursor.read_u32(little_endian)? as usize;
            if count > cursor.remaining() / 9 {
                return Err(GeometryError::Truncated);
            }
            let mut coordinates = Vec::with_capacity(count);
            for _ in 0..count {
                let member = decode_geometry(cursor)?;
                if member.get("type").and_then(Value::as_str) != Some("Polygon") {
                    return Err(GeometryError::InvalidMultiPolygonMember);
                }
                coordinates.push(
                    member
                        .get("coordinates")
                        .cloned()
                        .ok_or(GeometryError::InvalidMultiPolygonMember)?,
                );
            }
            Ok(json!({"type": "MultiPolygon", "coordinates": coordinates}))
        }
        other => Err(GeometryError::Unsupported(other)),
    }
}

fn decode_polygon(
    cursor: &mut Cursor<'_>,
    little_endian: bool,
) -> Result<Vec<Vec<[f64; 2]>>, GeometryError> {
    let ring_count = cursor.read_u32(little_endian)? as usize;
    if ring_count > cursor.remaining() / 4 {
        return Err(GeometryError::Truncated);
    }
    let mut rings = Vec::with_capacity(ring_count);
    for _ in 0..ring_count {
        let point_count = cursor.read_u32(little_endian)? as usize;
        if point_count < 4 {
            return Err(GeometryError::InvalidRing);
        }
        if point_count > cursor.remaining() / 16 {
            return Err(GeometryError::Truncated);
        }
        let mut points = Vec::with_capacity(point_count);
        for _ in 0..point_count {
            points.push(read_coordinate(cursor, little_endian)?);
        }
        if points.first() != points.last() {
            return Err(GeometryError::InvalidRing);
        }
        rings.push(points);
    }
    if rings.is_empty() {
        return Err(GeometryError::InvalidRing);
    }
    Ok(rings)
}

fn read_coordinate(
    cursor: &mut Cursor<'_>,
    little_endian: bool,
) -> Result<[f64; 2], GeometryError> {
    let longitude = cursor.read_f64(little_endian)?;
    let latitude = cursor.read_f64(little_endian)?;
    if !longitude.is_finite() || !latitude.is_finite() {
        return Err(GeometryError::NonFinite);
    }
    if !(-180.0..=180.0).contains(&longitude) || !(-90.0..=90.0).contains(&latitude) {
        return Err(GeometryError::CoordinateBounds);
    }
    Ok([longitude, latitude])
}

fn contains_geometry(
    cursor: &mut Cursor<'_>,
    longitude: f64,
    latitude: f64,
) -> Result<bool, GeometryError> {
    let little_endian = match cursor.read_u8()? {
        0 => false,
        1 => true,
        _ => return Err(GeometryError::ByteOrder),
    };
    let raw_type = cursor.read_u32(little_endian)?;
    let geometry_type = raw_type & 0x0000_00ff;
    match geometry_type {
        1 => {
            let x = cursor.read_f64(little_endian)?;
            let y = cursor.read_f64(little_endian)?;
            Ok((x - longitude).abs() < f64::EPSILON && (y - latitude).abs() < f64::EPSILON)
        }
        3 => contains_polygon(cursor, little_endian, longitude, latitude),
        6 => {
            let count = cursor.read_u32(little_endian)?;
            for _ in 0..count {
                if contains_geometry(cursor, longitude, latitude)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        other => Err(GeometryError::Unsupported(other)),
    }
}

fn contains_polygon(
    cursor: &mut Cursor<'_>,
    little_endian: bool,
    longitude: f64,
    latitude: f64,
) -> Result<bool, GeometryError> {
    let ring_count = cursor.read_u32(little_endian)?;
    let mut inside = false;
    for ring_index in 0..ring_count {
        let point_count = cursor.read_u32(little_endian)? as usize;
        let mut points = Vec::with_capacity(point_count);
        for _ in 0..point_count {
            points.push((
                cursor.read_f64(little_endian)?,
                cursor.read_f64(little_endian)?,
            ));
        }
        let ring_contains = ring_contains_point(&points, longitude, latitude);
        if ring_index == 0 {
            inside = ring_contains;
        } else if ring_contains {
            inside = false;
        }
    }
    Ok(inside)
}

fn ring_contains_point(points: &[(f64, f64)], x: f64, y: f64) -> bool {
    if points.len() < 3 {
        return false;
    }
    let mut inside = false;
    let mut previous = points[points.len() - 1];
    for &current in points {
        let crosses = (current.1 > y) != (previous.1 > y)
            && x < (previous.0 - current.0) * (y - current.1) / (previous.1 - current.1)
                + current.0;
        if crosses {
            inside = !inside;
        }
        previous = current;
    }
    inside
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, GeometryError> {
        let value = *self
            .bytes
            .get(self.position)
            .ok_or(GeometryError::Truncated)?;
        self.position += 1;
        Ok(value)
    }

    fn read_u32(&mut self, little_endian: bool) -> Result<u32, GeometryError> {
        let raw = self.take::<4>()?;
        Ok(if little_endian {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    }

    fn read_f64(&mut self, little_endian: bool) -> Result<f64, GeometryError> {
        let raw = self.take::<8>()?;
        Ok(if little_endian {
            f64::from_le_bytes(raw)
        } else {
            f64::from_be_bytes(raw)
        })
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], GeometryError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(GeometryError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(GeometryError::Truncated)?;
        self.position = end;
        bytes.try_into().map_err(|_| GeometryError::Truncated)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn zero_coordinate_is_valid() {
        assert!(valid_point(0.0, 0.0));
    }

    #[test]
    fn distance_is_zero_for_identical_points() {
        assert_eq!(haversine_m(52.1, -106.6, 52.1, -106.6), 0.0);
    }

    #[test]
    fn decodes_polygon_and_multipolygon_wkb() {
        let polygon = polygon_wkb(true);
        let decoded = wkb_to_geojson(&polygon).expect("polygon");
        assert_eq!(decoded["type"], "Polygon");
        assert_eq!(decoded["coordinates"][0][1], json!([-106.0, 52.0]));

        let mut multipolygon = Vec::new();
        multipolygon.push(1);
        multipolygon.extend_from_slice(&6_u32.to_le_bytes());
        multipolygon.extend_from_slice(&1_u32.to_le_bytes());
        multipolygon.extend_from_slice(&polygon);
        let decoded = wkb_to_geojson(&multipolygon).expect("multipolygon");
        assert_eq!(decoded["type"], "MultiPolygon");
        assert_eq!(decoded["coordinates"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn decodes_big_endian_point_wkb() {
        let mut bytes = vec![0];
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&(-106.67_f64).to_be_bytes());
        bytes.extend_from_slice(&(52.13_f64).to_be_bytes());
        let decoded = wkb_to_geojson(&bytes).expect("point");
        assert_eq!(
            decoded,
            json!({"type": "Point", "coordinates": [-106.67, 52.13]})
        );
    }

    #[test]
    fn rejects_trailing_truncated_and_invalid_wkb() {
        let mut trailing = polygon_wkb(true);
        trailing.push(0);
        assert!(matches!(
            wkb_to_geojson(&trailing),
            Err(GeometryError::Trailing)
        ));
        assert!(matches!(
            wkb_to_geojson(&polygon_wkb(true)[..8]),
            Err(GeometryError::Truncated)
        ));

        let mut point = vec![1];
        point.extend_from_slice(&1_u32.to_le_bytes());
        point.extend_from_slice(&f64::NAN.to_le_bytes());
        point.extend_from_slice(&0_f64.to_le_bytes());
        assert!(matches!(
            wkb_to_geojson(&point),
            Err(GeometryError::NonFinite)
        ));
    }

    fn polygon_wkb(little_endian: bool) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(u8::from(little_endian));
        if little_endian {
            bytes.extend_from_slice(&3_u32.to_le_bytes());
            bytes.extend_from_slice(&1_u32.to_le_bytes());
            bytes.extend_from_slice(&4_u32.to_le_bytes());
        } else {
            bytes.extend_from_slice(&3_u32.to_be_bytes());
            bytes.extend_from_slice(&1_u32.to_be_bytes());
            bytes.extend_from_slice(&4_u32.to_be_bytes());
        }
        for (longitude, latitude) in [
            (-107.0_f64, 52.0_f64),
            (-106.0, 52.0),
            (-106.0, 53.0),
            (-107.0, 52.0),
        ] {
            if little_endian {
                bytes
                    .write_all(&longitude.to_le_bytes())
                    .expect("longitude");
                bytes.write_all(&latitude.to_le_bytes()).expect("latitude");
            } else {
                bytes
                    .write_all(&longitude.to_be_bytes())
                    .expect("longitude");
                bytes.write_all(&latitude.to_be_bytes()).expect("latitude");
            }
        }
        bytes
    }
}
