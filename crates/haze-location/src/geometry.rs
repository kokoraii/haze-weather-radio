//! Minimal WKB and spherical-distance helpers used by point queries.

use std::f64::consts::PI;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_coordinate_is_valid() {
        assert!(valid_point(0.0, 0.0));
    }

    #[test]
    fn distance_is_zero_for_identical_points() {
        assert_eq!(haversine_m(52.1, -106.6, 52.1, -106.6), 0.0);
    }
}
