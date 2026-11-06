//! Circle tests against CAP `<polygon>` and `<circle>` areas.
//!
//! Distances use a local equirectangular projection around the subscriber's
//! point. That is accurate to well under a kilometre across the radii a client
//! may request (at most [`crate::interest::MAX_RADIUS_KM`]), which is far finer
//! than CAP zone geometry.

use serde::Serialize;

const EARTH_RADIUS_KM: f64 = 6371.0088;
const KM_PER_DEG_LAT: f64 = 110.574;
const KM_PER_DEG_LON_EQUATOR: f64 = 111.320;

/// A subscriber's point of interest and radius.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Circle {
    pub lat: f64,
    pub lon: f64,
    pub radius_km: f64,
}

/// A CAP area shape. Points are `(lat, lon)` in degrees.
#[derive(Debug, Clone, PartialEq)]
pub enum Shape {
    Polygon(Vec<(f64, f64)>),
    Circle(Circle),
}

impl Shape {
    /// Parses a CAP polygon: space-separated `lat,lon` pairs, at least three.
    pub fn parse_polygon(raw: &str) -> Option<Self> {
        let points = raw
            .split_whitespace()
            .map(parse_point)
            .collect::<Option<Vec<_>>>()?;
        (points.len() >= 3).then_some(Self::Polygon(points))
    }

    /// Parses a CAP circle: `lat,lon radius_km`.
    pub fn parse_circle(raw: &str) -> Option<Self> {
        let (center, radius) = raw.trim().split_once(char::is_whitespace)?;
        let (lat, lon) = parse_point(center)?;
        let radius_km = radius.trim().parse::<f64>().ok()?;
        (radius_km.is_finite() && radius_km >= 0.0).then_some(Self::Circle(Circle {
            lat,
            lon,
            radius_km,
        }))
    }
}

fn parse_point(raw: &str) -> Option<(f64, f64)> {
    let (lat, lon) = raw.split_once(',')?;
    let lat = lat.trim().parse::<f64>().ok()?;
    let lon = lon.trim().parse::<f64>().ok()?;
    valid_point(lat, lon).then_some((lat, lon))
}

pub fn valid_point(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
}

impl Circle {
    /// True when the circle overlaps the shape at all.
    pub fn intersects(&self, shape: &Shape) -> bool {
        match shape {
            Shape::Circle(other) => {
                haversine_km(self.lat, self.lon, other.lat, other.lon)
                    <= self.radius_km + other.radius_km
            }
            Shape::Polygon(points) => {
                let projected = points
                    .iter()
                    .map(|&(lat, lon)| self.project(lat, lon))
                    .collect::<Vec<_>>();
                contains_origin(&projected)
                    || projected
                        .iter()
                        .zip(projected.iter().cycle().skip(1))
                        .any(|(&a, &b)| segment_distance_to_origin(a, b) <= self.radius_km)
            }
        }
    }

    /// Kilometre offsets of a point from the circle's center.
    fn project(&self, lat: f64, lon: f64) -> (f64, f64) {
        let mut dlon = lon - self.lon;
        if dlon > 180.0 {
            dlon -= 360.0;
        } else if dlon < -180.0 {
            dlon += 360.0;
        }
        let x = dlon * KM_PER_DEG_LON_EQUATOR * self.lat.to_radians().cos();
        let y = (lat - self.lat) * KM_PER_DEG_LAT;
        (x, y)
    }
}

pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_KM * a.sqrt().asin()
}

/// Even-odd ray cast from the origin.
fn contains_origin(points: &[(f64, f64)]) -> bool {
    let mut inside = false;
    for (&(x1, y1), &(x2, y2)) in points.iter().zip(points.iter().cycle().skip(1)) {
        if (y1 > 0.0) != (y2 > 0.0) && 0.0 < (x2 - x1) * (0.0 - y1) / (y2 - y1) + x1 {
            inside = !inside;
        }
    }
    inside
}

fn segment_distance_to_origin(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (dx, dy) = (b.0 - a.0, b.1 - a.1);
    let length_sq = dx * dx + dy * dy;
    let t = if length_sq == 0.0 {
        0.0
    } else {
        (-(a.0 * dx + a.1 * dy) / length_sq).clamp(0.0, 1.0)
    };
    let (x, y) = (a.0 + t * dx, a.1 + t * dy);
    (x * x + y * y).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Roughly Regina's city limits.
    const REGINA: &str = "50.52,-104.75 50.52,-104.48 50.38,-104.48 50.38,-104.75 50.52,-104.75";

    #[test]
    fn point_inside_polygon_matches_at_zero_radius() {
        let polygon = Shape::parse_polygon(REGINA).unwrap();
        let downtown = Circle {
            lat: 50.4452,
            lon: -104.6189,
            radius_km: 0.0,
        };
        assert!(downtown.intersects(&polygon));
    }

    #[test]
    fn radius_reaches_a_nearby_polygon_edge() {
        let polygon = Shape::parse_polygon(REGINA).unwrap();
        // Pilot Butte sits about 7 km east of the eastern edge.
        let pilot_butte = Circle {
            lat: 50.47,
            lon: -104.38,
            radius_km: 3.0,
        };
        assert!(!pilot_butte.intersects(&polygon));
        let wider = Circle {
            radius_km: 10.0,
            ..pilot_butte
        };
        assert!(wider.intersects(&polygon));
    }

    #[test]
    fn circles_overlap_by_great_circle_distance() {
        let cap_circle = Shape::parse_circle("50.45,-104.61 5").unwrap();
        // Moose Jaw is about 65 km west of Regina.
        let moose_jaw = Circle {
            lat: 50.39,
            lon: -105.53,
            radius_km: 50.0,
        };
        assert!(!moose_jaw.intersects(&cap_circle));
        assert!(Circle {
            radius_km: 70.0,
            ..moose_jaw
        }
        .intersects(&cap_circle));
    }

    #[test]
    fn rejects_malformed_cap_shapes() {
        assert!(Shape::parse_polygon("50,-104 51,-104").is_none());
        assert!(Shape::parse_polygon("95,-104 51,-104 51,-105").is_none());
        assert!(Shape::parse_circle("50.45,-104.61").is_none());
    }
}
