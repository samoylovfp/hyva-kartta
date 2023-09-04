use std::ops::{Add, Sub};

use d3_geo_rs::{projection::mercator::Mercator, Transform};
use geo_types::Coord;
use h3o::LatLng;

use serde::Deserialize;
use size_of::SizeOf;

/// https://wiki.openstreetmap.org/wiki/Node#Structure
#[derive(Debug, Clone, Deserialize, SizeOf)]
pub struct GeoCoord {
    /// south-to-north is -90..90
    pub decimicro_lat: i32,
    /// west-to-east is -180..180 with 0 at greenwich
    pub decimicro_lon: i32,
}

/// mercator projection with y flipped, so it can be drawn on a [tiny_skia::Pixmap]
#[derive(Debug, Clone)]
pub struct PicMercator {
    pub x: f64,
    pub y: f64,
}

impl Sub for PicMercator {
    type Output = PicMercator;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
        }
    }
}

impl Add for PicMercator {
    type Output = PicMercator;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            x: self.x + rhs.x,
            y: self.y + rhs.y,
        }
    }
}

impl GeoCoord {
    pub fn project(&self) -> PicMercator {
        let (lat, lon) = self.to_latlon();

        let Coord { x, y } = Mercator {}.transform(
            &(Coord {
                x: lon.to_radians(),
                y: lat.to_radians(),
            }),
        );

        PicMercator { x, y: -y }
    }

    pub fn from_latlon(lat: f64, lon: f64) -> Self {
        Self {
            decimicro_lat: (lat * 1e7) as i32,
            decimicro_lon: (lon * 1e7) as i32,
        }
    }

    pub fn to_latlon(&self) -> (f64, f64) {
        (
            self.decimicro_lat as f64 / 1e7,
            self.decimicro_lon as f64 / 1e7,
        )
    }
}

impl PicMercator {
    pub fn unproject(&self) -> GeoCoord {
        let coord = Coord {
            x: self.x,
            y: -self.y,
        };
        let Coord { x, y } = Mercator {}.invert(&coord);
        GeoCoord::from_latlon(y.to_degrees(), x.to_degrees())
    }
}

impl From<GeoCoord> for LatLng {
    fn from(value: GeoCoord) -> Self {
        let (lat, lon) = value.to_latlon();
        LatLng::new(lat, lon).unwrap()
    }
}

impl From<LatLng> for GeoCoord {
    fn from(value: LatLng) -> Self {
        GeoCoord::from_latlon(value.lat(), value.lng())
    }
}

impl From<GeoCoord> for PicMercator {
    fn from(value: GeoCoord) -> Self {
        value.project()
    }
}

impl From<PicMercator> for GeoCoord {
    fn from(value: PicMercator) -> Self {
        value.unproject()
    }
}

impl From<LatLng> for PicMercator {
    fn from(value: LatLng) -> Self {
        let geo: GeoCoord = value.into();
        geo.project()
    }
}
