use d3_geo_rs::{projection::mercator::Mercator, Transform};
use geo_types::Coord;
use serde::Deserialize;

/// https://wiki.openstreetmap.org/wiki/Node#Structure
#[derive(Debug, Clone, Deserialize)]
pub struct GeoCoord {
    /// south-to-north is -90..90
    pub decimicro_lat: i32,
    /// west-to-east is -180..180 with 0 at greenwich
    pub decimicro_lon: i32,
}

impl GeoCoord {
    pub fn project(&self) -> PicMercator {
        let lat = (self.decimicro_lat as f64 / 1e7).to_radians();
        let lon = (self.decimicro_lon as f64 / 1e7).to_radians();

        let Coord { x, y } = Mercator {}.transform(&(Coord { x: lon, y: lat }));

        PicMercator { x, y: -y }
    }
}

/// mercator projection with y flipped, so it can be drawn on a [pixmap]
pub struct PicMercator {
    pub x: f64,
    pub y: f64,
}

impl PicMercator {
    pub fn unproject(&self) -> GeoCoord {
        let coord = Coord {
            x: self.x,
            y: -self.y,
        };
        let Coord { x, y } = Mercator {}.invert(&coord);
        GeoCoord {
            decimicro_lat: (y.to_degrees() / 1e7) as i32,
            decimicro_lon: (x.to_degrees() / 1e7) as i32,
        }
    }
}
