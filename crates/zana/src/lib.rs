use std::collections::HashMap;

use osmpbfreader::OsmPbfReader;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Path {
    pub points: Vec<(i32, i32)>,
    // tags: HashMap<String, String>,
}

/// useful for varint encoding
#[derive(Debug, Serialize, Deserialize)]
pub struct RelativePath {
    relative_points: Vec<(i32, i32)>,
}

impl<'s> Path {
    pub fn iter_lon_lat(&'s self) -> impl Iterator<Item = (f64, f64)> + 's {
        self.points
            .iter()
            .copied()
            .map(|(lon, lat)| (lon as f64 / 1e7, lat as f64 / 1e7))
    }

    pub fn relative_to(&self, c: h3o::CellIndex) -> RelativePath {
        let center = h3o::LatLng::from(c);
        let relative_points = self
            .points
            .iter()
            .copied()
            .map(|(lon, lat)| {
                (
                    lon - (center.lng() * 1e7) as i32,
                    lat - (center.lat() * 1e7) as i32,
                )
            })
            .collect();
        RelativePath { relative_points }
    }
}

pub fn read_ways() -> Vec<Path> {
    let mut f = OsmPbfReader::new(std::fs::File::open("uusimaa.pbf").unwrap());

    let mut node_coords = HashMap::new();
    let mut ways_without_coords = vec![];
    f.iter().filter_map(|o| o.ok()).for_each(|o| match o {
        osmpbfreader::OsmObj::Node(n) => {
            node_coords.insert(n.id, (n.decimicro_lon, n.decimicro_lat));
        }
        osmpbfreader::OsmObj::Way(w) => ways_without_coords.push((w.tags, w.nodes.clone())),
        _ => {}
    });

    ways_without_coords
        .into_iter()
        .filter_map(|(tags, nodes)| {
            let points: Vec<_> = nodes
                .into_iter()
                .filter_map(|n| node_coords.get(&n).copied())
                .collect();
            points.len().ge(&2).then(|| Path {
                points,
                // tags: tags
                //     .iter()
                //     .map(|(k, v)| (k.to_string(), v.to_string()))
                //     .collect(),
            })
        })
        .collect()
}
