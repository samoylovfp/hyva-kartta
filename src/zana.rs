use osmpbfreader::OsmPbfReader;
use eframe::epaint::ahash::HashSet;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Path {
    pub points: Vec<(i32, i32)>,
}

pub fn read_nodes_from_file() -> Vec<Path> {
    let mut reader = OsmPbfReader::new(std::fs::File::open("uusima.pbf").unwrap());

    const ROADS: usize = 100000;
    let ways: Vec<_> = reader
        .iter()
        .filter_map(|o| o.ok())
        .filter(|o| o.tags().contains_key("highway"))
        .filter_map(|o| o.way().cloned())
        .filter(|w| !w.nodes.is_empty())
        .take(ROADS)
        .collect();

    let nodes_to_read: HashSet<_> = ways.iter().flat_map(|w| w.nodes.clone()).collect();

    reader.rewind().unwrap();

    let node_coordinates: HashMap<_, _> = reader
        .iter()
        .filter_map(|o| o.ok())
        .filter_map(|o| o.node().cloned())
        .filter(|n| nodes_to_read.contains(&n.id))
        .map(|n| (n.id, (-n.decimicro_lon, -n.decimicro_lat)))
        .collect();

    ways.iter()
        .map(|w| {
            let points = w
                .nodes
                .iter()
                .filter_map(|n| node_coordinates.get(n).cloned())
                .collect();
            Path { points }
        })
        .filter(|p| !p.points.is_empty())
        .collect()
}
