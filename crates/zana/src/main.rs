use std::{
    collections::{
        hash_map::{Entry, OccupiedEntry},
        HashMap, HashSet,
    },
    fs::File,
    io::{BufReader, Read, Write},
};

use bincode::{DefaultOptions, Options};
use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
use fastbloom_rs::{BloomFilter, FilterBuilder, Membership};
use h3o::{CellIndex, LatLng, Resolution};
use itertools::izip;
use osmpbfreader::{
    blobs::result_blob_into_iter, osmformat::StringTable, primitive_block_from_blob, Node, OsmObj,
    OsmPbfReader,
};
use serde::{Deserialize, Serialize};
use stopwatch::Stopwatch;
use zana::{read_ways, Path};

fn main() {
    // check_bloom();
    // recompress_pbf();

    time_loading_files();
}

fn time_loading_files() {
    let sw_total = Stopwatch::start_new();
    for f in std::fs::read_dir("h3").unwrap() {
        let f = f.unwrap();
        if f.path().extension().and_then(|e| e.to_str()) == Some("h3z") {
            let mut chunks_per_file = 0;
            let mut sw = Stopwatch::start_new();
            let mut bufreader = BufReader::new(std::fs::File::open(f.path()).unwrap());
            let mut len = 0_u32.to_le_bytes();
            loop {
                match bufreader.read_exact(&mut len) {
                    Ok(()) => {
                        chunks_per_file += 1;
                    }
                    Err(_) => break,
                }
                println!("Chunk len {}", u32::from_le_bytes(len));
                let mut buffer = vec![0; u32::from_le_bytes(len) as usize];
                bufreader.read_exact(&mut buffer).unwrap();

                let decoded: ZanaDenseNodes =
                    bincode::DefaultOptions::new().deserialize(&buffer).unwrap();

                let ids = decoded.dids.iter().copied().original();
                let lats = decoded.dlats.iter().copied().original();
                let lons = decoded.dlons.iter().copied().original();
                let mut nodes = Vec::with_capacity(decoded.dids.len());
                for (did, dlat, dlon) in izip!(ids, lats, lons) {
                    nodes.push((did, dlat, dlon))
                }

                println!("Decoded chunk in {sw}");
                sw.restart();
            }
            println!("file {f:?} has {chunks_per_file} chunks");
        }
    }
    println!("Read all files in {sw_total}");
}

fn check_bloom() {
    let filter = BloomFilter::new(FilterBuilder::new(100_000, 0.01));
    dbg!(filter.get_u8_array().len());
}

// working with paths in-memory is too wasteful
// #[derive(Debug)]
// enum Cell {
//     Filling(Vec<Path>),
//     // capacity overfilled and the cell was split into children
//     Split,
// }

// struct BalancedH3 {
//     max_elements_per_cell: usize,
//     cells: HashMap<CellIndex, Cell>,
// }

// impl Default for BalancedH3 {
//     fn default() -> Self {
//         Self {
//             max_elements_per_cell: 10_000,
//             cells: Default::default(),
//         }
//     }
// }

// impl BalancedH3 {
//     fn add_no_balance(&mut self, p: Path) {
//         for (lon, lat) in p.iter_lon_lat() {
//             let mut res = Resolution::Zero;
//             let coord = LatLng::new(lat, lon).unwrap();
//             // FIXME: get rid of "unreachables"
//             loop {
//                 let c = coord.to_cell(res);
//                 let container = if self.cells.contains_key(&c) {
//                     match self.cells.get_mut(&c).unwrap() {
//                         Cell::Filling(f) => f,
//                         Cell::Split => {
//                             res = res
//                                 .succ()
//                                 .expect("balancing algo to never split the last resolution level");
//                             continue;
//                         }
//                     }
//                 } else {
//                     self.cells.insert(c, Cell::Filling(vec![]));
//                     match self.cells.get_mut(&c).unwrap() {
//                         Cell::Filling(f) => f,
//                         Cell::Split => unreachable!(),
//                     }
//                 };
//                 container.push(p.clone());
//             }
//         }
//     }

//     fn balance(&mut self) {
//         loop {
//             let keys_to_split: Vec<CellIndex> = self
//                 .cells
//                 .iter()
//                 .filter(|(k, v)| matches!(v, Cell::Filling(c) if c.len() > self.max_elements_per_cell && k.succ().is_some()))
//                 .map(|(k, _)| k.to_owned())
//                 .collect();
//             if keys_to_split.is_empty() {
//                 return;
//             }
//             for k in keys_to_split {
//                 let Cell::Filling(v) = self.cells.remove(&k).unwrap() else {
//                     unreachable!()
//                 };
//                 self.cells.insert(k, Cell::Split);
//                 for p in v {
//                     self.add_no_balance(p)
//                 }
//             }
//         }
//     }
// }

fn node_to_cell(n: &Node, res: Resolution) -> CellIndex {
    LatLng::new(n.decimicro_lat as f64 / 1e7, n.decimicro_lon as f64 / 1e7)
        .unwrap()
        .to_cell(res)
}

fn default_bloom() -> BloomFilter {
    BloomFilter::new(FilterBuilder::new(100_000, 0.01))
}

#[derive(Serialize, Deserialize)]
struct ZanaDenseNodes {
    dids: Vec<i64>,
    dlats: Vec<i32>,
    dlons: Vec<i32>,
}

fn recompress_pbf() {
    _ = std::fs::remove_dir_all("h3");
    std::fs::create_dir("h3").unwrap();
    let mut sw = Stopwatch::start_new();

    // let mut bloom_filters: HashMap<CellIndex, BloomFilter> = HashMap::new();

    let mut f = OsmPbfReader::new(File::open("uusimaa.pbf").unwrap());
    for blob in f.blobs() {
        let blob = blob.unwrap();
        let mut cells_to_nodes: HashMap<CellIndex, Vec<Node>> = HashMap::new();

        for obj in result_blob_into_iter(Ok(blob)) {
            let obj = obj.unwrap();
            match obj {
                OsmObj::Node(n) => {
                    let cell = node_to_cell(&n, Resolution::Five);
                    // bloom_filters
                    //     .entry(cell)
                    //     .or_insert_with(default_bloom)
                    //     .add(n.id.0.to_le_bytes().as_slice());
                    cells_to_nodes.entry(cell).or_default().push(n);
                }
                _ => {}
            }
        }
        println!("Sorted blob at {sw}");
        sw.restart();

        for (cell, mut nodes) in cells_to_nodes {
            nodes.sort_by_key(|n| n.id.0);
            let node_ids_deltas: Vec<i64> = nodes.iter().map(|n| n.id.0).deltas().collect();
            let node_lats_deltas: Vec<i32> =
                nodes.iter().map(|n| n.decimicro_lat).deltas().collect();
            let node_lons_deltas: Vec<i32> =
                nodes.iter().map(|n| n.decimicro_lon).deltas().collect();

            let data_for_serialization = ZanaDenseNodes {
                dids: node_ids_deltas,
                dlats: node_lats_deltas,
                dlons: node_lons_deltas,
            };

            let encoded = bincode::DefaultOptions::new()
                .serialize(&data_for_serialization)
                .unwrap();

            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("h3/{cell}.h3z"))
                .unwrap();
            let len: u32 = encoded.len().try_into().unwrap();
            println!("Writing {len} bytes for {cell}");
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&encoded).unwrap();
        }
        println!("Written files at {sw}");
        sw.restart();
    }

    // in each blob:
    // iter nodes
    //   1. separate nodes into proper h3 files
    //   2. save a bloom filter for a file
    // iter ways
    //   1. use bloom filter to add ways to file with respective nodes

    // iter through target files and check if further splitting is required.

    // println!("Read all paths in {sw}");
    // let points = paths.iter().flat_map(|p| p.points.iter()).count();
    // let unique_points = paths
    //     .iter()
    //     .flat_map(|p| p.points.iter())
    //     .collect::<HashSet<_>>()
    //     .len();
    // println!(
    //     "{points}, {unique_points}, {:.3} % duplicates",
    //     (points - unique_points) as f32 * 100.0 / points as f32
    // )

    // let mut tree = BalancedH3::default();

    // sw.restart();
    // for p in paths {
    //     tree.add_no_balance(p);
    // }
    // println!("Added all paths in {sw} to {}", tree.cells.len());
    // sw.restart();
    // tree.balance();
    // println!("Balanced all paths into {} nodes in {sw}", tree.cells.len());
    // println!("in {sw} got a map with keys {:?}", h3o_cells.keys());
}
