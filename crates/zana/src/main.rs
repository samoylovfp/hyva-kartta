use std::{
    collections::{
        hash_map::{Entry, OccupiedEntry},
        HashMap, HashSet,
    },
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    path::PathBuf,
};

use bincode::{DefaultOptions, Options};
use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
use h3o::{CellIndex, LatLng, Resolution};
use itertools::{izip, Itertools};
use osmpbfreader::{
    blobs::result_blob_into_iter, osmformat::StringTable, primitive_block_from_blob, Node, OsmObj,
    OsmPbfReader,
};
use serde::{Deserialize, Serialize};
use stopwatch::Stopwatch;
use tiny_skia::{Color, Paint, PathBuilder, Pixmap, Stroke, Transform};

fn main() {
    // recompress_pbf();
    // time_loading_files();
    // total_nodes();
    let sw = Stopwatch::start_new();
    draw_tiles(&[
        "851f1d4bfffffff",
        "851f18b3fffffff",
        "851f1d4ffffffff",
        "851f18b7fffffff",
    ]);
    println!("Read and drawn a busy tile in {sw}");
}

fn total_nodes() {
    let mut f = OsmPbfReader::new(File::open("uusimaa.pbf").unwrap());
    let num = f
        .iter()
        .filter(|o| matches!(o, Ok(OsmObj::Node(..))))
        .count();
    println!("{num} nodes in the file");
}

fn time_loading_files() {
    let sw_total = Stopwatch::start_new();
    for f in std::fs::read_dir("h3").unwrap() {
        let f = f.unwrap();
        if f.path().extension().and_then(|e| e.to_str()) == Some("h3z") {
            read_zana_data(f.path().to_str().unwrap());
        }
    }
    println!("Read all files in {sw_total}");
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

#[derive(Serialize, Deserialize)]
enum ZanaDenseData {
    Nodes(ZanaDenseNodes),
    Paths(ZanaDensePaths),
}

#[derive(Serialize, Deserialize)]
struct ZanaDenseNodes {
    dids: Vec<i64>,
    dlats: Vec<i32>,
    dlons: Vec<i32>,
}

#[derive(Debug)]
enum ZanaObj {
    Node(ZanaNode),
    Path(ZanaPath),
}
#[derive(Debug)]
struct ZanaNode {
    id: i64,
    decimicro_lat: i32,
    decimicro_lon: i32,
}

#[derive(Debug)]
struct ZanaPath {
    nodes: Vec<i64>,
    tags: Vec<(u32, u32)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ZanaDensePaths {
    dids: Vec<i64>,
    dnodes: Vec<Vec<i64>>,
    /// (key_i, val_i)*, 0
    tags: Vec<u32>,
}

fn append_data_to_file(cell: CellIndex, data: &ZanaDenseData) {
    let encoded = bincode::DefaultOptions::new().serialize(data).unwrap();

    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("h3/{cell}.h3z"))
        .unwrap();
    let len: u32 = encoded.len().try_into().unwrap();
    f.write_all(&len.to_le_bytes()).unwrap();
    f.write_all(&encoded).unwrap();
}

struct ZanaStringTable {
    next_free_idx: u32,
    strings: HashMap<String, u32>,
}

impl Default for ZanaStringTable {
    fn default() -> Self {
        Self {
            next_free_idx: 1,
            strings: Default::default(),
        }
    }
}

impl ZanaStringTable {
    fn intern(&mut self, s: &str) -> u32 {
        match self.strings.get(s) {
            Some(i) => *i,
            None => {
                let res = self.next_free_idx;
                self.strings.insert(s.to_string(), self.next_free_idx);
                self.next_free_idx = self.next_free_idx.checked_add(1).expect("Table is full");
                res
            }
        }
    }
}

/// it basically repeats the format of osm pbf but uses bincode instead of protobuf
pub fn recompress_pbf() {
    let h3_dir = PathBuf::from("h3");
    let h3_dir_backup = PathBuf::from("h3_old");
    if h3_dir.exists() {
        _ = std::fs::remove_dir_all(&h3_dir_backup);
        std::fs::rename(&h3_dir, &h3_dir_backup).unwrap();
    }

    std::fs::create_dir(&h3_dir).unwrap();
    let mut sw = Stopwatch::start_new();

    // FIXME: these maps are per-file, we need to make them per-blob
    let mut node_ids: HashMap<i64, CellIndex> = HashMap::new();
    let mut string_table = ZanaStringTable::default();

    let mut f = OsmPbfReader::new(File::open("berlin.pbf").unwrap());
    for blob in f.blobs() {
        let blob = blob.unwrap();
        let mut cells_to_nodes: HashMap<CellIndex, Vec<Node>> = HashMap::new();
        // FIXME: check all unwraps, api might sometimes return err on icompatible data,
        //        zana needs to skip incompatible blocks
        // TODO: iter only over nodes, should speed up conversion process
        for obj in result_blob_into_iter(Ok(blob)) {
            let obj = obj.unwrap();
            match obj {
                OsmObj::Node(n) => {
                    let cell = node_to_cell(&n, Resolution::Five);
                    node_ids.insert(n.id.0, cell);
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

            let data_for_serialization = ZanaDenseData::Nodes(ZanaDenseNodes {
                dids: node_ids_deltas,
                dlats: node_lats_deltas,
                dlons: node_lons_deltas,
            });

            append_data_to_file(cell, &data_for_serialization)
        }
        println!("Written files at {sw}");
        sw.restart();
    }

    f.rewind().unwrap();
    sw.restart();

    let mut cells_to_ways = HashMap::new();
    for blob in f.blobs() {
        let blob = blob.unwrap();

        for obj in result_blob_into_iter(Ok(blob)) {
            let obj = obj.unwrap();
            match obj {
                OsmObj::Way(w) => {
                    let cells_for_way: HashSet<_> = w
                        .nodes
                        .iter()
                        .filter_map(|n| node_ids.get(&n.0).copied())
                        .collect();

                    for c in cells_for_way {
                        cells_to_ways
                            .entry(c)
                            .or_insert_with(|| vec![])
                            .push(w.clone());
                    }
                }
                _ => {}
            }
        }
        println!("Ways from one blob processed in {sw}");
        sw.restart();
    }

    for (cell, ways) in cells_to_ways {
        let dids = ways.iter().map(|w| w.id.0).deltas().collect_vec();
        let dnodes = ways
            .iter()
            .map(|w| w.nodes.iter().map(|n| n.0).deltas().collect_vec())
            .collect_vec();
        let mut tags = vec![];

        for w in ways.iter() {
            for (k, v) in w.tags.iter() {
                tags.push(string_table.intern(&k));
                tags.push(string_table.intern(&v));
            }
            tags.push(0)
        }
        // remove last 0
        if tags.len() > 0 {
            tags.pop();
        }

        let data = ZanaDenseData::Paths(ZanaDensePaths { dids, dnodes, tags });
        append_data_to_file(cell, &data);
    }

    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .open(h3_dir.join("stringtable.binc"))
        .unwrap();
    let mut sorted_strings = string_table.strings.into_iter().collect_vec();
    sorted_strings.sort_by_key(|(_, k)| *k);
    let sorted_strings = sorted_strings.into_iter().map(|(k, _)| k).collect_vec();
    bincode::DefaultOptions::new()
        .serialize_into(f, &sorted_strings)
        .unwrap();
}

pub fn draw_tiles(tile_idexes: &[&str]) {
    let string_table_file = std::fs::File::open("h3/stringtable.binc").unwrap();
    let string_table: Vec<String> = bincode::DefaultOptions::new()
        .deserialize_from(string_table_file)
        .unwrap();

    let zana_data = tile_idexes
        .iter()
        .flat_map(|tile_name| read_zana_data(&format!("h3/{tile_name}.h3z")))
        .collect_vec();

    let node_id_hashmap: HashMap<_, _> = zana_data
        .iter()
        .filter_map(|o| match o {
            ZanaObj::Node(n) => Some((n.id, n)),
            _ => None,
        })
        .collect();

    let building_tag = string_table.iter().position(|t| t == "building").unwrap() as u32 + 1;
    let power_tag = string_table.iter().position(|t| t == "power").unwrap() as u32 + 1;

    let mut building_style = PaintStyle::default();
    building_style.paint.set_color_rgba8(20, 100, 20, 255);

    let mut power_style = PaintStyle::default();
    power_style.paint.set_color_rgba8(100, 200, 255, 150);
    power_style.stroke.width = 2.0;

    let (min_lat, max_lat) = node_id_hashmap
        .values()
        .map(|n| n.decimicro_lat)
        .minmax()
        .into_option()
        .unwrap();
    let (min_lon, max_lon) = node_id_hashmap
        .values()
        .map(|n| n.decimicro_lon)
        .minmax()
        .into_option()
        .unwrap();

    let size = 2048;

    let lon_scale = size as f64 / (max_lon - min_lon) as f64;
    let lat_scale = size as f64 / (max_lat - min_lat) as f64;

    let mut pixmap = Pixmap::new(size, size).unwrap();
    pixmap.fill(Color::BLACK);

    for obj in &zana_data {
        match obj {
            ZanaObj::Node(_) => {}
            ZanaObj::Path(p) => {
                if p.tags.iter().find(|(k, _)| *k == building_tag).is_some() {
                    draw_path(
                        &mut pixmap,
                        &p,
                        &node_id_hashmap,
                        (min_lat as f64, min_lon as f64),
                        (lat_scale, lon_scale),
                        &building_style,
                    )
                }
                if p.tags.iter().find(|(k, _)| *k == power_tag).is_some() {
                    draw_path(
                        &mut pixmap,
                        &p,
                        &node_id_hashmap,
                        (min_lat as f64, min_lon as f64),
                        (lat_scale, lon_scale),
                        &power_style,
                    )
                }
            }
        }
    }
    pixmap.save_png("berlin.png").unwrap();
}

#[derive(Default)]
struct PaintStyle<'paint> {
    paint: Paint<'paint>,
    stroke: Stroke,
}

fn draw_path(
    pixmap: &mut Pixmap,
    p: &ZanaPath,
    node_id_hashmap: &HashMap<i64, &ZanaNode>,
    offset: (f64, f64),
    scale: (f64, f64),
    PaintStyle { paint, stroke }: &PaintStyle,
) {
    let geo_to_pix = |lat: i32, lon: i32| {
        (
            (lon as f64 - offset.1) * scale.1,
            (lat as f64 - offset.0) * scale.0,
        )
    };
    let mut pb = PathBuilder::new();
    let node_coords = p
        .nodes
        .iter()
        .filter_map(|n| node_id_hashmap.get(n))
        .collect_vec();
    if node_coords.len() > 1 {
        let start = geo_to_pix(node_coords[0].decimicro_lat, node_coords[0].decimicro_lon);
        pb.move_to(start.0 as f32, start.1 as f32);
    }
    for node in &node_coords[1..] {
        let coords = geo_to_pix(node.decimicro_lat, node.decimicro_lon);
        pb.line_to(coords.0 as f32, coords.1 as f32);
    }

    if let Some(p) = pb.finish() {
        pixmap.stroke_path(&p, &paint, &stroke, Transform::identity(), None);
    }
}

fn read_zana_data(fname: &str) -> Vec<ZanaObj> {
    let mut result = vec![];
    let mut bufreader = BufReader::new(std::fs::File::open(fname).unwrap());
    let mut len = 0_u32.to_le_bytes();
    loop {
        match bufreader.read_exact(&mut len) {
            Ok(()) => {}
            Err(_) => break,
        }
        let mut buffer = vec![0; u32::from_le_bytes(len) as usize];
        bufreader.read_exact(&mut buffer).unwrap();

        let decoded: ZanaDenseData = bincode::DefaultOptions::new().deserialize(&buffer).unwrap();

        match decoded {
            ZanaDenseData::Nodes(nodes) => {
                let ids = nodes.dids.iter().copied().original();
                let lats = nodes.dlats.iter().copied().original();
                let lons = nodes.dlons.iter().copied().original();

                for (id, lat, lon) in izip!(ids, lats, lons) {
                    result.push(ZanaObj::Node(ZanaNode {
                        id,
                        decimicro_lat: lat,
                        decimicro_lon: lon,
                    }));
                }
            }
            ZanaDenseData::Paths(p) => {
                // let path_ids = p.dids.original ... etc
                let path_node_ids = p
                    .dnodes
                    .into_iter()
                    .map(|dnodes| dnodes.into_iter().original().collect_vec());
                let path_tags = p.tags.split(|t| *t == 0);

                for (node_ids, tags) in izip!(path_node_ids, path_tags) {
                    result.push(ZanaObj::Path(ZanaPath {
                        nodes: node_ids,
                        tags: tags.chunks_exact(2).map(|c| (c[0], c[1])).collect_vec(),
                    }))
                }
            }
        }
    }
    result
}
