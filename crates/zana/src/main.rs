use std::{
    collections::{
        hash_map::{Entry, OccupiedEntry},
        HashMap, HashSet,
    },
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    path::PathBuf,
    time::Instant,
};

use bincode::{DefaultOptions, Options};
use d3_geo_rs::{projection::mercator::Mercator, Transform};
use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
use geo_types::Coord;
use h3o::{CellIndex, LatLng, Resolution};
use itertools::{izip, Itertools};
use osmpbfreader::{
    blobs::result_blob_into_iter, osmformat::StringTable, primitive_block_from_blob, Node, OsmObj,
    OsmPbfReader,
};
use serde::{Deserialize, Serialize};
use tiny_skia::{Color, Paint, PathBuilder, Pixmap, Stroke, Transform as SkiaTransform};

fn main() {
    // recompress_pbf();
    // time_loading_files();
    // total_nodes();
    let sw = Instant::now();
    draw_tiles(&[
        "851126d3fffffff",
        "8508996bfffffff",
        "851126d7fffffff",
        "8508996ffffffff",
        "851126c3fffffff",
        "8508997bfffffff",
    ], "helsinki.png");

    // draw_tiles(
    //     &[
    //         "851f1d4bfffffff",
    //         "851f18b3fffffff",
    //         "851f1d4ffffffff",
    //         "851f18b7fffffff",
    //         "851f1887fffffff",
    //     ],
    //     "germ.png",
    // );
    // count_tiles_objects();
    println!("Read and drawn a busy tile in {:?}", sw.elapsed());
}

fn count_tiles_objects() {
    let files = std::fs::read_dir("h3").unwrap();
    for f in files {
        let f = f.unwrap();
        if f.path().extension().and_then(|s| s.to_str()) != Some("h3z") {
            continue;
        }
        println!(
            "{f:?}, {}",
            read_zana_data(f.path().to_str().unwrap()).len()
        )
    }
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
    let sw_total = Instant::now();
    for f in std::fs::read_dir("h3").unwrap() {
        let f = f.unwrap();
        if f.path().extension().and_then(|e| e.to_str()) == Some("h3z") {
            read_zana_data(f.path().to_str().unwrap());
        }
    }
    println!("Read all files in {:?}", sw_total.elapsed());
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
    let mut sw = Instant::now();

    // FIXME: these maps are per-file, we need to make them per-blob
    let mut node_ids: HashMap<i64, CellIndex> = HashMap::new();
    let mut string_table = ZanaStringTable::default();

    let mut f = OsmPbfReader::new(File::open("uusimaa.pbf").unwrap());
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
                    assert!(node_ids.insert(n.id.0, cell).is_none());
                    cells_to_nodes.entry(cell).or_default().push(n);
                }
                _ => {}
            }
        }
        println!("Sorted blob in {:?}", sw.elapsed());
        sw = Instant::now();

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
        println!("Written files in {:?}", sw.elapsed());
        sw = Instant::now();
    }

    f.rewind().unwrap();
    sw = Instant::now();

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
        println!("Ways from one blob processed in {:?}", sw.elapsed());
        sw = Instant::now();
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

pub fn draw_tiles(tile_idexes: &[&str], fname: &str) {
    let string_table_file = std::fs::File::open("h3/stringtable.binc").unwrap();
    let string_table: Vec<String> = bincode::DefaultOptions::new()
        .deserialize_from(string_table_file)
        .unwrap();
    let string_table: HashMap<_, _> = string_table
        .into_iter()
        .enumerate()
        .map(|(i, s)| (s, i + 1))
        .collect();

    let zana_data = tile_idexes
        .iter()
        .flat_map(|tile_name| read_zana_data(&format!("h3/{tile_name}.h3z")))
        .collect_vec();

    let proj = Mercator::default();

    let node_id_hashmap: HashMap<_, _> = zana_data
        .iter()
        .filter_map(|o| match o {
            ZanaObj::Node(n) => {
                let coord = proj.transform(&Coord {
                    x: (n.decimicro_lon as f64 / 1e7).to_radians(),
                    y: (n.decimicro_lat as f64 / 1e7).to_radians(),
                });

                Some((n.id, (coord.x, -coord.y)))
            }
            _ => None,
        })
        .collect();

    let building_tag = string_table["building"];
    let power_tag = string_table["power"];
    let highways_tag = string_table["highway"];

    let mut building_style = PaintStyle::default();
    building_style.paint.set_color_rgba8(20, 100, 20, 255);

    let mut highway_style = PaintStyle::default();
    highway_style.paint.set_color_rgba8(255, 150, 20, 50);

    let mut power_style = PaintStyle::default();
    power_style.paint.set_color_rgba8(10, 100, 255, 150);
    power_style.stroke.width = 2.0;

    let (min_y, max_y) = node_id_hashmap
        .values()
        .map(|&(_x, y)| y)
        .minmax()
        .into_option()
        .unwrap();
    let (min_x, max_x) = node_id_hashmap
        .values()
        .map(|&(x, _y)| x)
        .minmax()
        .into_option()
        .unwrap();

    let x_span = (max_x - min_x) as f64;
    let y_span = (max_y - min_y) as f64;

    let x_size = 4096;
    let y_size = (x_size as f64 / x_span * y_span) as u32;

    let x_scale = x_size as f64 / x_span;
    let y_scale = y_size as f64 / y_span;

    let mut pixmap = Pixmap::new(x_size,  y_size).unwrap();
    pixmap.fill(Color::BLACK);
    fn has_tag(p: &ZanaPath, tag: usize) -> bool {
        p.tags.iter().find(|(k, _)| *k == tag as u32).is_some()
    }

    for obj in &zana_data {
        match obj {
            ZanaObj::Node(_) => {}
            ZanaObj::Path(p) => {
                let mut style = None;

                if has_tag(p, building_tag) {
                    style = Some(&building_style);
                } else if has_tag(p, power_tag) {
                    style = Some(&power_style);
                } else if has_tag(p, highways_tag) {
                    style = Some(&highway_style);
                }
                if let Some(s) = style {
                    draw_path(
                        &mut pixmap,
                        &p,
                        &node_id_hashmap,
                        (min_x, min_y),
                        (x_scale, y_scale),
                        s,
                    )
                }
            }
        }
    }
    pixmap.save_png(fname).unwrap();
}

#[derive(Default)]
struct PaintStyle<'paint> {
    paint: Paint<'paint>,
    stroke: Stroke,
}

fn draw_path(
    pixmap: &mut Pixmap,
    p: &ZanaPath,
    node_id_hashmap: &HashMap<i64, (f64, f64)>,
    offset: (f64, f64),
    scale: (f64, f64),
    PaintStyle { paint, stroke }: &PaintStyle,
) {
    let transform = |x: f64, y: f64| {
        (
            (x as f64 - offset.0) * scale.0,
            (y as f64 - offset.1) * scale.1,
        )
    };
    let mut pb = PathBuilder::new();
    let node_coords = p
        .nodes
        .iter()
        .filter_map(|n| node_id_hashmap.get(n))
        .collect_vec();
    if node_coords.len() > 1 {
        let start = transform(node_coords[0].0, node_coords[0].1);
        pb.move_to(start.0 as f32, start.1 as f32);
    }
    for node in &node_coords[1..] {
        let coords = transform(node.0, node.1);
        pb.line_to(coords.0 as f32, coords.1 as f32);
    }

    if let Some(p) = pb.finish() {
        pixmap.stroke_path(&p, &paint, &stroke, SkiaTransform::identity(), None);
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
        if let Err(e) = bufreader.read_exact(&mut buffer) {
            panic!("{:?}", (e, u32::from_le_bytes(len), fname));
        }

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
