use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    time::Instant,
};

use bincode::Options;
use clickhouse::{insert::Insert, Client, Row};
use d3_geo_rs::{projection::mercator::Mercator, Transform};
use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
use geo_types::Coord;
use h3o::{CellIndex, LatLng, Resolution};
use itertools::{izip, Itertools};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use osmpbfreader::{blobs::result_blob_into_iter, Node, OsmObj, OsmPbfReader, Way};
use serde::{Deserialize, Serialize};
use tiny_skia::{Color, Paint, PathBuilder, Pixmap, Stroke, Transform as SkiaTransform};
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    let action = std::env::args().skip(1).next().unwrap_or_else(|| {
        println!("Pass an action, INGEST, DUMP, or DRAW");
        exit(1)
    });
    if action == "INGEST" {
        rt.block_on(ingest_into_clickhouse(&[
            "saint_petersburg.pbf",
            "berlin.pbf",
            "uusimaa.pbf",
        ]));
    }

    if action == "DUMP" {
        dump_all_ch_to_zana_files(&rt)
    }

    if action == "DRAW" {
        _ = std::fs::create_dir("drawings");
        for file in std::fs::read_dir("h3").unwrap() {
            let path = file.unwrap().path();
            let fname = path.file_name().unwrap().to_str().unwrap();
            draw_tile(path.to_str().unwrap(), &format!("drawings/{fname}.png"))
        }
    }
}

fn dump_all_ch_to_zana_files(rt: &Runtime) {
    let client = Client::default().with_url("http://localhost:8123");

    let mut cells_to_process = CellIndex::base_cells()
        .flat_map(|c| c.children(Resolution::Three))
        .collect_vec();

    while let Some(cell) = cells_to_process.pop() {
        println!("{} cells left to process", cells_to_process.len());
        let node_count = rt.block_on(query_nodes_count(&client, cell));
        if node_count == 0 {
            continue;
        }
        if node_count > MAX_NODES_PER_CELL && cell.resolution() < MIN_RESOLUTION {
            cells_to_process.extend(cell.children(cell.resolution().succ().unwrap()));
            println!("Too many nodes {node_count}, splitting");
            continue;
        }
        rt.block_on(zana_file_from_ch_tile(&client, cell));
    }
}

const MAX_NODES_PER_CELL: u64 = 100_000;
const MIN_RESOLUTION: Resolution = Resolution::Twelve;

async fn query_nodes_count(client: &Client, cell: CellIndex) -> u64 {
    let cell3 = cell.parent(Resolution::Three).unwrap();
    let cell3_num: u64 = cell3.into();
    let cell_num: u64 = cell.into();
    let res: u8 = cell.resolution().into();
    client
        .query("select count() from nodes WHERE h3ToParent(cell12, ?) == ? AND cell3 == ?")
        .bind(res)
        .bind(cell_num)
        .bind(cell3_num)
        .fetch_one()
        .await
        .unwrap()
}

#[derive(Row, Serialize, Deserialize)]
struct CHNode {
    id: i64,
    decimicro_lat: i32,
    decimicro_lon: i32,
    cell12: u64,
    cell3: u64,
    tags: Vec<(u64, u64)>,
}

#[derive(Row, Serialize, Deserialize)]
struct CHStringTableRow {
    id: u64,
    string: String,
}

#[derive(Row, Serialize, Deserialize)]
struct CHPath {
    id: i64,
    nodes: Vec<i64>,
    tags: Vec<(u64, u64)>,
}

#[derive(Default)]
struct StringTable {
    string_map: HashMap<String, u64>,
    new_inserts: Vec<CHStringTableRow>,
}

impl StringTable {
    async fn fetch(client: &Client) -> Self {
        StringTable {
            string_map: client
                .query("SELECT ?fields from string_table")
                .fetch_all::<CHStringTableRow>()
                .await
                .unwrap()
                .into_iter()
                .map(|CHStringTableRow { id, string }| (string, id))
                .collect(),
            new_inserts: Vec::new(),
        }
    }

    fn intern(&mut self, s: &str) -> u64 {
        let next_idx = self.string_map.len() as u64 + 1;
        match self.string_map.get(s) {
            Some(n) => *n,
            None => {
                self.string_map.insert(s.to_string(), next_idx);
                self.new_inserts.push(CHStringTableRow {
                    string: s.to_string(),
                    id: next_idx,
                });
                next_idx
            }
        }
    }
}

async fn ingest_into_clickhouse(files: &[&str]) {
    let client = Client::default().with_url("http://localhost:8123");
    let mut string_map = StringTable::fetch(&client).await;

    let mut node_inserter = client.insert("nodes").unwrap();
    let mut path_inserter = client.insert("paths").unwrap();

    for fname in files {
        for obj in osmobj(fname.to_string()).par_iter() {
            let obj = obj.unwrap();
            match obj {
                OsmObj::Node(n) => insert_node(&mut node_inserter, &mut string_map, n).await,
                OsmObj::Way(w) => insert_path(&mut path_inserter, &mut string_map, w).await,
                _ => {}
            };
        }
    }

    node_inserter.end().await.unwrap();
    path_inserter.end().await.unwrap();

    let mut string_inserter = client.insert("string_table").unwrap();
    for r in string_map.new_inserts {
        string_inserter.write(&r).await.unwrap()
    }
    string_inserter.end().await.unwrap();
}

async fn insert_node(node_insert: &mut Insert<CHNode>, string_table: &mut StringTable, node: Node) {
    let cell12 = node_to_cell(&node, Resolution::Twelve);
    let cell3 = cell12.parent(Resolution::Three).unwrap();
    node_insert
        .write(&CHNode {
            id: node.id.0,
            decimicro_lat: node.decimicro_lat,
            decimicro_lon: node.decimicro_lon,
            cell12: cell_index_to_num(cell12),
            cell3: cell_index_to_num(cell3),
            tags: node
                .tags
                .iter()
                .map(|(k, v)| (string_table.intern(k), string_table.intern(v)))
                .collect(),
        })
        .await
        .unwrap();
}

async fn insert_path(path_insert: &mut Insert<CHPath>, string_table: &mut StringTable, way: Way) {
    path_insert
        .write(&CHPath {
            id: way.id.0,
            nodes: way.nodes.into_iter().map(|n| n.0).collect(),
            tags: way
                .tags
                .iter()
                .map(|(k, v)| (string_table.intern(&k), string_table.intern(&v)))
                .collect(),
        })
        .await
        .unwrap();
}

fn cell_index_to_num(c: CellIndex) -> u64 {
    c.into()
}

fn osmobj(fname: String) -> OsmPbfReader<File> {
    OsmPbfReader::new(File::open(fname).unwrap())
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

fn node_to_cell(n: &Node, res: Resolution) -> CellIndex {
    LatLng::new(n.lat(), n.lon()).unwrap().to_cell(res)
}

#[derive(Serialize, Deserialize)]
struct ZanaDenseData {
    nodes: ZanaDenseNodes,
    paths: ZanaDensePaths,
    string_table: HashMap<String, u64>,
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
    tags: Vec<(u64, u64)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ZanaDensePaths {
    dids: Vec<i64>,
    dnodes: Vec<Vec<i64>>,
    /// (key_i, val_i)*, 0
    tags: Vec<u64>,
}

// fn append_data_to_file(cell: CellIndex, data: &ZanaDenseData) {
//     let encoded = bincode::DefaultOptions::new().serialize(data).unwrap();

//     let mut f = std::fs::OpenOptions::new()
//         .create(true)
//         .append(true)
//         .open(format!("h3/{cell}.h3z"))
//         .unwrap();
//     let len: u32 = encoded.len().try_into().unwrap();
//     f.write_all(&len.to_le_bytes()).unwrap();
//     f.write_all(&encoded).unwrap();
// }

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
// pub fn recompress_pbf() {
//     let h3_dir = PathBuf::from("h3");
//     let h3_dir_backup = PathBuf::from("h3_old");
//     if h3_dir.exists() {
//         _ = std::fs::remove_dir_all(&h3_dir_backup);
//         std::fs::rename(&h3_dir, &h3_dir_backup).unwrap();
//     }

//     std::fs::create_dir(&h3_dir).unwrap();
//     let mut sw = Instant::now();

//     // FIXME: these maps are per-file, we need to make them per-blob
//     let mut node_ids: HashMap<i64, CellIndex> = HashMap::new();
//     let mut string_table = ZanaStringTable::default();

//     let mut f = OsmPbfReader::new(File::open("uusimaa.pbf").unwrap());
//     for blob in f.blobs() {
//         let blob = blob.unwrap();
//         let mut cells_to_nodes: HashMap<CellIndex, Vec<Node>> = HashMap::new();
//         // FIXME: check all unwraps, api might sometimes return err on icompatible data,
//         //        zana needs to skip incompatible blocks
//         // TODO: iter only over nodes, should speed up conversion process
//         for obj in result_blob_into_iter(Ok(blob)) {
//             let obj = obj.unwrap();
//             match obj {
//                 OsmObj::Node(n) => {
//                     let cell = node_to_cell(&n, Resolution::Five);
//                     assert!(node_ids.insert(n.id.0, cell).is_none());
//                     cells_to_nodes.entry(cell).or_default().push(n);
//                 }
//                 _ => {}
//             }
//         }
//         println!("Sorted blob in {:?}", sw.elapsed());
//         sw = Instant::now();

//         // for (cell, mut nodes) in cells_to_nodes {
//         //     nodes.sort_by_key(|n| n.id.0);
//         //     let node_ids_deltas: Vec<i64> = nodes.iter().map(|n| n.id.0).deltas().collect();
//         //     let node_lats_deltas: Vec<i32> =
//         //         nodes.iter().map(|n| n.decimicro_lat).deltas().collect();
//         //     let node_lons_deltas: Vec<i32> =
//         //         nodes.iter().map(|n| n.decimicro_lon).deltas().collect();

//         //     let data_for_serialization = ZanaDenseData::Nodes(ZanaDenseNodes {
//         //         dids: node_ids_deltas,
//         //         dlats: node_lats_deltas,
//         //         dlons: node_lons_deltas,
//         //     });

//         //     append_data_to_file(cell, &data_for_serialization)
//         // }
//         // println!("Written files in {:?}", sw.elapsed());
//         // sw = Instant::now();
//     }

//     f.rewind().unwrap();
//     sw = Instant::now();

//     let mut cells_to_ways = HashMap::new();
//     for blob in f.blobs() {
//         let blob = blob.unwrap();

//         for obj in result_blob_into_iter(Ok(blob)) {
//             let obj = obj.unwrap();
//             match obj {
//                 OsmObj::Way(w) => {
//                     let cells_for_way: HashSet<_> = w
//                         .nodes
//                         .iter()
//                         .filter_map(|n| node_ids.get(&n.0).copied())
//                         .collect();

//                     for c in cells_for_way {
//                         cells_to_ways
//                             .entry(c)
//                             .or_insert_with(|| vec![])
//                             .push(w.clone());
//                     }
//                 }
//                 _ => {}
//             }
//         }
//         println!("Ways from one blob processed in {:?}", sw.elapsed());
//         sw = Instant::now();
//     }

//     // for (cell, ways) in cells_to_ways {
//     //     let dids = ways.iter().map(|w| w.id.0).deltas().collect_vec();
//     //     let dnodes = ways
//     //         .iter()
//     //         .map(|w| w.nodes.iter().map(|n| n.0).deltas().collect_vec())
//     //         .collect_vec();
//     //     let mut tags = vec![];

//     //     for w in ways.iter() {
//     //         for (k, v) in w.tags.iter() {
//     //             tags.push(string_table.intern(&k));
//     //             tags.push(string_table.intern(&v));
//     //         }
//     //         tags.push(0)
//     //     }
//     //     // remove last 0
//     //     if tags.len() > 0 {
//     //         tags.pop();
//     //     }

//     //     let data = ZanaDenseData::Paths(ZanaDensePaths { dids, dnodes, tags });
//     //     append_data_to_file(cell, &data);
//     // }

//     let f = OpenOptions::new()
//         .create(true)
//         .write(true)
//         .open(h3_dir.join("stringtable.binc"))
//         .unwrap();
//     let mut sorted_strings = string_table.strings.into_iter().collect_vec();
//     sorted_strings.sort_by_key(|(_, k)| *k);
//     let sorted_strings = sorted_strings.into_iter().map(|(k, _)| k).collect_vec();
//     bincode::DefaultOptions::new()
//         .serialize_into(f, &sorted_strings)
//         .unwrap();
// }

pub fn draw_tile(tile_name: &str, output_fname: &str) {
    let (string_table, zana_data) = read_zana_data(tile_name);

    let cell: CellIndex = PathBuf::from(tile_name)
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

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

    let find_tag = |s: &str| string_table.get(s).copied().unwrap_or(0);

    let building_tag = find_tag("building");
    let power_tag = find_tag("power");
    let highways_tag = find_tag("highway");

    let mut building_style = PaintStyle::default();
    building_style.paint.set_color_rgba8(20, 100, 20, 255);

    let mut highway_style = PaintStyle::default();
    highway_style.paint.set_color_rgba8(255, 150, 20, 50);

    let mut power_style = PaintStyle::default();
    power_style.paint.set_color_rgba8(10, 100, 255, 150);
    power_style.stroke.width = 2.0;

    let merc_vertices = || {
        cell.vertexes().map(Into::into).map(|ll: LatLng| {
            let coord = proj.transform(&Coord {
                x: ll.lng_radians(),
                y: -ll.lat_radians(),
            });
            (coord.x, coord.y)
        })
    };

    let (min_y, max_y) = merc_vertices()
        .map(|(_x, y)| y)
        .minmax()
        .into_option()
        .unwrap();
    let (min_x, max_x) = merc_vertices()
        .map(|(x, _y)| x)
        .minmax()
        .into_option()
        .unwrap();

    let x_span = (max_x - min_x) as f64;
    let y_span = (max_y - min_y) as f64;

    let x_size = 1024;
    let y_size = (x_size as f64 / x_span * y_span) as u32;

    let x_scale = x_size as f64 / x_span;
    let y_scale = y_size as f64 / y_span;

    let mut pixmap = Pixmap::new(x_size, y_size).unwrap();
    pixmap.fill(Color::BLACK);
    fn has_tag(p: &ZanaPath, tag: u64) -> bool {
        p.tags.iter().find(|(k, _)| *k == tag).is_some()
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
    pixmap.save_png(output_fname).unwrap();
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

fn read_zana_data(fname: &str) -> (HashMap<String, u64>, Vec<ZanaObj>) {
    println!("Reading {fname}");
    let mut result = vec![];
    let bufreader = FrameDecoder::new(BufReader::new(std::fs::File::open(fname).unwrap()));
    let ZanaDenseData {
        nodes,
        paths,
        string_table,
    } = bincode::DefaultOptions::new()
        .deserialize_from(bufreader)
        .unwrap();

    // nodes
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

    // paths
    let path_node_ids = paths
        .dnodes
        .into_iter()
        .map(|dnodes| dnodes.into_iter().original().collect_vec());
    let path_tags = paths.tags.split(|t| *t == 0);

    for (node_ids, tags) in izip!(path_node_ids, path_tags) {
        result.push(ZanaObj::Path(ZanaPath {
            nodes: node_ids,
            tags: tags.chunks_exact(2).map(|c| (c[0], c[1])).collect_vec(),
        }))
    }
    (string_table, result)
}

async fn zana_file_from_ch_tile(client: &Client, cell: CellIndex) {
    let sw = Instant::now();

    let cell3 = cell.parent(Resolution::Three).unwrap();
    let cell3_num: u64 = cell3.into();

    let cell_num: u64 = cell.into();
    let res: u8 = cell.resolution().into();
    // get nodes in cell
    // get paths that touch these nodes
    // get nodes in cell and in paths

    println!("Querying paths");
    let paths: Vec<CHPath> = client
        .query(
            "
                WITH 
                nodes_inside AS (
                    SELECT id as node_ids FROM nodes WHERE h3ToParent(cell12, ?) == ? AND cell3 == ?
                )
                SELECT ?fields FROM paths WHERE id in (
                    SELECT id from paths WHERE arrayJoin(nodes) in (select node_ids from nodes_inside)
                )
            ",
        )
        .bind(res)
        .bind(cell_num)
        .bind(cell3_num)
        .fetch_all()
        .await
        .unwrap();

    if paths.is_empty() {
        return;
    }

    println!("Querying nodes");
    let nodes: Vec<CHNode> = client
        .query(
            "
                WITH 
                nodes_inside AS (
                    SELECT id as node_ids FROM nodes WHERE h3ToParent(cell12, ?) == ? AND cell3 == ?
                ),
                paths_inside AS (
                    SELECT id FROM paths WHERE 
                        arrayJoin(nodes) in (select node_ids from nodes_inside)
                )
                SELECT ?fields from nodes where id IN (
                    SELECT arrayJoin(nodes) FROM paths WHERE id IN (SELECT id FROM paths_inside)
                )
                ORDER BY id
            ",
        )
        .bind(res)
        .bind(cell_num)
        .bind(cell3_num)
        .fetch_all()
        .await
        .unwrap();

    let mut missing_nodes = HashSet::new();
    let node_ids = nodes.iter().map(|n| n.id).collect::<HashSet<_>>();
    for p in &paths {
        for node in &p.nodes {
            if !node_ids.contains(&node) {
                missing_nodes.insert(node);
            }
        }
    }
    println!(
        "Queried {} paths and {} nodes in {:?}, missing {}",
        paths.len(),
        nodes.len(),
        sw.elapsed(),
        missing_nodes.len()
    );

    // write nodes
    let mut node_ids = vec![];
    let mut node_lats = vec![];
    let mut node_lons = vec![];

    for node in nodes {
        node_ids.push(node.id);
        node_lats.push(node.decimicro_lat);
        node_lons.push(node.decimicro_lon);
    }

    let nodes = ZanaDenseNodes {
        dids: node_ids.into_iter().deltas().collect(),
        dlats: node_lats.into_iter().deltas().collect(),
        dlons: node_lons.into_iter().deltas().collect(),
    };

    // write paths
    let ch_string_table = StringTable::fetch(client).await;
    let ch_reverse_table: HashMap<_, _> = ch_string_table
        .string_map
        .into_iter()
        .map(|(k, v)| (v, k))
        .collect();
    let mut string_table = StringTable::default();
    let dids = paths.iter().map(|w| w.id).deltas().collect_vec();
    let dnodes = paths
        .iter()
        .map(|w| w.nodes.iter().copied().deltas().collect_vec())
        .collect_vec();
    let mut tags = vec![];

    for w in paths.iter() {
        for (k, v) in w.tags.iter() {
            tags.push(string_table.intern(&ch_reverse_table[k]));
            tags.push(string_table.intern(&ch_reverse_table[v]));
        }
        tags.push(0)
    }
    // remove last 0
    if tags.len() > 0 {
        tags.pop();
    }

    let f = FrameEncoder::new(File::create(format!("h3/{cell}.zan")).unwrap());
    bincode::DefaultOptions::new()
        .serialize_into(
            f,
            &ZanaDenseData {
                nodes,
                paths: ZanaDensePaths { dids, dnodes, tags },
                string_table: string_table.string_map,
            },
        )
        .unwrap();
}
