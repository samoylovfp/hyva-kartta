mod server;

use bincode::Options;
use clickhouse::{insert::Insert, Client, Row};
use delta_encoding::DeltaEncoderExt;
use itertools::Itertools;
use lz4_flex::frame::FrameEncoder;
use osmpbfreader::{Node, OsmObj, OsmPbfReader, Way};
use serde::{Deserialize, Serialize};
use server::serve;
use std::{
    collections::{HashMap, HashSet},
    fs::{read_dir, File},
    io::BufReader,
    path::PathBuf,
    process::exit,
    str::FromStr,
    time::Instant,
};
use tokio::runtime::Runtime;
use zana::{
    draw_tile, read_zana_data, CellIndex, LatLng, Resolution, ZanaDenseData, ZanaDenseNodes,
    ZanaDensePaths,
};

fn main() {
    env_logger::init();

    let action = std::env::args().nth(1).unwrap_or_else(|| {
        println!("Pass an action");
        exit(1)
    });

    let rt = Runtime::new().unwrap();
    if action == "SERVE" {
        serve()
    }

    if action == "INGEST" {
        let filenames = read_dir(".")
            .into_iter()
            .flatten()
            .flatten()
            .map(|r| r.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("pbf"))
            .collect_vec();
        rt.block_on(ingest_into_clickhouse(&filenames));
    }

    if action == "DUMP" {
        dump_all_ch_to_zana_files(&rt)
    }

    if action == "DRAW" {
        _ = std::fs::create_dir("drawings");
        for file in std::fs::read_dir("h3").unwrap() {
            let path = file.unwrap().path();
            let fname = path.file_name().unwrap().to_str().unwrap();
            // let pixmap =
            // draw_tile( &format!("drawings/{fname}.png"))
        }
    }
    if action == "TIME" {
        time_loading_files();
    }
    if action == "CELL" {
        let url = std::env::args().nth(2).unwrap();
        let cell: CellIndex = url.parse().unwrap();
        let ll: LatLng = cell.into();
        let lat = ll.lat();
        let lon = ll.lng();
        println!("https://www.openstreetmap.org/#map=12/{lat}/{lon}")
    }
}

fn dump_all_ch_to_zana_files(rt: &Runtime) {
    let client = Client::default().with_url("http://localhost:8123");

    let mut cells_to_process = rt
        .block_on(async {
            let client = Client::default().with_url("http://localhost:8123");
            client
                .query("select distinct cell3 from nodes")
                .fetch_all::<String>()
                .await
                .unwrap()
        })
        .into_iter()
        .map(|c| CellIndex::from_str(&c).unwrap())
        .collect_vec();

    while let Some(cell) = cells_to_process.pop() {
        println!("{} cells left to process", cells_to_process.len());
        let node_count = rt.block_on(query_nodes_count(&client, cell));
        if node_count == 0 {
            continue;
        }
        if node_count > MAX_NODES_PER_CELL && cell.resolution() < MIN_RESOLUTION {
            // FIXME: probably some data loss on edges here
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
                .query("SELECT ?fields from strings")
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

async fn ingest_into_clickhouse(files: &[PathBuf]) {
    let client = Client::default().with_url("http://localhost:8123");
    let mut string_map = StringTable::fetch(&client).await;

    let mut node_inserter = client.insert("nodes").unwrap();
    let mut path_inserter = client.insert("paths").unwrap();

    for fname in files {
        for obj in osmobj(fname.to_string_lossy().to_string()).par_iter() {
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

    let mut string_inserter = client.insert("strings").unwrap();
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
        if f.path().extension().and_then(|e| e.to_str()) == Some("zan") {
            read_zana_data(BufReader::new(File::open(f.path()).unwrap()));
        }
    }
    println!("Read all files in {:?}", sw_total.elapsed());
}

fn node_to_cell(n: &Node, res: Resolution) -> CellIndex {
    LatLng::new(n.lat(), n.lon()).unwrap().to_cell(res)
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
