mod server;

use clickhouse::{insert::Insert, Client, Row};
use itertools::Itertools;
use osmpbfreader::{Node, OsmObj, OsmPbfReader, Way};
use serde::{Deserialize, Serialize};
use server::serve;
use std::{
    collections::{HashMap, HashSet},
    fs::{read_dir, File},
    io::BufReader,
    path::PathBuf,
    process::exit,
    time::Instant,
};
use tokio::runtime::Runtime;
use zana::{
    coords::GeoCoord, draw_tile, read_zana_data, write_zana_data, CellIndex, LatLng, Resolution,
    StringTable, ZanaNode, ZanaPath,
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
        let mut pixmap = zana::Pixmap::new(1024 << 3, 1024 << 3).unwrap();
        pixmap.fill(zana::Color::BLACK);
        _ = std::fs::create_dir("drawings");
        for file in std::fs::read_dir("h3").unwrap() {
            let path = file.unwrap().path();
            let mut data = Vec::new();

            use std::io::Read;
            File::open(&path).unwrap().read_to_end(&mut data).unwrap();
            let scale = 0.6;
            let x = 0.008;
            let y = -1.430;
            draw_tile(
                &mut pixmap,
                &mut BufReader::new(File::open(&path).unwrap()),
                (x, x + scale, y, y + scale),
            );
        }
        pixmap.save_png(&format!("all.png")).unwrap();
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
    let lookup_table = rt.block_on(fetch_string_table(&client)).inverse();

    let mut cells_to_process = rt
        .block_on(async {
            let client = Client::default().with_url("http://localhost:8123");
            client
                .query("select distinct cell3 from nodes")
                .fetch_all::<u64>()
                .await
                .unwrap()
        })
        .into_iter()
        .map(|c| CellIndex::try_from(c).unwrap())
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
        rt.block_on(zana_file_from_ch_tile(&client, cell, &lookup_table));
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

#[derive(Row, Serialize, Deserialize)]
struct CHPathNewType(CHPath);

async fn fetch_string_table(client: &Client) -> StringTable {
    StringTable::new(
        client
            .query("SELECT ?fields from strings")
            .fetch_all::<CHStringTableRow>()
            .await
            .unwrap()
            .into_iter()
            .map(|CHStringTableRow { id, string }| (string, id))
            .collect(),
    )
}

async fn ingest_into_clickhouse(files: &[PathBuf]) {
    let client = Client::default().with_url("http://localhost:8123");
    let mut string_map = fetch_string_table(&client).await;
    let original_string_map = string_map.clone();

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
    for (string, id) in string_map.diff(original_string_map) {
        string_inserter
            .write(&CHStringTableRow { id, string })
            .await
            .unwrap()
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
                .map(|(k, v)| (string_table.intern(k), string_table.intern(v)))
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

async fn zana_file_from_ch_tile(
    client: &Client,
    cell: CellIndex,
    lookup_table: &HashMap<u64, String>,
) {
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
            if !node_ids.contains(node) {
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
    let zana_nodes = nodes
        .into_iter()
        .map(|chn| ZanaNode {
            id: chn.id,
            coords: GeoCoord {
                decimicro_lat: chn.decimicro_lat,
                decimicro_lon: chn.decimicro_lon,
            },
        })
        .collect();

    let zana_paths = paths
        .into_iter()
        .map(|chp| ZanaPath {
            nodes: chp.nodes,
            tags: chp.tags,
        })
        .collect();

    write_zana_data(
        zana_nodes,
        zana_paths,
        lookup_table,
        File::create(format!("h3/{cell}.zan")).unwrap(),
    )
}
