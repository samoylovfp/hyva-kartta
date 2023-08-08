use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use bincode::Options;
use d3_geo_rs::{projection::mercator::Mercator, Transform as _};
use delta_encoding::DeltaDecoderExt;
use geo_types::Coord;
use itertools::{izip, Itertools};
use lz4_flex::frame::FrameDecoder;
use serde::{Deserialize, Serialize};

pub use h3o::{CellIndex, LatLng, Resolution};
use tiny_skia::{Color, Paint, PathBuilder, Pixmap, Stroke, Transform};

#[derive(Debug, Clone)]
pub struct Path {
    pub points: Vec<(i32, i32)>,
    // tags: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
pub struct ZanaDenseData {
    pub nodes: ZanaDenseNodes,
    pub paths: ZanaDensePaths,
    pub string_table: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
pub struct ZanaDenseNodes {
    pub dids: Vec<i64>,
    pub dlats: Vec<i32>,
    pub dlons: Vec<i32>,
}

#[derive(Debug)]
pub enum ZanaObj {
    Node(ZanaNode),
    Path(ZanaPath),
}
#[derive(Debug)]
pub struct ZanaNode {
    id: i64,
    decimicro_lat: i32,
    decimicro_lon: i32,
}

#[derive(Debug)]
pub struct ZanaPath {
    nodes: Vec<i64>,
    tags: Vec<(u64, u64)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ZanaDensePaths {
    pub dids: Vec<i64>,
    pub dnodes: Vec<Vec<i64>>,
    /// (key_i, val_i)*, 0
    pub tags: Vec<u64>,
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

pub fn draw_tile(tile_name: &str, output_fname: &str) {
    let (string_table, zana_data) = read_zana_data(BufReader::new(File::open(tile_name).unwrap()));

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
    building_style.stroke.width = 2.0;

    let mut highway_style = PaintStyle::default();
    highway_style.paint.set_color_rgba8(255, 150, 20, 50);
    highway_style.stroke.width = 2.0;

    let mut power_style = PaintStyle::default();
    power_style.paint.set_color_rgba8(10, 100, 255, 150);
    power_style.stroke.width = 3.0;

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
        pixmap.stroke_path(&p, &paint, &stroke, Transform::identity(), None);
    }
}

pub fn read_zana_data(r: impl Read) -> (HashMap<String, u64>, Vec<ZanaObj>) {
    let mut result = vec![];
    let bufreader = FrameDecoder::new(r);
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
