pub mod coords;

use bincode::Options;
use coords::{GeoCoord, PicMercator};
pub use d3_geo_rs::{projection::mercator::Mercator, Transform};
use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
pub use geo;
use geo::algorithm::Intersects;
pub use geo_types::Coord;
pub use h3o;
use h3o::geom::ToGeo;
pub use h3o::{CellIndex, LatLng, Resolution};
use itertools::{izip, Itertools};
use log::{debug, trace};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    collections::HashMap,
    io::{Read, Write},
};
pub use tiny_skia::{Color, Pixmap};
use tiny_skia::{Paint, PathBuilder, Stroke, Transform as SkiaTransform};

#[derive(Debug, Clone)]
pub struct Path {
    pub points: Vec<GeoCoord>,
    // tags: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, SizeOf)]
pub struct ZanaDenseData {
    pub nodes: ZanaDenseNodes,
    pub paths: ZanaDensePaths,
    pub string_table: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize, SizeOf)]
pub struct ZanaDenseNodes {
    pub dids: Vec<i64>,
    pub dlats: Vec<i32>,
    pub dlons: Vec<i32>,
}

#[derive(Debug, SizeOf)]
pub enum ZanaObj {
    Node(ZanaNode),
    Path(ZanaPath),
}
#[derive(Debug, SizeOf)]
pub struct ZanaNode {
    pub id: i64,
    pub coords: GeoCoord,
}

#[derive(Debug, SizeOf)]
pub struct ZanaPath {
    pub nodes: Vec<i64>,
    pub tags: Vec<(u64, u64)>,
}

#[derive(Debug, Serialize, Deserialize, SizeOf)]
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

pub fn draw_tile(pixmap: &mut Pixmap, data: impl Read, bbox: PicMercatorBoundingBox) {
    let (string_table, zana_data) = read_zana_data(data);

    let node_id_hashmap: HashMap<_, _> = zana_data
        .iter()
        .filter_map(|o| match o {
            ZanaObj::Node(n) => Some((n.id, (n.coords.clone()))),
            _ => None,
        })
        .collect();

    let find_tag = |s: &str| string_table.get(s).copied().unwrap_or(0);

    let building_tag = find_tag("building");
    let power_tag = find_tag("power");
    let highways_tag = find_tag("highway");

    let building_style = PaintStyle::new((20, 100, 20, 200), 1.0);
    let highway_style = PaintStyle::new((255, 150, 20, 200), 1.0);
    let power_style = PaintStyle::new((0, 100, 255, 150), 1.0);
    let _default = PaintStyle::new((5, 5, 5, 0), 0.1);

    let x_span = bbox.bottom_right.x - bbox.top_left.x;
    let y_span = bbox.bottom_right.y - bbox.top_left.y;
    assert!(x_span > 0.0, "x_span: {x_span}");
    assert!(y_span > 0.0, "y_span: {y_span}");

    let x_size = pixmap.width();
    let y_size = (x_size as f64 / x_span * y_span) as u32;

    let x_scale = x_size as f64 / x_span;
    let y_scale = y_size as f64 / y_span;

    fn has_tag(p: &ZanaPath, tag: u64) -> bool {
        p.tags.iter().any(|(k, _)| *k == tag)
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
                        pixmap,
                        p,
                        &node_id_hashmap,
                        (bbox.top_left.x, bbox.top_left.y),
                        (x_scale, y_scale),
                        s,
                    )
                }
            }
        }
    }
}

struct PaintStyle<'paint> {
    paint: Paint<'paint>,
    stroke: Stroke,
}

impl PaintStyle<'_> {
    fn new(color: (u8, u8, u8, u8), width: f32) -> Self {
        let mut paint = Paint::default();
        let mut stroke = Stroke::default();
        paint.set_color_rgba8(color.0, color.1, color.2, color.3);
        stroke.width = width;
        Self { paint, stroke }
    }
}

fn draw_path(
    pixmap: &mut Pixmap,
    p: &ZanaPath,
    node_id_hashmap: &HashMap<i64, GeoCoord>,
    offset: (f64, f64),
    scale: (f64, f64),
    PaintStyle { paint, stroke }: &PaintStyle,
) {
    let offset_and_scale = |x: f64, y: f64| ((x - offset.0) * scale.0, (y - offset.1) * scale.1);
    let mut pb = PathBuilder::new();
    let node_coords = p
        .nodes
        .iter()
        .filter_map(|n| node_id_hashmap.get(n))
        .collect_vec();
    if node_coords.len() > 1 {
        let PicMercator { x, y } = node_coords[0].project();
        let (x, y) = offset_and_scale(x, y);
        pb.move_to(x as f32, y as f32);
    }
    for node in &node_coords[1..] {
        let PicMercator { x, y } = node.project();
        trace!("mercator: {x}:{y}");
        let (x, y) = offset_and_scale(x, y);
        pb.line_to(x as f32, y as f32);
    }
    if let Some(p) = pb.finish() {
        trace!("{p:?}");
        pixmap.stroke_path(&p, paint, stroke, SkiaTransform::identity(), None);
    }
}

pub fn read_zana_data(r: impl Read) -> (HashMap<String, u64>, Vec<ZanaObj>) {
    let mut result = vec![];
    let bufreader = FrameDecoder::new(r);
    let data: ZanaDenseData = bincode::DefaultOptions::new()
        .deserialize_from(bufreader)
        .unwrap();
    debug!("Dense zana data takes up {:#?}", data.size_of());
    let ZanaDenseData {
        nodes,
        paths,
        string_table,
    } = data;

    // nodes
    let ids = nodes.dids.iter().copied().original();
    let lats = nodes.dlats.iter().copied().original();
    let lons = nodes.dlons.iter().copied().original();

    for (id, lat, lon) in izip!(ids, lats, lons) {
        result.push(ZanaObj::Node(ZanaNode {
            id,
            coords: GeoCoord {
                decimicro_lat: lat,
                decimicro_lon: lon,
            },
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
    debug!(
        "Undensified zana data takes {:#?}, string table takes {:#?}",
        result.size_of(),
        string_table.size_of()
    );
    (string_table, result)
}

pub fn filter_cells_with_mercator_rectangle(
    cells: &[CellIndex],
    bbox: PicMercatorBoundingBox,
) -> Vec<CellIndex> {
    let mercator_top = -bbox.top_left.y;
    let mercator_left = bbox.top_left.x;
    let mercator_right = bbox.bottom_right.x;
    let mercator_bottom = -bbox.bottom_right.y;

    let proj = Mercator {};

    let topleft = proj.invert(&Coord {
        x: mercator_left,
        y: mercator_top,
    });
    let topright = proj.invert(&Coord {
        x: mercator_right,
        y: mercator_top,
    });
    let bottomleft = proj.invert(&Coord {
        x: mercator_left,
        y: mercator_bottom,
    });
    let bottomright = proj.invert(&Coord {
        x: mercator_right,
        y: mercator_bottom,
    });

    let geo_polygon = geo::polygon!(topleft, topright, bottomright, bottomleft);
    cells
        .iter()
        .copied()
        .filter(|c| geo_polygon.intersects(&c.to_geom(false).unwrap()))
        .collect()
}

#[derive(Default, Clone)]
pub struct StringTable {
    map: HashMap<String, u64>,
}

impl StringTable {
    pub fn new(map: HashMap<String, u64>) -> Self {
        StringTable { map }
    }
    pub fn intern(&mut self, s: &str) -> u64 {
        let next_idx = self.map.len() as u64 + 1;
        match self.map.get(s) {
            Some(n) => *n,
            None => {
                self.map.insert(s.to_string(), next_idx);
                next_idx
            }
        }
    }
    pub fn inverse(self) -> HashMap<u64, String> {
        self.map.into_iter().map(|(k, v)| (v, k)).collect()
    }

    pub fn diff(self, other: StringTable) -> impl Iterator<Item = (String, u64)> {
        self.map
            .into_iter()
            .filter(move |(k, _v)| !other.map.contains_key(k))
    }

    pub fn get_map(self) -> HashMap<String, u64> {
        self.map
    }
}

pub fn write_zana_data(
    nodes: Vec<ZanaNode>,
    paths: Vec<ZanaPath>,
    lookup_table: &HashMap<u64, String>,
    f: impl Write,
) {
    let mut output_string_table = StringTable::default();
    // let dids = paths.iter().map(|w| w.id).deltas().collect_vec();
    let dnodes = paths
        .iter()
        .map(|w| w.nodes.iter().copied().deltas().collect_vec())
        .collect_vec();
    let mut tags = vec![];

    let mut node_ids = vec![];
    let mut node_lats = vec![];
    let mut node_lons = vec![];

    for node in nodes {
        node_ids.push(node.id);
        node_lats.push(node.coords.decimicro_lat);
        node_lons.push(node.coords.decimicro_lon);
    }

    let dense_nodes = ZanaDenseNodes {
        dids: node_ids.into_iter().deltas().collect(),
        dlats: node_lats.into_iter().deltas().collect(),
        dlons: node_lons.into_iter().deltas().collect(),
    };

    for w in paths.iter() {
        for (key, value) in w.tags.iter() {
            tags.push(output_string_table.intern(&lookup_table[key]));
            tags.push(output_string_table.intern(&lookup_table[value]));
        }
        tags.push(0)
    }
    // remove last 0
    if !tags.is_empty() {
        tags.pop();
    }

    let f = FrameEncoder::new(f).auto_finish();
    bincode::DefaultOptions::new()
        .serialize_into(
            f,
            &ZanaDenseData {
                nodes: dense_nodes,
                paths: ZanaDensePaths {
                    dids: vec![],
                    dnodes,
                    tags,
                },
                string_table: output_string_table.map,
            },
        )
        .unwrap();
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use crate::{
        coords::GeoCoord, read_zana_data, write_zana_data, StringTable, ZanaNode, ZanaPath,
    };

    #[test]
    fn smoke_test_string_table() {
        let mut t = StringTable::default();
        assert_eq!(t.intern("a"), 1);
        assert_eq!(t.intern("b"), 2);
        assert_eq!(t.intern("a"), 1);

        assert_eq!(
            t.map,
            [("a".into(), 1), ("b".into(), 2)].into_iter().collect()
        )
    }

    #[test]
    fn smoke_test_read_write() {
        let st = [("map", 1), ("q3dm5", 2)]
            .into_iter()
            .map(|(s, id)| (s.into(), id))
            .collect();

        let n = |i| ZanaNode {
            id: i,
            coords: GeoCoord {
                decimicro_lat: 5 + (i as i32),
                decimicro_lon: 6,
            },
        };
        let mut output = Vec::new();
        write_zana_data(
            vec![n(1), n(2), n(3)],
            vec![ZanaPath {
                nodes: vec![1, 2, 3],
                tags: vec![(1, 2)],
            }],
            &StringTable::new(st).inverse(),
            &mut output,
        );
        let (string_table, objects) = read_zana_data(std::io::Cursor::new(output));

        let string_table = string_table
            .into_iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .sorted()
            .collect_vec();

        insta::assert_debug_snapshot!(string_table, @r###"
        [
            "map:1",
            "q3dm5:2",
        ]
        "###);
        insta::assert_debug_snapshot!(objects, @r###"
        [
            Node(
                ZanaNode {
                    id: 1,
                    coords: GeoCoord {
                        decimicro_lat: 6,
                        decimicro_lon: 6,
                    },
                },
            ),
            Node(
                ZanaNode {
                    id: 2,
                    coords: GeoCoord {
                        decimicro_lat: 7,
                        decimicro_lon: 6,
                    },
                },
            ),
            Node(
                ZanaNode {
                    id: 3,
                    coords: GeoCoord {
                        decimicro_lat: 8,
                        decimicro_lon: 6,
                    },
                },
            ),
            Path(
                ZanaPath {
                    nodes: [
                        1,
                        2,
                        3,
                    ],
                    tags: [
                        (
                            1,
                            2,
                        ),
                    ],
                },
            ),
        ]
        "###);
    }
}

pub fn draw_hex(cell: CellIndex, pixmap: &mut Pixmap, width: f32) {
    let boundary = cell.boundary();
    let mut boundary_iter = boundary
        .into_iter()
        .copied()
        .map(|v| -> PicMercator { v.into() });
    let (x_min, x_max) = boundary_iter
        .clone()
        .map(|m| m.x)
        .minmax()
        .into_option()
        .unwrap();

    let x_scale = x_max - x_min;

    let (y_min, y_max) = boundary_iter
        .clone()
        .map(|m| m.y)
        .minmax()
        .into_option()
        .unwrap();

    let y_scale = y_max - y_min;

    let scale = std::cmp::min(pixmap.width(), pixmap.height()) as f64
        / std::cmp::max_by(x_scale, y_scale, f64::total_cmp);

    let offset_and_scale = |x: f64, y: f64| ((x - x_min) * scale, (y - y_min) * scale);

    let mut path = PathBuilder::new();
    let first_point = boundary_iter.next().unwrap();
    let (x, y) = offset_and_scale(first_point.x, first_point.y);
    path.move_to(x as f32, y as f32);

    for node in boundary_iter {
        let (x, y) = offset_and_scale(node.x, node.y);
        path.line_to(x as f32, y as f32);
    }
    path.close();

    let mut paint = Paint::default();
    paint.set_color_rgba8(200, 200, 0, 100);

    let mut stroke = Stroke::default();
    stroke.width = width;

    // pixmap.fill_path(
    //     &path.finish().unwrap(),
    //     &paint,
    //     tiny_skia::FillRule::EvenOdd,
    //     SkiaTransform::identity(),
    //     None,
    // );

    pixmap.stroke_path(
        &path.finish().unwrap(),
        &paint,
        &stroke,
        SkiaTransform::identity(),
        None,
    );
}

#[derive(Debug)]
pub struct PicMercatorBoundingBox {
    pub top_left: PicMercator,
    pub bottom_right: PicMercator,
}
impl PicMercatorBoundingBox {
    pub fn sizes(&self, scale: f64) -> (f64, f64) {
        let width = (self.bottom_right.x - self.top_left.x) / scale;
        // because Y goes up to down
        let height = (self.bottom_right.y - self.top_left.y) / scale;
        (width, height)
    }
}

pub fn cell_to_bounding_box(cell: CellIndex) -> PicMercatorBoundingBox {
    let bounds = cell.boundary();
    let (min_lon, max_lon) = bounds
        .clone()
        .into_iter()
        .map(|v| v.lng())
        .minmax_by(f64::total_cmp)
        .into_option()
        .unwrap();
    let (min_lat, max_lat) = bounds
        .into_iter()
        .map(|v| v.lat())
        .minmax_by(f64::total_cmp)
        .into_option()
        .unwrap();

    // y is flipped in pic mercator, so top is has higher lat value
    PicMercatorBoundingBox {
        top_left: GeoCoord::from_latlon(max_lat, min_lon).into(),
        bottom_right: GeoCoord::from_latlon(min_lat, max_lon).into(),
    }
}
