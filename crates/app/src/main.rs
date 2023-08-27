// CURRENT TASK:
// draw hexes panned with view_center

use std::collections::HashMap;

use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};
use gloo::{
    events::EventListener,
    render::{request_animation_frame, AnimationFrame},
    utils::{body, document, format::JsValueSerdeExt, window},
};
use hyka::db::create_database;
use idb::{Database, Query};
use instant::{Duration, Instant};
use itertools::Itertools;
use log::{debug, info};
use lru::LruCache;
use serde::Deserialize;
use tiny_skia::{Paint, PathBuilder, Pixmap, Point, Stroke, Transform};
use wasm_bindgen::{throw_str, Clamped, JsCast, JsValue};
use wasm_bindgen_futures::{spawn_local, JsFuture};
use web_sys::{
    CanvasRenderingContext2d, CustomEvent, Event, HtmlCanvasElement, ImageBitmap, ImageData,
    PointerEvent,
};
use yew::{html, Callback, Component, NodeRef};
use zana::{
    coords::{GeoCoord, PicMercator},
    filter_cells_with_mercator_rectangle,
    h3o::{CellIndex, LatLng, Resolution},
    Mercator,
};

struct App {
    view_center: PicMercator,
    mercator_scale: f64,
    canvas: NodeRef,
    html_size: (u32, u32),
    animation_frame: AnimationFrame,
    downloaded_cells: Vec<CellIndex>,
    pan_start: Option<(PanEvent, PicMercator)>,
}

impl App {
    fn compose_tiles(&mut self) {
        let canvas: HtmlCanvasElement = self.canvas.cast().unwrap();
        let ctx: CanvasRenderingContext2d = canvas
            .get_context("2d")
            .unwrap()
            .unwrap()
            .dyn_into()
            .unwrap();
        let (x, y) = get_body_size();
        let ratio = x as f64 / y as f64;

        let cells = filter_cells_with_mercator_rectangle(
            &self.downloaded_cells,
            self.view_center.clone(),
            ratio,
            self.mercator_scale,
        );
        for cell in cells {
            let res = draw_hex(cell);
            // TODO: finish panning and move hexes of cells that current viewport matches
            // THEN: add a worker that produces actual hexes with values
        }

        // for cell in cells {
        //     if !self.
        //     let (top_left, w, h, data) = draw_hex(cell);
        //             ctx.draw_image_with_image_bitmap(

        //     ((time.as_secs_f64() / speed).sin() + 1.0) * 100.0,
        //     ((time.as_secs_f64() / speed).cos() + 1.0) * 100.0,
        // ).unwrap();
        // }

        // info!("{cells:?}");

        // for cell in visible_cells :
        // ctx.draw_image_with_image_bitmap(
        //     image_data,
        //     ((time.as_secs_f64() / speed).sin() + 1.0) * 100.0,
        //     ((time.as_secs_f64() / speed).cos() + 1.0) * 100.0,
        // )
        // .unwrap();
    }
}

struct DrawnCell {
    topleft: PicMercator,
    scale_x: f32,
    scale_y: f32,
    data: Pixmap,
}

fn draw_hex(cell: CellIndex) -> DrawnCell {
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

    let x_size = 512.0;
    // assuming cell boundary is never flat
    assert!(y_scale > 0.0);
    let y_size = x_size / x_scale * y_scale;

    let mut pixmap = Pixmap::new(x_size as u32, y_size as u32).unwrap();

    let offset_and_scale = |x: f64, y: f64| ((x - x_min) / x_scale, (y - y_min) / y_scale);

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
    paint.set_color_rgba8(200, 200, 0, 255);

    pixmap.stroke_path(
        &path.finish().unwrap(),
        &paint,
        &Stroke::default(),
        Transform::identity(),
        None,
    );

    DrawnCell {
        topleft: PicMercator { x: x_min, y: y_min },
        scale_x: x_scale as f32,
        scale_y: y_scale as f32,
        data: pixmap,
    }
}

async fn find_cell(db: &Database, coord: LatLng) -> Option<(CellIndex, Vec<u8>)> {
    let mut res = Resolution::Twelve;
    let tr = db
        .transaction(&["cells"], idb::TransactionMode::ReadOnly)
        .unwrap();
    let store = tr.object_store("cells").unwrap();
    while res >= Resolution::Three {
        let cell = coord.to_cell(res);
        let key = format!("{cell}.zan");
        if let Some(o) = store.get(Query::Key(key.into())).await.unwrap() {
            let s = o.as_string().unwrap();
            return Some((cell, BASE64_STANDARD_NO_PAD.decode(s).unwrap()));
        }
        res = res.pred().unwrap();
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PointerId(i32);

#[derive(Debug, Clone)]
struct PanEvent {
    x: i32,
    y: i32,
    id: PointerId,
}

#[derive(Debug)]
enum Msg {
    Resized,
    DownloadedFiles,
    ReadFiles(Vec<String>),
    Recompose,
    GeoMoved(MovedEvent),
    Rendered(ImageBitmap),
    PanStart(PanEvent),
    Pan(PanEvent),
    PanStop,
}

fn get_body_size() -> (u32, u32) {
    let body = body();
    (
        body.client_width().try_into().unwrap(),
        body.client_height().try_into().unwrap(),
    )
}

// FIXME: remove after movement is implemented
#[allow(dead_code)]
/// this comes from JS
#[derive(Debug, Deserialize)]
struct MovedEvent {
    // lat: f32,
    // lon: f32,
}

impl Component for App {
    type Message = Msg;

    type Properties = ();

    fn create(ctx: &yew::Context<Self>) -> Self {
        let resize_callback = ctx.link().callback(|_| Msg::Resized);
        let resize_listener =
            EventListener::new(&window(), "resize", move |_| resize_callback.emit(()));
        resize_listener.forget();

        let files_loaded_callback = ctx.link().callback(Msg::ReadFiles);
        spawn_local(async move {
            files_loaded_callback.emit(load_cell_names_from_indexeddb().await);
        });

        let move_callback = ctx.link().callback(Msg::GeoMoved);
        let move_listener = EventListener::new(&document(), "geomove", move |e: &Event| {
            let custom_event: &CustomEvent = e.dyn_ref().unwrap();
            move_callback.emit(custom_event.detail().into_serde().unwrap())
        });
        move_listener.forget();

        let helsinki = GeoCoord::from_latlon(60.1684, 24.9438);

        let recompose = ctx.link().callback(|_| Msg::Recompose);

        App {
            view_center: helsinki.into(),
            canvas: NodeRef::default(),
            html_size: get_body_size(),
            animation_frame: request_animation_frame(move |_| recompose.emit(())),
            mercator_scale: 0.001,
            downloaded_cells: vec![],
            pan_start: None,
        }
    }

    fn update(&mut self, ctx: &yew::Context<Self>, msg: Self::Message) -> bool {
        let recompose = ctx.link().callback(|()| Msg::Recompose);
        match msg {
            Msg::Resized => {
                self.html_size = get_body_size();
                recompose.emit(());
            }
            Msg::DownloadedFiles => {
                let files_loaded_callback = ctx.link().callback(Msg::ReadFiles);
                spawn_local(async move {
                    files_loaded_callback.emit(load_cell_names_from_indexeddb().await);
                });
            }
            // wasm_bindgen_futures::spawn_local( load_downloaded_files(&db, ctx.link().callback(|f| Msg::ReadFiles(f))));
            Msg::ReadFiles(f) => {
                info!("Read {:?} cells from db", f);
                self.downloaded_cells = f.into_iter().map(|s| s.parse().unwrap()).collect();

                if self.downloaded_cells.is_empty() {
                    download_files(ctx.link().callback(|_| Msg::DownloadedFiles));
                }
            }
            Msg::GeoMoved(_) => {
                // TODO
            }
            Msg::Recompose => {
                self.compose_tiles();
                // spawn_local(self.compose_tiles());
                // self.animation_frame = request_animation_frame(move |_| recompose.emit(()));
            }
            Msg::Rendered(data) => recompose.emit(()),
            Msg::PanStart(pan_event) => {
                self.pan_start = Some((pan_event, self.view_center.clone()))
            }
            Msg::Pan(PanEvent { x, y, id }) => {
                if let Some((
                    PanEvent {
                        x: sx,
                        y: sy,
                        id: sid,
                    },
                    start_center,
                )) = self.pan_start.clone()
                {
                    if sid == id {
                        let dx = x - sx;
                        let dy = y - sy;
                        self.view_center = PicMercator {
                            x: start_center.x + dx as f64 * self.mercator_scale,
                            y: start_center.y + dy as f64 * self.mercator_scale,
                        };
                        debug!("Panned to {:?}", self.view_center);
                        self.animation_frame = request_animation_frame(move |_| recompose.emit(()));
                    }
                }
            }
            Msg::PanStop => self.pan_start = None,
        }
        true
    }

    fn view(&self, ctx: &yew::Context<Self>) -> yew::Html {
        let (width, height) = self.html_size;
        let p_down = ctx.link().callback(|e: PointerEvent| {
            Msg::PanStart(PanEvent {
                x: e.x(),
                y: e.y(),
                id: PointerId(e.pointer_id()),
            })
        });
        let p_move = ctx.link().callback(|e: PointerEvent| {
            Msg::Pan(PanEvent {
                x: e.x(),
                y: e.y(),
                id: PointerId(e.pointer_id()),
            })
        });
        let p_up = ctx.link().callback(|e: PointerEvent| Msg::PanStop);
        html! {
            <>
            <canvas
                ref={&self.canvas}
                onpointerdown={p_down}
                onpointermove={p_move}
                onpointerup={p_up}
                width={width.to_string()}
                height={height.to_string()}
            ></canvas>
            </>
        }
    }
}

// async fn draw_cell(cell: CellIndex, data: &[u8]) -> ImageBitmap {
//     let boundary = cell.boundary();
//     let proj = Mercator {};
//     let projected_boundary = boundary.into_iter().map(|b| {
//         proj.transform(&Coord {
//             x: b.lng_radians(),
//             y: -b.lat_radians(),
//         })
//     });

//     let (min_x, max_x) = projected_boundary
//         .clone()
//         .map(|c| c.x)
//         .minmax()
//         .into_option()
//         .unwrap();
//     let (min_y, max_y) = projected_boundary
//         .map(|c| c.y)
//         .minmax()
//         .into_option()
//         .unwrap();

//     let mut pixmap = Pixmap::new(1024, 1024).unwrap();

//     draw_tile(&mut pixmap, data, (min_x, max_x, min_y, max_y));

//     let future = window()
//         .create_image_bitmap_with_image_data(
//             &ImageData::new_with_u8_clamped_array(Clamped(&pixmap.data()), pixmap.width()).unwrap(),
//         )
//         .unwrap();
//     JsFuture::from(future).await.unwrap().into()
// }
fn main() {
    wasm_logger::init(wasm_logger::Config::default());
    yew::Renderer::<App>::new().render();
}

fn download_files(download_complete_callback: Callback<()>) {
    wasm_bindgen_futures::spawn_local(async move {
        let list: Vec<String> = gloo::net::http::Request::get("/api/list")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let db = create_database().await.unwrap();
        for file in list {
            let _cell = file.split_once('.').unwrap().0;

            let data = gloo::net::http::Request::get(&format!("/api/get/{file}"))
                .send()
                .await
                .unwrap()
                .binary()
                .await
                .unwrap();

            let tr = db
                .transaction(&["cells"], idb::TransactionMode::ReadWrite)
                .unwrap();
            let store = tr.object_store("cells").unwrap();
            let b64 = BASE64_STANDARD_NO_PAD.encode(&data);
            store
                .put(&JsValue::from(b64), Some(&JsValue::from(file)))
                .await
                .unwrap();
            tr.commit().await.unwrap();
        }
        download_complete_callback.emit(());
    })
}

async fn load_cell_names_from_indexeddb() -> Vec<String> {
    let db = create_database().await.unwrap();

    let t = db
        .transaction(&["cells"], idb::TransactionMode::ReadOnly)
        .unwrap();
    let store = t.object_store("cells").unwrap();
    store
        .get_all_keys(None, None)
        .await
        .unwrap()
        .into_iter()
        .map(|v| {
            v.as_string()
                .unwrap()
                .split_once(".")
                .unwrap()
                .0
                .to_string()
        })
        .collect()
}
