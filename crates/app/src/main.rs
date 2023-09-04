// CURRENT TASK:
// draw hexes panned with view_center

use std::{
    collections::{HashMap, HashSet},
    f64::consts::TAU,
};

use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};
use gloo::{
    events::EventListener,
    render::{request_animation_frame, AnimationFrame},
    utils::{body, document, format::JsValueSerdeExt, window},
};
use hyka::db::create_database;
use idb::{Database, Query};
use instant::Instant;
use itertools::Itertools;
use log::{debug, info};
use serde::Deserialize;
use tiny_skia::{Color, Paint, PathBuilder, Pixmap, Stroke, Transform};
use wasm_bindgen::{Clamped, JsCast, JsValue};
use wasm_bindgen_futures::{spawn_local, JsFuture};
use web_sys::{
    CanvasRenderingContext2d, CustomEvent, Event, HtmlCanvasElement, ImageBitmap, ImageData,
    PointerEvent,
};
use yew::{html, Callback, Component, NodeRef};
use zana::{
    cell_to_bounding_box,
    coords::{GeoCoord, PicMercator},
    draw_hex, draw_tile, filter_cells_with_mercator_rectangle,
    h3o::{CellIndex, LatLng, Resolution},
    Mercator, PicMercatorBoundingBox,
};

enum DeferredCell {
    Waiting,
    Done(UploadedCell),
}

struct App {
    view_center: PicMercator,
    /// mercator / pix
    mercator_scale: f64,
    canvas: NodeRef,
    html_size: (u32, u32),
    animation_frame: AnimationFrame,
    downloaded_cells: Vec<CellIndex>,
    pan_start: Option<(PanEvent, PicMercator)>,
    visible_cells: HashSet<CellIndex>,
    drawn_cells: HashMap<CellIndex, DeferredCell>,
}

impl App {
    fn compose_tiles(&mut self, callback: Callback<Vec<UploadedCell>>) {
        let canvas: HtmlCanvasElement = self.canvas.cast().unwrap();
        let ctx: CanvasRenderingContext2d = canvas
            .get_context("2d")
            .unwrap()
            .unwrap()
            .dyn_into()
            .unwrap();
        let (width, height) = get_body_size();

        let quarter_screen = PicMercator {
            x: width as f64 / 2.0 * self.mercator_scale,
            y: height as f64 / 2.0 * self.mercator_scale,
        };

        let bbox = PicMercatorBoundingBox {
            top_left: self.view_center.clone() - quarter_screen.clone(),
            bottom_right: self.view_center.clone() + quarter_screen,
        };

        let cells = filter_cells_with_mercator_rectangle(&self.downloaded_cells, bbox);
        ctx.clear_rect(0.0, 0.0, width as f64, height as f64);
        let cells_to_draw = cells
            .clone()
            .into_iter()
            .filter(|c| !self.drawn_cells.contains_key(c))
            .collect_vec();
        if !cells_to_draw.is_empty() {
            self.drawn_cells.extend(cells_to_draw.iter().copied().map(|c|(c, DeferredCell::Waiting)));
            // FIXME: concurrent drawing, should be fixed with a worker?
            spawn_local(async move {
                let mut results = vec![];
                let start = Instant::now();
                let cells_count = cells_to_draw.len();
                info!("Drawing {cells_count} cells...");
                let db = create_database().await.unwrap();
                for cell in cells_to_draw {
                    let mut pixmap = Pixmap::new(256, 256).unwrap();
                    // pixmap.fill(Color::BLACK);
                    draw_hex(cell, &mut pixmap, 10.0);
                    let data = get_cell(&db, cell).await;
                    let bbox = cell_to_bounding_box(cell);
                    draw_tile(
                        &mut pixmap,
                        data.as_slice(),
                        (
                            bbox.top_left.x,
                            bbox.bottom_right.x,
                            bbox.bottom_right.y,
                            bbox.top_left.y,
                        ),
                    );
                    let res = DrawnCell { cell, data: pixmap };
                    results.push(pixmap_to_imagedata(res).await);
                }
                info!("Rendered {cells_count} cells in {:?}", start.elapsed());

                callback.emit(results)
            })
        }

        self.drawn_cells.retain(|k, _v| cells.contains(k));
        debug!("Composing {} cells", self.drawn_cells.len());
        for (cell, data) in &self.drawn_cells {
            let DeferredCell::Done(data) = data else {continue};
            let bounding_box = cell_to_bounding_box(*cell);
            let width_px =
                (bounding_box.bottom_right.x - bounding_box.top_left.x) / self.mercator_scale;
            let screen_top_left_coords = self.view_center.clone()
                - PicMercator {
                    x: width as f64 * self.mercator_scale / 2.0,
                    y: height as f64 * self.mercator_scale / 2.0,
                };
            let mercator_offset = bounding_box.bottom_right - screen_top_left_coords;
            let screen_offset = (
                mercator_offset.x / self.mercator_scale,
                mercator_offset.y / self.mercator_scale,
            );
            debug!("{cell} {screen_offset:?} wide: {width_px}");
            ctx.draw_image_with_image_bitmap_and_dw_and_dh(
                &data.data,
                screen_offset.0,
                screen_offset.1,
                width_px,
                width_px,
            )
            .unwrap();
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
    cell: CellIndex,
    data: Pixmap,
}

#[derive(Debug)]
struct UploadedCell {
    cell: CellIndex,
    data: ImageBitmap,
}

async fn get_cell(db: &Database, cell: CellIndex) -> Vec<u8> {
    let tr = db
        .transaction(&["cells"], idb::TransactionMode::ReadOnly)
        .unwrap();
    let store = tr.object_store("cells").unwrap();
    let key = format!("{cell}.zan");
    let value = store.get(Query::Key(key.into())).await.unwrap().unwrap();
    BASE64_STANDARD_NO_PAD
        .decode(value.as_string().unwrap())
        .unwrap()
    // while res >= Resolution::Three {
    //     let cell = coord.to_cell(res);
    //     let key = format!("{cell}.zan");
    //     if let Some(o) = store.get(Query::Key(key.into())).await.unwrap() {
    //         let s = o.as_string().unwrap();
    //         return Some((cell, BASE64_STANDARD_NO_PAD.decode(s).unwrap()));
    //     }
    //     res = res.pred().unwrap();
    // }
    // None
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
    Rendered(Vec<UploadedCell>),
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
        // trigger the resize immediately
        recompose.emit(());

        App {
            view_center: helsinki.into(),
            canvas: NodeRef::default(),
            html_size: get_body_size(),
            animation_frame: request_animation_frame(move |_| recompose.emit(())),
            mercator_scale: 0.00001,
            downloaded_cells: vec![],
            pan_start: None,
            visible_cells: Default::default(),
            drawn_cells: Default::default(),
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
                self.compose_tiles(ctx.link().callback(|d| Msg::Rendered(d)));
                // spawn_local(self.compose_tiles());
                // self.animation_frame = request_animation_frame(move |_| recompose.emit(()));
            }
            Msg::Rendered(cells) => {
                self.drawn_cells
                    .extend(cells.into_iter().map(|c| (c.cell, DeferredCell::Done(c))));
                recompose.emit(());
            }
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
                        // FIXME: why is this minus?
                        self.view_center = PicMercator {
                            x: start_center.x - dx as f64 * self.mercator_scale,
                            y: start_center.y - dy as f64 * self.mercator_scale,
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

async fn pixmap_to_imagedata(DrawnCell { cell, data }: DrawnCell) -> UploadedCell {
    let future = window()
        .create_image_bitmap_with_image_data(
            &ImageData::new_with_u8_clamped_array(Clamped(&data.data()), data.width()).unwrap(),
        )
        .unwrap();
    let image_data: ImageBitmap = JsFuture::from(future).await.unwrap().into();

    UploadedCell {
        cell,
        data: image_data,
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
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    wasm_logger::init(wasm_logger::Config::new(log::Level::Info));
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
