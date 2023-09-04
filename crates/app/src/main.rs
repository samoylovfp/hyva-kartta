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
            x: width as f64 * self.mercator_scale,
            y: height as f64 * self.mercator_scale,
        };

        let bbox = PicMercatorBoundingBox {
            top_left: self.view_center.clone() - quarter_screen.clone(),
            bottom_right: self.view_center.clone() + quarter_screen,
        };
        // FIXME: cells disappear too early
        let cells = filter_cells_with_mercator_rectangle(&self.downloaded_cells, bbox);
        ctx.clear_rect(0.0, 0.0, width as f64, height as f64);
        let cells_to_draw = cells
            .clone()
            .into_iter()
            .filter(|c| !self.drawn_cells.contains_key(c))
            .take(20)
            .collect_vec();

        if !cells_to_draw.is_empty() {
            self.drawn_cells.extend(
                cells_to_draw
                    .iter()
                    .copied()
                    .map(|c| (c, DeferredCell::Waiting)),
            );
            let scale = self.mercator_scale;
            spawn_local(async move {
                let mut results = vec![];
                let start = Instant::now();
                let cells_count = cells_to_draw.len();
                info!("Drawing {cells_count} cells...");
                let db = create_database().await.unwrap();
                for cell in cells_to_draw {
                    let bbox = cell_to_bounding_box(cell);
                    let (width, height) = bbox.sizes(scale);

                    let Some(mut pixmap) = Pixmap::new(width as u32, height as u32) else {continue};
                    let data = get_cell(&db, cell).await;

                    // FIXME: very slow, take a look at lyon
                    // https://github.com/nical/lyon/tree/master/examples/wgpu
                    draw_tile(&mut pixmap, data.as_slice(), bbox);
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
            let screen_top_left_coords = self.view_center.clone()
                - PicMercator {
                    x: width as f64 * self.mercator_scale / 2.0,
                    y: height as f64 * self.mercator_scale / 2.0,
                };
            let mercator_offset = bounding_box.bottom_right.clone() - screen_top_left_coords;
            let screen_offset = (
                mercator_offset.x / self.mercator_scale,
                mercator_offset.y / self.mercator_scale,
            );
            let (width, height) = bounding_box.sizes(self.mercator_scale);
            ctx.draw_image_with_image_bitmap_and_dw_and_dh(
                &data.data,
                screen_offset.0,
                screen_offset.1,
                width,
                height,
            )
            .unwrap();
        }
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
    let dpr = window().device_pixel_ratio();
    let body = body();
    (
        (body.client_width() as f64 * dpr) as u32,
        (body.client_height() as f64 * dpr) as u32,
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
        let berlin = GeoCoord::from_latlon(52.5100, 13.4031);
        let start = if false { helsinki } else { berlin };

        let recompose = ctx.link().callback(|_| Msg::Recompose);
        // Trigger redraw immediately
        ctx.link().send_message(Msg::Resized);

        App {
            view_center: start.into(),
            canvas: NodeRef::default(),
            html_size: get_body_size(),
            animation_frame: request_animation_frame(move |_| recompose.emit(())),
            mercator_scale: 0.000001,
            downloaded_cells: vec![],
            pan_start: None,
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
                recompose.emit(());
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
                let dpr = window().device_pixel_ratio();
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
                            x: start_center.x - dx as f64 * self.mercator_scale * dpr,
                            y: start_center.y - dy as f64 * self.mercator_scale * dpr,
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
