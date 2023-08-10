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
use log::info;
use serde::Deserialize;
use tiny_skia::Pixmap;
use wasm_bindgen::{Clamped, JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;
use web_sys::{CanvasRenderingContext2d, CustomEvent, Event, HtmlCanvasElement, ImageData, ImageBitmap};
use yew::{html, Callback, Component, NodeRef};
use zana::{
    coords::{GeoCoord, PicMercator},
    draw_tile, CellIndex, Coord, LatLng, Mercator, Resolution, Transform,
};

struct App {
    window_center: PicMercator,
    canvas: NodeRef,
    html_size: (u32, u32),
    loaded_files: HashMap<String, Vec<u8>>,
    started: Instant,
    image_data: Option<ImageBitmap>,
    animation_frame: AnimationFrame,
}

impl App {
    async fn draw_selected_cell(coords: GeoCoord) -> Option<ImageBitmap> {
        let latlon = LatLng::from(coords.clone());
        let db = create_database().await.unwrap();
        if let Some((cell, data)) = find_cell(&db, latlon).await {
            Some(draw_cell(cell, &data).await)
        } else {
            info!("no cell found at {coords:?}");
            None
        }
    }

    fn compose_tiles(&mut self, time: Duration) {
        let Some(image_data) = &self.image_data else {return};
        let canvas: HtmlCanvasElement = self.canvas.cast().unwrap();
        let ctx: CanvasRenderingContext2d = canvas
            .get_context("2d")
            .unwrap()
            .unwrap()
            .dyn_into()
            .unwrap();
        let speed = 2.0;
        ctx.draw_image_with_image_bitmap(
            image_data,
            ((time.as_secs_f64() / speed).sin() + 1.0) * 100.0,
            ((time.as_secs_f64() / speed).cos() + 1.0) * 100.0,
        )
        .unwrap();
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

#[derive(Debug)]
enum Msg {
    Resized,
    DownloadedFiles,
    ReadFiles(HashMap<String, Vec<u8>>),
    Repaint,
    Recompose,
    GeoMoved(MovedEvent),
    Rendered(ImageBitmap),
    // PanStart((i32,i32)),
    // Pan((i32, i32)),
    // PanStop
}

fn get_body_size() -> (u32, u32) {
    let body = body();
    (
        body.client_width().try_into().unwrap(),
        body.client_height().try_into().unwrap(),
    )
}

/// this comes from JS
#[derive(Debug, Deserialize)]
struct MovedEvent {
    lat: f32,
    lon: f32,
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
            files_loaded_callback.emit(load_files_from_indexeddb().await);
        });

        let move_callback = ctx.link().callback(Msg::GeoMoved);
        let move_listener = EventListener::new(&document(), "geomove", move |e: &Event| {
            let custom_event: &CustomEvent = e.dyn_ref().unwrap();
            move_callback.emit(custom_event.detail().into_serde().unwrap())
        });
        move_listener.forget();

        let helsinki = GeoCoord::from_latlon(60.1684, 24.9438);

        // trigger a repaint on loading
        ctx.link().callback(|()| Msg::Repaint).emit(());

        let recompose = ctx.link().callback(|_| Msg::Recompose);

        App {
            window_center: helsinki.project(),
            loaded_files: HashMap::new(),
            canvas: NodeRef::default(),
            html_size: get_body_size(),
            started: Instant::now(),
            image_data: None,
            animation_frame: request_animation_frame(move |_| recompose.emit(())),
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
                    files_loaded_callback.emit(load_files_from_indexeddb().await);
                });
            }
            // wasm_bindgen_futures::spawn_local( load_downloaded_files(&db, ctx.link().callback(|f| Msg::ReadFiles(f))));
            Msg::ReadFiles(f) => {
                info!("Read {} files from db", f.len());
                self.loaded_files = f;
                if self.loaded_files.is_empty() {
                    download_files(ctx.link().callback(|_| Msg::DownloadedFiles));
                }
            }
            Msg::GeoMoved(_) => {
                // TODO
            }
            Msg::Repaint => {
                let cb = ctx.link().callback(Msg::Rendered);
                let coord = self.window_center.unproject();
                spawn_local(async move {
                    if let Some(d) = App::draw_selected_cell(coord).await {
                        cb.emit(d)
                    }
                });
            }
            Msg::Recompose => {
                self.compose_tiles(self.started.elapsed());
                self.animation_frame = request_animation_frame(move |_| recompose.emit(()));
            }
            Msg::Rendered(data) => {
                self.image_data = Some(data);
                recompose.emit(())
            }
        }
        true
    }

    fn view(&self, ctx: &yew::Context<Self>) -> yew::Html {
        let (width, height) = self.html_size;
        html! {
            <>
            <canvas
                ref={&self.canvas}
                width={width.to_string()}
                height={height.to_string()}
                onclick={ctx.link().callback(|_|Msg::Repaint)}
            ></canvas>
            </>
        }
    }
}

async fn draw_cell(cell: CellIndex, data: &[u8]) -> ImageBitmap {
    let boundary = cell.boundary();
    let proj = Mercator {};
    let projected_boundary = boundary.into_iter().map(|b| {
        proj.transform(&Coord {
            x: b.lng_radians(),
            y: -b.lat_radians(),
        })
    });

    let (min_x, max_x) = projected_boundary
        .clone()
        .map(|c| c.x)
        .minmax()
        .into_option()
        .unwrap();
    let (min_y, max_y) = projected_boundary
        .map(|c| c.y)
        .minmax()
        .into_option()
        .unwrap();

    let mut pixmap = Pixmap::new(1024, 1024).unwrap();

    draw_tile(&mut pixmap, data, (min_x, max_x, min_y, max_y));

    let future = window()
        .create_image_bitmap_with_image_data(
            &ImageData::new_with_u8_clamped_array(Clamped(&pixmap.data()), pixmap.width()).unwrap(),
        )
        .unwrap();
    wasm_bindgen_futures::JsFuture::from(future).await.unwrap().into()
}
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
            let cell = file.split_once(".").unwrap().0;

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

async fn load_files_from_indexeddb() -> HashMap<String, Vec<u8>> {
    let db = create_database().await.unwrap();

    let t = db
        .transaction(&["cells"], idb::TransactionMode::ReadOnly)
        .unwrap();
    let store = t.object_store("cells").unwrap();
    let keys: Vec<String> = store
        .get_all_keys(None, None)
        .await
        .unwrap()
        .into_iter()
        .take(10)
        .map(|v| v.as_string().unwrap())
        .collect();

    let mut values = vec![];

    for k in &keys {
        let val = store
            .get(Query::Key(JsValue::from(k)))
            .await
            .unwrap()
            .unwrap();
        let str: String = val.as_string().unwrap();
        let data = BASE64_STANDARD_NO_PAD.decode(str).unwrap();
        values.push(data)
    }

    keys.into_iter()
        .map(|f| f.split_once(".").unwrap().0.to_string())
        .zip(values)
        .collect()
}
