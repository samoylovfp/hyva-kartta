use std::collections::HashMap;

use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};
use gloo::{
    events::EventListener,
    utils::{body, format::JsValueSerdeExt, window},
};
use hyka::db::create_database;
use idb::{Database, Query};
use itertools::Itertools;
use log::info;
use tiny_skia::{Color, Pixmap};
use wasm_bindgen::{Clamped, JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;
use web_sys::{CanvasRenderingContext2d, HtmlCanvasElement, ImageData};
use yew::{html, Callback, Component, NodeRef};
use zana::{draw_tile, CellIndex, Coord, Mercator, Transform};

struct App {
    x: f32,
    y: f32,
    canvas: NodeRef,
    html_size: (u32, u32),
    downloaded_files: HashMap<String, Vec<u8>>,
    _resize_listener: EventListener,
    rendering_tile: usize,
}

impl App {
    fn draw_selected_cell(&self) {
        if let Some((cell, data)) = self.downloaded_files.iter().nth(self.rendering_tile) {
            draw_cell(cell, data, &self.canvas)
        }
    }
}

#[derive(Debug)]
enum Msg {
    Resized,
    DownloadedFile(String, Vec<u8>),
    ReadFiles(HashMap<String, Vec<u8>>),
    Next,
}

fn get_body_size() -> (u32, u32) {
    let body = body();
    (
        body.client_width().try_into().unwrap(),
        body.client_height().try_into().unwrap(),
    )
}

impl Component for App {
    type Message = Msg;

    type Properties = ();

    fn create(ctx: &yew::Context<Self>) -> Self {
        let event_emitter = ctx.link().callback(|_| Msg::Resized);
        let resize_listener =
            EventListener::new(&window(), "resize", move |_| event_emitter.emit(()));

        spawn_local(load_filed_from_indexeddb(
            ctx.link().callback(|d| Msg::ReadFiles(d)),
        ));

        App {
            x: 10.0,
            y: 10.0,
            downloaded_files: HashMap::new(),
            canvas: NodeRef::default(),
            html_size: get_body_size(),
            _resize_listener: resize_listener,
            rendering_tile: 0,
        }
    }

    fn update(&mut self, ctx: &yew::Context<Self>, msg: Self::Message) -> bool {
        info!("{:?}", std::mem::discriminant(&msg));
        match msg {
            Msg::Resized => {
                self.html_size = get_body_size();
                // FIXME: this should happen after "rendered"
                self.draw_selected_cell();
            }
            Msg::DownloadedFile(file, contents) => {
                self.downloaded_files.insert(file.clone(), contents.clone());
            }
            // wasm_bindgen_futures::spawn_local( load_downloaded_files(&db, ctx.link().callback(|f| Msg::ReadFiles(f))));
            Msg::ReadFiles(f) => {
                info!("Read {} files from db", f.len());
                self.downloaded_files = f;
                if self.downloaded_files.is_empty() {
                    download_files(
                        ctx.link()
                            .callback(|(file, data)| Msg::DownloadedFile(file, data)),
                    );
                } else {
                    self.draw_selected_cell();
                }
            }
            Msg::Next => {
                self.rendering_tile += 1;
                if self.rendering_tile > self.downloaded_files.len().saturating_sub(1) {
                    self.rendering_tile = 0;
                }
                self.draw_selected_cell();
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
                onclick={ctx.link().callback(|_|Msg::Next)}
            ></canvas>
            </>
        }
    }
}

fn draw_cell(cell: &str, data: &[u8], canvas: &NodeRef) {
    let cell: CellIndex = cell.parse().unwrap();
    let boundary = cell.boundary();
    let proj = Mercator {};
    let projected_boundary = boundary.into_iter().map(|b| {
        proj.transform(&Coord {
            x: b.lng_radians(),
            y: -b.lat_radians(),
        })
    });
    info!("Boundary is {:?}", projected_boundary.clone().collect_vec());
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
    let (x, y) = get_body_size();

    let mut pixmap = Pixmap::new(x, y).unwrap();

    draw_tile(&mut pixmap, data, (min_x, max_x, min_y, max_y));

    let image_data =
        ImageData::new_with_u8_clamped_array(Clamped(&pixmap.data()), pixmap.width()).unwrap();
    let canvas: HtmlCanvasElement = canvas.cast().unwrap();
    let ctx: CanvasRenderingContext2d = canvas
        .get_context("2d")
        .unwrap()
        .unwrap()
        .dyn_into()
        .unwrap();

    ctx.put_image_data(&image_data, 0.0, 0.0).unwrap();
}
fn main() {
    wasm_logger::init(wasm_logger::Config::default());
    yew::Renderer::<App>::new().render();
}

fn download_files(download_result_callback: Callback<(String, Vec<u8>)>) {
    wasm_bindgen_futures::spawn_local(async move {
        let list: Vec<String> = gloo::net::http::Request::get("/api/list")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let db = create_database().await.unwrap();
        for file in &list[0..50] {
            let cb = download_result_callback.clone();
            let cell = file.split_once(".").unwrap().0;

            let data = gloo::net::http::Request::get(&format!("/api/get/{file}"))
                .send()
                .await
                .unwrap()
                .binary()
                .await
                .unwrap();
            cb.emit((cell.to_string(), data.clone()));
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
    })
}

async fn load_filed_from_indexeddb(cb: Callback<HashMap<String, Vec<u8>>>) {
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
        .map(|v| v.into_serde().unwrap())
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

    cb.emit(
        keys.into_iter()
            .map(|f| f.split_once(".").unwrap().0.to_string())
            .zip(values)
            .collect(),
    )
}
