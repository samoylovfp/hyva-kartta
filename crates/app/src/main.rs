use yew::{html, Component};
use zana::read_zana_data;

struct App {
    x: f32,
    y: f32,
}

impl Component for App {
    type Message = ();

    type Properties = ();

    fn create(ctx: &yew::Context<Self>) -> Self {
        App { x: 10.0, y: 10.0 }
    }

    fn view(&self, ctx: &yew::Context<Self>) -> yew::Html {
        html! {
            <>
            <canvas id="map"></canvas>
            </>
        }
    }
}
fn main() {
    yew::Renderer::<App>::new().render();
}
