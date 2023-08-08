use gloo::utils::body;
use yew::{html, Component, NodeRef};
use zana::read_zana_data;

struct App {
    x: f32,
    y: f32,
    canvas: NodeRef,
    html_size: (u32, u32),
}

impl Component for App {
    type Message = ();

    type Properties = ();

    fn create(ctx: &yew::Context<Self>) -> Self {
        let body = body();
        App {
            x: 10.0,
            y: 10.0,
            canvas: NodeRef::default(),
            html_size: (
                body.client_width().try_into().unwrap(),
                body.client_height().try_into().unwrap(),
            ),
        }
    }

    fn update(&mut self, ctx: &yew::Context<Self>, msg: Self::Message) -> bool {
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
            ></canvas>
            </>
        }
    }
}
fn main() {
    yew::Renderer::<App>::new().render();
}
