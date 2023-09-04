use h3o::CellIndex;
use tiny_skia::{Pixmap, Color};
use zana::draw_hex;

fn main() {
    let mut pixmap = Pixmap::new(1024,1024).unwrap();
    pixmap.fill(Color::BLACK);
    draw_hex(CellIndex::first(h3o::Resolution::Fifteen), &mut pixmap, 10.0);
    pixmap.save_png("hex.png").unwrap();
}