use std::{cell::Cell, collections::HashMap, sync::Arc};

use egui::{epaint::ahash::HashSet, plot::Plot, Ui};
use osmpbfreader::{OsmObj, OsmPbfReader};
use wasm_bindgen_futures::spawn_local;

/// We derive Deserialize/Serialize so we can persist app state on shutdown.

pub struct TemplateApp {
    nodes: Vec<Path>,
    progress: Arc<Cell<f32>>,
}

impl TemplateApp {
    /// Called once before the first frame.
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // This is also where you can customize the look and feel of egui using
        // `cc.egui_ctx.set_visuals` and `cc.egui_ctx.set_fonts`.

        // // Load previous app state (if any).
        // // Note that you must enable the `persistence` feature for this to work.
        // if let Some(storage) = cc.storage {
        //     return eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default();
        // }

        let progress = Default::default();
        let p2 = Arc::clone(&progress);

        Self {
            nodes: read_nodes_from_file(),
            progress,
        }
    }
}

impl eframe::App for TemplateApp {
    /// Called by the frame work to save state before shutdown.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        // eframe::set_value(storage, eframe::APP_KEY, self);
    }

    /// Called each time the UI needs repainting, which may be many times per second.
    /// Put your widgets into a `SidePanel`, `TopPanel`, `CentralPanel`, `Window` or `Area`.
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let Self { nodes, progress } = self;

        // Examples of how to create different panels and windows.
        // Pick whichever suits you.
        // Tip: a good default choice is to just keep the `CentralPanel`.
        // For inspiration and more examples, go to https://emilk.github.io/egui

        #[cfg(not(target_arch = "wasm32"))] // no File->Quit on web pages!
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            // The top panel is often a good place for a menu bar:
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Quit").clicked() {
                        _frame.close();
                    }
                });
            });
        });

        egui::SidePanel::left("side_panel").show(ctx, |ui| {
            ui.heading("Map options");
            ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    ui.label("Created by ");
                    ui.hyperlink_to("Sorseg", "https://github.com/samoylovfp");
                    ui.label(" and ");
                    ui.hyperlink_to("Demoth", "https://demoth.dev");
                    ui.label(".");
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            // The central panel the region left after adding TopPanel's and SidePanel's
            draw_line(nodes, ui);
            egui::warn_if_debug_build(ui);
        });
    }
}

fn draw_line(nodes: &[Path], ui: &mut Ui) {
    use egui::plot::{Line, PlotPoints};
    // let n = 128;
    // let line_points: PlotPoints = (0..=n)
    //     .map(|i| {
    //         use std::f64::consts::TAU;
    //         let x = egui::remap(i as f64, 0.0..=n as f64, -TAU..=TAU);
    //         [x, x.sin()]
    //     })
    //     .collect();

    // let beninging = nodes[0].points[0];
    let beninging = (0, 0);

    let lines: Vec<_> = nodes
        .iter()
        .map(|p| {
            Line::new(PlotPoints::new(
                p.points
                    .iter()
                    .map(|(x, y)| [(beninging.0 - *x) as f64, (beninging.1 - *y) as f64])
                    .collect(),
            ))
        })
        .collect();

    // Line::new(line_points);
    egui::plot::Plot::new("example_plot")
        .data_aspect(1.0)
        .show(ui, |plot_ui| {
            lines.into_iter().for_each(|l| plot_ui.line(l))
        });
}

#[derive(Debug)]
struct Path {
    points: Vec<(i32, i32)>,
}

fn read_nodes_from_file() -> Vec<Path> {
    return vec![];
    let mut reader = OsmPbfReader::new(std::fs::File::open("uusima.pbf").unwrap());

    const ROADS: usize = 100000;
    let ways: Vec<_> = reader
        .iter()
        .filter_map(|o| o.ok())
        .filter(|o| o.tags().contains_key("highway"))
        .filter_map(|o| o.way().cloned())
        .filter(|w| !w.nodes.is_empty())
        .take(ROADS)
        .collect();

    let nodes_to_read: HashSet<_> = ways.iter().flat_map(|w| w.nodes.clone()).collect();

    reader.rewind().unwrap();

    let node_coordinates: HashMap<_, _> = reader
        .iter()
        .filter_map(|o| o.ok())
        .filter_map(|o| o.node().cloned())
        .filter(|n| nodes_to_read.contains(&n.id))
        .map(|n| (n.id, (-n.decimicro_lon, -n.decimicro_lat)))
        .collect();

    ways.iter()
        .map(|w| {
            let points = w
                .nodes
                .iter()
                .filter_map(|n| node_coordinates.get(n).cloned())
                .collect();
            Path { points }
        })
        .filter(|p| !p.points.is_empty())
        .collect()
}
