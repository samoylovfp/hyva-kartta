use egui::{plot::Plot, Ui};
use crate::zana;
use crate::zana::Path;

/// We derive Deserialize/Serialize so we can persist app state on shutdown.

pub struct TemplateApp {
    nodes: Vec<Path>,
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

        // Default::default()
        Self {
            nodes: zana::read_nodes_from_file(),
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
        let Self { nodes } = self;

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
                    ui.label(" and ");
                    ui.hyperlink_to("Julia", "https://juliabubnova.com");
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

    let beginning = nodes[0].points[0];

    let lines: Vec<_> = nodes
        .iter()
        .map(|p| {
            Line::new(PlotPoints::new(
                p.points
                    .iter()
                    .map(|(x, y)| [(beginning.0 - *x) as f64, (beginning.1 - *y) as f64])
                    .collect(),
            ))
        })
        .collect();

    // Line::new(line_points);
    Plot::new("example_plot")
        .data_aspect(1.0)
        .show(ui, |plot_ui| {
            lines.into_iter().for_each(|l| plot_ui.line(l))
        });
}
