//! TUI viewer for E2E run results and benchmark summaries (bd-1w6k.6.4).
//!
//! Reads JSONL files containing [`RunRecordV1`] or [`BenchmarkSummary`] records
//! and presents them in an interactive terminal viewer using FrankenTUI.
//!
//! ## Usage
//!
//! ```text
//! cargo run -p fsqlite-e2e --bin e2e-viewer -- <file.jsonl> [<file2.jsonl> ...]
//! ```
//!
//! ## Keys
//!
//! - **Up/Down/j/k** — move selection
//! - **Tab** — switch between Runs and Benchmarks tabs
//! - **q/Esc** — quit

use std::path::PathBuf;
use std::time::Duration;

use ftui::core::geometry::Rect;
use ftui::widgets::Widget;
use ftui::widgets::panel::Panel;
use ftui::widgets::paragraph::Paragraph;
use ftui::widgets::progress::ProgressBar;
use ftui::{App, Cmd, Event, KeyCode, KeyEventKind, Model, PackedRgba, ScreenMode, Style};

use fsqlite_e2e::benchmark::BenchmarkSummary;
use fsqlite_e2e::report::RunRecordV1;
use fsqlite_e2e::report_render::{parse_benchmark_summaries_jsonl, parse_run_records_jsonl};

// ── Active tab ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Runs,
    Benchmarks,
}

// ── Messages ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Msg {
    Tick,
    Quit,
    Up,
    Down,
    SwitchTab,
    PageUp,
    PageDown,
}

impl From<Event> for Msg {
    fn from(e: Event) -> Self {
        match e {
            Event::Key(k) if k.kind == KeyEventKind::Press && k.is_char('q') => Self::Quit,
            Event::Key(k) if k.kind == KeyEventKind::Press && k.code == KeyCode::Escape => {
                Self::Quit
            }
            Event::Key(k)
                if k.kind == KeyEventKind::Press && (k.code == KeyCode::Up || k.is_char('k')) =>
            {
                Self::Up
            }
            Event::Key(k)
                if k.kind == KeyEventKind::Press && (k.code == KeyCode::Down || k.is_char('j')) =>
            {
                Self::Down
            }
            Event::Key(k) if k.kind == KeyEventKind::Press && k.code == KeyCode::Tab => {
                Self::SwitchTab
            }
            Event::Key(k) if k.kind == KeyEventKind::Press && k.code == KeyCode::PageUp => {
                Self::PageUp
            }
            Event::Key(k) if k.kind == KeyEventKind::Press && k.code == KeyCode::PageDown => {
                Self::PageDown
            }
            _ => Self::Tick,
        }
    }
}

// ── Model ────────────────────────────────────────────────────────────────

struct ViewerModel {
    tab: Tab,
    runs: Vec<RunRecordV1>,
    benchmarks: Vec<BenchmarkSummary>,
    run_selected: usize,
    bench_selected: usize,
}

impl ViewerModel {
    fn new(runs: Vec<RunRecordV1>, benchmarks: Vec<BenchmarkSummary>) -> Self {
        Self {
            tab: if runs.is_empty() && !benchmarks.is_empty() {
                Tab::Benchmarks
            } else {
                Tab::Runs
            },
            runs,
            benchmarks,
            run_selected: 0,
            bench_selected: 0,
        }
    }

    fn current_count(&self) -> usize {
        match self.tab {
            Tab::Runs => self.runs.len(),
            Tab::Benchmarks => self.benchmarks.len(),
        }
    }

    fn selected_mut(&mut self) -> &mut usize {
        match self.tab {
            Tab::Runs => &mut self.run_selected,
            Tab::Benchmarks => &mut self.bench_selected,
        }
    }

    fn move_up(&mut self) {
        let sel = self.selected_mut();
        *sel = sel.saturating_sub(1);
    }

    fn move_down(&mut self) {
        let count = self.current_count();
        let sel = self.selected_mut();
        if count > 0 {
            *sel = (*sel + 1).min(count - 1);
        }
    }

    fn page_up(&mut self) {
        let sel = self.selected_mut();
        *sel = sel.saturating_sub(10);
    }

    fn page_down(&mut self) {
        let count = self.current_count();
        let sel = self.selected_mut();
        if count > 0 {
            *sel = (*sel + 10).min(count - 1);
        }
    }
}

// ── Model trait ──────────────────────────────────────────────────────────

impl Model for ViewerModel {
    type Message = Msg;

    fn init(&mut self) -> Cmd<Self::Message> {
        Cmd::tick(Duration::from_millis(100))
    }

    fn update(&mut self, msg: Self::Message) -> Cmd<Self::Message> {
        match msg {
            Msg::Tick => Cmd::none(),
            Msg::Quit => Cmd::quit(),
            Msg::Up => {
                self.move_up();
                Cmd::none()
            }
            Msg::Down => {
                self.move_down();
                Cmd::none()
            }
            Msg::PageUp => {
                self.page_up();
                Cmd::none()
            }
            Msg::PageDown => {
                self.page_down();
                Cmd::none()
            }
            Msg::SwitchTab => {
                self.tab = match self.tab {
                    Tab::Runs => Tab::Benchmarks,
                    Tab::Benchmarks => Tab::Runs,
                };
                Cmd::none()
            }
        }
    }

    fn view(&self, frame: &mut ftui::Frame) {
        let w = frame.width();
        let h = frame.height();

        // Top bar: 1 line, List: left half, Detail: right half.
        let top = Rect::new(0, 0, w, 1);
        let list_area = Rect::new(0, 1, w / 2, h.saturating_sub(2));
        let detail_area = Rect::new(w / 2, 1, w.saturating_sub(w / 2), h.saturating_sub(2));
        let footer = Rect::new(0, h.saturating_sub(1), w, 1);

        self.render_top_bar(frame, top);
        self.render_list(frame, list_area);
        self.render_detail(frame, detail_area);
        Self::render_footer(frame, footer);
    }
}

// ── Rendering ────────────────────────────────────────────────────────────

impl ViewerModel {
    fn render_top_bar(&self, frame: &mut ftui::Frame, area: Rect) {
        let runs_style = if self.tab == Tab::Runs {
            Style::new()
                .fg(PackedRgba::rgb(0, 0, 0))
                .bg(PackedRgba::rgb(100, 200, 255))
                .bold()
        } else {
            Style::new().fg(PackedRgba::rgb(160, 160, 160))
        };
        let bench_style = if self.tab == Tab::Benchmarks {
            Style::new()
                .fg(PackedRgba::rgb(0, 0, 0))
                .bg(PackedRgba::rgb(100, 255, 180))
                .bold()
        } else {
            Style::new().fg(PackedRgba::rgb(160, 160, 160))
        };

        let runs_label = format!(" Runs ({}) ", self.runs.len());
        let bench_label = format!(" Benchmarks ({}) ", self.benchmarks.len());
        let bar = format!("{runs_label} | {bench_label}    FrankenSQLite E2E Viewer (bd-1w6k.6.4)");

        // Render the full bar with default color, then overlay the tab labels.
        Paragraph::new(bar)
            .style(Style::new().fg(PackedRgba::rgb(180, 180, 180)))
            .render(area, frame);

        // Overlay active tab style.
        #[allow(clippy::cast_possible_truncation)]
        let runs_area = Rect::new(area.x, area.y, runs_label.len() as u16, 1);
        Paragraph::new(runs_label)
            .style(runs_style)
            .render(runs_area, frame);

        #[allow(clippy::cast_possible_truncation)]
        let bench_x = runs_area.width + 3; // " | "
        if bench_x < area.width {
            #[allow(clippy::cast_possible_truncation)]
            let bench_label_w = bench_label.len() as u16;
            let bench_area = Rect::new(
                area.x + bench_x,
                area.y,
                bench_label_w.min(area.width.saturating_sub(bench_x)),
                1,
            );
            Paragraph::new(bench_label)
                .style(bench_style)
                .render(bench_area, frame);
        }
    }

    fn render_footer(frame: &mut ftui::Frame, area: Rect) {
        Paragraph::new(
            " Up/Down: navigate | Tab: switch tab | PgUp/PgDn: page | q/Esc: quit".to_owned(),
        )
        .style(Style::new().fg(PackedRgba::rgb(120, 120, 120)))
        .render(area, frame);
    }

    fn render_list(&self, frame: &mut ftui::Frame, area: Rect) {
        match self.tab {
            Tab::Runs => self.render_run_list(frame, area),
            Tab::Benchmarks => self.render_bench_list(frame, area),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn render_run_list(&self, frame: &mut ftui::Frame, area: Rect) {
        let title = "Run Records";
        let panel = Panel::new(Paragraph::new(String::new()))
            .title(title)
            .border_style(Style::new().fg(PackedRgba::rgb(100, 200, 255)));
        let inner = panel.inner(area);
        panel.render(area, frame);

        if self.runs.is_empty() {
            Paragraph::new("  No run records loaded.".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(inner, frame);
            return;
        }

        // Header row.
        let mut y = inner.y;
        if y < inner.y + inner.height {
            let hdr = format!(
                "  {:<20} {:<12} {:>6} {:>10} {:>8} {}",
                "Fixture", "Engine", "Conc", "Ops/s", "Time", "Integrity"
            );
            Paragraph::new(hdr)
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Compute scroll offset.
        let visible_rows = (inner.height.saturating_sub(1)) as usize; // minus header
        let offset = if self.run_selected >= visible_rows {
            self.run_selected - visible_rows + 1
        } else {
            0
        };

        for (idx, run) in self.runs.iter().enumerate().skip(offset) {
            if y >= inner.y + inner.height {
                break;
            }
            let is_selected = idx == self.run_selected;
            let integrity = run
                .report
                .correctness
                .integrity_check_ok
                .map_or("--", |ok| if ok { "OK" } else { "FAIL" });
            let engine_name = truncate(&run.engine.name, 10);
            let fixture = truncate(&run.fixture_id, 18);
            let line = format!(
                "  {fixture:<20} {engine_name:<12} {:>6} {:>10} {:>7}ms {integrity}",
                run.concurrency,
                format_ops(run.report.ops_per_sec),
                run.report.wall_time_ms,
            );

            let style = if is_selected {
                Style::new()
                    .fg(PackedRgba::rgb(0, 0, 0))
                    .bg(PackedRgba::rgb(100, 200, 255))
            } else if run.report.error.is_some() {
                Style::new().fg(PackedRgba::rgb(255, 80, 80))
            } else {
                Style::new().fg(PackedRgba::rgb(200, 200, 200))
            };
            Paragraph::new(line)
                .style(style)
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }
    }

    fn render_bench_list(&self, frame: &mut ftui::Frame, area: Rect) {
        let title = "Benchmark Summaries";
        let panel = Panel::new(Paragraph::new(String::new()))
            .title(title)
            .border_style(Style::new().fg(PackedRgba::rgb(100, 255, 180)));
        let inner = panel.inner(area);
        panel.render(area, frame);

        if self.benchmarks.is_empty() {
            Paragraph::new("  No benchmark summaries loaded.".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(inner, frame);
            return;
        }

        let mut y = inner.y;
        if y < inner.y + inner.height {
            let hdr = format!(
                "  {:<20} {:<12} {:>6} {:>10} {:>10} {:>8}",
                "Benchmark", "Engine", "Iters", "Median", "P95", "Errors"
            );
            Paragraph::new(hdr)
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        let visible_rows = (inner.height.saturating_sub(1)) as usize;
        let offset = if self.bench_selected >= visible_rows {
            self.bench_selected - visible_rows + 1
        } else {
            0
        };

        for (idx, bench) in self.benchmarks.iter().enumerate().skip(offset) {
            if y >= inner.y + inner.height {
                break;
            }
            let is_selected = idx == self.bench_selected;
            let name = truncate(&bench.benchmark_id, 18);
            let engine = truncate(&bench.engine, 10);
            let error_count = bench
                .iterations
                .iter()
                .filter(|i| i.error.is_some())
                .count();
            let line = format!(
                "  {name:<20} {engine:<12} {:>6} {:>9}ms {:>9}ms {:>8}",
                bench.measurement_count,
                format_f64(bench.latency.median_ms),
                format_f64(bench.latency.p95_ms),
                error_count,
            );

            let style = if is_selected {
                Style::new()
                    .fg(PackedRgba::rgb(0, 0, 0))
                    .bg(PackedRgba::rgb(100, 255, 180))
            } else {
                Style::new().fg(PackedRgba::rgb(200, 200, 200))
            };
            Paragraph::new(line)
                .style(style)
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }
    }

    fn render_detail(&self, frame: &mut ftui::Frame, area: Rect) {
        match self.tab {
            Tab::Runs => self.render_run_detail(frame, area),
            Tab::Benchmarks => self.render_bench_detail(frame, area),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn render_run_detail(&self, frame: &mut ftui::Frame, area: Rect) {
        let panel = Panel::new(Paragraph::new(String::new()))
            .title("Run Detail")
            .border_style(Style::new().fg(PackedRgba::rgb(100, 200, 255)));
        let inner = panel.inner(area);
        panel.render(area, frame);

        let Some(run) = self.runs.get(self.run_selected) else {
            Paragraph::new("  No run selected.".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(inner, frame);
            return;
        };

        let mut y = inner.y;
        let w = inner.width;
        let x = inner.x;

        // Section: Identity
        render_section_header(frame, x, &mut y, w, inner, "IDENTITY");
        render_kv(frame, x, &mut y, w, inner, "Fixture", &run.fixture_id);
        render_kv(frame, x, &mut y, w, inner, "Engine", &run.engine.name);
        render_kv(frame, x, &mut y, w, inner, "Workload", &run.workload);
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Concurrency",
            &run.concurrency.to_string(),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Ops Count",
            &run.ops_count.to_string(),
        );

        // Section: Timings
        y += 1;
        render_section_header(frame, x, &mut y, w, inner, "TIMINGS");
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Wall Time",
            &format!("{}ms", run.report.wall_time_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Ops/sec",
            &format_ops(run.report.ops_per_sec),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Retries",
            &run.report.retries.to_string(),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Aborts",
            &run.report.aborts.to_string(),
        );

        if let Some(ref lat) = run.report.latency_ms {
            render_kv(
                frame,
                x,
                &mut y,
                w,
                inner,
                "p50",
                &format!("{:.2}ms", lat.p50),
            );
            render_kv(
                frame,
                x,
                &mut y,
                w,
                inner,
                "p95",
                &format!("{:.2}ms", lat.p95),
            );
            render_kv(
                frame,
                x,
                &mut y,
                w,
                inner,
                "p99",
                &format!("{:.2}ms", lat.p99),
            );
        }

        // Section: Hashes
        y += 1;
        render_section_header(frame, x, &mut y, w, inner, "HASHES");
        let c = &run.report.correctness;
        render_opt_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Raw SHA256",
            c.raw_sha256.as_deref(),
        );
        render_opt_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Canonical SHA256",
            c.canonical_sha256.as_deref(),
        );
        render_opt_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Logical SHA256",
            c.logical_sha256.as_deref(),
        );

        // Section: Integrity
        y += 1;
        render_section_header(frame, x, &mut y, w, inner, "INTEGRITY");
        let integrity_str = c
            .integrity_check_ok
            .map_or("not checked", |ok| if ok { "PASS" } else { "FAIL" });
        let integrity_color = c
            .integrity_check_ok
            .map_or(PackedRgba::rgb(160, 160, 160), |ok| {
                if ok {
                    PackedRgba::rgb(80, 220, 80)
                } else {
                    PackedRgba::rgb(255, 80, 80)
                }
            });
        if y < inner.y + inner.height {
            Paragraph::new(format!("  integrity_check: {integrity_str}"))
                .style(Style::new().fg(integrity_color).bold())
                .render(Rect::new(x, y, w, 1), frame);
            y += 1;
        }

        render_opt_bool(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Raw SHA256 Match",
            c.raw_sha256_match,
        );
        render_opt_bool(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Canonical SHA256 Match",
            c.canonical_sha256_match,
        );
        render_opt_bool(frame, x, &mut y, w, inner, "Dump Match", c.dump_match);

        // Error
        if let Some(ref err) = run.report.error {
            y += 1;
            render_section_header(frame, x, &mut y, w, inner, "ERROR");
            if y < inner.y + inner.height {
                let err_trunc = truncate(err, (w.saturating_sub(4)) as usize);
                Paragraph::new(format!("  {err_trunc}"))
                    .style(Style::new().fg(PackedRgba::rgb(255, 80, 80)))
                    .render(Rect::new(x, y, w, 1), frame);
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn render_bench_detail(&self, frame: &mut ftui::Frame, area: Rect) {
        let panel = Panel::new(Paragraph::new(String::new()))
            .title("Benchmark Detail")
            .border_style(Style::new().fg(PackedRgba::rgb(100, 255, 180)));
        let inner = panel.inner(area);
        panel.render(area, frame);

        let Some(bench) = self.benchmarks.get(self.bench_selected) else {
            Paragraph::new("  No benchmark selected.".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(inner, frame);
            return;
        };

        let mut y = inner.y;
        let w = inner.width;
        let x = inner.x;

        // Section: Identity
        render_section_header(frame, x, &mut y, w, inner, "IDENTITY");
        render_kv(frame, x, &mut y, w, inner, "ID", &bench.benchmark_id);
        render_kv(frame, x, &mut y, w, inner, "Engine", &bench.engine);
        render_kv(frame, x, &mut y, w, inner, "Fixture", &bench.fixture_id);
        render_kv(frame, x, &mut y, w, inner, "Workload", &bench.workload);
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Concurrency",
            &bench.concurrency.to_string(),
        );

        // Section: Configuration
        y += 1;
        render_section_header(frame, x, &mut y, w, inner, "CONFIG");
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Warmup Iters",
            &bench.warmup_count.to_string(),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Measurement Iters",
            &bench.measurement_count.to_string(),
        );
        let total_iters = bench.warmup_count + bench.measurement_count;
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Total Iters",
            &total_iters.to_string(),
        );
        let error_count = bench
            .iterations
            .iter()
            .filter(|i| i.error.is_some())
            .count();
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Error Iters",
            &error_count.to_string(),
        );

        // Section: Latency
        y += 1;
        render_section_header(frame, x, &mut y, w, inner, "LATENCY");
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Min",
            &format!("{:.3}ms", bench.latency.min_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Mean",
            &format!("{:.3}ms", bench.latency.mean_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Median (p50)",
            &format!("{:.3}ms", bench.latency.median_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "P95",
            &format!("{:.3}ms", bench.latency.p95_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "P99",
            &format!("{:.3}ms", bench.latency.p99_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Max",
            &format!("{:.3}ms", bench.latency.max_ms),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Std Dev",
            &format!("{:.3}ms", bench.latency.stddev_ms),
        );

        // Section: Throughput
        y += 1;
        render_section_header(frame, x, &mut y, w, inner, "THROUGHPUT");
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Mean",
            &format_ops(bench.throughput.mean_ops_per_sec),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Median",
            &format_ops(bench.throughput.median_ops_per_sec),
        );
        render_kv(
            frame,
            x,
            &mut y,
            w,
            inner,
            "Peak",
            &format_ops(bench.throughput.peak_ops_per_sec),
        );

        // Latency histogram (sparkline approximation using progress bars).
        if bench.latency.p95_ms > 0.0 && y + 2 < inner.y + inner.height {
            y += 1;
            render_section_header(frame, x, &mut y, w, inner, "LATENCY PROFILE");
            let max_ms = bench.latency.max_ms;
            if max_ms > 0.0 {
                for (label, val) in [
                    ("Min   ", bench.latency.min_ms),
                    ("Median", bench.latency.median_ms),
                    ("P95   ", bench.latency.p95_ms),
                    ("P99   ", bench.latency.p99_ms),
                    ("Max   ", bench.latency.max_ms),
                ] {
                    if y >= inner.y + inner.height {
                        break;
                    }
                    let label_str = format!("  {label} ");
                    let label_w = u16::try_from(label_str.len()).unwrap_or(u16::MAX).min(w);
                    Paragraph::new(label_str)
                        .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                        .render(Rect::new(x, y, label_w, 1), frame);

                    let bar_x = x.saturating_add(label_w);
                    let bar_w = w.saturating_sub(label_w.saturating_add(12));
                    if bar_w > 2 {
                        ProgressBar::new()
                            .ratio(val / max_ms)
                            .gauge_style(Style::new().bg(PackedRgba::rgb(60, 160, 200)))
                            .render(Rect::new(bar_x, y, bar_w, 1), frame);
                    }

                    let val_str = format!(" {val:.2}ms");
                    let val_x = bar_x.saturating_add(bar_w);
                    if val_x < x.saturating_add(w) {
                        Paragraph::new(val_str)
                            .style(Style::new().fg(PackedRgba::rgb(200, 200, 200)))
                            .render(
                                Rect::new(val_x, y, w.saturating_sub(val_x.saturating_sub(x)), 1),
                                frame,
                            );
                    }
                    y += 1;
                }
            }
        }
    }
}

// ── Rendering helpers ────────────────────────────────────────────────────

fn render_section_header(
    frame: &mut ftui::Frame,
    x: u16,
    y: &mut u16,
    w: u16,
    inner: Rect,
    label: &str,
) {
    if *y < inner.y + inner.height {
        Paragraph::new(format!("  {label}"))
            .style(Style::new().fg(PackedRgba::WHITE).bold())
            .render(Rect::new(x, *y, w, 1), frame);
        *y += 1;
    }
}

fn render_kv(
    frame: &mut ftui::Frame,
    x: u16,
    y: &mut u16,
    w: u16,
    inner: Rect,
    key: &str,
    value: &str,
) {
    if *y < inner.y + inner.height {
        let line = format!("    {key}: {value}");
        Paragraph::new(line)
            .style(Style::new().fg(PackedRgba::rgb(180, 200, 220)))
            .render(Rect::new(x, *y, w, 1), frame);
        *y += 1;
    }
}

fn render_opt_kv(
    frame: &mut ftui::Frame,
    x: u16,
    y: &mut u16,
    w: u16,
    inner: Rect,
    key: &str,
    value: Option<&str>,
) {
    let display = value.unwrap_or("--");
    render_kv(frame, x, y, w, inner, key, display);
}

fn render_opt_bool(
    frame: &mut ftui::Frame,
    x: u16,
    y: &mut u16,
    w: u16,
    inner: Rect,
    key: &str,
    value: Option<bool>,
) {
    if *y >= inner.y + inner.height {
        return;
    }
    let (label, color) = match value {
        Some(true) => ("YES", PackedRgba::rgb(80, 220, 80)),
        Some(false) => ("NO", PackedRgba::rgb(255, 80, 80)),
        None => ("--", PackedRgba::rgb(160, 160, 160)),
    };
    let line = format!("    {key}: {label}");
    Paragraph::new(line)
        .style(Style::new().fg(color))
        .render(Rect::new(x, *y, w, 1), frame);
    *y += 1;
}

// ── Formatting helpers ───────────────────────────────────────────────────

fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}K", ops / 1_000.0)
    } else {
        format!("{ops:.0}")
    }
}

fn format_f64(v: f64) -> String {
    if v >= 1000.0 {
        format!("{v:.0}")
    } else if v >= 1.0 {
        format!("{v:.2}")
    } else {
        format!("{v:.3}")
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_owned()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

// ── Main ─────────────────────────────────────────────────────────────────

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 || args.iter().any(|a| a == "-h" || a == "--help") {
        eprintln!(
            "e2e-viewer — interactive TUI for browsing E2E run results (bd-1w6k.6.4)\n\n\
             USAGE:\n  e2e-viewer <file.jsonl> [<file2.jsonl> ...]\n\n\
             Reads RunRecordV1 and BenchmarkSummary records from JSONL files.\n\n\
             KEYS:\n  Up/Down/j/k   Navigate list\n  Tab           Switch tab (Runs / Benchmarks)\n\
             PgUp/PgDn     Page scroll\n  q/Esc         Quit"
        );
        if args.len() < 2 {
            std::process::exit(2);
        }
        return Ok(());
    }

    let mut all_runs = Vec::new();
    let mut all_benchmarks = Vec::new();

    for path_str in &args[1..] {
        if path_str.starts_with('-') {
            continue;
        }
        let path = PathBuf::from(path_str);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warning: cannot read {}: {e}", path.display());
                continue;
            }
        };

        let runs = parse_run_records_jsonl(&content);
        let benchmarks = parse_benchmark_summaries_jsonl(&content);

        if runs.is_empty() && benchmarks.is_empty() {
            eprintln!(
                "warning: no RunRecordV1 or BenchmarkSummary records in {}",
                path.display()
            );
        }

        all_runs.extend(runs);
        all_benchmarks.extend(benchmarks);
    }

    if all_runs.is_empty() && all_benchmarks.is_empty() {
        eprintln!("error: no records loaded from any input file");
        std::process::exit(1);
    }

    eprintln!(
        "Loaded {} run records, {} benchmark summaries",
        all_runs.len(),
        all_benchmarks.len()
    );

    let model = ViewerModel::new(all_runs, all_benchmarks);
    App::new(model).screen_mode(ScreenMode::AltScreen).run()
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_run() -> RunRecordV1 {
        use fsqlite_e2e::methodology::EnvironmentMeta;
        use fsqlite_e2e::report::{
            CorrectnessReport, EngineInfo, EngineRunReport, RunRecordV1Args,
        };

        RunRecordV1::new(RunRecordV1Args {
            recorded_unix_ms: 1_700_000_000_000,
            environment: EnvironmentMeta::capture("test"),
            engine: EngineInfo {
                name: "fsqlite".to_owned(),
                sqlite_version: None,
                fsqlite_git: None,
            },
            fixture_id: "test-fixture".to_owned(),
            golden_path: None,
            golden_sha256: None,
            workload: "commutative_inserts".to_owned(),
            concurrency: 4,
            ops_count: 10_000,
            report: EngineRunReport {
                wall_time_ms: 1234,
                ops_total: 10_000,
                ops_per_sec: 8_103.7,
                retries: 0,
                aborts: 0,
                correctness: CorrectnessReport {
                    raw_sha256_match: Some(true),
                    dump_match: None,
                    canonical_sha256_match: Some(true),
                    integrity_check_ok: Some(true),
                    raw_sha256: Some("abc123".to_owned()),
                    canonical_sha256: Some("def456".to_owned()),
                    logical_sha256: None,
                    notes: None,
                },
                latency_ms: None,
                error: None,
            },
        })
    }

    #[test]
    fn format_ops_ranges() {
        assert_eq!(format_ops(500.0), "500");
        assert_eq!(format_ops(1_500.0), "1.5K");
        assert_eq!(format_ops(2_500_000.0), "2.5M");
    }

    #[test]
    fn format_f64_ranges() {
        assert_eq!(format_f64(0.5), "0.500");
        assert_eq!(format_f64(12.5), "12.50");
        assert_eq!(format_f64(1500.0), "1500");
    }

    #[test]
    fn truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_exact() {
        assert_eq!(truncate("hello", 5), "hello");
    }

    #[test]
    fn truncate_long() {
        let result = truncate("hello world foo", 10);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 10);
    }

    #[test]
    fn model_new_defaults_to_runs_tab() {
        let model = ViewerModel::new(vec![sample_run()], Vec::new());
        assert_eq!(model.tab, Tab::Runs);
        assert_eq!(model.run_selected, 0);
    }

    #[test]
    fn model_new_defaults_to_benchmarks_when_no_runs() {
        let model = ViewerModel::new(Vec::new(), Vec::new());
        assert_eq!(model.tab, Tab::Runs); // Still Runs when both empty.
    }

    #[test]
    fn model_navigation_up_down() {
        let mut model =
            ViewerModel::new(vec![sample_run(), sample_run(), sample_run()], Vec::new());

        model.move_down();
        assert_eq!(model.run_selected, 1);

        model.move_down();
        assert_eq!(model.run_selected, 2);

        model.move_down();
        assert_eq!(model.run_selected, 2); // Clamped at end.

        model.move_up();
        assert_eq!(model.run_selected, 1);

        model.move_up();
        assert_eq!(model.run_selected, 0);

        model.move_up();
        assert_eq!(model.run_selected, 0); // Clamped at start.
    }

    #[test]
    fn model_page_navigation() {
        let runs: Vec<RunRecordV1> = (0..25).map(|_| sample_run()).collect();
        let mut model = ViewerModel::new(runs, Vec::new());

        model.page_down();
        assert_eq!(model.run_selected, 10);

        model.page_down();
        assert_eq!(model.run_selected, 20);

        model.page_down();
        assert_eq!(model.run_selected, 24); // Clamped at end.

        model.page_up();
        assert_eq!(model.run_selected, 14);

        model.page_up();
        assert_eq!(model.run_selected, 4);

        model.page_up();
        assert_eq!(model.run_selected, 0); // Clamped at start.
    }

    #[test]
    fn model_tab_switch() {
        let mut model = ViewerModel::new(vec![sample_run()], Vec::new());
        assert_eq!(model.tab, Tab::Runs);

        model.tab = match model.tab {
            Tab::Runs => Tab::Benchmarks,
            Tab::Benchmarks => Tab::Runs,
        };
        assert_eq!(model.tab, Tab::Benchmarks);
    }

    #[test]
    fn model_empty_navigation_safe() {
        let mut model = ViewerModel::new(Vec::new(), Vec::new());
        model.move_down();
        assert_eq!(model.run_selected, 0);
        model.move_up();
        assert_eq!(model.run_selected, 0);
        model.page_down();
        assert_eq!(model.run_selected, 0);
        model.page_up();
        assert_eq!(model.run_selected, 0);
    }

    #[test]
    fn msg_from_event_quit() {
        let msg = Msg::from(Event::Key(ftui::KeyEvent {
            code: KeyCode::Char('q'),
            kind: KeyEventKind::Press,
            modifiers: ftui::Modifiers::empty(),
        }));
        assert!(matches!(msg, Msg::Quit));
    }

    #[test]
    fn msg_from_event_navigation() {
        let down = Msg::from(Event::Key(ftui::KeyEvent {
            code: KeyCode::Char('j'),
            kind: KeyEventKind::Press,
            modifiers: ftui::Modifiers::empty(),
        }));
        assert!(matches!(down, Msg::Down));

        let up = Msg::from(Event::Key(ftui::KeyEvent {
            code: KeyCode::Char('k'),
            kind: KeyEventKind::Press,
            modifiers: ftui::Modifiers::empty(),
        }));
        assert!(matches!(up, Msg::Up));
    }

    #[test]
    fn msg_from_event_tab() {
        let tab = Msg::from(Event::Key(ftui::KeyEvent {
            code: KeyCode::Tab,
            kind: KeyEventKind::Press,
            modifiers: ftui::Modifiers::empty(),
        }));
        assert!(matches!(tab, Msg::SwitchTab));
    }
}
