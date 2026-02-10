//! E2E dashboard binary — TUI for running and visualizing E2E test results.
//!
//! This is the initial scaffold for bd-3ppz:
//! - `ftui` (FrankenTUI) runtime for terminal lifecycle + event loop
//! - mpsc channel to feed background progress into the UI
//! - `--headless` mode for CI / non-terminal environments

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ftui::core::geometry::Rect;
use ftui::widgets::Widget;
use ftui::widgets::panel::Panel;
use ftui::widgets::paragraph::Paragraph;
use ftui::{App, Cmd, Event, KeyCode, KeyEventKind, Model, PackedRgba, ScreenMode, Style};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DashboardEvent {
    BenchmarkProgress {
        name: String,
        ops_per_sec: f64,
        elapsed_ms: u64,
    },
    BenchmarkComplete {
        name: String,
        wall_time_ms: u64,
        ops_per_sec: f64,
    },
    CorruptionInjected {
        page: u32,
        pattern: String,
    },
    RecoveryAttempt {
        group: u32,
        symbols_available: u32,
        needed: u32,
    },
    RecoverySuccess {
        page: u32,
        decode_proof: String,
    },
    RecoveryFailure {
        page: u32,
        reason: String,
    },
    CorrectnessCheck {
        workload: String,
        frank_hash: String,
        csqlite_hash: String,
        matched: bool,
    },
    StatusMessage {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PanelId {
    Benchmark,
    Recovery,
    Correctness,
    Summary,
}

impl PanelId {
    const fn title(self) -> &'static str {
        match self {
            Self::Benchmark => "Benchmark",
            Self::Recovery => "Recovery",
            Self::Correctness => "Correctness",
            Self::Summary => "Summary",
        }
    }

    const fn next(self) -> Self {
        match self {
            Self::Benchmark => Self::Recovery,
            Self::Recovery => Self::Correctness,
            Self::Correctness => Self::Summary,
            Self::Summary => Self::Benchmark,
        }
    }

    const fn prev(self) -> Self {
        match self {
            Self::Benchmark => Self::Summary,
            Self::Recovery => Self::Benchmark,
            Self::Correctness => Self::Recovery,
            Self::Summary => Self::Correctness,
        }
    }
}

#[derive(Debug, Clone)]
enum Msg {
    Tick,
    Quit,
    NextPanel,
    PrevPanel,
    Restart,
}

impl From<Event> for Msg {
    fn from(e: Event) -> Self {
        match e {
            Event::Key(k) if k.kind == KeyEventKind::Press && k.is_char('q') => Self::Quit,
            Event::Key(k) if k.kind == KeyEventKind::Press && k.is_char('r') => Self::Restart,
            Event::Key(k)
                if k.kind == KeyEventKind::Press && k.code == KeyCode::Tab && !k.shift() =>
            {
                Self::NextPanel
            }
            Event::Key(k)
                if k.kind == KeyEventKind::Press
                    && (k.code == KeyCode::BackTab || (k.code == KeyCode::Tab && k.shift())) =>
            {
                Self::PrevPanel
            }
            _ => Self::Tick,
        }
    }
}

#[derive(Debug, Clone)]
struct BenchState {
    name: String,
    ops_per_sec: f64,
    elapsed_ms: u64,
    done: bool,
}

#[derive(Debug, Clone)]
struct CorrectnessState {
    workload: String,
    matched: bool,
    frank_sha256: String,
    csqlite_sha256: String,
}

struct DashboardModel {
    active: PanelId,
    rx: mpsc::Receiver<DashboardEvent>,
    stop: Arc<AtomicBool>,
    log: VecDeque<String>,
    bench: Option<BenchState>,
    recovery: Option<String>,
    correctness: Option<CorrectnessState>,
}

impl DashboardModel {
    fn new(rx: mpsc::Receiver<DashboardEvent>, stop: Arc<AtomicBool>) -> Self {
        Self {
            active: PanelId::Benchmark,
            rx,
            stop,
            log: VecDeque::new(),
            bench: None,
            recovery: None,
            correctness: None,
        }
    }

    fn push_log(&mut self, line: impl Into<String>) {
        const MAX: usize = 50;
        if self.log.len() >= MAX {
            self.log.pop_front();
        }
        self.log.push_back(line.into());
    }

    fn clear(&mut self) {
        self.log.clear();
        self.bench = None;
        self.recovery = None;
        self.correctness = None;
        self.push_log("cleared state");
    }

    fn drain_events(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                DashboardEvent::BenchmarkProgress {
                    name,
                    ops_per_sec,
                    elapsed_ms,
                } => {
                    self.bench = Some(BenchState {
                        name: name.clone(),
                        ops_per_sec,
                        elapsed_ms,
                        done: false,
                    });
                    self.push_log(format!(
                        "bench {name}: {ops_per_sec:.1} ops/s @ {elapsed_ms}ms"
                    ));
                }
                DashboardEvent::BenchmarkComplete {
                    name,
                    wall_time_ms,
                    ops_per_sec,
                } => {
                    self.bench = Some(BenchState {
                        name: name.clone(),
                        ops_per_sec,
                        elapsed_ms: wall_time_ms,
                        done: true,
                    });
                    self.push_log(format!(
                        "bench {name}: DONE {ops_per_sec:.1} ops/s ({wall_time_ms}ms)"
                    ));
                }
                DashboardEvent::CorruptionInjected { page, pattern } => {
                    self.recovery = Some(format!(
                        "corruption injected: page={page} pattern={pattern}"
                    ));
                    self.push_log(format!("corrupt: page={page} ({pattern})"));
                }
                DashboardEvent::RecoveryAttempt {
                    group,
                    symbols_available,
                    needed,
                } => {
                    self.recovery = Some(format!(
                        "recovery attempt: group={group} symbols={symbols_available}/{needed}"
                    ));
                    self.push_log(format!(
                        "recover: group={group} symbols={symbols_available}/{needed}"
                    ));
                }
                DashboardEvent::RecoverySuccess { page, decode_proof } => {
                    self.recovery =
                        Some(format!("recovery succeeded: page={page}\n{decode_proof}"));
                    self.push_log(format!("recover: OK page={page}"));
                }
                DashboardEvent::RecoveryFailure { page, reason } => {
                    self.recovery = Some(format!("recovery failed: page={page} reason={reason}"));
                    self.push_log(format!("recover: FAIL page={page} ({reason})"));
                }
                DashboardEvent::CorrectnessCheck {
                    workload,
                    frank_hash,
                    csqlite_hash,
                    matched,
                } => {
                    self.correctness = Some(CorrectnessState {
                        workload: workload.clone(),
                        matched,
                        frank_sha256: frank_hash.clone(),
                        csqlite_sha256: csqlite_hash.clone(),
                    });
                    self.push_log(format!(
                        "check {workload}: {}",
                        if matched { "MATCH" } else { "MISMATCH" }
                    ));
                }
                DashboardEvent::StatusMessage { message } => {
                    self.push_log(format!("status: {message}"));
                }
            }
        }
    }
}

impl Model for DashboardModel {
    type Message = Msg;

    fn init(&mut self) -> Cmd<Self::Message> {
        Cmd::tick(Duration::from_millis(50))
    }

    fn update(&mut self, msg: Self::Message) -> Cmd<Self::Message> {
        match msg {
            Msg::Tick => {
                self.drain_events();
                Cmd::none()
            }
            Msg::Quit => {
                self.stop.store(true, Ordering::Relaxed);
                Cmd::quit()
            }
            Msg::NextPanel => {
                self.active = self.active.next();
                Cmd::none()
            }
            Msg::PrevPanel => {
                self.active = self.active.prev();
                Cmd::none()
            }
            Msg::Restart => {
                self.clear();
                Cmd::none()
            }
        }
    }

    fn view(&self, frame: &mut ftui::Frame) {
        let (a, b, c, d) = split_quadrants(frame.width(), frame.height());

        render_panel(
            frame,
            PanelId::Benchmark,
            a,
            self.active,
            &self.render_benchmark(),
        );
        render_panel(
            frame,
            PanelId::Recovery,
            b,
            self.active,
            &self.render_recovery(),
        );
        render_panel(
            frame,
            PanelId::Correctness,
            c,
            self.active,
            &self.render_correctness(),
        );
        render_panel(
            frame,
            PanelId::Summary,
            d,
            self.active,
            &self.render_summary(),
        );
    }
}

impl DashboardModel {
    fn render_benchmark(&self) -> String {
        let Some(ref b) = self.bench else {
            return "waiting for benchmark events...\n\nkeys: tab / shift-tab switch panel | r reset | q quit"
                .to_owned();
        };

        let status = if b.done { "DONE" } else { "RUN" };
        format!(
            "name: {}\nstatus: {}\nops_per_sec: {:.2}\nelapsed_ms: {}\n\nkeys: tab / shift-tab | r reset | q quit",
            b.name, status, b.ops_per_sec, b.elapsed_ms
        )
    }

    fn render_recovery(&self) -> String {
        self.recovery.clone().unwrap_or_else(|| {
            "waiting for recovery events...\n\nkeys: tab / shift-tab | r reset | q quit".to_owned()
        })
    }

    fn render_correctness(&self) -> String {
        let Some(ref c) = self.correctness else {
            return "waiting for correctness events...\n\nkeys: tab / shift-tab | r reset | q quit"
                .to_owned();
        };
        format!(
            "workload: {}\nmatch: {}\nfrank_sha256: {}\ncsqlite_sha256: {}\n\nkeys: tab / shift-tab | r reset | q quit",
            c.workload,
            if c.matched { "YES" } else { "NO" },
            c.frank_sha256,
            c.csqlite_sha256
        )
    }

    fn render_summary(&self) -> String {
        let mut out = String::new();
        for line in &self.log {
            out.push_str(line);
            out.push('\n');
        }
        if out.is_empty() {
            out.push_str("no events yet\n");
        }
        out.push_str("\nkeys: tab / shift-tab | r reset | q quit");
        out
    }
}

fn split_quadrants(width: u16, height: u16) -> (Rect, Rect, Rect, Rect) {
    let mid_x = width / 2;
    let mid_y = height / 2;

    let a = Rect::new(0, 0, mid_x, mid_y);
    let b = Rect::new(mid_x, 0, width.saturating_sub(mid_x), mid_y);
    let c = Rect::new(0, mid_y, mid_x, height.saturating_sub(mid_y));
    let d = Rect::new(
        mid_x,
        mid_y,
        width.saturating_sub(mid_x),
        height.saturating_sub(mid_y),
    );

    (a, b, c, d)
}

fn render_panel(frame: &mut ftui::Frame, id: PanelId, area: Rect, active: PanelId, body: &str) {
    let (border, title) = if id == active {
        (
            Style::default().fg(PackedRgba::rgb(255, 255, 0)),
            format!("{} [active]", id.title()),
        )
    } else {
        (
            Style::default().fg(PackedRgba::rgb(128, 128, 128)),
            id.title().to_owned(),
        )
    };

    Panel::new(Paragraph::new(body.to_owned()))
        .title(&title)
        .border_style(border)
        .render(area, frame);
}

#[derive(Debug, Clone, Serialize)]
struct HeadlessOutput {
    generated_at_unix_ms: u64,
    events: Vec<DashboardEvent>,
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return Ok(());
    }

    let headless = args.iter().any(|a| a == "--headless");
    let output_path = parse_output_path(&args);

    if headless {
        let out = HeadlessOutput {
            generated_at_unix_ms: unix_ms_now(),
            events: sample_events(),
        };
        write_headless(&out, output_path.as_deref())?;
        return Ok(());
    }

    let (tx, rx) = mpsc::channel::<DashboardEvent>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_bg = stop.clone();

    let bg = std::thread::spawn(move || demo_event_producer(&tx, &stop_bg));
    let model = DashboardModel::new(rx, stop.clone());

    let res = App::new(model).screen_mode(ScreenMode::AltScreen).run();

    stop.store(true, Ordering::Relaxed);
    let _ = bg.join();

    res
}

fn unix_ms_now() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    now.as_millis().try_into().unwrap_or(u64::MAX)
}

fn parse_output_path(args: &[String]) -> Option<PathBuf> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--output" && i + 1 < args.len() {
            return Some(PathBuf::from(&args[i + 1]));
        }
        i += 1;
    }
    None
}

fn write_headless(out: &HeadlessOutput, path: Option<&std::path::Path>) -> std::io::Result<()> {
    let json =
        serde_json::to_string_pretty(out).map_err(|e| std::io::Error::other(e.to_string()))?;

    if let Some(p) = path {
        std::fs::write(p, json.as_bytes())?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn sample_events() -> Vec<DashboardEvent> {
    vec![
        DashboardEvent::StatusMessage {
            message: "headless mode: sample run".to_owned(),
        },
        DashboardEvent::BenchmarkComplete {
            name: "commutative_inserts_disjoint_keys".to_owned(),
            wall_time_ms: 1234,
            ops_per_sec: 9876.0,
        },
        DashboardEvent::CorrectnessCheck {
            workload: "commutative_inserts_disjoint_keys".to_owned(),
            frank_hash: "aaa".to_owned(),
            csqlite_hash: "aaa".to_owned(),
            matched: true,
        },
    ]
}

fn demo_event_producer(tx: &mpsc::Sender<DashboardEvent>, stop: &Arc<AtomicBool>) {
    let _ = tx.send(DashboardEvent::StatusMessage {
        message: "dashboard online (demo event source)".to_owned(),
    });

    let started = Instant::now();
    let mut last_emit = Instant::now();
    let mut ops: f64 = 0.0;

    while !stop.load(Ordering::Relaxed) {
        if last_emit.elapsed() >= Duration::from_millis(250) {
            last_emit = Instant::now();
            ops += 1000.0;
            let elapsed_ms: u64 = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
            let _ = tx.send(DashboardEvent::BenchmarkProgress {
                name: "demo".to_owned(),
                ops_per_sec: ops,
                elapsed_ms,
            });

            if elapsed_ms > 3_000 {
                let _ = tx.send(DashboardEvent::BenchmarkComplete {
                    name: "demo".to_owned(),
                    wall_time_ms: elapsed_ms,
                    ops_per_sec: ops,
                });
                let _ = tx.send(DashboardEvent::CorrectnessCheck {
                    workload: "demo".to_owned(),
                    frank_hash: "deadbeef".to_owned(),
                    csqlite_hash: "deadbeef".to_owned(),
                    matched: true,
                });
                let _ = tx.send(DashboardEvent::CorruptionInjected {
                    page: 1,
                    pattern: "bit_flip".to_owned(),
                });
                let _ = tx.send(DashboardEvent::RecoveryAttempt {
                    group: 0,
                    symbols_available: 64,
                    needed: 64,
                });
                let _ = tx.send(DashboardEvent::RecoverySuccess {
                    page: 1,
                    decode_proof: "decode_succeeded=true".to_owned(),
                });
                break;
            }
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

fn print_help() {
    let text = "\
e2e-dashboard — FrankenTUI dashboard for FrankenSQLite E2E runs

USAGE:
    e2e-dashboard [--headless] [--output <FILE>]

OPTIONS:
    --headless          Skip TUI; emit JSON to stdout (or --output)
    --output <FILE>     Write headless JSON output to a file
    -h, --help          Show this help
";
    print!("{text}");
}
