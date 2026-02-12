//! E2E dashboard binary — TUI for running and visualizing E2E test results.
//!
//! Implements four rich visualization panels:
//! - **Benchmark** panel (bd-mmhw): real-time throughput sparkline, speedup ratio, progress bar
//! - **Recovery** panel (bd-s4qy): hex diff of corrupted/recovered bytes, decode progress
//! - **Correctness** panel (bd-1nqt): SHA-256 comparison table, per-workload pass/fail
//! - **Summary** panel (bd-17qs): aggregated statistics across all categories
//!
//! Also provides `--headless` mode (bd-17qs) for CI: structured JSON export with
//! corpus, correctness, performance, and recovery summaries.
//!
//! Architecture:
//! - `ftui` (FrankenTUI) runtime with Model/View/Update pattern
//! - mpsc channel feeds background progress into the UI
//! - `--headless` mode for CI / non-terminal environments

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use fsqlite::Connection as FsqliteConnection;
use fsqlite_e2e::HarnessSettings;
use fsqlite_e2e::batch_runner::{BatchConfig, CellResult, CellVerdict, run_matrix};
use fsqlite_e2e::benchmark::BenchmarkConfig;
use fsqlite_e2e::concurrency_showcase::{ShowcaseConfig, run_concurrency_showcase};
use fsqlite_e2e::corruption_demo_sqlite::{run_sqlite_corruption_scenario, verify_sqlite_result};
use fsqlite_e2e::corruption_scenarios::scenario_catalog;
use fsqlite_e2e::fsqlite_recovery_demo::run_scenario as run_fsqlite_recovery_scenario;
use fsqlite_e2e::report::EngineRunReport;

use rusqlite::OpenFlags;

use ftui::core::geometry::Rect;
use ftui::widgets::StatefulWidget;
use ftui::widgets::Widget;
use ftui::widgets::command_palette::{ActionItem, CommandPalette, PaletteAction};
use ftui::widgets::log_viewer::{LogViewer, LogViewerState};
use ftui::widgets::notification_queue::{
    NotificationPriority, NotificationQueue, NotificationStack, QueueConfig,
};
use ftui::widgets::panel::Panel;
use ftui::widgets::paragraph::Paragraph;
use ftui::widgets::progress::ProgressBar;
use ftui::widgets::sparkline::Sparkline;
use ftui::widgets::toast::{Toast, ToastIcon, ToastPosition, ToastStyle};
use ftui::{App, Cmd, Event, KeyCode, KeyEventKind, Model, PackedRgba, ScreenMode, Style};

// ── Dashboard events (contract between background worker and TUI) ────────

/// Events sent from background threads to the dashboard UI via mpsc channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DashboardEvent {
    // ── Benchmark events ─────────────────────────────────────────────
    /// Periodic throughput update for FrankenSQLite.
    BenchmarkProgress {
        name: String,
        ops_per_sec: f64,
        elapsed_ms: u64,
    },
    /// Periodic throughput update for C SQLite baseline.
    BenchmarkCsqliteProgress {
        name: String,
        ops_per_sec: f64,
        elapsed_ms: u64,
    },
    /// A single benchmark run completed.
    BenchmarkComplete {
        name: String,
        wall_time_ms: u64,
        ops_per_sec: f64,
    },
    /// Overall benchmark suite progress.
    BenchmarkSuiteProgress { completed: usize, total: usize },

    // ── Corruption recovery events ───────────────────────────────────
    /// Corruption was injected into a page.
    CorruptionInjected { page: u32, pattern: String },
    /// Hex dump of original bytes before corruption (first N bytes).
    CorruptionHexData {
        original_bytes: Vec<u8>,
        corrupted_bytes: Vec<u8>,
        page_offset: u64,
    },
    /// RaptorQ recovery phase update.
    RecoveryAttempt {
        group: u32,
        symbols_available: u32,
        needed: u32,
    },
    /// Recovery decode phase progress.
    RecoveryPhaseUpdate {
        phase: String,
        symbols_resolved: u32,
    },
    /// Recovery succeeded with hex proof.
    RecoverySuccess { page: u32, decode_proof: String },
    /// Recovered bytes for hex comparison.
    RecoveryHexData { recovered_bytes: Vec<u8> },
    /// Recovery failed.
    RecoveryFailure { page: u32, reason: String },
    /// C SQLite integrity check result after corruption.
    CsqliteIntegrityResult { passed: bool, message: String },

    // ── Correctness verification events ──────────────────────────────
    /// A new correctness workload is starting.
    CorrectnessWorkloadStart { workload: String, total_ops: usize },
    /// Progress within a correctness workload.
    CorrectnessOpProgress {
        workload: String,
        ops_done: usize,
        total_ops: usize,
        current_sql: String,
    },
    /// A correctness workload completed with hash comparison.
    CorrectnessCheck {
        workload: String,
        frank_hash: String,
        csqlite_hash: String,
        matched: bool,
    },

    // ── Corpus / summary events ────────────────────────────────────
    /// Corpus metadata (database count, total size, integrity).
    CorpusInfo {
        database_count: usize,
        total_bytes: u64,
        all_integrity_passed: bool,
    },
    /// Completed benchmark comparison: both engines measured.
    BenchmarkComparison {
        name: String,
        frank_ops_per_sec: f64,
        csqlite_ops_per_sec: f64,
    },
    /// Recovery scenario completed with outcome.
    RecoveryScenarioComplete {
        scenario: String,
        frank_recovered: bool,
        csqlite_recovered: bool,
    },

    // ── General ──────────────────────────────────────────────────────
    /// Freeform status message for the log.
    StatusMessage { message: String },
}

// ── Panel navigation ──────────────────────────────────────────────────────

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

// ── Messages ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Msg {
    Event(Event),
}

impl From<Event> for Msg {
    fn from(e: Event) -> Self {
        Self::Event(e)
    }
}

// ── State types ───────────────────────────────────────────────────────────

/// Maximum throughput history samples for the sparkline.
const MAX_SPARKLINE_SAMPLES: usize = 60;

/// Maximum hex dump display bytes per panel.
const HEX_DISPLAY_BYTES: usize = 64;

/// Maximum recent SQL lines in correctness log.
const MAX_RECENT_SQL: usize = 5;

/// Benchmark panel state: throughput history, comparison, suite progress.
#[derive(Debug, Clone)]
struct BenchState {
    name: String,
    ops_per_sec: f64,
    elapsed_ms: u64,
    done: bool,
    /// FrankenSQLite throughput history for sparkline.
    frank_history: VecDeque<f64>,
    /// C SQLite throughput history for sparkline.
    csqlite_history: VecDeque<f64>,
    /// Latest C SQLite throughput for speedup calculation.
    csqlite_ops_per_sec: Option<f64>,
    /// Suite-level progress.
    suite_completed: usize,
    suite_total: usize,
}

impl BenchState {
    fn new(name: String, ops_per_sec: f64, elapsed_ms: u64) -> Self {
        let mut frank_history = VecDeque::with_capacity(MAX_SPARKLINE_SAMPLES);
        frank_history.push_back(ops_per_sec);
        Self {
            name,
            ops_per_sec,
            elapsed_ms,
            done: false,
            frank_history,
            csqlite_history: VecDeque::with_capacity(MAX_SPARKLINE_SAMPLES),
            csqlite_ops_per_sec: None,
            suite_completed: 0,
            suite_total: 0,
        }
    }

    fn push_frank_sample(&mut self, ops: f64) {
        if self.frank_history.len() >= MAX_SPARKLINE_SAMPLES {
            self.frank_history.pop_front();
        }
        self.frank_history.push_back(ops);
    }

    fn push_csqlite_sample(&mut self, ops: f64) {
        if self.csqlite_history.len() >= MAX_SPARKLINE_SAMPLES {
            self.csqlite_history.pop_front();
        }
        self.csqlite_history.push_back(ops);
    }
}

/// Recovery panel state: corruption hex data, recovery progress, outcome.
#[derive(Debug, Clone)]
struct RecoveryState {
    /// Corrupted page number.
    page: u32,
    /// Corruption pattern description.
    pattern: String,
    /// Original bytes before corruption (first N bytes).
    original_bytes: Vec<u8>,
    /// Corrupted bytes (first N bytes).
    corrupted_bytes: Vec<u8>,
    /// Recovered bytes after RaptorQ decode (first N bytes).
    recovered_bytes: Vec<u8>,
    /// Page file offset.
    page_offset: u64,
    /// RaptorQ group being decoded.
    recovery_group: Option<u32>,
    /// Available symbols for decode.
    symbols_available: u32,
    /// Required symbols for decode.
    symbols_needed: u32,
    /// Current decode phase description.
    current_phase: String,
    /// Symbols resolved so far in current phase.
    phase_symbols_resolved: u32,
    /// Whether recovery succeeded.
    succeeded: Option<bool>,
    /// Decode proof / reason string.
    verdict: String,
    /// C SQLite integrity check outcome.
    csqlite_integrity_passed: Option<bool>,
    csqlite_integrity_message: String,
    /// Step-by-step log.
    steps: Vec<String>,
}

impl RecoveryState {
    fn new(page: u32, pattern: &str) -> Self {
        let steps = vec![format!("Corruption injected: page {page} ({pattern})")];
        Self {
            page,
            pattern: pattern.to_owned(),
            original_bytes: Vec::new(),
            corrupted_bytes: Vec::new(),
            recovered_bytes: Vec::new(),
            page_offset: u64::from(page.saturating_sub(1)) * 4096,
            recovery_group: None,
            symbols_available: 0,
            symbols_needed: 0,
            current_phase: String::new(),
            phase_symbols_resolved: 0,
            succeeded: None,
            verdict: String::new(),
            csqlite_integrity_passed: None,
            csqlite_integrity_message: String::new(),
            steps,
        }
    }
}

/// A single correctness workload result.
#[derive(Debug, Clone)]
struct WorkloadResult {
    workload: String,
    frank_hash: String,
    csqlite_hash: String,
    matched: bool,
}

/// Correctness panel state: multiple workload results, progress, recent SQL.
#[derive(Debug, Clone)]
struct CorrectnessCheckState {
    /// Completed workload results.
    results: Vec<WorkloadResult>,
    /// Currently running workload name.
    current_workload: Option<String>,
    /// Operations completed in current workload.
    ops_done: usize,
    /// Total operations in current workload.
    ops_total: usize,
    /// Recent SQL statements executed.
    recent_sql: VecDeque<String>,
}

impl Default for CorrectnessCheckState {
    fn default() -> Self {
        Self {
            results: Vec::new(),
            current_workload: None,
            ops_done: 0,
            ops_total: 0,
            recent_sql: VecDeque::with_capacity(MAX_RECENT_SQL),
        }
    }
}

impl CorrectnessCheckState {
    fn push_sql(&mut self, sql: String) {
        if self.recent_sql.len() >= MAX_RECENT_SQL {
            self.recent_sql.pop_front();
        }
        self.recent_sql.push_back(sql);
    }
}

/// A completed benchmark comparison record for the summary.
#[derive(Debug, Clone, Serialize)]
struct PerfRecord {
    name: String,
    frank_ops_per_sec: f64,
    csqlite_ops_per_sec: f64,
    speedup: f64,
}

/// A completed recovery scenario record for the summary.
#[derive(Debug, Clone, Serialize)]
struct RecoveryRecord {
    scenario: String,
    frank_recovered: bool,
    csqlite_recovered: bool,
}

/// Aggregated summary statistics (bd-17qs) across all test categories.
#[derive(Debug, Clone, Default)]
struct SummaryState {
    /// Corpus metadata.
    database_count: usize,
    total_bytes: u64,
    all_integrity_passed: bool,
    /// Total correctness operations verified.
    total_ops_verified: u64,
    /// Performance comparison records.
    perf_records: Vec<PerfRecord>,
    /// Recovery scenario records.
    recovery_records: Vec<RecoveryRecord>,
}

// ── Dashboard model ───────────────────────────────────────────────────────

struct DashboardModel {
    active: PanelId,
    rx: mpsc::Receiver<DashboardEvent>,
    stop: Arc<AtomicBool>,
    log: VecDeque<String>,
    log_viewer: LogViewer,
    log_viewer_state: RefCell<LogViewerState>,
    log_overlay_open: bool,
    palette: CommandPalette,
    notifications: NotificationQueue,
    bench: Option<BenchState>,
    recovery: Option<RecoveryState>,
    correctness: CorrectnessCheckState,
    summary: SummaryState,
}

impl DashboardModel {
    fn new(rx: mpsc::Receiver<DashboardEvent>, stop: Arc<AtomicBool>) -> Self {
        let mut palette = CommandPalette::new().with_max_visible(12);
        palette.register_action(
            ActionItem::new("cmd:focus_benchmark", "Focus Benchmark Panel")
                .with_description("Jump focus to benchmark view")
                .with_tags(&["panel", "benchmark"])
                .with_category("View"),
        );
        palette.register_action(
            ActionItem::new("cmd:focus_recovery", "Focus Recovery Panel")
                .with_description("Jump focus to recovery view")
                .with_tags(&["panel", "recovery"])
                .with_category("View"),
        );
        palette.register_action(
            ActionItem::new("cmd:focus_correctness", "Focus Correctness Panel")
                .with_description("Jump focus to correctness view")
                .with_tags(&["panel", "correctness"])
                .with_category("View"),
        );
        palette.register_action(
            ActionItem::new("cmd:focus_summary", "Focus Summary Panel")
                .with_description("Jump focus to summary view")
                .with_tags(&["panel", "summary"])
                .with_category("View"),
        );
        palette.register_action(
            ActionItem::new("cmd:toggle_log_overlay", "Toggle Log Overlay")
                .with_description("Show full event log viewer")
                .with_tags(&["log", "overlay"])
                .with_category("Logs"),
        );
        palette.register_action(
            ActionItem::new("cmd:log_top", "Scroll Log To Top")
                .with_description("Jump event log to earliest lines")
                .with_tags(&["log", "scroll"])
                .with_category("Logs"),
        );
        palette.register_action(
            ActionItem::new("cmd:log_bottom", "Scroll Log To Bottom")
                .with_description("Jump event log to newest lines")
                .with_tags(&["log", "scroll"])
                .with_category("Logs"),
        );
        palette.register_action(
            ActionItem::new("cmd:clear_state", "Clear Dashboard State")
                .with_description("Reset dashboard aggregates and log buffers")
                .with_tags(&["reset", "clear"])
                .with_category("Actions"),
        );
        palette.register_action(
            ActionItem::new("cmd:dismiss_notifications", "Dismiss Notifications")
                .with_description("Clear visible and queued toasts")
                .with_tags(&["toast", "alerts"])
                .with_category("Actions"),
        );
        palette.register_action(
            ActionItem::new("cmd:quit", "Quit Dashboard")
                .with_description("Stop background suite and exit")
                .with_tags(&["quit", "exit"])
                .with_category("App"),
        );

        Self {
            active: PanelId::Benchmark,
            rx,
            stop,
            log: VecDeque::new(),
            log_viewer: LogViewer::new(10_000),
            log_viewer_state: RefCell::new(LogViewerState::default()),
            log_overlay_open: false,
            palette,
            notifications: NotificationQueue::new(
                QueueConfig::new()
                    .max_visible(4)
                    .max_queued(32)
                    .position(ToastPosition::TopRight),
            ),
            bench: None,
            recovery: None,
            correctness: CorrectnessCheckState::default(),
            summary: SummaryState::default(),
        }
    }

    fn push_log(&mut self, line: impl Into<String>) {
        const MAX: usize = 50;
        let line = line.into();
        if self.log.len() >= MAX {
            self.log.pop_front();
        }
        self.log.push_back(line.clone());
        self.log_viewer.push(line);
    }

    fn clear(&mut self) {
        self.log.clear();
        self.log_viewer.clear();
        self.notifications.dismiss_all();
        self.bench = None;
        self.recovery = None;
        self.correctness = CorrectnessCheckState::default();
        self.summary = SummaryState::default();
        self.push_log("cleared state");
    }

    #[allow(clippy::too_many_lines)]
    fn drain_events(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                // ── Benchmark events ─────────────────────────────────
                DashboardEvent::BenchmarkProgress {
                    name,
                    ops_per_sec,
                    elapsed_ms,
                } => {
                    if let Some(ref mut b) = self.bench {
                        b.name.clone_from(&name);
                        b.ops_per_sec = ops_per_sec;
                        b.elapsed_ms = elapsed_ms;
                        b.done = false;
                        b.push_frank_sample(ops_per_sec);
                    } else {
                        self.bench = Some(BenchState::new(name.clone(), ops_per_sec, elapsed_ms));
                    }
                    self.push_log(format!(
                        "bench {name}: {ops_per_sec:.0} ops/s @ {elapsed_ms}ms"
                    ));
                }
                DashboardEvent::BenchmarkCsqliteProgress {
                    name,
                    ops_per_sec,
                    elapsed_ms,
                } => {
                    if let Some(ref mut b) = self.bench {
                        b.csqlite_ops_per_sec = Some(ops_per_sec);
                        b.push_csqlite_sample(ops_per_sec);
                    } else {
                        let mut state = BenchState::new(name.clone(), 0.0, elapsed_ms);
                        state.csqlite_ops_per_sec = Some(ops_per_sec);
                        state.push_csqlite_sample(ops_per_sec);
                        self.bench = Some(state);
                    }
                    self.push_log(format!(
                        "bench {name} (csqlite): {ops_per_sec:.0} ops/s @ {elapsed_ms}ms"
                    ));
                }
                DashboardEvent::BenchmarkComplete {
                    name,
                    wall_time_ms,
                    ops_per_sec,
                } => {
                    if let Some(ref mut b) = self.bench {
                        b.name.clone_from(&name);
                        b.ops_per_sec = ops_per_sec;
                        b.elapsed_ms = wall_time_ms;
                        b.done = true;
                        b.push_frank_sample(ops_per_sec);
                    } else {
                        let mut state = BenchState::new(name.clone(), ops_per_sec, wall_time_ms);
                        state.done = true;
                        self.bench = Some(state);
                    }
                    self.push_log(format!(
                        "bench {name}: DONE {ops_per_sec:.0} ops/s ({wall_time_ms}ms)"
                    ));
                }
                DashboardEvent::BenchmarkSuiteProgress { completed, total } => {
                    if let Some(ref mut b) = self.bench {
                        b.suite_completed = completed;
                        b.suite_total = total;
                    }
                    self.push_log(format!("suite: {completed}/{total}"));
                }

                // ── Recovery events ──────────────────────────────────
                DashboardEvent::CorruptionInjected { page, pattern } => {
                    self.recovery = Some(RecoveryState::new(page, &pattern));
                    self.push_log(format!("corrupt: page={page} ({pattern})"));
                }
                DashboardEvent::CorruptionHexData {
                    original_bytes,
                    corrupted_bytes,
                    page_offset,
                } => {
                    if let Some(ref mut r) = self.recovery {
                        r.original_bytes = original_bytes;
                        r.corrupted_bytes = corrupted_bytes;
                        r.page_offset = page_offset;
                        r.steps.push("Hex data captured".to_owned());
                    }
                }
                DashboardEvent::RecoveryAttempt {
                    group,
                    symbols_available,
                    needed,
                } => {
                    if let Some(ref mut r) = self.recovery {
                        r.recovery_group = Some(group);
                        r.symbols_available = symbols_available;
                        r.symbols_needed = needed;
                        r.steps.push(format!(
                            "Recovery: group={group} symbols={symbols_available}/{needed}"
                        ));
                    }
                    self.push_log(format!(
                        "recover: group={group} symbols={symbols_available}/{needed}"
                    ));
                }
                DashboardEvent::RecoveryPhaseUpdate {
                    phase,
                    symbols_resolved,
                } => {
                    if let Some(ref mut r) = self.recovery {
                        r.current_phase.clone_from(&phase);
                        r.phase_symbols_resolved = symbols_resolved;
                        r.steps.push(format!(
                            "Phase {phase}: {symbols_resolved} symbols resolved"
                        ));
                    }
                }
                DashboardEvent::RecoverySuccess { page, decode_proof } => {
                    if let Some(ref mut r) = self.recovery {
                        r.succeeded = Some(true);
                        r.verdict.clone_from(&decode_proof);
                        r.steps.push(format!("Page {page} RECOVERED"));
                    }
                    self.push_log(format!("recover: OK page={page}"));
                }
                DashboardEvent::RecoveryHexData { recovered_bytes } => {
                    if let Some(ref mut r) = self.recovery {
                        r.recovered_bytes = recovered_bytes;
                    }
                }
                DashboardEvent::RecoveryFailure { page, reason } => {
                    if let Some(ref mut r) = self.recovery {
                        r.succeeded = Some(false);
                        r.verdict.clone_from(&reason);
                        r.steps.push(format!("FAILED: page={page} ({reason})"));
                    }
                    self.push_log(format!("recover: FAIL page={page} ({reason})"));
                }
                DashboardEvent::CsqliteIntegrityResult { passed, message } => {
                    if let Some(ref mut r) = self.recovery {
                        r.csqlite_integrity_passed = Some(passed);
                        r.csqlite_integrity_message.clone_from(&message);
                        r.steps.push(format!(
                            "C SQLite: {}",
                            if passed {
                                "integrity OK"
                            } else {
                                "INTEGRITY FAILED"
                            }
                        ));
                    }
                }

                // ── Correctness events ───────────────────────────────
                DashboardEvent::CorrectnessWorkloadStart {
                    workload,
                    total_ops,
                } => {
                    self.correctness.current_workload = Some(workload.clone());
                    self.correctness.ops_done = 0;
                    self.correctness.ops_total = total_ops;
                    self.correctness.recent_sql.clear();
                    self.push_log(format!("correctness: start {workload} ({total_ops} ops)"));
                }
                DashboardEvent::CorrectnessOpProgress {
                    workload: _,
                    ops_done,
                    total_ops,
                    current_sql,
                } => {
                    self.correctness.ops_done = ops_done;
                    self.correctness.ops_total = total_ops;
                    self.correctness.push_sql(current_sql);
                }
                DashboardEvent::CorrectnessCheck {
                    workload,
                    frank_hash,
                    csqlite_hash,
                    matched,
                } => {
                    // Accumulate total ops verified for summary.
                    self.summary.total_ops_verified += self.correctness.ops_total as u64;
                    self.correctness.results.push(WorkloadResult {
                        workload: workload.clone(),
                        frank_hash: frank_hash.clone(),
                        csqlite_hash: csqlite_hash.clone(),
                        matched,
                    });
                    self.correctness.current_workload = None;
                    self.correctness.ops_done = 0;
                    self.correctness.ops_total = 0;
                    self.push_log(format!(
                        "check {workload}: {}",
                        if matched { "MATCH" } else { "MISMATCH" }
                    ));
                    if !matched {
                        let toast = Toast::new(format!("Parity mismatch: {workload}"))
                            .icon(ToastIcon::Error)
                            .style_variant(ToastStyle::Error)
                            .duration(Duration::from_secs(8));
                        self.notifications.push(toast, NotificationPriority::High);
                    }
                }

                // ── Corpus / summary events ──────────────────────────
                DashboardEvent::CorpusInfo {
                    database_count,
                    total_bytes,
                    all_integrity_passed,
                } => {
                    self.summary.database_count = database_count;
                    self.summary.total_bytes = total_bytes;
                    self.summary.all_integrity_passed = all_integrity_passed;
                    self.push_log(format!(
                        "corpus: {database_count} dbs, {} MB",
                        total_bytes / (1024 * 1024)
                    ));
                    let toast = Toast::new(format!("Loaded {database_count} fixtures"))
                        .icon(if all_integrity_passed {
                            ToastIcon::Success
                        } else {
                            ToastIcon::Warning
                        })
                        .style_variant(if all_integrity_passed {
                            ToastStyle::Success
                        } else {
                            ToastStyle::Warning
                        })
                        .duration(Duration::from_secs(4));
                    self.notifications.push(toast, NotificationPriority::Normal);
                }
                DashboardEvent::BenchmarkComparison {
                    name,
                    frank_ops_per_sec,
                    csqlite_ops_per_sec,
                } => {
                    let speedup = if csqlite_ops_per_sec > 0.0 {
                        frank_ops_per_sec / csqlite_ops_per_sec
                    } else {
                        0.0
                    };
                    self.summary.perf_records.push(PerfRecord {
                        name: name.clone(),
                        frank_ops_per_sec,
                        csqlite_ops_per_sec,
                        speedup,
                    });
                    self.push_log(format!("perf: {name} speedup={speedup:.2}x"));
                    let toast = Toast::new(format!("{name}: {speedup:.2}x speedup"))
                        .icon(if speedup >= 1.0 {
                            ToastIcon::Success
                        } else {
                            ToastIcon::Warning
                        })
                        .style_variant(if speedup >= 1.0 {
                            ToastStyle::Success
                        } else {
                            ToastStyle::Warning
                        })
                        .duration(Duration::from_secs(4));
                    self.notifications.push(toast, NotificationPriority::Low);
                }
                DashboardEvent::RecoveryScenarioComplete {
                    scenario,
                    frank_recovered,
                    csqlite_recovered,
                } => {
                    self.summary.recovery_records.push(RecoveryRecord {
                        scenario: scenario.clone(),
                        frank_recovered,
                        csqlite_recovered,
                    });
                    self.push_log(format!(
                        "recovery: {scenario} frank={frank_recovered} csqlite={csqlite_recovered}"
                    ));
                    let toast = Toast::new(format!(
                        "{scenario}: frank={frank_recovered}, sqlite={csqlite_recovered}"
                    ))
                    .icon(if frank_recovered && !csqlite_recovered {
                        ToastIcon::Success
                    } else if frank_recovered {
                        ToastIcon::Info
                    } else {
                        ToastIcon::Warning
                    })
                    .style_variant(if frank_recovered && !csqlite_recovered {
                        ToastStyle::Success
                    } else if frank_recovered {
                        ToastStyle::Info
                    } else {
                        ToastStyle::Warning
                    })
                    .duration(Duration::from_secs(6));
                    self.notifications.push(toast, NotificationPriority::Normal);
                }

                // ── General ──────────────────────────────────────────
                DashboardEvent::StatusMessage { message } => {
                    self.push_log(format!("status: {message}"));
                }
            }
        }
    }
}

// ── Model implementation ──────────────────────────────────────────────────

impl Model for DashboardModel {
    type Message = Msg;

    fn init(&mut self) -> Cmd<Self::Message> {
        Cmd::tick(Duration::from_millis(50))
    }

    fn update(&mut self, msg: Self::Message) -> Cmd<Self::Message> {
        let Msg::Event(event) = msg;

        if event == Event::Tick {
            self.drain_events();
            let _ = self.notifications.tick(Duration::from_millis(50));
            return Cmd::none();
        }

        if let Some(action) = self.palette.handle_event(&event) {
            return self.execute_palette_action(action);
        }
        if self.palette.is_visible() {
            return Cmd::none();
        }

        if let Event::Key(k) = event
            && k.kind == KeyEventKind::Press
        {
            if k.is_char('q') {
                self.stop.store(true, Ordering::Relaxed);
                return Cmd::quit();
            }
            if k.is_char('r') {
                self.clear();
                return Cmd::none();
            }
            if k.code == KeyCode::Tab && !k.shift() {
                self.active = self.active.next();
                return Cmd::none();
            }
            if k.code == KeyCode::BackTab || (k.code == KeyCode::Tab && k.shift()) {
                self.active = self.active.prev();
                return Cmd::none();
            }
            if k.is_char('l') {
                self.log_overlay_open = !self.log_overlay_open;
                return Cmd::none();
            }

            if self.log_overlay_open {
                match k.code {
                    KeyCode::Escape => self.log_overlay_open = false,
                    KeyCode::Up => self.log_viewer.scroll_up(1),
                    KeyCode::Down => self.log_viewer.scroll_down(1),
                    KeyCode::PageUp => {
                        let state = self.log_viewer_state.borrow();
                        self.log_viewer.page_up(&state);
                    }
                    KeyCode::PageDown => {
                        let state = self.log_viewer_state.borrow();
                        self.log_viewer.page_down(&state);
                    }
                    KeyCode::Home => self.log_viewer.scroll_to_top(),
                    KeyCode::End => self.log_viewer.scroll_to_bottom(),
                    KeyCode::Char('f') => self.log_viewer.toggle_follow(),
                    _ => {}
                }
            }
        }

        Cmd::none()
    }

    fn view(&self, frame: &mut ftui::Frame) {
        let (a, b, c, d) = split_quadrants(frame.width(), frame.height());

        self.render_benchmark_panel(frame, a);
        self.render_recovery_panel(frame, b);
        self.render_correctness_panel(frame, c);
        self.render_summary_panel(frame, d);

        let full = Rect::new(0, 0, frame.width(), frame.height());
        NotificationStack::new(&self.notifications)
            .margin(1)
            .render(full, frame);

        if self.log_overlay_open {
            self.render_log_overlay(frame, full);
        }

        self.palette.render(full, frame);
    }
}

impl DashboardModel {
    fn execute_palette_action(&mut self, action: PaletteAction) -> Cmd<Msg> {
        match action {
            PaletteAction::Dismiss => Cmd::none(),
            PaletteAction::Execute(id) => match id.as_str() {
                "cmd:focus_benchmark" => {
                    self.active = PanelId::Benchmark;
                    Cmd::none()
                }
                "cmd:focus_recovery" => {
                    self.active = PanelId::Recovery;
                    Cmd::none()
                }
                "cmd:focus_correctness" => {
                    self.active = PanelId::Correctness;
                    Cmd::none()
                }
                "cmd:focus_summary" => {
                    self.active = PanelId::Summary;
                    Cmd::none()
                }
                "cmd:toggle_log_overlay" => {
                    self.log_overlay_open = !self.log_overlay_open;
                    Cmd::none()
                }
                "cmd:log_top" => {
                    self.log_viewer.scroll_to_top();
                    Cmd::none()
                }
                "cmd:log_bottom" => {
                    self.log_viewer.scroll_to_bottom();
                    Cmd::none()
                }
                "cmd:clear_state" => {
                    self.clear();
                    Cmd::none()
                }
                "cmd:dismiss_notifications" => {
                    self.notifications.dismiss_all();
                    Cmd::none()
                }
                "cmd:quit" => {
                    self.stop.store(true, Ordering::Relaxed);
                    Cmd::quit()
                }
                _ => Cmd::none(),
            },
        }
    }

    fn render_log_overlay(&self, frame: &mut ftui::Frame, area: Rect) {
        let width = area.width.saturating_sub(6);
        let height = area.height.saturating_sub(4);
        if width < 20 || height < 6 {
            return;
        }

        let x = area.x + (area.width.saturating_sub(width)) / 2;
        let y = area.y + (area.height.saturating_sub(height)) / 2;
        let overlay = Rect::new(x, y, width, height);

        let panel = Panel::new(Paragraph::new(String::new()))
            .title("Event Log Overlay (Esc close, f follow, PgUp/PgDn)")
            .border_style(Style::new().fg(PackedRgba::rgb(120, 190, 255)));
        let inner = panel.inner(overlay);
        panel.render(overlay, frame);
        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let mut state = self.log_viewer_state.borrow_mut();
        StatefulWidget::render(&self.log_viewer, inner, frame, &mut state);
    }
}

// ── Benchmark panel rendering (bd-mmhw) ──────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_benchmark_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Benchmark, self.active);
        let title = panel_title(PanelId::Benchmark, self.active);

        let Some(ref b) = self.bench else {
            Panel::new(Paragraph::new(
                "Waiting for benchmark events...\n\n\
                 Keys: Tab/Shift-Tab switch panel | r reset | q quit"
                    .to_owned(),
            ))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
            return;
        };

        // Compute inner area for custom layout.
        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // Status line.
        let status = if b.done { "DONE" } else { "RUNNING" };
        let status_line = format!("  {}: {} [{status}]", b.name, format_ops(b.ops_per_sec));
        Paragraph::new(status_line)
            .style(Style::new().fg(if b.done {
                PackedRgba::rgb(100, 220, 100)
            } else {
                PackedRgba::rgb(100, 180, 255)
            }))
            .render(Rect::new(inner.x, y, inner.width, 1), frame);
        y += 1;

        // Speedup ratio.
        if let Some(csqlite_ops) = b.csqlite_ops_per_sec {
            let speedup = if csqlite_ops > 0.0 {
                b.ops_per_sec / csqlite_ops
            } else {
                0.0
            };
            let speedup_line = format!(
                "  FrankenSQLite: {}  |  C SQLite: {}  |  Speedup: {speedup:.2}x",
                format_ops(b.ops_per_sec),
                format_ops(csqlite_ops)
            );
            let color = if speedup >= 2.0 {
                PackedRgba::rgb(80, 220, 80)
            } else if speedup >= 1.0 {
                PackedRgba::rgb(220, 220, 80)
            } else {
                PackedRgba::rgb(220, 80, 80)
            };
            Paragraph::new(speedup_line)
                .style(Style::new().fg(color))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Sparkline: FrankenSQLite throughput.
        if y < inner.y + inner.height && !b.frank_history.is_empty() {
            y += 1; // blank line
            let label = "  Throughput (FrankenSQLite):";
            Paragraph::new(label.to_owned())
                .style(Style::new().fg(PackedRgba::rgb(100, 220, 100)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let data: Vec<f64> = b.frank_history.iter().copied().collect();
                let spark_width = inner
                    .width
                    .saturating_sub(2)
                    .min(u16::try_from(data.len()).unwrap_or(u16::MAX));
                let visible = &data[data.len().saturating_sub(spark_width as usize)..];
                Sparkline::new(visible)
                    .style(Style::new().fg(PackedRgba::rgb(80, 220, 80)))
                    .render(Rect::new(inner.x + 2, y, spark_width, 1), frame);
                y += 1;
            }
        }

        // Sparkline: C SQLite throughput (if available).
        if y < inner.y + inner.height && !b.csqlite_history.is_empty() {
            let label = "  Throughput (C SQLite):";
            Paragraph::new(label.to_owned())
                .style(Style::new().fg(PackedRgba::rgb(220, 180, 60)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let data: Vec<f64> = b.csqlite_history.iter().copied().collect();
                let spark_width = inner
                    .width
                    .saturating_sub(2)
                    .min(u16::try_from(data.len()).unwrap_or(u16::MAX));
                let visible = &data[data.len().saturating_sub(spark_width as usize)..];
                Sparkline::new(visible)
                    .style(Style::new().fg(PackedRgba::rgb(220, 180, 60)))
                    .render(Rect::new(inner.x + 2, y, spark_width, 1), frame);
                y += 1;
            }
        }

        // Suite progress bar.
        if y + 1 < inner.y + inner.height && b.suite_total > 0 {
            y += 1;
            let ratio = if b.suite_total > 0 {
                b.suite_completed as f64 / b.suite_total as f64
            } else {
                0.0
            };
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let pct = (ratio * 100.0) as u32;
            let label = format!("  Suite: {}/{} ({pct}%)", b.suite_completed, b.suite_total);
            Paragraph::new(label)
                .style(Style::new().fg(PackedRgba::WHITE))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                ProgressBar::new()
                    .ratio(ratio)
                    .gauge_style(Style::new().bg(PackedRgba::rgb(60, 120, 200)))
                    .render(
                        Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                        frame,
                    );
            }
        }
    }
}

// ── Recovery panel rendering (bd-s4qy) ───────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_recovery_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Recovery, self.active);
        let title = panel_title(PanelId::Recovery, self.active);

        let Some(ref r) = self.recovery else {
            Panel::new(Paragraph::new(
                "Waiting for recovery events...\n\n\
                 Keys: Tab/Shift-Tab switch panel | r reset | q quit"
                    .to_owned(),
            ))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
            return;
        };

        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // Header: page info.
        let header = format!(
            "  Page {} (offset {:#X}): {}",
            r.page, r.page_offset, r.pattern
        );
        Paragraph::new(header)
            .style(Style::new().fg(PackedRgba::rgb(255, 180, 80)))
            .render(Rect::new(inner.x, y, inner.width, 1), frame);
        y += 1;

        // Hex diff: original vs corrupted.
        if !r.original_bytes.is_empty() && !r.corrupted_bytes.is_empty() {
            y += 1;
            let hex_lines_available =
                ((inner.y + inner.height).saturating_sub(y).saturating_sub(8)) / 2;
            let bytes_per_line: usize = 8;
            let max_lines = (hex_lines_available as usize).min(HEX_DISPLAY_BYTES / bytes_per_line);

            // Original bytes.
            Paragraph::new("  Original:".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            for line_idx in 0..max_lines {
                if y >= inner.y + inner.height {
                    break;
                }
                let start = line_idx * bytes_per_line;
                let hex = format_hex_line(&r.original_bytes, start, bytes_per_line);
                Paragraph::new(format!("  {hex}"))
                    .style(Style::new().fg(PackedRgba::rgb(100, 200, 100)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }

            // Corrupted bytes.
            if y < inner.y + inner.height {
                Paragraph::new("  Corrupted:".to_owned())
                    .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;

                for line_idx in 0..max_lines {
                    if y >= inner.y + inner.height {
                        break;
                    }
                    let start = line_idx * bytes_per_line;
                    let hex = format_hex_line_diff(
                        &r.corrupted_bytes,
                        &r.original_bytes,
                        start,
                        bytes_per_line,
                    );
                    Paragraph::new(format!("  {hex}"))
                        .style(Style::new().fg(PackedRgba::rgb(255, 80, 80)))
                        .render(Rect::new(inner.x, y, inner.width, 1), frame);
                    y += 1;
                }
            }
        }

        // Recovery status.
        if y < inner.y + inner.height {
            y += 1;
            Paragraph::new("  Recovery Status:".to_owned())
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Symbol availability progress.
        if r.symbols_needed > 0 && y < inner.y + inner.height {
            let ratio = if r.symbols_needed > 0 {
                f64::from(r.symbols_available) / f64::from(r.symbols_needed)
            } else {
                0.0
            };
            let decodable = if r.symbols_available >= r.symbols_needed {
                "DECODABLE"
            } else {
                "INSUFFICIENT"
            };
            let sym_line = format!(
                "  Symbols: {}/{} ({decodable})",
                r.symbols_available, r.symbols_needed
            );
            Paragraph::new(sym_line)
                .style(Style::new().fg(if r.symbols_available >= r.symbols_needed {
                    PackedRgba::rgb(80, 220, 80)
                } else {
                    PackedRgba::rgb(255, 180, 80)
                }))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                ProgressBar::new()
                    .ratio(ratio.min(1.0))
                    .gauge_style(Style::new().bg(PackedRgba::rgb(60, 180, 120)))
                    .render(
                        Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                        frame,
                    );
                y += 1;
            }
        }

        // Phase progress.
        if !r.current_phase.is_empty() && y < inner.y + inner.height {
            let phase_line = format!(
                "  Phase: {} ({} resolved)",
                r.current_phase, r.phase_symbols_resolved
            );
            Paragraph::new(phase_line)
                .style(Style::new().fg(PackedRgba::rgb(180, 180, 255)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Verdict.
        if let Some(succeeded) = r.succeeded {
            if y < inner.y + inner.height {
                let (icon, color) = if succeeded {
                    ("RECOVERED", PackedRgba::rgb(80, 255, 80))
                } else {
                    ("FAILED", PackedRgba::rgb(255, 80, 80))
                };
                let verdict_line = format!("  FrankenSQLite: {icon}");
                Paragraph::new(verdict_line)
                    .style(Style::new().fg(color).bold())
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }
        }

        // C SQLite integrity result.
        if let Some(passed) = r.csqlite_integrity_passed {
            if y < inner.y + inner.height {
                let (icon, color) = if passed {
                    ("integrity OK", PackedRgba::rgb(160, 160, 160))
                } else {
                    ("INTEGRITY FAILED", PackedRgba::rgb(255, 80, 80))
                };
                let line = format!("  C SQLite: {icon}");
                Paragraph::new(line)
                    .style(Style::new().fg(color))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
            }
        }
    }
}

// ── Correctness panel rendering (bd-1nqt) ────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_correctness_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Correctness, self.active);
        let title = panel_title(PanelId::Correctness, self.active);
        let c = &self.correctness;

        if c.results.is_empty() && c.current_workload.is_none() {
            Panel::new(Paragraph::new(
                "Waiting for correctness events...\n\n\
                 Keys: Tab/Shift-Tab switch panel | r reset | q quit"
                    .to_owned(),
            ))
            .title(&title)
            .border_style(border_style)
            .render(area, frame);
            return;
        }

        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // Current workload progress.
        if let Some(ref wl) = c.current_workload {
            let progress_line = format!("  Workload: {wl}");
            Paragraph::new(progress_line)
                .style(Style::new().fg(PackedRgba::rgb(100, 180, 255)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if c.ops_total > 0 && y < inner.y + inner.height {
                let ratio = c.ops_done as f64 / c.ops_total as f64;
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let pct = (ratio * 100.0) as u32;
                let ops_line = format!("  Progress: {}/{} ({pct}%)", c.ops_done, c.ops_total);
                Paragraph::new(ops_line)
                    .style(Style::new().fg(PackedRgba::WHITE))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;

                if y < inner.y + inner.height {
                    ProgressBar::new()
                        .ratio(ratio)
                        .gauge_style(Style::new().bg(PackedRgba::rgb(60, 120, 200)))
                        .render(
                            Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                            frame,
                        );
                    y += 1;
                }
            }
            y += 1;
        }

        // Results table header.
        if y < inner.y + inner.height {
            let hdr = format!(
                "  {:<20} {:<10} {:<10} {}",
                "Workload", "Frank", "CSQLite", "Result"
            );
            Paragraph::new(hdr)
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Separator.
        if y < inner.y + inner.height {
            let sep: String = "-".repeat(inner.width.saturating_sub(2) as usize);
            Paragraph::new(format!("  {sep}"))
                .style(Style::new().fg(PackedRgba::rgb(80, 80, 80)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Result rows.
        for result in &c.results {
            if y >= inner.y + inner.height {
                break;
            }
            let frank_short = truncate_hash(&result.frank_hash, 8);
            let csqlite_short = truncate_hash(&result.csqlite_hash, 8);
            let (icon, color) = if result.matched {
                ("MATCH", PackedRgba::rgb(80, 220, 80))
            } else {
                ("MISMATCH", PackedRgba::rgb(255, 80, 80))
            };
            let wl_name = truncate_str(&result.workload, 18);
            let row = format!("  {wl_name:<20} {frank_short:<10} {csqlite_short:<10} {icon}");
            Paragraph::new(row)
                .style(Style::new().fg(color))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Currently running workload placeholder row.
        if c.current_workload.is_some() && y < inner.y + inner.height {
            let wl_name = c
                .current_workload
                .as_deref()
                .map_or_else(|| "...".to_owned(), |w| truncate_str(w, 18));
            let row = format!("  {wl_name:<20} {:<10} {:<10} ...", "running", "running");
            Paragraph::new(row)
                .style(Style::new().fg(PackedRgba::rgb(180, 180, 180)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;
        }

        // Recent SQL log.
        if !c.recent_sql.is_empty() && y + 1 < inner.y + inner.height {
            y += 1;
            Paragraph::new("  Recent SQL:".to_owned())
                .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            for sql in &c.recent_sql {
                if y >= inner.y + inner.height {
                    break;
                }
                let truncated = truncate_str(sql, (inner.width.saturating_sub(4)) as usize);
                Paragraph::new(format!("  {truncated}"))
                    .style(Style::new().fg(PackedRgba::rgb(120, 120, 120)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }
        }

        // Overall summary.
        if y < inner.y + inner.height && !c.results.is_empty() {
            y += 1;
            let passed = c.results.iter().filter(|r| r.matched).count();
            let running = i32::from(c.current_workload.is_some());
            let summary = format!(
                "  Overall: {passed}/{} passed, {running} running",
                c.results.len()
            );
            Paragraph::new(summary)
                .style(Style::new().fg(PackedRgba::WHITE))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
        }
    }
}

// ── Summary panel rendering (bd-17qs) ─────────────────────────────────────

impl DashboardModel {
    #[allow(clippy::too_many_lines)]
    fn render_summary_panel(&self, frame: &mut ftui::Frame, area: Rect) {
        let border_style = panel_border_style(PanelId::Summary, self.active);
        let title = panel_title(PanelId::Summary, self.active);

        let has_data = self.summary.database_count > 0
            || !self.correctness.results.is_empty()
            || !self.summary.perf_records.is_empty()
            || !self.summary.recovery_records.is_empty();

        if !has_data {
            // Fall back to scrollable log when no aggregated data yet.
            let mut body = String::new();
            for line in &self.log {
                body.push_str(line);
                body.push('\n');
            }
            if body.is_empty() {
                body.push_str("No events yet\n");
            }
            body.push_str("\nKeys: Tab/Shift-Tab switch | r reset | q quit");
            Panel::new(Paragraph::new(body))
                .title(&title)
                .border_style(border_style)
                .render(area, frame);
            return;
        }

        let panel = Panel::new(Paragraph::new(String::new()))
            .title(&title)
            .border_style(border_style);
        let inner = panel.inner(area);
        panel.render(area, frame);

        if inner.height < 3 || inner.width < 10 {
            return;
        }

        let mut y = inner.y;

        // ── DATABASE CORPUS ──
        if self.summary.database_count > 0 {
            Paragraph::new("  DATABASE CORPUS".to_owned())
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let mb = self.summary.total_bytes / (1024 * 1024);
                let integrity = if self.summary.all_integrity_passed {
                    "All integrity checks passed"
                } else {
                    "Some integrity checks FAILED"
                };
                let line = format!(
                    "  {} databases | {} MB total | {integrity}",
                    self.summary.database_count, mb
                );
                Paragraph::new(line)
                    .style(Style::new().fg(if self.summary.all_integrity_passed {
                        PackedRgba::rgb(100, 200, 100)
                    } else {
                        PackedRgba::rgb(255, 80, 80)
                    }))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 2;
            }
        }

        // ── CORRECTNESS ──
        if !self.correctness.results.is_empty() && y < inner.y + inner.height {
            Paragraph::new("  CORRECTNESS".to_owned())
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let passed = self
                    .correctness
                    .results
                    .iter()
                    .filter(|r| r.matched)
                    .count();
                let total = self.correctness.results.len();
                let ratio = if total > 0 {
                    passed as f64 / total as f64
                } else {
                    0.0
                };
                let all_pass = passed == total;
                let line = format!("  {passed}/{total} workloads passed");
                Paragraph::new(line)
                    .style(Style::new().fg(if all_pass {
                        PackedRgba::rgb(80, 220, 80)
                    } else {
                        PackedRgba::rgb(255, 80, 80)
                    }))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;

                if y < inner.y + inner.height {
                    ProgressBar::new()
                        .ratio(ratio)
                        .gauge_style(Style::new().bg(if all_pass {
                            PackedRgba::rgb(60, 180, 60)
                        } else {
                            PackedRgba::rgb(200, 60, 60)
                        }))
                        .render(
                            Rect::new(inner.x + 2, y, inner.width.saturating_sub(4), 1),
                            frame,
                        );
                    y += 1;
                }

                if y < inner.y + inner.height && self.summary.total_ops_verified > 0 {
                    let line = format!(
                        "  Total operations verified: {}",
                        format_count(self.summary.total_ops_verified)
                    );
                    Paragraph::new(line)
                        .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                        .render(Rect::new(inner.x, y, inner.width, 1), frame);
                    y += 1;
                }
                y += 1;
            }
        }

        // ── PERFORMANCE ──
        if !self.summary.perf_records.is_empty() && y < inner.y + inner.height {
            Paragraph::new("  PERFORMANCE".to_owned())
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            // Table header.
            if y < inner.y + inner.height {
                let hdr = format!("  {:<22} {:>8} {:>8}", "Category", "Speedup", "Winner");
                Paragraph::new(hdr)
                    .style(Style::new().fg(PackedRgba::rgb(180, 180, 180)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }

            for rec in &self.summary.perf_records {
                if y >= inner.y + inner.height {
                    break;
                }
                let winner = if rec.speedup >= 1.0 {
                    "Frank"
                } else {
                    "CSQLite"
                };
                let color = if rec.speedup >= 1.5 {
                    PackedRgba::rgb(80, 220, 80)
                } else if rec.speedup >= 1.0 {
                    PackedRgba::rgb(220, 220, 80)
                } else {
                    PackedRgba::rgb(220, 80, 80)
                };
                let name = truncate_str(&rec.name, 20);
                let row = format!("  {name:<22} {:>7.2}x {winner:>8}", rec.speedup);
                Paragraph::new(row)
                    .style(Style::new().fg(color))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;
            }
            y += 1;
        }

        // ── CORRUPTION RECOVERY ──
        if !self.summary.recovery_records.is_empty() && y < inner.y + inner.height {
            Paragraph::new("  CORRUPTION RECOVERY".to_owned())
                .style(Style::new().fg(PackedRgba::WHITE).bold())
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
            y += 1;

            if y < inner.y + inner.height {
                let frank_recovered = self
                    .summary
                    .recovery_records
                    .iter()
                    .filter(|r| r.frank_recovered)
                    .count();
                let csqlite_recovered = self
                    .summary
                    .recovery_records
                    .iter()
                    .filter(|r| r.csqlite_recovered)
                    .count();
                let total = self.summary.recovery_records.len();

                let line =
                    format!("  {frank_recovered}/{total} scenarios recovered (FrankenSQLite)");
                Paragraph::new(line)
                    .style(Style::new().fg(PackedRgba::rgb(80, 220, 80)))
                    .render(Rect::new(inner.x, y, inner.width, 1), frame);
                y += 1;

                if y < inner.y + inner.height {
                    let line = format!("  C SQLite: {csqlite_recovered}/{total} recovered");
                    Paragraph::new(line)
                        .style(Style::new().fg(PackedRgba::rgb(160, 160, 160)))
                        .render(Rect::new(inner.x, y, inner.width, 1), frame);
                    y += 1;
                }
                y += 1;
            }
        }

        // ── OVERALL narrative ──
        if y < inner.y + inner.height && has_data {
            Paragraph::new(self.generate_narrative())
                .style(Style::new().fg(PackedRgba::rgb(180, 220, 255)))
                .render(Rect::new(inner.x, y, inner.width, 1), frame);
        }
    }

    /// Generate a one-line narrative summarizing the overall results.
    fn generate_narrative(&self) -> String {
        let correctness_ok = self.correctness.results.iter().all(|r| r.matched);
        let avg_speedup = if self.summary.perf_records.is_empty() {
            0.0
        } else {
            self.summary
                .perf_records
                .iter()
                .map(|r| r.speedup)
                .sum::<f64>()
                / self.summary.perf_records.len() as f64
        };
        let frank_recovered = self
            .summary
            .recovery_records
            .iter()
            .filter(|r| r.frank_recovered)
            .count();

        let mut narrative = String::from("  ");
        if correctness_ok && !self.correctness.results.is_empty() {
            narrative.push_str("Correctness parity confirmed. ");
        }
        if avg_speedup > 1.0 {
            let _ = write!(narrative, "Avg speedup: {avg_speedup:.2}x. ");
        }
        if frank_recovered > 0 {
            let _ = write!(
                narrative,
                "{frank_recovered} corruption scenarios recovered via RaptorQ."
            );
        }
        if narrative.len() <= 2 {
            narrative.push_str("Results collecting...");
        }
        narrative
    }
}

// ── Layout + styling helpers ─────────────────────────────────────────────

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

fn panel_border_style(id: PanelId, active: PanelId) -> Style {
    if id == active {
        Style::default().fg(PackedRgba::rgb(255, 255, 0))
    } else {
        Style::default().fg(PackedRgba::rgb(80, 80, 80))
    }
}

fn panel_title(id: PanelId, active: PanelId) -> String {
    if id == active {
        format!("{} [active]", id.title())
    } else {
        id.title().to_owned()
    }
}

// ── Formatting helpers ───────────────────────────────────────────────────

/// Format a large count with comma separators.
fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result
}

/// Format operations per second with SI suffix.
fn format_ops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.1}M ops/s", ops / 1_000_000.0)
    } else if ops >= 1_000.0 {
        format!("{:.1}K ops/s", ops / 1_000.0)
    } else {
        format!("{ops:.0} ops/s")
    }
}

/// Format a hex line from a byte slice.
fn format_hex_line(bytes: &[u8], start: usize, count: usize) -> String {
    let end = (start + count).min(bytes.len());
    if start >= bytes.len() {
        return String::new();
    }
    let mut hex = String::with_capacity(count * 3);
    for (i, &b) in bytes[start..end].iter().enumerate() {
        if i > 0 {
            hex.push(' ');
        }
        let _ = write!(hex, "{b:02X}");
    }
    hex
}

/// Format a hex line highlighting bytes that differ from reference.
fn format_hex_line_diff(bytes: &[u8], reference: &[u8], start: usize, count: usize) -> String {
    let end = (start + count).min(bytes.len());
    if start >= bytes.len() {
        return String::new();
    }
    let mut hex = String::with_capacity(count * 3);
    for (i, &b) in bytes[start..end].iter().enumerate() {
        if i > 0 {
            hex.push(' ');
        }
        let ref_byte = reference.get(start + i).copied().unwrap_or(0);
        if b == ref_byte {
            let _ = write!(hex, "{b:02X}");
        } else {
            // Mark differing bytes with brackets.
            let _ = write!(hex, "[{b:02X}]");
        }
    }
    hex
}

/// Truncate a hash string to `max_len` characters.
fn truncate_hash(hash: &str, max_len: usize) -> String {
    truncate_str(hash, max_len)
}

/// Truncate a string reference to `max_len` characters.
fn truncate_str(s: &str, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }

    let char_count = s.chars().count();
    if char_count <= max_len {
        return s.to_owned();
    }
    if max_len <= 3 {
        return ".".repeat(max_len);
    }

    let keep = max_len - 3;
    let mut out = String::with_capacity(max_len);
    out.extend(s.chars().take(keep));
    out.push_str("...");
    out
}

// ── Headless mode (bd-17qs) ────────────────────────────────────────────────

/// Structured JSON output for `--headless` mode (bd-17qs).
///
/// Provides a machine-readable summary across all test categories:
/// corpus, correctness, performance, and recovery.
#[derive(Debug, Clone, Serialize)]
struct SummaryReport {
    /// ISO-8601 timestamp of when this report was generated.
    timestamp: String,
    /// Database corpus metadata.
    corpus: CorpusSummary,
    /// Correctness verification summary.
    correctness: CorrectnessSummary,
    /// Performance comparison records.
    performance: Vec<PerfRecord>,
    /// Corruption recovery summary.
    recovery: RecoverySummary,
    /// Raw events (for downstream tools).
    events: Vec<DashboardEvent>,
}

/// Corpus metadata in the summary report.
#[derive(Debug, Clone, Serialize)]
struct CorpusSummary {
    databases: usize,
    total_bytes: u64,
    all_integrity_passed: bool,
}

/// Correctness verification in the summary report.
#[derive(Debug, Clone, Serialize)]
struct CorrectnessSummary {
    workloads_passed: usize,
    workloads_total: usize,
    total_operations: u64,
    all_matched: bool,
}

/// Recovery statistics in the summary report.
#[derive(Debug, Clone, Serialize)]
struct RecoverySummary {
    scenarios_recovered: usize,
    scenarios_total: usize,
    csqlite_recovered: usize,
    scenarios: Vec<RecoveryRecord>,
}

// ── Runtime suite config ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SuiteConfig {
    project_root: PathBuf,
    quick: bool,
    fixture_limit: usize,
    seed: u64,
    scale: u32,
    parity_concurrency_levels: Vec<u16>,
    benchmark_concurrency_levels: Vec<u16>,
    parity_presets: Vec<String>,
    benchmark_config: BenchmarkConfig,
}

impl Default for SuiteConfig {
    fn default() -> Self {
        Self {
            project_root: PathBuf::from("."),
            quick: false,
            fixture_limit: 6,
            seed: 42,
            scale: 40,
            parity_concurrency_levels: vec![1, 2, 4],
            benchmark_concurrency_levels: vec![1, 2, 4, 8, 16],
            parity_presets: vec![
                "deterministic_transform".to_owned(),
                "mixed_read_write".to_owned(),
                "schema_migration".to_owned(),
                "commutative_inserts_disjoint_keys".to_owned(),
                "hot_page_contention".to_owned(),
                "multi_table_foreign_keys".to_owned(),
            ],
            benchmark_config: BenchmarkConfig {
                warmup_iterations: 1,
                min_iterations: 4,
                measurement_time_secs: 2,
            },
        }
    }
}

impl SuiteConfig {
    fn from_args(args: &[String]) -> Self {
        let mut cfg = Self::default();
        if args.iter().any(|a| a == "--quick") {
            cfg.quick = true;
            cfg.fixture_limit = 3;
            cfg.scale = 20;
            cfg.parity_concurrency_levels = vec![1, 2];
            cfg.benchmark_concurrency_levels = vec![1, 2, 4, 8];
            cfg.parity_presets.truncate(4);
            cfg.benchmark_config = BenchmarkConfig {
                warmup_iterations: 1,
                min_iterations: 2,
                measurement_time_secs: 1,
            };
        }

        if let Some(project_root) = parse_option(args, "--project-root") {
            cfg.project_root = PathBuf::from(project_root);
        }
        if let Some(fixture_limit) = parse_option(args, "--fixture-limit")
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
        {
            cfg.fixture_limit = fixture_limit;
        }
        if let Some(seed) = parse_option(args, "--seed").and_then(|s| s.parse::<u64>().ok()) {
            cfg.seed = seed;
        }
        if let Some(scale) = parse_option(args, "--scale").and_then(|s| s.parse::<u32>().ok()) {
            cfg.scale = scale.max(1);
        }
        if let Some(csv) = parse_option(args, "--parity-concurrency") {
            let parsed = parse_u16_csv(csv);
            if !parsed.is_empty() {
                cfg.parity_concurrency_levels = parsed;
            }
        }
        if let Some(csv) = parse_option(args, "--bench-concurrency") {
            let parsed = parse_u16_csv(csv);
            if !parsed.is_empty() {
                cfg.benchmark_concurrency_levels = parsed;
            }
        }
        if let Some(csv) = parse_option(args, "--parity-presets") {
            let parsed = parse_csv(csv);
            if !parsed.is_empty() {
                cfg.parity_presets = parsed;
            }
        }

        cfg
    }
}

#[derive(Debug, Clone)]
struct FixtureSample {
    id: String,
    bytes: u64,
    integrity_ok: bool,
}

// ── Main ──────────────────────────────────────────────────────────────────

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return Ok(());
    }

    let headless = args.iter().any(|a| a == "--headless");
    let output_path = parse_output_path(&args);
    let suite_config = SuiteConfig::from_args(&args);

    if headless {
        let stop = AtomicBool::new(false);
        let report = run_real_suite(&suite_config, &stop, None);
        write_headless(&report, output_path.as_deref())?;
        return Ok(());
    }

    let (tx, rx) = mpsc::channel::<DashboardEvent>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_bg = stop.clone();

    let bg = std::thread::spawn(move || {
        let _ = run_real_suite(&suite_config, &stop_bg, Some(&tx));
    });
    let model = DashboardModel::new(rx, stop.clone());
    let res = App::new(model).screen_mode(ScreenMode::AltScreen).run();

    stop.store(true, Ordering::Relaxed);
    let _ = bg.join();
    res
}

fn parse_output_path(args: &[String]) -> Option<PathBuf> {
    parse_option(args, "--output").map(PathBuf::from)
}

fn parse_option<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == name && i + 1 < args.len() {
            return Some(args[i + 1].as_str());
        }
        i += 1;
    }
    None
}

fn parse_u16_csv(value: &str) -> Vec<u16> {
    value
        .split(',')
        .filter_map(|raw| raw.trim().parse::<u16>().ok())
        .filter(|n| *n > 0)
        .collect()
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn write_headless(out: &SummaryReport, path: Option<&Path>) -> std::io::Result<()> {
    let json =
        serde_json::to_string_pretty(out).map_err(|e| std::io::Error::other(e.to_string()))?;

    if let Some(p) = path {
        std::fs::write(p, json.as_bytes())?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn iso8601_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    format!("{}", now.as_secs())
}

#[allow(clippy::too_many_lines)]
fn run_real_suite(
    config: &SuiteConfig,
    stop: &AtomicBool,
    tx: Option<&mpsc::Sender<DashboardEvent>>,
) -> SummaryReport {
    let mut events = Vec::new();
    let mut emit = |event: DashboardEvent| {
        if let Some(ch) = tx {
            let _ = ch.send(event.clone());
        }
        events.push(event);
    };

    emit(DashboardEvent::StatusMessage {
        message: format!(
            "starting real suite: root={} quick={} fixtures<={}",
            config.project_root.display(),
            config.quick,
            config.fixture_limit
        ),
    });

    let fixtures = discover_fixtures(config, &mut emit);
    let corpus = CorpusSummary {
        databases: fixtures.len(),
        total_bytes: fixtures.iter().map(|f| f.bytes).sum(),
        all_integrity_passed: fixtures.iter().all(|f| f.integrity_ok),
    };
    emit(DashboardEvent::CorpusInfo {
        database_count: corpus.databases,
        total_bytes: corpus.total_bytes,
        all_integrity_passed: corpus.all_integrity_passed,
    });

    let correctness = run_correctness_phase(config, &fixtures, stop, &mut emit);
    let performance = if stop.load(Ordering::Relaxed) {
        Vec::new()
    } else {
        run_performance_phase(config, &fixtures, stop, &mut emit)
    };
    let recovery = if stop.load(Ordering::Relaxed) {
        RecoverySummary {
            scenarios_recovered: 0,
            scenarios_total: 0,
            csqlite_recovered: 0,
            scenarios: Vec::new(),
        }
    } else {
        run_recovery_phase(config, stop, &mut emit)
    };

    emit(DashboardEvent::StatusMessage {
        message: format!(
            "suite complete: parity {}/{} | perf {} comparisons | recovery {}/{}",
            correctness.workloads_passed,
            correctness.workloads_total,
            performance.len(),
            recovery.scenarios_recovered,
            recovery.scenarios_total
        ),
    });

    SummaryReport {
        timestamp: iso8601_now(),
        corpus,
        correctness,
        performance,
        recovery,
        events,
    }
}

fn discover_fixtures(
    config: &SuiteConfig,
    emit: &mut impl FnMut(DashboardEvent),
) -> Vec<FixtureSample> {
    let golden_dir = config.project_root.join("sample_sqlite_db_files/golden");
    let Ok(read_dir) = std::fs::read_dir(&golden_dir) else {
        emit(DashboardEvent::StatusMessage {
            message: format!("golden directory not found: {}", golden_dir.display()),
        });
        return Vec::new();
    };

    let mut candidates: Vec<(PathBuf, u64)> = read_dir
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("db"))
        .filter_map(|path| {
            let bytes = path.metadata().ok()?.len();
            Some((path, bytes))
        })
        .collect();

    candidates.sort_by_key(|entry| Reverse(entry.1));

    let mut fixtures = Vec::with_capacity(config.fixture_limit);
    let mut skipped_integrity = 0usize;
    let mut skipped_probe = 0usize;
    let mut emitted_probe_details = 0usize;
    for (path, bytes) in candidates {
        if fixtures.len() >= config.fixture_limit {
            break;
        }

        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let integrity_ok = sqlite_integrity_check(&path);
        if !integrity_ok {
            skipped_integrity += 1;
            continue;
        }

        if let Err(err) = probe_fsqlite_open_fixture(&path) {
            skipped_probe += 1;
            if emitted_probe_details < 5 {
                emit(DashboardEvent::StatusMessage {
                    message: format!("fixture skipped {stem}: fsqlite preflight failed: {err}"),
                });
                emitted_probe_details += 1;
            }
            continue;
        }

        fixtures.push(FixtureSample {
            id: stem.to_owned(),
            bytes,
            integrity_ok,
        });
    }

    emit(DashboardEvent::StatusMessage {
        message: format!(
            "fixture corpus selected: {} databases (skipped: integrity={} preflight={})",
            fixtures.len(),
            skipped_integrity,
            skipped_probe
        ),
    });
    fixtures
}

fn sqlite_integrity_check(path: &Path) -> bool {
    let Ok(conn) = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
    else {
        return false;
    };
    conn.query_row("PRAGMA integrity_check;", [], |row| row.get::<_, String>(0))
        .is_ok_and(|v| v == "ok")
}

fn probe_fsqlite_open_fixture(path: &Path) -> Result<(), String> {
    let temp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let db_name = path
        .file_name()
        .ok_or_else(|| format!("invalid fixture path: {}", path.display()))?;
    let db_copy = temp.path().join(db_name);
    std::fs::copy(path, &db_copy).map_err(|e| format!("copy db: {e}"))?;
    make_writable_for_probe(&db_copy).map_err(|e| format!("chmod db: {e}"))?;

    for ext in ["-wal", "-shm", "-journal"] {
        let mut src = path.as_os_str().to_os_string();
        src.push(ext);
        let sidecar_src = PathBuf::from(src);
        if !sidecar_src.exists() {
            continue;
        }
        let sidecar_name = sidecar_src
            .file_name()
            .ok_or_else(|| format!("invalid sidecar path: {}", sidecar_src.display()))?;
        let sidecar_copy = temp.path().join(sidecar_name);
        std::fs::copy(&sidecar_src, &sidecar_copy).map_err(|e| format!("copy sidecar: {e}"))?;
        make_writable_for_probe(&sidecar_copy).map_err(|e| format!("chmod sidecar: {e}"))?;
    }

    catch_unwind(AssertUnwindSafe(|| {
        let conn = FsqliteConnection::open(db_copy.display().to_string())
            .map_err(|e| format!("open failed: {e}"))?;
        let _ = conn
            .query("SELECT 1;")
            .map_err(|e| format!("probe query failed: {e}"))?;
        Ok(())
    }))
    .map_err(|payload| format!("probe panic: {}", panic_payload_to_string(payload)))?
}

fn make_writable_for_probe(path: &Path) -> std::io::Result<()> {
    let metadata = std::fs::metadata(path)?;

    #[cfg(unix)]
    {
        let mut mode = metadata.permissions().mode();
        let user_write = 0o200;
        if mode & user_write == 0 {
            mode |= user_write;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
        }
    }

    #[cfg(not(unix))]
    {
        let mut perms = metadata.permissions();
        if perms.readonly() {
            perms.set_readonly(false);
            std::fs::set_permissions(path, perms)?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_correctness_phase(
    config: &SuiteConfig,
    fixtures: &[FixtureSample],
    stop: &AtomicBool,
    emit: &mut impl FnMut(DashboardEvent),
) -> CorrectnessSummary {
    if fixtures.is_empty() {
        emit(DashboardEvent::StatusMessage {
            message: "correctness phase skipped: no fixtures".to_owned(),
        });
        return CorrectnessSummary {
            workloads_passed: 0,
            workloads_total: 0,
            total_operations: 0,
            all_matched: false,
        };
    }

    let total_cells =
        fixtures.len() * config.parity_presets.len() * config.parity_concurrency_levels.len();
    let mut completed = 0usize;
    let mut passed = 0usize;
    let mut total_ops = 0u64;

    emit(DashboardEvent::StatusMessage {
        message: format!(
            "correctness matrix start: {} fixtures × {} presets × {} conc",
            fixtures.len(),
            config.parity_presets.len(),
            config.parity_concurrency_levels.len()
        ),
    });
    emit(DashboardEvent::BenchmarkSuiteProgress {
        completed: 0,
        total: total_cells,
    });

    'outer: for fixture in fixtures {
        for preset in &config.parity_presets {
            let workload = format!("{}::{preset}", fixture.id);
            emit(DashboardEvent::CorrectnessWorkloadStart {
                workload: workload.clone(),
                total_ops: config.parity_concurrency_levels.len(),
            });

            for (idx, &concurrency) in config.parity_concurrency_levels.iter().enumerate() {
                if stop.load(Ordering::Relaxed) {
                    emit(DashboardEvent::StatusMessage {
                        message: "correctness phase interrupted".to_owned(),
                    });
                    break 'outer;
                }

                emit(DashboardEvent::CorrectnessOpProgress {
                    workload: workload.clone(),
                    ops_done: idx + 1,
                    total_ops: config.parity_concurrency_levels.len(),
                    current_sql: format!(
                        "run_matrix fixture={} preset={} c={} seed={}",
                        fixture.id, preset, concurrency, config.seed
                    ),
                });

                match run_single_parity_cell(config, &fixture.id, preset, concurrency) {
                    Ok(cell) => {
                        let matched = matches!(cell.verdict, CellVerdict::Pass { .. });
                        let result_workload = format!("{}::{preset}::c{concurrency}", fixture.id);
                        let frank_hash = best_hash(cell.fsqlite_report.as_ref());
                        let csqlite_hash = best_hash(cell.sqlite_report.as_ref());
                        emit(DashboardEvent::CorrectnessCheck {
                            workload: result_workload,
                            frank_hash,
                            csqlite_hash,
                            matched,
                        });

                        completed += 1;
                        if matched {
                            passed += 1;
                        } else {
                            emit(DashboardEvent::StatusMessage {
                                message: format!(
                                    "parity mismatch {}::{preset}::c{concurrency}: {}",
                                    fixture.id,
                                    cell_verdict_summary(&cell.verdict)
                                ),
                            });
                            if let Some(dir) = &cell.artifact_dir {
                                emit(DashboardEvent::StatusMessage {
                                    message: format!("mismatch artifact bundle: {dir}"),
                                });
                            }
                        }
                        total_ops = total_ops.saturating_add(ops_from_cell(&cell));
                    }
                    Err(err) => {
                        completed += 1;
                        emit(DashboardEvent::CorrectnessCheck {
                            workload: format!("{}::{preset}::c{concurrency}", fixture.id),
                            frank_hash: "error".to_owned(),
                            csqlite_hash: "error".to_owned(),
                            matched: false,
                        });
                        emit(DashboardEvent::StatusMessage {
                            message: format!(
                                "cell error {}::{preset}::c{concurrency}: {err}",
                                fixture.id
                            ),
                        });
                    }
                }

                emit(DashboardEvent::BenchmarkSuiteProgress {
                    completed,
                    total: total_cells,
                });
            }
        }
    }

    let all_matched = completed > 0 && passed == completed;
    CorrectnessSummary {
        workloads_passed: passed,
        workloads_total: completed,
        total_operations: total_ops,
        all_matched,
    }
}

fn run_single_parity_cell(
    config: &SuiteConfig,
    fixture_id: &str,
    preset: &str,
    concurrency: u16,
) -> Result<CellResult, String> {
    let batch = catch_unwind(AssertUnwindSafe(|| {
        run_matrix(&BatchConfig {
            project_root: config.project_root.clone(),
            fixture_ids: vec![fixture_id.to_owned()],
            preset_names: vec![preset.to_owned()],
            concurrency_levels: vec![concurrency],
            seeds: vec![config.seed],
            scale: config.scale,
            settings: HarnessSettings::default(),
            fail_fast: false,
            bundle_config: fsqlite_e2e::mismatch_artifacts::BundleConfig::default(),
        })
    }))
    .map_err(|payload| format!("batch runner panic: {}", panic_payload_to_string(payload)))?
    .map_err(|e| e.to_string())?;

    batch
        .cells
        .into_iter()
        .next()
        .ok_or_else(|| "batch runner returned no cells".to_owned())
}

type PerfCellStats = (f64, u64, u64, u64);
type PerfPair = (Option<PerfCellStats>, Option<PerfCellStats>);
type PerfPairMap = BTreeMap<(String, String, u16), PerfPair>;

#[allow(clippy::too_many_lines)]
fn run_performance_phase(
    config: &SuiteConfig,
    fixtures: &[FixtureSample],
    stop: &AtomicBool,
    emit: &mut impl FnMut(DashboardEvent),
) -> Vec<PerfRecord> {
    if fixtures.is_empty() || stop.load(Ordering::Relaxed) {
        return Vec::new();
    }

    let fixture_ids: Vec<String> = fixtures
        .iter()
        .take(if config.quick { 1 } else { 2 })
        .map(|f| f.id.clone())
        .collect();

    let mut showcase_cfg = ShowcaseConfig::new(fixture_ids, config.project_root.clone());
    showcase_cfg.seed = config.seed;
    showcase_cfg.scale = config.scale;
    showcase_cfg
        .concurrency_levels
        .clone_from(&config.benchmark_concurrency_levels);
    showcase_cfg.benchmark_config = config.benchmark_config.clone();
    showcase_cfg.settings = HarnessSettings::default();
    showcase_cfg.fail_fast = false;

    emit(DashboardEvent::StatusMessage {
        message: format!(
            "performance showcase start: c={:?} scale={} warmup={} min_iter={} time_floor={}s",
            showcase_cfg.concurrency_levels,
            showcase_cfg.scale,
            showcase_cfg.benchmark_config.warmup_iterations,
            showcase_cfg.benchmark_config.min_iterations,
            showcase_cfg.benchmark_config.measurement_time_secs
        ),
    });

    let showcase = match catch_unwind(AssertUnwindSafe(|| run_concurrency_showcase(&showcase_cfg)))
    {
        Ok(showcase) => showcase,
        Err(payload) => {
            emit(DashboardEvent::StatusMessage {
                message: format!(
                    "performance showcase panicked: {}",
                    panic_payload_to_string(payload)
                ),
            });
            return Vec::new();
        }
    };
    let mut grouped: PerfPairMap = BTreeMap::new();

    for cell in &showcase.perf.cells {
        let Some(summary) = cell.summary.as_ref() else {
            continue;
        };
        let failed_iterations = summary
            .iterations
            .iter()
            .filter(|it| it.error.is_some())
            .count();
        if failed_iterations > 0 {
            emit(DashboardEvent::StatusMessage {
                message: format!(
                    "perf cell skipped {}:{}:{}:c{} ({} failed iteration(s))",
                    summary.engine,
                    summary.fixture_id,
                    summary.workload,
                    summary.concurrency,
                    failed_iterations
                ),
            });
            continue;
        }

        let median_ops = summary.throughput.median_ops_per_sec;
        if median_ops <= 0.0 {
            emit(DashboardEvent::StatusMessage {
                message: format!(
                    "perf cell skipped {}:{}:{}:c{} (non-positive throughput)",
                    summary.engine, summary.fixture_id, summary.workload, summary.concurrency
                ),
            });
            continue;
        }

        let retries = summary.iterations.iter().map(|it| it.retries).sum::<u64>();
        let aborts = summary.iterations.iter().map(|it| it.aborts).sum::<u64>();
        let entry = grouped
            .entry((
                summary.fixture_id.clone(),
                summary.workload.clone(),
                summary.concurrency,
            ))
            .or_default();
        let payload = (median_ops, summary.total_measurement_ms, retries, aborts);
        if summary.engine == "sqlite3" {
            entry.0 = Some(payload);
        } else if summary.engine == "fsqlite" {
            entry.1 = Some(payload);
        }
    }

    let comparable = grouped
        .values()
        .filter(|(sq, fs)| sq.is_some() && fs.is_some())
        .count();
    emit(DashboardEvent::BenchmarkSuiteProgress {
        completed: 0,
        total: comparable,
    });

    let mut completed = 0usize;
    let mut records = Vec::new();
    for ((fixture_id, workload, concurrency), (sqlite, frank)) in grouped {
        let (
            Some((sqlite_ops, sqlite_ms, sqlite_retries, sqlite_aborts)),
            Some((frank_ops, frank_ms, frank_retries, frank_aborts)),
        ) = (sqlite, frank)
        else {
            continue;
        };
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let name = format!("{fixture_id}::{workload}_c{concurrency}");
        emit(DashboardEvent::BenchmarkProgress {
            name: name.clone(),
            ops_per_sec: frank_ops,
            elapsed_ms: frank_ms,
        });
        emit(DashboardEvent::BenchmarkCsqliteProgress {
            name: name.clone(),
            ops_per_sec: sqlite_ops,
            elapsed_ms: sqlite_ms,
        });
        emit(DashboardEvent::BenchmarkComplete {
            name: name.clone(),
            wall_time_ms: frank_ms,
            ops_per_sec: frank_ops,
        });
        emit(DashboardEvent::BenchmarkComparison {
            name: name.clone(),
            frank_ops_per_sec: frank_ops,
            csqlite_ops_per_sec: sqlite_ops,
        });
        emit(DashboardEvent::StatusMessage {
            message: format!(
                "perf {name}: sqlite retries={sqlite_retries} aborts={sqlite_aborts} | frank retries={frank_retries} aborts={frank_aborts}"
            ),
        });

        let speedup = if sqlite_ops > 0.0 {
            frank_ops / sqlite_ops
        } else {
            0.0
        };
        records.push(PerfRecord {
            name,
            frank_ops_per_sec: frank_ops,
            csqlite_ops_per_sec: sqlite_ops,
            speedup,
        });

        completed += 1;
        emit(DashboardEvent::BenchmarkSuiteProgress {
            completed,
            total: comparable,
        });
    }

    records
}

#[allow(clippy::too_many_lines)]
fn run_recovery_phase(
    config: &SuiteConfig,
    stop: &AtomicBool,
    emit: &mut impl FnMut(DashboardEvent),
) -> RecoverySummary {
    let mut scenarios = scenario_catalog();
    if config.quick {
        scenarios.truncate(4);
    }

    emit(DashboardEvent::StatusMessage {
        message: format!("recovery suite start: {} scenarios", scenarios.len()),
    });

    let mut records = Vec::new();
    let mut frank_recovered_total = 0usize;
    let mut csqlite_recovered_total = 0usize;

    for scenario in scenarios {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        emit(DashboardEvent::StatusMessage {
            message: format!("recovery scenario: {}", scenario.name),
        });

        let sqlite_result = tempfile::tempdir()
            .ok()
            .and_then(|dir| run_sqlite_corruption_scenario(&scenario, dir.path()).ok());
        if let Some(result) = &sqlite_result {
            emit(DashboardEvent::CsqliteIntegrityResult {
                passed: result.integrity_ok,
                message: result
                    .integrity_check
                    .clone()
                    .or_else(|| result.error.clone())
                    .unwrap_or_else(|| "no integrity output".to_owned()),
            });
            if let Err(err) = verify_sqlite_result(result, &scenario) {
                emit(DashboardEvent::StatusMessage {
                    message: format!("sqlite expectation mismatch {}: {err}", scenario.name),
                });
            }
        }

        let frank = match catch_unwind(AssertUnwindSafe(|| {
            run_fsqlite_recovery_scenario(&scenario)
        })) {
            Ok(report) => report,
            Err(payload) => {
                let panic_msg = panic_payload_to_string(payload);
                emit(DashboardEvent::StatusMessage {
                    message: format!(
                        "frank recovery panic in scenario {}: {}",
                        scenario.name, panic_msg
                    ),
                });
                let csqlite_recovered = sqlite_result.as_ref().is_some_and(|result| {
                    result.open_succeeded
                        && result.integrity_ok
                        && result.rows_recovered == Some(result.rows_inserted)
                });
                if csqlite_recovered {
                    csqlite_recovered_total += 1;
                }
                let record = RecoveryRecord {
                    scenario: scenario.name.to_owned(),
                    frank_recovered: false,
                    csqlite_recovered,
                };
                emit(DashboardEvent::RecoveryFailure {
                    page: 0,
                    reason: format!("panic: {panic_msg}"),
                });
                emit(DashboardEvent::RecoveryScenarioComplete {
                    scenario: record.scenario.clone(),
                    frank_recovered: record.frank_recovered,
                    csqlite_recovered: record.csqlite_recovered,
                });
                records.push(record);
                continue;
            }
        };
        let page = frank
            .corruption_report
            .as_ref()
            .and_then(|r| r.affected_pages.first().copied())
            .unwrap_or(0);
        emit(DashboardEvent::CorruptionInjected {
            page,
            pattern: format!("{:?}", scenario.pattern),
        });

        if let Some(log) = &frank.recovery_log {
            emit(DashboardEvent::RecoveryAttempt {
                group: log.group_id.end_frame_no,
                symbols_available: log.available_symbols,
                needed: log.required_symbols,
            });
            emit(DashboardEvent::RecoveryPhaseUpdate {
                phase: "verify".to_owned(),
                symbols_resolved: log.validated_source_symbols + log.validated_repair_symbols,
            });
            emit(DashboardEvent::RecoveryPhaseUpdate {
                phase: if log.decode_succeeded {
                    "decode".to_owned()
                } else {
                    "fallback".to_owned()
                },
                symbols_resolved: log.available_symbols.min(log.required_symbols),
            });
        }

        if frank.recovery_succeeded {
            emit(DashboardEvent::RecoverySuccess {
                page,
                decode_proof: frank.verdict.clone(),
            });
            frank_recovered_total += 1;
        } else {
            emit(DashboardEvent::RecoveryFailure {
                page,
                reason: frank.verdict.clone(),
            });
        }
        if !frank.passed {
            emit(DashboardEvent::StatusMessage {
                message: format!(
                    "frank scenario expectation mismatch: {}",
                    frank.scenario_name
                ),
            });
        }

        let csqlite_recovered = sqlite_result.as_ref().is_some_and(|result| {
            result.open_succeeded
                && result.integrity_ok
                && result.rows_recovered == Some(result.rows_inserted)
        });
        if csqlite_recovered {
            csqlite_recovered_total += 1;
        }

        let record = RecoveryRecord {
            scenario: scenario.name.to_owned(),
            frank_recovered: frank.recovery_succeeded,
            csqlite_recovered,
        };
        emit(DashboardEvent::RecoveryScenarioComplete {
            scenario: record.scenario.clone(),
            frank_recovered: record.frank_recovered,
            csqlite_recovered: record.csqlite_recovered,
        });
        records.push(record);
    }

    RecoverySummary {
        scenarios_recovered: frank_recovered_total,
        scenarios_total: records.len(),
        csqlite_recovered: csqlite_recovered_total,
        scenarios: records,
    }
}

fn best_hash(report: Option<&EngineRunReport>) -> String {
    let Some(report) = report else {
        return "n/a".to_owned();
    };
    report
        .correctness
        .canonical_sha256
        .clone()
        .or_else(|| report.correctness.logical_sha256.clone())
        .or_else(|| report.correctness.raw_sha256.clone())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}

fn ops_from_cell(cell: &CellResult) -> u64 {
    let sqlite_ops = cell.sqlite_report.as_ref().map_or(0, |r| r.ops_total);
    let frank_ops = cell.fsqlite_report.as_ref().map_or(0, |r| r.ops_total);
    sqlite_ops.max(frank_ops)
}

fn cell_verdict_summary(verdict: &CellVerdict) -> String {
    match verdict {
        CellVerdict::Pass { achieved_tier } => format!("pass ({achieved_tier})"),
        CellVerdict::Mismatch {
            expected_tier,
            achieved_tier,
            detail,
        } => format!(
            "mismatch expected={expected_tier} achieved={} detail={detail}",
            achieved_tier.as_deref().unwrap_or("none")
        ),
        CellVerdict::Error(err) => format!("error: {err}"),
    }
}

fn print_help() {
    let text = "\
e2e-dashboard — FrankenTUI evidence dashboard for FrankenSQLite E2E runs

USAGE:
    e2e-dashboard [OPTIONS]

OPTIONS:
    --headless                  Run suite without TUI and emit JSON report
    --output <FILE>             Write headless JSON output to file
    --quick                     Reduced matrix for faster iteration
    --project-root <DIR>        Project root (default: .)
    --fixture-limit <N>         Max golden fixtures to include (default: 6)
    --seed <N>                  Deterministic seed (default: 42)
    --scale <N>                 Workload scale factor (default: 40; quick: 20)
    --parity-concurrency <CSV>  Correctness matrix concurrency levels
    --bench-concurrency <CSV>   Performance sweep concurrency levels
    --parity-presets <CSV>      Workload preset names for parity matrix
    -h, --help                  Show this help

PANELS:
    Benchmark       Throughput + speedup comparisons from real benchmark runs
    Recovery        Corruption/recovery status from real scenario execution
    Correctness     Per-cell parity checks and hash comparisons
    Summary         Corpus, parity, perf, and recovery aggregates

ADVANCED TUI:
    Ctrl+P          Open command palette
    l               Toggle full log overlay (LogViewer)
    Tab/Shift-Tab   Cycle active panel
    r               Reset dashboard state buffers
    q               Quit
";
    print!("{text}");
}
