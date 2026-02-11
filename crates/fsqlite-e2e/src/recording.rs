//! Demo Recording Mode: deterministic, non-interactive, reproducible demo runs.
//!
//! Bead: bd-2als.5.4
//!
//! Provides a unified "recording mode" configuration that sets all determinism
//! knobs at once — fixed seeds, stable output paths, non-interactive defaults,
//! structured logging, and CI-friendly (no-TTY) operation.
//!
//! # Design
//!
//! Recording mode is a *meta-configuration layer* that consolidates settings
//! scattered across [`crate::workload::WorkloadConfig`], the CLI binaries, and
//! harness settings into a single [`RecordingConfig`] struct.  Named
//! [`RecordingPreset`]s provide one-flag shortcuts for common demo scenarios.
//!
//! # Presets
//!
//! | Preset | Description | Seed | Workload |
//! |--------|-------------|------|----------|
//! | `CorruptionRecovery` | WAL-FEC corruption + recovery walkthrough | 2024 | 8 scenarios |
//! | `PerfScaling` | Concurrent writer scaling (1..32 workers) | 42 | disjoint + hot-page |
//! | `CorrectnessBaseline` | Hash-match correctness for all fixtures | 7 | commutative inserts |
//! | `FullSuite` | All of the above, sequentially | 42 | all |

use std::fmt;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// ── Recording preset ─────────────────────────────────────────────────

/// Named presets that bundle all recording-mode knobs for a specific demo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingPreset {
    /// WAL-FEC corruption injection + recovery walkthrough.
    CorruptionRecovery,
    /// Concurrent-writer performance scaling (1..32 workers).
    PerfScaling,
    /// Correctness hash-match baseline across all fixtures.
    CorrectnessBaseline,
    /// Full suite: corruption + perf + correctness.
    FullSuite,
}

impl RecordingPreset {
    /// Human-readable label for reports and filenames.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::CorruptionRecovery => "corruption-recovery",
            Self::PerfScaling => "perf-scaling",
            Self::CorrectnessBaseline => "correctness-baseline",
            Self::FullSuite => "full-suite",
        }
    }

    /// Default seed for this preset.
    #[must_use]
    pub fn default_seed(self) -> u64 {
        match self {
            Self::CorruptionRecovery => 2024,
            Self::PerfScaling | Self::FullSuite => 42,
            Self::CorrectnessBaseline => 7,
        }
    }

    /// Parse a preset name (case-insensitive, accepts hyphens or underscores).
    #[must_use]
    pub fn from_str_loose(s: &str) -> Option<Self> {
        let normalised = s.to_lowercase().replace('-', "_");
        match normalised.as_str() {
            "corruption_recovery" | "corruption" | "recovery" => Some(Self::CorruptionRecovery),
            "perf_scaling" | "perf" | "scaling" | "benchmark" => Some(Self::PerfScaling),
            "correctness_baseline" | "correctness" | "baseline" => Some(Self::CorrectnessBaseline),
            "full_suite" | "full" | "all" => Some(Self::FullSuite),
            _ => None,
        }
    }

    /// List all preset names (for help text / error messages).
    #[must_use]
    pub fn all_labels() -> &'static [&'static str] {
        &[
            "corruption-recovery",
            "perf-scaling",
            "correctness-baseline",
            "full-suite",
        ]
    }
}

impl fmt::Display for RecordingPreset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

// ── Recording configuration ──────────────────────────────────────────

/// Unified recording-mode configuration.
///
/// When recording mode is active, all tools use these settings instead of
/// their own defaults.  This ensures that every run produces identical,
/// deterministic output for video capture and CI regression baselines.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct RecordingConfig {
    /// The preset that created this config (if any).
    pub preset: Option<RecordingPreset>,

    /// RNG seed — every random decision in workload generation, corruption
    /// injection, and fixture selection uses this seed (or a deterministic
    /// derivative).
    pub seed: u64,

    /// Output directory — stable, predictable path (not timestamp-based).
    ///
    /// Default: `sample_sqlite_db_files/recordings/<preset-label>`.
    pub output_dir: PathBuf,

    /// Whether to suppress ANSI colour codes in text output.
    pub no_color: bool,

    /// Whether to emit structured JSON instead of human-readable text.
    pub json_output: bool,

    /// Quiet mode — suppress progress spinners, animations, and non-essential
    /// status output.  Structured log lines are still emitted.
    pub quiet: bool,

    /// Whether to write an event log (JSONL) capturing every `RecordingEvent`
    /// emitted during the session.
    pub capture_events: bool,

    /// Maximum wall-clock seconds the recording may run before being stopped.
    /// `None` means no limit.
    pub timeout_secs: Option<u64>,
}

impl RecordingConfig {
    /// Build a recording config from a preset with default paths.
    #[must_use]
    pub fn from_preset(preset: RecordingPreset) -> Self {
        Self {
            preset: Some(preset),
            seed: preset.default_seed(),
            output_dir: PathBuf::from(format!(
                "sample_sqlite_db_files/recordings/{}",
                preset.label()
            )),
            no_color: true,
            json_output: false,
            quiet: false,
            capture_events: true,
            timeout_secs: None,
        }
    }

    /// Ensure the output directory exists.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be created.
    pub fn ensure_output_dir(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.output_dir)
    }

    /// Path to the event log file.
    #[must_use]
    pub fn event_log_path(&self) -> PathBuf {
        self.output_dir.join("events.jsonl")
    }

    /// Path to the final summary report (JSON).
    #[must_use]
    pub fn summary_json_path(&self) -> PathBuf {
        self.output_dir.join("summary.json")
    }

    /// Path to the final summary report (Markdown).
    #[must_use]
    pub fn summary_md_path(&self) -> PathBuf {
        self.output_dir.join("summary.md")
    }
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self {
            preset: None,
            seed: 42,
            output_dir: PathBuf::from("sample_sqlite_db_files/recordings/default"),
            no_color: true,
            json_output: false,
            quiet: false,
            capture_events: true,
            timeout_secs: None,
        }
    }
}

// ── Recording events ─────────────────────────────────────────────────

/// Timestamped events emitted during a recording session.
///
/// These are written to `events.jsonl` for replay, analysis, or video
/// subtitle generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RecordingEvent {
    /// Session started with this config.
    SessionStart {
        seed: u64,
        preset: Option<String>,
        timestamp: String,
    },

    /// A named phase began (e.g. "corruption-injection", "recovery", "benchmark").
    PhaseStart { name: String, description: String },

    /// Progress within a phase.
    Progress {
        phase: String,
        step: usize,
        total: usize,
        detail: String,
    },

    /// A phase completed.
    PhaseComplete {
        name: String,
        duration_ms: u64,
        outcome: String,
    },

    /// Informational message (logged but not an error).
    Info { message: String },

    /// Warning (non-fatal).
    Warning { message: String },

    /// Session ended.
    SessionEnd {
        duration_ms: u64,
        total_events: usize,
        outcome: String,
    },
}

// ── Recording session ────────────────────────────────────────────────

/// A recording session captures events and writes them to the event log.
pub struct RecordingSession {
    config: RecordingConfig,
    events: Vec<TimestampedEvent>,
    start_ms: u64,
}

/// An event with its wall-clock offset from session start.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedEvent {
    /// Milliseconds since session start.
    pub offset_ms: u64,
    /// The event payload.
    pub event: RecordingEvent,
}

impl RecordingSession {
    /// Start a new recording session.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the output directory cannot be created.
    pub fn start(config: RecordingConfig) -> std::io::Result<Self> {
        config.ensure_output_dir()?;
        let start_ms = epoch_ms();
        let mut session = Self {
            config,
            events: Vec::with_capacity(256),
            start_ms,
        };
        session.emit(RecordingEvent::SessionStart {
            seed: session.config.seed,
            preset: session.config.preset.map(|p| p.label().to_owned()),
            timestamp: epoch_iso(start_ms),
        });
        Ok(session)
    }

    /// Record an event.
    pub fn emit(&mut self, event: RecordingEvent) {
        let offset_ms = epoch_ms().saturating_sub(self.start_ms);
        self.events.push(TimestampedEvent { offset_ms, event });
    }

    /// Signal the start of a named phase.
    pub fn phase_start(&mut self, name: &str, description: &str) {
        self.emit(RecordingEvent::PhaseStart {
            name: name.to_owned(),
            description: description.to_owned(),
        });
    }

    /// Signal progress within a phase.
    pub fn progress(&mut self, phase: &str, step: usize, total: usize, detail: &str) {
        self.emit(RecordingEvent::Progress {
            phase: phase.to_owned(),
            step,
            total,
            detail: detail.to_owned(),
        });
    }

    /// Signal phase completion.
    pub fn phase_complete(&mut self, name: &str, duration_ms: u64, outcome: &str) {
        self.emit(RecordingEvent::PhaseComplete {
            name: name.to_owned(),
            duration_ms,
            outcome: outcome.to_owned(),
        });
    }

    /// Emit an informational message.
    pub fn info(&mut self, message: &str) {
        self.emit(RecordingEvent::Info {
            message: message.to_owned(),
        });
    }

    /// Emit a warning.
    pub fn warning(&mut self, message: &str) {
        self.emit(RecordingEvent::Warning {
            message: message.to_owned(),
        });
    }

    /// Access the recording config.
    #[must_use]
    pub fn config(&self) -> &RecordingConfig {
        &self.config
    }

    /// Number of events captured so far.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Borrow all captured events.
    #[must_use]
    pub fn events(&self) -> &[TimestampedEvent] {
        &self.events
    }

    /// Finish the session: emit `SessionEnd`, flush events to JSONL, and
    /// write summary artifacts.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if artifacts cannot be written.
    pub fn finish(mut self, outcome: &str) -> std::io::Result<RecordingSummary> {
        let duration_ms = epoch_ms().saturating_sub(self.start_ms);
        let total_events = self.events.len() + 1; // +1 for the SessionEnd event itself
        self.emit(RecordingEvent::SessionEnd {
            duration_ms,
            total_events,
            outcome: outcome.to_owned(),
        });

        let summary = RecordingSummary {
            seed: self.config.seed,
            preset: self.config.preset.map(|p| p.label().to_owned()),
            duration_ms,
            total_events: self.events.len(),
            outcome: outcome.to_owned(),
            output_dir: self.config.output_dir.clone(),
        };

        if self.config.capture_events {
            self.write_event_log()?;
        }
        self.write_summary_json(&summary)?;
        self.write_summary_md(&summary)?;

        Ok(summary)
    }

    /// Write all events to `events.jsonl`.
    fn write_event_log(&self) -> std::io::Result<()> {
        let path = self.config.event_log_path();
        let mut buf = String::with_capacity(self.events.len() * 128);
        for te in &self.events {
            // Manual JSON to avoid serde_json dependency on the hot path.
            let _ = writeln!(
                buf,
                "{{\"offset_ms\":{},\"event\":{}}}",
                te.offset_ms,
                event_to_json(&te.event)
            );
        }
        fs::write(path, buf)
    }

    /// Write `summary.json`.
    fn write_summary_json(&self, summary: &RecordingSummary) -> std::io::Result<()> {
        let path = self.config.summary_json_path();
        let json = summary_to_json(summary);
        fs::write(path, json)
    }

    /// Write `summary.md`.
    fn write_summary_md(&self, summary: &RecordingSummary) -> std::io::Result<()> {
        let path = self.config.summary_md_path();
        let md = render_summary_md(summary, &self.events);
        fs::write(path, md)
    }
}

// ── Summary ──────────────────────────────────────────────────────────

/// Post-session summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingSummary {
    pub seed: u64,
    pub preset: Option<String>,
    pub duration_ms: u64,
    pub total_events: usize,
    pub outcome: String,
    pub output_dir: PathBuf,
}

// ── CLI helpers ──────────────────────────────────────────────────────

/// Parse recording-mode flags from a CLI argument list.
///
/// Looks for:
/// - `--record` — enable recording mode with default config.
/// - `--record-preset <NAME>` — enable with a named preset.
/// - `--record-seed <N>` — override seed.
/// - `--record-output <DIR>` — override output directory.
///
/// Returns `None` if recording mode is not requested.
#[must_use]
pub fn parse_recording_args(args: &[String]) -> Option<RecordingConfig> {
    let has_record = args.iter().any(|a| a == "--record");
    let preset =
        find_flag_value(args, "--record-preset").and_then(|s| RecordingPreset::from_str_loose(&s));

    if !has_record && preset.is_none() {
        return None;
    }

    let mut config = if let Some(p) = preset {
        RecordingConfig::from_preset(p)
    } else {
        RecordingConfig::default()
    };

    if let Some(seed_str) = find_flag_value(args, "--record-seed") {
        if let Ok(s) = seed_str.parse::<u64>() {
            config.seed = s;
        }
    }

    if let Some(dir) = find_flag_value(args, "--record-output") {
        config.output_dir = PathBuf::from(dir);
    }

    if args.iter().any(|a| a == "--json") {
        config.json_output = true;
    }

    if args.iter().any(|a| a == "--no-color") {
        config.no_color = true;
    }

    if args.iter().any(|a| a == "--quiet" || a == "-q") {
        config.quiet = true;
    }

    Some(config)
}

/// Find the value following a flag in an argument list.
fn find_flag_value(args: &[String], flag: &str) -> Option<String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == flag {
            return iter.next().cloned();
        }
    }
    None
}

/// Print help text for recording-mode flags.
#[must_use]
pub fn recording_help_text() -> &'static str {
    "\
RECORDING MODE:
    --record                 Enable recording mode (deterministic, non-interactive)
    --record-preset <NAME>   Use a named preset:
                               corruption-recovery  WAL-FEC corruption + recovery demo
                               perf-scaling         Concurrent writer scaling (1..32)
                               correctness-baseline Hash-match correctness baseline
                               full-suite           All demos sequentially
    --record-seed <N>        Override the preset's default RNG seed
    --record-output <DIR>    Override the output directory
    --quiet, -q              Suppress progress animations and non-essential output"
}

// ── Stable output path helpers ───────────────────────────────────────

/// Compute a stable, filesystem-safe run directory name.
///
/// Unlike the default timestamp-based names, recording-mode paths are
/// human-readable and version-control-friendly.
#[must_use]
pub fn stable_run_dir(base: &Path, preset: RecordingPreset, seed: u64) -> PathBuf {
    base.join(format!("{}-seed{seed}", preset.label()))
}

// ── JSON serialisation (no serde_json dependency) ────────────────────

fn event_to_json(event: &RecordingEvent) -> String {
    match event {
        RecordingEvent::SessionStart {
            seed,
            preset,
            timestamp,
        } => {
            let preset_val = preset
                .as_deref()
                .map_or_else(|| "null".to_owned(), |p| format!("\"{p}\""));
            format!(
                "{{\"kind\":\"session_start\",\"seed\":{seed},\"preset\":{preset_val},\"timestamp\":\"{timestamp}\"}}"
            )
        }
        RecordingEvent::PhaseStart { name, description } => {
            format!(
                "{{\"kind\":\"phase_start\",\"name\":\"{}\",\"description\":\"{}\"}}",
                json_escape(name),
                json_escape(description)
            )
        }
        RecordingEvent::Progress {
            phase,
            step,
            total,
            detail,
        } => {
            format!(
                "{{\"kind\":\"progress\",\"phase\":\"{}\",\"step\":{step},\"total\":{total},\"detail\":\"{}\"}}",
                json_escape(phase),
                json_escape(detail)
            )
        }
        RecordingEvent::PhaseComplete {
            name,
            duration_ms,
            outcome,
        } => {
            format!(
                "{{\"kind\":\"phase_complete\",\"name\":\"{}\",\"duration_ms\":{duration_ms},\"outcome\":\"{}\"}}",
                json_escape(name),
                json_escape(outcome)
            )
        }
        RecordingEvent::Info { message } => {
            format!(
                "{{\"kind\":\"info\",\"message\":\"{}\"}}",
                json_escape(message)
            )
        }
        RecordingEvent::Warning { message } => {
            format!(
                "{{\"kind\":\"warning\",\"message\":\"{}\"}}",
                json_escape(message)
            )
        }
        RecordingEvent::SessionEnd {
            duration_ms,
            total_events,
            outcome,
        } => {
            format!(
                "{{\"kind\":\"session_end\",\"duration_ms\":{duration_ms},\"total_events\":{total_events},\"outcome\":\"{}\"}}",
                json_escape(outcome)
            )
        }
    }
}

fn summary_to_json(summary: &RecordingSummary) -> String {
    let preset_val = summary
        .as_preset_str()
        .map_or_else(|| "null".to_owned(), |p| format!("\"{p}\""));
    format!(
        "{{\n  \"seed\": {},\n  \"preset\": {},\n  \"duration_ms\": {},\n  \"total_events\": {},\n  \"outcome\": \"{}\",\n  \"output_dir\": \"{}\"\n}}",
        summary.seed,
        preset_val,
        summary.duration_ms,
        summary.total_events,
        json_escape(&summary.outcome),
        json_escape(&summary.output_dir.display().to_string())
    )
}

impl RecordingSummary {
    fn as_preset_str(&self) -> Option<&str> {
        self.preset.as_deref()
    }
}

fn render_summary_md(summary: &RecordingSummary, events: &[TimestampedEvent]) -> String {
    let mut out = String::with_capacity(1024);
    let _ = writeln!(out, "# Recording Summary\n");
    let _ = writeln!(out, "- **Seed:** {}", summary.seed);
    if let Some(preset) = &summary.preset {
        let _ = writeln!(out, "- **Preset:** {preset}");
    }
    let _ = writeln!(out, "- **Duration:** {}ms", summary.duration_ms);
    let _ = writeln!(out, "- **Events:** {}", summary.total_events);
    let _ = writeln!(out, "- **Outcome:** {}", summary.outcome);
    let _ = writeln!(out, "- **Output:** {}", summary.output_dir.display());

    // Phase timeline.
    let phases: Vec<&TimestampedEvent> = events
        .iter()
        .filter(|te| matches!(te.event, RecordingEvent::PhaseComplete { .. }))
        .collect();

    if !phases.is_empty() {
        let _ = writeln!(out, "\n## Phases\n");
        let _ = writeln!(out, "| Phase | Duration | Outcome |");
        let _ = writeln!(out, "|-------|----------|---------|");
        for te in &phases {
            if let RecordingEvent::PhaseComplete {
                name,
                duration_ms,
                outcome,
            } = &te.event
            {
                let _ = writeln!(out, "| {name} | {duration_ms}ms | {outcome} |");
            }
        }
    }

    out
}

/// Minimal JSON string escaping.
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

/// Current epoch time in milliseconds.
fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// ISO-ish timestamp from epoch milliseconds.
fn epoch_iso(ms: u64) -> String {
    // Simple format without chrono dependency.
    format!("{}", ms / 1000)
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preset_from_str_loose() {
        assert_eq!(
            RecordingPreset::from_str_loose("corruption-recovery"),
            Some(RecordingPreset::CorruptionRecovery)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("corruption_recovery"),
            Some(RecordingPreset::CorruptionRecovery)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("corruption"),
            Some(RecordingPreset::CorruptionRecovery)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("recovery"),
            Some(RecordingPreset::CorruptionRecovery)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("perf-scaling"),
            Some(RecordingPreset::PerfScaling)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("perf"),
            Some(RecordingPreset::PerfScaling)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("benchmark"),
            Some(RecordingPreset::PerfScaling)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("correctness-baseline"),
            Some(RecordingPreset::CorrectnessBaseline)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("correctness"),
            Some(RecordingPreset::CorrectnessBaseline)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("full-suite"),
            Some(RecordingPreset::FullSuite)
        );
        assert_eq!(
            RecordingPreset::from_str_loose("all"),
            Some(RecordingPreset::FullSuite)
        );
        assert_eq!(RecordingPreset::from_str_loose("nonexistent"), None);
    }

    #[test]
    fn test_preset_labels() {
        assert_eq!(
            RecordingPreset::CorruptionRecovery.label(),
            "corruption-recovery"
        );
        assert_eq!(RecordingPreset::PerfScaling.label(), "perf-scaling");
        assert_eq!(
            RecordingPreset::CorrectnessBaseline.label(),
            "correctness-baseline"
        );
        assert_eq!(RecordingPreset::FullSuite.label(), "full-suite");
    }

    #[test]
    fn test_preset_default_seeds() {
        assert_eq!(RecordingPreset::CorruptionRecovery.default_seed(), 2024);
        assert_eq!(RecordingPreset::PerfScaling.default_seed(), 42);
        assert_eq!(RecordingPreset::CorrectnessBaseline.default_seed(), 7);
        assert_eq!(RecordingPreset::FullSuite.default_seed(), 42);
    }

    #[test]
    fn test_config_from_preset() {
        let config = RecordingConfig::from_preset(RecordingPreset::CorruptionRecovery);
        assert_eq!(config.seed, 2024);
        assert!(config.no_color);
        assert!(config.capture_events);
        assert_eq!(
            config.output_dir,
            PathBuf::from("sample_sqlite_db_files/recordings/corruption-recovery")
        );
    }

    #[test]
    fn test_config_paths() {
        let config = RecordingConfig::from_preset(RecordingPreset::PerfScaling);
        assert!(config.event_log_path().ends_with("events.jsonl"));
        assert!(config.summary_json_path().ends_with("summary.json"));
        assert!(config.summary_md_path().ends_with("summary.md"));
    }

    #[test]
    fn test_parse_recording_args_absent() {
        let args: Vec<String> = vec!["e2e-runner".into(), "run-all".into()];
        assert!(parse_recording_args(&args).is_none());
    }

    #[test]
    fn test_parse_recording_args_basic() {
        let args: Vec<String> = vec!["e2e-runner".into(), "--record".into(), "run-all".into()];
        let config = parse_recording_args(&args).expect("should parse --record");
        assert_eq!(config.seed, 42);
        assert!(config.no_color);
    }

    #[test]
    fn test_parse_recording_args_preset() {
        let args: Vec<String> = vec![
            "e2e-runner".into(),
            "--record-preset".into(),
            "corruption-recovery".into(),
        ];
        let config = parse_recording_args(&args).expect("should parse preset");
        assert_eq!(config.preset, Some(RecordingPreset::CorruptionRecovery));
        assert_eq!(config.seed, 2024);
    }

    #[test]
    fn test_parse_recording_args_overrides() {
        let args: Vec<String> = vec![
            "e2e-runner".into(),
            "--record".into(),
            "--record-seed".into(),
            "99".into(),
            "--record-output".into(),
            "/tmp/rec".into(),
            "--json".into(),
            "--quiet".into(),
        ];
        let config = parse_recording_args(&args).expect("should parse");
        assert_eq!(config.seed, 99);
        assert_eq!(config.output_dir, PathBuf::from("/tmp/rec"));
        assert!(config.json_output);
        assert!(config.quiet);
    }

    #[test]
    fn test_session_lifecycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = RecordingConfig {
            output_dir: dir.path().to_path_buf(),
            ..RecordingConfig::default()
        };

        let mut session = RecordingSession::start(config).unwrap();
        assert_eq!(session.event_count(), 1); // SessionStart

        session.phase_start("test-phase", "A test phase");
        session.progress("test-phase", 1, 3, "step 1 done");
        session.progress("test-phase", 2, 3, "step 2 done");
        session.progress("test-phase", 3, 3, "step 3 done");
        session.phase_complete("test-phase", 100, "passed");
        session.info("Everything went well");

        let summary = session.finish("success").unwrap();

        assert_eq!(summary.seed, 42);
        assert_eq!(summary.outcome, "success");
        assert!(summary.total_events >= 7); // Start + phase_start + 3 progress + phase_complete + info + end

        // Verify artifacts were written.
        assert!(dir.path().join("events.jsonl").exists());
        assert!(dir.path().join("summary.json").exists());
        assert!(dir.path().join("summary.md").exists());

        // Verify JSONL has the right number of lines.
        let events_content = fs::read_to_string(dir.path().join("events.jsonl")).unwrap();
        let line_count = events_content.lines().count();
        assert_eq!(line_count, summary.total_events);

        // Verify summary.json is valid.
        let summary_json = fs::read_to_string(dir.path().join("summary.json")).unwrap();
        assert!(summary_json.contains("\"seed\": 42"));
        assert!(summary_json.contains("\"outcome\": \"success\""));

        // Verify summary.md has headings.
        let summary_md = fs::read_to_string(dir.path().join("summary.md")).unwrap();
        assert!(summary_md.contains("# Recording Summary"));
        assert!(summary_md.contains("test-phase"));
    }

    #[test]
    fn test_session_no_event_capture() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = RecordingConfig {
            output_dir: dir.path().to_path_buf(),
            capture_events: false,
            ..RecordingConfig::default()
        };

        let session = RecordingSession::start(config).unwrap();
        let _ = session.finish("success").unwrap();

        // events.jsonl should NOT be written.
        assert!(!dir.path().join("events.jsonl").exists());
        // summary.json/md should still be written.
        assert!(dir.path().join("summary.json").exists());
        assert!(dir.path().join("summary.md").exists());
    }

    #[test]
    fn test_event_json_serialisation() {
        let event = RecordingEvent::Info {
            message: "hello \"world\"\nnewline".to_owned(),
        };
        let json = event_to_json(&event);
        assert!(json.contains("\\\"world\\\""));
        assert!(json.contains("\\n"));
        assert!(!json.contains('\n'));
    }

    #[test]
    fn test_stable_run_dir() {
        let base = Path::new("runs");
        let dir = stable_run_dir(base, RecordingPreset::PerfScaling, 42);
        assert_eq!(dir, PathBuf::from("runs/perf-scaling-seed42"));
    }

    #[test]
    fn test_preset_display() {
        let s = format!("{}", RecordingPreset::FullSuite);
        assert_eq!(s, "full-suite");
    }

    #[test]
    fn test_all_labels() {
        let labels = RecordingPreset::all_labels();
        assert_eq!(labels.len(), 4);
        assert!(labels.contains(&"corruption-recovery"));
        assert!(labels.contains(&"full-suite"));
    }

    #[test]
    fn test_summary_md_phases_table() {
        let events = vec![
            TimestampedEvent {
                offset_ms: 0,
                event: RecordingEvent::SessionStart {
                    seed: 42,
                    preset: Some("test".to_owned()),
                    timestamp: "12345".to_owned(),
                },
            },
            TimestampedEvent {
                offset_ms: 100,
                event: RecordingEvent::PhaseComplete {
                    name: "phase-a".to_owned(),
                    duration_ms: 100,
                    outcome: "ok".to_owned(),
                },
            },
            TimestampedEvent {
                offset_ms: 300,
                event: RecordingEvent::PhaseComplete {
                    name: "phase-b".to_owned(),
                    duration_ms: 200,
                    outcome: "ok".to_owned(),
                },
            },
        ];
        let summary = RecordingSummary {
            seed: 42,
            preset: Some("test".to_owned()),
            duration_ms: 300,
            total_events: 3,
            outcome: "success".to_owned(),
            output_dir: PathBuf::from("/tmp/test"),
        };
        let md = render_summary_md(&summary, &events);
        assert!(md.contains("## Phases"));
        assert!(md.contains("phase-a"));
        assert!(md.contains("phase-b"));
        assert!(md.contains("| Phase | Duration | Outcome |"));
    }

    #[test]
    fn test_json_escape() {
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape("a\"b"), "a\\\"b");
        assert_eq!(json_escape("a\nb"), "a\\nb");
        assert_eq!(json_escape("a\\b"), "a\\\\b");
        assert_eq!(json_escape("a\tb"), "a\\tb");
        assert_eq!(json_escape("a\rb"), "a\\rb");
    }

    #[test]
    fn test_recording_help_text() {
        let help = recording_help_text();
        assert!(help.contains("--record"));
        assert!(help.contains("--record-preset"));
        assert!(help.contains("corruption-recovery"));
        assert!(help.contains("full-suite"));
    }

    #[test]
    fn test_session_warning() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = RecordingConfig {
            output_dir: dir.path().to_path_buf(),
            ..RecordingConfig::default()
        };
        let mut session = RecordingSession::start(config).unwrap();
        session.warning("something odd happened");
        assert_eq!(session.event_count(), 2); // SessionStart + Warning
    }

    #[test]
    fn test_config_default() {
        let config = RecordingConfig::default();
        assert_eq!(config.seed, 42);
        assert!(config.no_color);
        assert!(config.capture_events);
        assert!(!config.json_output);
        assert!(!config.quiet);
        assert!(config.timeout_secs.is_none());
        assert!(config.preset.is_none());
    }
}
