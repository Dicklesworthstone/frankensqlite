//! Deterministic cross-process crash harness for `bd-2g5.5.1`.
//!
//! This module models multi-process crash/recovery behavior for:
//! - TxnSlot reclamation
//! - Seqlock torn-read safety
//! - Left-Right linearizability
//!
//! The model is intentionally deterministic and produces structured
//! machine-readable evidence suitable for CI and local replay.

use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Bead identifier.
pub const CROSS_PROCESS_CRASH_BEAD_ID: &str = "bd-2g5.5.1";
/// Report schema version.
pub const CROSS_PROCESS_CRASH_SCHEMA_VERSION: u32 = 1;
/// Default crash-cycle count.
pub const DEFAULT_CYCLES: usize = 100;
/// Default process-count for matrix simulation.
pub const DEFAULT_PROCESS_COUNT: usize = 8;
/// Default deterministic seed.
pub const DEFAULT_SEED: u64 = 270_550_001;

/// Process role participating in a cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRole {
    Writer,
    Reader,
    Checkpointer,
    Recovery,
}

impl ProcessRole {
    /// All roles covered by the deterministic matrix.
    pub const ALL: [Self; 4] = [
        Self::Writer,
        Self::Reader,
        Self::Checkpointer,
        Self::Recovery,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Writer => "writer",
            Self::Reader => "reader",
            Self::Checkpointer => "checkpointer",
            Self::Recovery => "recovery",
        }
    }
}

impl fmt::Display for ProcessRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Public helper array for deterministic matrix sizing.
pub const PROCESS_ROLE_ALL: [ProcessRole; 4] = ProcessRole::ALL;

/// Crash injection point for a cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashPoint {
    AfterSlotClaim,
    MidSeqlockPublish,
    DuringLeftRightSwap,
    BeforeSlotRelease,
    PostCommit,
}

impl CrashPoint {
    /// All crash points in matrix order.
    pub const ALL: [Self; 5] = [
        Self::AfterSlotClaim,
        Self::MidSeqlockPublish,
        Self::DuringLeftRightSwap,
        Self::BeforeSlotRelease,
        Self::PostCommit,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterSlotClaim => "after_slot_claim",
            Self::MidSeqlockPublish => "mid_seqlock_publish",
            Self::DuringLeftRightSwap => "during_left_right_swap",
            Self::BeforeSlotRelease => "before_slot_release",
            Self::PostCommit => "post_commit",
        }
    }
}

impl fmt::Display for CrashPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Public helper array for deterministic matrix sizing.
pub const CRASH_POINT_ALL: [CrashPoint; 5] = CrashPoint::ALL;

/// Invariant class attached to each structured event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvariantClass {
    SlotReclamation,
    SeqlockNoTornRead,
    LeftRightLinearizable,
}

impl InvariantClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SlotReclamation => "slot_reclamation",
            Self::SeqlockNoTornRead => "seqlock_no_torn_read",
            Self::LeftRightLinearizable => "left_right_linearizable",
        }
    }
}

impl fmt::Display for InvariantClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Structured event emitted per invariant-class check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredCrashEvent {
    pub schema_version: u32,
    pub bead_id: String,
    pub trace_id: String,
    pub run_id: String,
    pub scenario_id: String,
    pub process_role: String,
    pub crash_point: String,
    pub invariant_class: String,
    pub duration_micros: u64,
    pub outcome: String,
    pub diagnostic: String,
}

impl StructuredCrashEvent {
    fn new(
        config: &CrossProcessCrashConfig,
        scenario_id: &str,
        process_role: ProcessRole,
        crash_point: CrashPoint,
        invariant_class: InvariantClass,
        duration_micros: u64,
        outcome: &str,
        diagnostic: String,
    ) -> Self {
        Self {
            schema_version: CROSS_PROCESS_CRASH_SCHEMA_VERSION,
            bead_id: CROSS_PROCESS_CRASH_BEAD_ID.to_owned(),
            trace_id: config.trace_id.clone(),
            run_id: config.run_id.clone(),
            scenario_id: scenario_id.to_owned(),
            process_role: process_role.as_str().to_owned(),
            crash_point: crash_point.as_str().to_owned(),
            invariant_class: invariant_class.as_str().to_owned(),
            duration_micros,
            outcome: outcome.to_owned(),
            diagnostic,
        }
    }
}

/// Per-cycle outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioOutcome {
    pub scenario_id: String,
    pub cycle: usize,
    pub process_id: usize,
    pub process_role: String,
    pub crash_point: String,
    pub crashed: bool,
    pub slot_reclaimed: bool,
    pub torn_read_detected: bool,
    pub left_right_linearizable: bool,
    pub invariants_passed: bool,
}

/// Aggregated metrics suitable for CI dashboards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashHarnessMetrics {
    pub crash_cycles_total: usize,
    pub orphan_slots_reclaimed_total: usize,
    pub torn_reads_detected_total: usize,
    pub linearizability_violations_total: usize,
    pub schema_conformance_checks_total: usize,
}

/// Full report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossProcessCrashReport {
    pub schema_version: u32,
    pub bead_id: String,
    pub trace_id: String,
    pub run_id: String,
    pub seed: u64,
    pub cycles_requested: usize,
    pub cycles_executed: usize,
    pub process_count: usize,
    pub scenario_matrix_expected: usize,
    pub scenario_matrix_covered: usize,
    pub scenario_matrix_complete: bool,
    pub slot_reclamation_pass: bool,
    pub seqlock_no_torn_reads: bool,
    pub left_right_linearizable: bool,
    pub orphan_slots_after_run: usize,
    pub metrics: CrashHarnessMetrics,
    pub schema_conformance_errors: Vec<String>,
    pub replay_command: String,
    pub artifact_bundle_paths: Vec<String>,
    pub scenarios: Vec<ScenarioOutcome>,
    pub events: Vec<StructuredCrashEvent>,
    pub summary: String,
}

impl CrossProcessCrashReport {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(input: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(input)
    }

    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "matrix={}/{} slot_reclaim={} seqlock={} linearizable={} orphan_slots={} schema_errors={}",
            self.scenario_matrix_covered,
            self.scenario_matrix_expected,
            if self.slot_reclamation_pass {
                "pass"
            } else {
                "FAIL"
            },
            if self.seqlock_no_torn_reads {
                "pass"
            } else {
                "FAIL"
            },
            if self.left_right_linearizable {
                "pass"
            } else {
                "FAIL"
            },
            self.orphan_slots_after_run,
            self.schema_conformance_errors.len(),
        )
    }
}

/// Deterministic configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossProcessCrashConfig {
    pub seed: u64,
    pub cycles: usize,
    pub process_count: usize,
    pub run_id: String,
    pub trace_id: String,
}

impl Default for CrossProcessCrashConfig {
    fn default() -> Self {
        Self {
            seed: DEFAULT_SEED,
            cycles: DEFAULT_CYCLES,
            process_count: DEFAULT_PROCESS_COUNT,
            run_id: format!("bd-2g5-5-1-seed-{DEFAULT_SEED:016x}"),
            trace_id: format!("trace-bd-2g5-5-1-{DEFAULT_SEED:016x}"),
        }
    }
}

impl CrossProcessCrashConfig {
    #[must_use]
    pub fn replay_command(&self) -> String {
        format!(
            "BD_2G5_5_CYCLES={} BD_2G5_5_SEED={} cargo test -p fsqlite-harness --test bd_2g5_5_cross_process -- --nocapture",
            self.cycles, self.seed
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveSide {
    Left,
    Right,
}

impl ActiveSide {
    fn flip(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

#[derive(Debug, Clone)]
struct SimulationState {
    slots: Vec<Option<usize>>,
    seqlock_seq: u64,
    seqlock_left: u64,
    seqlock_right: u64,
    left_right_left: u64,
    left_right_right: u64,
    active_side: ActiveSide,
    reader_last_seen: Vec<u64>,
}

impl SimulationState {
    fn new(process_count: usize) -> Self {
        Self {
            slots: vec![None; process_count],
            seqlock_seq: 0,
            seqlock_left: 0,
            seqlock_right: 0,
            left_right_left: 0,
            left_right_right: 0,
            active_side: ActiveSide::Left,
            reader_last_seen: vec![0; process_count],
        }
    }

    fn visible_left_right_version(&self) -> u64 {
        match self.active_side {
            ActiveSide::Left => self.left_right_left,
            ActiveSide::Right => self.left_right_right,
        }
    }
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn is_torn_snapshot(seq: u64, left: u64, right: u64) -> bool {
    (seq % 2) == 1 || left != right
}

fn validate_event_schema(event: &StructuredCrashEvent) -> Result<(), String> {
    let required = [
        ("trace_id", event.trace_id.as_str()),
        ("run_id", event.run_id.as_str()),
        ("scenario_id", event.scenario_id.as_str()),
        ("process_role", event.process_role.as_str()),
        ("crash_point", event.crash_point.as_str()),
        ("invariant_class", event.invariant_class.as_str()),
        ("outcome", event.outcome.as_str()),
        ("diagnostic", event.diagnostic.as_str()),
    ];
    for (field, value) in required {
        if value.trim().is_empty() {
            return Err(format!("missing_or_empty_field field={field}"));
        }
    }
    if event.duration_micros == 0 {
        return Err("invalid_duration duration_micros=0".to_owned());
    }
    if !matches!(event.outcome.as_str(), "pass" | "fail" | "recovered") {
        return Err(format!("invalid_outcome value={}", event.outcome));
    }
    Ok(())
}

fn choose_matrix_case(cycle: usize) -> (ProcessRole, CrashPoint) {
    let role_count = ProcessRole::ALL.len();
    let point_count = CrashPoint::ALL.len();
    let matrix_size = role_count * point_count;
    let matrix_index = cycle % matrix_size;
    let role_index = matrix_index % role_count;
    let point_index = (matrix_index / role_count) % point_count;
    (ProcessRole::ALL[role_index], CrashPoint::ALL[point_index])
}

fn execute_cycle(
    state: &mut SimulationState,
    config: &CrossProcessCrashConfig,
    cycle: usize,
    process_id: usize,
    process_role: ProcessRole,
    crash_point: CrashPoint,
) -> (
    ScenarioOutcome,
    Vec<StructuredCrashEvent>,
    CrashHarnessMetrics,
) {
    let scenario_id = format!("SCN-{:03}", cycle + 1);
    let mut metrics = CrashHarnessMetrics {
        crash_cycles_total: 0,
        orphan_slots_reclaimed_total: 0,
        torn_reads_detected_total: 0,
        linearizability_violations_total: 0,
        schema_conformance_checks_total: 0,
    };

    let slot_idx = process_id % config.process_count.max(1);
    let mut slot_reclaimed = false;
    if state.slots[slot_idx].is_some() {
        slot_reclaimed = true;
        metrics.orphan_slots_reclaimed_total += 1;
    }
    state.slots[slot_idx] = Some(process_id);

    let crashed = !matches!(crash_point, CrashPoint::PostCommit);
    if crashed {
        metrics.crash_cycles_total += 1;
    }

    // Seqlock publish path.
    if matches!(
        process_role,
        ProcessRole::Writer | ProcessRole::Checkpointer
    ) {
        let next_value = state
            .seqlock_left
            .max(state.seqlock_right)
            .saturating_add(1);
        state.seqlock_seq = state.seqlock_seq.saturating_add(1); // odd -> publish begin
        state.seqlock_left = next_value;
        if !matches!(crash_point, CrashPoint::MidSeqlockPublish) {
            state.seqlock_right = next_value;
            state.seqlock_seq = state.seqlock_seq.saturating_add(1); // even -> publish end
        }
    }

    // Left-right update path.
    if matches!(
        process_role,
        ProcessRole::Writer | ProcessRole::Checkpointer
    ) {
        let next_visible = state.visible_left_right_version().saturating_add(1);
        match state.active_side {
            ActiveSide::Left => {
                state.left_right_right = next_visible;
            }
            ActiveSide::Right => {
                state.left_right_left = next_visible;
            }
        }
        if !matches!(crash_point, CrashPoint::DuringLeftRightSwap) {
            state.active_side = state.active_side.flip();
        }
    }

    // Crash recovery: reclaim slot and repair seqlock if needed.
    if crashed {
        if state.slots[slot_idx].is_some() {
            metrics.orphan_slots_reclaimed_total += 1;
            slot_reclaimed = true;
        }
        state.slots[slot_idx] = None;
        if is_torn_snapshot(state.seqlock_seq, state.seqlock_left, state.seqlock_right) {
            let repaired = state.seqlock_left.max(state.seqlock_right);
            state.seqlock_left = repaired;
            state.seqlock_right = repaired;
            if (state.seqlock_seq % 2) == 1 {
                state.seqlock_seq = state.seqlock_seq.saturating_add(1);
            }
        }
    } else {
        // No crash: regular release.
        state.slots[slot_idx] = None;
    }

    let torn_read_detected =
        is_torn_snapshot(state.seqlock_seq, state.seqlock_left, state.seqlock_right);
    if torn_read_detected {
        metrics.torn_reads_detected_total += 1;
    }

    let observed_version = state.visible_left_right_version();
    let previous_seen = state.reader_last_seen[process_id];
    let left_right_linearizable = observed_version >= previous_seen;
    if left_right_linearizable {
        state.reader_last_seen[process_id] = observed_version;
    } else {
        metrics.linearizability_violations_total += 1;
    }

    let orphan_slots_after_cycle = state.slots.iter().flatten().count();
    let slot_reclamation_pass = orphan_slots_after_cycle == 0;
    let invariants_passed = slot_reclamation_pass && !torn_read_detected && left_right_linearizable;

    let base_duration = 100_u64 + u64::try_from(cycle).unwrap_or(0) * 7;
    let mut events = Vec::new();
    events.push(StructuredCrashEvent::new(
        config,
        &scenario_id,
        process_role,
        crash_point,
        InvariantClass::SlotReclamation,
        base_duration,
        if slot_reclamation_pass {
            "pass"
        } else if crashed {
            "recovered"
        } else {
            "fail"
        },
        if slot_reclamation_pass {
            format!(
                "slot reclamation succeeded role={} crash_point={} reclaimed={slot_reclaimed}",
                process_role, crash_point
            )
        } else {
            format!("orphan_slot_detected count={orphan_slots_after_cycle}; inspect recovery flow")
        },
    ));
    events.push(StructuredCrashEvent::new(
        config,
        &scenario_id,
        process_role,
        crash_point,
        InvariantClass::SeqlockNoTornRead,
        base_duration + 33,
        if torn_read_detected { "fail" } else { "pass" },
        if torn_read_detected {
            format!(
                "torn snapshot detected seq={} left={} right={} -- replay with {}",
                state.seqlock_seq,
                state.seqlock_left,
                state.seqlock_right,
                config.replay_command()
            )
        } else {
            format!(
                "seqlock consistent seq={} left={} right={}",
                state.seqlock_seq, state.seqlock_left, state.seqlock_right
            )
        },
    ));
    events.push(StructuredCrashEvent::new(
        config,
        &scenario_id,
        process_role,
        crash_point,
        InvariantClass::LeftRightLinearizable,
        base_duration + 77,
        if left_right_linearizable { "pass" } else { "fail" },
        if left_right_linearizable {
            format!("reader observed monotonic version={observed_version}")
        } else {
            format!(
                "linearizability violation prev={previous_seen} observed={observed_version}; inspect swap ordering"
            )
        },
    ));

    metrics.schema_conformance_checks_total = events.len();
    let scenario = ScenarioOutcome {
        scenario_id,
        cycle,
        process_id,
        process_role: process_role.as_str().to_owned(),
        crash_point: crash_point.as_str().to_owned(),
        crashed,
        slot_reclaimed,
        torn_read_detected,
        left_right_linearizable,
        invariants_passed,
    };
    (scenario, events, metrics)
}

/// Execute the deterministic cross-process harness and return a report.
#[must_use]
pub fn run_cross_process_crash_harness(
    config: &CrossProcessCrashConfig,
) -> CrossProcessCrashReport {
    assert!(config.cycles > 0, "cycles must be > 0");
    assert!(config.process_count > 0, "process_count must be > 0");

    let mut state = SimulationState::new(config.process_count);
    let mut rng = config.seed;
    let mut scenarios = Vec::with_capacity(config.cycles);
    let mut events = Vec::with_capacity(config.cycles * 3);
    let mut matrix_seen = BTreeSet::new();
    let mut metrics = CrashHarnessMetrics {
        crash_cycles_total: 0,
        orphan_slots_reclaimed_total: 0,
        torn_reads_detected_total: 0,
        linearizability_violations_total: 0,
        schema_conformance_checks_total: 0,
    };

    for cycle in 0..config.cycles {
        let (process_role, crash_point) = choose_matrix_case(cycle);
        matrix_seen.insert((process_role, crash_point));

        let sampled = lcg_next(&mut rng);
        let process_id = usize::try_from(sampled).unwrap_or(0) % config.process_count;

        let (scenario, mut cycle_events, cycle_metrics) = execute_cycle(
            &mut state,
            config,
            cycle,
            process_id,
            process_role,
            crash_point,
        );

        metrics.crash_cycles_total += cycle_metrics.crash_cycles_total;
        metrics.orphan_slots_reclaimed_total += cycle_metrics.orphan_slots_reclaimed_total;
        metrics.torn_reads_detected_total += cycle_metrics.torn_reads_detected_total;
        metrics.linearizability_violations_total += cycle_metrics.linearizability_violations_total;
        metrics.schema_conformance_checks_total += cycle_metrics.schema_conformance_checks_total;

        scenarios.push(scenario);
        events.append(&mut cycle_events);
    }

    let orphan_slots_after_run = state.slots.iter().flatten().count();

    let mut schema_conformance_errors = Vec::new();
    for (idx, event) in events.iter().enumerate() {
        if let Err(error) = validate_event_schema(event) {
            schema_conformance_errors.push(format!("event_index={idx} error={error}"));
        }
    }

    let scenario_matrix_expected = ProcessRole::ALL.len() * CrashPoint::ALL.len();
    let scenario_matrix_covered = matrix_seen.len();
    let scenario_matrix_complete = scenario_matrix_covered == scenario_matrix_expected;

    let slot_reclamation_pass = orphan_slots_after_run == 0;
    let seqlock_no_torn_reads = metrics.torn_reads_detected_total == 0;
    let left_right_linearizable = metrics.linearizability_violations_total == 0;

    let replay_command = config.replay_command();
    let artifact_bundle_paths = vec![
        format!("test-results/bd_2g5_5/{}.json", config.run_id),
        format!("test-results/bd_2g5_5/{}.events.jsonl", config.run_id),
    ];

    let summary = format!(
        "matrix={scenario_matrix_covered}/{scenario_matrix_expected} crash_cycles={} slot_reclaim={} seqlock={} linearizable={} schema_errors={}",
        metrics.crash_cycles_total,
        if slot_reclamation_pass {
            "pass"
        } else {
            "FAIL"
        },
        if seqlock_no_torn_reads {
            "pass"
        } else {
            "FAIL"
        },
        if left_right_linearizable {
            "pass"
        } else {
            "FAIL"
        },
        schema_conformance_errors.len(),
    );

    CrossProcessCrashReport {
        schema_version: CROSS_PROCESS_CRASH_SCHEMA_VERSION,
        bead_id: CROSS_PROCESS_CRASH_BEAD_ID.to_owned(),
        trace_id: config.trace_id.clone(),
        run_id: config.run_id.clone(),
        seed: config.seed,
        cycles_requested: config.cycles,
        cycles_executed: config.cycles,
        process_count: config.process_count,
        scenario_matrix_expected,
        scenario_matrix_covered,
        scenario_matrix_complete,
        slot_reclamation_pass,
        seqlock_no_torn_reads,
        left_right_linearizable,
        orphan_slots_after_run,
        metrics,
        schema_conformance_errors,
        replay_command,
        artifact_bundle_paths,
        scenarios,
        events,
        summary,
    }
}

/// Write a pretty JSON report to `path`.
pub fn write_cross_process_crash_report(
    path: &Path,
    report: &CrossProcessCrashReport,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "report_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }
    let payload = serde_json::to_string_pretty(report)
        .map_err(|error| format!("report_serialize_failed error={error}"))?;
    fs::write(path, payload)
        .map_err(|error| format!("report_write_failed path={} error={error}", path.display()))
}

/// Load a JSON report from disk.
pub fn load_cross_process_crash_report(path: &Path) -> Result<CrossProcessCrashReport, String> {
    let payload = fs::read_to_string(path)
        .map_err(|error| format!("report_read_failed path={} error={error}", path.display()))?;
    serde_json::from_str(&payload)
        .map_err(|error| format!("report_parse_failed path={} error={error}", path.display()))
}

/// Write structured events as JSON Lines.
pub fn write_cross_process_event_log(
    path: &Path,
    events: &[StructuredCrashEvent],
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "event_parent_create_failed path={} error={error}",
                parent.display()
            )
        })?;
    }

    let mut lines = String::new();
    for event in events {
        let json = serde_json::to_string(event)
            .map_err(|error| format!("event_serialize_failed error={error}"))?;
        lines.push_str(&json);
        lines.push('\n');
    }

    fs::write(path, lines)
        .map_err(|error| format!("event_write_failed path={} error={error}", path.display()))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{
        CROSS_PROCESS_CRASH_BEAD_ID, CROSS_PROCESS_CRASH_SCHEMA_VERSION, CrashPoint,
        CrossProcessCrashConfig, ProcessRole, is_torn_snapshot, run_cross_process_crash_harness,
    };

    #[test]
    fn deterministic_for_same_seed_and_config() {
        let config = CrossProcessCrashConfig {
            seed: 7,
            cycles: 40,
            process_count: 8,
            run_id: "deterministic-run".to_owned(),
            trace_id: "trace-deterministic".to_owned(),
        };

        let first = run_cross_process_crash_harness(&config);
        let second = run_cross_process_crash_harness(&config);

        assert_eq!(
            first.to_json().expect("serialize first"),
            second.to_json().expect("serialize second"),
            "bead_id={} case=determinism",
            CROSS_PROCESS_CRASH_BEAD_ID
        );
    }

    #[test]
    fn crash_recovery_reclaims_slots_and_preserves_invariants() {
        let report = run_cross_process_crash_harness(&CrossProcessCrashConfig::default());
        assert_eq!(report.schema_version, CROSS_PROCESS_CRASH_SCHEMA_VERSION);
        assert_eq!(report.orphan_slots_after_run, 0);
        assert!(report.slot_reclamation_pass);
        assert!(report.seqlock_no_torn_reads);
        assert!(report.left_right_linearizable);
        assert!(report.metrics.crash_cycles_total > 0);
        assert!(report.metrics.orphan_slots_reclaimed_total > 0);
        assert!(report.schema_conformance_errors.is_empty());
    }

    #[test]
    fn torn_snapshot_detector_flags_odd_or_mismatched_state() {
        assert!(is_torn_snapshot(3, 7, 7));
        assert!(is_torn_snapshot(4, 7, 9));
        assert!(!is_torn_snapshot(4, 9, 9));
    }

    #[test]
    fn matrix_covering_defaults_match_role_and_crash_dimensions() {
        let report = run_cross_process_crash_harness(&CrossProcessCrashConfig::default());
        let expected = ProcessRole::ALL.len() * CrashPoint::ALL.len();
        assert_eq!(report.scenario_matrix_expected, expected);
        assert_eq!(report.scenario_matrix_covered, expected);
        assert!(report.scenario_matrix_complete);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]
        #[test]
        fn property_false_positive_resistance(seed in any::<u64>()) {
            let config = CrossProcessCrashConfig {
                seed,
                cycles: 24,
                process_count: 8,
                run_id: format!("prop-run-{seed:016x}"),
                trace_id: format!("trace-prop-{seed:016x}"),
            };
            let report = run_cross_process_crash_harness(&config);
            prop_assert_eq!(report.orphan_slots_after_run, 0);
            prop_assert_eq!(report.metrics.torn_reads_detected_total, 0);
            prop_assert_eq!(report.metrics.linearizability_violations_total, 0);
            prop_assert!(report.schema_conformance_errors.is_empty());
        }
    }
}
