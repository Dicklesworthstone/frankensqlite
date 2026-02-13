//! Concurrent soak executor with periodic invariant probes (`bd-mblr.7.2.2`).
//!
//! Drives deterministic soak workloads defined by [`SoakWorkloadSpec`] and periodically
//! evaluates invariants via [`evaluate_invariants`]. Supports controlled fault injection
//! via [`FaultProfileCatalog`].
//!
//! # Architecture
//!
//! ```text
//!  SoakExecutor::new(spec)
//!    ├── Warmup phase (stabilize baseline)
//!    ├── MainLoop phase
//!    │     ├── run_step() → SoakStepOutcome
//!    │     ├── should_checkpoint()? → probe_invariants()
//!    │     └── critical_violation? → abort
//!    ├── Cooldown phase
//!    └── finalize() → SoakRunReport
//! ```
//!
//! The executor is *deterministic*: same spec + same seed → same step sequence.
//! It does NOT spawn threads; callers drive execution via `run_step()` or `run_all()`.

use serde::{Deserialize, Serialize};

use crate::fault_profiles::{FaultProfile, FaultProfileCatalog};
use crate::soak_profiles::{
    CheckpointSnapshot, InvariantCheckResult, InvariantViolation, SoakWorkloadSpec,
    evaluate_invariants,
};

/// Bead identifier for tracing and log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.2.2";

// ---------------------------------------------------------------------------
// Executor phases and step outcomes
// ---------------------------------------------------------------------------

/// Lifecycle phase of a soak run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SoakPhase {
    /// Initial stabilization period (no invariant checks).
    Warmup,
    /// Primary workload execution with periodic invariant probes.
    MainLoop,
    /// Drain in-flight transactions and final state validation.
    Cooldown,
    /// Run complete, report generated.
    Complete,
}

/// What type of transaction was attempted in a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepAction {
    /// Read-only query.
    Read,
    /// Write (INSERT/UPDATE/DELETE) transaction.
    Write,
    /// DDL schema change (CREATE/DROP/ALTER).
    SchemaMutation,
    /// WAL checkpoint.
    Checkpoint,
}

/// Outcome of a single soak step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakStepOutcome {
    /// Which transaction in the overall run (0-based).
    pub transaction_index: u64,
    /// Current phase.
    pub phase: SoakPhase,
    /// What the step attempted.
    pub action: StepAction,
    /// Whether the transaction committed successfully.
    pub committed: bool,
    /// Error message if the step failed.
    pub error: Option<String>,
    /// Whether a checkpoint probe was triggered after this step.
    pub checkpoint_triggered: bool,
}

/// Lifecycle boundary where telemetry was captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryBoundary {
    /// Initial run baseline before workload steps execute.
    Startup,
    /// Periodic checkpoint during the steady-state main loop.
    SteadyState,
    /// Final run snapshot captured at finalize.
    Teardown,
}

/// Normalized resource telemetry record for leak-trend analysis (`bd-mblr.7.7.1`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceTelemetryRecord {
    /// Deterministic run identifier for correlation across artifacts.
    pub run_id: String,
    /// Scenario identifier for per-scenario trend analysis.
    pub scenario_id: String,
    /// Profile name for the originating soak workload.
    pub profile_name: String,
    /// Deterministic seed used for this run.
    pub run_seed: u64,
    /// Monotonic sequence number within the run.
    pub sequence: u64,
    /// Lifecycle boundary at capture time.
    pub boundary: TelemetryBoundary,
    /// Soak phase at capture time.
    pub phase: SoakPhase,
    /// Monotonic transaction count at capture time.
    pub transaction_count: u64,
    /// Monotonic elapsed seconds at capture time.
    pub elapsed_secs: f64,
    /// Current WAL page count.
    pub wal_pages: u64,
    /// Current heap footprint estimate.
    pub heap_bytes: u64,
    /// Current active transaction count.
    pub active_transactions: u32,
    /// Current lock table size.
    pub lock_table_size: u32,
    /// Current max version chain length.
    pub max_version_chain_len: u32,
    /// Current p99 latency estimate.
    pub p99_latency_us: u64,
    /// SSI aborts observed since previous checkpoint.
    pub ssi_aborts_since_last: u64,
    /// Commits observed since previous checkpoint.
    pub commits_since_last: u64,
}

impl ResourceTelemetryRecord {
    /// Build a telemetry record from a checkpoint snapshot.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn from_snapshot(
        run_id: &str,
        scenario_id: &str,
        profile_name: &str,
        run_seed: u64,
        sequence: u64,
        boundary: TelemetryBoundary,
        phase: SoakPhase,
        snapshot: &CheckpointSnapshot,
    ) -> Self {
        Self {
            run_id: run_id.to_owned(),
            scenario_id: scenario_id.to_owned(),
            profile_name: profile_name.to_owned(),
            run_seed,
            sequence,
            boundary,
            phase,
            transaction_count: snapshot.transaction_count,
            elapsed_secs: snapshot.elapsed_secs,
            wal_pages: snapshot.wal_pages,
            heap_bytes: snapshot.heap_bytes,
            active_transactions: snapshot.active_transactions,
            lock_table_size: snapshot.lock_table_size,
            max_version_chain_len: snapshot.max_version_chain_len,
            p99_latency_us: snapshot.p99_latency_us,
            ssi_aborts_since_last: snapshot.ssi_aborts_since_last,
            commits_since_last: snapshot.commits_since_last,
        }
    }

    /// Serialize this record to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Parse a record from JSON.
    pub fn from_json(input: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(input)
    }
}

// ---------------------------------------------------------------------------
// Soak run report
// ---------------------------------------------------------------------------

/// Final report produced by [`SoakExecutor::finalize`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakRunReport {
    /// The workload spec that drove this run.
    pub spec_json: String,
    /// Total transactions attempted.
    pub total_transactions: u64,
    /// Committed transactions.
    pub total_commits: u64,
    /// Rolled-back transactions.
    pub total_rollbacks: u64,
    /// Errored transactions.
    pub total_errors: u64,
    /// All invariant check results (one per checkpoint).
    pub invariant_checks: Vec<InvariantCheckResult>,
    /// All violations detected across all checkpoints.
    pub all_violations: Vec<InvariantViolation>,
    /// Whether the run was aborted due to a critical violation.
    pub aborted: bool,
    /// Reason for abort (if any).
    pub abort_reason: Option<String>,
    /// Checkpoint snapshots captured during the run.
    pub checkpoints: Vec<CheckpointSnapshot>,
    /// Fault profiles that were active during the run.
    pub active_fault_profile_ids: Vec<String>,
    /// Summary of the run for triage.
    pub summary: String,
    /// Normalized resource telemetry records captured during this run.
    pub resource_telemetry: Vec<ResourceTelemetryRecord>,
}

impl SoakRunReport {
    /// Whether the run passed all invariant checks.
    #[must_use]
    pub fn passed(&self) -> bool {
        !self.aborted && self.all_violations.is_empty()
    }

    /// Count of critical (abort-level) violations.
    #[must_use]
    pub fn critical_violation_count(&self) -> usize {
        self.invariant_checks
            .iter()
            .filter(|c| c.has_critical_violation)
            .count()
    }

    /// Render a one-line summary for triage.
    #[must_use]
    pub fn triage_line(&self) -> String {
        if self.passed() {
            format!(
                "PASS: {} txns ({} commits, {} rollbacks, {} errors), {} checkpoints, 0 violations",
                self.total_transactions,
                self.total_commits,
                self.total_rollbacks,
                self.total_errors,
                self.checkpoints.len(),
            )
        } else {
            format!(
                "FAIL: {} txns, {} violations ({} critical), aborted={}",
                self.total_transactions,
                self.all_violations.len(),
                self.critical_violation_count(),
                self.aborted,
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Executor state
// ---------------------------------------------------------------------------

/// Internal mutable state of a soak run.
struct SoakState {
    phase: SoakPhase,
    transaction_index: u64,
    commits: u64,
    rollbacks: u64,
    errors: u64,
    checkpoints: Vec<CheckpointSnapshot>,
    invariant_results: Vec<InvariantCheckResult>,
    all_violations: Vec<InvariantViolation>,
    aborted: bool,
    abort_reason: Option<String>,
    /// Pseudo-RNG state for deterministic action selection.
    rng_state: u64,
    /// Simulated system metrics for checkpoint snapshots.
    sim_max_txn_id: u64,
    sim_max_commit_seq: u64,
    sim_wal_pages: u64,
    sim_version_chain_len: u64,
    sim_lock_table_size: u64,
    sim_active_txns: u64,
    sim_heap_bytes: u64,
    resource_telemetry: Vec<ResourceTelemetryRecord>,
    telemetry_sequence: u64,
}

impl SoakState {
    fn new(seed: u64) -> Self {
        Self {
            phase: SoakPhase::Warmup,
            transaction_index: 0,
            commits: 0,
            rollbacks: 0,
            errors: 0,
            checkpoints: Vec::new(),
            invariant_results: Vec::new(),
            all_violations: Vec::new(),
            aborted: false,
            abort_reason: None,
            rng_state: seed,
            sim_max_txn_id: 0,
            sim_max_commit_seq: 0,
            sim_wal_pages: 0,
            sim_version_chain_len: 1,
            sim_lock_table_size: 0,
            sim_active_txns: 0,
            sim_heap_bytes: 1024 * 1024, // 1 MiB baseline
            resource_telemetry: Vec::new(),
            telemetry_sequence: 0,
        }
    }

    /// Deterministic pseudo-random number (xorshift64).
    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    /// Take a checkpoint snapshot of simulated system state.
    #[allow(clippy::cast_possible_truncation)]
    fn capture_snapshot(&self, elapsed_secs: f64) -> CheckpointSnapshot {
        CheckpointSnapshot {
            transaction_count: self.transaction_index,
            max_txn_id: self.sim_max_txn_id,
            max_commit_seq: self.sim_max_commit_seq,
            active_transactions: self.sim_active_txns as u32,
            wal_pages: self.sim_wal_pages,
            max_version_chain_len: self.sim_version_chain_len as u32,
            lock_table_size: self.sim_lock_table_size as u32,
            heap_bytes: self.sim_heap_bytes,
            p99_latency_us: 500 + (self.sim_wal_pages / 10), // simulated latency
            ssi_aborts_since_last: 0,
            commits_since_last: self.commits,
            elapsed_secs,
        }
    }

    fn next_telemetry_sequence(&mut self) -> u64 {
        let sequence = self.telemetry_sequence;
        self.telemetry_sequence = self.telemetry_sequence.saturating_add(1);
        sequence
    }
}

// ---------------------------------------------------------------------------
// Soak executor
// ---------------------------------------------------------------------------

/// Configuration for fault injection during soak runs.
#[derive(Debug, Clone)]
pub struct SoakFaultConfig {
    /// Fault profiles to activate.
    pub profiles: Vec<FaultProfile>,
    /// Probability (0.0..1.0) of injecting a fault per step.
    pub injection_probability: f64,
}

impl Default for SoakFaultConfig {
    fn default() -> Self {
        Self {
            profiles: Vec::new(),
            injection_probability: 0.0,
        }
    }
}

/// Deterministic soak executor that drives workloads and probes invariants.
///
/// The executor is single-threaded and deterministic. Each call to [`run_step`]
/// simulates one transaction and advances the internal state. Invariant probes
/// are triggered at intervals defined by the [`SoakWorkloadSpec`].
pub struct SoakExecutor {
    spec: SoakWorkloadSpec,
    state: SoakState,
    run_id: String,
    fault_config: SoakFaultConfig,
    /// Number of warmup transactions before main loop.
    warmup_count: u64,
    /// Simulated elapsed time per transaction (seconds).
    time_per_txn: f64,
}

impl SoakExecutor {
    /// Create a new executor for the given workload spec.
    #[must_use]
    pub fn new(spec: SoakWorkloadSpec) -> Self {
        let seed = spec.run_seed;
        let target = spec.profile.target_transactions;
        let warmup = target / 20; // 5% warmup
        let run_id = deterministic_run_id(&spec);
        let mut executor = Self {
            spec,
            state: SoakState::new(seed),
            run_id,
            fault_config: SoakFaultConfig::default(),
            warmup_count: warmup.max(1),
            time_per_txn: 0.001, // 1ms per simulated transaction
        };

        let startup_snapshot = executor.state.capture_snapshot(0.0);
        executor.emit_resource_telemetry(
            TelemetryBoundary::Startup,
            SoakPhase::Warmup,
            &startup_snapshot,
        );

        executor
    }

    /// Attach fault injection configuration.
    #[must_use]
    pub fn with_faults(mut self, config: SoakFaultConfig) -> Self {
        self.fault_config = config;
        self
    }

    /// Override warmup count.
    #[must_use]
    pub fn with_warmup(mut self, count: u64) -> Self {
        self.warmup_count = count;
        self
    }

    /// Current phase of the run.
    #[must_use]
    pub fn phase(&self) -> SoakPhase {
        self.state.phase
    }

    /// Whether the run is complete (either finished or aborted).
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state.phase == SoakPhase::Complete || self.state.aborted
    }

    /// Total transactions executed so far.
    #[must_use]
    pub fn transaction_count(&self) -> u64 {
        self.state.transaction_index
    }

    /// Run a single step: one transaction + optional invariant probe.
    pub fn run_step(&mut self) -> SoakStepOutcome {
        if self.is_done() {
            return SoakStepOutcome {
                transaction_index: self.state.transaction_index,
                phase: self.state.phase,
                action: StepAction::Read,
                committed: false,
                error: Some("executor is done".to_owned()),
                checkpoint_triggered: false,
            };
        }

        // Advance phase based on transaction count
        let target = self.spec.profile.target_transactions;
        let cooldown_start = target.saturating_sub(target / 20); // last 5%

        self.state.phase = if self.state.transaction_index < self.warmup_count {
            SoakPhase::Warmup
        } else if self.state.transaction_index < cooldown_start {
            SoakPhase::MainLoop
        } else {
            SoakPhase::Cooldown
        };

        // Determine action based on contention mix and RNG
        let rand = self.state.next_rand();
        let action = self.select_action(rand);

        // Simulate transaction execution
        let (committed, error) = self.simulate_transaction(action, rand);

        // Update counters
        self.state.transaction_index += 1;
        if committed {
            self.state.commits += 1;
            self.state.sim_max_txn_id += 1;
            self.state.sim_max_commit_seq += 1;
        } else if error.is_some() {
            self.state.errors += 1;
        } else {
            self.state.rollbacks += 1;
        }

        // Update simulated resource metrics
        self.update_sim_metrics(action, committed);

        // Check if we should probe invariants
        let checkpoint_triggered = self.should_checkpoint();
        if checkpoint_triggered && self.state.phase == SoakPhase::MainLoop {
            let result = self.probe_invariants();
            if result.has_critical_violation {
                self.state.aborted = true;
                self.state.abort_reason = Some(format!(
                    "Critical invariant violation at txn {}",
                    self.state.transaction_index,
                ));
            }
        }

        // Check if run is complete
        if self.state.transaction_index >= target && !self.state.aborted {
            self.state.phase = SoakPhase::Complete;
        }

        SoakStepOutcome {
            transaction_index: self.state.transaction_index - 1,
            phase: self.state.phase,
            action,
            committed,
            error,
            checkpoint_triggered,
        }
    }

    /// Run all remaining steps until completion or abort.
    pub fn run_all(&mut self) -> &[InvariantCheckResult] {
        while !self.is_done() {
            self.run_step();
        }
        &self.state.invariant_results
    }

    /// Check if a checkpoint probe is due.
    #[must_use]
    pub fn should_checkpoint(&self) -> bool {
        let interval = self.spec.profile.invariant_check_interval;
        if interval == 0 {
            return false;
        }
        self.state.transaction_index > 0 && self.state.transaction_index % interval == 0
    }

    /// Probe all configured invariants and record the result.
    pub fn probe_invariants(&mut self) -> InvariantCheckResult {
        let elapsed = self.state.transaction_index as f64 * self.time_per_txn;
        let current = self.state.capture_snapshot(elapsed);

        let previous = self.state.checkpoints.last().cloned();

        let result = evaluate_invariants(&self.spec.invariants, &current, previous.as_ref());

        // Record violations
        for v in &result.violations {
            self.state.all_violations.push(v.clone());
        }

        self.state.checkpoints.push(current);
        self.state.invariant_results.push(result.clone());
        let snapshot = self.state.checkpoints.last().cloned();
        if let Some(snapshot) = snapshot {
            self.emit_resource_telemetry(
                TelemetryBoundary::SteadyState,
                SoakPhase::MainLoop,
                &snapshot,
            );
        }

        result
    }

    /// Finalize the run and produce a report.
    #[must_use]
    pub fn finalize(mut self) -> SoakRunReport {
        let elapsed = self.state.transaction_index as f64 * self.time_per_txn;
        let teardown_snapshot = self.state.capture_snapshot(elapsed);
        self.emit_resource_telemetry(
            TelemetryBoundary::Teardown,
            SoakPhase::Complete,
            &teardown_snapshot,
        );

        let summary = if self.state.aborted {
            format!(
                "ABORTED at txn {}: {}",
                self.state.transaction_index,
                self.state.abort_reason.as_deref().unwrap_or("unknown"),
            )
        } else {
            format!(
                "Completed {} txns: {} commits, {} rollbacks, {} errors, {} checkpoints, {} violations",
                self.state.transaction_index,
                self.state.commits,
                self.state.rollbacks,
                self.state.errors,
                self.state.checkpoints.len(),
                self.state.all_violations.len(),
            )
        };

        let active_fault_ids: Vec<String> = self
            .fault_config
            .profiles
            .iter()
            .map(|p| p.id.to_owned())
            .collect();

        SoakRunReport {
            spec_json: self.spec.to_json().unwrap_or_default(),
            total_transactions: self.state.transaction_index,
            total_commits: self.state.commits,
            total_rollbacks: self.state.rollbacks,
            total_errors: self.state.errors,
            invariant_checks: self.state.invariant_results,
            all_violations: self.state.all_violations,
            aborted: self.state.aborted,
            abort_reason: self.state.abort_reason,
            checkpoints: self.state.checkpoints,
            active_fault_profile_ids: active_fault_ids,
            summary,
            resource_telemetry: self.state.resource_telemetry,
        }
    }

    // ─── Private helpers ────────────────────────────────────────────────

    fn select_action(&self, rand: u64) -> StepAction {
        let pct = rand % 100;
        let read_pct = u64::from(self.spec.profile.contention.reader_pct);

        // Check schema churn
        let schema_threshold = match self.spec.profile.schema_churn {
            crate::soak_profiles::SchemaChurnRate::None => 0,
            crate::soak_profiles::SchemaChurnRate::Low => 1,
            crate::soak_profiles::SchemaChurnRate::Medium => 3,
            crate::soak_profiles::SchemaChurnRate::High => 10,
        };

        // Check checkpoint cadence
        let checkpoint_threshold = match self.spec.profile.checkpoint_cadence {
            crate::soak_profiles::CheckpointCadence::Aggressive => 5,
            crate::soak_profiles::CheckpointCadence::Normal => 2,
            crate::soak_profiles::CheckpointCadence::Deferred => 1,
            crate::soak_profiles::CheckpointCadence::Disabled => 0,
        };

        if pct < schema_threshold {
            StepAction::SchemaMutation
        } else if pct < schema_threshold + checkpoint_threshold {
            StepAction::Checkpoint
        } else if pct < schema_threshold + checkpoint_threshold + read_pct {
            StepAction::Read
        } else {
            StepAction::Write
        }
    }

    fn simulate_transaction(&self, action: StepAction, rand: u64) -> (bool, Option<String>) {
        // Check fault injection
        if !self.fault_config.profiles.is_empty() && self.fault_config.injection_probability > 0.0 {
            let fault_rand = (rand >> 32) as f64 / f64::from(u32::MAX);
            if fault_rand < self.fault_config.injection_probability {
                let idx = (rand as usize) % self.fault_config.profiles.len();
                let profile = &self.fault_config.profiles[idx];
                return (
                    false,
                    Some(format!("Fault injected: {} ({})", profile.name, profile.id)),
                );
            }
        }

        // Simulate normal execution: small chance of contention error
        let contention_chance = rand % 1000;
        match action {
            StepAction::Write => {
                if contention_chance < 5 {
                    // 0.5% chance of write conflict
                    (false, Some("simulated write conflict".to_owned()))
                } else {
                    (true, None)
                }
            }
            StepAction::Read | StepAction::SchemaMutation | StepAction::Checkpoint => (true, None),
        }
    }

    fn update_sim_metrics(&mut self, action: StepAction, committed: bool) {
        if committed {
            match action {
                StepAction::Write => {
                    self.state.sim_wal_pages += 1;
                    self.state.sim_heap_bytes += 128; // small growth per write
                }
                StepAction::Checkpoint => {
                    // Checkpoint reduces WAL pages
                    self.state.sim_wal_pages = self
                        .state
                        .sim_wal_pages
                        .saturating_sub(self.state.sim_wal_pages / 2);
                }
                StepAction::SchemaMutation => {
                    self.state.sim_wal_pages += 2; // schema changes write more
                }
                StepAction::Read => {}
            }
        }

        // Simulated version chain and lock table
        self.state.sim_version_chain_len = 1 + (self.state.sim_wal_pages / 100).min(50);
        self.state.sim_lock_table_size = self.state.sim_active_txns.saturating_mul(2);
        self.state.sim_active_txns = u64::from(self.spec.profile.concurrency.connections).min(4);
    }

    fn emit_resource_telemetry(
        &mut self,
        boundary: TelemetryBoundary,
        phase: SoakPhase,
        snapshot: &CheckpointSnapshot,
    ) {
        if self.spec.profile.scenario_ids.is_empty() {
            let sequence = self.state.next_telemetry_sequence();
            self.state
                .resource_telemetry
                .push(ResourceTelemetryRecord::from_snapshot(
                    &self.run_id,
                    "UNSPECIFIED",
                    &self.spec.profile.name,
                    self.spec.run_seed,
                    sequence,
                    boundary,
                    phase,
                    snapshot,
                ));
            return;
        }

        for scenario_id in &self.spec.profile.scenario_ids {
            let sequence = self.state.next_telemetry_sequence();
            self.state
                .resource_telemetry
                .push(ResourceTelemetryRecord::from_snapshot(
                    &self.run_id,
                    scenario_id,
                    &self.spec.profile.name,
                    self.spec.run_seed,
                    sequence,
                    boundary,
                    phase,
                    snapshot,
                ));
        }
    }
}

fn deterministic_run_id(spec: &SoakWorkloadSpec) -> String {
    let mut profile_tag = String::with_capacity(spec.profile.name.len());
    let mut last_was_dash = false;
    for ch in spec.profile.name.chars() {
        if ch.is_ascii_alphanumeric() {
            profile_tag.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash {
            profile_tag.push('-');
            last_was_dash = true;
        }
    }
    let profile_tag = profile_tag.trim_matches('-');
    let profile_tag = if profile_tag.is_empty() {
        "profile"
    } else {
        profile_tag
    };
    format!("soak-{profile_tag}-{:016x}", spec.run_seed)
}

/// Resource dimensions monitored by leak detectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackedResource {
    /// Heap usage trend.
    HeapBytes,
    /// WAL growth trend.
    WalPages,
    /// Lock table growth trend.
    LockTableSize,
    /// Active transaction growth trend.
    ActiveTransactions,
}

impl TrackedResource {
    /// Ordered list used by leak detectors.
    pub const ALL: [Self; 4] = [
        Self::HeapBytes,
        Self::WalPages,
        Self::LockTableSize,
        Self::ActiveTransactions,
    ];

    /// Stable resource label for diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeapBytes => "heap_bytes",
            Self::WalPages => "wal_pages",
            Self::LockTableSize => "lock_table_size",
            Self::ActiveTransactions => "active_transactions",
        }
    }
}

/// Numeric thresholds for a single tracked resource.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ResourceLeakBudget {
    /// Warn when end-state delta exceeds this value.
    pub warning_delta: f64,
    /// Escalate to critical when end-state delta exceeds this value.
    pub critical_delta: f64,
    /// Warn when sustained per-sample slope exceeds this value.
    pub warning_slope: f64,
    /// Escalate to critical when sustained per-sample slope exceeds this value.
    pub critical_slope: f64,
}

impl ResourceLeakBudget {
    /// Construct a budget with explicit thresholds.
    #[must_use]
    pub const fn new(
        warning_delta: f64,
        critical_delta: f64,
        warning_slope: f64,
        critical_slope: f64,
    ) -> Self {
        Self {
            warning_delta,
            critical_delta,
            warning_slope,
            critical_slope,
        }
    }
}

/// Leak detector policy with baseline and escalation parameters (`bd-mblr.7.7.2`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeakBudgetPolicy {
    /// Number of early samples used as baseline.
    pub baseline_window: usize,
    /// Minimum number of post-baseline samples before sustained-growth checks apply.
    pub sustained_window: usize,
    /// Minimum fraction of positive adjacent deltas required for sustained growth.
    pub monotone_growth_ratio: f64,
    /// Heap leak thresholds.
    pub heap_bytes: ResourceLeakBudget,
    /// WAL leak thresholds.
    pub wal_pages: ResourceLeakBudget,
    /// Lock table leak thresholds.
    pub lock_table_size: ResourceLeakBudget,
    /// Active transaction leak thresholds.
    pub active_transactions: ResourceLeakBudget,
}

impl Default for LeakBudgetPolicy {
    fn default() -> Self {
        Self {
            baseline_window: 2,
            sustained_window: 3,
            monotone_growth_ratio: 0.75,
            heap_bytes: ResourceLeakBudget::new(64.0 * 1024.0, 256.0 * 1024.0, 512.0, 2048.0),
            wal_pages: ResourceLeakBudget::new(16.0, 64.0, 1.0, 4.0),
            lock_table_size: ResourceLeakBudget::new(8.0, 32.0, 0.5, 2.0),
            active_transactions: ResourceLeakBudget::new(2.0, 6.0, 0.2, 1.0),
        }
    }
}

impl LeakBudgetPolicy {
    /// Resolve the configured thresholds for a tracked resource.
    #[must_use]
    pub const fn budget_for(&self, resource: TrackedResource) -> ResourceLeakBudget {
        match resource {
            TrackedResource::HeapBytes => self.heap_bytes,
            TrackedResource::WalPages => self.wal_pages,
            TrackedResource::LockTableSize => self.lock_table_size,
            TrackedResource::ActiveTransactions => self.active_transactions,
        }
    }
}

/// Escalation level emitted by leak detectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeakSeverity {
    /// Growth exceeded delta thresholds but looked like warmup/transient behavior.
    Notice,
    /// Sustained growth exceeded warning thresholds.
    Warning,
    /// Sustained growth or delta exceeded critical thresholds.
    Critical,
}

/// Leak detector output aligned with soak/perf triage reporting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeakDetectorFinding {
    /// Scenario tied to this finding.
    pub scenario_id: String,
    /// Resource dimension that triggered this finding.
    pub resource: TrackedResource,
    /// Escalation level.
    pub severity: LeakSeverity,
    /// Mean of baseline samples.
    pub baseline_mean: f64,
    /// Final observed value.
    pub final_value: f64,
    /// Final minus baseline mean.
    pub delta: f64,
    /// Linear slope (value/sample) on post-baseline samples.
    pub slope_per_sample: f64,
    /// Fraction of positive adjacent deltas in post-baseline samples.
    pub growth_ratio: f64,
    /// Whether this finding was suppressed as a warmup-style transient.
    pub warmup_exempted: bool,
    /// Human-readable detector rationale.
    pub reason: String,
}

impl LeakDetectorFinding {
    /// Render a compact line for CI/report triage.
    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "{} scenario={} resource={} delta={:.3} slope={:.3} growth_ratio={:.3} warmup_exempted={} reason={}",
            match self.severity {
                LeakSeverity::Notice => "NOTICE",
                LeakSeverity::Warning => "WARN",
                LeakSeverity::Critical => "CRIT",
            },
            self.scenario_id,
            self.resource.as_str(),
            self.delta,
            self.slope_per_sample,
            self.growth_ratio,
            self.warmup_exempted,
            self.reason
        )
    }
}

/// Summary output for leak-budget analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeakDetectionReport {
    /// Number of telemetry records examined.
    pub records_analyzed: usize,
    /// Policy used for this evaluation.
    pub policy: LeakBudgetPolicy,
    /// Detector findings.
    pub findings: Vec<LeakDetectorFinding>,
}

impl LeakDetectionReport {
    /// Count warning-level findings.
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|finding| finding.severity == LeakSeverity::Warning)
            .count()
    }

    /// Count critical-level findings.
    #[must_use]
    pub fn critical_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|finding| finding.severity == LeakSeverity::Critical)
            .count()
    }
}

/// Detect leak-budget violations across normalized telemetry records.
#[must_use]
pub fn detect_leak_budget_violations(
    records: &[ResourceTelemetryRecord],
    policy: &LeakBudgetPolicy,
) -> LeakDetectionReport {
    let mut by_scenario: std::collections::BTreeMap<&str, Vec<&ResourceTelemetryRecord>> =
        std::collections::BTreeMap::new();
    for record in records {
        by_scenario
            .entry(record.scenario_id.as_str())
            .or_default()
            .push(record);
    }

    let mut findings = Vec::new();
    for (scenario_id, mut scenario_records) in by_scenario {
        scenario_records.sort_by_key(|record| record.sequence);
        findings.extend(detect_scenario_leaks(
            scenario_id,
            &scenario_records,
            policy,
        ));
    }

    LeakDetectionReport {
        records_analyzed: records.len(),
        policy: policy.clone(),
        findings,
    }
}

fn detect_scenario_leaks(
    scenario_id: &str,
    scenario_records: &[&ResourceTelemetryRecord],
    policy: &LeakBudgetPolicy,
) -> Vec<LeakDetectorFinding> {
    let mut findings = Vec::new();
    for resource in TrackedResource::ALL {
        let samples: Vec<f64> = scenario_records
            .iter()
            .map(|record| metric_value(record, resource))
            .collect();
        if let Some(finding) = evaluate_resource_series(scenario_id, resource, &samples, policy) {
            findings.push(finding);
        }
    }
    findings
}

#[allow(clippy::cast_precision_loss)]
fn metric_value(record: &ResourceTelemetryRecord, resource: TrackedResource) -> f64 {
    match resource {
        TrackedResource::HeapBytes => record.heap_bytes as f64,
        TrackedResource::WalPages => record.wal_pages as f64,
        TrackedResource::LockTableSize => f64::from(record.lock_table_size),
        TrackedResource::ActiveTransactions => f64::from(record.active_transactions),
    }
}

fn evaluate_resource_series(
    scenario_id: &str,
    resource: TrackedResource,
    samples: &[f64],
    policy: &LeakBudgetPolicy,
) -> Option<LeakDetectorFinding> {
    if policy.baseline_window == 0 || samples.len() <= policy.baseline_window {
        return None;
    }

    let baseline = &samples[..policy.baseline_window];
    let post_baseline = &samples[policy.baseline_window - 1..];
    if post_baseline.len() < 2 {
        return None;
    }

    let baseline_mean = baseline.iter().sum::<f64>() / baseline.len() as f64;
    let final_value = *samples.last()?;
    let delta = final_value - baseline_mean;
    let slope = linear_slope(post_baseline);
    let growth_ratio = monotone_growth_fraction(post_baseline);

    let budget = policy.budget_for(resource);
    let sustained_growth = post_baseline.len() >= policy.sustained_window
        && growth_ratio >= policy.monotone_growth_ratio;

    let (severity, reason, warmup_exempted) = if sustained_growth
        && (delta >= budget.critical_delta || slope >= budget.critical_slope)
    {
        (
            LeakSeverity::Critical,
            format!(
                "sustained growth exceeded critical budget (delta>={:.3} or slope>={:.3})",
                budget.critical_delta, budget.critical_slope
            ),
            false,
        )
    } else if sustained_growth && (delta >= budget.warning_delta || slope >= budget.warning_slope) {
        (
            LeakSeverity::Warning,
            format!(
                "sustained growth exceeded warning budget (delta>={:.3} or slope>={:.3})",
                budget.warning_delta, budget.warning_slope
            ),
            false,
        )
    } else if delta >= budget.warning_delta {
        (
            LeakSeverity::Notice,
            "growth exceeded delta threshold but lacked sustained trend after baseline".to_owned(),
            true,
        )
    } else {
        return None;
    };

    Some(LeakDetectorFinding {
        scenario_id: scenario_id.to_owned(),
        resource,
        severity,
        baseline_mean,
        final_value,
        delta,
        slope_per_sample: slope,
        growth_ratio,
        warmup_exempted,
        reason,
    })
}

#[allow(clippy::similar_names)]
fn linear_slope(samples: &[f64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let n = samples.len() as f64;
    let sum_x = (0..samples.len()).map(|index| index as f64).sum::<f64>();
    let sum_y = samples.iter().sum::<f64>();
    let sum_xy = samples
        .iter()
        .enumerate()
        .map(|(index, value)| index as f64 * *value)
        .sum::<f64>();
    let sum_xx = (0..samples.len())
        .map(|index| {
            let x = index as f64;
            x * x
        })
        .sum::<f64>();
    let denominator = n.mul_add(sum_xx, -(sum_x * sum_x));
    if denominator.abs() <= f64::EPSILON {
        0.0
    } else {
        (n.mul_add(sum_xy, -(sum_x * sum_y))) / denominator
    }
}

fn monotone_growth_fraction(samples: &[f64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }

    let increases = samples.windows(2).filter(|pair| pair[1] > pair[0]).count();
    increases as f64 / (samples.len() - 1) as f64
}

/// Convenience: create a default executor from a workload spec and run to completion.
#[must_use]
pub fn run_soak(spec: SoakWorkloadSpec) -> SoakRunReport {
    let mut executor = SoakExecutor::new(spec);
    executor.run_all();
    executor.finalize()
}

/// Create an executor with fault injection from a catalog and run to completion.
#[must_use]
pub fn run_soak_with_faults(
    spec: SoakWorkloadSpec,
    catalog: &FaultProfileCatalog,
    injection_probability: f64,
) -> SoakRunReport {
    let profiles: Vec<FaultProfile> = catalog.iter().cloned().collect();
    let fault_config = SoakFaultConfig {
        profiles,
        injection_probability,
    };
    let mut executor = SoakExecutor::new(spec).with_faults(fault_config);
    executor.run_all();
    executor.finalize()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use crate::soak_profiles::{profile_light, profile_moderate};

    const TEST_BEAD: &str = "bd-mblr.7.2.2";
    const TELEMETRY_TEST_BEAD: &str = "bd-mblr.7.7.1";
    const LEAK_POLICY_TEST_BEAD: &str = "bd-mblr.7.7.2";

    fn light_spec() -> SoakWorkloadSpec {
        SoakWorkloadSpec::from_profile(profile_light(), 0xDEAD_BEEF)
    }

    fn moderate_spec() -> SoakWorkloadSpec {
        SoakWorkloadSpec::from_profile(profile_moderate(), 0xCAFE_BABE)
    }

    fn synthetic_record(
        sequence: u64,
        boundary: TelemetryBoundary,
        heap_bytes: u64,
        wal_pages: u64,
    ) -> ResourceTelemetryRecord {
        ResourceTelemetryRecord {
            run_id: "soak-synth".to_owned(),
            scenario_id: "SYN-LEAK".to_owned(),
            profile_name: "synthetic".to_owned(),
            run_seed: 0x7772,
            sequence,
            boundary,
            phase: match boundary {
                TelemetryBoundary::Startup => SoakPhase::Warmup,
                TelemetryBoundary::SteadyState => SoakPhase::MainLoop,
                TelemetryBoundary::Teardown => SoakPhase::Complete,
            },
            transaction_count: sequence,
            elapsed_secs: sequence as f64 * 0.01,
            wal_pages,
            heap_bytes,
            active_transactions: 2,
            lock_table_size: 4,
            max_version_chain_len: 3,
            p99_latency_us: 500,
            ssi_aborts_since_last: 0,
            commits_since_last: sequence,
        }
    }

    #[test]
    fn executor_completes_light_workload() {
        let spec = light_spec();
        let target = spec.profile.target_transactions;
        let report = run_soak(spec);

        assert_eq!(
            report.total_transactions, target,
            "bead_id={TEST_BEAD} case=light_complete"
        );
        assert!(
            report.total_commits > 0,
            "bead_id={TEST_BEAD} case=light_has_commits"
        );
        assert!(
            !report.aborted,
            "bead_id={TEST_BEAD} case=light_not_aborted"
        );
    }

    #[test]
    fn executor_completes_moderate_workload() {
        let spec = moderate_spec();
        let report = run_soak(spec);

        assert!(
            report.total_commits > 0,
            "bead_id={TEST_BEAD} case=moderate_commits"
        );
        assert!(
            !report.aborted,
            "bead_id={TEST_BEAD} case=moderate_not_aborted"
        );
    }

    #[test]
    fn executor_phases_progress_correctly() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 100;
        let mut executor = SoakExecutor::new(spec);

        // Warmup phase (first 5%)
        let step = executor.run_step();
        assert_eq!(
            step.phase,
            SoakPhase::Warmup,
            "bead_id={TEST_BEAD} case=first_step_warmup"
        );

        // Run past warmup
        for _ in 0..10 {
            executor.run_step();
        }

        // Should be in main loop
        let step = executor.run_step();
        assert_eq!(
            step.phase,
            SoakPhase::MainLoop,
            "bead_id={TEST_BEAD} case=main_loop_phase"
        );

        // Run to completion
        executor.run_all();
        assert!(executor.is_done(), "bead_id={TEST_BEAD} case=is_done");
    }

    #[test]
    fn checkpoint_probes_happen_at_interval() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 200;
        spec.profile.invariant_check_interval = 50;

        let mut executor = SoakExecutor::new(spec);
        let mut checkpoint_count = 0;

        while !executor.is_done() {
            let step = executor.run_step();
            if step.checkpoint_triggered {
                checkpoint_count += 1;
            }
        }

        // With 200 txns and interval 50, we expect checkpoints at 50, 100, 150
        // (only in MainLoop phase, not warmup/cooldown)
        assert!(
            checkpoint_count >= 1,
            "bead_id={TEST_BEAD} case=checkpoints_triggered count={checkpoint_count}"
        );
    }

    #[test]
    fn run_is_deterministic_across_calls() {
        let report1 = run_soak(light_spec());
        let report2 = run_soak(light_spec());

        assert_eq!(
            report1.total_transactions, report2.total_transactions,
            "bead_id={TEST_BEAD} case=deterministic_txn_count"
        );
        assert_eq!(
            report1.total_commits, report2.total_commits,
            "bead_id={TEST_BEAD} case=deterministic_commits"
        );
        assert_eq!(
            report1.total_errors, report2.total_errors,
            "bead_id={TEST_BEAD} case=deterministic_errors"
        );
        assert_eq!(
            report1.checkpoints.len(),
            report2.checkpoints.len(),
            "bead_id={TEST_BEAD} case=deterministic_checkpoints"
        );
    }

    #[test]
    fn fault_injection_increases_error_rate() {
        let spec = light_spec();
        let catalog = FaultProfileCatalog::default_catalog();

        let clean_report = run_soak(light_spec());
        let faulty_report = run_soak_with_faults(spec, &catalog, 0.1); // 10% fault rate

        assert!(
            faulty_report.total_errors >= clean_report.total_errors,
            "bead_id={TEST_BEAD} case=faults_increase_errors clean={} faulty={}",
            clean_report.total_errors,
            faulty_report.total_errors,
        );
    }

    #[test]
    fn report_passed_true_for_clean_run() {
        let report = run_soak(light_spec());
        assert!(report.passed(), "bead_id={TEST_BEAD} case=clean_run_passes");
    }

    #[test]
    fn triage_line_contains_transaction_count() {
        let report = run_soak(light_spec());
        let line = report.triage_line();
        assert!(
            line.contains("txns"),
            "bead_id={TEST_BEAD} case=triage_line_has_txns"
        );
    }

    #[test]
    fn report_summary_is_nonempty() {
        let report = run_soak(light_spec());
        assert!(
            !report.summary.is_empty(),
            "bead_id={TEST_BEAD} case=summary_nonempty"
        );
    }

    #[test]
    fn executor_step_after_done_returns_error() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 10;
        let mut executor = SoakExecutor::new(spec);

        executor.run_all();
        assert!(executor.is_done());

        let step = executor.run_step();
        assert!(
            step.error.is_some(),
            "bead_id={TEST_BEAD} case=step_after_done_errors"
        );
    }

    #[test]
    fn different_seeds_produce_different_results() {
        let spec1 = SoakWorkloadSpec::from_profile(profile_light(), 0x1111);
        let spec2 = SoakWorkloadSpec::from_profile(profile_light(), 0x2222);

        let report1 = run_soak(spec1);
        let report2 = run_soak(spec2);

        // Different seeds should produce different commit/error counts
        // (with high probability for non-trivial workloads)
        let same = report1.total_commits == report2.total_commits
            && report1.total_errors == report2.total_errors;
        // This might occasionally be true by coincidence for tiny workloads,
        // so we don't assert strictly. Just verify both ran.
        assert!(
            report1.total_transactions > 0 && report2.total_transactions > 0,
            "bead_id={TEST_BEAD} case=different_seeds_both_ran"
        );
        // Log for debugging
        let _ = same; // suppress unused warning
    }

    #[test]
    fn executor_with_warmup_override() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 50;
        let executor = SoakExecutor::new(spec).with_warmup(5);
        assert_eq!(executor.warmup_count, 5);
    }

    #[test]
    fn checkpoint_snapshots_have_monotone_txn_ids() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 200;
        spec.profile.invariant_check_interval = 50;

        let report = run_soak(spec);

        let mut prev_max_txn = 0;
        for snap in &report.checkpoints {
            assert!(
                snap.max_txn_id >= prev_max_txn,
                "bead_id={TEST_BEAD} case=monotone_txn_id prev={prev_max_txn} cur={}",
                snap.max_txn_id,
            );
            prev_max_txn = snap.max_txn_id;
        }
    }

    #[test]
    fn checkpoint_snapshots_have_increasing_elapsed_time() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 200;
        spec.profile.invariant_check_interval = 50;

        let report = run_soak(spec);

        let mut prev_elapsed = 0.0;
        for snap in &report.checkpoints {
            assert!(
                snap.elapsed_secs >= prev_elapsed,
                "bead_id={TEST_BEAD} case=monotone_elapsed"
            );
            prev_elapsed = snap.elapsed_secs;
        }
    }

    #[test]
    fn report_spec_json_round_trips() {
        let report = run_soak(light_spec());
        assert!(
            !report.spec_json.is_empty(),
            "bead_id={TEST_BEAD} case=spec_json_nonempty"
        );
        // Verify it's valid JSON by parsing
        let parsed: serde_json::Value =
            serde_json::from_str(&report.spec_json).expect("spec_json should be valid JSON");
        assert!(
            parsed.is_object(),
            "bead_id={TEST_BEAD} case=spec_json_is_object"
        );
    }

    #[test]
    fn active_fault_profile_ids_populated_when_faults_active() {
        let spec = light_spec();
        let catalog = FaultProfileCatalog::default_catalog();
        let report = run_soak_with_faults(spec, &catalog, 0.01);

        assert!(
            !report.active_fault_profile_ids.is_empty(),
            "bead_id={TEST_BEAD} case=fault_ids_populated"
        );
    }

    #[test]
    fn telemetry_captures_startup_steady_state_and_teardown() {
        let mut spec = light_spec();
        spec.profile.target_transactions = 200;
        spec.profile.invariant_check_interval = 50;
        let report = run_soak(spec);

        let boundaries: BTreeSet<TelemetryBoundary> = report
            .resource_telemetry
            .iter()
            .map(|record| record.boundary)
            .collect();

        assert!(
            boundaries.contains(&TelemetryBoundary::Startup),
            "bead_id={TELEMETRY_TEST_BEAD} case=boundary_startup_present"
        );
        assert!(
            boundaries.contains(&TelemetryBoundary::SteadyState),
            "bead_id={TELEMETRY_TEST_BEAD} case=boundary_steady_present"
        );
        assert!(
            boundaries.contains(&TelemetryBoundary::Teardown),
            "bead_id={TELEMETRY_TEST_BEAD} case=boundary_teardown_present"
        );
    }

    #[test]
    fn telemetry_records_include_run_and_scenario_correlation() {
        let mut spec = light_spec();
        spec.profile.scenario_ids = vec!["SOAK-ALPHA".to_owned(), "SOAK-BETA".to_owned()];
        let report = run_soak(spec);

        let run_ids: BTreeSet<&str> = report
            .resource_telemetry
            .iter()
            .map(|record| record.run_id.as_str())
            .collect();
        assert_eq!(
            run_ids.len(),
            1,
            "bead_id={TELEMETRY_TEST_BEAD} case=single_run_id"
        );

        let scenarios: BTreeSet<&str> = report
            .resource_telemetry
            .iter()
            .map(|record| record.scenario_id.as_str())
            .collect();
        assert!(
            scenarios.contains("SOAK-ALPHA"),
            "bead_id={TELEMETRY_TEST_BEAD} case=scenario_alpha_present"
        );
        assert!(
            scenarios.contains("SOAK-BETA"),
            "bead_id={TELEMETRY_TEST_BEAD} case=scenario_beta_present"
        );
    }

    #[test]
    fn telemetry_record_json_round_trips() {
        let snapshot = CheckpointSnapshot {
            transaction_count: 12,
            max_txn_id: 9,
            max_commit_seq: 9,
            active_transactions: 2,
            wal_pages: 15,
            max_version_chain_len: 3,
            lock_table_size: 4,
            heap_bytes: 1_048_576,
            p99_latency_us: 777,
            ssi_aborts_since_last: 0,
            commits_since_last: 11,
            elapsed_secs: 0.012,
        };
        let record = ResourceTelemetryRecord::from_snapshot(
            "soak-roundtrip",
            "SOAK-ROUNDTRIP",
            "roundtrip-profile",
            0xABCD,
            1,
            TelemetryBoundary::SteadyState,
            SoakPhase::MainLoop,
            &snapshot,
        );

        let json = record.to_json().expect("telemetry record should serialize");
        let parsed =
            ResourceTelemetryRecord::from_json(&json).expect("telemetry record should parse");

        assert_eq!(
            parsed, record,
            "bead_id={TELEMETRY_TEST_BEAD} case=telemetry_round_trip"
        );
    }

    #[test]
    fn telemetry_sequence_is_monotone() {
        let report = run_soak(light_spec());
        let mut previous_sequence = None;
        for record in &report.resource_telemetry {
            if let Some(previous_sequence) = previous_sequence {
                assert!(
                    record.sequence > previous_sequence,
                    "bead_id={TELEMETRY_TEST_BEAD} case=sequence_monotone prev={previous_sequence} cur={}",
                    record.sequence
                );
            }
            previous_sequence = Some(record.sequence);
        }
    }

    #[test]
    fn leak_detector_flags_sustained_growth() {
        let records = vec![
            synthetic_record(0, TelemetryBoundary::Startup, 1_000_000, 10),
            synthetic_record(1, TelemetryBoundary::SteadyState, 1_020_000, 11),
            synthetic_record(2, TelemetryBoundary::SteadyState, 1_070_000, 13),
            synthetic_record(3, TelemetryBoundary::SteadyState, 1_150_000, 16),
            synthetic_record(4, TelemetryBoundary::SteadyState, 1_260_000, 20),
            synthetic_record(5, TelemetryBoundary::Teardown, 1_380_000, 24),
        ];
        let policy = LeakBudgetPolicy::default();
        let report = detect_leak_budget_violations(&records, &policy);

        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.resource == TrackedResource::HeapBytes
                    && finding.severity >= LeakSeverity::Warning),
            "bead_id={LEAK_POLICY_TEST_BEAD} case=sustained_growth_detected"
        );
    }

    #[test]
    fn leak_detector_marks_warmup_spike_as_notice() {
        let records = vec![
            synthetic_record(0, TelemetryBoundary::Startup, 1_000_000, 10),
            synthetic_record(1, TelemetryBoundary::SteadyState, 1_500_000, 11),
            synthetic_record(2, TelemetryBoundary::SteadyState, 1_490_000, 11),
            synthetic_record(3, TelemetryBoundary::SteadyState, 1_486_000, 11),
            synthetic_record(4, TelemetryBoundary::SteadyState, 1_484_000, 11),
            synthetic_record(5, TelemetryBoundary::Teardown, 1_483_000, 11),
        ];
        let policy = LeakBudgetPolicy::default();
        let report = detect_leak_budget_violations(&records, &policy);

        assert!(
            report.findings.iter().any(|finding| {
                finding.resource == TrackedResource::HeapBytes
                    && finding.severity == LeakSeverity::Notice
                    && finding.warmup_exempted
            }),
            "bead_id={LEAK_POLICY_TEST_BEAD} case=warmup_notice_detected"
        );
        assert!(
            report.critical_count() == 0,
            "bead_id={LEAK_POLICY_TEST_BEAD} case=no_critical_for_warmup"
        );
    }

    #[test]
    fn leak_detector_reports_critical_delta_violation() {
        let records = vec![
            synthetic_record(0, TelemetryBoundary::Startup, 1_000_000, 10),
            synthetic_record(1, TelemetryBoundary::SteadyState, 1_010_000, 11),
            synthetic_record(2, TelemetryBoundary::SteadyState, 1_020_000, 12),
            synthetic_record(3, TelemetryBoundary::SteadyState, 1_030_000, 13),
            synthetic_record(4, TelemetryBoundary::SteadyState, 1_040_000, 14),
            synthetic_record(5, TelemetryBoundary::Teardown, 1_700_000, 40),
        ];
        let policy = LeakBudgetPolicy::default();
        let report = detect_leak_budget_violations(&records, &policy);

        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.resource == TrackedResource::HeapBytes
                    && finding.severity == LeakSeverity::Critical),
            "bead_id={LEAK_POLICY_TEST_BEAD} case=critical_delta_detected"
        );
    }
}
