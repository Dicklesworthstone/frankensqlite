use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Verification gate scope bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateScope {
    Universal,
    Phase2,
    Phase3,
    Phase4,
    Phase5,
    Phase6,
    Phase7,
    Phase8,
    Phase9,
}

/// Execution status for a single gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Passed,
    Failed,
    Skipped,
}

/// Declarative gate entry used for execution and reporting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct GatePlanEntry {
    pub gate_id: String,
    pub gate_name: String,
    pub scope: GateScope,
    pub command: Vec<String>,
    pub env: Vec<(String, String)>,
    pub expected_exit_code: i32,
}

/// Concrete result of running a single gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct GateExecutionResult {
    pub gate_id: String,
    pub gate_name: String,
    pub scope: GateScope,
    pub status: GateStatus,
    pub command: Vec<String>,
    pub env: Vec<(String, String)>,
    pub expected_exit_code: i32,
    pub actual_exit_code: Option<i32>,
    pub duration_ms: u128,
    pub stdout: String,
    pub stderr: String,
    pub skipped_reason: Option<String>,
}

/// Machine-readable report for Phase 1-3 verification gates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
#[allow(clippy::struct_field_names)]
pub struct PhaseGateReport {
    pub schema_version: u32,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub overall_pass: bool,
    pub universal_pass: bool,
    pub phase2_pass: bool,
    pub phase3_pass: bool,
    pub blocked_by_universal_failure: bool,
    pub blocked_by_phase2_failure: bool,
    pub gates: Vec<GateExecutionResult>,
}

/// Machine-readable report for Phase 4-6 verification gates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
#[allow(clippy::struct_field_names)]
pub struct CorePhaseGateReport {
    pub schema_version: u32,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub overall_pass: bool,
    pub phase4_pass: bool,
    pub phase5_pass: bool,
    pub phase6_pass: bool,
    pub blocked_by_phase4_failure: bool,
    pub blocked_by_phase5_failure: bool,
    pub gates: Vec<GateExecutionResult>,
}

/// Machine-readable report for Phase 7-9 verification gates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
#[allow(clippy::struct_field_names)]
pub struct LatePhaseGateReport {
    pub schema_version: u32,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub overall_pass: bool,
    pub phase7_pass: bool,
    pub phase8_pass: bool,
    pub phase9_pass: bool,
    pub blocked_by_phase7_failure: bool,
    pub blocked_by_phase8_failure: bool,
    pub gates: Vec<GateExecutionResult>,
}

/// Summary counters for a gate run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct GateRunSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
}

/// Machine-readable report for universal verification gates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct UniversalGateReport {
    pub schema_version: u32,
    pub generated_unix_ms: u128,
    pub workspace_root: String,
    pub trace_id: String,
    pub gates: Vec<GateExecutionResult>,
    pub summary: GateRunSummary,
    pub phase_ready: bool,
}

/// Raw command output captured from gate execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateCommandOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Abstraction for running gate commands.
pub trait GateCommandRunner {
    /// Execute a single gate command in the workspace root.
    ///
    /// Returns stdout/stderr and process exit code.
    fn run_gate(
        &self,
        gate_id: &str,
        command: &[String],
        env: &[(String, String)],
        workspace_root: &Path,
    ) -> io::Result<GateCommandOutput>;
}

/// Default process-backed command executor.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessGateCommandRunner;

impl GateCommandRunner for ProcessGateCommandRunner {
    fn run_gate(
        &self,
        gate_id: &str,
        command: &[String],
        env: &[(String, String)],
        workspace_root: &Path,
    ) -> io::Result<GateCommandOutput> {
        let Some((program, args)) = command.split_first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("empty_command gate_id={gate_id}"),
            ));
        };

        let mut process = Command::new(program);
        process.args(args).current_dir(workspace_root);
        for (key, value) in env {
            process.env(key, value);
        }

        let output = process.output()?;
        Ok(GateCommandOutput {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// Return the canonical Phase 1-3 gate plan.
#[must_use]
pub fn phase_1_to_3_gate_plan() -> Vec<GatePlanEntry> {
    gate_specs().iter().map(as_plan_entry).collect()
}

/// Return the canonical Phase 4-6 gate plan.
#[must_use]
pub fn phase_4_to_6_gate_plan() -> Vec<GatePlanEntry> {
    core_gate_specs().iter().map(as_plan_entry).collect()
}

/// Return the canonical Phase 7-9 gate plan.
#[must_use]
pub fn phase_7_to_9_gate_plan() -> Vec<GatePlanEntry> {
    late_phase_gate_specs().iter().map(as_plan_entry).collect()
}

/// Run Phase 1-3 gates using the default process-backed executor.
#[must_use]
pub fn run_phase_1_to_3_gates(workspace_root: &Path) -> PhaseGateReport {
    let runner = ProcessGateCommandRunner;
    run_phase_1_to_3_gates_with_runner(workspace_root, &runner)
}

/// Run universal gates using the default process-backed executor.
#[must_use]
pub fn run_universal_gates(workspace_root: &Path) -> UniversalGateReport {
    let runner = ProcessGateCommandRunner;
    run_universal_gates_with_runner(workspace_root, &runner)
}

/// Run Phase 4-6 gates using the default process-backed executor.
#[must_use]
pub fn run_phase_4_to_6_gates(workspace_root: &Path) -> CorePhaseGateReport {
    let runner = ProcessGateCommandRunner;
    run_phase_4_to_6_gates_with_runner(workspace_root, &runner)
}

/// Run Phase 7-9 gates using the default process-backed executor.
#[must_use]
pub fn run_phase_7_to_9_gates(workspace_root: &Path) -> LatePhaseGateReport {
    let runner = ProcessGateCommandRunner;
    run_phase_7_to_9_gates_with_runner(workspace_root, &runner)
}

/// Run universal gates with a custom executor.
#[must_use]
pub fn run_universal_gates_with_runner<R: GateCommandRunner>(
    workspace_root: &Path,
    runner: &R,
) -> UniversalGateReport {
    let gate_plan = phase_1_to_3_gate_plan();
    let mut gates = Vec::new();
    let phase_ready = run_scope(
        GateScope::Universal,
        &gate_plan,
        workspace_root,
        runner,
        &mut gates,
    );
    let passed = gates
        .iter()
        .filter(|gate| gate.status == GateStatus::Passed)
        .count();
    let failed = gates
        .iter()
        .filter(|gate| gate.status == GateStatus::Failed)
        .count();

    UniversalGateReport {
        schema_version: 1,
        generated_unix_ms: unix_time_ms(),
        workspace_root: workspace_root.display().to_string(),
        trace_id: format!("uvg-{}", unix_time_ms()),
        gates,
        summary: GateRunSummary {
            total: passed + failed,
            passed,
            failed,
        },
        phase_ready,
    }
}

/// Run Phase 1-3 gates with a custom executor.
#[must_use]
pub fn run_phase_1_to_3_gates_with_runner<R: GateCommandRunner>(
    workspace_root: &Path,
    runner: &R,
) -> PhaseGateReport {
    let gate_plan = phase_1_to_3_gate_plan();
    let mut gates = Vec::with_capacity(gate_plan.len());

    let universal_pass = run_scope(
        GateScope::Universal,
        &gate_plan,
        workspace_root,
        runner,
        &mut gates,
    );

    let mut phase2_pass = false;
    let mut phase3_pass = false;
    let mut blocked_by_universal_failure = false;
    let mut blocked_by_phase2_failure = false;

    if universal_pass {
        phase2_pass = run_scope(
            GateScope::Phase2,
            &gate_plan,
            workspace_root,
            runner,
            &mut gates,
        );
        if phase2_pass {
            phase3_pass = run_scope(
                GateScope::Phase3,
                &gate_plan,
                workspace_root,
                runner,
                &mut gates,
            );
        } else {
            blocked_by_phase2_failure = true;
            push_skipped_scope(
                GateScope::Phase3,
                &gate_plan,
                "blocked_by_phase2_failure",
                &mut gates,
            );
        }
    } else {
        blocked_by_universal_failure = true;
        push_skipped_scope(
            GateScope::Phase2,
            &gate_plan,
            "blocked_by_universal_failure",
            &mut gates,
        );
        push_skipped_scope(
            GateScope::Phase3,
            &gate_plan,
            "blocked_by_universal_failure",
            &mut gates,
        );
    }

    let overall_pass = universal_pass && phase2_pass && phase3_pass;

    PhaseGateReport {
        schema_version: 1,
        generated_unix_ms: unix_time_ms(),
        workspace_root: workspace_root.display().to_string(),
        overall_pass,
        universal_pass,
        phase2_pass,
        phase3_pass,
        blocked_by_universal_failure,
        blocked_by_phase2_failure,
        gates,
    }
}

/// Run Phase 4-6 gates with a custom executor.
#[must_use]
pub fn run_phase_4_to_6_gates_with_runner<R: GateCommandRunner>(
    workspace_root: &Path,
    runner: &R,
) -> CorePhaseGateReport {
    let gate_plan = phase_4_to_6_gate_plan();
    let mut gates = Vec::with_capacity(gate_plan.len());

    let phase4_pass = run_scope(
        GateScope::Phase4,
        &gate_plan,
        workspace_root,
        runner,
        &mut gates,
    );

    let mut phase5_pass = false;
    let mut phase6_pass = false;
    let mut blocked_by_phase4_failure = false;
    let mut blocked_by_phase5_failure = false;

    if phase4_pass {
        phase5_pass = run_scope(
            GateScope::Phase5,
            &gate_plan,
            workspace_root,
            runner,
            &mut gates,
        );
        if phase5_pass {
            phase6_pass = run_scope(
                GateScope::Phase6,
                &gate_plan,
                workspace_root,
                runner,
                &mut gates,
            );
        } else {
            blocked_by_phase5_failure = true;
            push_skipped_scope(
                GateScope::Phase6,
                &gate_plan,
                "blocked_by_phase5_failure",
                &mut gates,
            );
        }
    } else {
        blocked_by_phase4_failure = true;
        push_skipped_scope(
            GateScope::Phase5,
            &gate_plan,
            "blocked_by_phase4_failure",
            &mut gates,
        );
        push_skipped_scope(
            GateScope::Phase6,
            &gate_plan,
            "blocked_by_phase4_failure",
            &mut gates,
        );
    }

    let overall_pass = phase4_pass && phase5_pass && phase6_pass;

    CorePhaseGateReport {
        schema_version: 1,
        generated_unix_ms: unix_time_ms(),
        workspace_root: workspace_root.display().to_string(),
        overall_pass,
        phase4_pass,
        phase5_pass,
        phase6_pass,
        blocked_by_phase4_failure,
        blocked_by_phase5_failure,
        gates,
    }
}

/// Run Phase 7-9 gates with a custom executor.
#[must_use]
pub fn run_phase_7_to_9_gates_with_runner<R: GateCommandRunner>(
    workspace_root: &Path,
    runner: &R,
) -> LatePhaseGateReport {
    let gate_plan = phase_7_to_9_gate_plan();
    let mut gates = Vec::with_capacity(gate_plan.len());

    let phase7_pass = run_scope(
        GateScope::Phase7,
        &gate_plan,
        workspace_root,
        runner,
        &mut gates,
    );

    let mut phase8_pass = false;
    let mut phase9_pass = false;
    let mut blocked_by_phase7_failure = false;
    let mut blocked_by_phase8_failure = false;

    if phase7_pass {
        phase8_pass = run_scope(
            GateScope::Phase8,
            &gate_plan,
            workspace_root,
            runner,
            &mut gates,
        );
        if phase8_pass {
            phase9_pass = run_scope(
                GateScope::Phase9,
                &gate_plan,
                workspace_root,
                runner,
                &mut gates,
            );
        } else {
            blocked_by_phase8_failure = true;
            push_skipped_scope(
                GateScope::Phase9,
                &gate_plan,
                "blocked_by_phase8_failure",
                &mut gates,
            );
        }
    } else {
        blocked_by_phase7_failure = true;
        push_skipped_scope(
            GateScope::Phase8,
            &gate_plan,
            "blocked_by_phase7_failure",
            &mut gates,
        );
        push_skipped_scope(
            GateScope::Phase9,
            &gate_plan,
            "blocked_by_phase7_failure",
            &mut gates,
        );
    }

    let overall_pass = phase7_pass && phase8_pass && phase9_pass;

    LatePhaseGateReport {
        schema_version: 1,
        generated_unix_ms: unix_time_ms(),
        workspace_root: workspace_root.display().to_string(),
        overall_pass,
        phase7_pass,
        phase8_pass,
        phase9_pass,
        blocked_by_phase7_failure,
        blocked_by_phase8_failure,
        gates,
    }
}

/// Persist a phase gate report as pretty JSON.
///
/// # Errors
///
/// Returns an error if serialization or writing fails.
pub fn write_phase_gate_report(path: &Path, report: &PhaseGateReport) -> io::Result<()> {
    let json = serde_json::to_string_pretty(report).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("phase_gate_report_serialize_failed: {error}"),
        )
    })?;
    fs::write(path, json)
}

/// Persist a core phase gate report as pretty JSON.
///
/// # Errors
///
/// Returns an error if serialization or writing fails.
pub fn write_core_phase_gate_report(path: &Path, report: &CorePhaseGateReport) -> io::Result<()> {
    let json = serde_json::to_string_pretty(report).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("core_phase_gate_report_serialize_failed: {error}"),
        )
    })?;
    fs::write(path, json)
}

/// Persist a late phase gate report as pretty JSON.
///
/// # Errors
///
/// Returns an error if serialization or writing fails.
pub fn write_late_phase_gate_report(path: &Path, report: &LatePhaseGateReport) -> io::Result<()> {
    let json = serde_json::to_string_pretty(report).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("late_phase_gate_report_serialize_failed: {error}"),
        )
    })?;
    fs::write(path, json)
}

/// Persist a universal gate report as pretty JSON.
///
/// # Errors
///
/// Returns an error if serialization or writing fails.
pub fn write_universal_gate_report(path: &Path, report: &UniversalGateReport) -> io::Result<()> {
    let json = serde_json::to_string_pretty(report).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("universal_gate_report_serialize_failed: {error}"),
        )
    })?;
    fs::write(path, json)
}

#[derive(Debug, Clone, Copy)]
struct GateSpec {
    gate_id: &'static str,
    gate_name: &'static str,
    scope: GateScope,
    command: &'static [&'static str],
    env: &'static [(&'static str, &'static str)],
    expected_exit_code: i32,
}

#[allow(clippy::too_many_lines)]
fn gate_specs() -> Vec<GateSpec> {
    vec![
        GateSpec {
            gate_id: "universal.cargo_check",
            gate_name: "Universal gate: cargo check --all-targets",
            scope: GateScope::Universal,
            command: &["cargo", "check", "--workspace", "--all-targets"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.cargo_clippy",
            gate_name: "Universal gate: cargo clippy --all-targets -D warnings",
            scope: GateScope::Universal,
            command: &[
                "cargo",
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.cargo_fmt",
            gate_name: "Universal gate: cargo fmt --check",
            scope: GateScope::Universal,
            command: &["cargo", "fmt", "--all", "--", "--check"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.cargo_test",
            gate_name: "Universal gate: cargo test --workspace",
            scope: GateScope::Universal,
            command: &["cargo", "test", "--workspace"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.undocumented_ignores",
            gate_name: "Universal gate: no undocumented #[ignore] annotations",
            scope: GateScope::Universal,
            command: &[
                "bash",
                "-lc",
                "set -euo pipefail; undocumented=0; while IFS=: read -r file line _; do prev_line=$((line-1)); prev=\"\"; if [ \"$prev_line\" -ge 1 ]; then prev=$(sed -n \"${prev_line}p\" \"$file\"); fi; cur=$(sed -n \"${line}p\" \"$file\"); if [[ \"$prev\" != *\"//\"* && \"$cur\" != *\"//\"* ]]; then echo \"undocumented_ignore ${file}:${line}\"; undocumented=1; fi; done < <(rg -n --glob '*.rs' '#\\\\[ignore\\\\]' crates tests || true); exit \"$undocumented\"",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.cargo_doc",
            gate_name: "Universal gate: cargo doc --workspace --no-deps",
            scope: GateScope::Universal,
            command: &["cargo", "doc", "--workspace", "--no-deps"],
            env: &[("RUSTDOCFLAGS", "-D warnings")],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.no_unsafe_allow_override",
            gate_name: "Universal gate: forbid allow(unsafe_code) overrides",
            scope: GateScope::Universal,
            command: &[
                "rg",
                "-n",
                "allow\\(unsafe_code\\)",
                "crates",
                "src",
                "tests",
            ],
            env: &[],
            expected_exit_code: 1,
        },
        GateSpec {
            gate_id: "universal.no_unsafe_block",
            gate_name: "Universal gate: forbid unsafe blocks",
            scope: GateScope::Universal,
            command: &["rg", "-n", "\\bunsafe\\s*\\{", "crates", "src", "tests"],
            env: &[],
            expected_exit_code: 1,
        },
        GateSpec {
            gate_id: "universal.br_dep_cycles",
            gate_name: "Universal gate: beads dependency graph has no cycles",
            scope: GateScope::Universal,
            command: &["br", "dep", "cycles"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.beads_synced",
            gate_name: "Universal gate: beads export synced to .beads/",
            scope: GateScope::Universal,
            command: &[
                "bash",
                "-lc",
                "set -euo pipefail; br sync --flush-only >/dev/null; git diff --exit-code -- .beads/",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.audit_spec_authority",
            gate_name: "Universal gate: spec authority + TOC integrity audit",
            scope: GateScope::Universal,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "--test",
                "spec_authority_integrity_audit",
                "test_e2e_bd_1wx_5_compliance",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.audit_scope_doctrine",
            gate_name: "Universal gate: scope doctrine enforcement audit",
            scope: GateScope::Universal,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "--test",
                "spec_to_beads_audit",
                "test_e2e_bd_1wx_3_compliance",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.audit_spec_beads_completeness",
            gate_name: "Universal gate: spec-to-beads completeness audit",
            scope: GateScope::Universal,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "--test",
                "spec_to_beads_audit",
                "test_e2e_spec_to_beads_audit_report_schema_stable",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase2.memoryvfs_contract",
            gate_name: "Phase 2 gate: MemoryVfs contract tests",
            scope: GateScope::Phase2,
            command: &["cargo", "test", "-p", "fsqlite-vfs"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase2.record_roundtrip",
            gate_name: "Phase 2 gate: record round-trip proptest (10k cases)",
            scope: GateScope::Phase2,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-types",
                "prop_record_roundtrip_arbitrary",
            ],
            env: &[("PROPTEST_CASES", "10000")],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase2.no_unsafe",
            gate_name: "Phase 2 gate: no unsafe blocks in workspace",
            scope: GateScope::Phase2,
            command: &["rg", "--glob", "*.rs", "unsafe\\s*\\{", "crates", "tests"],
            env: &[],
            expected_exit_code: 1,
        },
        GateSpec {
            gate_id: "phase3.btree_proptest",
            gate_name: "Phase 3 gate: B-tree proptest invariants",
            scope: GateScope::Phase3,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-btree",
                "prop_btree_order_invariant",
            ],
            env: &[("PROPTEST_CASES", "10000")],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase3.btree_cursor_reference",
            gate_name: "Phase 3 gate: B-tree cursor vs BTreeMap reference",
            scope: GateScope::Phase3,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-btree",
                "prop_btree_vs_btreemap_reference",
            ],
            env: &[("PROPTEST_CASES", "10000")],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase3.parser_coverage",
            gate_name: "Phase 3 gate: parser coverage suite",
            scope: GateScope::Phase3,
            command: &["cargo", "test", "-p", "fsqlite-parser"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase3.parser_fuzz",
            gate_name: "Phase 3 gate: parser fuzz surrogate run",
            scope: GateScope::Phase3,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-parser",
                "test_parser_roundtrip_proptest",
            ],
            env: &[("PROPTEST_CASES", "10000")],
            expected_exit_code: 0,
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn core_gate_specs() -> Vec<GateSpec> {
    vec![
        GateSpec {
            gate_id: "phase4.sql_conformance_20",
            gate_name: "Phase 4 gate: SQL conformance (20 tests)",
            scope: GateScope::Phase4,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase4_gate_sql_conformance_20",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase4.vdbe_explain",
            gate_name: "Phase 4 gate: VDBE EXPLAIN output sequence",
            scope: GateScope::Phase4,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase4_gate_vdbe_explain",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase4.sorter_100k",
            gate_name: "Phase 4 gate: sorter correctness on 100k rows",
            scope: GateScope::Phase4,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase4_gate_sorter_100k",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase5.format_write_read_c",
            gate_name: "Phase 5 gate: FrankenSQLite DB readable by C sqlite3",
            scope: GateScope::Phase5,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase5_gate_format_write_read_c",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase5.format_read_c_write",
            gate_name: "Phase 5 gate: C sqlite3 DB readable by FrankenSQLite",
            scope: GateScope::Phase5,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase5_gate_format_read_c_write",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase5.wal_crash_recovery",
            gate_name: "Phase 5 gate: WAL crash recovery (100 scenarios)",
            scope: GateScope::Phase5,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase5_gate_wal_crash_recovery",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase5.raptorq_wal",
            gate_name: "Phase 5 gate: RaptorQ WAL recovery under corruption",
            scope: GateScope::Phase5,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase5_gate_raptorq_wal",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase5.raptorq_harness",
            gate_name: "Phase 5 gate: RaptorQ harness (loss/corruption/perf)",
            scope: GateScope::Phase5,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_e2e_raptorq_harness",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.mvcc_stress",
            gate_name: "Phase 6 gate: MVCC stress (100 writers x 100 ops)",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_mvcc_stress",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.ssi_write_skew",
            gate_name: "Phase 6 gate: SSI write skew detection",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_ssi_write_skew",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.ssi_mazurkiewicz",
            gate_name: "Phase 6 gate: SSI Mazurkiewicz no-false-negatives",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_ssi_mazurkiewicz",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.ssi_witness_epoch",
            gate_name: "Phase 6 gate: witness plane TxnEpoch validation",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_ssi_witness_epoch",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.ssi_witness_decode",
            gate_name: "Phase 6 gate: witness decode under symbol loss",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_ssi_witness_decode",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.snapshot_mazurkiewicz",
            gate_name: "Phase 6 gate: snapshot isolation Mazurkiewicz exploration",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_snapshot_mazurkiewicz",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.eprocess_inv",
            gate_name: "Phase 6 gate: e-process invariants INV-1..INV-7",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_eprocess_inv",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.gc_memory",
            gate_name: "Phase 6 gate: GC memory bound <= 2x theoretical minimum",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_gc_memory",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.serialized_parity",
            gate_name: "Phase 6 gate: serialized-mode parity with C SQLite",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_serialized_parity",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.rebase_merge",
            gate_name: "Phase 6 gate: rebase merge success (1k attempts)",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_rebase_merge",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.structured_merge",
            gate_name: "Phase 6 gate: structured merge safety",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_structured_merge",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase6.crash_model",
            gate_name: "Phase 6 gate: crash model durability scenarios",
            scope: GateScope::Phase6,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase6_gate_crash_model",
            ],
            env: &[],
            expected_exit_code: 0,
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn late_phase_gate_specs() -> Vec<GateSpec> {
    vec![
        GateSpec {
            gate_id: "phase7.index_usage",
            gate_name: "Phase 7 gate: EXPLAIN QUERY PLAN index usage",
            scope: GateScope::Phase7,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase7_gate_index_usage",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase7.window_functions",
            gate_name: "Phase 7 gate: window functions (50 conformance tests)",
            scope: GateScope::Phase7,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase7_gate_window_functions",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase7.recursive_cte_limit",
            gate_name: "Phase 7 gate: recursive CTE terminates with LIMIT",
            scope: GateScope::Phase7,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase7_gate_recursive_cte_limit",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase7.leak_budget_ci",
            gate_name: "Phase 7 gate: leak budget CI diagnostics artifact",
            scope: GateScope::Phase7,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "--test",
                "bd_mblr_7_7_3_ci_leak_gate",
                "ci_leak_gate_enforces_budget_and_emits_actionable_diagnostics",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase8.json1",
            gate_name: "Phase 8 gate: JSON1 conformance (200 tests)",
            scope: GateScope::Phase8,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase8_gate_json1",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase8.fts5",
            gate_name: "Phase 8 gate: FTS5 full-text search (100 queries)",
            scope: GateScope::Phase8,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase8_gate_fts5",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase8.rtree",
            gate_name: "Phase 8 gate: R*-Tree spatial queries (50 bbox tests)",
            scope: GateScope::Phase8,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase8_gate_rtree",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase9.conformance_golden",
            gate_name: "Phase 9 gate: 100% parity target (1,000+ golden files)",
            scope: GateScope::Phase9,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase9_gate_conformance_golden",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase9.benchmark_3x",
            gate_name: "Phase 9 gate: single-writer benchmark within 3x of C SQLite",
            scope: GateScope::Phase9,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase9_gate_benchmark_3x",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase9.no_regression",
            gate_name: "Phase 9 gate: no regression vs Phase 8 (conformal p-value)",
            scope: GateScope::Phase9,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase9_gate_no_regression",
            ],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "phase9.replication_loss",
            gate_name: "Phase 9 gate: replication under 10% packet loss",
            scope: GateScope::Phase9,
            command: &[
                "cargo",
                "test",
                "-p",
                "fsqlite-harness",
                "test_phase9_gate_replication_loss",
            ],
            env: &[],
            expected_exit_code: 0,
        },
    ]
}

fn as_plan_entry(spec: &GateSpec) -> GatePlanEntry {
    GatePlanEntry {
        gate_id: spec.gate_id.to_owned(),
        gate_name: spec.gate_name.to_owned(),
        scope: spec.scope,
        command: spec.command.iter().map(|part| (*part).to_owned()).collect(),
        env: spec
            .env
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        expected_exit_code: spec.expected_exit_code,
    }
}

fn run_scope<R: GateCommandRunner>(
    scope: GateScope,
    gate_plan: &[GatePlanEntry],
    workspace_root: &Path,
    runner: &R,
    sink: &mut Vec<GateExecutionResult>,
) -> bool {
    let mut all_pass = true;
    for gate in gate_plan.iter().filter(|gate| gate.scope == scope) {
        let execution = execute_gate(gate, workspace_root, runner);
        if execution.status != GateStatus::Passed {
            all_pass = false;
        }
        sink.push(execution);
    }
    all_pass
}

fn push_skipped_scope(
    scope: GateScope,
    gate_plan: &[GatePlanEntry],
    reason: &str,
    sink: &mut Vec<GateExecutionResult>,
) {
    for gate in gate_plan.iter().filter(|gate| gate.scope == scope) {
        sink.push(GateExecutionResult {
            gate_id: gate.gate_id.clone(),
            gate_name: gate.gate_name.clone(),
            scope: gate.scope,
            status: GateStatus::Skipped,
            command: gate.command.clone(),
            env: gate.env.clone(),
            expected_exit_code: gate.expected_exit_code,
            actual_exit_code: None,
            duration_ms: 0,
            stdout: String::new(),
            stderr: String::new(),
            skipped_reason: Some(reason.to_owned()),
        });
    }
}

fn execute_gate<R: GateCommandRunner>(
    gate: &GatePlanEntry,
    workspace_root: &Path,
    runner: &R,
) -> GateExecutionResult {
    let started_at = Instant::now();
    match runner.run_gate(&gate.gate_id, &gate.command, &gate.env, workspace_root) {
        Ok(output) => {
            let status = if output.exit_code == gate.expected_exit_code {
                GateStatus::Passed
            } else {
                GateStatus::Failed
            };

            GateExecutionResult {
                gate_id: gate.gate_id.clone(),
                gate_name: gate.gate_name.clone(),
                scope: gate.scope,
                status,
                command: gate.command.clone(),
                env: gate.env.clone(),
                expected_exit_code: gate.expected_exit_code,
                actual_exit_code: Some(output.exit_code),
                duration_ms: started_at.elapsed().as_millis(),
                stdout: output.stdout,
                stderr: output.stderr,
                skipped_reason: None,
            }
        }
        Err(error) => GateExecutionResult {
            gate_id: gate.gate_id.clone(),
            gate_name: gate.gate_name.clone(),
            scope: gate.scope,
            status: GateStatus::Failed,
            command: gate.command.clone(),
            env: gate.env.clone(),
            expected_exit_code: gate.expected_exit_code,
            actual_exit_code: None,
            duration_ms: started_at.elapsed().as_millis(),
            stdout: String::new(),
            stderr: format!("runner_error: {error}"),
            skipped_reason: None,
        },
    }
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use super::{
        GateCommandOutput, GateCommandRunner, GateScope, GateStatus, phase_1_to_3_gate_plan,
        phase_4_to_6_gate_plan, phase_7_to_9_gate_plan, run_phase_1_to_3_gates_with_runner,
        run_phase_4_to_6_gates_with_runner, run_phase_7_to_9_gates_with_runner,
        run_universal_gates_with_runner,
    };

    #[derive(Debug, Default)]
    struct MockRunner {
        fail_gate_ids: HashSet<String>,
        invocations: Mutex<Vec<String>>,
    }

    impl MockRunner {
        fn with_failures<I, S>(failures: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: AsRef<str>,
        {
            Self {
                fail_gate_ids: failures
                    .into_iter()
                    .map(|id| id.as_ref().to_owned())
                    .collect(),
                invocations: Mutex::new(Vec::new()),
            }
        }

        fn invocations(&self) -> Vec<String> {
            self.invocations
                .lock()
                .expect("mock runner lock should not poison")
                .clone()
        }
    }

    impl GateCommandRunner for MockRunner {
        fn run_gate(
            &self,
            gate_id: &str,
            _command: &[String],
            _env: &[(String, String)],
            _workspace_root: &std::path::Path,
        ) -> std::io::Result<GateCommandOutput> {
            self.invocations
                .lock()
                .expect("mock runner lock should not poison")
                .push(gate_id.to_owned());

            let exit_code = if self.fail_gate_ids.contains(gate_id) {
                2
            } else {
                match gate_id {
                    "phase2.no_unsafe"
                    | "universal.no_unsafe_allow_override"
                    | "universal.no_unsafe_block" => 1,
                    _ => 0,
                }
            };

            Ok(GateCommandOutput {
                exit_code,
                stdout: format!("stdout gate_id={gate_id}"),
                stderr: String::new(),
            })
        }
    }

    fn find_gate<'a>(plan: &'a [super::GatePlanEntry], gate_id: &str) -> &'a super::GatePlanEntry {
        plan.iter()
            .find(|gate| gate.gate_id == gate_id)
            .expect("gate should exist in plan")
    }

    #[test]
    fn test_phase2_gate_memoryvfs_contract() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase2.memoryvfs_contract");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase2);
        assert!(command.contains("cargo test -p fsqlite-vfs"));
    }

    #[test]
    fn test_universal_gate_cargo_check() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "universal.cargo_check");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Universal);
        assert!(command.contains("cargo check --workspace --all-targets"));
    }

    #[test]
    fn test_universal_gate_clippy() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "universal.cargo_clippy");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Universal);
        assert!(command.contains("cargo clippy --workspace --all-targets -- -D warnings"));
    }

    #[test]
    fn test_universal_gate_fmt() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "universal.cargo_fmt");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Universal);
        assert!(command.contains("cargo fmt --all -- --check"));
    }

    #[test]
    fn test_universal_gate_tests() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "universal.cargo_test");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Universal);
        assert!(command.contains("cargo test --workspace"));
    }

    #[test]
    fn test_universal_gate_docs() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "universal.cargo_doc");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Universal);
        assert!(command.contains("cargo doc --workspace --no-deps"));
        assert!(
            gate.env
                .contains(&("RUSTDOCFLAGS".to_owned(), "-D warnings".to_owned()))
        );
    }

    #[test]
    fn test_gate_fails_on_warning() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["universal.cargo_check"]);
        let report = run_universal_gates_with_runner(workspace, &runner);

        assert!(!report.phase_ready);
        assert_eq!(report.summary.failed, 1);
        assert!(report.gates.iter().any(|gate| {
            gate.gate_id == "universal.cargo_check" && gate.status == GateStatus::Failed
        }));
    }

    #[test]
    fn test_gate_fails_on_undocumented_ignore() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["universal.undocumented_ignores"]);
        let report = run_universal_gates_with_runner(workspace, &runner);

        assert!(!report.phase_ready);
        assert!(report.gates.iter().any(|gate| {
            gate.gate_id == "universal.undocumented_ignores" && gate.status == GateStatus::Failed
        }));
    }

    #[test]
    fn test_universal_gate_no_unsafe() {
        let plan = phase_1_to_3_gate_plan();
        let allow_gate = find_gate(&plan, "universal.no_unsafe_allow_override");
        let unsafe_block_gate = find_gate(&plan, "universal.no_unsafe_block");

        assert_eq!(allow_gate.scope, GateScope::Universal);
        assert_eq!(allow_gate.expected_exit_code, 1);
        assert!(
            allow_gate
                .command
                .join(" ")
                .contains("allow\\(unsafe_code\\)")
        );

        assert_eq!(unsafe_block_gate.scope, GateScope::Universal);
        assert_eq!(unsafe_block_gate.expected_exit_code, 1);
        assert!(
            unsafe_block_gate
                .command
                .join(" ")
                .contains("\\bunsafe\\s*\\{")
        );
    }

    #[test]
    fn test_universal_gate_beads_and_governance() {
        let plan = phase_1_to_3_gate_plan();
        let dep_cycles = find_gate(&plan, "universal.br_dep_cycles");
        let beads_synced = find_gate(&plan, "universal.beads_synced");
        let audit_authority = find_gate(&plan, "universal.audit_spec_authority");
        let audit_scope = find_gate(&plan, "universal.audit_scope_doctrine");
        let audit_completeness = find_gate(&plan, "universal.audit_spec_beads_completeness");

        assert_eq!(dep_cycles.scope, GateScope::Universal);
        assert_eq!(beads_synced.scope, GateScope::Universal);
        assert_eq!(audit_authority.scope, GateScope::Universal);
        assert_eq!(audit_scope.scope, GateScope::Universal);
        assert_eq!(audit_completeness.scope, GateScope::Universal);

        assert_eq!(dep_cycles.command.join(" "), "br dep cycles");
        assert!(
            beads_synced
                .command
                .join(" ")
                .contains("br sync --flush-only")
        );
        assert!(
            audit_authority
                .command
                .join(" ")
                .contains("spec_authority_integrity_audit")
        );
        assert!(
            audit_scope
                .command
                .join(" ")
                .contains("test_e2e_bd_1wx_3_compliance")
        );
        assert!(
            audit_completeness
                .command
                .join(" ")
                .contains("test_e2e_spec_to_beads_audit_report_schema_stable")
        );
    }

    #[test]
    fn test_phase2_gate_record_roundtrip() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase2.record_roundtrip");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase2);
        assert!(command.contains("prop_record_roundtrip_arbitrary"));
        assert!(
            gate.env
                .contains(&("PROPTEST_CASES".to_owned(), "10000".to_owned()))
        );
    }

    #[test]
    fn test_phase2_gate_no_unsafe() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase2.no_unsafe");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase2);
        assert_eq!(gate.expected_exit_code, 1);
        assert!(command.starts_with("rg "));
    }

    #[test]
    fn test_phase3_gate_btree_proptest() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase3.btree_proptest");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase3);
        assert!(command.contains("prop_btree_order_invariant"));
    }

    #[test]
    fn test_phase3_gate_btree_cursor_reference() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase3.btree_cursor_reference");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase3);
        assert!(command.contains("prop_btree_vs_btreemap_reference"));
    }

    #[test]
    fn test_phase3_gate_parser_coverage() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase3.parser_coverage");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase3);
        assert!(command.contains("cargo test -p fsqlite-parser"));
    }

    #[test]
    fn test_phase3_gate_parser_fuzz() {
        let plan = phase_1_to_3_gate_plan();
        let gate = find_gate(&plan, "phase3.parser_fuzz");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase3);
        assert!(command.contains("test_parser_roundtrip_proptest"));
        assert!(
            gate.env
                .contains(&("PROPTEST_CASES".to_owned(), "10000".to_owned()))
        );
    }

    #[test]
    fn test_gate_runner_blocks_phase_gates_when_universal_fails() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["universal.cargo_check"]);
        let report = run_phase_1_to_3_gates_with_runner(workspace, &runner);

        assert!(!report.overall_pass);
        assert!(!report.universal_pass);
        assert!(report.blocked_by_universal_failure);
        assert!(!report.phase2_pass);
        assert!(!report.phase3_pass);

        let phase2_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase2)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();
        let phase3_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase3)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();

        assert!(
            phase2_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
        assert!(
            phase3_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
    }

    #[test]
    fn test_gate_runner_skips_phase3_when_phase2_fails() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["phase2.record_roundtrip"]);
        let report = run_phase_1_to_3_gates_with_runner(workspace, &runner);

        assert!(report.universal_pass);
        assert!(!report.phase2_pass);
        assert!(!report.phase3_pass);
        assert!(report.blocked_by_phase2_failure);

        let phase3_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase3)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();
        assert!(
            phase3_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );

        let invocations = runner.invocations();
        assert!(
            !invocations.iter().any(|id| id.starts_with("phase3.")),
            "phase3 gates should not execute after phase2 failure"
        );
    }

    #[test]
    fn test_gate_runner_all_pass() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::default();
        let report = run_phase_1_to_3_gates_with_runner(workspace, &runner);

        assert!(report.overall_pass);
        assert!(report.universal_pass);
        assert!(report.phase2_pass);
        assert!(report.phase3_pass);
        assert!(!report.blocked_by_universal_failure);
        assert!(!report.blocked_by_phase2_failure);
        assert!(
            report
                .gates
                .iter()
                .all(|gate| gate.status == GateStatus::Passed)
        );
    }

    #[test]
    fn test_phase4_gate_sql_conformance_20() {
        let plan = phase_4_to_6_gate_plan();
        let gate = find_gate(&plan, "phase4.sql_conformance_20");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase4);
        assert!(command.contains("test_phase4_gate_sql_conformance_20"));
    }

    #[test]
    fn test_phase5_gate_wal_crash_recovery() {
        let plan = phase_4_to_6_gate_plan();
        let gate = find_gate(&plan, "phase5.wal_crash_recovery");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase5);
        assert!(command.contains("test_phase5_gate_wal_crash_recovery"));
    }

    #[test]
    fn test_phase5_gate_raptorq_harness() {
        let plan = phase_4_to_6_gate_plan();
        let gate = find_gate(&plan, "phase5.raptorq_harness");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase5);
        assert!(command.contains("test_e2e_raptorq_harness"));
    }

    #[test]
    fn test_phase6_gate_crash_model() {
        let plan = phase_4_to_6_gate_plan();
        let gate = find_gate(&plan, "phase6.crash_model");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase6);
        assert!(command.contains("test_phase6_gate_crash_model"));
    }

    #[test]
    fn test_core_gate_runner_blocks_phase5_and_phase6_when_phase4_fails() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["phase4.vdbe_explain"]);
        let report = run_phase_4_to_6_gates_with_runner(workspace, &runner);

        assert!(!report.overall_pass);
        assert!(!report.phase4_pass);
        assert!(report.blocked_by_phase4_failure);
        assert!(!report.phase5_pass);
        assert!(!report.phase6_pass);

        let phase5_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase5)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();
        let phase6_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase6)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();

        assert!(
            phase5_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
        assert!(
            phase6_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
    }

    #[test]
    fn test_core_gate_runner_skips_phase6_when_phase5_fails() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["phase5.wal_crash_recovery"]);
        let report = run_phase_4_to_6_gates_with_runner(workspace, &runner);

        assert!(report.phase4_pass);
        assert!(!report.phase5_pass);
        assert!(!report.phase6_pass);
        assert!(report.blocked_by_phase5_failure);

        let phase6_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase6)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();
        assert!(
            phase6_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
    }

    #[test]
    fn test_core_gate_runner_all_pass() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::default();
        let report = run_phase_4_to_6_gates_with_runner(workspace, &runner);

        assert!(report.overall_pass);
        assert!(report.phase4_pass);
        assert!(report.phase5_pass);
        assert!(report.phase6_pass);
        assert!(!report.blocked_by_phase4_failure);
        assert!(!report.blocked_by_phase5_failure);
        assert!(
            report
                .gates
                .iter()
                .all(|gate| gate.status == GateStatus::Passed)
        );
    }

    #[test]
    fn test_phase7_gate_index_usage() {
        let plan = phase_7_to_9_gate_plan();
        let gate = find_gate(&plan, "phase7.index_usage");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase7);
        assert!(command.contains("test_phase7_gate_index_usage"));
    }

    #[test]
    fn test_phase7_gate_leak_budget_ci_diagnostics() {
        let plan = phase_7_to_9_gate_plan();
        let gate = find_gate(&plan, "phase7.leak_budget_ci");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase7);
        assert!(command.contains("bd_mblr_7_7_3_ci_leak_gate"));
        assert!(command.contains("ci_leak_gate_enforces_budget_and_emits_actionable_diagnostics"));
    }

    #[test]
    fn test_phase9_gate_replication_loss() {
        let plan = phase_7_to_9_gate_plan();
        let gate = find_gate(&plan, "phase9.replication_loss");
        let command = gate.command.join(" ");

        assert_eq!(gate.scope, GateScope::Phase9);
        assert!(command.contains("test_phase9_gate_replication_loss"));
    }

    #[test]
    fn test_late_gate_runner_blocks_phase8_and_phase9_when_phase7_fails() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["phase7.window_functions"]);
        let report = run_phase_7_to_9_gates_with_runner(workspace, &runner);

        assert!(!report.overall_pass);
        assert!(!report.phase7_pass);
        assert!(report.blocked_by_phase7_failure);
        assert!(!report.phase8_pass);
        assert!(!report.phase9_pass);

        let phase8_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase8)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();
        let phase9_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase9)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();

        assert!(
            phase8_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
        assert!(
            phase9_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
    }

    #[test]
    fn test_late_gate_runner_skips_phase9_when_phase8_fails() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::with_failures(["phase8.json1"]);
        let report = run_phase_7_to_9_gates_with_runner(workspace, &runner);

        assert!(report.phase7_pass);
        assert!(!report.phase8_pass);
        assert!(!report.phase9_pass);
        assert!(report.blocked_by_phase8_failure);

        let phase9_statuses = report
            .gates
            .iter()
            .filter(|gate| gate.scope == GateScope::Phase9)
            .map(|gate| gate.status)
            .collect::<Vec<_>>();
        assert!(
            phase9_statuses
                .iter()
                .all(|status| *status == GateStatus::Skipped)
        );
    }

    #[test]
    fn test_late_gate_runner_all_pass() {
        let workspace = std::path::Path::new(".");
        let runner = MockRunner::default();
        let report = run_phase_7_to_9_gates_with_runner(workspace, &runner);

        assert!(report.overall_pass);
        assert!(report.phase7_pass);
        assert!(report.phase8_pass);
        assert!(report.phase9_pass);
        assert!(!report.blocked_by_phase7_failure);
        assert!(!report.blocked_by_phase8_failure);
        assert!(
            report
                .gates
                .iter()
                .all(|gate| gate.status == GateStatus::Passed)
        );
    }
}
