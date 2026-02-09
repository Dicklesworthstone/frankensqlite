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

/// Run Phase 1-3 gates using the default process-backed executor.
#[must_use]
pub fn run_phase_1_to_3_gates(workspace_root: &Path) -> PhaseGateReport {
    let runner = ProcessGateCommandRunner;
    run_phase_1_to_3_gates_with_runner(workspace_root, &runner)
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

#[derive(Debug, Clone, Copy)]
struct GateSpec {
    gate_id: &'static str,
    gate_name: &'static str,
    scope: GateScope,
    command: &'static [&'static str],
    env: &'static [(&'static str, &'static str)],
    expected_exit_code: i32,
}

fn gate_specs() -> Vec<GateSpec> {
    vec![
        GateSpec {
            gate_id: "universal.cargo_check",
            gate_name: "Universal gate: cargo check --all-targets",
            scope: GateScope::Universal,
            command: &["cargo", "check", "--all-targets"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.cargo_clippy",
            gate_name: "Universal gate: cargo clippy --all-targets -D warnings",
            scope: GateScope::Universal,
            command: &["cargo", "clippy", "--all-targets", "--", "-D", "warnings"],
            env: &[],
            expected_exit_code: 0,
        },
        GateSpec {
            gate_id: "universal.cargo_fmt",
            gate_name: "Universal gate: cargo fmt --check",
            scope: GateScope::Universal,
            command: &["cargo", "fmt", "--check"],
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
            gate_id: "universal.cargo_doc",
            gate_name: "Universal gate: cargo doc --workspace --no-deps",
            scope: GateScope::Universal,
            command: &["cargo", "doc", "--workspace", "--no-deps"],
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
        run_phase_1_to_3_gates_with_runner,
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
            } else if gate_id == "phase2.no_unsafe" {
                1
            } else {
                0
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
}
