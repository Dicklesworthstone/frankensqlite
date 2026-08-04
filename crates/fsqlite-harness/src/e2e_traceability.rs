//! E2E script inventory and scenario traceability matrix (bd-mblr.4.5.1).
//!
//! Provides a complete machine-readable inventory of all E2E integration scripts
//! in the FrankenSQLite workspace, along with scenario-to-script linkage,
//! storage/concurrency mode annotations, and gap analysis.
//!
//! # Architecture
//!
//! The traceability matrix connects three layers:
//! 1. **Scripts** — concrete test files (Rust integration tests, shell runners)
//! 2. **Scenarios** — logical test scenarios with stable IDs
//! 3. **Artifacts** — expected output paths and log schemas
//!
//! Every cataloged script has explicit scenario linkage; scripts without a
//! matching scenario carry a [`GapAnnotation`] with rationale.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.4.5.1";

// ─── Core Types ─────────────────────────────────────────────────────────

/// A cataloged E2E script or integration test.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScriptEntry {
    /// Workspace-relative file path (e.g. `e2e/build_matrix.sh`).
    pub path: String,
    /// Kind of test file.
    pub kind: ScriptKind,
    /// Associated bead ID, if any.
    pub bead_id: Option<String>,
    /// Human-readable description of what this script tests.
    pub description: String,
    /// Invocation contract: how to run this script.
    pub invocation: InvocationContract,
    /// Scenario IDs this script exercises.
    pub scenario_ids: Vec<String>,
    /// Storage modes exercised.
    pub storage_modes: Vec<StorageMode>,
    /// Concurrency modes exercised.
    pub concurrency_modes: Vec<ConcurrencyMode>,
    /// Expected failure artifact paths (relative to workspace).
    pub artifact_paths: Vec<String>,
    /// Logging schema version this script emits.
    pub log_schema_version: Option<String>,
}

/// Classification of test script type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScriptKind {
    /// Shell script in `e2e/` directory.
    ShellE2e,
    /// Shell script in `scripts/` directory.
    ShellUtility,
    /// Rust integration test in `crates/fsqlite-e2e/tests/`.
    RustE2eTest,
    /// Rust integration test in `crates/fsqlite-harness/tests/`.
    RustHarnessTest,
    /// Rust integration test in `crates/fsqlite-wal/tests/`.
    RustWalTest,
    /// TypeScript spec file.
    TypeScriptSpec,
}

/// How to invoke a script.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InvocationContract {
    /// The shell command to run (e.g. `cargo test -p fsqlite-e2e --test foo`).
    pub command: String,
    /// Environment variables required.
    pub env_vars: Vec<String>,
    /// Whether the script supports `--json` output.
    pub json_output: bool,
    /// Timeout hint in seconds.
    pub timeout_secs: Option<u32>,
}

/// Storage mode exercised by a test.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageMode {
    InMemory,
    FileBacked,
    Wal,
    RollbackJournal,
}

/// Concurrency mode exercised by a test.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConcurrencyMode {
    Sequential,
    ConcurrentWriters,
    MvccIsolation,
    Ssi,
}

/// Annotation for a script that doesn't map to a cataloged scenario.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GapAnnotation {
    /// Scenario ID or area that lacks coverage.
    pub area: String,
    /// Why this gap exists.
    pub rationale: String,
    /// Whether this gap is intentional (e.g. out of scope).
    pub intentional: bool,
}

/// Category for grouping scenarios.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScenarioCategory {
    Correctness,
    Concurrency,
    Recovery,
    Corruption,
    Compatibility,
    Extensions,
    Performance,
    Compliance,
    Infrastructure,
    Observability,
}

// ─── Traceability Matrix ────────────────────────────────────────────────

/// The complete traceability matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceabilityMatrix {
    /// Schema version for the matrix itself.
    pub schema_version: String,
    /// Bead ID that produced this matrix.
    pub bead_id: String,
    /// All cataloged scripts.
    pub scripts: Vec<ScriptEntry>,
    /// Gap annotations for uncovered areas.
    pub gaps: Vec<GapAnnotation>,
}

impl TraceabilityMatrix {
    /// Validate structural invariants of the matrix.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // 1. No duplicate script paths
        let mut seen_paths = BTreeSet::new();
        for s in &self.scripts {
            if !seen_paths.insert(&s.path) {
                errors.push(format!("Duplicate script path: {}", s.path));
            }
        }

        // 2. Every script must have at least one scenario or a gap annotation
        for s in &self.scripts {
            if s.scenario_ids.is_empty() && !self.has_gap_for_script(&s.path) {
                errors.push(format!(
                    "Script {} has no scenario IDs and no gap annotation",
                    s.path
                ));
            }
        }

        // 3. Every script must have at least one storage mode
        for s in &self.scripts {
            if s.storage_modes.is_empty() {
                errors.push(format!("Script {} has no storage mode annotations", s.path));
            }
        }

        // 4. Every script must have a non-empty invocation command
        for s in &self.scripts {
            if s.invocation.command.is_empty() {
                errors.push(format!("Script {} has empty invocation command", s.path));
            }
        }

        // 5. Scenario IDs should follow naming convention (CATEGORY-NUMBER)
        for s in &self.scripts {
            for sid in &s.scenario_ids {
                if !sid.contains('-') {
                    errors.push(format!(
                        "Scenario ID '{}' in {} doesn't follow CATEGORY-NUMBER convention",
                        sid, s.path
                    ));
                }
            }
        }

        errors
    }

    fn has_gap_for_script(&self, path: &str) -> bool {
        self.gaps
            .iter()
            .any(|g| g.area == path || g.area.contains(path))
    }

    /// Compute coverage statistics.
    pub fn coverage_stats(&self) -> CoverageStats {
        let total_scripts = self.scripts.len();
        let scripts_with_scenarios = self
            .scripts
            .iter()
            .filter(|s| !s.scenario_ids.is_empty())
            .count();
        let scripts_with_beads = self.scripts.iter().filter(|s| s.bead_id.is_some()).count();

        let all_scenario_ids: BTreeSet<_> = self
            .scripts
            .iter()
            .flat_map(|s| s.scenario_ids.iter().cloned())
            .collect();

        let mut by_kind: BTreeMap<ScriptKind, u32> = BTreeMap::new();
        for s in &self.scripts {
            *by_kind.entry(s.kind).or_default() += 1;
        }

        let mut by_storage: BTreeMap<StorageMode, u32> = BTreeMap::new();
        for s in &self.scripts {
            for mode in &s.storage_modes {
                *by_storage.entry(*mode).or_default() += 1;
            }
        }

        let mut by_concurrency: BTreeMap<ConcurrencyMode, u32> = BTreeMap::new();
        for s in &self.scripts {
            for mode in &s.concurrency_modes {
                *by_concurrency.entry(*mode).or_default() += 1;
            }
        }

        CoverageStats {
            total_scripts,
            scripts_with_scenarios,
            scripts_with_beads,
            unique_scenario_count: all_scenario_ids.len(),
            gap_count: self.gaps.len(),
            intentional_gaps: self.gaps.iter().filter(|g| g.intentional).count(),
            by_kind,
            by_storage,
            by_concurrency,
        }
    }

    /// Serialize to deterministic JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Coverage statistics summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageStats {
    pub total_scripts: usize,
    pub scripts_with_scenarios: usize,
    pub scripts_with_beads: usize,
    pub unique_scenario_count: usize,
    pub gap_count: usize,
    pub intentional_gaps: usize,
    pub by_kind: BTreeMap<ScriptKind, u32>,
    pub by_storage: BTreeMap<StorageMode, u32>,
    pub by_concurrency: BTreeMap<ConcurrencyMode, u32>,
}

// ─── Builders ───────────────────────────────────────────────────────────

/// Builder for constructing script entries ergonomically.
pub struct ScriptEntryBuilder {
    entry: ScriptEntry,
}

impl ScriptEntryBuilder {
    pub fn new(path: &str, kind: ScriptKind, description: &str) -> Self {
        Self {
            entry: ScriptEntry {
                path: path.to_owned(),
                kind,
                bead_id: None,
                description: description.to_owned(),
                invocation: InvocationContract {
                    command: String::new(),
                    env_vars: Vec::new(),
                    json_output: false,
                    timeout_secs: None,
                },
                scenario_ids: Vec::new(),
                storage_modes: Vec::new(),
                concurrency_modes: Vec::new(),
                artifact_paths: Vec::new(),
                log_schema_version: None,
            },
        }
    }

    #[must_use]
    pub fn bead(mut self, id: &str) -> Self {
        self.entry.bead_id = Some(id.to_owned());
        self
    }

    #[must_use]
    pub fn command(mut self, cmd: &str) -> Self {
        cmd.clone_into(&mut self.entry.invocation.command);
        self
    }

    #[must_use]
    pub fn env(mut self, var: &str) -> Self {
        self.entry.invocation.env_vars.push(var.to_owned());
        self
    }

    #[must_use]
    pub fn json_output(mut self) -> Self {
        self.entry.invocation.json_output = true;
        self
    }

    #[must_use]
    pub fn timeout(mut self, secs: u32) -> Self {
        self.entry.invocation.timeout_secs = Some(secs);
        self
    }

    #[must_use]
    pub fn scenarios(mut self, ids: &[&str]) -> Self {
        self.entry.scenario_ids = ids.iter().map(|s| (*s).to_owned()).collect();
        self
    }

    #[must_use]
    pub fn storage(mut self, modes: &[StorageMode]) -> Self {
        self.entry.storage_modes = modes.to_vec();
        self
    }

    #[must_use]
    pub fn concurrency(mut self, modes: &[ConcurrencyMode]) -> Self {
        self.entry.concurrency_modes = modes.to_vec();
        self
    }

    #[must_use]
    pub fn artifacts(mut self, paths: &[&str]) -> Self {
        self.entry.artifact_paths = paths.iter().map(|s| (*s).to_owned()).collect();
        self
    }

    #[must_use]
    pub fn log_schema(mut self, version: &str) -> Self {
        self.entry.log_schema_version = Some(version.to_owned());
        self
    }

    #[must_use]
    pub fn build(self) -> ScriptEntry {
        self.entry
    }
}

// ─── Canonical Inventory Builder ────────────────────────────────────────

/// Build the canonical E2E script inventory for the FrankenSQLite workspace.
///
/// This function catalogs every known E2E integration script with its
/// scenario linkage, storage/concurrency mode annotations, and invocation
/// contract.
#[allow(clippy::too_many_lines, clippy::vec_init_then_push)]
pub fn build_canonical_inventory() -> TraceabilityMatrix {
    let mut scripts = Vec::new();

    // ── Shell E2E scripts (e2e/) ─────────────────────────────────────
    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/build_matrix.sh",
            ScriptKind::ShellE2e,
            "Multi-variant feature-flag build matrix",
        )
        .bead("bd-2v8x")
        .command("bash e2e/build_matrix.sh")
        .scenarios(&["BUILD-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["target/bd-2v8x-build-matrix/logs/"])
        .log_schema("1.0.0")
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_2ddl_compliance.sh",
            ScriptKind::ShellE2e,
            "Per-crate test matrix compliance across all 23 workspace crates",
        )
        .bead("bd-2ddl")
        .command("bash e2e/bd_2ddl_compliance.sh")
        .json_output()
        .scenarios(&["COMPL-1", "COMPL-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["test-results/bd_2ddl_compliance_report.jsonl"])
        .log_schema("1.0.0")
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_2uza4_1_swizzle_protocol_pilot.sh",
            ScriptKind::ShellE2e,
            "Cache-line swizzle protocol pilot with structured telemetry",
        )
        .bead("bd-2uza4.1")
        .command("bash e2e/bd_2uza4_1_swizzle_protocol_pilot.sh --json")
        .json_output()
        .scenarios(&["CON-6"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["artifacts/swizzle_protocol_pilot/"])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_2v8x_compliance.sh",
            ScriptKind::ShellE2e,
            "Feature-flag variant build compliance",
        )
        .bead("bd-2v8x")
        .command("bash e2e/bd_2v8x_compliance.sh")
        .scenarios(&["BUILD-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_bca_1_compliance.sh",
            ScriptKind::ShellE2e,
            "Phase 5 concurrent-writer compliance gate",
        )
        .bead("bd-bca.1")
        .command("bash e2e/bd_bca_1_compliance.sh")
        .scenarios(&["CON-1", "CON-2", "MVCC-1"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::ConcurrentWriters,
            ConcurrencyMode::MvccIsolation,
        ])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_sxm2_compliance.sh",
            ScriptKind::ShellE2e,
            "Architectural verification compliance",
        )
        .bead("bd-sxm2")
        .command("bash e2e/bd_sxm2_compliance.sh")
        .scenarios(&["ARCH-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_i0m5_networking_stack_replication_under_loss.sh",
            ScriptKind::ShellE2e,
            "Network replication under packet loss",
        )
        .bead("bd-i0m5")
        .command("bash e2e/bd_i0m5_networking_stack_replication_under_loss.sh")
        .scenarios(&["NET-1", "REP-1"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .log_schema("1.0.0")
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_ncivz_1_parallel_wal_buffer_pilot.sh",
            ScriptKind::ShellE2e,
            "Parallel per-core WAL buffer pilot with deterministic replay artifacts",
        )
        .bead("bd-ncivz.1")
        .command("bash e2e/bd_ncivz_1_parallel_wal_buffer_pilot.sh --json")
        .json_output()
        .scenarios(&["E2E-CNC-007"])
        .storage(&[StorageMode::Wal, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .artifacts(&["artifacts/ncivz_1_parallel_wal_buffer/"])
        .log_schema("1.0.0")
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/fts3_compat_report.sh",
            ScriptKind::ShellE2e,
            "FTS3 extension compatibility verification",
        )
        .command("bash e2e/fts3_compat_report.sh")
        .scenarios(&["EXT-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/json_fts_wave_report.sh",
            ScriptKind::ShellE2e,
            "JSON1 + FTS parity closure wave report with differential evidence",
        )
        .bead("bd-1dp9.5.2")
        .command("bash e2e/json_fts_wave_report.sh --json")
        .json_output()
        .scenarios(&["EXT-1", "EXT-2", "EXT-4"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["test-results/bd_1dp9_5_2/"])
        .log_schema("1.0.0")
        .timeout(1200)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/extension_integrated_wave_report.sh",
            ScriptKind::ShellE2e,
            "Integrated extension parity wave with structured SQL traces and mismatch digests",
        )
        .bead("bd-1dp9.5.4")
        .command("bash e2e/extension_integrated_wave_report.sh --json")
        .json_output()
        .scenarios(&["EXT-1", "EXT-2", "EXT-3", "EXT-4"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["test-results/bd_1dp9_5_4/"])
        .log_schema("1.0.0")
        .timeout(1800)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/arc_warmup_report.sh",
            ScriptKind::ShellE2e,
            "ARC cache performance warmup analysis",
        )
        .command("bash e2e/arc_warmup_report.sh")
        .scenarios(&["PERF-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_1dp9_6_2_sql_pipeline_optimization_report.sh",
            ScriptKind::ShellE2e,
            "SQL pipeline hotspot optimization evidence report",
        )
        .bead("bd-1dp9.6.2")
        .command("bash e2e/bd_1dp9_6_2_sql_pipeline_optimization_report.sh --json")
        .json_output()
        .scenarios(&[
            "SQL-PIPELINE-OPT",
            "SQL-PIPELINE-OPT-UNIT",
            "SQL-PIPELINE-OPT-ARTIFACT",
        ])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["test-results/bd_1dp9_6_2/"])
        .log_schema("1.0.0")
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/bd_1dp9_9_1_execution_waves_report.sh",
            ScriptKind::ShellE2e,
            "Dependency-aware execution waves and staffing-lane report",
        )
        .bead("bd-1dp9.9.1")
        .command("bash e2e/bd_1dp9_9_1_execution_waves_report.sh --json")
        .json_output()
        .scenarios(&["PLAN-1", "INFRA-6"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["test-results/bd_1dp9_9_1/"])
        .log_schema("1.0.0")
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/realdb_integrity_check.sh",
            ScriptKind::ShellE2e,
            "Real database format integrity verification",
        )
        .command("bash e2e/realdb_integrity_check.sh")
        .scenarios(&["COMPAT-1", "COMPAT-2"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/reference_index_audit.sh",
            ScriptKind::ShellE2e,
            "Reference index correctness audit",
        )
        .bead("bd-4eue")
        .command("bash e2e/reference_index_audit.sh")
        .scenarios(&["IDX-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/spec_viz_smoke.sh",
            ScriptKind::ShellE2e,
            "Specification visualization smoke test",
        )
        .command("bash e2e/spec_viz_smoke.sh")
        .scenarios(&["INFRA-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(60)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/risk_register_report.sh",
            ScriptKind::ShellE2e,
            "Risk register generation and validation",
        )
        .bead("bd-3kp.2")
        .command("bash e2e/risk_register_report.sh")
        .scenarios(&["DOC-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/future_work_report.sh",
            ScriptKind::ShellE2e,
            "Future work item catalog and prioritization",
        )
        .bead("bd-3kp.3")
        .command("bash e2e/future_work_report.sh")
        .scenarios(&["DOC-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .log_schema("1.0.0")
        .timeout(120)
        .build(),
    );

    // ── Shell utility scripts (scripts/) ─────────────────────────────
    scripts.push(
        ScriptEntryBuilder::new(
            "scripts/test_inventory.sh",
            ScriptKind::ShellUtility,
            "Tracked-source test realism and Turso adaptation inventory",
        )
        .command("bash scripts/test_inventory.sh")
        .scenarios(&["INFRA-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&[
            "target/test-inventory/test_inventory.json",
            "target/test-inventory/summary.md",
            "target/test-inventory/test_inventory.csv",
        ])
        .timeout(180)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "scripts/coverage.sh",
            ScriptKind::ShellUtility,
            "LLVM-based code coverage reporting",
        )
        .command("bash scripts/coverage.sh")
        .scenarios(&["INFRA-3"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["target/coverage/"])
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "scripts/verify_parity_taxonomy.sh",
            ScriptKind::ShellUtility,
            "Parity taxonomy validation and scoring",
        )
        .bead("bd-1dp9.1.1")
        .command("bash scripts/verify_parity_taxonomy.sh --json")
        .json_output()
        .scenarios(&["PARITY-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "scripts/verify_bd_1sf8n_phase9_time_travel.sh",
            ScriptKind::ShellUtility,
            "Phase 9 time-travel verification gate artifact runner",
        )
        .bead("bd-1sf8n")
        .command("bash scripts/verify_bd_1sf8n_phase9_time_travel.sh")
        .scenarios(&["MVCC-7"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::MvccIsolation])
        .artifacts(&["artifacts/bd-1sf8n/"])
        .timeout(300)
        .build(),
    );

    // ── TypeScript spec ──────────────────────────────────────────────
    scripts.push(
        ScriptEntryBuilder::new(
            "e2e/frankensqlite.spec.ts",
            ScriptKind::TypeScriptSpec,
            "TypeScript E2E specification and test definitions",
        )
        .command("npx jest e2e/frankensqlite.spec.ts")
        .scenarios(&["SPEC-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    // ── Rust E2E tests (crates/fsqlite-e2e/tests/) ──────────────────
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/correctness_sequential_insert.rs",
            ScriptKind::RustE2eTest,
            "Sequential insertion correctness (fsqlite vs rusqlite)",
        )
        .command("cargo test -p fsqlite-e2e --test correctness_sequential_insert")
        .scenarios(&["COR-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/correctness_mixed_dml.rs",
            ScriptKind::RustE2eTest,
            "Mixed DML operations correctness",
        )
        .command("cargo test -p fsqlite-e2e --test correctness_mixed_dml")
        .scenarios(&["COR-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/correctness_mvcc_isolation.rs",
            ScriptKind::RustE2eTest,
            "MVCC isolation level validation",
        )
        .command("cargo test -p fsqlite-e2e --test correctness_mvcc_isolation")
        .scenarios(&["MVCC-2", "CON-4"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::MvccIsolation,
            ConcurrencyMode::ConcurrentWriters,
        ])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/correctness_concurrent_writes.rs",
            ScriptKind::RustE2eTest,
            "Concurrent multi-thread write correctness",
        )
        .bead("bd-244z")
        .command("cargo test -p fsqlite-e2e --test correctness_concurrent_writes")
        .scenarios(&["CON-3", "MVCC-3"])
        .storage(&[
            StorageMode::InMemory,
            StorageMode::FileBacked,
            StorageMode::Wal,
        ])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/correctness_transactions.rs",
            ScriptKind::RustE2eTest,
            "Transaction semantics (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)",
        )
        .command("cargo test -p fsqlite-e2e --test correctness_transactions")
        .scenarios(&["TXN-1", "TXN-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/mvcc_concurrent_writers.rs",
            ScriptKind::RustE2eTest,
            "MVCC concurrent writer stress tests",
        )
        .command("cargo test -p fsqlite-e2e --test mvcc_concurrent_writers")
        .scenarios(&["MVCC-4", "CON-5"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::ConcurrentWriters,
            ConcurrencyMode::MvccIsolation,
        ])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/ssi_write_skew.rs",
            ScriptKind::RustE2eTest,
            "SSI write-skew detection and prevention",
        )
        .bead("bd-mblr.4.2.2")
        .command("cargo test -p fsqlite-e2e --test ssi_write_skew")
        .scenarios(&["SSI-1", "SSI-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Ssi, ConcurrencyMode::ConcurrentWriters])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/recovery_crash_wal_replay.rs",
            ScriptKind::RustE2eTest,
            "WAL replay after simulated crash",
        )
        .command("cargo test -p fsqlite-e2e --test recovery_crash_wal_replay")
        .scenarios(&["REC-1", "WAL-1"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/recovery_single_page.rs",
            ScriptKind::RustE2eTest,
            "Single-page recovery correctness",
        )
        .command("cargo test -p fsqlite-e2e --test recovery_single_page")
        .scenarios(&["REC-2"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/recovery_wal_corruption.rs",
            ScriptKind::RustE2eTest,
            "WAL corruption recovery",
        )
        .command("cargo test -p fsqlite-e2e --test recovery_wal_corruption")
        .scenarios(&["REC-3", "CORRUPT-1"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/golden_integrity.rs",
            ScriptKind::RustE2eTest,
            "Golden dataset integrity and hash validation",
        )
        .command("cargo test -p fsqlite-e2e --test golden_integrity")
        .scenarios(&["GOLD-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/e2e_storage_stack.rs",
            ScriptKind::RustE2eTest,
            "Full storage stack integration",
        )
        .command("cargo test -p fsqlite-e2e --test e2e_storage_stack")
        .scenarios(&["STOR-1", "STOR-2"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/compat_file_format.rs",
            ScriptKind::RustE2eTest,
            "SQLite file format compatibility",
        )
        .command("cargo test -p fsqlite-e2e --test compat_file_format")
        .scenarios(&["COMPAT-3"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/seed_reproducibility.rs",
            ScriptKind::RustE2eTest,
            "Seed-based deterministic test reproducibility",
        )
        .command("cargo test -p fsqlite-e2e --test seed_reproducibility")
        .scenarios(&["SEED-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
        ])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/manifest_v1.rs",
            ScriptKind::RustE2eTest,
            "Manifest format v1 validation",
        )
        .command("cargo test -p fsqlite-e2e --test manifest_v1")
        .scenarios(&["INFRA-4"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(60)
        .build(),
    );

    add_additional_rust_e2e_tests(&mut scripts);

    // ── Key Rust harness tests (crates/fsqlite-harness/tests/) ──────
    add_harness_tests(&mut scripts);

    // ── WAL integration tests ───────────────────────────────────────
    add_wal_tests(&mut scripts);
    add_uncataloged_rust_e2e_tests(&mut scripts);

    // ── Gap annotations ─────────────────────────────────────────────
    let gaps = build_gap_annotations();

    TraceabilityMatrix {
        schema_version: "1.0.0".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        scripts,
        gaps,
    }
}

fn add_uncataloged_rust_e2e_tests(scripts: &mut Vec<ScriptEntry>) {
    let mut known_paths: BTreeSet<String> =
        scripts.iter().map(|script| script.path.clone()).collect();
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = manifest_dir.join("../fsqlite-e2e/tests");
    let Ok(entries) = std::fs::read_dir(&tests_dir) else {
        return;
    };

    let mut discovered = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() || entry.path().extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let Some(file_name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        discovered.push(format!("crates/fsqlite-e2e/tests/{file_name}"));
    }
    discovered.sort();

    for (index, path) in discovered.into_iter().enumerate() {
        if !known_paths.insert(path.clone()) {
            continue;
        }
        let stem = Path::new(&path)
            .file_stem()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("unknown_e2e_test")
            .to_owned();
        scripts.push(ScriptEntry {
            path,
            kind: ScriptKind::RustE2eTest,
            bead_id: None,
            description: format!("Auto-cataloged Rust e2e test {stem}"),
            invocation: InvocationContract {
                command: format!("cargo test -p fsqlite-e2e --test {stem}"),
                env_vars: Vec::new(),
                json_output: false,
                timeout_secs: Some(300),
            },
            scenario_ids: vec![format!("AUTO-E2E-{index:03}")],
            storage_modes: vec![StorageMode::InMemory, StorageMode::FileBacked],
            concurrency_modes: vec![ConcurrencyMode::Sequential],
            artifact_paths: Vec::new(),
            log_schema_version: None,
        });
    }
}

#[allow(clippy::too_many_lines)]
fn add_additional_rust_e2e_tests(scripts: &mut Vec<ScriptEntry>) {
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_1lsfu_1_generate_baseline.rs",
            ScriptKind::RustE2eTest,
            "Operation baseline generation artifact",
        )
        .bead("bd-1lsfu.1")
        .command("cargo test -p fsqlite-e2e --test bd_1lsfu_1_generate_baseline")
        .scenarios(&["BASELINE-GENERATE-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_1lsfu_1_operation_baselines.rs",
            ScriptKind::RustE2eTest,
            "Operation baseline capture and regression detection",
        )
        .bead("bd-1lsfu.1")
        .command("cargo test -p fsqlite-e2e --test bd_1lsfu_1_operation_baselines")
        .scenarios(&["BASELINE-OPS-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_1r0ha_3_concurrent_writer_e2e.rs",
            ScriptKind::RustE2eTest,
            "Deterministic concurrent-writer fairness and no-serialization matrix",
        )
        .bead("bd-1r0ha.3")
        .command("cargo test -p fsqlite-e2e --test bd_1r0ha_3_concurrent_writer_e2e")
        .scenarios(&[
            "MVCC-E2E-RW10W10-DISJOINT",
            "MVCC-E2E-HOT-WRITE-CONTENTION",
            "MVCC-E2E-NO-GLOBAL-SERIALIZATION",
        ])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::ConcurrentWriters,
            ConcurrencyMode::MvccIsolation,
        ])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_1rw_3_jit_hot_query_replay.rs",
            ScriptKind::RustE2eTest,
            "JIT hot-query replay, fallback, and artifact emission",
        )
        .bead("bd-1rw.3")
        .command("cargo test -p fsqlite-e2e --test bd_1rw_3_jit_hot_query_replay")
        .env("FSQLITE_JIT_E2E_ARTIFACT")
        .scenarios(&[
            "JIT-HOT-QUERY",
            "JIT-FALLBACK-UNSUPPORTED",
            "JIT-HOT-QUERY-E2E-REPLAY",
        ])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_2y306_2_chain_length_controls.rs",
            ScriptKind::RustE2eTest,
            "Chain-length bounds, backpressure, and replay artifact coverage",
        )
        .bead("bd-2y306.2")
        .command("cargo test -p fsqlite-e2e --test bd_2y306_2_chain_length_controls")
        .env("FSQLITE_CHAIN_LENGTH_E2E_ARTIFACT")
        .scenarios(&[
            "CHAIN-LENGTH-HOT-PAGE-BOUND",
            "CHAIN-LENGTH-BACKPRESSURE",
            "CHAIN-LENGTH-CONTROLS-E2E-REPLAY",
        ])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential, ConcurrencyMode::MvccIsolation])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_2y306_4_chain_memory_benchmarks.rs",
            ScriptKind::RustE2eTest,
            "Chain-memory plateau and long-reader recovery benchmarks",
        )
        .bead("bd-2y306.4")
        .command("cargo test -p fsqlite-e2e --test bd_2y306_4_chain_memory_benchmarks")
        .env("FSQLITE_CHAIN_MEMORY_BENCH_ARTIFACT")
        .scenarios(&[
            "CHAIN-MEMORY-BENCH-WORKLOADS",
            "CHAIN-MEMORY-BENCH-LONG-READER",
            "CHAIN-MEMORY-BENCH-E2E",
        ])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential, ConcurrencyMode::MvccIsolation])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_2yqp6_6_1_ssi_serialization_anomaly_differential.rs",
            ScriptKind::RustE2eTest,
            "Deterministic SSI serialization anomaly differential suite",
        )
        .bead("bd-2yqp6.6.1")
        .command(
            "cargo test -p fsqlite-e2e --test bd_2yqp6_6_1_ssi_serialization_anomaly_differential",
        )
        .env("FSQLITE_SSI_ANOMALY_ARTIFACT")
        .scenarios(&[
            "SSI-WRITE-SKEW-GUARD",
            "SSI-DISJOINT-WRITES",
            "SSI-HOT-ROW-CONFLICT",
        ])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Ssi, ConcurrencyMode::ConcurrentWriters])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_2yqp6_6_2_busy_retry_error_semantics_matrix.rs",
            ScriptKind::RustE2eTest,
            "BUSY and BUSY_SNAPSHOT parity matrix with default-mode guard",
        )
        .bead("bd-2yqp6.6.2")
        .command("cargo test -p fsqlite-e2e --test bd_2yqp6_6_2_busy_retry_error_semantics_matrix")
        .env("FSQLITE_BUSY_MATRIX_ARTIFACT")
        .scenarios(&[
            "BUSY-SNAPSHOT-CONFLICT",
            "BUSY-LOCK-IMMEDIATE",
            "CONCURRENT-MODE-DEFAULT-GUARD",
        ])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
            ConcurrencyMode::MvccIsolation,
        ])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_2yqp6_6_3_crash_recovery_corruption_matrix.rs",
            ScriptKind::RustE2eTest,
            "Crash-recovery and corruption differential matrix",
        )
        .bead("bd-2yqp6.6.3")
        .command("cargo test -p fsqlite-e2e --test bd_2yqp6_6_3_crash_recovery_corruption_matrix")
        .env("FSQLITE_CRASH_MATRIX_ARTIFACT")
        .scenarios(&["CRASH-MATRIX-DB", "CRASH-MATRIX-WAL"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_2yqp6_6_5_concurrent_mode_defaults.rs",
            ScriptKind::RustE2eTest,
            "Concurrent-mode default anti-regression checks",
        )
        .bead("bd-2yqp6.6.5")
        .command("cargo test -p fsqlite-e2e --test bd_2yqp6_6_5_concurrent_mode_defaults")
        .scenarios(&["CONCURRENT-DEFAULTS-F5"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
        ])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_3plop_5_ssi_serialization_correctness.rs",
            ScriptKind::RustE2eTest,
            "SSI serialization correctness under concurrent writers",
        )
        .bead("bd-3plop.5")
        .command("cargo test -p fsqlite-e2e --test bd_3plop_5_ssi_serialization_correctness")
        .scenarios(&[
            "SSI-CI-SCALE",
            "SSI-SINGLE-WRITER-SMOKE",
            "SSI-STRESS-PROFILE",
        ])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Ssi, ConcurrencyMode::ConcurrentWriters])
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_3plop_6_raptorq_self_healing.rs",
            ScriptKind::RustE2eTest,
            "RaptorQ self-healing corruption and repair showcase",
        )
        .bead("bd-3plop.6")
        .command("cargo test -p fsqlite-e2e --test bd_3plop_6_raptorq_self_healing")
        .scenarios(&[
            "RAPTORQ-REPAIR-5PCT",
            "RAPTORQ-REPAIR-10PCT",
            "RAPTORQ-GRACEFUL-DEGRADATION",
            "RAPTORQ-WITNESS-VARIANTS",
        ])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_3wop3_7_contention_elimination.rs",
            ScriptKind::RustE2eTest,
            "Historical contention-elimination and split-lock scaffolding",
        )
        .bead("bd-3wop3.7")
        .command("cargo test -p fsqlite-e2e --test bd_3wop3_7_contention_elimination")
        .scenarios(&["CONTENTION-ELIMINATION-HISTORICAL"])
        .storage(&[
            StorageMode::InMemory,
            StorageMode::FileBacked,
            StorageMode::Wal,
        ])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
        ])
        .timeout(900)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_aztlm_flat_hash_page_cache.rs",
            ScriptKind::RustE2eTest,
            "Flat-hash page-cache oracle and concurrent-writer evidence",
        )
        .bead("bd-aztlm")
        .command("cargo test -p fsqlite-e2e --test bd_aztlm_flat_hash_page_cache")
        .scenarios(&[
            "TRACK-Q-INSERT-10K-CORRECTNESS",
            "TRACK-Q-FOUR-WRITER-NO-DATA-LOSS",
        ])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
        ])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_db300_5_5_3_writer_routing_protocol.rs",
            ScriptKind::RustE2eTest,
            "Synthetic writer-routing protocol evidence and conflict reduction",
        )
        .bead("bd-db300.5.5.3")
        .command("cargo test -p fsqlite-e2e --test bd_db300_5_5_3_writer_routing_protocol")
        .scenarios(&[
            "WRITER-ROUTING-DISJOINT",
            "WRITER-ROUTING-OVERLAP",
            "WRITER-ROUTING-HOT-PAGE",
        ])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_ezg4p_prefetch.rs",
            ScriptKind::RustE2eTest,
            "Prefetch correctness and lookup-latency improvement proofs",
        )
        .bead("bd-ezg4p")
        .command("cargo test -p fsqlite-e2e --test bd_ezg4p_prefetch")
        .scenarios(&["TRACK-P-INSERT-10K-CORRECTNESS", "TRACK-P-LATENCY-LOOKUP"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_mblr_2_3_fault_injection_reliability.rs",
            ScriptKind::RustE2eTest,
            "Fault-injection reliability checks for disk, read, and sync failures",
        )
        .bead("bd-mblr.2.3")
        .command("cargo test -p fsqlite-e2e --test bd_mblr_2_3_fault_injection_reliability")
        .scenarios(&[
            "FAULT-INJECTION-DISK-FULL",
            "FAULT-INJECTION-IO-ERROR",
            "FAULT-INJECTION-PARTIAL-READ",
            "FAULT-INJECTION-FSYNC",
        ])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_n0g4q_6_repair_matrix.rs",
            ScriptKind::RustE2eTest,
            "Deterministic corruption and repair matrix with evidence validation",
        )
        .bead("bd-n0g4q.6")
        .command("cargo test -p fsqlite-e2e --test bd_n0g4q_6_repair_matrix")
        .scenarios(&["REPAIR-MATRIX-1"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_qayid_track_t_append_path.rs",
            ScriptKind::RustE2eTest,
            "Append-path oracle, sequential-vs-gapped metrics, and throughput probe",
        )
        .bead("bd-qayid")
        .command("cargo test -p fsqlite-e2e --test bd_qayid_track_t_append_path")
        .env("FSQLITE_TRACK_T_E2E_ARTIFACT")
        .scenarios(&[
            "TRACK-T-ORACLE-10K-SEQ",
            "TRACK-T-SEQ-VS-INTERLEAVED",
            "TRACK-T-APPEND-THROUGHPUT",
        ])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
        ])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_rjc_deterministic_hot_row_sum_probe.rs",
            ScriptKind::RustE2eTest,
            "Deterministic hot-row conservation and integrity probe",
        )
        .bead("bd-rjc")
        .command("cargo test -p fsqlite-e2e --test bd_rjc_deterministic_hot_row_sum_probe")
        .scenarios(&["HOT-ROW-SUM-PROBE"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/bd_zjisk_1_backend_wiring_modes.rs",
            ScriptKind::RustE2eTest,
            "Backend wiring mode checks across runtime and certifying modes",
        )
        .bead("bd-zjisk.1")
        .command("cargo test -p fsqlite-e2e --test bd_zjisk_1_backend_wiring_modes")
        .scenarios(&["BACKEND-WIRING-MODES"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/complete_benchmark_matrix.rs",
            ScriptKind::RustE2eTest,
            "Complete benchmark matrix runner and artifact bundle validation",
        )
        .command("cargo test -p fsqlite-e2e --test complete_benchmark_matrix")
        .env("FSQLITE_MATRIX_ARTIFACT_DIR")
        .env("FSQLITE_MATRIX_BASELINE_JSONL")
        .scenarios(&["BENCH-MATRIX-1", "HOT-PROFILE-DIAGNOSIS"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::Sequential,
            ConcurrencyMode::ConcurrentWriters,
            ConcurrencyMode::MvccIsolation,
        ])
        .timeout(1800)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/cross_conn_upsert_visibility.rs",
            ScriptKind::RustE2eTest,
            "Cross-connection UPSERT visibility and conflict refresh checks",
        )
        .command("cargo test -p fsqlite-e2e --test cross_conn_upsert_visibility")
        .scenarios(&["UPSERT-CROSS-CONNECTION-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/extension_json_fts_parity.rs",
            ScriptKind::RustE2eTest,
            "JSON1 and FTS scalar extension parity checks",
        )
        .bead("bd-1dp9.5.2")
        .command("cargo test -p fsqlite-e2e --test extension_json_fts_parity")
        .scenarios(&["EXT-JSON1-SCALAR", "EXT-FTS5-SCALAR"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/extension_rtree_session_icu_misc_parity.rs",
            ScriptKind::RustE2eTest,
            "RTREE, session, ICU, and misc extension parity closure wave",
        )
        .bead("bd-1dp9.5.3")
        .command("cargo test -p fsqlite-e2e --test extension_rtree_session_icu_misc_parity")
        .scenarios(&["EXT-RTREE-1", "EXT-SESSION-1", "EXT-SESSION-CONFLICT-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["target/bd_1dp9_5_3_e2e_runtime/"])
        .timeout(600)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/interop_integrity.rs",
            ScriptKind::RustE2eTest,
            "Interop integrity checks against stock SQLite integrity_check",
        )
        .command("cargo test -p fsqlite-e2e --test interop_integrity")
        .scenarios(&[
            "INTEROP-INTEGRITY-1",
            "INTEROP-INTEGRITY-ISSUE54",
            "INTEROP-INTEGRITY-ISSUE55",
        ])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/strict_table_type_enforcement.rs",
            ScriptKind::RustE2eTest,
            "STRICT table type-enforcement surface checks",
        )
        .bead("bd-xr8t1")
        .command("cargo test -p fsqlite-e2e --test strict_table_type_enforcement")
        .scenarios(&["STRICT-TABLE-TYPE-ENFORCEMENT"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-e2e/tests/upsert_on_conflict.rs",
            ScriptKind::RustE2eTest,
            "UPSERT and INSERT ... ON CONFLICT end-to-end coverage",
        )
        .bead("bd-nyami")
        .command("cargo test -p fsqlite-e2e --test upsert_on_conflict")
        .scenarios(&["UPSERT-ON-CONFLICT-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );
}

#[allow(clippy::too_many_lines)]
fn add_harness_tests(scripts: &mut Vec<ScriptEntry>) {
    // MVCC and concurrency compliance tests
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_1xds_deterministic_concurrency.rs",
            ScriptKind::RustHarnessTest,
            "Deterministic concurrency with seed reproducibility",
        )
        .bead("bd-1xds")
        .command("cargo test -p fsqlite-harness --test bd_1xds_deterministic_concurrency")
        .scenarios(&["CON-6", "SEED-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2npr_mvcc_concurrent_writer_stress.rs",
            ScriptKind::RustHarnessTest,
            "MVCC concurrent writer stress",
        )
        .bead("bd-2npr")
        .command("cargo test -p fsqlite-harness --test bd_2npr_mvcc_concurrent_writer_stress")
        .scenarios(&["MVCC-5", "CON-7"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[
            ConcurrencyMode::ConcurrentWriters,
            ConcurrencyMode::MvccIsolation,
        ])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_bca_2_phase6_mvcc_ssi_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Phase 6 MVCC+SSI compliance",
        )
        .bead("bd-bca.2")
        .command("cargo test -p fsqlite-harness --test bd_bca_2_phase6_mvcc_ssi_compliance")
        .scenarios(&["SSI-3", "MVCC-6"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Ssi, ConcurrencyMode::MvccIsolation])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2d3i_1_ssi_witness_plane_deterministic_scenarios_compliance.rs",
            ScriptKind::RustHarnessTest,
            "SSI witness plane deterministic scenarios",
        )
        .bead("bd-2d3i.1")
        .command("cargo test -p fsqlite-harness --test bd_2d3i_1_ssi_witness_plane_deterministic_scenarios_compliance")
        .scenarios(&["SSI-4", "SSI-5"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Ssi])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2d3i_2_no_false_negatives_compliance.rs",
            ScriptKind::RustHarnessTest,
            "SSI no-false-negatives completeness check",
        )
        .bead("bd-2d3i.2")
        .command("cargo test -p fsqlite-harness --test bd_2d3i_2_no_false_negatives_compliance")
        .scenarios(&["SSI-6"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Ssi])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2d3i_3_tiered_storage_remote_idempotency_saga_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Tiered storage remote idempotency saga",
        )
        .bead("bd-2d3i.3")
        .command("cargo test -p fsqlite-harness --test bd_2d3i_3_tiered_storage_remote_idempotency_saga_compliance")
        .scenarios(&["STOR-3", "REP-2"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(120)
        .build(),
    );

    // WAL and recovery harness tests
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_3a7d_crash_recovery_wal_integrity.rs",
            ScriptKind::RustHarnessTest,
            "WAL crash recovery with corruption injection",
        )
        .bead("bd-3a7d")
        .command("cargo test -p fsqlite-harness --test bd_3a7d_crash_recovery_wal_integrity")
        .scenarios(&["REC-4", "WAL-2", "CORRUPT-2"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2fas_wal_checksum_chain_recovery_compliance.rs",
            ScriptKind::RustHarnessTest,
            "WAL checksum chain validation and recovery",
        )
        .bead("bd-2fas")
        .command(
            "cargo test -p fsqlite-harness --test bd_2fas_wal_checksum_chain_recovery_compliance",
        )
        .scenarios(&["WAL-3", "CHECKSUM-1"])
        .storage(&[StorageMode::FileBacked, StorageMode::Wal])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2ha1_wal_fec_group_meta_compliance.rs",
            ScriptKind::RustHarnessTest,
            "WAL FEC group metadata",
        )
        .bead("bd-2ha1")
        .command("cargo test -p fsqlite-harness --test bd_2ha1_wal_fec_group_meta_compliance")
        .scenarios(&["FEC-1"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_1gyi_wal_fec_repair_symbols_compliance.rs",
            ScriptKind::RustHarnessTest,
            "WAL FEC repair symbol correctness",
        )
        .bead("bd-1gyi")
        .command("cargo test -p fsqlite-harness --test bd_1gyi_wal_fec_repair_symbols_compliance")
        .scenarios(&["FEC-2"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    // SQL compliance tests
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_316x_fts5_extension_compliance.rs",
            ScriptKind::RustHarnessTest,
            "FTS5 extension compliance",
        )
        .bead("bd-316x")
        .command("cargo test -p fsqlite-harness --test bd_316x_fts5_extension_compliance")
        .scenarios(&["EXT-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2xl9_fts3_fts4_compliance.rs",
            ScriptKind::RustHarnessTest,
            "FTS3/FTS4 backward compatibility",
        )
        .bead("bd-2xl9")
        .command("cargo test -p fsqlite-harness --test bd_2xl9_fts3_fts4_compliance")
        .scenarios(&["EXT-3"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_3cvl_json1_extension_compliance.rs",
            ScriptKind::RustHarnessTest,
            "JSON1 extension functions",
        )
        .bead("bd-3cvl")
        .command("cargo test -p fsqlite-harness --test bd_3cvl_json1_extension_compliance")
        .scenarios(&["EXT-4"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_3kin_ddl_compliance.rs",
            ScriptKind::RustHarnessTest,
            "DDL statement compliance",
        )
        .bead("bd-3kin")
        .command("cargo test -p fsqlite-harness --test bd_3kin_ddl_compliance")
        .scenarios(&["SQL-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2d6i_select_compliance.rs",
            ScriptKind::RustHarnessTest,
            "SELECT statement compliance",
        )
        .bead("bd-2d6i")
        .command("cargo test -p fsqlite-harness --test bd_2d6i_select_compliance")
        .scenarios(&["SQL-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_340i_full_sql_roundtrip_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Full SQL parsing and execution roundtrip",
        )
        .bead("bd-340i")
        .command("cargo test -p fsqlite-harness --test bd_340i_full_sql_roundtrip_compliance")
        .scenarios(&["SQL-3"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_7pxb_transaction_control_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Transaction control (BEGIN/COMMIT/ROLLBACK)",
        )
        .bead("bd-7pxb")
        .command("cargo test -p fsqlite-harness --test bd_7pxb_transaction_control_compliance")
        .scenarios(&["TXN-3"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_1mrj_vacuum_pragma_compliance.rs",
            ScriptKind::RustHarnessTest,
            "VACUUM and PRAGMA compliance",
        )
        .bead("bd-1mrj")
        .command("cargo test -p fsqlite-harness --test bd_1mrj_vacuum_pragma_compliance")
        .scenarios(&["SQL-4", "PRAGMA-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_202x_query_pipeline_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Query planning and execution pipeline",
        )
        .bead("bd-202x")
        .command("cargo test -p fsqlite-harness --test bd_202x_query_pipeline_compliance")
        .scenarios(&["SQL-5"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_3lhq_datetime_functions_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Date/time function correctness",
        )
        .bead("bd-3lhq")
        .command("cargo test -p fsqlite-harness --test bd_3lhq_datetime_functions_compliance")
        .scenarios(&["FUN-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_22l4_behavioral_quirks_compliance.rs",
            ScriptKind::RustHarnessTest,
            "SQLite behavioral quirks compatibility",
        )
        .bead("bd-22l4")
        .command("cargo test -p fsqlite-harness --test bd_22l4_behavioral_quirks_compliance")
        .scenarios(&["COMPAT-4"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_1uzb_file_format_compatibility_compliance.rs",
            ScriptKind::RustHarnessTest,
            "File format version compatibility",
        )
        .bead("bd-1uzb")
        .command(
            "cargo test -p fsqlite-harness --test bd_1uzb_file_format_compatibility_compliance",
        )
        .scenarios(&["COMPAT-5"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_cfj0_time_travel_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Time-travel query support",
        )
        .bead("bd-cfj0")
        .command("cargo test -p fsqlite-harness --test bd_cfj0_time_travel_compliance")
        .scenarios(&["MVCC-7"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::MvccIsolation])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_1sf8n_phase9_time_travel_gate.rs",
            ScriptKind::RustHarnessTest,
            "Phase 9 time-travel verification gate",
        )
        .bead("bd-1sf8n")
        .command(
            "cargo test -p fsqlite-harness --test bd_1sf8n_phase9_time_travel_gate -- --nocapture --test-threads=1",
        )
        .scenarios(&["MVCC-7"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::MvccIsolation])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_d2m7_begin_concurrent_cross_db_2pc_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Cross-database 2-phase commit",
        )
        .bead("bd-d2m7")
        .command(
            "cargo test -p fsqlite-harness --test bd_d2m7_begin_concurrent_cross_db_2pc_compliance",
        )
        .scenarios(&["TXN-4", "CON-8"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_4eue_reference_index_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Reference index correctness",
        )
        .bead("bd-4eue")
        .command("cargo test -p fsqlite-harness --test bd_4eue_reference_index_compliance")
        .scenarios(&["IDX-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2832_sql_pattern_coverage.rs",
            ScriptKind::RustHarnessTest,
            "SQL pattern coverage (37 test patterns)",
        )
        .bead("bd-2832")
        .command("cargo test -p fsqlite-harness --test bd_2832_sql_pattern_coverage")
        .scenarios(&["SQL-6", "SQL-7"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    // Infrastructure tests
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_2ddl_test_matrix_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Test matrix compliance validation",
        )
        .bead("bd-2ddl")
        .command("cargo test -p fsqlite-harness --test bd_2ddl_test_matrix_compliance")
        .scenarios(&["COMPL-3"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_bca_1_phase5_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Phase 5 compliance gates",
        )
        .bead("bd-bca.1")
        .command("cargo test -p fsqlite-harness --test bd_bca_1_phase5_compliance")
        .scenarios(&["GATE-1"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_331_1_universal_gates_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Universal verification gates",
        )
        .bead("bd-331.1")
        .command("cargo test -p fsqlite-harness --test bd_331_1_universal_gates_compliance")
        .scenarios(&["GATE-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_331_2_phase_foundation_gates_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Phase foundation verification gates",
        )
        .bead("bd-331.2")
        .command("cargo test -p fsqlite-harness --test bd_331_2_phase_foundation_gates_compliance")
        .scenarios(&["GATE-3"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_331_3_phase_core_engine_gates_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Core engine verification gates",
        )
        .bead("bd-331.3")
        .command("cargo test -p fsqlite-harness --test bd_331_3_phase_core_engine_gates_compliance")
        .scenarios(&["GATE-4"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_331_4_phase_7_8_9_verification_gates_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Phase 7-8-9 verification gates",
        )
        .bead("bd-331.4")
        .command("cargo test -p fsqlite-harness --test bd_331_4_phase_7_8_9_verification_gates_compliance")
        .scenarios(&["GATE-5"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    // Networking and replication
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_i0m5_networking_stack_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Networking stack compliance",
        )
        .bead("bd-i0m5")
        .command("cargo test -p fsqlite-harness --test bd_i0m5_networking_stack_compliance")
        .scenarios(&["NET-2"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::ConcurrentWriters])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_m0l2_raptorq_e2e_integration.rs",
            ScriptKind::RustHarnessTest,
            "RaptorQ FEC end-to-end integration",
        )
        .bead("bd-m0l2")
        .command("cargo test -p fsqlite-harness --test bd_m0l2_raptorq_e2e_integration")
        .scenarios(&["FEC-3"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    // Performance and observability
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_yvhd_ssi_perf_validation_compliance.rs",
            ScriptKind::RustHarnessTest,
            "SSI performance validation",
        )
        .bead("bd-yvhd")
        .command("cargo test -p fsqlite-harness --test bd_yvhd_ssi_perf_validation_compliance")
        .scenarios(&["PERF-2"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Ssi])
        .timeout(300)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_25q8_btree_hotspot_compliance.rs",
            ScriptKind::RustHarnessTest,
            "B-tree hotspot detection and optimization",
        )
        .bead("bd-25q8")
        .command("cargo test -p fsqlite-harness --test bd_25q8_btree_hotspot_compliance")
        .scenarios(&["PERF-3"])
        .storage(&[StorageMode::InMemory, StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_3q1g_observability_evidence_ledger_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Observability evidence ledger",
        )
        .bead("bd-3q1g")
        .command(
            "cargo test -p fsqlite-harness --test bd_3q1g_observability_evidence_ledger_compliance",
        )
        .scenarios(&["OBS-1"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/bd_3go_11_observability_policy_controller_compliance.rs",
            ScriptKind::RustHarnessTest,
            "Observability policy controller",
        )
        .bead("bd-3go.11")
        .command("cargo test -p fsqlite-harness --test bd_3go_11_observability_policy_controller_compliance")
        .scenarios(&["OBS-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    // Encoding / RaptorQ verification
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/rfc6330_conformance_verification.rs",
            ScriptKind::RustHarnessTest,
            "RFC 6330 (RaptorQ) conformance",
        )
        .command("cargo test -p fsqlite-harness --test rfc6330_conformance_verification")
        .scenarios(&["FEC-4"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/encoding_pipeline_verification.rs",
            ScriptKind::RustHarnessTest,
            "Encoding pipeline correctness",
        )
        .command("cargo test -p fsqlite-harness --test encoding_pipeline_verification")
        .scenarios(&["FEC-5"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/decoding_pipeline_verification.rs",
            ScriptKind::RustHarnessTest,
            "Decoding pipeline correctness",
        )
        .command("cargo test -p fsqlite-harness --test decoding_pipeline_verification")
        .scenarios(&["FEC-6"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    // Workspace and infrastructure
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/workspace_layering.rs",
            ScriptKind::RustHarnessTest,
            "Crate dependency layering validation",
        )
        .command("cargo test -p fsqlite-harness --test workspace_layering")
        .scenarios(&["INFRA-5"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(60)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/logging_standard.rs",
            ScriptKind::RustHarnessTest,
            "Structured logging format standard",
        )
        .command("cargo test -p fsqlite-harness --test logging_standard")
        .scenarios(&["INFRA-6"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(60)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/no_tokio_enforcement.rs",
            ScriptKind::RustHarnessTest,
            "No-tokio dependency enforcement",
        )
        .command("cargo test -p fsqlite-harness --test no_tokio_enforcement")
        .scenarios(&["INFRA-7"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(60)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-harness/tests/parity_taxonomy_test.rs",
            ScriptKind::RustHarnessTest,
            "Parity taxonomy structural validation",
        )
        .bead("bd-1dp9.1.1")
        .command("cargo test -p fsqlite-harness --test parity_taxonomy_test")
        .scenarios(&["PARITY-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );
}

fn add_wal_tests(scripts: &mut Vec<ScriptEntry>) {
    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-wal/tests/checksum_algorithms.rs",
            ScriptKind::RustWalTest,
            "WAL checksum algorithm verification",
        )
        .command("cargo test -p fsqlite-wal --test checksum_algorithms")
        .scenarios(&["CHECKSUM-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(60)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-wal/tests/wal_fec_pipeline.rs",
            ScriptKind::RustWalTest,
            "WAL FEC pipeline integration",
        )
        .command("cargo test -p fsqlite-wal --test wal_fec_pipeline")
        .scenarios(&["FEC-7"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-wal/tests/wal_fec_sidecar.rs",
            ScriptKind::RustWalTest,
            "FEC sidecar file handling",
        )
        .command("cargo test -p fsqlite-wal --test wal_fec_sidecar")
        .scenarios(&["FEC-8"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );

    scripts.push(
        ScriptEntryBuilder::new(
            "crates/fsqlite-wal/tests/wal_fec_recovery.rs",
            ScriptKind::RustWalTest,
            "FEC recovery operations",
        )
        .command("cargo test -p fsqlite-wal --test wal_fec_recovery")
        .scenarios(&["FEC-9", "REC-5"])
        .storage(&[StorageMode::FileBacked])
        .concurrency(&[ConcurrencyMode::Sequential])
        .timeout(120)
        .build(),
    );
}

fn build_gap_annotations() -> Vec<GapAnnotation> {
    vec![
        GapAnnotation {
            area: "TRIGGER-*".to_owned(),
            rationale: "Trigger execution engine not yet implemented; trigger tests \
                        deferred until parser-to-VDBE path complete"
                .to_owned(),
            intentional: true,
        },
        GapAnnotation {
            area: "VTAB-*".to_owned(),
            rationale: "Virtual table interface is stub-only; no E2E coverage possible \
                        until xConnect/xBestIndex implemented"
                .to_owned(),
            intentional: true,
        },
        GapAnnotation {
            area: "ICU-*".to_owned(),
            rationale: "ICU extension collation tests require system-level ICU library; \
                        CI environment may lack ICU4C, so coverage is conditional"
                .to_owned(),
            intentional: true,
        },
        GapAnnotation {
            area: "AUTH-*".to_owned(),
            rationale: "sqlite3_set_authorizer callback not yet implemented in \
                        FrankenSQLite connection API"
                .to_owned(),
            intentional: true,
        },
        GapAnnotation {
            area: "PERF-regression-suite".to_owned(),
            rationale: "The honest overlay gate and no-regression detector now cover \
                        the canonical c1 and persistent 8t/16t truth surfaces. The \
                        remaining gap is a fully automated provenance-backed release \
                        perf suite beyond the current canonical matrix and scorecard \
                        harnesses."
                .to_owned(),
            intentional: false,
        },
        GapAnnotation {
            area: "BACKUP-*".to_owned(),
            rationale: "Online backup API (sqlite3_backup_*) not yet implemented".to_owned(),
            intentional: true,
        },
        GapAnnotation {
            area: "UPSERT-*".to_owned(),
            rationale: "INSERT ... ON CONFLICT (UPSERT) parsing is implemented but \
                        E2E differential coverage is pending corpus expansion (bd-1dp9.2.1)"
                .to_owned(),
            intentional: false,
        },
        GapAnnotation {
            area: "WINDOW-*".to_owned(),
            rationale: "Window function execution deferred; parser coverage exists \
                        but VDBE codegen for window frames is incomplete"
                .to_owned(),
            intentional: true,
        },
        GapAnnotation {
            area: "CTE-recursive".to_owned(),
            rationale: "Recursive CTE execution is partial; non-recursive CTEs are \
                        tested via SQL roundtrip but recursive variant lacks dedicated E2E"
                .to_owned(),
            intentional: false,
        },
    ]
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::Path};

    fn workspace_root() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root should resolve")
    }

    fn directory_file_set(relative_dir: &str) -> BTreeSet<String> {
        let dir = workspace_root().join(relative_dir);
        fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read_dir failed for {}: {e}", dir.display()))
            .filter_map(|entry| {
                let entry =
                    entry.unwrap_or_else(|e| panic!("dir entry failed for {}: {e}", dir.display()));
                let file_type = entry.file_type().unwrap_or_else(|e| {
                    panic!("file_type failed for {}: {e}", entry.path().display())
                });
                if file_type.is_dir() {
                    return None;
                }
                assert!(
                    file_type.is_file(),
                    "expected only files under {} but found {}",
                    dir.display(),
                    entry.path().display()
                );
                let relative = entry
                    .path()
                    .strip_prefix(workspace_root())
                    .unwrap_or_else(|e| {
                        panic!("strip_prefix failed for {}: {e}", entry.path().display())
                    })
                    .to_path_buf();
                Some(
                    relative
                        .to_str()
                        .unwrap_or_else(|| panic!("non-utf8 path in {}", dir.display()))
                        .to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn canonical_inventory_builds() {
        let matrix = build_canonical_inventory();
        assert!(!matrix.scripts.is_empty());
        assert_eq!(matrix.schema_version, "1.0.0");
        assert_eq!(matrix.bead_id, "bd-mblr.4.5.1");
    }

    #[test]
    fn canonical_inventory_validates() {
        let matrix = build_canonical_inventory();
        let errors = matrix.validate();
        assert!(
            errors.is_empty(),
            "Validation errors:\n{}",
            errors.join("\n")
        );
    }

    #[test]
    fn no_duplicate_paths() {
        let matrix = build_canonical_inventory();
        let mut paths = BTreeSet::new();
        for s in &matrix.scripts {
            assert!(paths.insert(&s.path), "Duplicate path: {}", s.path);
        }
    }

    #[test]
    fn all_scripts_have_invocation() {
        let matrix = build_canonical_inventory();
        for s in &matrix.scripts {
            assert!(
                !s.invocation.command.is_empty(),
                "Script {} has no invocation command",
                s.path
            );
        }
    }

    #[test]
    fn all_scripts_have_storage_mode() {
        let matrix = build_canonical_inventory();
        for s in &matrix.scripts {
            assert!(
                !s.storage_modes.is_empty(),
                "Script {} has no storage mode",
                s.path
            );
        }
    }

    #[test]
    fn all_scripts_have_scenarios_or_gaps() {
        let matrix = build_canonical_inventory();
        for s in &matrix.scripts {
            let has_scenarios = !s.scenario_ids.is_empty();
            let has_gap = matrix.has_gap_for_script(&s.path);
            assert!(
                has_scenarios || has_gap,
                "Script {} has no scenarios and no gap annotation",
                s.path
            );
        }
    }

    #[test]
    fn scenario_ids_follow_convention() {
        let matrix = build_canonical_inventory();
        for s in &matrix.scripts {
            for sid in &s.scenario_ids {
                assert!(
                    sid.contains('-'),
                    "Scenario ID '{}' in {} should be CATEGORY-NUMBER",
                    sid,
                    s.path
                );
            }
        }
    }

    #[test]
    fn coverage_stats_are_plausible() {
        let matrix = build_canonical_inventory();
        let stats = matrix.coverage_stats();
        assert!(
            stats.total_scripts >= 50,
            "Expected at least 50 scripts, got {}",
            stats.total_scripts
        );
        assert!(stats.scripts_with_scenarios >= 50);
        assert!(stats.unique_scenario_count >= 40);
        assert!(stats.gap_count >= 5);
    }

    #[test]
    fn all_script_kinds_represented() {
        let matrix = build_canonical_inventory();
        let stats = matrix.coverage_stats();
        assert!(stats.by_kind.contains_key(&ScriptKind::ShellE2e));
        assert!(stats.by_kind.contains_key(&ScriptKind::ShellUtility));
        assert!(stats.by_kind.contains_key(&ScriptKind::RustE2eTest));
        assert!(stats.by_kind.contains_key(&ScriptKind::RustHarnessTest));
        assert!(stats.by_kind.contains_key(&ScriptKind::RustWalTest));
        assert!(stats.by_kind.contains_key(&ScriptKind::TypeScriptSpec));
    }

    #[test]
    fn all_storage_modes_covered() {
        let matrix = build_canonical_inventory();
        let stats = matrix.coverage_stats();
        assert!(stats.by_storage.contains_key(&StorageMode::InMemory));
        assert!(stats.by_storage.contains_key(&StorageMode::FileBacked));
        assert!(stats.by_storage.contains_key(&StorageMode::Wal));
    }

    #[test]
    fn all_concurrency_modes_covered() {
        let matrix = build_canonical_inventory();
        let stats = matrix.coverage_stats();
        assert!(
            stats
                .by_concurrency
                .contains_key(&ConcurrencyMode::Sequential)
        );
        assert!(
            stats
                .by_concurrency
                .contains_key(&ConcurrencyMode::ConcurrentWriters)
        );
        assert!(
            stats
                .by_concurrency
                .contains_key(&ConcurrencyMode::MvccIsolation)
        );
        assert!(stats.by_concurrency.contains_key(&ConcurrencyMode::Ssi));
    }

    #[test]
    fn mvcc_scenarios_have_concurrent_mode() {
        let matrix = build_canonical_inventory();
        for s in &matrix.scripts {
            let has_mvcc_scenario = s.scenario_ids.iter().any(|id| id.starts_with("MVCC-"));
            if has_mvcc_scenario {
                let has_concurrent = s.concurrency_modes.iter().any(|m| {
                    matches!(
                        m,
                        ConcurrencyMode::ConcurrentWriters
                            | ConcurrencyMode::MvccIsolation
                            | ConcurrencyMode::Ssi
                    )
                });
                assert!(
                    has_concurrent,
                    "Script {} has MVCC scenario but no concurrent mode",
                    s.path
                );
            }
        }
    }

    #[test]
    fn json_roundtrip() {
        let matrix = build_canonical_inventory();
        let json = matrix.to_json().expect("serialize");
        let deserialized: TraceabilityMatrix = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.scripts.len(), matrix.scripts.len());
        assert_eq!(deserialized.gaps.len(), matrix.gaps.len());
    }

    #[test]
    fn score_determinism() {
        let m1 = build_canonical_inventory();
        let m2 = build_canonical_inventory();
        let s1 = m1.coverage_stats();
        let s2 = m2.coverage_stats();
        assert_eq!(s1.total_scripts, s2.total_scripts);
        assert_eq!(s1.unique_scenario_count, s2.unique_scenario_count);
    }

    #[test]
    fn e2e_directories_are_fully_cataloged() {
        let matrix = build_canonical_inventory();
        let matrix_paths: BTreeSet<String> =
            matrix.scripts.iter().map(|s| s.path.clone()).collect();

        let e2e_files = directory_file_set("e2e");
        let rust_e2e_files = directory_file_set("crates/fsqlite-e2e/tests");

        let missing_e2e: Vec<_> = e2e_files.difference(&matrix_paths).cloned().collect();
        let missing_rust_e2e: Vec<_> = rust_e2e_files.difference(&matrix_paths).cloned().collect();

        assert!(
            missing_e2e.is_empty(),
            "uncataloged e2e files: {missing_e2e:?}"
        );
        assert!(
            missing_rust_e2e.is_empty(),
            "uncataloged Rust e2e tests: {missing_rust_e2e:?}"
        );
    }

    #[test]
    fn gap_annotations_have_rationale() {
        let matrix = build_canonical_inventory();
        for g in &matrix.gaps {
            assert!(
                !g.rationale.is_empty(),
                "Gap {} has empty rationale",
                g.area
            );
            assert!(!g.area.is_empty(), "Gap has empty area");
        }
    }
}
