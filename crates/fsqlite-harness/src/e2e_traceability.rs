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
        .timeout(900)
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
        .timeout(600)
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
        .timeout(300)
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
        .timeout(120)
        .build(),
    );

    // ── Shell utility scripts (scripts/) ─────────────────────────────
    scripts.push(
        ScriptEntryBuilder::new(
            "scripts/test_inventory.sh",
            ScriptKind::ShellUtility,
            "Test realism inventory analyzer (CSV output)",
        )
        .command("bash scripts/test_inventory.sh")
        .scenarios(&["INFRA-2"])
        .storage(&[StorageMode::InMemory])
        .concurrency(&[ConcurrencyMode::Sequential])
        .artifacts(&["target/test-inventory/test_inventory.csv"])
        .timeout(120)
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

    // ── Key Rust harness tests (crates/fsqlite-harness/tests/) ──────
    add_harness_tests(&mut scripts);

    // ── WAL integration tests ───────────────────────────────────────
    add_wal_tests(&mut scripts);

    // ── Gap annotations ─────────────────────────────────────────────
    let gaps = build_gap_annotations();

    TraceabilityMatrix {
        schema_version: "1.0.0".to_owned(),
        bead_id: BEAD_ID.to_owned(),
        scripts,
        gaps,
    }
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
            rationale: "Full performance regression suite planned in bd-mblr.7.3; \
                        current PERF scenarios cover warmup and SSI only"
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
