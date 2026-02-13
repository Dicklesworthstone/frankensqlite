//! Code-area to invariant/scenario mapping graph (bd-mblr.7.9.1).
//!
//! Builds a bipartite mapping from source modules (code areas) to the
//! invariants, scenarios, and validation lanes that must be exercised
//! when that code changes.  This powers **risk-aware lane selection**:
//! when a PR touches crate X, the graph determines which test suites
//! and invariant checks are relevant.
//!
//! # Design
//!
//! A [`CodeArea`] represents a crate or sub-module.  Each code area is
//! connected to [`InvariantRef`]s and [`ScenarioRef`]s via typed edges.
//! A [`ValidationLane`] groups related scenarios into CI lanes with
//! priority ordering.
//!
//! The [`ImpactGraph`] is the top-level structure consumed by the lane
//! selection engine (bd-mblr.7.9.2).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.9.1";

// ---------------------------------------------------------------------------
// Code area
// ---------------------------------------------------------------------------

/// A source module or crate in the workspace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CodeArea {
    /// Crate name (e.g., "fsqlite-mvcc").
    pub crate_name: String,

    /// Sub-module path within the crate, if relevant (e.g., "version_store").
    pub module_path: Option<String>,

    /// Risk tier: 0 = critical (any change needs full suite), 1 = high, 2 = medium.
    pub risk_tier: u8,

    /// Human-readable description.
    pub description: String,
}

impl CodeArea {
    /// Canonical ID for this code area.
    #[must_use]
    pub fn id(&self) -> String {
        match &self.module_path {
            Some(m) => format!("{}::{m}", self.crate_name),
            None => self.crate_name.clone(),
        }
    }
}

impl fmt::Display for CodeArea {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (tier={})", self.id(), self.risk_tier)
    }
}

// ---------------------------------------------------------------------------
// References to invariants and scenarios
// ---------------------------------------------------------------------------

/// Reference to an invariant that a code area must satisfy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvariantRef {
    /// Invariant ID (e.g., "INV-1", "SOAK-INV-001").
    pub id: String,

    /// Human-readable name.
    pub name: String,

    /// Whether this invariant is critical (violation = abort).
    pub critical: bool,
}

/// Reference to a test scenario.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScenarioRef {
    /// Scenario ID (e.g., "MVCC-001", "REC-042").
    pub id: String,

    /// Human-readable name.
    pub name: String,

    /// Scenario category.
    pub category: ScenarioCategory,
}

/// Scenario classification (mirrors e2e_traceability).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ScenarioCategory {
    Correctness,
    Concurrency,
    Recovery,
    Performance,
    Compatibility,
}

impl fmt::Display for ScenarioCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Correctness => write!(f, "correctness"),
            Self::Concurrency => write!(f, "concurrency"),
            Self::Recovery => write!(f, "recovery"),
            Self::Performance => write!(f, "performance"),
            Self::Compatibility => write!(f, "compatibility"),
        }
    }
}

// ---------------------------------------------------------------------------
// Validation lanes
// ---------------------------------------------------------------------------

/// A CI validation lane grouping related scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ValidationLane {
    /// Unit tests for the changed crate(s).
    UnitTests,
    /// Storage-layer integration tests (pager, WAL, MVCC, B-tree).
    StorageIntegration,
    /// SQL pipeline tests (parser, planner, VDBE).
    SqlPipeline,
    /// Concurrency and MVCC stress tests.
    ConcurrencyStress,
    /// Recovery and durability tests.
    RecoveryDurability,
    /// Soak / endurance tests.
    SoakEndurance,
    /// Metamorphic differential tests.
    MetamorphicDifferential,
    /// Performance regression tests.
    PerformanceRegression,
    /// Full E2E suite (all scenarios).
    FullE2e,
}

impl ValidationLane {
    /// All lanes in priority order (most important first).
    pub const ALL: &[Self] = &[
        Self::UnitTests,
        Self::StorageIntegration,
        Self::SqlPipeline,
        Self::ConcurrencyStress,
        Self::RecoveryDurability,
        Self::SoakEndurance,
        Self::MetamorphicDifferential,
        Self::PerformanceRegression,
        Self::FullE2e,
    ];

    /// Approximate CI time budget in seconds.
    #[must_use]
    pub fn time_budget_secs(&self) -> u64 {
        match self {
            Self::UnitTests => 120,
            Self::StorageIntegration | Self::RecoveryDurability => 300,
            Self::SqlPipeline => 180,
            Self::ConcurrencyStress | Self::MetamorphicDifferential => 600,
            Self::SoakEndurance => 1800,
            Self::PerformanceRegression => 900,
            Self::FullE2e => 3600,
        }
    }
}

impl fmt::Display for ValidationLane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnitTests => write!(f, "unit-tests"),
            Self::StorageIntegration => write!(f, "storage-integration"),
            Self::SqlPipeline => write!(f, "sql-pipeline"),
            Self::ConcurrencyStress => write!(f, "concurrency-stress"),
            Self::RecoveryDurability => write!(f, "recovery-durability"),
            Self::SoakEndurance => write!(f, "soak-endurance"),
            Self::MetamorphicDifferential => write!(f, "metamorphic-differential"),
            Self::PerformanceRegression => write!(f, "perf-regression"),
            Self::FullE2e => write!(f, "full-e2e"),
        }
    }
}

// ---------------------------------------------------------------------------
// Graph edges
// ---------------------------------------------------------------------------

/// An edge from a code area to an invariant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct InvariantEdge {
    /// Source code area ID.
    pub code_area_id: String,
    /// Target invariant ID.
    pub invariant_id: String,
    /// Relationship strength: 0 = direct (code implements invariant),
    /// 1 = indirect (code affects invariant through dependency).
    pub strength: u8,
}

/// An edge from a code area to a scenario.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScenarioEdge {
    /// Source code area ID.
    pub code_area_id: String,
    /// Target scenario ID.
    pub scenario_id: String,
}

/// An edge from a code area to a validation lane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LaneEdge {
    /// Source code area ID.
    pub code_area_id: String,
    /// Required validation lane.
    pub lane: ValidationLane,
    /// Whether this lane is mandatory (must run) or advisory (should run).
    pub mandatory: bool,
}

// ---------------------------------------------------------------------------
// Impact graph
// ---------------------------------------------------------------------------

/// The full code-area to invariant/scenario mapping graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactGraph {
    /// All code areas.
    pub code_areas: Vec<CodeArea>,

    /// All invariant references.
    pub invariants: Vec<InvariantRef>,

    /// All scenario references.
    pub scenarios: Vec<ScenarioRef>,

    /// Code area -> invariant edges.
    pub invariant_edges: Vec<InvariantEdge>,

    /// Code area -> scenario edges.
    pub scenario_edges: Vec<ScenarioEdge>,

    /// Code area -> validation lane edges.
    pub lane_edges: Vec<LaneEdge>,
}

impl ImpactGraph {
    /// Build the canonical impact graph for the FrankenSQLite workspace.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn canonical() -> Self {
        let code_areas = canonical_code_areas();
        let invariants = canonical_invariant_refs();
        let scenarios = canonical_scenario_refs();
        let invariant_edges = canonical_invariant_edges();
        let scenario_edges = canonical_scenario_edges();
        let lane_edges = canonical_lane_edges();

        Self {
            code_areas,
            invariants,
            scenarios,
            invariant_edges,
            scenario_edges,
            lane_edges,
        }
    }

    /// Validate the graph for internal consistency.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        let area_ids: BTreeSet<String> = self.code_areas.iter().map(CodeArea::id).collect();
        let inv_ids: BTreeSet<&str> = self.invariants.iter().map(|i| i.id.as_str()).collect();
        let scen_ids: BTreeSet<&str> = self.scenarios.iter().map(|s| s.id.as_str()).collect();

        // Check edge references.
        for edge in &self.invariant_edges {
            if !area_ids.contains(&edge.code_area_id) {
                errors.push(format!(
                    "invariant edge references unknown code area: {}",
                    edge.code_area_id
                ));
            }
            if !inv_ids.contains(edge.invariant_id.as_str()) {
                errors.push(format!(
                    "invariant edge references unknown invariant: {}",
                    edge.invariant_id
                ));
            }
        }

        for edge in &self.scenario_edges {
            if !area_ids.contains(&edge.code_area_id) {
                errors.push(format!(
                    "scenario edge references unknown code area: {}",
                    edge.code_area_id
                ));
            }
            if !scen_ids.contains(edge.scenario_id.as_str()) {
                errors.push(format!(
                    "scenario edge references unknown scenario: {}",
                    edge.scenario_id
                ));
            }
        }

        for edge in &self.lane_edges {
            if !area_ids.contains(&edge.code_area_id) {
                errors.push(format!(
                    "lane edge references unknown code area: {}",
                    edge.code_area_id
                ));
            }
        }

        // Every code area should have at least one lane.
        for area in &self.code_areas {
            let has_lane = self.lane_edges.iter().any(|e| e.code_area_id == area.id());
            if !has_lane {
                errors.push(format!("code area {} has no validation lanes", area.id()));
            }
        }

        errors
    }

    /// Get required lanes for a set of changed code areas.
    #[must_use]
    pub fn lanes_for_changes(&self, changed_area_ids: &[&str]) -> Vec<(ValidationLane, bool)> {
        let mut lanes: BTreeMap<ValidationLane, bool> = BTreeMap::new();

        for edge in &self.lane_edges {
            if changed_area_ids.contains(&edge.code_area_id.as_str()) {
                let entry = lanes.entry(edge.lane).or_insert(false);
                if edge.mandatory {
                    *entry = true;
                }
            }
        }

        // Always include unit tests.
        lanes.entry(ValidationLane::UnitTests).or_insert(true);

        let mut result: Vec<(ValidationLane, bool)> = lanes.into_iter().collect();
        result.sort_by_key(|(lane, _)| *lane);
        result
    }

    /// Get invariants affected by changes to given code areas.
    #[must_use]
    pub fn invariants_for_changes(&self, changed_area_ids: &[&str]) -> Vec<&InvariantRef> {
        let affected_ids: BTreeSet<&str> = self
            .invariant_edges
            .iter()
            .filter(|e| changed_area_ids.contains(&e.code_area_id.as_str()))
            .map(|e| e.invariant_id.as_str())
            .collect();

        self.invariants
            .iter()
            .filter(|inv| affected_ids.contains(inv.id.as_str()))
            .collect()
    }

    /// Get scenarios affected by changes to given code areas.
    #[must_use]
    pub fn scenarios_for_changes(&self, changed_area_ids: &[&str]) -> Vec<&ScenarioRef> {
        let affected_ids: BTreeSet<&str> = self
            .scenario_edges
            .iter()
            .filter(|e| changed_area_ids.contains(&e.code_area_id.as_str()))
            .map(|e| e.scenario_id.as_str())
            .collect();

        self.scenarios
            .iter()
            .filter(|s| affected_ids.contains(s.id.as_str()))
            .collect()
    }

    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Canonical data
// ---------------------------------------------------------------------------

fn canonical_code_areas() -> Vec<CodeArea> {
    vec![
        CodeArea {
            crate_name: "fsqlite-types".into(),
            module_path: None,
            risk_tier: 1,
            description: "Core type definitions".into(),
        },
        CodeArea {
            crate_name: "fsqlite-vfs".into(),
            module_path: None,
            risk_tier: 0,
            description: "Virtual file system layer".into(),
        },
        CodeArea {
            crate_name: "fsqlite-pager".into(),
            module_path: None,
            risk_tier: 0,
            description: "Page cache and I/O".into(),
        },
        CodeArea {
            crate_name: "fsqlite-wal".into(),
            module_path: None,
            risk_tier: 0,
            description: "Write-ahead log".into(),
        },
        CodeArea {
            crate_name: "fsqlite-mvcc".into(),
            module_path: None,
            risk_tier: 0,
            description: "MVCC page-level versioning".into(),
        },
        CodeArea {
            crate_name: "fsqlite-btree".into(),
            module_path: None,
            risk_tier: 0,
            description: "B-tree storage engine".into(),
        },
        CodeArea {
            crate_name: "fsqlite-parser".into(),
            module_path: None,
            risk_tier: 1,
            description: "SQL parser".into(),
        },
        CodeArea {
            crate_name: "fsqlite-planner".into(),
            module_path: None,
            risk_tier: 1,
            description: "Query planner".into(),
        },
        CodeArea {
            crate_name: "fsqlite-vdbe".into(),
            module_path: None,
            risk_tier: 0,
            description: "Virtual database engine (bytecode)".into(),
        },
        CodeArea {
            crate_name: "fsqlite-functions".into(),
            module_path: None,
            risk_tier: 2,
            description: "Built-in SQL functions".into(),
        },
        CodeArea {
            crate_name: "fsqlite-extensions".into(),
            module_path: None,
            risk_tier: 2,
            description: "Extension modules".into(),
        },
        CodeArea {
            crate_name: "fsqlite-core".into(),
            module_path: None,
            risk_tier: 0,
            description: "Core connection and statement handling".into(),
        },
        CodeArea {
            crate_name: "fsqlite-cli".into(),
            module_path: None,
            risk_tier: 2,
            description: "Command-line interface".into(),
        },
        CodeArea {
            crate_name: "fsqlite-harness".into(),
            module_path: None,
            risk_tier: 2,
            description: "Test harness and verification".into(),
        },
    ]
}

fn canonical_invariant_refs() -> Vec<InvariantRef> {
    vec![
        InvariantRef {
            id: "INV-1".into(),
            name: "monotone_txn_id".into(),
            critical: true,
        },
        InvariantRef {
            id: "INV-2".into(),
            name: "lock_exclusivity".into(),
            critical: true,
        },
        InvariantRef {
            id: "INV-3".into(),
            name: "version_chain_order".into(),
            critical: true,
        },
        InvariantRef {
            id: "INV-4".into(),
            name: "write_set_consistency".into(),
            critical: true,
        },
        InvariantRef {
            id: "INV-5".into(),
            name: "snapshot_stability".into(),
            critical: true,
        },
        InvariantRef {
            id: "INV-6".into(),
            name: "commit_atomicity".into(),
            critical: true,
        },
        InvariantRef {
            id: "INV-7".into(),
            name: "serialized_mode_exclusivity".into(),
            critical: true,
        },
        InvariantRef {
            id: "WAL-1".into(),
            name: "wal_consistency".into(),
            critical: true,
        },
        InvariantRef {
            id: "BTREE-1".into(),
            name: "btree_balance".into(),
            critical: true,
        },
        InvariantRef {
            id: "PAGER-1".into(),
            name: "page_cache_coherence".into(),
            critical: true,
        },
    ]
}

fn canonical_scenario_refs() -> Vec<ScenarioRef> {
    vec![
        ScenarioRef {
            id: "MVCC-001".into(),
            name: "concurrent_write_read".into(),
            category: ScenarioCategory::Concurrency,
        },
        ScenarioRef {
            id: "MVCC-002".into(),
            name: "ssi_conflict_detection".into(),
            category: ScenarioCategory::Concurrency,
        },
        ScenarioRef {
            id: "REC-001".into(),
            name: "crash_recovery_wal".into(),
            category: ScenarioCategory::Recovery,
        },
        ScenarioRef {
            id: "REC-002".into(),
            name: "checkpoint_crash".into(),
            category: ScenarioCategory::Recovery,
        },
        ScenarioRef {
            id: "SQL-001".into(),
            name: "complex_query_correctness".into(),
            category: ScenarioCategory::Correctness,
        },
        ScenarioRef {
            id: "SQL-002".into(),
            name: "join_ordering".into(),
            category: ScenarioCategory::Correctness,
        },
        ScenarioRef {
            id: "PERF-001".into(),
            name: "bulk_insert_throughput".into(),
            category: ScenarioCategory::Performance,
        },
        ScenarioRef {
            id: "COMPAT-001".into(),
            name: "sqlite_format_compat".into(),
            category: ScenarioCategory::Compatibility,
        },
    ]
}

fn canonical_invariant_edges() -> Vec<InvariantEdge> {
    vec![
        // MVCC crate owns INV-1 through INV-7
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-1".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-2".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-3".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-4".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-5".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-6".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-mvcc".into(),
            invariant_id: "INV-7".into(),
            strength: 0,
        },
        // WAL crate owns WAL-1, indirectly affects INV-5, INV-6
        InvariantEdge {
            code_area_id: "fsqlite-wal".into(),
            invariant_id: "WAL-1".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-wal".into(),
            invariant_id: "INV-5".into(),
            strength: 1,
        },
        InvariantEdge {
            code_area_id: "fsqlite-wal".into(),
            invariant_id: "INV-6".into(),
            strength: 1,
        },
        // B-tree crate owns BTREE-1
        InvariantEdge {
            code_area_id: "fsqlite-btree".into(),
            invariant_id: "BTREE-1".into(),
            strength: 0,
        },
        // Pager crate owns PAGER-1, indirectly affects WAL-1
        InvariantEdge {
            code_area_id: "fsqlite-pager".into(),
            invariant_id: "PAGER-1".into(),
            strength: 0,
        },
        InvariantEdge {
            code_area_id: "fsqlite-pager".into(),
            invariant_id: "WAL-1".into(),
            strength: 1,
        },
        // Core crate indirectly affects everything
        InvariantEdge {
            code_area_id: "fsqlite-core".into(),
            invariant_id: "INV-6".into(),
            strength: 1,
        },
        // VDBE indirectly affects correctness invariants
        InvariantEdge {
            code_area_id: "fsqlite-vdbe".into(),
            invariant_id: "INV-6".into(),
            strength: 1,
        },
    ]
}

fn canonical_scenario_edges() -> Vec<ScenarioEdge> {
    vec![
        // MVCC -> concurrency scenarios
        ScenarioEdge {
            code_area_id: "fsqlite-mvcc".into(),
            scenario_id: "MVCC-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-mvcc".into(),
            scenario_id: "MVCC-002".into(),
        },
        // WAL -> recovery scenarios
        ScenarioEdge {
            code_area_id: "fsqlite-wal".into(),
            scenario_id: "REC-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-wal".into(),
            scenario_id: "REC-002".into(),
        },
        // Pager -> recovery scenarios
        ScenarioEdge {
            code_area_id: "fsqlite-pager".into(),
            scenario_id: "REC-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-pager".into(),
            scenario_id: "REC-002".into(),
        },
        // Parser/Planner/VDBE -> SQL correctness
        ScenarioEdge {
            code_area_id: "fsqlite-parser".into(),
            scenario_id: "SQL-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-planner".into(),
            scenario_id: "SQL-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-planner".into(),
            scenario_id: "SQL-002".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-vdbe".into(),
            scenario_id: "SQL-001".into(),
        },
        // Core -> all correctness and performance
        ScenarioEdge {
            code_area_id: "fsqlite-core".into(),
            scenario_id: "SQL-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-core".into(),
            scenario_id: "PERF-001".into(),
        },
        ScenarioEdge {
            code_area_id: "fsqlite-core".into(),
            scenario_id: "COMPAT-001".into(),
        },
        // B-tree -> performance
        ScenarioEdge {
            code_area_id: "fsqlite-btree".into(),
            scenario_id: "PERF-001".into(),
        },
    ]
}

fn canonical_lane_edges() -> Vec<LaneEdge> {
    let mut edges = Vec::new();

    // Every crate gets unit tests (mandatory).
    for area in canonical_code_areas() {
        edges.push(LaneEdge {
            code_area_id: area.id(),
            lane: ValidationLane::UnitTests,
            mandatory: true,
        });
    }

    // Storage crates -> storage integration + recovery
    for crate_name in &[
        "fsqlite-vfs",
        "fsqlite-pager",
        "fsqlite-wal",
        "fsqlite-mvcc",
        "fsqlite-btree",
    ] {
        edges.push(LaneEdge {
            code_area_id: (*crate_name).into(),
            lane: ValidationLane::StorageIntegration,
            mandatory: true,
        });
        edges.push(LaneEdge {
            code_area_id: (*crate_name).into(),
            lane: ValidationLane::RecoveryDurability,
            mandatory: true,
        });
    }

    // MVCC -> concurrency stress + soak
    edges.push(LaneEdge {
        code_area_id: "fsqlite-mvcc".into(),
        lane: ValidationLane::ConcurrencyStress,
        mandatory: true,
    });
    edges.push(LaneEdge {
        code_area_id: "fsqlite-mvcc".into(),
        lane: ValidationLane::SoakEndurance,
        mandatory: false,
    });

    // SQL pipeline -> sql pipeline lane + metamorphic
    for crate_name in &["fsqlite-parser", "fsqlite-planner", "fsqlite-vdbe"] {
        edges.push(LaneEdge {
            code_area_id: (*crate_name).into(),
            lane: ValidationLane::SqlPipeline,
            mandatory: true,
        });
        edges.push(LaneEdge {
            code_area_id: (*crate_name).into(),
            lane: ValidationLane::MetamorphicDifferential,
            mandatory: false,
        });
    }

    // Core -> full E2E + performance regression
    edges.push(LaneEdge {
        code_area_id: "fsqlite-core".into(),
        lane: ValidationLane::FullE2e,
        mandatory: true,
    });
    edges.push(LaneEdge {
        code_area_id: "fsqlite-core".into(),
        lane: ValidationLane::PerformanceRegression,
        mandatory: false,
    });
    edges.push(LaneEdge {
        code_area_id: "fsqlite-core".into(),
        lane: ValidationLane::StorageIntegration,
        mandatory: true,
    });
    edges.push(LaneEdge {
        code_area_id: "fsqlite-core".into(),
        lane: ValidationLane::SqlPipeline,
        mandatory: true,
    });

    // Functions/Extensions -> sql pipeline
    edges.push(LaneEdge {
        code_area_id: "fsqlite-functions".into(),
        lane: ValidationLane::SqlPipeline,
        mandatory: true,
    });
    edges.push(LaneEdge {
        code_area_id: "fsqlite-extensions".into(),
        lane: ValidationLane::SqlPipeline,
        mandatory: false,
    });

    // CLI -> full E2E
    edges.push(LaneEdge {
        code_area_id: "fsqlite-cli".into(),
        lane: ValidationLane::FullE2e,
        mandatory: false,
    });

    // Harness -> no additional lanes (self-testing)
    // (already has unit tests from the universal loop)

    edges
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

/// Coverage report for the impact graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactCoverage {
    /// Total code areas.
    pub code_area_count: usize,
    /// Total invariant references.
    pub invariant_count: usize,
    /// Total scenario references.
    pub scenario_count: usize,
    /// Total edges (all types).
    pub total_edges: usize,
    /// Code areas per risk tier.
    pub by_risk_tier: BTreeMap<u8, usize>,
    /// Scenarios per category.
    pub by_category: BTreeMap<String, usize>,
    /// Lanes used.
    pub lanes_used: Vec<String>,
}

/// Compute coverage metrics for an impact graph.
#[must_use]
pub fn compute_impact_coverage(graph: &ImpactGraph) -> ImpactCoverage {
    let mut by_risk_tier: BTreeMap<u8, usize> = BTreeMap::new();
    for area in &graph.code_areas {
        *by_risk_tier.entry(area.risk_tier).or_insert(0) += 1;
    }

    let mut by_category: BTreeMap<String, usize> = BTreeMap::new();
    for scen in &graph.scenarios {
        *by_category.entry(format!("{}", scen.category)).or_insert(0) += 1;
    }

    let lanes_used: BTreeSet<String> = graph
        .lane_edges
        .iter()
        .map(|e| format!("{}", e.lane))
        .collect();

    ImpactCoverage {
        code_area_count: graph.code_areas.len(),
        invariant_count: graph.invariants.len(),
        scenario_count: graph.scenarios.len(),
        total_edges: graph.invariant_edges.len()
            + graph.scenario_edges.len()
            + graph.lane_edges.len(),
        by_risk_tier,
        by_category,
        lanes_used: lanes_used.into_iter().collect(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_graph_valid() {
        let graph = ImpactGraph::canonical();
        let errors = graph.validate();
        assert!(errors.is_empty(), "canonical graph has errors: {errors:?}");
    }

    #[test]
    fn test_code_areas_non_empty() {
        let graph = ImpactGraph::canonical();
        assert!(graph.code_areas.len() >= 10);
    }

    #[test]
    fn test_code_area_ids_unique() {
        let graph = ImpactGraph::canonical();
        let ids: BTreeSet<String> = graph.code_areas.iter().map(CodeArea::id).collect();
        assert_eq!(ids.len(), graph.code_areas.len());
    }

    #[test]
    fn test_invariant_refs_cover_mvcc() {
        let graph = ImpactGraph::canonical();
        let inv_ids: BTreeSet<&str> = graph.invariants.iter().map(|i| i.id.as_str()).collect();
        for expected in &[
            "INV-1", "INV-2", "INV-3", "INV-4", "INV-5", "INV-6", "INV-7",
        ] {
            assert!(inv_ids.contains(expected), "missing invariant: {expected}");
        }
    }

    #[test]
    fn test_scenario_refs_cover_categories() {
        let graph = ImpactGraph::canonical();
        let categories: BTreeSet<ScenarioCategory> =
            graph.scenarios.iter().map(|s| s.category).collect();
        assert!(categories.contains(&ScenarioCategory::Concurrency));
        assert!(categories.contains(&ScenarioCategory::Recovery));
        assert!(categories.contains(&ScenarioCategory::Correctness));
    }

    #[test]
    fn test_lanes_for_mvcc_changes() {
        let graph = ImpactGraph::canonical();
        let lanes = graph.lanes_for_changes(&["fsqlite-mvcc"]);
        let lane_names: Vec<ValidationLane> = lanes.iter().map(|(l, _)| *l).collect();

        assert!(lane_names.contains(&ValidationLane::UnitTests));
        assert!(lane_names.contains(&ValidationLane::StorageIntegration));
        assert!(lane_names.contains(&ValidationLane::ConcurrencyStress));
        assert!(lane_names.contains(&ValidationLane::RecoveryDurability));
    }

    #[test]
    fn test_lanes_for_parser_changes() {
        let graph = ImpactGraph::canonical();
        let lanes = graph.lanes_for_changes(&["fsqlite-parser"]);
        let lane_names: Vec<ValidationLane> = lanes.iter().map(|(l, _)| *l).collect();

        assert!(lane_names.contains(&ValidationLane::UnitTests));
        assert!(lane_names.contains(&ValidationLane::SqlPipeline));
    }

    #[test]
    fn test_lanes_for_core_changes() {
        let graph = ImpactGraph::canonical();
        let lanes = graph.lanes_for_changes(&["fsqlite-core"]);
        let lane_names: Vec<ValidationLane> = lanes.iter().map(|(l, _)| *l).collect();

        assert!(lane_names.contains(&ValidationLane::FullE2e));
        assert!(lane_names.contains(&ValidationLane::StorageIntegration));
        assert!(lane_names.contains(&ValidationLane::SqlPipeline));
    }

    #[test]
    fn test_lanes_always_include_unit_tests() {
        let graph = ImpactGraph::canonical();
        let lanes = graph.lanes_for_changes(&["fsqlite-harness"]);
        let lane_names: Vec<ValidationLane> = lanes.iter().map(|(l, _)| *l).collect();
        assert!(lane_names.contains(&ValidationLane::UnitTests));
    }

    #[test]
    fn test_invariants_for_mvcc_changes() {
        let graph = ImpactGraph::canonical();
        let invs = graph.invariants_for_changes(&["fsqlite-mvcc"]);
        let inv_ids: BTreeSet<&str> = invs.iter().map(|i| i.id.as_str()).collect();
        assert!(inv_ids.contains("INV-1"));
        assert!(inv_ids.contains("INV-7"));
    }

    #[test]
    fn test_scenarios_for_wal_changes() {
        let graph = ImpactGraph::canonical();
        let scens = graph.scenarios_for_changes(&["fsqlite-wal"]);
        let scen_ids: BTreeSet<&str> = scens.iter().map(|s| s.id.as_str()).collect();
        assert!(scen_ids.contains("REC-001"));
    }

    #[test]
    fn test_graph_json_roundtrip() {
        let graph = ImpactGraph::canonical();
        let json = graph.to_json().expect("serialize");
        let restored = ImpactGraph::from_json(&json).expect("deserialize");
        assert_eq!(restored.code_areas.len(), graph.code_areas.len());
        assert_eq!(restored.invariants.len(), graph.invariants.len());
    }

    #[test]
    fn test_coverage_report() {
        let graph = ImpactGraph::canonical();
        let cov = compute_impact_coverage(&graph);
        assert!(cov.code_area_count >= 10);
        assert!(cov.invariant_count >= 7);
        assert!(cov.scenario_count >= 5);
        assert!(cov.total_edges > 20);
        assert!(!cov.lanes_used.is_empty());
    }

    #[test]
    fn test_validation_lane_time_budgets() {
        for lane in ValidationLane::ALL {
            assert!(lane.time_budget_secs() > 0, "{lane} has zero time budget");
        }
    }

    #[test]
    fn test_validation_lane_all_count() {
        assert_eq!(ValidationLane::ALL.len(), 9);
    }

    #[test]
    fn test_code_area_display() {
        let area = CodeArea {
            crate_name: "fsqlite-mvcc".into(),
            module_path: None,
            risk_tier: 0,
            description: "test".into(),
        };
        let s = area.to_string();
        assert!(s.contains("fsqlite-mvcc"));
        assert!(s.contains("tier=0"));
    }

    #[test]
    fn test_code_area_id_with_module() {
        let area = CodeArea {
            crate_name: "fsqlite-mvcc".into(),
            module_path: Some("version_store".into()),
            risk_tier: 0,
            description: "test".into(),
        };
        assert_eq!(area.id(), "fsqlite-mvcc::version_store");
    }

    #[test]
    fn test_multi_area_change_impact() {
        let graph = ImpactGraph::canonical();
        let lanes = graph.lanes_for_changes(&["fsqlite-mvcc", "fsqlite-parser"]);
        let lane_names: Vec<ValidationLane> = lanes.iter().map(|(l, _)| *l).collect();

        // Should include lanes from both crates.
        assert!(lane_names.contains(&ValidationLane::ConcurrencyStress));
        assert!(lane_names.contains(&ValidationLane::SqlPipeline));
    }
}
