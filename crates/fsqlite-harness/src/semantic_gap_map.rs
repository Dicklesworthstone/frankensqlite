//! SQL semantic gap map and closure backlog (bd-1dp9.3.1).
//!
//! Generates an explicit mapping from differential failures to concrete code
//! locations and spec semantics clauses, organized by subsystem pipeline
//! stage (parser → resolver → planner → VDBE).
//!
//! # Purpose
//!
//! The gap map serves as the authoritative bridge between automated failure
//! detection (Track 2) and manual closure work (Track 3). Every open mismatch
//! must be linked to a closure task with:
//! - The originating feature(s) in the parity taxonomy
//! - The attributed subsystem and pipeline stage
//! - A behavior contract (expected vs actual)
//! - Reproduction steps (schema + minimal workload)
//!
//! # Architecture
//!
//! ```text
//! MinimalReproduction[] + FeatureUniverse
//!          ↓
//!    GapAnalyzer.analyze()
//!          ↓
//!    SemanticGapMap { entries: GapEntry[], stats }
//!          ↓
//!    closure_backlog() → ClosureBacklog { items: ClosureItem[] }
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::mismatch_minimizer::{MinimalReproduction, MismatchSignature, Subsystem};
use crate::parity_taxonomy::{FeatureCategory, FeatureId, FeatureUniverse};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.3.1";

/// Schema version for gap map output format.
pub const GAP_MAP_SCHEMA_VERSION: u32 = 1;

// ===========================================================================
// Pipeline Stage
// ===========================================================================

/// Pipeline stage within the SQL execution path.
///
/// More granular than [`Subsystem`]: maps to the sequential processing stages
/// that a SQL statement flows through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PipelineStage {
    /// Tokenization and parsing into AST.
    Parse,
    /// Name resolution and schema binding.
    Resolve,
    /// Query planning and optimization.
    Plan,
    /// VDBE bytecode generation.
    Codegen,
    /// VDBE bytecode execution.
    Execute,
    /// Storage operations (B-tree, pager).
    Storage,
    /// Cross-cutting (type system, functions, PRAGMAs).
    CrossCutting,
}

impl PipelineStage {
    /// Map from subsystem to pipeline stage.
    #[must_use]
    pub fn from_subsystem(subsystem: Subsystem) -> Self {
        match subsystem {
            Subsystem::Parser => Self::Parse,
            Subsystem::Resolver => Self::Resolve,
            Subsystem::Planner => Self::Plan,
            Subsystem::Vdbe => Self::Execute,
            Subsystem::Storage | Subsystem::Wal | Subsystem::Mvcc => Self::Storage,
            Subsystem::Functions
            | Subsystem::Extension
            | Subsystem::TypeSystem
            | Subsystem::Pragma
            | Subsystem::Unknown => Self::CrossCutting,
        }
    }

    /// All stages in pipeline order.
    pub const ALL: [Self; 7] = [
        Self::Parse,
        Self::Resolve,
        Self::Plan,
        Self::Codegen,
        Self::Execute,
        Self::Storage,
        Self::CrossCutting,
    ];
}

impl fmt::Display for PipelineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse => write!(f, "parse"),
            Self::Resolve => write!(f, "resolve"),
            Self::Plan => write!(f, "plan"),
            Self::Codegen => write!(f, "codegen"),
            Self::Execute => write!(f, "execute"),
            Self::Storage => write!(f, "storage"),
            Self::CrossCutting => write!(f, "cross_cutting"),
        }
    }
}

// ===========================================================================
// Behavior Contract
// ===========================================================================

/// Expected vs actual behavior for a semantic gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorContract {
    /// What SQLite does (expected behavior).
    pub expected: String,
    /// What FrankenSQLite does (observed behavior).
    pub actual: String,
    /// Spec section or reference.
    pub spec_reference: Option<String>,
}

// ===========================================================================
// Gap Entry
// ===========================================================================

/// A single semantic gap: one mismatch mapped to its root cause location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GapEntry {
    /// Unique gap identifier (derived from signature hash).
    pub gap_id: String,
    /// Mismatch signature for deduplication.
    pub signature: MismatchSignature,
    /// Pipeline stage where the gap manifests.
    pub pipeline_stage: PipelineStage,
    /// Attributed subsystem.
    pub subsystem: Subsystem,
    /// Related feature IDs from the parity taxonomy.
    pub feature_ids: Vec<FeatureId>,
    /// Behavior contract.
    pub behavior_contract: BehaviorContract,
    /// Code locations likely involved.
    pub code_locations: Vec<CodeLocation>,
    /// Minimal reproduction SQL.
    pub reproduction: GapReproduction,
    /// Severity: how broadly this gap impacts parity.
    pub severity: GapSeverity,
}

/// A code location reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeLocation {
    /// Crate name (e.g., "fsqlite-parser").
    pub crate_name: String,
    /// Module path (e.g., "src/select.rs").
    pub module_path: String,
    /// Optional function/method name.
    pub function: Option<String>,
}

impl fmt::Display for CodeLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.crate_name, self.module_path)?;
        if let Some(func) = &self.function {
            write!(f, "::{func}")?;
        }
        Ok(())
    }
}

/// Minimal reproduction for a gap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GapReproduction {
    /// Schema setup SQL.
    pub schema: Vec<String>,
    /// Minimal workload.
    pub workload: Vec<String>,
    /// Original seed.
    pub seed: u64,
    /// Reduction ratio from minimization.
    pub reduction_ratio: f64,
}

/// Severity of a semantic gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum GapSeverity {
    /// Correctness failure: wrong results.
    Critical,
    /// Behavioral divergence: different error handling or edge cases.
    Major,
    /// Minor cosmetic difference: formatting, message text.
    Minor,
    /// Information-only: detected but no user impact.
    Info,
}

impl GapSeverity {
    /// Numeric priority (lower = more urgent).
    #[must_use]
    pub const fn priority(self) -> u8 {
        match self {
            Self::Critical => 0,
            Self::Major => 1,
            Self::Minor => 2,
            Self::Info => 3,
        }
    }
}

impl fmt::Display for GapSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Critical => write!(f, "critical"),
            Self::Major => write!(f, "major"),
            Self::Minor => write!(f, "minor"),
            Self::Info => write!(f, "info"),
        }
    }
}

// ===========================================================================
// Semantic Gap Map
// ===========================================================================

/// Complete semantic gap map: all identified gaps organized by pipeline stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticGapMap {
    /// Schema version.
    pub schema_version: u32,
    /// Deterministic content hash.
    pub map_hash: String,
    /// Gap entries sorted by severity then pipeline stage.
    pub entries: Vec<GapEntry>,
    /// Statistics.
    pub stats: GapMapStats,
}

/// Statistics for a gap map.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GapMapStats {
    /// Total gaps identified.
    pub total_gaps: usize,
    /// Gaps per pipeline stage.
    pub by_stage: BTreeMap<String, usize>,
    /// Gaps per subsystem.
    pub by_subsystem: BTreeMap<String, usize>,
    /// Gaps per severity.
    pub by_severity: BTreeMap<String, usize>,
    /// Gaps per feature category.
    pub by_category: BTreeMap<String, usize>,
    /// Feature IDs with gaps.
    pub affected_feature_ids: BTreeSet<String>,
}

impl SemanticGapMap {
    /// Serialize to JSON.
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

    /// Human-readable summary.
    #[must_use]
    pub fn summary_line(&self) -> String {
        format!(
            "Gap map: {} gaps ({} critical, {} major, {} minor), {} features affected",
            self.stats.total_gaps,
            self.stats.by_severity.get("critical").unwrap_or(&0),
            self.stats.by_severity.get("major").unwrap_or(&0),
            self.stats.by_severity.get("minor").unwrap_or(&0),
            self.stats.affected_feature_ids.len(),
        )
    }
}

// ===========================================================================
// Gap Analyzer
// ===========================================================================

/// Configuration for gap analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GapAnalyzerConfig {
    /// Whether to auto-generate code location hints.
    pub infer_code_locations: bool,
    /// Whether to auto-link to taxonomy features.
    pub auto_link_features: bool,
}

impl Default for GapAnalyzerConfig {
    fn default() -> Self {
        Self {
            infer_code_locations: true,
            auto_link_features: true,
        }
    }
}

/// Analyzes minimized reproductions and generates a semantic gap map.
pub struct GapAnalyzer {
    config: GapAnalyzerConfig,
}

impl GapAnalyzer {
    /// Create a new gap analyzer.
    #[must_use]
    pub fn new(config: GapAnalyzerConfig) -> Self {
        Self { config }
    }

    /// Analyze a set of minimized reproductions against the feature universe.
    #[must_use]
    pub fn analyze(
        &self,
        reproductions: &[MinimalReproduction],
        universe: &FeatureUniverse,
    ) -> SemanticGapMap {
        let mut entries: Vec<GapEntry> = reproductions
            .iter()
            .map(|repro| self.repro_to_gap(repro, universe))
            .collect();

        // Sort by severity (critical first), then pipeline stage.
        entries.sort_by(|a, b| {
            a.severity
                .priority()
                .cmp(&b.severity.priority())
                .then_with(|| a.pipeline_stage.cmp(&b.pipeline_stage))
                .then_with(|| a.gap_id.cmp(&b.gap_id))
        });

        let stats = compute_stats(&entries);
        let map_hash = compute_map_hash(&entries);

        SemanticGapMap {
            schema_version: GAP_MAP_SCHEMA_VERSION,
            map_hash,
            entries,
            stats,
        }
    }

    /// Convert a minimized reproduction to a gap entry.
    fn repro_to_gap(&self, repro: &MinimalReproduction, universe: &FeatureUniverse) -> GapEntry {
        let subsystem = repro.signature.subsystem;
        let pipeline_stage = PipelineStage::from_subsystem(subsystem);

        let feature_ids = if self.config.auto_link_features {
            infer_feature_ids(repro, universe)
        } else {
            Vec::new()
        };

        let code_locations = if self.config.infer_code_locations {
            infer_code_locations(subsystem, &repro.signature.first_diverging_sql)
        } else {
            Vec::new()
        };

        let behavior_contract = infer_behavior_contract(repro);
        let severity = infer_severity(repro);

        let gap_id = format!(
            "GAP-{}",
            &repro.signature.hash[..8.min(repro.signature.hash.len())]
        );

        GapEntry {
            gap_id,
            signature: repro.signature.clone(),
            pipeline_stage,
            subsystem,
            feature_ids,
            behavior_contract,
            code_locations,
            reproduction: GapReproduction {
                schema: repro.schema.clone(),
                workload: repro.minimal_workload.clone(),
                seed: repro.original_seed,
                reduction_ratio: repro.reduction_ratio,
            },
            severity,
        }
    }
}

// ===========================================================================
// Closure Backlog
// ===========================================================================

/// A closure item: one task in the gap closure backlog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosureItem {
    /// Unique closure task ID.
    pub closure_id: String,
    /// Related gap ID.
    pub gap_id: String,
    /// Title for the closure task.
    pub title: String,
    /// Description including behavior contract and reproduction.
    pub description: String,
    /// Priority (from severity).
    pub priority: u8,
    /// Pipeline stage.
    pub pipeline_stage: PipelineStage,
    /// Affected feature IDs.
    pub feature_ids: Vec<FeatureId>,
    /// Code locations to investigate.
    pub code_locations: Vec<CodeLocation>,
}

/// A closure backlog: prioritized list of tasks to close all gaps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosureBacklog {
    /// Schema version.
    pub schema_version: u32,
    /// Closure items sorted by priority.
    pub items: Vec<ClosureItem>,
    /// Total items.
    pub total_items: usize,
    /// Items per pipeline stage.
    pub by_stage: BTreeMap<String, usize>,
}

/// Generate a closure backlog from a gap map.
#[must_use]
pub fn closure_backlog(gap_map: &SemanticGapMap) -> ClosureBacklog {
    let items: Vec<ClosureItem> = gap_map
        .entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let mut description = String::new();
            let _ = write!(
                description,
                "## Behavior Contract\n\n**Expected**: {}\n**Actual**: {}\n",
                entry.behavior_contract.expected, entry.behavior_contract.actual,
            );
            if let Some(ref spec) = entry.behavior_contract.spec_reference {
                let _ = writeln!(description, "**Spec ref**: {spec}");
            }
            let _ = write!(
                description,
                "\n## Reproduction\n\nSchema:\n```sql\n{}\n```\n\nWorkload:\n```sql\n{}\n```\n",
                entry.reproduction.schema.join("\n"),
                entry.reproduction.workload.join("\n"),
            );

            ClosureItem {
                closure_id: format!("CL-{i:04}"),
                gap_id: entry.gap_id.clone(),
                title: format!(
                    "[{}] Fix {} gap: {}",
                    entry.pipeline_stage,
                    entry.severity,
                    entry
                        .signature
                        .first_diverging_sql
                        .chars()
                        .take(60)
                        .collect::<String>()
                ),
                description,
                priority: entry.severity.priority(),
                pipeline_stage: entry.pipeline_stage,
                feature_ids: entry.feature_ids.clone(),
                code_locations: entry.code_locations.clone(),
            }
        })
        .collect();

    let mut by_stage: BTreeMap<String, usize> = BTreeMap::new();
    for item in &items {
        *by_stage.entry(item.pipeline_stage.to_string()).or_insert(0) += 1;
    }

    let total_items = items.len();
    ClosureBacklog {
        schema_version: GAP_MAP_SCHEMA_VERSION,
        items,
        total_items,
        by_stage,
    }
}

// ===========================================================================
// Inference Helpers
// ===========================================================================

/// Infer feature IDs from a reproduction by keyword matching.
fn infer_feature_ids(repro: &MinimalReproduction, universe: &FeatureUniverse) -> Vec<FeatureId> {
    let all_sql: String = repro
        .schema
        .iter()
        .chain(repro.minimal_workload.iter())
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    let upper = all_sql.to_uppercase();

    let mut matched = Vec::new();
    for (fid, feature) in &universe.features {
        // Match by title keywords in SQL.
        let title_upper = feature.title.to_uppercase();
        let words: Vec<&str> = title_upper
            .split_whitespace()
            .filter(|w: &&str| w.len() > 3) // Skip short words
            .collect();

        if words.iter().any(|w| upper.contains(w)) {
            matched.push(fid.clone());
        }
    }

    // Limit to most relevant (max 5).
    matched.truncate(5);
    matched
}

/// Infer code locations from subsystem and SQL content.
fn infer_code_locations(subsystem: Subsystem, sql: &str) -> Vec<CodeLocation> {
    let upper = sql.to_uppercase();
    let mut locations = Vec::new();

    match subsystem {
        Subsystem::Parser => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-parser".to_owned(),
                module_path: "src/parser.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::Resolver => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-planner".to_owned(),
                module_path: "src/resolver.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::Planner => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-planner".to_owned(),
                module_path: "src/planner.rs".to_owned(),
                function: None,
            });
            if upper.contains("JOIN") {
                locations.push(CodeLocation {
                    crate_name: "fsqlite-planner".to_owned(),
                    module_path: "src/join.rs".to_owned(),
                    function: None,
                });
            }
        }
        Subsystem::Vdbe => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-vdbe".to_owned(),
                module_path: "src/codegen.rs".to_owned(),
                function: None,
            });
            if upper.contains("INSERT") || upper.contains("UPDATE") || upper.contains("DELETE") {
                locations.push(CodeLocation {
                    crate_name: "fsqlite-vdbe".to_owned(),
                    module_path: "src/engine.rs".to_owned(),
                    function: None,
                });
            }
        }
        Subsystem::Storage | Subsystem::Wal | Subsystem::Mvcc => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-btree".to_owned(),
                module_path: "src/btree.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::Functions => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-functions".to_owned(),
                module_path: "src/lib.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::Extension => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-ext-json".to_owned(),
                module_path: "src/lib.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::TypeSystem => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-types".to_owned(),
                module_path: "src/affinity.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::Pragma => {
            locations.push(CodeLocation {
                crate_name: "fsqlite-core".to_owned(),
                module_path: "src/pragma.rs".to_owned(),
                function: None,
            });
        }
        Subsystem::Unknown => {}
    }

    locations
}

/// Infer behavior contract from a reproduction.
fn infer_behavior_contract(repro: &MinimalReproduction) -> BehaviorContract {
    if let Some(div) = repro.divergences.first() {
        BehaviorContract {
            expected: format!("C SQLite outcome: {:?}", div.csqlite_outcome),
            actual: format!("FrankenSQLite outcome: {:?}", div.fsqlite_outcome),
            spec_reference: None,
        }
    } else {
        BehaviorContract {
            expected: "No divergence expected".to_owned(),
            actual: "Divergence detected".to_owned(),
            spec_reference: None,
        }
    }
}

/// Infer severity from a reproduction.
fn infer_severity(repro: &MinimalReproduction) -> GapSeverity {
    // Critical if results diverge (wrong data), Major if errors differ,
    // Minor for cosmetic, Info otherwise.
    if repro.divergences.is_empty() {
        return GapSeverity::Info;
    }

    let has_result_divergence = repro.divergences.iter().any(|d| {
        use crate::differential_v2::StmtOutcome;
        matches!(
            (&d.csqlite_outcome, &d.fsqlite_outcome),
            (StmtOutcome::Rows(_), StmtOutcome::Rows(_))
        )
    });

    let has_error_divergence = repro.divergences.iter().any(|d| {
        use crate::differential_v2::StmtOutcome;
        matches!(
            (&d.csqlite_outcome, &d.fsqlite_outcome),
            (StmtOutcome::Error(_), StmtOutcome::Rows(_))
                | (StmtOutcome::Rows(_), StmtOutcome::Error(_))
        )
    });

    if has_error_divergence {
        GapSeverity::Critical
    } else if has_result_divergence {
        GapSeverity::Major
    } else {
        GapSeverity::Minor
    }
}

// ===========================================================================
// Stats and Hashing
// ===========================================================================

/// Compute statistics from gap entries.
fn compute_stats(entries: &[GapEntry]) -> GapMapStats {
    let mut stats = GapMapStats {
        total_gaps: entries.len(),
        ..Default::default()
    };

    for entry in entries {
        *stats
            .by_stage
            .entry(entry.pipeline_stage.to_string())
            .or_insert(0) += 1;
        *stats
            .by_subsystem
            .entry(entry.subsystem.to_string())
            .or_insert(0) += 1;
        *stats
            .by_severity
            .entry(entry.severity.to_string())
            .or_insert(0) += 1;

        for fid in &entry.feature_ids {
            stats.affected_feature_ids.insert(fid.to_string());
        }

        // Category from subsystem.
        let category = subsystem_to_category(entry.subsystem);
        *stats
            .by_category
            .entry(category.display_name().to_owned())
            .or_insert(0) += 1;
    }

    stats
}

/// Map subsystem to feature category.
fn subsystem_to_category(subsystem: Subsystem) -> FeatureCategory {
    match subsystem {
        Subsystem::Parser | Subsystem::Resolver | Subsystem::Planner | Subsystem::Unknown => {
            FeatureCategory::SqlGrammar
        }
        Subsystem::Vdbe => FeatureCategory::VdbeOpcodes,
        Subsystem::Storage | Subsystem::Wal | Subsystem::Mvcc => {
            FeatureCategory::StorageTransaction
        }
        Subsystem::Functions => FeatureCategory::BuiltinFunctions,
        Subsystem::Extension => FeatureCategory::Extensions,
        Subsystem::TypeSystem => FeatureCategory::TypeSystem,
        Subsystem::Pragma => FeatureCategory::Pragma,
    }
}

/// Compute deterministic hash for the gap map.
fn compute_map_hash(entries: &[GapEntry]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"gapmap-v1:");
    for entry in entries {
        hasher.update(entry.gap_id.as_bytes());
        hasher.update(b":");
        hasher.update(entry.signature.hash.as_bytes());
        hasher.update(b"\n");
    }
    let digest = hasher.finalize();
    let mut s = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(s, "{byte:02x}");
    }
    s
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::differential_v2::{NormalizedValue, StatementDivergence, StmtOutcome};
    use crate::metamorphic::MismatchClassification;

    fn make_signature(hash: &str, subsystem: Subsystem, sql: &str) -> MismatchSignature {
        MismatchSignature {
            hash: hash.to_owned(),
            classification: MismatchClassification::TrueDivergence {
                description: "test divergence".to_owned(),
            },
            subsystem,
            minimal_statement_count: 1,
            first_diverging_sql: sql.to_owned(),
        }
    }

    fn make_divergence(index: usize, sql: &str) -> StatementDivergence {
        StatementDivergence {
            index,
            sql: sql.to_owned(),
            csqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
            fsqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(2)]]),
        }
    }

    fn make_repro(
        sig: MismatchSignature,
        schema: Vec<String>,
        workload: Vec<String>,
        divergences: Vec<StatementDivergence>,
    ) -> MinimalReproduction {
        MinimalReproduction {
            schema_version: 1,
            signature: sig,
            original_seed: 42,
            schema,
            minimal_workload: workload,
            original_workload_size: 10,
            reduction_ratio: 0.8,
            first_divergence_index: divergences.first().map(|d| d.index),
            divergences,
            repro_command: "test".to_owned(),
        }
    }

    fn test_universe() -> FeatureUniverse {
        crate::parity_taxonomy::build_canonical_universe()
    }

    // --- Pipeline Stage ---

    #[test]
    fn test_pipeline_stage_from_subsystem() {
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Parser),
            PipelineStage::Parse
        );
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Resolver),
            PipelineStage::Resolve
        );
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Planner),
            PipelineStage::Plan
        );
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Vdbe),
            PipelineStage::Execute
        );
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Storage),
            PipelineStage::Storage
        );
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Wal),
            PipelineStage::Storage
        );
        assert_eq!(
            PipelineStage::from_subsystem(Subsystem::Functions),
            PipelineStage::CrossCutting
        );
    }

    #[test]
    fn test_pipeline_stage_display() {
        assert_eq!(PipelineStage::Parse.to_string(), "parse");
        assert_eq!(PipelineStage::Execute.to_string(), "execute");
        assert_eq!(PipelineStage::CrossCutting.to_string(), "cross_cutting");
    }

    // --- Gap Analyzer ---

    #[test]
    fn test_analyze_empty() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig::default());
        let universe = test_universe();
        let map = analyzer.analyze(&[], &universe);

        assert_eq!(map.entries.len(), 0);
        assert_eq!(map.stats.total_gaps, 0);
    }

    #[test]
    fn test_analyze_single_repro() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig::default());
        let universe = test_universe();

        let sig = make_signature("abcdef0123456789", Subsystem::Vdbe, "SELECT a FROM t");
        let div = make_divergence(0, "SELECT a FROM t");
        let repro = make_repro(
            sig,
            vec!["CREATE TABLE t(a INTEGER);".to_owned()],
            vec!["SELECT a FROM t;".to_owned()],
            vec![div],
        );

        let map = analyzer.analyze(&[repro], &universe);
        assert_eq!(map.entries.len(), 1);
        assert_eq!(map.stats.total_gaps, 1);

        let entry = &map.entries[0];
        assert_eq!(entry.pipeline_stage, PipelineStage::Execute);
        assert_eq!(entry.subsystem, Subsystem::Vdbe);
        assert!(entry.gap_id.starts_with("GAP-"));
    }

    #[test]
    fn test_analyze_sorts_by_severity() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig::default());
        let universe = test_universe();

        // Make a minor gap (no divergences → Info).
        let sig1 = make_signature("1111111111111111", Subsystem::Parser, "SELECT 1");
        let repro1 = make_repro(sig1, vec![], vec!["SELECT 1;".to_owned()], vec![]);

        // Make a major gap (result divergence).
        let sig2 = make_signature("2222222222222222", Subsystem::Vdbe, "SELECT a FROM t");
        let div = make_divergence(0, "SELECT a FROM t");
        let repro2 = make_repro(
            sig2,
            vec!["CREATE TABLE t(a);".to_owned()],
            vec!["SELECT a FROM t;".to_owned()],
            vec![div],
        );

        let map = analyzer.analyze(&[repro1, repro2], &universe);
        assert_eq!(map.entries.len(), 2);
        // Major (1) should come before Info (3).
        assert!(map.entries[0].severity.priority() <= map.entries[1].severity.priority());
    }

    // --- Code Location Inference ---

    #[test]
    fn test_infer_code_locations_parser() {
        let locations = infer_code_locations(Subsystem::Parser, "SELECT 1");
        assert!(!locations.is_empty());
        assert!(locations.iter().any(|l| l.crate_name == "fsqlite-parser"));
    }

    #[test]
    fn test_infer_code_locations_vdbe_with_insert() {
        let locations = infer_code_locations(Subsystem::Vdbe, "INSERT INTO t VALUES(1)");
        assert!(locations.len() >= 2);
        assert!(locations.iter().any(|l| l.module_path.contains("engine")));
    }

    #[test]
    fn test_infer_code_locations_planner_with_join() {
        let locations = infer_code_locations(
            Subsystem::Planner,
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
        );
        assert!(locations.len() >= 2);
        assert!(locations.iter().any(|l| l.module_path.contains("join")));
    }

    // --- Severity Inference ---

    #[test]
    fn test_severity_info_no_divergences() {
        let sig = make_signature("aaaa", Subsystem::Vdbe, "SELECT 1");
        let repro = make_repro(sig, vec![], vec!["SELECT 1;".to_owned()], vec![]);
        assert_eq!(infer_severity(&repro), GapSeverity::Info);
    }

    #[test]
    fn test_severity_major_result_divergence() {
        let sig = make_signature("bbbb", Subsystem::Vdbe, "SELECT a FROM t");
        let div = make_divergence(0, "SELECT a FROM t");
        let repro = make_repro(
            sig,
            vec!["CREATE TABLE t(a);".to_owned()],
            vec!["SELECT a FROM t;".to_owned()],
            vec![div],
        );
        assert_eq!(infer_severity(&repro), GapSeverity::Major);
    }

    #[test]
    fn test_severity_critical_error_divergence() {
        let sig = make_signature("cccc", Subsystem::Vdbe, "SELECT a FROM t");
        let div = StatementDivergence {
            index: 0,
            sql: "SELECT a FROM t".to_owned(),
            csqlite_outcome: StmtOutcome::Error("no such table: t".to_owned()),
            fsqlite_outcome: StmtOutcome::Rows(vec![vec![NormalizedValue::Integer(1)]]),
        };
        let repro = make_repro(sig, vec![], vec!["SELECT a FROM t;".to_owned()], vec![div]);
        assert_eq!(infer_severity(&repro), GapSeverity::Critical);
    }

    // --- Closure Backlog ---

    #[test]
    fn test_closure_backlog_from_gap_map() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig::default());
        let universe = test_universe();

        let sig = make_signature(
            "dddddddddddddddd",
            Subsystem::Planner,
            "SELECT * FROM t1 JOIN t2",
        );
        let div = make_divergence(0, "SELECT * FROM t1 JOIN t2");
        let repro = make_repro(
            sig,
            vec![
                "CREATE TABLE t1(a);".to_owned(),
                "CREATE TABLE t2(b);".to_owned(),
            ],
            vec!["SELECT * FROM t1 JOIN t2 ON t1.a = t2.b;".to_owned()],
            vec![div],
        );

        let map = analyzer.analyze(&[repro], &universe);
        let backlog = closure_backlog(&map);

        assert_eq!(backlog.total_items, 1);
        assert!(backlog.items[0].title.contains("plan"));
        assert!(backlog.items[0].description.contains("Behavior Contract"));
    }

    // --- Stats ---

    #[test]
    fn test_stats_counts() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig {
            auto_link_features: false,
            ..GapAnalyzerConfig::default()
        });
        let universe = test_universe();

        let sig1 = make_signature("eeeeeeeeeeeeeeee", Subsystem::Parser, "SELCT 1");
        let sig2 = make_signature("ffffffffffffffff", Subsystem::Vdbe, "SELECT a FROM t");
        let div = make_divergence(0, "SELECT a FROM t");

        let repro1 = make_repro(sig1, vec![], vec!["SELCT 1;".to_owned()], vec![]);
        let repro2 = make_repro(
            sig2,
            vec!["CREATE TABLE t(a);".to_owned()],
            vec!["SELECT a FROM t;".to_owned()],
            vec![div],
        );

        let map = analyzer.analyze(&[repro1, repro2], &universe);
        assert_eq!(map.stats.total_gaps, 2);
        assert!(map.stats.by_subsystem.contains_key("parser"));
        assert!(map.stats.by_subsystem.contains_key("vdbe"));
    }

    // --- JSON Round-trip ---

    #[test]
    fn test_gap_map_json_roundtrip() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig {
            auto_link_features: false,
            ..GapAnalyzerConfig::default()
        });
        let universe = test_universe();

        let sig = make_signature("0000000000000000", Subsystem::Vdbe, "SELECT 1");
        let div = make_divergence(0, "SELECT 1");
        let repro = make_repro(sig, vec![], vec!["SELECT 1;".to_owned()], vec![div]);

        let map = analyzer.analyze(&[repro], &universe);
        let json = map.to_json().expect("serialize");
        let restored = SemanticGapMap::from_json(&json).expect("deserialize");

        assert_eq!(restored.entries.len(), map.entries.len());
        assert_eq!(restored.map_hash, map.map_hash);
    }

    // --- Map Hash Determinism ---

    #[test]
    fn test_map_hash_deterministic() {
        let analyzer = GapAnalyzer::new(GapAnalyzerConfig {
            auto_link_features: false,
            ..GapAnalyzerConfig::default()
        });
        let universe = test_universe();

        let make_map = || {
            let sig = make_signature("1234567890abcdef", Subsystem::Vdbe, "SELECT 1");
            let div = make_divergence(0, "SELECT 1");
            let repro = make_repro(sig, vec![], vec!["SELECT 1;".to_owned()], vec![div]);
            analyzer.analyze(&[repro], &universe)
        };

        let m1 = make_map();
        let m2 = make_map();
        assert_eq!(m1.map_hash, m2.map_hash);
    }

    // --- Code Location Display ---

    #[test]
    fn test_code_location_display() {
        let loc = CodeLocation {
            crate_name: "fsqlite-parser".to_owned(),
            module_path: "src/parser.rs".to_owned(),
            function: Some("parse_select".to_owned()),
        };
        let s = loc.to_string();
        assert!(s.contains("fsqlite-parser"));
        assert!(s.contains("parse_select"));
    }

    #[test]
    fn test_code_location_display_no_function() {
        let loc = CodeLocation {
            crate_name: "fsqlite-vdbe".to_owned(),
            module_path: "src/engine.rs".to_owned(),
            function: None,
        };
        let s = loc.to_string();
        assert_eq!(s, "fsqlite-vdbe:src/engine.rs");
    }

    // --- Severity Display and Priority ---

    #[test]
    fn test_severity_display() {
        assert_eq!(GapSeverity::Critical.to_string(), "critical");
        assert_eq!(GapSeverity::Major.to_string(), "major");
        assert_eq!(GapSeverity::Minor.to_string(), "minor");
        assert_eq!(GapSeverity::Info.to_string(), "info");
    }

    #[test]
    fn test_severity_priority_ordering() {
        assert!(GapSeverity::Critical.priority() < GapSeverity::Major.priority());
        assert!(GapSeverity::Major.priority() < GapSeverity::Minor.priority());
        assert!(GapSeverity::Minor.priority() < GapSeverity::Info.priority());
    }

    // --- Summary Line ---

    #[test]
    fn test_summary_line() {
        let map = SemanticGapMap {
            schema_version: 1,
            map_hash: "test".to_owned(),
            entries: vec![],
            stats: GapMapStats {
                total_gaps: 5,
                by_severity: [("critical".to_owned(), 2), ("major".to_owned(), 3)]
                    .into_iter()
                    .collect(),
                affected_feature_ids: ["F-SQL-001".to_owned(), "F-VDBE-002".to_owned()]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        };
        let line = map.summary_line();
        assert!(line.contains("5 gaps"));
        assert!(line.contains("2 critical"));
        assert!(line.contains("2 features"));
    }
}
