//! CI coverage gate workflow and threshold checker (bd-mblr.3.1.1).
//!
//! Computes coverage metrics from the unit matrix and no-mock evidence map,
//! then enforces approved SLO thresholds with actionable failure output.
//!
//! # Architecture
//!
//! The gate pulls data from:
//! 1. [`UnitMatrix`] — per-category test counts, fill percentages, invariant counts
//! 2. [`NoMockEvidenceMap`] — real-component evidence for critical-path invariants
//! 3. [`CoverageThresholds`] — SLO thresholds from bd-mblr.1.4.1
//!
//! It produces a [`CoverageGateReport`] with:
//! - Per-category pass/fail results
//! - Global coverage verdict
//! - Actionable annotations for every failing threshold
//! - Machine-readable JSON and human-readable markdown output
//!
//! # Upstream Dependencies
//!
//! - [`unit_matrix`](crate::unit_matrix) (bd-1dp9.7.1)
//! - [`no_mock_evidence`](crate::no_mock_evidence) (bd-mblr.6.7)
//! - [`parity_taxonomy`](crate::parity_taxonomy) (bd-1dp9.1.1)
//! - SLO policy from bd-mblr.1.4.1
//!
//! # Downstream Consumers
//!
//! - **bd-mblr.7.9.2**: Lane selection engine uses gate results
//! - **bd-mblr.3.3**: Flake budget and quarantine workflow

use std::fmt;
use std::fmt::Write as FmtWrite;

use serde::{Deserialize, Serialize};

use crate::no_mock_evidence::{NoMockEvidenceMap, build_evidence_map};
use crate::parity_taxonomy::FeatureCategory;
use crate::unit_matrix::{UnitMatrix, build_canonical_matrix};

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.3.1.1";

/// Schema version for report compatibility.
pub const COVERAGE_GATE_SCHEMA_VERSION: &str = "1.0.0";

// ---------------------------------------------------------------------------
// Threshold configuration
// ---------------------------------------------------------------------------

/// SLO threshold configuration for the coverage gate.
///
/// Thresholds are expressed as fractions (0.0–1.0). Each threshold is
/// checked independently and produces a separate annotation on failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageThresholds {
    /// Minimum required global fill percentage across all categories.
    pub global_fill_pct_min: f64,

    /// Minimum required fill percentage for any individual category.
    pub category_fill_pct_min: f64,

    /// Minimum required non-mock evidence coverage percentage.
    pub evidence_coverage_pct_min: f64,

    /// Minimum required number of invariants per category.
    pub min_invariants_per_category: usize,

    /// Minimum required number of property-based tests globally.
    pub min_property_tests_global: usize,

    /// Categories that are critical: failure here is always blocking.
    pub critical_categories: Vec<FeatureCategory>,
}

impl Default for CoverageThresholds {
    fn default() -> Self {
        Self {
            global_fill_pct_min: 0.60,
            category_fill_pct_min: 0.30,
            evidence_coverage_pct_min: 0.50,
            min_invariants_per_category: 2,
            min_property_tests_global: 5,
            critical_categories: vec![
                FeatureCategory::SqlGrammar,
                FeatureCategory::VdbeOpcodes,
                FeatureCategory::StorageTransaction,
            ],
        }
    }
}

impl CoverageThresholds {
    /// Strict thresholds for release readiness.
    #[must_use]
    pub fn strict() -> Self {
        Self {
            global_fill_pct_min: 0.80,
            category_fill_pct_min: 0.50,
            evidence_coverage_pct_min: 0.70,
            min_invariants_per_category: 5,
            min_property_tests_global: 10,
            critical_categories: vec![
                FeatureCategory::SqlGrammar,
                FeatureCategory::VdbeOpcodes,
                FeatureCategory::StorageTransaction,
                FeatureCategory::BuiltinFunctions,
            ],
        }
    }

    /// Lenient thresholds for early development.
    #[must_use]
    pub fn lenient() -> Self {
        Self {
            global_fill_pct_min: 0.30,
            category_fill_pct_min: 0.10,
            evidence_coverage_pct_min: 0.20,
            min_invariants_per_category: 1,
            min_property_tests_global: 1,
            critical_categories: vec![FeatureCategory::StorageTransaction],
        }
    }
}

// ---------------------------------------------------------------------------
// Gate violations
// ---------------------------------------------------------------------------

/// Severity of a coverage gate violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationSeverity {
    /// Informational: logged but does not block.
    Info,
    /// Warning: logged and tracked but does not block.
    Warning,
    /// Blocking: prevents merge / release.
    Blocking,
}

impl fmt::Display for ViolationSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Blocking => write!(f, "blocking"),
        }
    }
}

/// A single threshold violation detected by the coverage gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoverageViolation {
    /// Which threshold was breached.
    pub check: String,
    /// Severity of the violation.
    pub severity: ViolationSeverity,
    /// Category this violation relates to (if per-category).
    pub category: Option<String>,
    /// Observed value (human-readable).
    pub observed: String,
    /// Required threshold (human-readable).
    pub required: String,
    /// Actionable remediation instruction.
    pub remediation: String,
}

// ---------------------------------------------------------------------------
// Per-category result
// ---------------------------------------------------------------------------

/// Coverage result for a single taxonomy category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryCoverageResult {
    /// Feature category.
    pub category: FeatureCategory,
    /// Display name.
    pub display_name: String,
    /// Fill percentage (from unit matrix).
    pub fill_pct: f64,
    /// Number of tests in this category.
    pub test_count: usize,
    /// Number of invariants covered.
    pub invariant_count: usize,
    /// Number of property-based tests.
    pub property_test_count: usize,
    /// Whether this category is critical.
    pub is_critical: bool,
    /// Whether this category passes all thresholds.
    pub passes: bool,
    /// Missing coverage areas.
    pub missing_areas: Vec<String>,
}

// ---------------------------------------------------------------------------
// Gate report
// ---------------------------------------------------------------------------

/// Overall verdict of the coverage gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageVerdict {
    /// All thresholds met.
    Pass,
    /// One or more blocking violations.
    Fail,
    /// No blocking violations but warnings present.
    PassWithWarnings,
}

impl fmt::Display for CoverageVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "PASS"),
            Self::Fail => write!(f, "FAIL"),
            Self::PassWithWarnings => write!(f, "PASS_WITH_WARNINGS"),
        }
    }
}

/// Complete report produced by the coverage gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageGateReport {
    /// Schema version.
    pub schema_version: String,
    /// Bead ID.
    pub bead_id: String,
    /// Overall verdict.
    pub verdict: CoverageVerdict,
    /// Total tests in the matrix.
    pub total_tests: usize,
    /// Total invariants across all tests.
    pub total_invariants: usize,
    /// Global fill percentage (weighted average).
    pub global_fill_pct: f64,
    /// Evidence coverage percentage (from no-mock map).
    pub evidence_coverage_pct: f64,
    /// Total property-based tests.
    pub property_test_count: usize,
    /// Per-category results.
    pub categories: Vec<CategoryCoverageResult>,
    /// All violations found.
    pub violations: Vec<CoverageViolation>,
    /// Blocking violation count.
    pub blocking_count: usize,
    /// Warning count.
    pub warning_count: usize,
    /// Human-readable summary.
    pub summary: String,
}

impl CoverageGateReport {
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

    /// Render a human-readable markdown summary.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# Coverage Gate Report");
        let _ = writeln!(out);
        let _ = writeln!(out, "**Verdict**: {}", self.verdict);
        let _ = writeln!(
            out,
            "**Global Fill**: {:.1}% | **Evidence Coverage**: {:.1}%",
            self.global_fill_pct * 100.0,
            self.evidence_coverage_pct * 100.0,
        );
        let _ = writeln!(
            out,
            "**Tests**: {} | **Invariants**: {} | **Property Tests**: {}",
            self.total_tests, self.total_invariants, self.property_test_count,
        );
        let _ = writeln!(out);

        // Category table.
        let _ = writeln!(out, "## Per-Category Results");
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "| Category | Fill % | Tests | Invariants | Critical | Status |"
        );
        let _ = writeln!(
            out,
            "|----------|--------|-------|------------|----------|--------|"
        );
        for cat in &self.categories {
            let status = if cat.passes { "PASS" } else { "FAIL" };
            let crit = if cat.is_critical { "yes" } else { "no" };
            let _ = writeln!(
                out,
                "| {} | {:.1}% | {} | {} | {} | {} |",
                cat.display_name,
                cat.fill_pct * 100.0,
                cat.test_count,
                cat.invariant_count,
                crit,
                status,
            );
        }
        let _ = writeln!(out);

        if !self.violations.is_empty() {
            let _ = writeln!(
                out,
                "## Violations ({} blocking, {} warnings)",
                self.blocking_count, self.warning_count
            );
            let _ = writeln!(out);
            for v in &self.violations {
                let cat_str = v.category.as_deref().unwrap_or("global");
                let _ = writeln!(
                    out,
                    "- **[{}]** `{}` ({cat_str}): observed={}, required={}",
                    v.severity, v.check, v.observed, v.required,
                );
                let _ = writeln!(out, "  - Remediation: {}", v.remediation);
            }
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Gate evaluation
// ---------------------------------------------------------------------------

/// Run the coverage gate against the canonical matrix and evidence map.
///
/// Returns a complete [`CoverageGateReport`] with per-category results
/// and actionable violations.
#[must_use]
pub fn evaluate_coverage_gate(thresholds: &CoverageThresholds) -> CoverageGateReport {
    let matrix = build_canonical_matrix();
    let evidence_map = build_evidence_map();
    evaluate_coverage_gate_with(thresholds, &matrix, &evidence_map)
}

/// Run the coverage gate against provided matrix and evidence map.
///
/// This variant is useful for testing with synthetic data.
#[must_use]
pub fn evaluate_coverage_gate_with(
    thresholds: &CoverageThresholds,
    matrix: &UnitMatrix,
    evidence_map: &NoMockEvidenceMap,
) -> CoverageGateReport {
    let mut violations = Vec::new();
    let mut categories = Vec::new();

    // Compute global metrics.
    let total_tests = matrix.tests.len();
    let total_invariants: usize = matrix.tests.iter().map(|t| t.invariants.len()).sum();
    let property_test_count: usize = matrix.tests.iter().filter(|t| t.property_based).count();

    // Weighted global fill percentage.
    let global_fill_pct = if matrix.coverage.is_empty() {
        0.0
    } else {
        let total_weight: usize = matrix.coverage.iter().map(|b| b.test_count.max(1)).sum();
        let weighted_sum: f64 = matrix
            .coverage
            .iter()
            .map(|b| b.fill_pct * (b.test_count.max(1) as f64))
            .sum();
        if total_weight == 0 {
            0.0
        } else {
            weighted_sum / total_weight as f64
        }
    };

    let evidence_coverage_pct = evidence_map.stats.coverage_pct;

    // Check global fill threshold.
    if global_fill_pct < thresholds.global_fill_pct_min {
        violations.push(CoverageViolation {
            check: "global_fill_pct".to_owned(),
            severity: ViolationSeverity::Blocking,
            category: None,
            observed: format!("{:.1}%", global_fill_pct * 100.0),
            required: format!("{:.1}%", thresholds.global_fill_pct_min * 100.0),
            remediation: "Add more unit tests to increase overall coverage fill percentage."
                .to_owned(),
        });
    }

    // Check evidence coverage threshold.
    if evidence_coverage_pct < thresholds.evidence_coverage_pct_min {
        violations.push(CoverageViolation {
            check: "evidence_coverage_pct".to_owned(),
            severity: ViolationSeverity::Blocking,
            category: None,
            observed: format!("{:.1}%", evidence_coverage_pct * 100.0),
            required: format!("{:.1}%", thresholds.evidence_coverage_pct_min * 100.0),
            remediation:
                "Add no-mock evidence entries for critical invariants in no_mock_evidence.rs."
                    .to_owned(),
        });
    }

    // Check property test count.
    if property_test_count < thresholds.min_property_tests_global {
        violations.push(CoverageViolation {
            check: "min_property_tests_global".to_owned(),
            severity: ViolationSeverity::Warning,
            category: None,
            observed: format!("{property_test_count}"),
            required: format!("{}", thresholds.min_property_tests_global),
            remediation: "Add property-based tests (proptest) to improve coverage breadth."
                .to_owned(),
        });
    }

    // Per-category evaluation.
    for bucket in &matrix.coverage {
        let is_critical = thresholds.critical_categories.contains(&bucket.category);

        let mut cat_passes = true;

        // Check category fill percentage.
        if bucket.fill_pct < thresholds.category_fill_pct_min {
            let severity = if is_critical {
                ViolationSeverity::Blocking
            } else {
                ViolationSeverity::Warning
            };
            violations.push(CoverageViolation {
                check: "category_fill_pct".to_owned(),
                severity,
                category: Some(bucket.category.display_name().to_owned()),
                observed: format!("{:.1}%", bucket.fill_pct * 100.0),
                required: format!("{:.1}%", thresholds.category_fill_pct_min * 100.0),
                remediation: format!(
                    "Add tests for category '{}'. Missing areas: {}",
                    bucket.category.display_name(),
                    if bucket.missing_coverage.is_empty() {
                        "(none listed)".to_owned()
                    } else {
                        bucket.missing_coverage.join(", ")
                    },
                ),
            });
            if severity == ViolationSeverity::Blocking {
                cat_passes = false;
            }
        }

        // Check invariant count.
        if bucket.invariant_count < thresholds.min_invariants_per_category {
            let severity = if is_critical {
                ViolationSeverity::Blocking
            } else {
                ViolationSeverity::Info
            };
            violations.push(CoverageViolation {
                check: "min_invariants_per_category".to_owned(),
                severity,
                category: Some(bucket.category.display_name().to_owned()),
                observed: format!("{}", bucket.invariant_count),
                required: format!("{}", thresholds.min_invariants_per_category),
                remediation: format!(
                    "Add more invariant assertions to tests in category '{}'.",
                    bucket.category.display_name(),
                ),
            });
            if severity == ViolationSeverity::Blocking {
                cat_passes = false;
            }
        }

        categories.push(CategoryCoverageResult {
            category: bucket.category,
            display_name: bucket.category.display_name().to_owned(),
            fill_pct: bucket.fill_pct,
            test_count: bucket.test_count,
            invariant_count: bucket.invariant_count,
            property_test_count: bucket.property_test_count,
            is_critical,
            passes: cat_passes,
            missing_areas: bucket.missing_coverage.clone(),
        });
    }

    let blocking_count = violations
        .iter()
        .filter(|v| v.severity == ViolationSeverity::Blocking)
        .count();
    let warning_count = violations
        .iter()
        .filter(|v| v.severity == ViolationSeverity::Warning)
        .count();

    let verdict = if blocking_count > 0 {
        CoverageVerdict::Fail
    } else if warning_count > 0 {
        CoverageVerdict::PassWithWarnings
    } else {
        CoverageVerdict::Pass
    };

    let summary = format!(
        "Coverage gate {verdict}: {total_tests} tests, {total_invariants} invariants, \
         global fill {:.1}%, evidence {:.1}%, {blocking_count} blocking, {warning_count} warnings",
        global_fill_pct * 100.0,
        evidence_coverage_pct * 100.0,
    );

    CoverageGateReport {
        schema_version: COVERAGE_GATE_SCHEMA_VERSION.to_owned(),
        bead_id: BEAD_ID.to_owned(),
        verdict,
        total_tests,
        total_invariants,
        global_fill_pct,
        evidence_coverage_pct,
        property_test_count,
        categories,
        violations,
        blocking_count,
        warning_count,
        summary,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn run_default_gate() -> CoverageGateReport {
        evaluate_coverage_gate(&CoverageThresholds::default())
    }

    fn run_lenient_gate() -> CoverageGateReport {
        evaluate_coverage_gate(&CoverageThresholds::lenient())
    }

    #[test]
    fn default_thresholds_reasonable() {
        let t = CoverageThresholds::default();
        assert!(t.global_fill_pct_min > 0.0 && t.global_fill_pct_min <= 1.0);
        assert!(t.category_fill_pct_min > 0.0 && t.category_fill_pct_min <= 1.0);
        assert!(t.evidence_coverage_pct_min > 0.0 && t.evidence_coverage_pct_min <= 1.0);
        assert!(t.min_invariants_per_category > 0);
        assert!(t.min_property_tests_global > 0);
        assert!(!t.critical_categories.is_empty());
    }

    #[test]
    fn strict_thresholds_higher_than_default() {
        let d = CoverageThresholds::default();
        let s = CoverageThresholds::strict();
        assert!(s.global_fill_pct_min >= d.global_fill_pct_min);
        assert!(s.category_fill_pct_min >= d.category_fill_pct_min);
        assert!(s.evidence_coverage_pct_min >= d.evidence_coverage_pct_min);
        assert!(s.min_invariants_per_category >= d.min_invariants_per_category);
        assert!(s.critical_categories.len() >= d.critical_categories.len());
    }

    #[test]
    fn lenient_thresholds_lower_than_default() {
        let d = CoverageThresholds::default();
        let l = CoverageThresholds::lenient();
        assert!(l.global_fill_pct_min <= d.global_fill_pct_min);
        assert!(l.category_fill_pct_min <= d.category_fill_pct_min);
        assert!(l.evidence_coverage_pct_min <= d.evidence_coverage_pct_min);
    }

    #[test]
    fn gate_report_has_all_categories() {
        let report = run_default_gate();
        assert_eq!(report.categories.len(), FeatureCategory::ALL.len());
    }

    #[test]
    fn gate_report_global_metrics_non_negative() {
        let report = run_default_gate();
        assert!(report.global_fill_pct >= 0.0);
        assert!(report.evidence_coverage_pct >= 0.0);
        assert!(report.total_tests > 0);
        assert!(report.total_invariants > 0);
    }

    #[test]
    fn gate_report_json_roundtrip() {
        let report = run_default_gate();
        let json = report.to_json().expect("serialize");
        let restored = CoverageGateReport::from_json(&json).expect("deserialize");
        assert_eq!(restored.verdict, report.verdict);
        assert_eq!(restored.total_tests, report.total_tests);
        assert_eq!(restored.blocking_count, report.blocking_count);
        assert_eq!(restored.categories.len(), report.categories.len());
    }

    #[test]
    fn verdict_display() {
        assert_eq!(format!("{}", CoverageVerdict::Pass), "PASS");
        assert_eq!(format!("{}", CoverageVerdict::Fail), "FAIL");
        assert_eq!(
            format!("{}", CoverageVerdict::PassWithWarnings),
            "PASS_WITH_WARNINGS"
        );
    }

    #[test]
    fn severity_display() {
        assert_eq!(format!("{}", ViolationSeverity::Info), "info");
        assert_eq!(format!("{}", ViolationSeverity::Warning), "warning");
        assert_eq!(format!("{}", ViolationSeverity::Blocking), "blocking");
    }

    #[test]
    fn severity_ordering() {
        assert!(ViolationSeverity::Info < ViolationSeverity::Warning);
        assert!(ViolationSeverity::Warning < ViolationSeverity::Blocking);
    }

    #[test]
    fn summary_contains_verdict() {
        let report = run_default_gate();
        assert!(
            report.summary.contains("PASS") || report.summary.contains("FAIL"),
            "summary should contain verdict: {}",
            report.summary,
        );
    }

    #[test]
    fn render_summary_contains_category_table() {
        let report = run_default_gate();
        let md = report.render_summary();
        assert!(
            md.contains("Per-Category Results"),
            "missing category section"
        );
        assert!(md.contains("Fill %"), "missing table header");
        // Check at least one category appears.
        assert!(
            md.contains("SQL Grammar") || md.contains("VDBE Opcodes"),
            "no category names in summary",
        );
    }

    #[test]
    fn lenient_gate_passes_canonical_data() {
        // The canonical matrix/evidence should pass lenient thresholds.
        let report = run_lenient_gate();
        assert_eq!(
            report.verdict,
            CoverageVerdict::Pass,
            "lenient gate should pass: violations={:?}",
            report
                .violations
                .iter()
                .filter(|v| v.severity == ViolationSeverity::Blocking)
                .map(|v| format!(
                    "{}: {} (obs={}, req={})",
                    v.check,
                    v.category.as_deref().unwrap_or("global"),
                    v.observed,
                    v.required
                ))
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn critical_category_failure_is_blocking() {
        // With thresholds that set fill_pct_min very high, critical category
        // failures should be blocking.
        let t = CoverageThresholds {
            category_fill_pct_min: 0.999,
            ..CoverageThresholds::default()
        };
        let report = evaluate_coverage_gate(&t);
        let blocking_cats: Vec<_> = report
            .violations
            .iter()
            .filter(|v| v.severity == ViolationSeverity::Blocking && v.check == "category_fill_pct")
            .filter_map(|v| v.category.as_deref())
            .collect();
        // At least one critical category should be blocking.
        let critical_names: Vec<_> = t
            .critical_categories
            .iter()
            .map(|c| c.display_name())
            .collect();
        let any_critical_blocked = blocking_cats
            .iter()
            .any(|name| critical_names.contains(name));
        assert!(
            any_critical_blocked,
            "expected at least one critical category to be blocking",
        );
    }

    #[test]
    fn non_critical_category_failure_is_warning() {
        // Extensions is not in default critical categories; its failure
        // should be a warning, not blocking.
        let t = CoverageThresholds {
            category_fill_pct_min: 0.999,
            ..CoverageThresholds::default()
        };
        let report = evaluate_coverage_gate(&t);
        let ext_violations: Vec<_> = report
            .violations
            .iter()
            .filter(|v| {
                v.category.as_deref() == Some("Extensions") && v.check == "category_fill_pct"
            })
            .collect();
        for v in &ext_violations {
            assert_eq!(
                v.severity,
                ViolationSeverity::Warning,
                "Extensions category failure should be warning, not {:?}",
                v.severity,
            );
        }
    }

    #[test]
    fn zero_threshold_always_passes() {
        let t = CoverageThresholds {
            global_fill_pct_min: 0.0,
            category_fill_pct_min: 0.0,
            evidence_coverage_pct_min: 0.0,
            min_invariants_per_category: 0,
            min_property_tests_global: 0,
            critical_categories: vec![],
        };
        let report = evaluate_coverage_gate(&t);
        assert_eq!(report.blocking_count, 0);
        assert!(
            report.verdict == CoverageVerdict::Pass
                || report.verdict == CoverageVerdict::PassWithWarnings,
        );
    }

    #[test]
    fn category_results_have_display_names() {
        let report = run_default_gate();
        for cat in &report.categories {
            assert!(
                !cat.display_name.is_empty(),
                "empty display name for {:?}",
                cat.category
            );
        }
    }

    #[test]
    fn violations_have_remediation() {
        let report = run_default_gate();
        for v in &report.violations {
            assert!(
                !v.remediation.is_empty(),
                "violation {:?} missing remediation",
                v.check
            );
        }
    }

    #[test]
    fn gate_is_deterministic() {
        let r1 = run_default_gate();
        let r2 = run_default_gate();
        assert_eq!(r1.verdict, r2.verdict);
        assert_eq!(r1.total_tests, r2.total_tests);
        assert_eq!(r1.total_invariants, r2.total_invariants);
        assert_eq!(r1.blocking_count, r2.blocking_count);
        assert_eq!(r1.categories.len(), r2.categories.len());
    }

    #[test]
    fn schema_version_set() {
        let report = run_default_gate();
        assert_eq!(report.schema_version, COVERAGE_GATE_SCHEMA_VERSION);
    }

    #[test]
    fn bead_id_set() {
        let report = run_default_gate();
        assert_eq!(report.bead_id, BEAD_ID);
    }
}
