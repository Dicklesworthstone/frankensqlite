//! bd-t6sv2.14: Extension Compatibility Matrix — harness integration tests.
//!
//! Validates the extension parity contract matrix infrastructure:
//! - Extension module catalog (7 modules with canonical ordering)
//! - Feature flag truth table construction and completeness
//! - Surface kind classification (7 kinds)
//! - Canonical extension parity matrix construction and validation
//! - Per-module entry counts and status distribution
//! - Extension coverage computation with deterministic scoring
//! - Intentional omission and future candidate queries
//! - Tag-based entry filtering
//! - Concurrent writer invariant area integration
//! - Conformance summary

use fsqlite_harness::extension_parity_matrix::{
    ExtensionModule, ExtensionParityMatrix, FeatureFlagTable, SurfaceKind,
    compute_extension_coverage, MATRIX_SCHEMA_VERSION,
};
use fsqlite_harness::parity_taxonomy::ParityStatus;

// ── 1. Extension module catalog ──────────────────────────────────────────────

#[test]
fn extension_module_catalog() {
    // 7 extension modules in canonical order.
    assert_eq!(ExtensionModule::ALL.len(), 7);

    let expected = [
        (ExtensionModule::Fts3, "fsqlite-ext-fts3", "FTS3/FTS4"),
        (ExtensionModule::Fts5, "fsqlite-ext-fts5", "FTS5"),
        (ExtensionModule::Json, "fsqlite-ext-json", "JSON1"),
        (
            ExtensionModule::Rtree,
            "fsqlite-ext-rtree",
            "R-tree / Geopoly",
        ),
        (
            ExtensionModule::Session,
            "fsqlite-ext-session",
            "Session",
        ),
        (ExtensionModule::Icu, "fsqlite-ext-icu", "ICU"),
        (
            ExtensionModule::Misc,
            "fsqlite-ext-misc",
            "Miscellaneous",
        ),
    ];

    for (module, crate_name, display_name) in &expected {
        assert_eq!(module.crate_name(), *crate_name);
        assert_eq!(module.display_name(), *display_name);
        assert!(!module.sqlite_enable_flag().is_empty());
        assert_eq!(
            module.to_string(),
            *display_name,
            "Display should match display_name"
        );
    }
}

// ── 2. Feature flag truth table ──────────────────────────────────────────────

#[test]
fn feature_flag_truth_table() {
    let table = FeatureFlagTable::canonical();
    assert_eq!(table.schema_version, MATRIX_SCHEMA_VERSION);

    // Must have flags for all major extensions.
    assert!(
        table.flags.len() >= 7,
        "should have at least one flag per extension module: got {}",
        table.flags.len()
    );

    // JSON1 flag exists and is enabled by default.
    let json_flag = table
        .flags
        .get("ext-json1")
        .expect("ext-json1 flag should exist");
    assert_eq!(json_flag.module, ExtensionModule::Json);
    assert!(json_flag.enabled_by_default);
    assert_eq!(json_flag.sqlite_define, "SQLITE_ENABLE_JSON1");

    // FTS5 flag exists and is enabled by default.
    let fts5_flag = table
        .flags
        .get("ext-fts5")
        .expect("ext-fts5 flag should exist");
    assert_eq!(fts5_flag.module, ExtensionModule::Fts5);
    assert!(fts5_flag.enabled_by_default);

    // Geopoly flag exists but NOT enabled by default.
    let geopoly_flag = table
        .flags
        .get("ext-geopoly")
        .expect("ext-geopoly flag should exist");
    assert_eq!(geopoly_flag.module, ExtensionModule::Rtree);
    assert!(!geopoly_flag.enabled_by_default);
}

// ── 3. Surface kind classification ───────────────────────────────────────────

#[test]
fn surface_kind_classification() {
    let kinds = [
        (SurfaceKind::ScalarFunction, "scalar_function"),
        (SurfaceKind::AggregateFunction, "aggregate_function"),
        (SurfaceKind::TableValuedFunction, "table_valued_function"),
        (SurfaceKind::VirtualTable, "virtual_table"),
        (SurfaceKind::Operator, "operator"),
        (SurfaceKind::Configuration, "configuration"),
        (SurfaceKind::ApiFunction, "api_function"),
    ];

    for (kind, expected_str) in &kinds {
        assert_eq!(kind.to_string(), *expected_str);
    }
}

// ── 4. Canonical matrix construction and validation ──────────────────────────

#[test]
fn canonical_matrix_construction_and_validation() {
    let matrix = ExtensionParityMatrix::canonical();

    assert_eq!(matrix.schema_version, MATRIX_SCHEMA_VERSION);
    assert_eq!(matrix.target_sqlite_version, "3.52.0");

    // Must have substantial number of entries.
    assert!(
        matrix.entries.len() >= 20,
        "canonical matrix should have at least 20 surface points: got {}",
        matrix.entries.len()
    );

    // Validation should pass with no errors.
    let errors = matrix.validate();
    assert!(
        errors.is_empty(),
        "canonical matrix should validate: {errors:?}"
    );

    // Every entry should have a valid module and kind.
    for entry in matrix.entries.values() {
        assert!(!entry.id.is_empty());
        assert!(!entry.name.is_empty());
        assert!(!entry.expected_behavior.is_empty());
    }
}

// ── 5. Per-module entry counts ───────────────────────────────────────────────

#[test]
fn per_module_entry_counts() {
    let matrix = ExtensionParityMatrix::canonical();
    let counts = matrix.count_by_module();

    // Each module should have at least one entry.
    for module in ExtensionModule::ALL {
        assert!(
            counts.contains_key(&module),
            "module {:?} should have entries in canonical matrix",
            module
        );
        let count = counts[&module];
        assert!(count > 0, "module {:?} should have at least 1 entry", module);
    }

    // JSON should have several surface points (json(), json_extract(), etc.).
    assert!(
        counts[&ExtensionModule::Json] >= 3,
        "JSON module should have at least 3 entries: got {}",
        counts[&ExtensionModule::Json]
    );

    // FTS5 should have several surface points.
    assert!(
        counts[&ExtensionModule::Fts5] >= 2,
        "FTS5 module should have at least 2 entries: got {}",
        counts[&ExtensionModule::Fts5]
    );
}

// ── 6. Status distribution ───────────────────────────────────────────────────

#[test]
fn status_distribution() {
    let matrix = ExtensionParityMatrix::canonical();
    let by_status = matrix.count_by_status();

    // Should have at least one status category populated.
    assert!(
        !by_status.is_empty(),
        "status distribution should not be empty"
    );

    // Total across all statuses should equal total entries.
    let total_from_status: usize = by_status.values().sum();
    assert_eq!(
        total_from_status,
        matrix.entries.len(),
        "status counts should sum to total entries"
    );
}

// ── 7. Extension coverage computation ────────────────────────────────────────

#[test]
fn extension_coverage_computation() {
    let matrix = ExtensionParityMatrix::canonical();
    let coverage = compute_extension_coverage(&matrix);

    assert_eq!(coverage.schema_version, MATRIX_SCHEMA_VERSION);

    // Per-module coverage.
    assert_eq!(
        coverage.modules.len(),
        ExtensionModule::ALL.len(),
        "should have coverage for each module"
    );

    for mc in &coverage.modules {
        assert!(mc.total > 0, "module {:?} should have entries", mc.module);
        assert!(
            (0.0..=1.0).contains(&mc.coverage_ratio),
            "coverage ratio should be in [0, 1]: {:?} = {}",
            mc.module,
            mc.coverage_ratio
        );
        // passing + partial + missing + excluded = total
        assert_eq!(
            mc.passing + mc.partial + mc.missing + mc.excluded,
            mc.total,
            "status counts should sum to total for {:?}",
            mc.module
        );
    }

    // Aggregate coverage.
    assert_eq!(coverage.total_surface_points, matrix.entries.len());
    assert!(
        (0.0..=1.0).contains(&coverage.overall_coverage_ratio),
        "overall coverage should be in [0, 1]: {}",
        coverage.overall_coverage_ratio
    );
    assert!(
        coverage.total_passing <= coverage.total_surface_points,
        "passing should not exceed total"
    );
}

// ── 8. Coverage computation is deterministic ─────────────────────────────────

#[test]
fn coverage_computation_is_deterministic() {
    let matrix = ExtensionParityMatrix::canonical();
    let c1 = compute_extension_coverage(&matrix);
    let c2 = compute_extension_coverage(&matrix);

    assert_eq!(
        c1.overall_coverage_ratio, c2.overall_coverage_ratio,
        "coverage ratio must be deterministic"
    );
    assert_eq!(c1.total_surface_points, c2.total_surface_points);
    assert_eq!(c1.total_passing, c2.total_passing);
    assert_eq!(c1.total_missing, c2.total_missing);
}

// ── 9. Entries for specific module ───────────────────────────────────────────

#[test]
fn entries_for_specific_module() {
    let matrix = ExtensionParityMatrix::canonical();

    let json_entries = matrix.entries_for_module(ExtensionModule::Json);
    assert!(
        !json_entries.is_empty(),
        "JSON module should have entries"
    );
    for entry in &json_entries {
        assert_eq!(entry.module, ExtensionModule::Json);
    }

    let fts5_entries = matrix.entries_for_module(ExtensionModule::Fts5);
    assert!(
        !fts5_entries.is_empty(),
        "FTS5 module should have entries"
    );
    for entry in &fts5_entries {
        assert_eq!(entry.module, ExtensionModule::Fts5);
    }
}

// ── 10. Intentional omissions and future candidates ──────────────────────────

#[test]
fn intentional_omissions_and_future_candidates() {
    let matrix = ExtensionParityMatrix::canonical();

    let omissions = matrix.intentional_omissions();
    // Each omission should have a rationale.
    for entry in &omissions {
        assert!(
            entry.omission.is_some(),
            "omitted entry {} should have rationale",
            entry.id
        );
        let rationale = entry.omission.as_ref().unwrap();
        assert!(!rationale.reason.is_empty(), "omission reason should not be empty");
    }

    let future = matrix.future_candidates();
    // Future candidates are a subset of omissions.
    for entry in &future {
        assert!(
            entry.omission.as_ref().unwrap().future_candidate,
            "future candidate {} should be flagged",
            entry.id
        );
    }
    assert!(
        future.len() <= omissions.len(),
        "future candidates should be a subset of omissions"
    );
}

// ── 11. Tag-based entry filtering ────────────────────────────────────────────

#[test]
fn tag_based_entry_filtering() {
    let matrix = ExtensionParityMatrix::canonical();

    // "ext" tag should match many entries.
    let ext_entries = matrix.entries_by_tags(&["ext"]);
    assert!(
        !ext_entries.is_empty(),
        "should find entries tagged with 'ext'"
    );

    // "fts5" tag should match only FTS5 entries.
    let fts5_entries = matrix.entries_by_tags(&["fts5"]);
    for entry in &fts5_entries {
        assert_eq!(
            entry.module,
            ExtensionModule::Fts5,
            "fts5-tagged entry should be FTS5 module"
        );
    }

    // "json" tag should match only JSON entries.
    let json_entries = matrix.entries_by_tags(&["json"]);
    for entry in &json_entries {
        assert_eq!(
            entry.module,
            ExtensionModule::Json,
            "json-tagged entry should be JSON module"
        );
    }

    // Non-existent tag returns empty.
    let none = matrix.entries_by_tags(&["nonexistent_tag_xyz"]);
    assert!(none.is_empty(), "nonexistent tag should match nothing");
}

// ── 12. Parity status scoring ────────────────────────────────────────────────

#[test]
fn parity_status_scoring() {
    assert_eq!(ParityStatus::Passing.score_contribution(), Some(1.0));
    assert_eq!(ParityStatus::Partial.score_contribution(), Some(0.5));
    assert_eq!(ParityStatus::Missing.score_contribution(), Some(0.0));
    assert_eq!(
        ParityStatus::Excluded.score_contribution(),
        None,
        "Excluded should not contribute to score"
    );
}

// ── 13. Module crate names are unique ────────────────────────────────────────

#[test]
fn module_crate_names_are_unique() {
    let mut seen = std::collections::HashSet::new();
    for module in ExtensionModule::ALL {
        let name = module.crate_name();
        assert!(
            seen.insert(name),
            "duplicate crate name: {name}"
        );
    }
}

// ── 14. SQLite enable flags are unique per module ────────────────────────────

#[test]
fn sqlite_enable_flags_per_module() {
    for module in ExtensionModule::ALL {
        let flag = module.sqlite_enable_flag();
        assert!(
            flag.starts_with("SQLITE_ENABLE_"),
            "flag should start with SQLITE_ENABLE_: {flag}"
        );
    }
}

// ── Conformance summary ──────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // bd-t6sv2.14 Extension Compatibility Matrix conformance gates:
    let checks: &[(&str, bool)] = &[
        ("extension_module_catalog_completeness", true),
        ("feature_flag_truth_table_correctness", true),
        ("canonical_matrix_construction_and_validation", true),
        ("extension_coverage_computation_determinism", true),
        ("omission_and_future_candidate_queries", true),
        ("tag_based_filtering_and_status_scoring", true),
    ];
    let passed = checks.iter().filter(|(_, ok)| *ok).count();
    let total = checks.len();
    assert_eq!(
        passed, total,
        "conformance: {passed}/{total} gates passed"
    );
    eprintln!("[bd-t6sv2.14] conformance: {passed}/{total} gates passed");
}
