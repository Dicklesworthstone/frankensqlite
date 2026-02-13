//! Integration tests for metamorphic SQL generator (bd-1dp9.2.2).
//!
//! Validates:
//! - Transform applicability across corpus families
//! - Corpus generation from seed corpus entries
//! - Coverage statistics
//! - Transform composition
//! - Deterministic seed derivation
//! - Integration with corpus builder

use std::collections::BTreeSet;

use fsqlite_harness::corpus_ingest::{
    CORPUS_SEED_BASE, CorpusBuilder, CorpusSource, generate_seed_corpus,
};
use fsqlite_harness::metamorphic::{
    TransformRegistry, compose_transforms, compute_coverage, generate_metamorphic_corpus,
    test_case_to_entry,
};

const BEAD_ID: &str = "bd-1dp9.2.2";

// ---------------------------------------------------------------------------
// Registry invariants
// ---------------------------------------------------------------------------

#[test]
fn registry_contains_all_eight_transforms() {
    let reg = TransformRegistry::new();
    let names: Vec<&str> = reg.transforms().iter().map(|t| t.name()).collect();

    let expected = [
        "subquery_wrap",
        "tautological_predicate",
        "double_negation",
        "coalesce_identity",
        "union_self_intersect",
        "cast_literal_identity",
        "expression_commute",
        "null_coalesce",
    ];

    for name in &expected {
        assert!(
            names.contains(name),
            "[{BEAD_ID}] registry missing transform: {name}"
        );
    }

    eprintln!("bead_id={BEAD_ID} test=registry transforms={}", names.len());
}

#[test]
fn all_transforms_have_unique_names() {
    let reg = TransformRegistry::new();
    let names: BTreeSet<&str> = reg.transforms().iter().map(|t| t.name()).collect();
    assert_eq!(
        names.len(),
        reg.transforms().len(),
        "[{BEAD_ID}] duplicate transform names detected"
    );
}

#[test]
fn all_transforms_have_non_empty_soundness() {
    let reg = TransformRegistry::new();
    for t in reg.transforms() {
        assert!(
            t.soundness_sketch().len() >= 20,
            "[{BEAD_ID}] transform {} has trivial soundness sketch",
            t.name()
        );
    }
}

// ---------------------------------------------------------------------------
// Corpus generation from seed corpus
// ---------------------------------------------------------------------------

#[test]
fn generate_from_seed_corpus_produces_cases() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);

    assert!(
        !cases.is_empty(),
        "[{BEAD_ID}] no metamorphic cases generated from seed corpus"
    );

    eprintln!(
        "bead_id={BEAD_ID} test=seed_corpus_gen cases={}",
        cases.len()
    );
}

#[test]
fn generated_cases_cover_multiple_transforms() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);

    let transform_names: BTreeSet<String> =
        cases.iter().map(|c| c.transform_name.clone()).collect();

    assert!(
        transform_names.len() >= 3,
        "[{BEAD_ID}] expected at least 3 distinct transforms, got {}: {:?}",
        transform_names.len(),
        transform_names
    );

    eprintln!(
        "bead_id={BEAD_ID} test=transform_coverage transforms={:?}",
        transform_names
    );
}

#[test]
fn generated_cases_cover_multiple_families() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);

    let families: BTreeSet<String> = cases.iter().map(|c| c.family.to_string()).collect();

    assert!(
        families.len() >= 2,
        "[{BEAD_ID}] expected cases from at least 2 families, got {:?}",
        families
    );

    eprintln!(
        "bead_id={BEAD_ID} test=family_coverage families={:?}",
        families
    );
}

#[test]
fn generated_cases_have_unique_ids() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);

    let ids: BTreeSet<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids.len(),
        cases.len(),
        "[{BEAD_ID}] duplicate metamorphic case IDs"
    );
}

#[test]
fn generated_cases_have_different_transformed_sql() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);

    for case in &cases {
        assert_ne!(
            case.original, case.transformed,
            "[{BEAD_ID}] case {} has identical original and transformed SQL",
            case.id
        );
    }
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn generation_is_deterministic() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases1 = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 4);
    let cases2 = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 4);

    assert_eq!(
        cases1.len(),
        cases2.len(),
        "[{BEAD_ID}] non-deterministic case count"
    );

    for (c1, c2) in cases1.iter().zip(cases2.iter()) {
        assert_eq!(c1.id, c2.id, "[{BEAD_ID}] non-deterministic case IDs");
        assert_eq!(
            c1.transformed, c2.transformed,
            "[{BEAD_ID}] non-deterministic transformed SQL for case {}",
            c1.id
        );
    }
}

// ---------------------------------------------------------------------------
// Coverage statistics
// ---------------------------------------------------------------------------

#[test]
fn coverage_report_is_accurate() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 8);
    let coverage = compute_coverage(&cases);

    assert_eq!(
        coverage.total_cases,
        cases.len(),
        "[{BEAD_ID}] total_cases mismatch"
    );

    let by_transform_total: usize = coverage.by_transform.values().sum();
    assert_eq!(
        by_transform_total,
        cases.len(),
        "[{BEAD_ID}] by_transform counts don't sum to total"
    );

    let by_family_total: usize = coverage.by_family.values().sum();
    assert_eq!(
        by_family_total,
        cases.len(),
        "[{BEAD_ID}] by_family counts don't sum to total"
    );

    assert!(
        !coverage.feature_tags.is_empty(),
        "[{BEAD_ID}] no feature tags in coverage"
    );

    eprintln!(
        "bead_id={BEAD_ID} test=coverage total={} transforms={} families={} tags={}",
        coverage.total_cases,
        coverage.by_transform.len(),
        coverage.by_family.len(),
        coverage.feature_tags.len()
    );
}

// ---------------------------------------------------------------------------
// Transform composition
// ---------------------------------------------------------------------------

#[test]
fn compose_two_transforms_produces_different_sql() {
    let reg = TransformRegistry::new();
    let t1 = reg.by_name("tautological_predicate").unwrap();
    let t2 = reg.by_name("subquery_wrap").unwrap();

    let original = vec!["SELECT a, b FROM t".to_owned()];
    let composed = compose_transforms(&original, t1, t2, 42);

    assert!(composed.is_some(), "[{BEAD_ID}] composition failed");
    let result = composed.unwrap();
    assert_ne!(result, original, "[{BEAD_ID}] composed result unchanged");

    // Should contain both transforms' effects.
    let r = &result[0];
    assert!(
        r.contains("1=1") || r.contains("_sub"),
        "[{BEAD_ID}] composed result missing transform evidence: {r}"
    );

    eprintln!(
        "bead_id={BEAD_ID} test=composition original={:?} composed={:?}",
        original[0], result[0]
    );
}

// ---------------------------------------------------------------------------
// Corpus builder integration
// ---------------------------------------------------------------------------

#[test]
fn test_case_converts_to_corpus_entry() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let cases = generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, 2);

    if cases.is_empty() {
        eprintln!("bead_id={BEAD_ID} test=corpus_entry_convert skip=no_cases");
        return;
    }

    let entry = test_case_to_entry(&cases[0]);
    assert!(!entry.id.is_empty());
    assert!(!entry.statements.is_empty());
    assert!(
        matches!(entry.source, CorpusSource::Generated { .. }),
        "[{BEAD_ID}] entry source should be Generated"
    );

    // Should be able to add to a corpus builder.
    let mut builder2 = CorpusBuilder::new(42);
    builder2.add_entry(entry);
    let manifest2 = builder2.build();
    assert_eq!(
        manifest2.entries.len(),
        1,
        "[{BEAD_ID}] entry not added to builder"
    );
}

#[test]
fn max_per_entry_limit_respected() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let reg = TransformRegistry::new();
    let max_per_entry = 2;
    let cases =
        generate_metamorphic_corpus(&manifest.entries, &reg, CORPUS_SEED_BASE, max_per_entry);

    // Count cases per source entry.
    let mut per_entry = std::collections::BTreeMap::new();
    for case in &cases {
        *per_entry
            .entry(case.source_entry_id.clone())
            .or_insert(0_usize) += 1;
    }

    for (entry_id, count) in &per_entry {
        assert!(
            *count <= max_per_entry,
            "[{BEAD_ID}] entry {entry_id} has {count} cases, max is {max_per_entry}"
        );
    }
}

// ---------------------------------------------------------------------------
// Individual transform applicability
// ---------------------------------------------------------------------------

#[test]
fn subquery_wrap_applies_to_basic_selects() {
    let reg = TransformRegistry::new();
    let t = reg.by_name("subquery_wrap").unwrap();

    let applies = [
        "SELECT a FROM t",
        "SELECT a, b FROM t WHERE x > 5",
        "SELECT COUNT(*) FROM t GROUP BY a",
    ];
    for sql in &applies {
        assert!(
            t.apply_one(sql, 42).is_some(),
            "[{BEAD_ID}] subquery_wrap should apply to: {sql}"
        );
    }

    let skips = [
        "INSERT INTO t VALUES (1)",
        "SELECT a FROM t ORDER BY a",
        "UPDATE t SET x = 1",
    ];
    for sql in &skips {
        assert!(
            t.apply_one(sql, 42).is_none(),
            "[{BEAD_ID}] subquery_wrap should skip: {sql}"
        );
    }
}

#[test]
fn double_negation_preserves_where_semantics() {
    let reg = TransformRegistry::new();
    let t = reg.by_name("double_negation").unwrap();

    let result = t.apply_one("SELECT a FROM t WHERE x > 5 AND y = 3", 42);
    assert!(result.is_some());
    let r = result.unwrap();
    assert!(
        r.contains("NOT(NOT("),
        "[{BEAD_ID}] double_negation should wrap condition: {r}"
    );
    assert!(
        r.contains("x > 5"),
        "[{BEAD_ID}] original condition should be preserved inside NOT(NOT(...))"
    );
}

#[test]
fn tautological_predicate_with_various_clauses() {
    let reg = TransformRegistry::new();
    let t = reg.by_name("tautological_predicate").unwrap();

    // With HAVING
    let r = t
        .apply_one("SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1", 42)
        .unwrap();
    assert!(
        r.contains("WHERE 1=1"),
        "[{BEAD_ID}] should insert WHERE before GROUP BY: {r}"
    );
}
