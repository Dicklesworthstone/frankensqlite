//! bd-19u.3: Provenance semirings for query lineage integration tests.
//!
//! Validates the semiring provenance framework:
//!   1. Semiring zero/one identity laws
//!   2. Semiring absorption (0 * x = 0)
//!   3. Why-provenance extraction from compound expressions
//!   4. How-provenance derivation string
//!   5. Tracker annotate-propagate-result pipeline
//!   6. Tracker disabled mode (zero overhead)
//!   7. Why-not provenance (missing witnesses)
//!   8. Metrics lifecycle (counters increment correctly)
//!   9. Report summary serialization
//!  10. Machine-readable conformance output

use std::collections::BTreeSet;

use fsqlite_mvcc::{
    ProvenanceAnnotation, ProvenanceMode, ProvenanceToken, ProvenanceTracker, TupleId,
    provenance_metrics, reset_provenance_metrics, why_not,
};

// ---------------------------------------------------------------------------
// Test 1: Semiring zero identity laws
// ---------------------------------------------------------------------------

#[test]
fn test_semiring_zero_identity() {
    let a = ProvenanceToken::base(1, 42);

    // 0 + a = a
    assert_eq!(ProvenanceToken::Zero.plus(a.clone()), a);
    // a + 0 = a
    assert_eq!(a.clone().plus(ProvenanceToken::Zero), a);
    // 0 * a = 0
    assert!(ProvenanceToken::Zero.times(a.clone()).is_zero());
    // a * 0 = 0
    assert!(a.times(ProvenanceToken::Zero).is_zero());

    println!("[PASS] semiring zero identity: all 4 laws hold");
}

// ---------------------------------------------------------------------------
// Test 2: Semiring one identity laws
// ---------------------------------------------------------------------------

#[test]
fn test_semiring_one_identity() {
    let a = ProvenanceToken::base(2, 99);

    // 1 * a = a
    assert_eq!(ProvenanceToken::One.times(a.clone()), a);
    // a * 1 = a
    assert_eq!(a.clone().times(ProvenanceToken::One), a);

    println!("[PASS] semiring one identity: both laws hold");
}

// ---------------------------------------------------------------------------
// Test 3: Why-provenance extraction
// ---------------------------------------------------------------------------

#[test]
fn test_why_provenance_extraction() {
    let t1 = ProvenanceToken::base(10, 1);
    let t2 = ProvenanceToken::base(20, 2);
    let t3 = ProvenanceToken::base(10, 3);

    // Simulate: SELECT ... FROM T1 JOIN T2 UNION SELECT ... FROM T3
    let join_result = t1.times(t2);
    let union_result = join_result.plus(t3);

    let why = union_result.why_provenance();
    assert_eq!(why.len(), 3, "should extract 3 base tuples");
    assert!(why.contains(&TupleId::new(10, 1)));
    assert!(why.contains(&TupleId::new(20, 2)));
    assert!(why.contains(&TupleId::new(10, 3)));

    println!("[PASS] why-provenance: extracted 3 contributors from join+union");
}

// ---------------------------------------------------------------------------
// Test 4: How-provenance derivation
// ---------------------------------------------------------------------------

#[test]
fn test_how_provenance_derivation() {
    let t1 = ProvenanceToken::base(1, 10);
    let t2 = ProvenanceToken::base(2, 20);
    let t3 = ProvenanceToken::base(3, 30);

    // join(t1, t2) + t3
    let expr = t1.times(t2).plus(t3);
    let how = expr.how_provenance();

    // Should contain all base tuple references and operators.
    assert!(how.contains("t1:10"), "how should reference t1:10");
    assert!(how.contains("t2:20"), "how should reference t2:20");
    assert!(how.contains("t3:30"), "how should reference t3:30");
    assert!(how.contains("*"), "how should contain multiplication");
    assert!(how.contains("+"), "how should contain addition");

    println!("[PASS] how-provenance: derivation={how}");
}

// ---------------------------------------------------------------------------
// Test 5: Tracker annotate-propagate-result pipeline
// ---------------------------------------------------------------------------

#[test]
fn test_tracker_pipeline() {
    let mut tracker = ProvenanceTracker::new(ProvenanceMode::How, 1, 8);

    // Column from table 100, rowid 42 -> reg 0
    tracker.annotate_base(0, 100, 42);
    // Column from table 200, rowid 99 -> reg 1
    tracker.annotate_base(1, 200, 99);
    // Binary op: reg 2 = f(reg 0, reg 1)
    tracker.propagate_binary(2, 0, 1);
    // Copy: reg 3 = reg 0
    tracker.propagate_copy(3, 0);
    // Result row: output cols [2, 3]
    tracker.record_result_row(&[2, 3]);

    let output = tracker.output_annotations();
    assert_eq!(output.len(), 1, "should have 1 output row");

    let why = output[0].why();
    assert_eq!(why.len(), 2, "output depends on 2 base tuples");
    assert!(why.contains(&TupleId::new(100, 42)));
    assert!(why.contains(&TupleId::new(200, 99)));

    assert!(
        tracker.annotations_propagated() >= 4,
        "at least 4 propagations: 2 annotate + 1 binary + 1 copy"
    );

    println!(
        "[PASS] tracker pipeline: {} output rows, {} propagations",
        output.len(),
        tracker.annotations_propagated()
    );
}

// ---------------------------------------------------------------------------
// Test 6: Tracker disabled mode
// ---------------------------------------------------------------------------

#[test]
fn test_tracker_disabled_zero_overhead() {
    let mut tracker = ProvenanceTracker::new(ProvenanceMode::Disabled, 42, 16);

    tracker.annotate_base(0, 1, 1);
    tracker.annotate_base(1, 2, 2);
    tracker.propagate_binary(2, 0, 1);
    tracker.propagate_copy(3, 0);
    tracker.propagate_union(4, 1);
    tracker.record_result_row(&[0, 1, 2, 3]);

    assert!(
        tracker.output_annotations().is_empty(),
        "disabled mode should collect no output"
    );
    assert_eq!(
        tracker.annotations_propagated(),
        0,
        "disabled mode should count 0 propagations"
    );

    println!("[PASS] tracker disabled mode: zero overhead confirmed");
}

// ---------------------------------------------------------------------------
// Test 7: Why-not provenance
// ---------------------------------------------------------------------------

#[test]
fn test_why_not_provenance() {
    let mut existing = BTreeSet::new();
    existing.insert(TupleId::new(1, 10));
    existing.insert(TupleId::new(1, 20));
    existing.insert(TupleId::new(2, 30));

    // We expected contributions from t(1,10), t(2,30), t(3,50).
    // t(3,50) is missing.
    let expected = vec![
        TupleId::new(1, 10),
        TupleId::new(2, 30),
        TupleId::new(3, 50),
    ];

    let result = why_not(&existing, &expected);
    assert_eq!(result.missing_witnesses.len(), 1, "1 tuple missing");
    assert_eq!(result.missing_witnesses[0], TupleId::new(3, 50));
    assert!(
        result.explanation.contains("Missing 1 base tuple"),
        "explanation should describe missing count"
    );

    // Case 2: all exist => filter explanation.
    let expected_all = vec![TupleId::new(1, 10), TupleId::new(2, 30)];
    let result2 = why_not(&existing, &expected_all);
    assert!(result2.missing_witnesses.is_empty());
    assert!(result2.explanation.contains("filter predicate"));

    println!("[PASS] why-not: 1 missing witness identified, filter case handled");
}

// ---------------------------------------------------------------------------
// Test 8: Metrics lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_lifecycle() {
    reset_provenance_metrics();

    let m0 = provenance_metrics();
    assert_eq!(m0.fsqlite_provenance_annotations_total, 0);
    assert_eq!(m0.fsqlite_provenance_rows_emitted, 0);
    assert_eq!(m0.fsqlite_provenance_queries_total, 0);

    // Create tokens (each base() and plus()/times() call records an annotation).
    let t1 = ProvenanceToken::base(1, 10);
    let t2 = ProvenanceToken::base(2, 20);
    let _join = t1.times(t2);

    let m1 = provenance_metrics();
    assert!(
        m1.fsqlite_provenance_annotations_total >= 3,
        "2 base + 1 times = at least 3 annotations"
    );

    // Create a result row.
    let _ = ProvenanceAnnotation::new(0, ProvenanceToken::One);

    let m2 = provenance_metrics();
    assert!(
        m2.fsqlite_provenance_rows_emitted >= 1,
        "1 row emitted"
    );

    // Query provenance (why/how).
    let t3 = ProvenanceToken::base(1, 30);
    let _ = t3.why_provenance();
    let _ = ProvenanceToken::base(1, 40).how_provenance();

    let m3 = provenance_metrics();
    assert!(
        m3.fsqlite_provenance_queries_total >= 2,
        "at least 2 queries"
    );

    // Serializable.
    let json = serde_json::to_string(&m3).unwrap();
    assert!(json.contains("fsqlite_provenance_annotations_total"));
    assert!(json.contains("fsqlite_provenance_rows_emitted"));
    assert!(json.contains("fsqlite_provenance_queries_total"));

    println!(
        "[PASS] metrics: annotations={} rows={} queries={}",
        m3.fsqlite_provenance_annotations_total,
        m3.fsqlite_provenance_rows_emitted,
        m3.fsqlite_provenance_queries_total
    );
}

// ---------------------------------------------------------------------------
// Test 9: Report summary serialization
// ---------------------------------------------------------------------------

#[test]
fn test_report_serialization() {
    let mut tracker = ProvenanceTracker::new(ProvenanceMode::Why, 777, 4);
    tracker.annotate_base(0, 1, 10);
    tracker.record_result_row(&[0]);
    tracker.annotate_base(0, 1, 20);
    tracker.record_result_row(&[0]);
    tracker.annotate_base(0, 2, 30);
    tracker.record_result_row(&[0]);

    let report = tracker.summary();
    assert_eq!(report.query_id, 777);
    assert_eq!(report.output_rows, 3);
    assert_eq!(report.mode, ProvenanceMode::Why);
    assert!(report.annotations_propagated > 0);

    let json = serde_json::to_string(&report).unwrap();
    assert!(json.contains("\"query_id\":777"));
    assert!(json.contains("\"output_rows\":3"));
    assert!(json.contains("\"mode\":\"Why\""));

    println!(
        "[PASS] report serialization: {} rows, {} propagations",
        report.output_rows, report.annotations_propagated
    );
}

// ---------------------------------------------------------------------------
// Test 10: Conformance summary (JSON)
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
        detail: String,
    }

    let mut results = Vec::new();

    // 1. Semiring laws.
    {
        let a = ProvenanceToken::base(1, 1);
        let pass = ProvenanceToken::Zero.plus(a.clone()) == a
            && a.clone().times(ProvenanceToken::One) == a
            && ProvenanceToken::Zero.times(a.clone()).is_zero();
        results.push(TestResult {
            name: "semiring_laws",
            pass,
            detail: "zero/one identity + absorption".to_string(),
        });
    }

    // 2. Why extraction.
    {
        let join = ProvenanceToken::base(1, 10).times(ProvenanceToken::base(2, 20));
        let why = join.why_provenance();
        let pass = why.len() == 2;
        results.push(TestResult {
            name: "why_extraction",
            pass,
            detail: format!("{} contributors", why.len()),
        });
    }

    // 3. How expression.
    {
        let expr = ProvenanceToken::base(1, 10).times(ProvenanceToken::base(2, 20));
        let how = expr.how_provenance();
        let pass = how.contains("*") && how.contains("t1:10");
        results.push(TestResult {
            name: "how_expression",
            pass,
            detail: how,
        });
    }

    // 4. Tracker pipeline.
    {
        let mut t = ProvenanceTracker::new(ProvenanceMode::How, 1, 4);
        t.annotate_base(0, 1, 10);
        t.record_result_row(&[0]);
        let pass = t.output_annotations().len() == 1;
        results.push(TestResult {
            name: "tracker_pipeline",
            pass,
            detail: format!("{} output rows", t.output_annotations().len()),
        });
    }

    // 5. Why-not.
    {
        let mut existing = BTreeSet::new();
        existing.insert(TupleId::new(1, 10));
        let result = why_not(&existing, &[TupleId::new(1, 10), TupleId::new(2, 20)]);
        let pass = result.missing_witnesses.len() == 1;
        results.push(TestResult {
            name: "why_not",
            pass,
            detail: format!("{} missing", result.missing_witnesses.len()),
        });
    }

    // 6. Metrics.
    {
        reset_provenance_metrics();
        let _ = ProvenanceToken::base(1, 1);
        let m = provenance_metrics();
        let pass = m.fsqlite_provenance_annotations_total >= 1;
        results.push(TestResult {
            name: "metrics",
            pass,
            detail: format!("annotations={}", m.fsqlite_provenance_annotations_total),
        });
    }

    // Summary.
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-19u.3: Provenance Semirings Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-19u.3\",");
    println!("  \"suite\": \"provenance_semirings\",");
    println!("  \"total\": {total},");
    println!("  \"passed\": {passed},");
    println!("  \"failed\": {failed},");
    println!(
        "  \"pass_rate\": \"{:.1}%\",",
        passed as f64 / total as f64 * 100.0
    );
    println!("  \"cases\": [");
    for (i, r) in results.iter().enumerate() {
        let comma = if i + 1 < total { "," } else { "" };
        let status = if r.pass { "PASS" } else { "FAIL" };
        println!(
            "    {{ \"name\": \"{}\", \"status\": \"{status}\", \"detail\": \"{}\" }}{comma}",
            r.name, r.detail
        );
    }
    println!("  ]");
    println!("}}");

    assert_eq!(
        failed, 0,
        "{failed}/{total} provenance semiring conformance tests failed"
    );

    println!("[PASS] all {total} provenance semiring conformance tests passed");
}
