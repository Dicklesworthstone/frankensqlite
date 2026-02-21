//! bd-ehk.3: Bloodstream — FrankenSQLite materialized view push to render tree
//! harness integration tests.
//!
//! Validates the Bloodstream delta-propagation infrastructure:
//! - DeltaKind variants and display
//! - AlgebraicDelta construction and field access
//! - DeltaBatch lifecycle (push, source_tables, count_by_kind)
//! - Delta coalescing algebra (INSERT+DELETE cancel, INSERT+UPDATE merge, etc.)
//! - ViewBinding lifecycle (active → suspended → resumed → detached)
//! - ViewBinding table matching and delivery tracking
//! - PropagationEngine bind/unbind/suspend/resume lifecycle
//! - PropagationEngine delta routing to matching bindings
//! - PropagationEngine partial propagation (suspended/detached skipping)
//! - PropagationEngine shutdown rejection
//! - PropagationEngine max bindings enforcement
//! - PropagationMetrics accumulation and percentile computation
//! - PropagationConfig defaults
//! - PropagationResult classification
//! - Contract violation detection (verify_metrics_contract)
//! - Tracing/metrics contract constants
//! - BindingError display
//! - Conformance summary

use std::collections::BTreeSet;

use fsqlite_harness::bloodstream::{
    AlgebraicDelta, BindingError, BindingState, DeltaBatch, DeltaKind, PropagationConfig,
    PropagationEngine, PropagationMetrics, PropagationResult, ViewBinding,
    BLOODSTREAM_SCHEMA_VERSION, DELTA_SPAN_NAME, REQUIRED_METRICS, REQUIRED_SPAN_FIELDS,
    coalesce_deltas, verify_metrics_contract,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_delta(table: &str, row_id: i64, kind: DeltaKind, seq: u64) -> AlgebraicDelta {
    AlgebraicDelta {
        source_table: table.to_string(),
        row_id,
        kind,
        affected_columns: vec![0, 1],
        seq,
    }
}

fn make_batch(txn_id: u64, commit_seq: u64, deltas: Vec<AlgebraicDelta>) -> DeltaBatch {
    let mut batch = DeltaBatch::new(txn_id, commit_seq, 1_000_000_000);
    for d in deltas {
        batch.push(d);
    }
    batch
}

fn tables(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

// ── 1. DeltaKind variants and display ────────────────────────────────────────

#[test]
fn delta_kind_variants_and_display() {
    assert_eq!(DeltaKind::ALL.len(), 3);
    assert_eq!(DeltaKind::Insert.to_string(), "INSERT");
    assert_eq!(DeltaKind::Update.to_string(), "UPDATE");
    assert_eq!(DeltaKind::Delete.to_string(), "DELETE");
}

// ── 2. AlgebraicDelta construction ───────────────────────────────────────────

#[test]
fn algebraic_delta_construction() {
    let d = make_delta("users", 42, DeltaKind::Insert, 0);
    assert_eq!(d.source_table, "users");
    assert_eq!(d.row_id, 42);
    assert_eq!(d.kind, DeltaKind::Insert);
    assert_eq!(d.affected_columns, vec![0, 1]);
    assert_eq!(d.seq, 0);
}

// ── 3. DeltaBatch lifecycle ──────────────────────────────────────────────────

#[test]
fn delta_batch_lifecycle() {
    let mut batch = DeltaBatch::new(1, 100, 999);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    assert_eq!(batch.txn_id, 1);
    assert_eq!(batch.commit_seq, 100);
    assert_eq!(batch.created_at_ns, 999);

    batch.push(make_delta("users", 1, DeltaKind::Insert, 0));
    batch.push(make_delta("orders", 2, DeltaKind::Update, 1));
    batch.push(make_delta("users", 3, DeltaKind::Delete, 2));
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());

    let tables = batch.source_tables();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains("users"));
    assert!(tables.contains("orders"));

    let counts = batch.count_by_kind();
    assert_eq!(counts[&DeltaKind::Insert], 1);
    assert_eq!(counts[&DeltaKind::Update], 1);
    assert_eq!(counts[&DeltaKind::Delete], 1);
}

// ── 4. Delta coalescing: INSERT + DELETE cancels ─────────────────────────────

#[test]
fn coalesce_insert_then_delete_cancels() {
    let deltas = vec![
        make_delta("t", 1, DeltaKind::Insert, 0),
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Delete,
            affected_columns: vec![],
            seq: 1,
        },
    ];
    let result = coalesce_deltas(&deltas);
    assert!(result.is_empty(), "INSERT+DELETE should cancel completely");
}

// ── 5. Delta coalescing: INSERT + UPDATE → INSERT ────────────────────────────

#[test]
fn coalesce_insert_then_update_merges_to_insert() {
    let deltas = vec![
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Insert,
            affected_columns: vec![0],
            seq: 0,
        },
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Update,
            affected_columns: vec![1, 2],
            seq: 1,
        },
    ];
    let result = coalesce_deltas(&deltas);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].kind, DeltaKind::Insert);
    // Columns merged: 0 from insert + 1,2 from update.
    assert!(result[0].affected_columns.contains(&0));
    assert!(result[0].affected_columns.contains(&1));
    assert!(result[0].affected_columns.contains(&2));
}

// ── 6. Delta coalescing: UPDATE + DELETE → DELETE ────────────────────────────

#[test]
fn coalesce_update_then_delete() {
    let deltas = vec![
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Update,
            affected_columns: vec![0],
            seq: 0,
        },
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Delete,
            affected_columns: vec![],
            seq: 1,
        },
    ];
    let result = coalesce_deltas(&deltas);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].kind, DeltaKind::Delete);
}

// ── 7. Delta coalescing: DELETE + INSERT → UPDATE ────────────────────────────

#[test]
fn coalesce_delete_then_insert_becomes_update() {
    let deltas = vec![
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Delete,
            affected_columns: vec![],
            seq: 0,
        },
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Insert,
            affected_columns: vec![0, 1, 2],
            seq: 1,
        },
    ];
    let result = coalesce_deltas(&deltas);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].kind, DeltaKind::Update);
}

// ── 8. Delta coalescing: distinct rows preserved ─────────────────────────────

#[test]
fn coalesce_distinct_rows_preserved() {
    let deltas = vec![
        make_delta("t", 1, DeltaKind::Insert, 0),
        make_delta("t", 2, DeltaKind::Update, 1),
        make_delta("t", 3, DeltaKind::Delete, 2),
    ];
    let result = coalesce_deltas(&deltas);
    assert_eq!(result.len(), 3, "distinct rows should all survive coalescing");
}

// ── 9. BindingState lifecycle ────────────────────────────────────────────────

#[test]
fn binding_state_lifecycle() {
    assert_eq!(BindingState::ALL.len(), 3);
    assert!(BindingState::Active.is_receiving());
    assert!(!BindingState::Suspended.is_receiving());
    assert!(!BindingState::Detached.is_receiving());

    assert_eq!(BindingState::Active.to_string(), "active");
    assert_eq!(BindingState::Suspended.to_string(), "suspended");
    assert_eq!(BindingState::Detached.to_string(), "detached");
}

// ── 10. ViewBinding lifecycle ────────────────────────────────────────────────

#[test]
fn view_binding_lifecycle() {
    let mut binding = ViewBinding::new(
        1,
        "user_summary".to_string(),
        "widget_1".to_string(),
        tables(&["users", "orders"]),
    );
    assert_eq!(binding.state, BindingState::Active);
    assert!(binding.matches_table("users"));
    assert!(binding.matches_table("orders"));
    assert!(!binding.matches_table("products"));
    assert_eq!(binding.deltas_delivered, 0);
    assert_eq!(binding.last_commit_seq, None);

    // Suspend.
    binding.suspend();
    assert_eq!(binding.state, BindingState::Suspended);

    // Resume.
    binding.resume();
    assert_eq!(binding.state, BindingState::Active);

    // Detach is permanent.
    binding.detach();
    assert_eq!(binding.state, BindingState::Detached);

    // Resume from detached has no effect.
    binding.resume();
    assert_eq!(binding.state, BindingState::Detached);
}

// ── 11. ViewBinding delivery tracking ────────────────────────────────────────

#[test]
fn view_binding_delivery_tracking() {
    let mut binding = ViewBinding::new(
        1,
        "v".to_string(),
        "w".to_string(),
        tables(&["users"]),
    );

    let batch = make_batch(
        1,
        50,
        vec![
            make_delta("users", 1, DeltaKind::Insert, 0),
            make_delta("users", 2, DeltaKind::Update, 1),
            make_delta("orders", 3, DeltaKind::Delete, 2), // not matched
        ],
    );

    binding.record_delivery(&batch);
    assert_eq!(binding.deltas_delivered, 2, "only users deltas counted");
    assert_eq!(binding.last_commit_seq, Some(50));
}

// ── 12. PropagationEngine bind and routing ───────────────────────────────────

#[test]
fn propagation_engine_bind_and_routing() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());

    let id1 = engine
        .bind("v1".to_string(), "w1".to_string(), tables(&["users"]))
        .expect("bind v1");
    let id2 = engine
        .bind("v2".to_string(), "w2".to_string(), tables(&["orders"]))
        .expect("bind v2");

    assert!(id1 > 0);
    assert!(id2 > id1);
    assert_eq!(engine.bindings().len(), 2);
    assert_eq!(engine.metrics().active_bindings, 2);

    // Propagate a batch touching "users" → only w1 receives.
    let batch = make_batch(1, 1, vec![make_delta("users", 1, DeltaKind::Insert, 0)]);
    let result = engine.propagate(&batch, 100);
    assert_eq!(result.widgets_invalidated(), 1);
    assert!(result.is_success());

    // Propagate a batch touching "orders" → only w2 receives.
    let batch2 = make_batch(2, 2, vec![make_delta("orders", 1, DeltaKind::Update, 0)]);
    let result2 = engine.propagate(&batch2, 200);
    assert_eq!(result2.widgets_invalidated(), 1);

    // Metrics check.
    let m = engine.metrics();
    assert_eq!(m.deltas_total, 2);
    assert_eq!(m.batches_total, 2);
    assert_eq!(m.total_widgets_invalidated, 2);
}

// ── 13. PropagationEngine no-match result ────────────────────────────────────

#[test]
fn propagation_engine_no_match() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());
    engine
        .bind("v1".to_string(), "w1".to_string(), tables(&["users"]))
        .unwrap();

    // Batch touches "products" — no bindings match.
    let batch = make_batch(1, 1, vec![make_delta("products", 1, DeltaKind::Insert, 0)]);
    let result = engine.propagate(&batch, 50);
    assert!(matches!(result, PropagationResult::NoMatch));
    assert_eq!(result.widgets_invalidated(), 0);
    assert_eq!(engine.metrics().no_match_count, 1);
}

// ── 14. PropagationEngine partial propagation ────────────────────────────────

#[test]
fn propagation_engine_partial_propagation() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());

    let id1 = engine
        .bind("v1".to_string(), "w1".to_string(), tables(&["users"]))
        .unwrap();
    let _id2 = engine
        .bind("v2".to_string(), "w2".to_string(), tables(&["users"]))
        .unwrap();

    // Suspend one binding.
    engine.suspend(id1).unwrap();

    let batch = make_batch(1, 1, vec![make_delta("users", 1, DeltaKind::Insert, 0)]);
    let result = engine.propagate(&batch, 100);

    match result {
        PropagationResult::Partial {
            widgets_invalidated,
            skipped_suspended,
            skipped_detached,
        } => {
            assert_eq!(widgets_invalidated, 1);
            assert_eq!(skipped_suspended, 1);
            assert_eq!(skipped_detached, 0);
        }
        other => panic!("expected Partial, got {other:?}"),
    }
}

// ── 15. PropagationEngine shutdown rejection ─────────────────────────────────

#[test]
fn propagation_engine_shutdown_rejection() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());
    assert!(!engine.is_shutdown());

    engine.shutdown();
    assert!(engine.is_shutdown());

    // Bind should fail.
    let err = engine
        .bind("v".to_string(), "w".to_string(), tables(&["t"]))
        .unwrap_err();
    assert_eq!(err, BindingError::EngineShutdown);

    // Propagate should return Shutdown.
    let batch = make_batch(1, 1, vec![make_delta("t", 1, DeltaKind::Insert, 0)]);
    let result = engine.propagate(&batch, 0);
    assert!(matches!(result, PropagationResult::Shutdown));
    assert!(!result.is_success());
}

// ── 16. PropagationEngine max bindings enforcement ───────────────────────────

#[test]
fn propagation_engine_max_bindings() {
    let config = PropagationConfig {
        max_active_bindings: 2,
        ..PropagationConfig::default()
    };
    let mut engine = PropagationEngine::new(config);

    engine
        .bind("v1".to_string(), "w1".to_string(), tables(&["t"]))
        .unwrap();
    engine
        .bind("v2".to_string(), "w2".to_string(), tables(&["t"]))
        .unwrap();

    // Third binding should fail.
    let err = engine
        .bind("v3".to_string(), "w3".to_string(), tables(&["t"]))
        .unwrap_err();
    assert_eq!(
        err,
        BindingError::MaxBindingsExceeded { limit: 2 }
    );
}

// ── 17. PropagationEngine unbind ─────────────────────────────────────────────

#[test]
fn propagation_engine_unbind() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());

    let id = engine
        .bind("v".to_string(), "w".to_string(), tables(&["t"]))
        .unwrap();
    assert_eq!(engine.metrics().active_bindings, 1);

    engine.unbind(id).unwrap();
    assert_eq!(engine.metrics().active_bindings, 0);
    assert_eq!(engine.metrics().detached_bindings, 1);

    // Unbind non-existent binding.
    let err = engine.unbind(999).unwrap_err();
    assert_eq!(err, BindingError::NotFound { binding_id: 999 });
}

// ── 18. PropagationMetrics computation ───────────────────────────────────────

#[test]
fn propagation_metrics_computation() {
    let mut m = PropagationMetrics::new();
    assert_eq!(m.mean_duration_us(), 0.0);
    assert_eq!(m.p99_duration_us(), 0);

    m.propagation_durations_us = vec![100, 200, 300, 400, 500];
    m.batches_total = 5;
    m.deltas_total = 50;

    let mean = m.mean_duration_us();
    assert!((mean - 300.0).abs() < 1e-10, "mean of [100..500] = 300");

    let p99 = m.p99_duration_us();
    assert_eq!(p99, 500);
}

// ── 19. PropagationConfig defaults ───────────────────────────────────────────

#[test]
fn propagation_config_defaults() {
    let config = PropagationConfig::default();
    assert_eq!(config.max_batch_size, 1024);
    assert_eq!(config.target_latency_us, 1000);
    assert!(config.coalesce_row_deltas);
    assert_eq!(config.max_active_bindings, 256);
}

// ── 20. Contract violation detection ─────────────────────────────────────────

#[test]
fn contract_violation_detection() {
    // Clean metrics — no violations.
    let clean = PropagationMetrics::new();
    let violations = verify_metrics_contract(&clean);
    assert!(violations.is_empty(), "fresh metrics should have no violations");

    // Broken metrics: deltas > 0 but batches == 0.
    let mut broken = PropagationMetrics::new();
    broken.deltas_total = 10;
    broken.batches_total = 0;
    let violations = verify_metrics_contract(&broken);
    assert!(!violations.is_empty());
    assert!(violations.iter().any(|v| v.field == "batches_total"));
}

// ── 21. Tracing contract constants ───────────────────────────────────────────

#[test]
fn tracing_contract_constants() {
    assert_eq!(DELTA_SPAN_NAME, "bloodstream.delta");
    assert_eq!(REQUIRED_SPAN_FIELDS.len(), 4);
    assert!(REQUIRED_SPAN_FIELDS.contains(&"source_table"));
    assert!(REQUIRED_SPAN_FIELDS.contains(&"rows_changed"));
    assert!(REQUIRED_SPAN_FIELDS.contains(&"propagation_duration_us"));
    assert!(REQUIRED_SPAN_FIELDS.contains(&"widgets_invalidated"));

    assert_eq!(REQUIRED_METRICS.len(), 3);
    assert!(REQUIRED_METRICS.contains(&"bloodstream_deltas_total"));
    assert!(REQUIRED_METRICS.contains(&"bloodstream_propagation_duration_us"));
    assert!(REQUIRED_METRICS.contains(&"bloodstream_active_bindings"));
}

// ── 22. BindingError display ─────────────────────────────────────────────────

#[test]
fn binding_error_display() {
    let shutdown = BindingError::EngineShutdown;
    assert!(shutdown.to_string().contains("shut down"));

    let max = BindingError::MaxBindingsExceeded { limit: 10 };
    assert!(max.to_string().contains("10"));

    let not_found = BindingError::NotFound { binding_id: 42 };
    assert!(not_found.to_string().contains("42"));
}

// ── 23. Schema version ──────────────────────────────────────────────────────

#[test]
fn schema_version() {
    assert_eq!(BLOODSTREAM_SCHEMA_VERSION, 1);
}

// ── 24. Engine resume from suspend ──────────────────────────────────────────

#[test]
fn engine_suspend_resume_lifecycle() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());
    let id = engine
        .bind("v".to_string(), "w".to_string(), tables(&["t"]))
        .unwrap();

    assert_eq!(engine.metrics().active_bindings, 1);
    assert_eq!(engine.metrics().suspended_bindings, 0);

    engine.suspend(id).unwrap();
    assert_eq!(engine.metrics().active_bindings, 0);
    assert_eq!(engine.metrics().suspended_bindings, 1);

    engine.resume(id).unwrap();
    assert_eq!(engine.metrics().active_bindings, 1);
    assert_eq!(engine.metrics().suspended_bindings, 0);
}

// ── 25. Multi-table binding with cross-table batch ──────────────────────────

#[test]
fn multi_table_binding_cross_table_batch() {
    let mut engine = PropagationEngine::new(PropagationConfig::default());
    engine
        .bind(
            "dashboard".to_string(),
            "main_panel".to_string(),
            tables(&["users", "orders", "products"]),
        )
        .unwrap();

    // Batch touches users and products.
    let batch = make_batch(
        1,
        1,
        vec![
            make_delta("users", 1, DeltaKind::Insert, 0),
            make_delta("products", 5, DeltaKind::Update, 1),
        ],
    );
    let result = engine.propagate(&batch, 150);
    assert_eq!(result.widgets_invalidated(), 1);
    assert!(result.is_success());

    // The binding should have recorded 2 deltas (both tables match).
    let binding = engine.get_binding(1).unwrap();
    assert_eq!(binding.deltas_delivered, 2);
    assert_eq!(binding.last_commit_seq, Some(1));
}

// ── Conformance summary ─────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // Gate 1: Delta types.
    assert_eq!(DeltaKind::ALL.len(), 3);

    // Gate 2: Binding states.
    assert_eq!(BindingState::ALL.len(), 3);

    // Gate 3: Coalescing algebra.
    let cancel = coalesce_deltas(&[
        make_delta("t", 1, DeltaKind::Insert, 0),
        AlgebraicDelta {
            source_table: "t".to_string(),
            row_id: 1,
            kind: DeltaKind::Delete,
            affected_columns: vec![],
            seq: 1,
        },
    ]);
    assert!(cancel.is_empty());

    // Gate 4: Engine propagation.
    let mut engine = PropagationEngine::new(PropagationConfig::default());
    engine
        .bind("v".to_string(), "w".to_string(), tables(&["t"]))
        .unwrap();
    let batch = make_batch(1, 1, vec![make_delta("t", 1, DeltaKind::Insert, 0)]);
    let result = engine.propagate(&batch, 100);
    assert!(result.is_success());

    // Gate 5: Metrics contract.
    let violations = verify_metrics_contract(engine.metrics());
    assert!(violations.is_empty());

    // Gate 6: Tracing contract constants.
    assert_eq!(REQUIRED_SPAN_FIELDS.len(), 4);
    assert_eq!(REQUIRED_METRICS.len(), 3);

    let total_gates = 6;
    let passed = 6;
    println!("[bd-ehk.3] conformance: {passed}/{total_gates} gates passed");
}
