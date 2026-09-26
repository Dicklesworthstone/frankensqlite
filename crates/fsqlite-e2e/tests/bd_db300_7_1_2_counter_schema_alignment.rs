//! Counter schema and artifact layout alignment verification (bd-db300.7.1.2).
//!
//! Proves that the comparison harness emits identical counter schemas,
//! row identity fields, and artifact layout across all three modes
//! (SQLite reference, FrankenSQLite MVCC, FrankenSQLite single-writer).
//!
//! ## Scenarios
//!
//! | ID | Name | Verified property |
//! |----|------|-------------------|
//! | A1 | counter_ids_aligned | Same 10 comparable counter IDs across modes |
//! | A2 | counter_units_aligned | Same units for each counter ID across modes |
//! | A3 | counter_semantics_aligned | Same semantics text for each counter ID |
//! | A4 | row_identity_cross_mode | fixture/workload/concurrency match, mode_id differs |
//! | A5 | mode_specific_isolation | mode_specific empty without hot path profile |
//! | A6 | json_roundtrip_stable | Serialize/deserialize preserves all fields |
//! | A7 | benchmark_mode_coverage | All 3 mode strings stable and distinct |
//! | A8 | counter_value_encoding | Integer vs float encoding is consistent |
//! | A9 | anonymous_provenance_defaults | Anonymous row has correct defaults |
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_db300_7_1_2_counter_schema_alignment -- --nocapture
//! ```

#![allow(clippy::too_many_lines)]

use std::collections::HashSet;

use serde_json::json;

use fsqlite_e2e::benchmark::{
    BENCHMARK_COMPARABLE_COUNTER_IDS, BenchmarkComparisonMetadata, BenchmarkCounterSchema,
    BenchmarkCounterValue, BenchmarkSummary, IterationRecord, LatencyStats, ThroughputStats,
};
use fsqlite_e2e::fixture_select::BenchmarkMode;
use fsqlite_e2e::methodology::{EnvironmentMeta, MethodologyMeta};

const BEAD_ID: &str = "bd-db300.7.1.2";

fn emit_log(scenario_id: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "COUNTER_SCHEMA_ALIGNMENT:{}",
        json!({
            "bead_id": BEAD_ID,
            "scenario_id": scenario_id,
            "phase": phase,
            "data": data,
        })
    );
}

fn make_methodology() -> MethodologyMeta {
    MethodologyMeta {
        version: "fsqlite-e2e.methodology.v1".to_owned(),
        warmup_iterations: 3,
        min_measurement_iterations: 5,
        measurement_time_secs: 10,
        primary_statistic: "median".to_owned(),
        tail_statistic: "p95".to_owned(),
        fresh_db_per_iteration: true,
        identical_pragmas_enforced: true,
    }
}

fn make_environment() -> EnvironmentMeta {
    EnvironmentMeta {
        capture_mode: Default::default(),
        os: "Linux 6.17.0-23-generic".to_owned(),
        arch: "x86_64".to_owned(),
        cpu_count: 8,
        cpu_model: Some("test-cpu".to_owned()),
        ram_bytes: Some(16 * 1024 * 1024 * 1024),
        rustc_version: "nightly-2026-05-01".to_owned(),
        cargo_profile: "release".to_owned(),
        build_hygiene: fsqlite_e2e::methodology::BuildHygieneMeta::unknown(),
    }
}

fn make_iteration(idx: u32, wall_time_ms: u64, ops: u64) -> IterationRecord {
    IterationRecord {
        iteration: idx,
        wall_time_ms,
        wall_time_ns: wall_time_ms.saturating_mul(1_000_000),
        ops_per_sec: if wall_time_ms > 0 {
            (ops as f64) / (wall_time_ms as f64 / 1000.0)
        } else {
            0.0
        },
        ops_total: ops,
        retries: 0,
        aborts: 0,
        error: None,
    }
}

fn make_summary(engine: &str) -> BenchmarkSummary {
    let iterations = vec![
        make_iteration(0, 100, 1000),
        make_iteration(1, 95, 1000),
        make_iteration(2, 105, 1000),
        make_iteration(3, 98, 1000),
        make_iteration(4, 102, 1000),
    ];

    BenchmarkSummary {
        benchmark_id: format!("{engine}:oltp_rw:chinook:c4"),
        engine: engine.to_owned(),
        workload: "oltp_rw".to_owned(),
        fixture_id: "chinook".to_owned(),
        concurrency: 4,
        methodology: make_methodology(),
        environment: make_environment(),
        warmup_count: 3,
        measurement_count: 5,
        total_measurement_ms: 500,
        latency: LatencyStats {
            min_ms: 95.0,
            max_ms: 105.0,
            mean_ms: 100.0,
            median_ms: 100.0,
            p95_ms: 105.0,
            p99_ms: 105.0,
            stddev_ms: 3.5,
        },
        throughput: ThroughputStats {
            mean_ops_per_sec: 10000.0,
            median_ops_per_sec: 10000.0,
            peak_ops_per_sec: 10526.0,
        },
        comparison: None,
        aggregated_hot_path: None,
        iterations,
    }
}

// ─── A1: Counter IDs aligned ────────────────────────────────────────

#[test]
fn a1_counter_ids_aligned() {
    let sqlite_schema = BenchmarkCounterSchema::from_summary(&make_summary("sqlite3"));
    let mvcc_schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));
    let sw_schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));

    let sqlite_ids: Vec<&str> = sqlite_schema
        .comparable
        .iter()
        .map(|c| c.counter_id.as_str())
        .collect();
    let mvcc_ids: Vec<&str> = mvcc_schema
        .comparable
        .iter()
        .map(|c| c.counter_id.as_str())
        .collect();
    let sw_ids: Vec<&str> = sw_schema
        .comparable
        .iter()
        .map(|c| c.counter_id.as_str())
        .collect();

    assert_eq!(
        sqlite_ids, mvcc_ids,
        "[A1] SQLite and MVCC comparable counter IDs must match"
    );
    assert_eq!(
        mvcc_ids, sw_ids,
        "[A1] MVCC and single-writer comparable counter IDs must match"
    );
    assert_eq!(
        sqlite_ids.len(),
        BENCHMARK_COMPARABLE_COUNTER_IDS.len(),
        "[A1] counter count must match canonical list"
    );

    let canonical: Vec<&str> = BENCHMARK_COMPARABLE_COUNTER_IDS.to_vec();
    assert_eq!(
        sqlite_ids, canonical,
        "[A1] counter IDs must match canonical ordered list"
    );

    emit_log(
        "A1",
        "result",
        json!({
            "counter_count": sqlite_ids.len(),
            "ids_match": true,
            "order_matches_canonical": true,
        }),
    );
}

// ─── A2: Counter units aligned ──────────────────────────────────────

#[test]
fn a2_counter_units_aligned() {
    let sqlite_schema = BenchmarkCounterSchema::from_summary(&make_summary("sqlite3"));
    let mvcc_schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));

    for (s, m) in sqlite_schema
        .comparable
        .iter()
        .zip(mvcc_schema.comparable.iter())
    {
        assert_eq!(
            s.unit, m.unit,
            "[A2] unit mismatch for counter {}: sqlite={} mvcc={}",
            s.counter_id, s.unit, m.unit
        );
    }

    emit_log(
        "A2",
        "result",
        json!({"units_aligned": true, "counter_count": sqlite_schema.comparable.len()}),
    );
}

// ─── A3: Counter semantics aligned ──────────────────────────────────

#[test]
fn a3_counter_semantics_aligned() {
    let sqlite_schema = BenchmarkCounterSchema::from_summary(&make_summary("sqlite3"));
    let mvcc_schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));
    let sw_schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));

    for i in 0..sqlite_schema.comparable.len() {
        let s = &sqlite_schema.comparable[i];
        let m = &mvcc_schema.comparable[i];
        let w = &sw_schema.comparable[i];

        assert_eq!(
            s.semantics, m.semantics,
            "[A3] semantics mismatch for {}: sqlite vs mvcc",
            s.counter_id,
        );
        assert_eq!(
            m.semantics, w.semantics,
            "[A3] semantics mismatch for {}: mvcc vs sw",
            m.counter_id,
        );
        assert_eq!(
            s.aggregation, m.aggregation,
            "[A3] aggregation mismatch for {}",
            s.counter_id
        );
    }

    emit_log(
        "A3",
        "result",
        json!({"semantics_aligned": true, "aggregation_aligned": true}),
    );
}

// ─── A4: Row identity cross-mode ────────────────────────────────────

#[test]
fn a4_row_identity_cross_mode() {
    let sqlite_cmp =
        BenchmarkComparisonMetadata::anonymous(&make_summary("sqlite3"), "sqlite_reference");
    let mvcc_cmp = BenchmarkComparisonMetadata::anonymous(&make_summary("fsqlite"), "fsqlite_mvcc");
    let sw_cmp =
        BenchmarkComparisonMetadata::anonymous(&make_summary("fsqlite"), "fsqlite_single_writer");

    assert_eq!(
        sqlite_cmp.row_identity.fixture_id, mvcc_cmp.row_identity.fixture_id,
        "[A4] fixture_id must match across modes"
    );
    assert_eq!(
        mvcc_cmp.row_identity.fixture_id, sw_cmp.row_identity.fixture_id,
        "[A4] fixture_id must match across modes"
    );
    assert_eq!(
        sqlite_cmp.row_identity.workload, mvcc_cmp.row_identity.workload,
        "[A4] workload must match"
    );
    assert_eq!(
        sqlite_cmp.row_identity.concurrency, mvcc_cmp.row_identity.concurrency,
        "[A4] concurrency must match"
    );

    let mode_ids: HashSet<&str> = [
        sqlite_cmp.row_identity.mode_id.as_str(),
        mvcc_cmp.row_identity.mode_id.as_str(),
        sw_cmp.row_identity.mode_id.as_str(),
    ]
    .into_iter()
    .collect();
    assert_eq!(mode_ids.len(), 3, "[A4] mode_ids must be distinct");
    assert!(
        mode_ids.contains("sqlite_reference"),
        "[A4] missing sqlite_reference"
    );
    assert!(
        mode_ids.contains("fsqlite_mvcc"),
        "[A4] missing fsqlite_mvcc"
    );
    assert!(
        mode_ids.contains("fsqlite_single_writer"),
        "[A4] missing fsqlite_single_writer"
    );

    emit_log(
        "A4",
        "result",
        json!({
            "fixture_ids_match": true,
            "workloads_match": true,
            "concurrency_match": true,
            "mode_ids_distinct": true,
        }),
    );
}

// ─── A5: Mode-specific isolation ────────────────────────────────────

#[test]
fn a5_mode_specific_isolation() {
    let sqlite_schema = BenchmarkCounterSchema::from_summary(&make_summary("sqlite3"));
    let fsqlite_schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));

    assert!(
        sqlite_schema.mode_specific.is_empty(),
        "[A5] without hot path, SQLite mode_specific must be empty"
    );
    assert!(
        fsqlite_schema.mode_specific.is_empty(),
        "[A5] without hot path, FrankenSQLite mode_specific must also be empty"
    );

    let comparable_ids: HashSet<&str> = sqlite_schema
        .comparable
        .iter()
        .map(|c| c.counter_id.as_str())
        .collect();
    assert_eq!(
        comparable_ids.len(),
        BENCHMARK_COMPARABLE_COUNTER_IDS.len(),
        "[A5] comparable set must have all canonical counters"
    );

    emit_log(
        "A5",
        "result",
        json!({
            "sqlite_mode_specific_empty": true,
            "fsqlite_mode_specific_empty_without_hot_path": true,
            "comparable_count": comparable_ids.len(),
        }),
    );
}

// ─── A6: JSON roundtrip stable ──────────────────────────────────────

#[test]
fn a6_json_roundtrip_stable() {
    let cmp = BenchmarkComparisonMetadata::anonymous(&make_summary("fsqlite"), "fsqlite_mvcc");

    let serialized = serde_json::to_string_pretty(&cmp).expect("serialize");
    let deserialized: BenchmarkComparisonMetadata =
        serde_json::from_str(&serialized).expect("deserialize");

    assert_eq!(
        cmp.row_identity, deserialized.row_identity,
        "[A6] row_identity must survive roundtrip"
    );
    assert_eq!(
        cmp.counter_schema, deserialized.counter_schema,
        "[A6] counter_schema must survive roundtrip"
    );

    let reserialized = serde_json::to_string_pretty(&deserialized).expect("reserialize");
    assert_eq!(
        serialized, reserialized,
        "[A6] double roundtrip must be stable"
    );

    emit_log(
        "A6",
        "result",
        json!({
            "roundtrip_stable": true,
            "serialized_bytes": serialized.len(),
        }),
    );
}

// ─── A7: BenchmarkMode exhaustive coverage ──────────────────────────

#[test]
fn a7_benchmark_mode_coverage() {
    let modes = [
        BenchmarkMode::SqliteReference,
        BenchmarkMode::FsqliteMvcc,
        BenchmarkMode::FsqliteSingleWriter,
    ];

    let mode_strs: Vec<&str> = modes.iter().map(|m| m.as_str()).collect();
    let expected = ["sqlite_reference", "fsqlite_mvcc", "fsqlite_single_writer"];
    assert_eq!(
        mode_strs, expected,
        "[A7] mode string representations must be stable"
    );

    let unique: HashSet<&str> = mode_strs.iter().copied().collect();
    assert_eq!(unique.len(), 3, "[A7] all mode strings must be distinct");

    for mode in &modes {
        let cmp = BenchmarkComparisonMetadata::anonymous(&make_summary("test"), mode.as_str());
        assert_eq!(
            cmp.row_identity.mode_id,
            mode.as_str(),
            "[A7] anonymous row mode_id must match mode.as_str()"
        );
    }

    emit_log(
        "A7",
        "result",
        json!({
            "mode_count": 3,
            "strings_stable": true,
            "anonymous_construction_works": true,
        }),
    );
}

// ─── A8: Counter value encoding ─────────────────────────────────────

#[test]
fn a8_counter_value_encoding() {
    let schema = BenchmarkCounterSchema::from_summary(&make_summary("fsqlite"));

    let integer_counters = [
        "measurement_iteration_count",
        "measurement_wall_time_total_ms",
        "measurement_ops_total",
        "retry_total",
        "abort_total",
    ];
    let float_counters = [
        "latency_median_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "throughput_median_ops_per_sec",
        "throughput_peak_ops_per_sec",
    ];

    for counter in &schema.comparable {
        let is_int = matches!(counter.value, BenchmarkCounterValue::Integer(_));
        let is_float = matches!(counter.value, BenchmarkCounterValue::Float(_));

        if integer_counters.contains(&counter.counter_id.as_str()) {
            assert!(
                is_int,
                "[A8] counter {} should be Integer, got Float",
                counter.counter_id
            );
        }
        if float_counters.contains(&counter.counter_id.as_str()) {
            assert!(
                is_float,
                "[A8] counter {} should be Float, got Integer",
                counter.counter_id
            );
        }
    }

    emit_log(
        "A8",
        "result",
        json!({"integer_float_encoding_correct": true}),
    );
}

// ─── A9: Anonymous provenance defaults ──────────────────────────────

#[test]
fn a9_anonymous_provenance_defaults() {
    let cmp =
        BenchmarkComparisonMetadata::anonymous(&make_summary("fsqlite"), "fsqlite_single_writer");

    assert!(
        cmp.row_identity.row_id.is_none(),
        "[A9] anonymous row must have no row_id"
    );
    assert!(
        cmp.row_identity.run_id.is_none(),
        "[A9] anonymous row must have no run_id"
    );
    assert!(
        cmp.row_identity.source_revision.is_none(),
        "[A9] anonymous row must have no source_revision"
    );
    assert!(
        cmp.artifact_layout.is_none(),
        "[A9] anonymous row must have no artifact_layout"
    );
    assert!(
        cmp.canonical_artifact_manifest.is_none(),
        "[A9] anonymous row must have no manifest"
    );

    assert_eq!(cmp.row_identity.fixture_id, "chinook");
    assert_eq!(cmp.row_identity.workload, "oltp_rw");
    assert_eq!(cmp.row_identity.concurrency, 4);
    assert_eq!(cmp.row_identity.mode_id, "fsqlite_single_writer");
    assert_eq!(cmp.row_identity.build_profile_id, "release");

    assert!(!cmp.counter_schema.comparable.is_empty());

    emit_log(
        "A9",
        "result",
        json!({
            "anonymous_row_complete": true,
            "counter_schema_populated": true,
            "no_manifest_artifacts": true,
        }),
    );
}
