//! Integration tests for the Decision Contract (bd-1lsfu.6).
//!
//! Exercises the full planner → contract → calibration pipeline using
//! realistic table stats and index configurations.

#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

use fsqlite_planner::decision_contract::{
    ActualCost, DecisionLog, GENESIS_HASH, MISCALIBRATION_HIGH, MISCALIBRATION_LOW,
    MiscalibrationAlert, build_contract, compute_calibration,
};
use fsqlite_planner::{
    AccessPathKind, IndexInfo, QueryPlan, StatsSource, TableStats, best_access_path, order_joins,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn oltp_tables() -> Vec<TableStats> {
    vec![
        TableStats {
            name: "users".to_owned(),
            n_pages: 200,
            n_rows: 50_000,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "orders".to_owned(),
            n_pages: 1000,
            n_rows: 500_000,
            source: StatsSource::Analyze,
        },
        TableStats {
            name: "products".to_owned(),
            n_pages: 50,
            n_rows: 5000,
            source: StatsSource::Analyze,
        },
    ]
}

fn oltp_indexes() -> Vec<IndexInfo> {
    vec![
        IndexInfo {
            name: "idx_orders_user_id".to_owned(),
            table: "orders".to_owned(),
            columns: vec!["user_id".to_owned()],
            unique: false,
            n_pages: 100,
            source: StatsSource::Analyze,
        },
        IndexInfo {
            name: "idx_orders_product_id".to_owned(),
            table: "orders".to_owned(),
            columns: vec!["product_id".to_owned()],
            unique: false,
            n_pages: 80,
            source: StatsSource::Analyze,
        },
        IndexInfo {
            name: "idx_users_email".to_owned(),
            table: "users".to_owned(),
            columns: vec!["email".to_owned()],
            unique: true,
            n_pages: 30,
            source: StatsSource::Analyze,
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn single_table_plan_produces_contract() {
    let tables = vec![TableStats {
        name: "t".to_owned(),
        n_pages: 10,
        n_rows: 100,
        source: StatsSource::Heuristic,
    }];
    let indexes = vec![];

    let plan = order_joins(&tables, &indexes, &[], None, &[]);
    assert_eq!(plan.join_order, vec!["t"]);
    assert!((plan.total_cost - 10.0).abs() < 0.01); // Full scan of 10 pages.

    let contract = build_contract(
        "SELECT * FROM t",
        &tables,
        &indexes,
        0,
        None,
        0,
        &plan,
        1,
        false,
        GENESIS_HASH,
    );

    assert_eq!(contract.state.tables.len(), 1);
    assert_eq!(contract.state.tables[0].source, "heuristic");
    assert_eq!(contract.action.join_order, vec!["t"]);
    assert_eq!(contract.action.access_paths.len(), 1);
    assert_eq!(contract.action.access_paths[0].kind, "full_table_scan");
    assert!(contract.calibration.is_none());
}

#[test]
fn multi_table_join_plan_contract() {
    let tables = oltp_tables();
    let indexes = oltp_indexes();

    let plan = order_joins(&tables, &indexes, &[], None, &[]);
    assert_eq!(plan.join_order.len(), 3);

    let contract = build_contract(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id",
        &tables,
        &indexes,
        2,
        None,
        0,
        &plan,
        5,
        false,
        GENESIS_HASH,
    );

    assert_eq!(contract.state.tables.len(), 3);
    assert_eq!(contract.state.indexes.len(), 3);
    assert_eq!(contract.state.where_term_count, 2);
    assert_eq!(contract.action.beam_width, 5);
    assert!(!contract.action.star_query_detected);
    assert!(contract.loss.estimated_cost > 0.0);
}

#[test]
fn decision_log_with_real_planner() {
    let tables = oltp_tables();
    let indexes = oltp_indexes();

    let mut log = DecisionLog::new();

    // Record several plans.
    let plan1 = order_joins(&tables[..1], &indexes, &[], None, &[]);
    let id1 = log.record_plan(
        "SELECT * FROM users",
        &tables[..1],
        &indexes,
        0,
        None,
        0,
        &plan1,
        1,
        false,
    );

    let plan2 = order_joins(&tables, &indexes, &[], None, &[]);
    let id2 = log.record_plan(
        "SELECT * FROM users JOIN orders JOIN products",
        &tables,
        &indexes,
        0,
        None,
        0,
        &plan2,
        5,
        false,
    );

    assert_eq!(log.len(), 2);
    assert!(log.verify_chain_integrity());

    // Simulate well-calibrated execution for plan1.
    let estimated1 = log.get(id1).unwrap().loss.estimated_cost;
    let estimated1_pages = estimated1.max(0.0) as u64;
    log.record_actual(
        id1,
        ActualCost {
            page_reads: estimated1_pages,
            cpu_micros: 500,
            actual_rows: 50_000,
            wall_time_micros: 1000,
        },
    );

    // Verify calibration is good.
    let contract1 = log.get(id1).unwrap();
    let cal1 = contract1.calibration.as_ref().unwrap();
    assert!((cal1.ratio - 1.0).abs() < 0.1);
    assert!(!cal1.miscalibrated);

    // Simulate poorly-calibrated execution for plan2.
    log.record_actual(
        id2,
        ActualCost {
            page_reads: 1_000_000, // Massive underestimate.
            cpu_micros: 500_000,
            actual_rows: 1_000_000,
            wall_time_micros: 2_000_000,
        },
    );

    let contract2 = log.get(id2).unwrap();
    assert!(contract2.is_miscalibrated());

    // Aggregate stats.
    let stats = log.calibration_stats();
    assert_eq!(stats.calibrated_decisions, 2);
    assert_eq!(stats.miscalibrated_count, 1);
    assert!(!stats.is_well_calibrated());
}

#[test]
fn calibration_thresholds() {
    // Well calibrated: ratio between 0.2 and 5.0.
    let good = compute_calibration(100.0, 100).unwrap();
    assert!(!good.miscalibrated);
    assert!((good.ratio - 1.0).abs() < f64::EPSILON);

    // Boundary: exactly at threshold.
    let at_high = compute_calibration(100.0, 500).unwrap();
    assert!(!at_high.miscalibrated); // 5.0 is NOT > 5.0
    assert!((at_high.ratio - MISCALIBRATION_HIGH).abs() < f64::EPSILON);

    // Just above high threshold.
    let above = compute_calibration(100.0, 501).unwrap();
    assert!(above.miscalibrated);
    assert!(matches!(
        above.alert,
        Some(MiscalibrationAlert::Underestimate { .. })
    ));

    // Just below low threshold.
    let below = compute_calibration(100.0, 19).unwrap();
    assert!(below.miscalibrated);
    assert!(matches!(
        below.alert,
        Some(MiscalibrationAlert::Overestimate { .. })
    ));

    // At low threshold boundary.
    let at_low = compute_calibration(100.0, 20).unwrap();
    assert!(!at_low.miscalibrated); // 0.2 is NOT < 0.2
    assert!((at_low.ratio - MISCALIBRATION_LOW).abs() < f64::EPSILON);
}

#[test]
fn chain_integrity_survives_calibration_updates() {
    let tables = oltp_tables();
    let indexes = oltp_indexes();
    let plan = order_joins(&tables[..1], &indexes, &[], None, &[]);

    let mut log = DecisionLog::new();
    let id = log.record_plan(
        "SELECT 1",
        &tables[..1],
        &indexes,
        0,
        None,
        0,
        &plan,
        1,
        false,
    );

    assert!(log.verify_chain_integrity());

    // Record actual cost.
    log.record_actual(
        id,
        ActualCost {
            page_reads: 200,
            cpu_micros: 100,
            actual_rows: 50_000,
            wall_time_micros: 500,
        },
    );

    // Chain still valid (calibration is addendum, doesn't change hash).
    assert!(log.verify_chain_integrity());
}

#[test]
fn json_roundtrip_preserves_data() {
    let tables = oltp_tables();
    let indexes = oltp_indexes();
    let plan = order_joins(&tables[..2], &indexes, &[], None, &[]);

    let mut log = DecisionLog::new();
    let id = log.record_plan(
        "SELECT * FROM users JOIN orders",
        &tables[..2],
        &indexes,
        0,
        None,
        0,
        &plan,
        5,
        false,
    );

    log.record_actual(
        id,
        ActualCost {
            page_reads: 300,
            cpu_micros: 200,
            actual_rows: 100,
            wall_time_micros: 600,
        },
    );

    let json = log.to_json().unwrap();
    assert!(json.contains("\"query_text\""));
    assert!(json.contains("\"calibration\""));
    assert!(json.contains("\"record_hash\""));

    // Deserialize back.
    let parsed: Vec<fsqlite_planner::decision_contract::DecisionContract> =
        serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.len(), 1);
    assert!(parsed[0].calibration.is_some());
    assert_eq!(parsed[0].loss.actual_cost.as_ref().unwrap().page_reads, 300);
}

#[test]
fn calibration_stats_format() {
    let tables = oltp_tables();
    let indexes = oltp_indexes();
    let plan = order_joins(&tables[..1], &indexes, &[], None, &[]);

    let mut log = DecisionLog::new();

    // Generate 20 well-calibrated decisions.
    for i in 0..20 {
        let id = log.record_plan(
            &format!("SELECT {i}"),
            &tables[..1],
            &indexes,
            0,
            None,
            0,
            &plan,
            1,
            false,
        );
        let est = log.get(id).unwrap().loss.estimated_cost;
        let pages = (est * f64::from(i).mul_add(0.01, 0.9)).max(0.0) as u64;
        log.record_actual(
            id,
            ActualCost {
                page_reads: pages,
                cpu_micros: 50,
                actual_rows: 50_000,
                wall_time_micros: 100,
            },
        );
    }

    let stats = log.calibration_stats();
    assert_eq!(stats.calibrated_decisions, 20);
    assert!(stats.is_well_calibrated());

    // Display format works.
    let display = format!("{stats}");
    assert!(display.contains("20/20"));
    assert!(display.contains("calibrated"));
}

#[test]
fn access_path_selection_logged_in_contract() {
    let table = TableStats {
        name: "orders".to_owned(),
        n_pages: 1000,
        n_rows: 500_000,
        source: StatsSource::Analyze,
    };
    let indexes = vec![IndexInfo {
        name: "idx_orders_user_id".to_owned(),
        table: "orders".to_owned(),
        columns: vec!["user_id".to_owned()],
        unique: false,
        n_pages: 100,
        source: StatsSource::Analyze,
    }];

    // Without WHERE → full table scan.
    let ap = best_access_path(&table, &indexes, &[], None);
    assert!(matches!(ap.kind, AccessPathKind::FullTableScan));

    // Plan captures the access path.
    let plan = QueryPlan {
        join_order: vec!["orders".to_owned()],
        access_paths: vec![ap],
        total_cost: 1000.0,
    };
    let contract = build_contract(
        "SELECT * FROM orders",
        &[table],
        &indexes,
        0,
        None,
        0,
        &plan,
        1,
        false,
        GENESIS_HASH,
    );
    assert_eq!(contract.action.access_paths[0].kind, "full_table_scan");
}

#[test]
fn empty_log_statistics() {
    let log = DecisionLog::new();
    let stats = log.calibration_stats();
    assert_eq!(stats.total_decisions, 0);
    assert_eq!(stats.calibrated_decisions, 0);
    assert!(stats.is_well_calibrated());
    assert!(stats.miscalibration_rate().abs() < f64::EPSILON);
}

#[test]
fn record_actual_for_nonexistent_id_returns_false() {
    let mut log = DecisionLog::new();
    assert!(!log.record_actual(
        999,
        ActualCost {
            page_reads: 1,
            cpu_micros: 1,
            actual_rows: 1,
            wall_time_micros: 1,
        }
    ));
}
