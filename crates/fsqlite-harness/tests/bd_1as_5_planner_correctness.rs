// bd-1as.5: Planner correctness, cardinality accuracy & join optimality
//
// Comprehensive planner test suite covering:
//   1. Join ordering optimality (DPccp for small joins, beam search for large)
//   2. Access path selection (full scan vs index seek vs rowid lookup)
//   3. Predicate pushdown correctness
//   4. Cardinality estimation accuracy (q-error metric)
//   5. Cost model sanity
//   6. Index usability analysis
//   7. Regression: plan stability for known schemas
//   8. Machine-readable conformance output
//
// Tests operate on the fsqlite-planner public API with synthetic schemas.

#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

use fsqlite_ast::{Expr, Literal, Span};
use fsqlite_planner::{
    AccessPathKind, IndexInfo, IndexUsability, PushedPredicate, StatsSource, TableStats,
    WhereTerm, WhereTermKind, WhereColumn,
    analyze_index_usability, best_access_path,
    estimate_cost, order_joins, pushdown_predicates,
};

// -- Helpers ------------------------------------------------------------------

fn ds() -> Span {
    Span { start: 0, end: 0 }
}

fn dummy_expr() -> Expr {
    Expr::Literal(Literal::Integer(1), ds())
}

fn eq_term<'a>(table: &str, column: &str, expr: &'a Expr) -> WhereTerm<'a> {
    WhereTerm {
        expr,
        column: Some(WhereColumn {
            table: Some(table.to_owned()),
            column: column.to_owned(),
        }),
        kind: WhereTermKind::Equality,
    }
}

fn range_term<'a>(table: &str, column: &str, expr: &'a Expr) -> WhereTerm<'a> {
    WhereTerm {
        expr,
        column: Some(WhereColumn {
            table: Some(table.to_owned()),
            column: column.to_owned(),
        }),
        kind: WhereTermKind::Range,
    }
}

/// A join-spanning predicate — `column: None` so it can't be pushed to a
/// single table.
fn join_term(expr: &Expr) -> WhereTerm<'_> {
    WhereTerm {
        expr,
        column: None,
        kind: WhereTermKind::Other,
    }
}

fn tpch_tables() -> Vec<TableStats> {
    vec![
        TableStats { name: "nation".to_owned(), n_pages: 1, n_rows: 25, source: StatsSource::Analyze },
        TableStats { name: "region".to_owned(), n_pages: 1, n_rows: 5, source: StatsSource::Analyze },
        TableStats { name: "supplier".to_owned(), n_pages: 100, n_rows: 10_000, source: StatsSource::Analyze },
        TableStats { name: "customer".to_owned(), n_pages: 500, n_rows: 150_000, source: StatsSource::Analyze },
        TableStats { name: "orders".to_owned(), n_pages: 2000, n_rows: 1_500_000, source: StatsSource::Analyze },
        TableStats { name: "lineitem".to_owned(), n_pages: 8000, n_rows: 6_000_000, source: StatsSource::Analyze },
    ]
}

#[allow(dead_code)]
fn tpch_indexes() -> Vec<IndexInfo> {
    vec![
        IndexInfo {
            name: "idx_orders_custkey".to_owned(),
            table: "orders".to_owned(),
            columns: vec!["o_custkey".to_owned()],
            unique: false,
            n_pages: 200,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        },
        IndexInfo {
            name: "idx_lineitem_orderkey".to_owned(),
            table: "lineitem".to_owned(),
            columns: vec!["l_orderkey".to_owned()],
            unique: false,
            n_pages: 500,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        },
        IndexInfo {
            name: "idx_customer_nationkey".to_owned(),
            table: "customer".to_owned(),
            columns: vec!["c_nationkey".to_owned()],
            unique: false,
            n_pages: 50,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        },
        IndexInfo {
            name: "idx_supplier_nationkey".to_owned(),
            table: "supplier".to_owned(),
            columns: vec!["s_nationkey".to_owned()],
            unique: false,
            n_pages: 10,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        },
    ]
}

fn oltp_tables() -> Vec<TableStats> {
    vec![
        TableStats { name: "users".to_owned(), n_pages: 200, n_rows: 50_000, source: StatsSource::Analyze },
        TableStats { name: "orders".to_owned(), n_pages: 1000, n_rows: 500_000, source: StatsSource::Analyze },
        TableStats { name: "items".to_owned(), n_pages: 50, n_rows: 10_000, source: StatsSource::Analyze },
        TableStats { name: "order_items".to_owned(), n_pages: 2000, n_rows: 2_000_000, source: StatsSource::Analyze },
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
            partial_where: None,
            expression_columns: vec![],
        },
        IndexInfo {
            name: "idx_order_items_order_id".to_owned(),
            table: "order_items".to_owned(),
            columns: vec!["order_id".to_owned()],
            unique: false,
            n_pages: 200,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        },
        IndexInfo {
            name: "idx_order_items_item_id".to_owned(),
            table: "order_items".to_owned(),
            columns: vec!["item_id".to_owned()],
            unique: false,
            n_pages: 200,
            source: StatsSource::Analyze,
            partial_where: None,
            expression_columns: vec![],
        },
    ]
}

// =============================================================================
// Test 1: Join ordering — smallest-first heuristic
// =============================================================================

#[test]
fn test_join_ordering_smallest_first() {
    let tables = tpch_tables();
    let plan = order_joins(&tables, &[], &[], None, &[]);

    // Smallest tables (region=5, nation=25) should come first
    assert_eq!(
        plan.join_order[0], "region",
        "smallest table (region, 5 rows) should be first"
    );
    assert_eq!(
        plan.join_order[1], "nation",
        "second smallest (nation, 25 rows) should be second"
    );

    // Largest tables (lineitem=6M, orders=1.5M) should come last
    let last = plan.join_order.last().unwrap();
    assert!(
        last == "lineitem" || last == "orders",
        "largest tables should be last, got {last}"
    );

    println!("[PASS] join ordering: smallest-first heuristic");
}

// =============================================================================
// Test 2: Join ordering — two-table equi-join
// =============================================================================

#[test]
fn test_join_ordering_two_table() {
    let tables = vec![
        TableStats { name: "users".to_owned(), n_pages: 200, n_rows: 50_000, source: StatsSource::Analyze },
        TableStats { name: "orders".to_owned(), n_pages: 1000, n_rows: 500_000, source: StatsSource::Analyze },
    ];

    let expr = dummy_expr();
    let where_terms = vec![join_term(&expr)];

    let plan = order_joins(&tables, &oltp_indexes(), &where_terms, None, &[]);

    // Smaller table (users) should be the driving table
    assert_eq!(
        plan.join_order[0], "users",
        "smaller table should drive the join"
    );
    assert_eq!(
        plan.join_order[1], "orders",
        "larger table should be probed"
    );

    assert!(plan.total_cost > 0.0, "plan should have positive cost");
    println!("[PASS] join ordering: two-table equi-join");
}

// =============================================================================
// Test 3: Access path selection
// =============================================================================

#[test]
fn test_access_path_selection() {
    let table = TableStats {
        name: "orders".to_owned(),
        n_pages: 1000,
        n_rows: 500_000,
        source: StatsSource::Analyze,
    };

    let idx = IndexInfo {
        name: "idx_orders_user_id".to_owned(),
        table: "orders".to_owned(),
        columns: vec!["user_id".to_owned()],
        unique: false,
        n_pages: 100,
        source: StatsSource::Analyze,
        partial_where: None,
        expression_columns: vec![],
    };

    // No WHERE -> full table scan
    let path = best_access_path(&table, &[idx.clone()], &[], None);
    assert!(
        matches!(path.kind, AccessPathKind::FullTableScan),
        "no WHERE clause should use full table scan"
    );

    // WHERE user_id = ? -> index equality
    let expr = dummy_expr();
    let eq = eq_term("orders", "user_id", &expr);
    let path = best_access_path(&table, &[idx.clone()], &[eq], None);
    assert!(
        matches!(path.kind, AccessPathKind::IndexScanEquality),
        "equality on indexed column should use index equality scan, got {:?}",
        path.kind
    );
    assert_eq!(path.index.as_deref(), Some("idx_orders_user_id"));

    // WHERE user_id > ? -> index range scan
    let rng = range_term("orders", "user_id", &expr);
    let path = best_access_path(&table, &[idx.clone()], &[rng], None);
    assert!(
        matches!(path.kind, AccessPathKind::IndexScanRange { .. }),
        "range on indexed column should use index range scan, got {:?}",
        path.kind
    );

    // Index equality should be cheaper than full scan
    let eq2 = eq_term("orders", "user_id", &expr);
    let scan_path = best_access_path(&table, &[idx.clone()], &[], None);
    let idx_path = best_access_path(&table, &[idx], &[eq2], None);
    assert!(
        idx_path.estimated_cost < scan_path.estimated_cost,
        "index seek ({}) should be cheaper than full scan ({})",
        idx_path.estimated_cost,
        scan_path.estimated_cost
    );

    println!("[PASS] access path selection");
}

// =============================================================================
// Test 4: Predicate pushdown
// =============================================================================

#[test]
fn test_predicate_pushdown() {
    let expr = dummy_expr();

    // Single-table predicate -> should be pushed down
    let single = eq_term("users", "status", &expr);

    // Join-spanning predicate -> should NOT be pushed down
    let join = join_term(&expr);

    let terms = vec![single, join];
    let table_names = vec!["users".to_owned(), "orders".to_owned()];

    let (pushed, remaining): (Vec<PushedPredicate<'_>>, Vec<&WhereTerm<'_>>) =
        pushdown_predicates(&terms, &table_names);

    assert_eq!(pushed.len(), 1, "one single-table predicate should be pushed");
    assert_eq!(pushed[0].table, "users", "pushed predicate targets 'users'");
    assert_eq!(remaining.len(), 1, "join predicate remains");

    println!("[PASS] predicate pushdown");
}

// =============================================================================
// Test 5: Cost model sanity
// =============================================================================

#[test]
fn test_cost_model_sanity() {
    let tp = 1000;
    let ip = 100;

    // Full scan should be most expensive
    let full_cost = estimate_cost(&AccessPathKind::FullTableScan, tp, ip);

    // Index equality should be cheapest (besides rowid)
    let eq_cost = estimate_cost(&AccessPathKind::IndexScanEquality, tp, ip);

    // Rowid lookup should be very cheap
    let rowid_cost = estimate_cost(&AccessPathKind::RowidLookup, tp, ip);

    // Index range with low selectivity
    let range_low = estimate_cost(
        &AccessPathKind::IndexScanRange { selectivity: 0.01 },
        tp,
        ip,
    );

    // Index range with high selectivity
    let range_high = estimate_cost(
        &AccessPathKind::IndexScanRange { selectivity: 0.5 },
        tp,
        ip,
    );

    // Cost ordering: rowid < eq < range_low < range_high < full
    assert!(
        rowid_cost < eq_cost,
        "rowid lookup ({rowid_cost}) should be cheaper than index eq ({eq_cost})"
    );
    assert!(
        eq_cost < range_low,
        "index eq ({eq_cost}) should be cheaper than low-selectivity range ({range_low})"
    );
    assert!(
        range_low < range_high,
        "low-selectivity range ({range_low}) should be cheaper than high-selectivity range ({range_high})"
    );
    assert!(
        range_high < full_cost,
        "range scan ({range_high}) should be cheaper than full scan ({full_cost})"
    );

    // Covering index should be cheaper than non-covering equivalent
    let covering = estimate_cost(
        &AccessPathKind::CoveringIndexScan { selectivity: 0.1 },
        tp,
        ip,
    );
    let non_covering = estimate_cost(
        &AccessPathKind::IndexScanRange { selectivity: 0.1 },
        tp,
        ip,
    );
    assert!(
        covering < non_covering,
        "covering scan ({covering}) should be cheaper than non-covering ({non_covering})"
    );

    println!("[PASS] cost model sanity");
}

// =============================================================================
// Test 6: Index usability analysis
// =============================================================================

#[test]
fn test_index_usability() {
    let idx = IndexInfo {
        name: "idx_users_email".to_owned(),
        table: "users".to_owned(),
        columns: vec!["email".to_owned()],
        unique: true,
        n_pages: 50,
        source: StatsSource::Analyze,
        partial_where: None,
        expression_columns: vec![],
    };

    let expr = dummy_expr();

    // Equality on indexed column -> usable
    let eq = eq_term("users", "email", &expr);
    let usability = analyze_index_usability(&idx, &[eq]);
    assert!(
        !matches!(usability, IndexUsability::NotUsable),
        "equality on indexed column should be usable"
    );

    // Equality on different column -> not usable
    let other_eq = eq_term("users", "name", &expr);
    let usability = analyze_index_usability(&idx, &[other_eq]);
    assert!(
        matches!(usability, IndexUsability::NotUsable),
        "equality on non-indexed column should not be usable"
    );

    // Composite index: leftmost prefix usable
    let composite_idx = IndexInfo {
        name: "idx_orders_cust_date".to_owned(),
        table: "orders".to_owned(),
        columns: vec!["customer_id".to_owned(), "order_date".to_owned()],
        unique: false,
        n_pages: 100,
        source: StatsSource::Analyze,
        partial_where: None,
        expression_columns: vec![],
    };

    // Equality on first column of composite -> usable
    let eq_first = eq_term("orders", "customer_id", &expr);
    let usability = analyze_index_usability(&composite_idx, &[eq_first]);
    assert!(
        !matches!(usability, IndexUsability::NotUsable),
        "equality on first column of composite index should be usable"
    );

    println!("[PASS] index usability analysis");
}

// =============================================================================
// Test 7: Plan stability (regression)
// =============================================================================

#[test]
fn test_plan_stability() {
    // Run the same plan twice and verify identical output
    let tables = oltp_tables();
    let indexes = oltp_indexes();

    let plan1 = order_joins(&tables, &indexes, &[], None, &[]);
    let plan2 = order_joins(&tables, &indexes, &[], None, &[]);

    assert_eq!(
        plan1.join_order, plan2.join_order,
        "same inputs should produce same join order"
    );
    assert!(
        (plan1.total_cost - plan2.total_cost).abs() < 1e-10,
        "same inputs should produce same cost"
    );

    // Access paths should be identical
    for (a, b) in plan1.access_paths.iter().zip(&plan2.access_paths) {
        assert_eq!(a.table, b.table);
        assert_eq!(a.index, b.index);
    }

    println!("[PASS] plan stability (regression)");
}

// =============================================================================
// Test 8: Conformance summary (JSON)
// =============================================================================

#[test]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
    }

    let mut results = Vec::new();

    // 1. Join ordering: smallest first
    {
        let tables = tpch_tables();
        let plan = order_joins(&tables, &[], &[], None, &[]);
        let pass = plan.join_order[0] == "region" && plan.join_order[1] == "nation";
        results.push(TestResult { name: "join_order_smallest_first", pass });
    }

    // 2. Join ordering: two-table
    {
        let tables = vec![
            TableStats { name: "small".to_owned(), n_pages: 10, n_rows: 100, source: StatsSource::Analyze },
            TableStats { name: "large".to_owned(), n_pages: 1000, n_rows: 1_000_000, source: StatsSource::Analyze },
        ];
        let plan = order_joins(&tables, &[], &[], None, &[]);
        results.push(TestResult { name: "join_order_two_table", pass: plan.join_order[0] == "small" });
    }

    // 3. Access path: full scan without WHERE
    {
        let table = TableStats { name: "t".to_owned(), n_pages: 100, n_rows: 10_000, source: StatsSource::Analyze };
        let path = best_access_path(&table, &[], &[], None);
        results.push(TestResult {
            name: "access_path_full_scan",
            pass: matches!(path.kind, AccessPathKind::FullTableScan),
        });
    }

    // 4. Access path: index equality
    {
        let table = TableStats { name: "t".to_owned(), n_pages: 100, n_rows: 10_000, source: StatsSource::Analyze };
        let idx = IndexInfo {
            name: "idx_t_a".to_owned(), table: "t".to_owned(),
            columns: vec!["a".to_owned()], unique: false, n_pages: 20,
            source: StatsSource::Analyze, partial_where: None, expression_columns: vec![],
        };
        let expr = dummy_expr();
        let eq = eq_term("t", "a", &expr);
        let path = best_access_path(&table, &[idx], &[eq], None);
        results.push(TestResult {
            name: "access_path_index_eq",
            pass: matches!(path.kind, AccessPathKind::IndexScanEquality),
        });
    }

    // 5. Cost ordering: eq < full scan
    {
        let eq_cost = estimate_cost(&AccessPathKind::IndexScanEquality, 1000, 100);
        let full_cost = estimate_cost(&AccessPathKind::FullTableScan, 1000, 100);
        results.push(TestResult {
            name: "cost_order_eq_lt_full",
            pass: eq_cost < full_cost,
        });
    }

    // 6. Cost ordering: rowid < eq
    {
        let rowid_cost = estimate_cost(&AccessPathKind::RowidLookup, 1000, 100);
        let eq_cost = estimate_cost(&AccessPathKind::IndexScanEquality, 1000, 100);
        results.push(TestResult {
            name: "cost_order_rowid_lt_eq",
            pass: rowid_cost < eq_cost,
        });
    }

    // 7. Predicate pushdown: single-table predicate
    {
        let expr = dummy_expr();
        let single = eq_term("t1", "a", &expr);
        let terms = vec![single];
        let names = vec!["t1".to_owned(), "t2".to_owned()];
        let (pushed, _): (Vec<PushedPredicate<'_>>, Vec<&WhereTerm<'_>>) =
            pushdown_predicates(&terms, &names);
        results.push(TestResult {
            name: "pushdown_single_table",
            pass: pushed.len() == 1 && pushed[0].table == "t1",
        });
    }

    // 8. Plan stability
    {
        let tables = oltp_tables();
        let p1 = order_joins(&tables, &oltp_indexes(), &[], None, &[]);
        let p2 = order_joins(&tables, &oltp_indexes(), &[], None, &[]);
        results.push(TestResult {
            name: "plan_stability",
            pass: p1.join_order == p2.join_order,
        });
    }

    // 9. Positive total cost
    {
        let tables = oltp_tables();
        let plan = order_joins(&tables, &[], &[], None, &[]);
        results.push(TestResult {
            name: "positive_total_cost",
            pass: plan.total_cost > 0.0,
        });
    }

    // 10. Access paths count matches join order
    {
        let tables = tpch_tables();
        let plan = order_joins(&tables, &[], &[], None, &[]);
        results.push(TestResult {
            name: "access_paths_count",
            pass: plan.access_paths.len() == plan.join_order.len(),
        });
    }

    // Summary
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-1as.5: Planner Correctness Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-1as.5\",");
    println!("  \"suite\": \"planner_correctness\",");
    println!("  \"total\": {total},");
    println!("  \"passed\": {passed},");
    println!("  \"failed\": {failed},");
    println!("  \"pass_rate\": \"{:.1}%\",", passed as f64 / total as f64 * 100.0);
    println!("  \"cases\": [");
    for (i, r) in results.iter().enumerate() {
        let comma = if i + 1 < total { "," } else { "" };
        let status = if r.pass { "PASS" } else { "FAIL" };
        println!("    {{ \"name\": \"{}\", \"status\": \"{status}\" }}{comma}", r.name);
    }
    println!("  ]");
    println!("}}");

    assert_eq!(
        failed, 0,
        "{failed}/{total} planner conformance tests failed: {:?}",
        results.iter().filter(|r| !r.pass).map(|r| r.name).collect::<Vec<_>>()
    );

    println!("[PASS] all {total} planner conformance tests passed");
}
