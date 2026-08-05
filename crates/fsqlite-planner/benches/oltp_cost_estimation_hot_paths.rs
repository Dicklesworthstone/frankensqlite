//! Planner-only cost-estimation microbench for the mixed-OLTP statement shapes
//! used by `crates/fsqlite-e2e/src/bin/comprehensive_bench.rs`.
//!
//! This measures the schema-aware planner seam for `INTEGER PRIMARY KEY`
//! equality predicates. Range predicates use no-index range probes until the
//! planner grows an explicit rowid-range access-path representation.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::hint::black_box;
use std::time::Instant;

use fsqlite_ast::{BinaryOp as AstBinaryOp, ColumnRef, Expr, Literal, Span};
use fsqlite_planner::{
    AccessPath, RowidAliasHint, StatsSource, TableStats, WhereTerm,
    best_access_path_with_rowid_alias_hints, classify_where_term, cost_metrics_snapshot,
    reset_cost_metrics, snapshot_index_selection_totals,
};

const DEFAULT_ITERATIONS: u64 = 2_000_000;
const MIXED_OLTP_SEED_ROWS: u64 = 5_000;
const MIXED_OLTP_TABLE_PAGES: u64 = 128;

#[derive(Debug)]
struct BenchResult {
    ns_per_op: f64,
    planner_ops: u64,
    skipped_ops: u64,
    cost_estimates_total: u64,
    selection_delta: BTreeMap<String, u64>,
}

fn planner_table_stats() -> TableStats {
    TableStats {
        name: "bench".to_owned(),
        n_pages: MIXED_OLTP_TABLE_PAGES,
        n_rows: MIXED_OLTP_SEED_ROWS,
        source: StatsSource::Heuristic,
    }
}

fn eq_term(column: &str, value: i64) -> WhereTerm<'static> {
    let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Column(ColumnRef::bare(column), Span::ZERO)),
        op: AstBinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::Integer(value), Span::ZERO)),
        span: Span::ZERO,
    }));
    classify_where_term(expr)
}

fn ge_term(column: &str, value: i64) -> WhereTerm<'static> {
    let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Column(ColumnRef::bare(column), Span::ZERO)),
        op: AstBinaryOp::Ge,
        right: Box::new(Expr::Literal(Literal::Integer(value), Span::ZERO)),
        span: Span::ZERO,
    }));
    classify_where_term(expr)
}

fn lt_term(column: &str, value: i64) -> WhereTerm<'static> {
    let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Column(ColumnRef::bare(column), Span::ZERO)),
        op: AstBinaryOp::Lt,
        right: Box::new(Expr::Literal(Literal::Integer(value), Span::ZERO)),
        span: Span::ZERO,
    }));
    classify_where_term(expr)
}

#[allow(clippy::cast_precision_loss)]
fn ns_per_op(elapsed_ns: f64, ops: u64) -> f64 {
    elapsed_ns / ops as f64
}

#[allow(clippy::cast_precision_loss)]
fn cost_estimates_per_op(cost_estimates_total: u64, planner_ops: u64) -> f64 {
    cost_estimates_total as f64 / planner_ops as f64
}

fn selection_delta(
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    let mut keys = BTreeSet::new();
    keys.extend(before.keys().cloned());
    keys.extend(after.keys().cloned());

    keys.into_iter()
        .filter_map(|key| {
            let delta = after
                .get(&key)
                .copied()
                .unwrap_or(0)
                .saturating_sub(before.get(&key).copied().unwrap_or(0));
            (delta > 0).then_some((key, delta))
        })
        .collect()
}

fn selection_delta_string(selection_delta: &BTreeMap<String, u64>) -> String {
    if selection_delta.is_empty() {
        return "none".to_owned();
    }
    selection_delta
        .iter()
        .map(|(kind, count)| format!("{kind}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn capture_result(
    start: Instant,
    planner_ops: u64,
    skipped_ops: u64,
    selection_before: &BTreeMap<String, u64>,
) -> BenchResult {
    let elapsed_ns = start.elapsed().as_secs_f64() * 1_000_000_000.0;
    let cost_after = cost_metrics_snapshot();
    let selection_after = snapshot_index_selection_totals();
    BenchResult {
        ns_per_op: ns_per_op(elapsed_ns, planner_ops),
        planner_ops,
        skipped_ops,
        cost_estimates_total: cost_after.fsqlite_planner_cost_estimates_total,
        selection_delta: selection_delta(selection_before, &selection_after),
    }
}

fn run_shape(iterations: u64, terms: &[WhereTerm<'_>]) -> BenchResult {
    reset_cost_metrics();
    let table = planner_table_stats();
    let rowid_alias_hints = [RowidAliasHint::new("id")];
    let selection_before = snapshot_index_selection_totals();
    let start = Instant::now();
    for _ in 0..iterations {
        let access_path: AccessPath = best_access_path_with_rowid_alias_hints(
            black_box(&table),
            &[],
            terms,
            None,
            &rowid_alias_hints,
        );
        black_box(access_path);
    }
    capture_result(start, iterations, 0, &selection_before)
}

fn run_mixed_compile_mix(iterations: u64) -> BenchResult {
    reset_cost_metrics();
    let table = planner_table_stats();
    let rowid_alias_hints = [RowidAliasHint::new("id")];
    let point_terms = [eq_term("id", 1)];
    let range_terms = [ge_term("id", 100), lt_term("id", 150)];
    let empty_terms: [WhereTerm<'static>; 0] = [];

    let selection_before = snapshot_index_selection_totals();
    let start = Instant::now();
    let mut planner_ops = 0_u64;
    let mut skipped_ops = 0_u64;

    for step in 0..iterations {
        let bucket = step % 100;
        let terms: Option<&[WhereTerm<'_>]> = if bucket < 40 {
            Some(&point_terms)
        } else if bucket < 60 {
            Some(&range_terms)
        } else if bucket < 80 {
            Some(&empty_terms)
        } else if bucket < 95 {
            skipped_ops += 1;
            None
        } else {
            // UPDATE/DELETE in the mixed-OLTP workload reuse the same
            // `WHERE id = ?1` planner seam as point lookups.
            Some(&point_terms)
        };

        let Some(terms) = terms else {
            continue;
        };

        planner_ops += 1;
        let access_path: AccessPath = best_access_path_with_rowid_alias_hints(
            black_box(&table),
            &[],
            terms,
            None,
            &rowid_alias_hints,
        );
        black_box(access_path);
    }

    capture_result(start, planner_ops, skipped_ops, &selection_before)
}

fn parse_iterations() -> u64 {
    let mut args = env::args().skip(1);
    let mut iterations = DEFAULT_ITERATIONS;
    while let Some(arg) = args.next() {
        if arg == "--iterations"
            && let Some(value) = args.next()
        {
            match value.parse() {
                Ok(parsed) => iterations = parsed,
                Err(_) => {
                    eprintln!("invalid --iterations value: {value}");
                    std::process::exit(2);
                }
            }
        }
    }
    iterations
}

fn print_result(label: &str, result: &BenchResult) {
    println!(
        "oltp_cost_estimation_hot_paths {label}_ns_per_op={:.2} planner_ops={} skipped_ops={} cost_estimates_total={} cost_estimates_per_op={:.2} selections={}",
        result.ns_per_op,
        result.planner_ops,
        result.skipped_ops,
        result.cost_estimates_total,
        cost_estimates_per_op(result.cost_estimates_total, result.planner_ops),
        selection_delta_string(&result.selection_delta),
    );
}

fn main() {
    let iterations = parse_iterations();
    let point_terms = [eq_term("id", 1)];
    let range_terms = [ge_term("id", 100), lt_term("id", 150)];
    let empty_terms: [WhereTerm<'static>; 0] = [];

    let ipk_point = run_shape(iterations, &point_terms);
    let ipk_range = run_shape(iterations, &range_terms);
    let aggregate = run_shape(iterations, &empty_terms);
    let mixed = run_mixed_compile_mix(iterations);

    print_result("ipk_point_lookup", &ipk_point);
    print_result("ipk_range_count", &ipk_range);
    print_result("aggregate_full_scan", &aggregate);
    print_result("mixed_compile_mix", &mixed);
}
