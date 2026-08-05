use std::env;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use fsqlite_ast::{BinaryOp as AstBinaryOp, ColumnRef, Expr, Literal, Span};
use fsqlite_planner::{
    AccessPath, AccessPathKind, IndexInfo, PlannerFeatureFlags, QueryPlan, QueryPlanner,
    StatsSource, TableStats, WhereTerm, classify_where_term, order_joins_with_hints_and_features,
};

fn table_stats(name: &str, n_pages: u64, n_rows: u64) -> TableStats {
    TableStats {
        name: name.to_owned(),
        n_pages,
        n_rows,
        source: StatsSource::Heuristic,
    }
}

fn index_info(name: &str, table: &str, columns: &[&str], unique: bool, n_pages: u64) -> IndexInfo {
    IndexInfo {
        name: name.to_owned(),
        table: table.to_owned(),
        columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        unique,
        n_pages,
        source: StatsSource::Heuristic,
        partial_where: None,
        expression_columns: vec![],
    }
}

fn eq_term(column: &str) -> WhereTerm<'static> {
    let expr: &'static Expr = Box::leak(Box::new(Expr::BinaryOp {
        left: Box::new(Expr::Column(ColumnRef::bare(column), Span::ZERO)),
        op: AstBinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::Integer(1), Span::ZERO)),
        span: Span::ZERO,
    }));
    classify_where_term(expr)
}

fn sample_query_plan() -> QueryPlan {
    QueryPlan {
        join_order: vec!["users".to_owned()],
        access_paths: vec![AccessPath {
            table: "users".to_owned(),
            kind: AccessPathKind::IndexScanEquality,
            index: Some("idx_users_email".to_owned()),
            estimated_cost: 4.0,
            estimated_rows: 1.0,
            time_travel: None,
            probe: None,
        }],
        join_segments: vec![],
        total_cost: 4.0,
        morsel_eligibility: None,
    }
}

fn bench_cached_plan_hit(iterations: u64) -> (f64, u64) {
    let mut planner = QueryPlanner::new();
    let sql = "SELECT * FROM users WHERE email = ?1";
    let schema_cookie = 7;
    let warmed = planner.cached_plan(sql, schema_cookie, sample_query_plan);
    black_box(Rc::clone(&warmed));

    let mut unexpected_misses = 0;
    let start = Instant::now();
    for _ in 0..iterations {
        let plan = planner.cached_plan(black_box(sql), black_box(schema_cookie), sample_query_plan);
        unexpected_misses += u64::from(!Rc::ptr_eq(&plan, &warmed));
        black_box(plan);
    }
    (
        start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64,
        unexpected_misses,
    )
}

fn bench_order_joins_with_cache_hit(iterations: u64) -> (f64, u64) {
    let mut planner = QueryPlanner::new();
    let sql = "SELECT email FROM users WHERE email = ?1";
    let schema_cookie = 11;
    let tables = [table_stats("users", 512, 200_000)];
    let indexes = [index_info("idx_users_email", "users", &["email"], true, 64)];
    let where_terms = [eq_term("email")];
    let needed_columns = [String::from("email")];
    let feature_flags = PlannerFeatureFlags::default();

    let warmed = planner.order_joins_with_cache(
        sql,
        schema_cookie,
        &tables,
        &indexes,
        &where_terms,
        Some(&needed_columns),
        &[],
        None,
        None,
        feature_flags,
    );
    black_box(Rc::clone(&warmed));

    let mut unexpected_misses = 0;
    let start = Instant::now();
    for _ in 0..iterations {
        let plan = planner.order_joins_with_cache(
            black_box(sql),
            black_box(schema_cookie),
            &tables,
            &indexes,
            &where_terms,
            Some(&needed_columns),
            &[],
            None,
            None,
            feature_flags,
        );
        unexpected_misses += u64::from(!Rc::ptr_eq(&plan, &warmed));
        black_box(plan);
    }
    (
        start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64,
        unexpected_misses,
    )
}

/// Cache-MISS single-table planning path: calls the join planner directly so
/// every iteration rebuilds the plan (no plan-cache short-circuit). This is the
/// dominant OLTP shape — one table, one equality predicate — and exercises the
/// `n == 1` arm of `order_joins_with_hints_and_features`.
fn bench_order_joins_single_table_miss(iterations: u64) -> f64 {
    let tables = [table_stats("users", 512, 200_000)];
    let indexes = [index_info("idx_users_email", "users", &["email"], true, 64)];
    let where_terms = [eq_term("email")];
    let needed_columns = [String::from("email")];
    let feature_flags = PlannerFeatureFlags::default();

    let start = Instant::now();
    for _ in 0..iterations {
        let plan = order_joins_with_hints_and_features(
            black_box(&tables),
            black_box(&indexes),
            black_box(&where_terms),
            Some(black_box(&needed_columns)),
            &[],
            None,
            None,
            feature_flags,
        );
        black_box(plan);
    }
    start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64
}

fn parse_iterations() -> u64 {
    let mut args = env::args().skip(1);
    let mut iterations = 2_000_000_u64;
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

fn main() {
    let iterations = parse_iterations();
    let (cached_plan_ns, cached_plan_misses) = bench_cached_plan_hit(iterations);
    let (order_joins_ns, order_joins_misses) = bench_order_joins_with_cache_hit(iterations);
    let order_joins_miss_ns = bench_order_joins_single_table_miss(iterations);

    println!(
        "plan_cache_hot_paths cached_plan_hit_ns_per_op={cached_plan_ns:.2} iterations={iterations} unexpected_misses={cached_plan_misses}"
    );
    println!(
        "plan_cache_hot_paths order_joins_with_cache_hit_ns_per_op={order_joins_ns:.2} iterations={iterations} unexpected_misses={order_joins_misses}"
    );
    println!(
        "plan_cache_hot_paths order_joins_single_table_miss_ns_per_op={order_joins_miss_ns:.2} iterations={iterations}"
    );
}
