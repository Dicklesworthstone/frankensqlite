#![recursion_limit = "512"]

//! bd-ct8zm: LIMIT must stop row evaluation on JOIN, GROUP BY / WINDOW,
//! and derived-table / CTE compound queries, matching live bundled
//! SQLite 3.54.1 (`rusqlite`).
//!
//! Each case runs on both `:memory:` and a file-backed database, through both
//! direct `query()` / `query_with_params()` and `prepare().query()` /
//! `query_with_params()`, and compares exact outcomes (rows or error kind)
//! against live bundled `rusqlite 0.41.1` (SQLite 3.54.1).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fsqlite_core::connection::Connection;
use fsqlite_func::scalar::ScalarFunction;
use fsqlite_types::value::SqliteValue;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Rows(Vec<Vec<String>>),
    Err(String),
}

fn fmt_fsqlite_val(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(i) => format!("I:{i}"),
        SqliteValue::Float(f) => format!("R:{f:?}"),
        SqliteValue::Text(s) => format!("T:{s}"),
        SqliteValue::Blob(b) => format!("B:{b:?}"),
    }
}

fn fmt_rusqlite_val(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(i) => format!("I:{i}"),
        rusqlite::types::Value::Real(f) => format!("R:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("T:{s}"),
        rusqlite::types::Value::Blob(b) => format!("B:{b:?}"),
    }
}

fn classify_error(msg: &str) -> String {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("integer overflow") {
        "integer overflow".to_owned()
    } else if lower.contains("datatype mismatch") {
        "datatype mismatch".to_owned()
    } else if lower.contains("no such column") {
        "no such column".to_owned()
    } else if lower.contains("no such table") {
        "no such table".to_owned()
    } else {
        lower
    }
}

fn stock_query(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Outcome {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return Outcome::Err(classify_error(&e.to_string())),
    };
    let col_count = stmt.column_count();
    let bound: Vec<rusqlite::types::Value> = params
        .iter()
        .copied()
        .map(rusqlite::types::Value::Integer)
        .collect();
    let rows_iter = match stmt.query_map(rusqlite::params_from_iter(bound.iter()), |row| {
        let mut cols = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: rusqlite::types::Value = row.get(i)?;
            cols.push(fmt_rusqlite_val(&val));
        }
        Ok(cols)
    }) {
        Ok(iter) => iter,
        Err(e) => return Outcome::Err(classify_error(&e.to_string())),
    };
    let mut out = Vec::new();
    for r in rows_iter {
        match r {
            Ok(row) => out.push(row),
            Err(e) => return Outcome::Err(classify_error(&e.to_string())),
        }
    }
    Outcome::Rows(out)
}

async fn frank_query_direct(conn: &Connection, sql: &str, params: &[i64]) -> Outcome {
    let bound: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
    let res = if bound.is_empty() {
        conn.query(sql).await
    } else {
        conn.query_with_params(sql, &bound).await
    };
    match res {
        Ok(rows) => Outcome::Rows(
            rows.into_iter()
                .map(|r| r.values().iter().map(fmt_fsqlite_val).collect())
                .collect(),
        ),
        Err(e) => Outcome::Err(classify_error(&e.to_string())),
    }
}

async fn frank_query_prepared(conn: &Connection, sql: &str, params: &[i64]) -> Outcome {
    let stmt = match conn.prepare(sql).await {
        Ok(s) => s,
        Err(e) => return Outcome::Err(classify_error(&e.to_string())),
    };
    let bound: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
    let res = if bound.is_empty() {
        stmt.query().await
    } else {
        stmt.query_with_params(&bound).await
    };
    match res {
        Ok(rows) => Outcome::Rows(
            rows.into_iter()
                .map(|r| r.values().iter().map(fmt_fsqlite_val).collect())
                .collect(),
        ),
        Err(e) => Outcome::Err(classify_error(&e.to_string())),
    }
}

const SETUP_SQL: &str = "\
CREATE TABLE e(k INTEGER PRIMARY KEY, x INTEGER);
INSERT INTO e VALUES (1, 1), (2, -9223372036854775808), (3, 5);
CREATE TABLE e_first_min(k INTEGER PRIMARY KEY, x INTEGER);
INSERT INTO e_first_min VALUES (1, -9223372036854775808), (2, 5);
CREATE TABLE u(a INTEGER PRIMARY KEY);
INSERT INTO u VALUES (1), (2);
CREATE TABLE s(k INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO s VALUES (1, 10), (2, 80);
CREATE TABLE big(x INTEGER);
INSERT INTO big VALUES (9223372036854775807), (1);
";

const CASES: &[(&str, &str, &[i64])] = &[
    // ── Shape 1: JOIN ... LIMIT ──
    (
        "join_limit_1_skips_overflow_row",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 LIMIT 1",
        &[],
    ),
    (
        "join_limit_1_param",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 LIMIT ?1",
        &[1],
    ),
    (
        "join_limit_0_literal",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 LIMIT 0",
        &[],
    ),
    (
        "join_limit_0_param",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 LIMIT ?1",
        &[0],
    ),
    (
        "join_limit_1_offset_1_skips_first_row_projection",
        "SELECT abs(e_first_min.x) FROM e_first_min JOIN u ON u.a = 1 LIMIT 1 OFFSET 1",
        &[],
    ),
    (
        "join_where_overflow_after_limit_1",
        "SELECT e.k FROM e JOIN u ON u.a = 1 WHERE abs(e.x) > 0 LIMIT 1",
        &[],
    ),
    (
        "join_on_overflow_after_limit_1",
        "SELECT e.k FROM e JOIN u ON abs(e.x) = 1 LIMIT 1",
        &[],
    ),
    (
        "join_left_limit_1_skips_overflow_row",
        "SELECT abs(e.x) FROM e LEFT JOIN u ON u.a = 1 LIMIT 1",
        &[],
    ),
    (
        "join_order_by_pk_limit_1_skips_non_key_projection",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 ORDER BY e.k LIMIT 1",
        &[],
    ),
    (
        "join_order_by_pk_desc_limit_1_skips_non_key_projection",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 ORDER BY e.k DESC LIMIT 1",
        &[],
    ),
    (
        "join_order_by_min_x_limit_1_overflows_on_winner",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 ORDER BY e.x LIMIT 1",
        &[],
    ),
    (
        "join_order_by_abs_limit_1_must_evaluate_all",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 ORDER BY abs(e.x) LIMIT 1",
        &[],
    ),
    (
        "join_order_by_ordinal_1_limit_1_must_evaluate_all",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 ORDER BY 1 LIMIT 1",
        &[],
    ),
    (
        "join_distinct_limit_1",
        "SELECT DISTINCT abs(e.x) FROM e JOIN u ON u.a = 1 LIMIT 1",
        &[],
    ),
    // ── Shape 2: GROUP BY / WINDOW / AGGREGATE ... LIMIT 0 ──
    (
        "group_by_limit_0_literal",
        "SELECT abs(x) FROM e GROUP BY x LIMIT 0",
        &[],
    ),
    (
        "group_by_limit_0_param",
        "SELECT abs(x) FROM e GROUP BY x LIMIT ?1",
        &[0],
    ),
    (
        "group_by_limit_0_expr",
        "SELECT abs(x) FROM e GROUP BY x LIMIT 1 - 1",
        &[],
    ),
    (
        "group_by_join_limit_0",
        "SELECT abs(e.x) FROM e JOIN u ON u.a = 1 GROUP BY e.x LIMIT 0",
        &[],
    ),
    (
        "window_sum_limit_0",
        "SELECT sum(abs(x)) OVER () FROM e LIMIT 0",
        &[],
    ),
    (
        "window_row_number_limit_0",
        "SELECT abs(x), row_number() OVER (ORDER BY x) FROM e LIMIT 0",
        &[],
    ),
    (
        "group_by_window_limit_0",
        "SELECT abs(x), sum(x) OVER () FROM e GROUP BY x LIMIT 0",
        &[],
    ),
    (
        "implicit_aggregate_sum_overflow_limit_0",
        "SELECT sum(x) FROM big LIMIT 0",
        &[],
    ),
    (
        "order_by_overflow_limit_0",
        "SELECT abs(x) FROM e ORDER BY x LIMIT 0",
        &[],
    ),
    (
        "group_by_limit_1_must_still_overflow",
        "SELECT abs(x) FROM e GROUP BY x LIMIT 1",
        &[],
    ),
    (
        "group_by_limit_bad_type_must_error",
        "SELECT abs(x) FROM e GROUP BY x LIMIT 'abc'",
        &[],
    ),
    (
        "group_by_limit_0_unknown_column_still_errors",
        "SELECT nosuch FROM e GROUP BY x LIMIT 0",
        &[],
    ),
    // ── Shape 3: Derived table and CTE over a compound ──
    (
        "scalar_subquery_derived_compound",
        "SELECT (SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)))",
        &[],
    ),
    (
        "derived_compound_limit_1",
        "SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) LIMIT 1",
        &[],
    ),
    (
        "derived_compound_where_matches_first_arm_limit_1",
        "SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) WHERE v = 80 LIMIT 1",
        &[],
    ),
    (
        "derived_compound_where_misses_first_arm_overflows",
        "SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) WHERE v = 999 LIMIT 1",
        &[],
    ),
    (
        "nested_derived_compound_limit_1",
        "SELECT v FROM (SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808))) LIMIT 1",
        &[],
    ),
    (
        "derived_compound_order_by_must_overflow",
        "SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) ORDER BY v LIMIT 1",
        &[],
    ),
    (
        "derived_compound_limit_0",
        "SELECT v FROM (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) LIMIT 0",
        &[],
    ),
    (
        "cte_default_compound_limit_1",
        "WITH c AS (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) SELECT v FROM c LIMIT 1",
        &[],
    ),
    (
        "cte_not_materialized_compound_limit_1",
        "WITH c AS NOT MATERIALIZED (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) SELECT v FROM c LIMIT 1",
        &[],
    ),
    (
        "cte_materialized_compound_limit_1_must_overflow",
        "WITH c AS MATERIALIZED (SELECT v FROM s WHERE k = 2 UNION ALL SELECT abs(-9223372036854775808)) SELECT v FROM c LIMIT 1",
        &[],
    ),
    (
        "cte_default_limit_0",
        "WITH c AS (SELECT abs(-9223372036854775808)) SELECT * FROM c LIMIT 0",
        &[],
    ),
    (
        "cte_materialized_limit_0",
        "WITH c AS MATERIALIZED (SELECT abs(-9223372036854775808)) SELECT * FROM c LIMIT 0",
        &[],
    ),
];

async fn run_suite(label: &str, fconn: &Connection, rconn: &rusqlite::Connection) {
    fconn.execute_batch(SETUP_SQL).await.expect("frank setup");
    rconn.execute_batch(SETUP_SQL).expect("stock setup");

    let mut diffs = Vec::new();
    for &(name, sql, params) in CASES {
        let stock = stock_query(rconn, sql, params);
        let direct = frank_query_direct(fconn, sql, params).await;
        let prepared = frank_query_prepared(fconn, sql, params).await;
        if direct != stock {
            diffs.push(format!(
                "[{label} direct] `{name}`:\n  sql:   {sql}\n  stock: {stock:?}\n  frank: {direct:?}"
            ));
        }
        if prepared != stock {
            diffs.push(format!(
                "[{label} prepared] `{name}`:\n  sql:   {sql}\n  stock: {stock:?}\n  frank: {prepared:?}"
            ));
        }
    }
    assert!(
        diffs.is_empty(),
        "[{label}] {} mismatch(es) across {} cases:\n{}",
        diffs.len(),
        CASES.len(),
        diffs.join("\n")
    );
}

#[test]
fn limit_stops_evaluation_on_join_group_by_window_and_derived_compound_oracle() {
    asupersync::test_utils::run_test(|| async {
        // 1. :memory:
        let fmem = Connection::open(":memory:").await.expect("fmem");
        let rmem = rusqlite::Connection::open_in_memory().expect("rmem");
        run_suite("memory", &fmem, &rmem).await;

        // 2. File-backed
        let dir = tempfile::tempdir().expect("tempdir");
        let fpath = dir.path().join("frank.db");
        let rpath = dir.path().join("stock.db");
        let ffile = Connection::open(fpath.to_str().expect("utf8"))
            .await
            .expect("ffile");
        let rfile = rusqlite::Connection::open(&rpath).expect("rfile");
        run_suite("file", &ffile, &rfile).await;
    });
}

struct CountingIdentityFn {
    calls: Arc<AtomicUsize>,
}

impl ScalarFunction for CountingIdentityFn {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(args.first().cloned().unwrap_or(SqliteValue::Null))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &'static str {
        "count_eval"
    }
}

/// Prove work reduction on `JOIN ... LIMIT 1`: over a 2,000-row table joined
/// against a 2-row table, `SELECT count_eval(big_e.v) FROM big_e JOIN u ON u.a = 1 LIMIT 1`
/// must evaluate `count_eval` exactly once (not 2,000 times), and live stock
/// SQLite 3.54.1 proves it also stops at row 1 by returning row 1 cleanly even
/// when row 2 holds `INT64_MIN` under `abs(big_e.v)`.
#[test]
fn join_limit_1_evaluates_only_returned_rows_matching_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let fpath = dir.path().join("work_frank.db");
        let rpath = dir.path().join("work_stock.db");

        let fconn = Connection::open(fpath.to_str().expect("utf8"))
            .await
            .expect("fconn");
        let rconn = rusqlite::Connection::open(&rpath).expect("rconn");

        let frank_calls = Arc::new(AtomicUsize::new(0));
        fconn.register_nondeterministic_scalar_function(CountingIdentityFn {
            calls: Arc::clone(&frank_calls),
        });

        let setup = "\
CREATE TABLE big_e(k INTEGER PRIMARY KEY, v INTEGER);
WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 2000)
INSERT INTO big_e SELECT x, CASE WHEN x = 2 THEN -9223372036854775808 ELSE x * 3 END FROM cnt;
CREATE TABLE u(a INTEGER PRIMARY KEY);
INSERT INTO u VALUES (1), (2);
";
        fconn.execute_batch(setup).await.expect("frank setup");
        rconn.execute_batch(setup).expect("stock setup");

        // Live stock + frank oracle on the 2,000-row table with row 2 = INT64_MIN:
        // stopping after row 1 returns [[3]] without evaluating row 2's abs().
        let abs_sql = "SELECT abs(big_e.v) FROM big_e JOIN u ON u.a = 1 LIMIT 1";
        let stock_abs = stock_query(&rconn, abs_sql, &[]);
        assert_eq!(stock_abs, Outcome::Rows(vec![vec!["I:3".to_owned()]]));
        let frank_abs = frank_query_direct(&fconn, abs_sql, &[]).await;
        assert_eq!(frank_abs, stock_abs);

        // Work counter on frank: projection must be evaluated exactly once, not 2,000 times.
        let count_sql = "SELECT count_eval(big_e.v) FROM big_e JOIN u ON u.a = 1 LIMIT 1";
        let frank_out = frank_query_direct(&fconn, count_sql, &[]).await;
        assert_eq!(frank_out, Outcome::Rows(vec![vec!["I:3".to_owned()]]));

        let fc = frank_calls.load(Ordering::Relaxed);
        eprintln!(
            "bd-ct8zm work check (2000-row JOIN ... LIMIT 1): frank projection evals={fc} (stock stops at row 1)"
        );
        assert_eq!(
            fc, 1,
            "JOIN ... LIMIT 1 evaluated projection {fc} times instead of 1"
        );
    });
}
