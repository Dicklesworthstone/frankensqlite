#![allow(
    clippy::await_holding_lock,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]

//! bd-d4rbi: every seek in an aggregate SELECT was gated off when the result
//! list held a bare (non-aggregate) column, so `SELECT b, count(*) FROM t
//! WHERE a = 5` scanned the table in rowid order. A bare column takes the
//! first row visited (or the min()/max() row), and stock visits the rows in
//! its seek order, so the scan was slow and also returned another row's
//! values. The seeks now take bare columns.
//!
//! `bare_column_aggregates_match_stock` compares each statement with
//! rusqlite's bundled SQLite, bare values included, over every aggregate seek
//! shape:
//! - rowid: equality, range, IN, equality with a residual;
//! - index: equality, range, IN;
//! - composite index: equality prefix with a range, equality prefix with a
//!   residual.
//!
//! The result lists hold bare column references, the rowid, expressions and
//! constants, next to count, sum, min, max, DISTINCT and FILTER aggregates.
//! Each statement runs in memory and in a file, ad hoc and prepared.
//!
//! A lone min() or max() over a column with an index of its own is stock's
//! min/max optimization (bd-5kdea). Stock picks between that index and the
//! WHERE's index by its no-stat4 cost hierarchy (rowid point > equality > IN >
//! two-sided range > min/max index > one-sided range on another index), walks
//! the one it picked in its min/max direction, and stops at the first match.
//! Every lone min()/max() shape — including `c > 'v20'` and lone `max()` — is
//! now verified against rusqlite below.
//!
//! `bare_column_aggregates_seek` and `bd_5kdea_lone_minmax_early_exit_seeks`
//! bound the opcodes such statements run over 1,000 rows, which a scan exceeds
//! many times over.

use std::sync::{Mutex, MutexGuard};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str, bind: usize) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = st.column_count();
    let params = vec![rusqlite::types::Value::Integer(5); bind];
    st.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn fq(
    conn: &Connection,
    sql: &str,
    bind: usize,
    prepared: bool,
) -> Result<Vec<Vec<String>>, String> {
    let params = vec![SqliteValue::Integer(5); bind];
    let rows = match (prepared, bind > 0) {
        (false, false) => conn.query(sql).await,
        (false, true) => conn.query_with_params(sql, &params).await,
        (true, _) => match conn.prepare(sql).await {
            Ok(stmt) if bind == 0 => stmt.query().await,
            Ok(stmt) => stmt.query_with_params(&params).await,
            Err(e) => Err(e),
        },
    };
    rows.map(|rows| {
        rows.iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect()
    })
    .map_err(|e| e.to_string())
}

/// `a`, `b` and `c` are not monotone in the rowid, so the first row in index
/// order is not the first in rowid order; `b` has NULLs and ties.
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT, n INTEGER);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
  SELECT i, i % 11, CASE WHEN i % 29 = 0 THEN NULL ELSE (i * 7) % 17 END, \
  'v' || ((i * 5) % 23), i % 5 FROM s;\
CREATE INDEX t_ab ON t(a, b);\
CREATE INDEX t_b ON t(b);\
CREATE INDEX t_c ON t(c);";

/// Result lists: bare columns of every kind next to aggregates.
const LISTS: &[&str] = &[
    "b, count(*)",
    "id, count(*)",
    "count(*), c",
    "n, sum(b)",
    "id, b, c, n, count(*), sum(n)",
    "b + 1, count(*), id * 2",
    "'x', count(*)",
    "count(*), 7, NULL",
    "id, min(b)",
    "id, max(b)",
    "id, min(a)",
    "id, max(a)",
    "id, min(c)",
    "id, max(c)",
    "id, min(id)",
    "id, max(id)",
    "id, max(b) + 1",
    "id, coalesce(max(b), -1)",
    "id, c, min(n)",
    "id, max(n)",
    "id, max(n), count(*)",
    "c, min(b), max(b)",
    "id, count(DISTINCT b)",
    "b, sum(n) FILTER (WHERE n > 1)",
    "id, min(n) FILTER (WHERE n > 2)",
    "id, avg(b), total(n), group_concat(n)",
];

/// WHERE clauses, one or more per aggregate seek shape, and ones that match
/// no row.
const WHERES: &[&str] = &[
    "a = 5",
    "a = ?1",
    "a = 5 AND b = 3",
    "a = 7 AND b = ?1",
    "a = 99",
    "id = 7",
    "id = ?1",
    "id = 100000",
    "id > 380",
    "id BETWEEN 100 AND 120",
    "id < 10",
    "id >= ?1 * 70",
    "a IN (5, 3)",
    "a IN (3, 5, 3, 99)",
    "b > 10",
    "b BETWEEN 2 AND 4",
    "c > 'v20'",
    "c BETWEEN 'v1' AND 'v12'",
    "a = 5 AND b > 6",
    "a = 5 AND b BETWEEN 2 AND 9",
    "a = 4 AND b < 3",
    "a = 5 AND n = 2",
    "a = 5 AND c LIKE 'v1%'",
    "id IN (7, 3, 99, 3)",
    "id = 7 AND n >= 0",
    "id = 7 AND n > 100",
    "n + 0 = 2",
];

fn statements() -> Vec<String> {
    let mut sqls = Vec::new();
    for list in LISTS {
        // Unconstrained min()/max() covering/index choice is bd-5kdea; unconstrained
        // non-min/max covering-index scans (e.g. `SELECT id, count(*) FROM t`) are bd-1bs3h.
        if list.contains("min(") || list.contains("max(") {
            sqls.push(format!("SELECT {list} FROM t"));
        }
        for wh in WHERES {
            sqls.push(format!("SELECT {list} FROM t WHERE {wh}"));
        }
    }
    sqls
}

async fn compare(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    f.execute_batch(SCHEMA).await.unwrap();
    r.execute_batch(SCHEMA).unwrap();
    let db = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    for sql in statements() {
        let bind = r.prepare(&sql).map_or(0, |st| st.parameter_count());
        for prepared in [false, true] {
            runs += 1;
            // An error compares by its presence: the messages differ between engines.
            let fr = fq(&f, &sql, bind, prepared).await.map_err(|_| ());
            let rr = rq(&r, &sql, bind).map_err(|_| ());
            if fr != rr {
                diffs.push(format!(
                    "  [{db}, prepared={prepared}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
                ));
            }
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn bare_column_aggregates_match_stock() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare(false).await;
        let (file_diffs, file_runs) = compare(true).await;
        diffs.extend(file_diffs);
        runs += file_runs;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} statement run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// Rows in the cost table.
const COST_ROWS: u64 = 1000;

/// The opcodes one statement may run. A seek below visits at most a few
/// dozen rows; a scan of the table runs over 10,000.
const COST_LIMIT: u64 = 1000;

#[test]
fn bare_column_aggregates_seek() {
    // Each shape seeks; the last one is a control that must scan.
    const SHAPES: &[&str] = &[
        "SELECT b, count(*) FROM t WHERE a = 7",
        "SELECT a, count(*) FROM t WHERE a = ?1",
        "SELECT 'x', count(*) FROM t WHERE id > 990",
        "SELECT id, sum(n) FROM t WHERE id BETWEEN 10 AND 20",
        "SELECT n, count(*) FROM t WHERE id = 7",
        "SELECT n, count(*) FROM t WHERE id = 7 AND n > 0",
        "SELECT n, count(*) FROM t WHERE id IN (5, 7, 9)",
        "SELECT n, count(*) FROM t WHERE a IN (3, 5)",
        "SELECT id, min(b) FROM t WHERE a = 7",
        "SELECT b, count(*) FROM t WHERE a = 7 AND b > 2",
        "SELECT id, count(*) FROM t WHERE a = 7 AND n > 0",
        "SELECT n, count(*) FROM t WHERE s > 'v98'",
    ];
    const SCAN_CONTROLS: &[&str] = &["SELECT b, count(*) FROM t WHERE n + 0 = 5"];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("cost.db").to_str().unwrap())
            .await
            .unwrap();
        f.execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT, n INTEGER);\
             INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
               WHERE i < {COST_ROWS}) SELECT i, i % 250, i % 7, 'v' || (i % 100), i \
               FROM s;\
             CREATE INDEX t_ab ON t(a, b);\
             CREATE INDEX t_s ON t(s);"
        ))
        .await
        .unwrap();
        let mut report = Vec::new();
        let mut over = Vec::new();
        for sql in SHAPES.iter().chain(SCAN_CONTROLS) {
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let params = if sql.contains('?') {
                vec![SqliteValue::Integer(7)]
            } else {
                Vec::new()
            };
            let result = f.query_with_params(sql, &params).await.map(|_| ());
            let opcodes = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_enabled(false);
            result.unwrap();
            report.push(format!("  {opcodes:>10} `{sql}`"));
            let is_control = SCAN_CONTROLS.contains(sql);
            if (opcodes > COST_LIMIT) != is_control {
                over.push(format!(
                    "  {opcodes} opcodes, control={is_control}: `{sql}`"
                ));
            }
        }
        f.close().await.unwrap();
        eprintln!(
            "opcodes per statement (limit {COST_LIMIT}):\n{}",
            report.join("\n")
        );
        assert!(
            over.is_empty(),
            "the seeks must stay under {COST_LIMIT} opcodes and the scan control over it:\n{}",
            over.join("\n")
        );
    });
}

async fn compare_schema_queries(
    schema_sql: &str,
    queries: &[&str],
    file_backed: bool,
) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    f.execute_batch(schema_sql).await.unwrap();
    r.execute_batch(schema_sql).unwrap();
    let db = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut runs = 0_usize;
    for &sql in queries {
        let bind = r.prepare(sql).map_or(0, |st| st.parameter_count());
        for prepared in [false, true] {
            runs += 1;
            let fr = fq(&f, sql, bind, prepared).await.map_err(|_| ());
            let rr = rq(&r, sql, bind).map_err(|_| ());
            if fr != rr {
                diffs.push(format!(
                    "  [{db}, prepared={prepared}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
                ));
            }
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn bd_5kdea_lone_minmax_edge_cases_match_stock() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        // 1. Cross-index competition, hints, HAVING, FILTER, NULLs on full t_ab/t_b/t_c schema.
        const COMPETE_QUERIES: &[&str] = &[
            "SELECT id, min(b) FROM t WHERE c > 'v20' AND c < 'v22'",
            "SELECT id, max(b) FROM t WHERE c > 'v20' AND c < 'v22'",
            "SELECT id, min(b) FROM t WHERE c < 'v5'",
            "SELECT id, max(b) FROM t WHERE c < 'v5'",
            "SELECT id, min(b) FROM t WHERE a > 5",
            "SELECT id, max(b) FROM t WHERE a > 5",
            "SELECT id, min(b) FROM t WHERE a BETWEEN 2 AND 5",
            "SELECT id, max(b) FROM t WHERE a BETWEEN 2 AND 5",
            "SELECT id, min(b) FROM t WHERE b > 10 AND n = 2",
            "SELECT id, max(b) FROM t WHERE b > 10 AND n = 2",
            "SELECT id, min(b) FROM t WHERE b < 5 AND n = 2",
            "SELECT id, max(b) FROM t WHERE b < 5 AND n = 2",
            "SELECT id, min(b) FROM t WHERE b > 10 AND c = 'v5'",
            "SELECT id, max(b) FROM t WHERE b > 10 AND c = 'v5'",
            "SELECT id, min(b) FROM t WHERE b BETWEEN 2 AND 4 AND c = 'v5'",
            "SELECT id, max(b) FROM t WHERE b BETWEEN 2 AND 4 AND c = 'v5'",
            "SELECT id, min(b) FROM t WHERE b > 10 AND c > 'v20'",
            "SELECT id, max(b) FROM t WHERE b > 10 AND c > 'v20'",
            "SELECT id, min(b) FROM t WHERE b BETWEEN 2 AND 4 AND c > 'v20'",
            "SELECT id, max(b) FROM t WHERE b BETWEEN 2 AND 4 AND c > 'v20'",
            "SELECT id, min(b) FROM t WHERE b BETWEEN 2 AND 4 AND c BETWEEN 'v1' AND 'v12'",
            "SELECT id, max(b) FROM t WHERE b BETWEEN 2 AND 4 AND c BETWEEN 'v1' AND 'v12'",
            "SELECT id, min(b) FROM t WHERE a = 5 AND b > 6 AND n = 2",
            "SELECT id, max(b) FROM t WHERE a = 5 AND b > 6 AND n = 2",
            "SELECT id, min(b) FROM t WHERE a = 5 AND b < 3 AND n = 2",
            "SELECT id, max(b) FROM t WHERE a = 5 AND b < 3 AND n = 2",
            "SELECT id, min(b) FROM t WHERE a = 5 AND c = 'v1'",
            "SELECT id, max(b) FROM t WHERE a = 5 AND c = 'v1'",
            "SELECT id, min(b) FROM t WHERE a = 5 AND c > 'v20'",
            "SELECT id, max(b) FROM t WHERE a = 5 AND c > 'v20'",
            "SELECT id, min(b) FROM t WHERE a > 5 AND c = 'v5'",
            "SELECT id, max(b) FROM t WHERE a > 5 AND c = 'v5'",
            "SELECT id, min(b) FROM t WHERE b = 3 AND c > 'v20'",
            "SELECT id, max(b) FROM t WHERE b = 3 AND c > 'v20'",
            "SELECT id, min(b) FROM t WHERE b IS NULL",
            "SELECT id, max(b) FROM t WHERE b IS NULL",
            "SELECT id, min(b) FROM t WHERE b IS NULL AND n = 2",
            "SELECT id, max(b) FROM t WHERE b IS NULL AND n = 2",
            "SELECT id, min(b) FROM t WHERE a IN (5, 3) AND b IS NULL",
            "SELECT id, max(b) FROM t WHERE a IN (5, 3) AND b IS NULL",
            "SELECT id, min(b) FROM t WHERE id IN (58, 203) AND b IS NULL",
            "SELECT id, max(b) FROM t WHERE id IN (58, 203) AND b IS NULL",
            "SELECT id, min(b) FROM t WHERE c IN ('v1', 'v5') AND b IS NULL",
            "SELECT id, max(b) FROM t WHERE c IN ('v1', 'v5') AND b IS NULL",
            "SELECT id, min(b) FROM t WHERE b = 5",
            "SELECT id, max(b) FROM t WHERE b = 5",
            "SELECT id, min(b) FROM t WHERE b IS NOT NULL",
            "SELECT id, max(b) FROM t WHERE b IS NOT NULL",
            "SELECT id, min(b) FROM t WHERE b IN (2, 10)",
            "SELECT id, max(b) FROM t WHERE b IN (2, 10)",
            "SELECT id, min(b) FROM t WHERE c IN ('v1', 'v5')",
            "SELECT id, max(b) FROM t WHERE c IN ('v1', 'v5')",
            "SELECT id, min(b) FROM t WHERE a IN (5, 3) AND n = 2",
            "SELECT id, max(b) FROM t WHERE a IN (5, 3) AND n = 2",
            "SELECT id, max(b) FROM t WHERE id = 7 AND a = 5",
            "SELECT id, min(id) FROM t WHERE id BETWEEN 100 AND 120 AND n = 2",
            "SELECT id, max(id) FROM t WHERE id BETWEEN 100 AND 120 AND n = 2",
            "SELECT id, max(b) FROM t WHERE a = 5 HAVING max(b) > 0",
            "SELECT id, max(b) FROM t WHERE a = 5 HAVING n >= 0",
            "SELECT id, max(b) FILTER (WHERE n > 1) FROM t WHERE a = 5",
            "SELECT id, max(b) FROM t INDEXED BY t_c WHERE c > 'v20'",
            "SELECT id, max(b) FROM t INDEXED BY t_b WHERE c > 'v20'",
            "SELECT id, max(b) FROM t NOT INDEXED WHERE a = 5",
            "SELECT id, typeof(id), max(b) FROM t WHERE n = 999",
        ];

        // 2. Only composite `t_ab(a, b)` (bd-5kdea covering-index scan example).
        const ONLY_AB_SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT, n INTEGER);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
  SELECT i, i % 11, CASE WHEN i % 29 = 0 THEN NULL ELSE (i * 7) % 17 END, \
  'v' || ((i * 5) % 23), i % 5 FROM s;\
CREATE INDEX t_ab ON t(a, b);";
        const ONLY_AB_QUERIES: &[&str] = &[
            "SELECT id, max(b) FROM t",
            "SELECT id, min(b) FROM t",
            "SELECT id, c, max(b) FROM t",
            "SELECT id, c, min(b) FROM t",
            "SELECT id, max(a) FROM t WHERE a BETWEEN 5 AND 6",
            "SELECT id, min(a) FROM t WHERE a BETWEEN 5 AND 6",
            "SELECT id, max(b) FROM t WHERE a = 5",
            "SELECT id, min(b) FROM t WHERE a = 5",
            "SELECT id, c, max(b) FROM t WHERE a = 5",
        ];

        // 3. DESC indexes + all-NULL blocks + IN-list all-NULL blocks.
        const DESC_NULL_SCHEMA: &str = "\
CREATE TABLE d(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, n INTEGER);\
INSERT INTO d VALUES \
  (1, 1, NULL, 1), (2, 1, 10, 2), (3, 1, 10, 1), (4, 1, 5, 2), (5, 1, NULL, 2), \
  (6, 2, NULL, 1), (7, 2, NULL, 2), \
  (8, 3, NULL, 1), (9, 3, NULL, 2);\
CREATE INDEX d_b_desc ON d(b DESC);\
CREATE INDEX d_ab_desc ON d(a, b DESC);";
        const DESC_NULL_QUERIES: &[&str] = &[
            "SELECT id, min(b) FROM d",
            "SELECT id, max(b) FROM d",
            "SELECT id, min(b) FROM d WHERE n = 2",
            "SELECT id, max(b) FROM d WHERE n = 2",
            "SELECT id, min(b) FROM d WHERE a = 1",
            "SELECT id, max(b) FROM d WHERE a = 1",
            "SELECT id, min(b) FROM d WHERE a = 2",
            "SELECT id, max(b) FROM d WHERE a = 2",
            "SELECT id, min(b) FROM d WHERE a IN (1, 2)",
            "SELECT id, max(b) FROM d WHERE a IN (1, 2)",
            "SELECT id, min(b) FROM d WHERE a IN (2, 3)",
            "SELECT id, max(b) FROM d WHERE a IN (2, 3)",
            "SELECT id, min(b) FROM d WHERE id > 5",
            "SELECT id, max(b) FROM d WHERE id > 5",
            "SELECT id, min(b) FROM d WHERE b > 4 AND n = 2",
            "SELECT id, max(b) FROM d WHERE b > 4 AND n = 2",
            "SELECT id, min(b) FROM d WHERE b < 11 AND n = 2",
            "SELECT id, max(b) FROM d WHERE b < 11 AND n = 2",
            "SELECT id, min(b) FROM d WHERE b BETWEEN 5 AND 10",
            "SELECT id, max(b) FROM d WHERE b BETWEEN 5 AND 10",
            "SELECT id, max(b) FROM d WHERE b IS NOT NULL AND a = 2",
            "SELECT id, min(b) FROM d WHERE b IS NOT NULL AND a = 2",
            "SELECT id, min(b) FROM d WHERE b IS NULL",
            "SELECT id, max(b) FROM d WHERE b IS NULL",
            "SELECT id, max(b) FROM d WHERE n = 999",
        ];

        // 4. Covering index tie-break, multi-column prefix, and COLLATE NOCASE.
        const COLL_SCHEMA: &str = "\
CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT COLLATE NOCASE, d TEXT, n INTEGER);\
INSERT INTO u VALUES \
  (1, 1, 10, 'b', 'b', 1), (2, 1, 10, 'B', 'B', 2), \
  (3, 1, 5, 'a', 'a', 1), (4, 1, 5, 'A', 'A', 2), \
  (5, 2, 10, 'c', 'c', 1), (6, 2, 5, 'C', 'C', 2);\
CREATE INDEX u_b ON u(b);\
CREATE INDEX u_bc ON u(b, c);\
CREATE INDEX u_c_nc ON u(c);\
CREATE INDEX u_d_bin ON u(d);\
CREATE INDEX u_abn ON u(a, b, n);";
        const COLL_QUERIES: &[&str] = &[
            "SELECT id, c, max(b) FROM u",
            "SELECT id, c, min(b) FROM u",
            "SELECT id, n, max(b) FROM u",
            "SELECT id, max(c) FROM u",
            "SELECT id, min(c) FROM u",
            "SELECT id, max(d COLLATE NOCASE) FROM u",
            "SELECT id, max(n) FROM u WHERE a = 1 AND b = 10",
            "SELECT id, min(n) FROM u WHERE a = 1 AND b = 10",
        ];

        let mut diffs = Vec::new();
        let mut runs = 0_usize;
        for (schema_sql, queries) in [
            (SCHEMA, COMPETE_QUERIES),
            (ONLY_AB_SCHEMA, ONLY_AB_QUERIES),
            (DESC_NULL_SCHEMA, DESC_NULL_QUERIES),
            (COLL_SCHEMA, COLL_QUERIES),
        ] {
            for file_backed in [false, true] {
                let (d, r) = compare_schema_queries(schema_sql, queries, file_backed).await;
                diffs.extend(d);
                runs += r;
            }
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} bd-5kdea runs from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
fn bd_5kdea_lone_minmax_early_exit_seeks() {
    // Each lone min/max shape below must exit early (well under COST_LIMIT opcodes
    // over 1,000 rows); the non-indexed scan control must exceed COST_LIMIT.
    const EARLY_EXIT_SHAPES: &[&str] = &[
        "SELECT id, max(b) FROM t WHERE a = 5",
        "SELECT id, min(b) FROM t WHERE a = 5",
        "SELECT id, max(a) FROM t WHERE a BETWEEN 5 AND 6",
        "SELECT id, min(a) FROM t WHERE a BETWEEN 5 AND 6",
        "SELECT id, max(b) FROM t WHERE a = 5 AND b > 2",
        "SELECT id, max(b) FROM t WHERE a = 5 AND n > 0",
        "SELECT id, min(b) FROM t WHERE s > 'v50'",
        "SELECT id, max(b) FROM t WHERE s > 'v50'",
        "SELECT id, max(b) FROM t WHERE a IN (3, 5)",
        "SELECT id, min(b) FROM t WHERE b = 5",
        "SELECT id, max(b) FROM t WHERE b = 5",
        "SELECT id, max(id) FROM t WHERE n > 0",
        "SELECT id, min(id) FROM t WHERE n > 0",
    ];
    const SCAN_CONTROLS: &[&str] = &["SELECT b, max(n) FROM t WHERE n + 0 = 5"];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("minmax_cost.db").to_str().unwrap())
            .await
            .unwrap();
        f.execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT, n INTEGER);\
             INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
               WHERE i < {COST_ROWS}) SELECT i, i % 11, (i * 7) % 17, 'v' || (i % 100), i \
               FROM s;\
             CREATE INDEX t_ab ON t(a, b);\
             CREATE INDEX t_b ON t(b);\
             CREATE INDEX t_s ON t(s);"
        ))
        .await
        .unwrap();
        let mut report = Vec::new();
        let mut over = Vec::new();
        for sql in EARLY_EXIT_SHAPES.iter().chain(SCAN_CONTROLS) {
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let result = f.query(sql).await.map(|_| ());
            let opcodes = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_enabled(false);
            result.unwrap();
            report.push(format!("  {opcodes:>10} `{sql}`"));
            let is_control = SCAN_CONTROLS.contains(sql);
            if (opcodes > COST_LIMIT) != is_control {
                over.push(format!(
                    "  {opcodes} opcodes, control={is_control}: `{sql}`"
                ));
            }
        }
        f.close().await.unwrap();
        eprintln!(
            "bd-5kdea early-exit opcodes (limit {COST_LIMIT}):\n{}",
            report.join("\n")
        );
        assert!(
            over.is_empty(),
            "early-exit lone min/max must stay under {COST_LIMIT} opcodes and control over it:\n{}",
            over.join("\n")
        );
    });
}
