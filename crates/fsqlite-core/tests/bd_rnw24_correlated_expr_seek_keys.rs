#![recursion_limit = "512"]
#![allow(
    clippy::await_holding_lock,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]

//! bd-rnw24: correlated fallback seek keys for `BoundOuterValue` nested inside
//! constant expressions (`t.b = u.b + 1`, `t.a = abs(u.a)`, `t.s = u.s || ''`).
//!
//! When the correlated fallback substitutes outer-row values into an inner
//! `WHERE`, a `BoundOuterValue` nested inside a `BinaryOp`, deterministic
//! `FunctionCall` (excluding `likely`/`unlikely`/`likelihood`), or non-`Plus`
//! `UnaryOp` is constant for that probe and carries no affinity or collation of
//! its own in SQLite. A bare `BoundOuterValue` at the root — or wrapped only in
//! `UnaryOp::Plus` or `CAST`, which inherit the outer column's declared
//! collation/affinity — must NOT be admitted as a raw seek key.
//!
//! Covers:
//! 1. Live `rusqlite` 3.54.1 differential parity across outer values of every
//!    storage class (INTEGER, REAL, TEXT, numeric text, NOCASE text, BLOB,
//!    NULL) under arithmetic, concatenation, and function calls, against
//!    INTEGER, TEXT (BINARY), NOCASE, BLOB, REAL, NUMERIC, typeless, rowid,
//!    composite, expression, and WITHOUT ROWID inner keys, for equalities and
//!    ranges, EXISTS/NOT EXISTS and scalar subqueries, in `:memory:` and in a
//!    file, ad hoc and prepared.
//! 2. Opcode-budget proof that nested-`BoundOuterValue` probes actually seek
//!    their full composite/single-column keys instead of walking runs (RED on
//!    unmodified main -> GREEN on fix).
//! 3. Same-invocation A/B benchmark (`#[ignore]`) against live bundled stock
//!    SQLite 3.54.1 on the 20,000-row `t_ab(a, b)` workload from bd-rnw24.

use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

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
    let params: Vec<rusqlite::types::Value> = vec![rusqlite::types::Value::Integer(1); bind];
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
    let params = vec![SqliteValue::Integer(1); bind];
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

/// Outer table `u` carries every SQLite storage class, numeric text forms,
/// mixed-case NOCASE text, blobs, negatives, and NULL.
/// Inner tables `t`, `k`, `ex`, and `wr` carry INTEGER, TEXT, NOCASE, BLOB,
/// REAL, NUMERIC, typeless, composite, expression, and WITHOUT ROWID keys.
const PARITY_SCHEMA: &str = "\
CREATE TABLE u(\
  id INTEGER PRIMARY KEY, \
  i INTEGER, \
  j INTEGER, \
  r REAL, \
  t TEXT, \
  nc TEXT COLLATE NOCASE, \
  bl BLOB, \
  x\
);\
INSERT INTO u VALUES \
  (1, 4, 4, 4.5, '5', 'abc', x'35', 4), \
  (2, -5, -6, -5.5, '-5', 'ABC', x'616263', '5'), \
  (3, 5, 4, 5.0, ' 5', 'AbC', x'35', 5.0), \
  (4, 0, -1, 0.0, '5.0', 'abd', x'30', 'abc'), \
  (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
  (6, 2, 1, 2.5, 'abc', '5', x'3530', x'35'), \
  (7, -6, -5, 3.0, '05', 'ABD', x'61', -5), \
  (8, 3, 2, 5.5, 'Abc', 'x', x'00', ' 5');\
CREATE TABLE t(\
  id INTEGER PRIMARY KEY, \
  ai INTEGER, \
  bt TEXT, \
  cn TEXT COLLATE NOCASE, \
  db BLOB, \
  er REAL, \
  fn_num NUMERIC, \
  gn\
);\
INSERT INTO t VALUES \
  (1, 5, '5', 'abc', x'35', 5.0, 5, 5), \
  (2, -5, '-5', 'ABC', '5', -5.5, '5.0', '5'), \
  (3, 5, '5.0', 'Abc', 5, 5.5, ' 5', 5.0), \
  (4, 3, ' 5', 'abd', -5, 3.5, -5, -5), \
  (5, 0, 'abc', 'ABC', 'abc', 0.0, 'abc', 'abc'), \
  (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
  (7, 6, 'ABC', '5', x'616263', 6.0, 3, x'35'), \
  (8, -4, '05', '-5', '-5', -4.5, 5.5, '-5'), \
  (9, 4, 'Abc', 'ABD', ' 5', 4.5, 4, ' 5'), \
  (10, 10, '5x', 'x', '5x', 10.0, 10, '5x'), \
  (11, 2, 'ab', 'ab', '35', 2.0, 2, '35'), \
  (-5, -5, 'abc', 'abc', '5.0', -5.0, -5, -5);\
INSERT INTO t WITH RECURSIVE s(v) AS (SELECT 20 UNION ALL SELECT v + 1 FROM s WHERE v < 80) \
  SELECT v, (v % 13) - 6, ((v % 13) - 6) || '', 'ab' || char(97 + (v % 4)), \
         ((v % 13) - 6) || '', ((v % 13) - 6) * 0.5, (v % 13) - 6, (v % 13) - 6 FROM s;\
CREATE INDEX t_ai ON t(ai);\
CREATE INDEX t_bt ON t(bt);\
CREATE INDEX t_cn ON t(cn);\
CREATE INDEX t_db ON t(db);\
CREATE INDEX t_er ON t(er);\
CREATE INDEX t_fn ON t(fn_num);\
CREATE INDEX t_gn ON t(gn);\
CREATE TABLE k(\
  id INTEGER PRIMARY KEY, \
  a INTEGER, \
  b INTEGER, \
  s TEXT, \
  nc TEXT COLLATE NOCASE, \
  bl BLOB\
);\
INSERT INTO k VALUES \
  (1, 4, 5, '5', 'abc', x'35'), \
  (2, 4, 3, '5x', 'ABC', '5'), \
  (3, -5, -5, '-5', 'Abc', 'abc'), \
  (4, 5, 5, ' 5', 'abd', x'35'), \
  (5, 0, 0, '5.0', 'ABD', '0'), \
  (6, NULL, 5, '5', 'abc', x'35'), \
  (7, 4, NULL, NULL, NULL, NULL), \
  (8, 2, 2, 'abc', '5', '35'), \
  (9, 3, 3, 'Abc', 'x', '5x');\
INSERT INTO k WITH RECURSIVE s(v) AS (SELECT 20 UNION ALL SELECT v + 1 FROM s WHERE v < 90) \
  SELECT v, (v % 9) - 4, (v % 11) - 5, ((v % 11) - 5) || '', \
         'ab' || char(97 + (v % 4)), ((v % 11) - 5) || '' FROM s;\
CREATE INDEX k_ab ON k(a, b);\
CREATE INDEX k_as ON k(a, s);\
CREATE INDEX k_anc ON k(a, nc);\
CREATE INDEX k_abl ON k(a, bl);\
CREATE TABLE ex(id INTEGER PRIMARY KEY, s TEXT);\
INSERT INTO ex SELECT id, bt FROM t;\
CREATE INDEX ex_lower ON ex(lower(s));\
CREATE TABLE wr(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID;\
INSERT INTO wr SELECT a, b, min(coalesce(s, '')) FROM k WHERE a IS NOT NULL AND b IS NOT NULL GROUP BY a, b;";

/// Nested-BoundOuterValue expressions (admitted as seek keys) plus controls
/// that MUST stay out of raw seeks (bare outer column, unary `+`, root `CAST`,
/// `COLLATE`, `likely`/`unlikely`).
const CORRELATED_PROBES: &[&str] = &[
    // Arithmetic over outer values of every storage class
    "u.i + 1",
    "u.i - 1",
    "u.i * 2",
    "u.i / 2",
    "-u.i",
    "~u.i",
    "u.r + 0.5",
    "u.t + 0",
    "u.x + 1",
    "CAST(u.t AS INTEGER) + 0",
    "u.i + ?1",
    // Concatenation
    "u.t || ''",
    "u.i || ''",
    "u.nc || ''",
    "u.bl || ''",
    "u.t || 'x'",
    "'-' || u.t",
    // Deterministic scalar functions
    "abs(u.i)",
    "abs(u.r)",
    "lower(u.t)",
    "upper(u.nc)",
    "lower(u.nc)",
    "coalesce(u.i, -5)",
    "length(u.t)",
    "substr(u.t, 1, 2)",
    "hex(u.bl)",
    // Non-seekable controls (planted-negative sensitive!)
    "u.nc",
    "+u.nc",
    "+(+u.nc)",
    "+u.i",
    "u.i",
    "u.t",
    "u.bl",
    "CAST(u.i AS TEXT)",
    "+CAST(u.nc AS TEXT)",
    "u.t COLLATE NOCASE",
    "(u.t COLLATE NOCASE) || ''",
    "likely(u.i)",
    "unlikely(u.nc)",
];

const INNER_COLUMNS: &[&str] = &["id", "ai", "bt", "cn", "db", "er", "fn_num", "gn"];

fn build_parity_queries() -> Vec<String> {
    let mut sqls = Vec::new();
    for &col in INNER_COLUMNS {
        for &probe in CORRELATED_PROBES {
            // EXISTS with inner column on left AND on right (operand-order sensitive
            // for bare outer NOCASE / INTEGER vs BINARY TEXT!).
            sqls.push(format!(
                "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.{col} = {probe}) ORDER BY u.id"
            ));
            sqls.push(format!(
                "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM t WHERE {probe} = t.{col}) ORDER BY u.id"
            ));
            // Scalar subquery
            sqls.push(format!(
                "SELECT u.id, (SELECT min(t.id) FROM t WHERE t.{col} = {probe}) FROM u ORDER BY u.id"
            ));
        }
    }
    // Composite keys across INTEGER, TEXT, NOCASE, BLOB, plus range keys, NOT EXISTS,
    // expression index, and WITHOUT ROWID.
    let extra = [
        // Composite equality: bare leading key + nested-expression second key
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.b = u.j + 1) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.b = abs(u.j)) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.b = -u.j) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.b = u.j + ?1) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.s = u.t || '') ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.s = u.t || 'x') ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.nc = lower(u.nc)) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.nc = upper(u.t)) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.bl = u.bl || '') ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.bl = hex(u.bl)) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = abs(u.i) AND k.b = u.j + 1) ORDER BY u.id",
        "SELECT u.id FROM u WHERE NOT EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.b = u.j + 1) ORDER BY u.id",
        // Composite scalar subqueries
        "SELECT u.id, (SELECT min(k.id) FROM k WHERE k.a = u.i AND k.b = u.j + 1) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM k WHERE k.a = u.i AND k.s = u.t || '') FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM k WHERE k.a = u.i AND k.nc = lower(u.nc)) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM k WHERE k.a = u.i AND k.bl = u.bl || '') FROM u ORDER BY u.id",
        // Range keys (rowid, single-column INTEGER/TEXT/NOCASE/BLOB, composite prefix + range)
        "SELECT u.id, (SELECT count(*) FROM t WHERE t.id > u.i + 1 AND t.id <= u.i + 5) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM t WHERE t.ai > u.i - 1 AND t.ai < u.i + 3) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM t WHERE t.ai BETWEEN u.i - 1 AND u.i + 2) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM t WHERE t.bt >= lower(u.t) AND t.bt <= upper(u.t)) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM t WHERE t.cn >= lower(u.nc) AND t.cn <= upper(u.nc)) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM t WHERE t.db >= u.bl || '') FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM k WHERE k.a = u.i AND k.b > u.j - 1) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM k WHERE k.a = u.i AND k.b BETWEEN u.j - 1 AND u.j + 2) FROM u ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND k.b > u.j + 1) ORDER BY u.id",
        // Expression index and WITHOUT ROWID
        "SELECT u.id, (SELECT count(*) FROM ex WHERE lower(ex.s) = lower(u.t)) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT count(*) FROM ex WHERE lower(ex.s) = u.nc || '') FROM u ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM wr WHERE wr.a = u.i AND wr.b = u.j + 1) ORDER BY u.id",
        "SELECT u.id, (SELECT wr.v FROM wr WHERE wr.a = u.i AND wr.b = u.j + 1) FROM u ORDER BY u.id",
        // Non-seekable controls on composite indexes (bare outer NOCASE on BINARY column, unary `+`)
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND u.nc = k.s) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND +u.nc = k.s) ORDER BY u.id",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM k WHERE k.a = u.i AND u.i = k.s) ORDER BY u.id",
    ];
    sqls.extend(extra.into_iter().map(str::to_owned));
    sqls
}

async fn compare_parity(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().expect("utf8"))
                .await
                .expect("native open"),
            rusqlite::Connection::open(dir.path().join("r.db")).expect("stock open"),
        )
    } else {
        (
            Connection::open(":memory:").await.expect("native mem"),
            rusqlite::Connection::open_in_memory().expect("stock mem"),
        )
    };
    f.execute_batch(PARITY_SCHEMA).await.expect("native schema");
    r.execute_batch(PARITY_SCHEMA).expect("stock schema");
    let mode = if file_backed { "file" } else { "memory" };
    let sqls = build_parity_queries();
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    for sql in &sqls {
        let bind = usize::from(sql.contains("?1"));
        let rr = rq(&r, sql, bind);
        for prepared in [false, true] {
            runs += 1;
            let fr = fq(&f, sql, bind, prepared).await;
            if fr != rr {
                diffs.push(format!(
                    "  [{mode}, prepared={prepared}] `{sql}`\n    frank = {fr:?}\n    stock = {rr:?}"
                ));
            }
        }
    }
    f.close().await.expect("close");
    (diffs, runs)
}

#[test]
fn correlated_expr_seek_keys_match_stock() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare_parity(false).await;
        let (file_diffs, file_runs) = compare_parity(true).await;
        diffs.extend(file_diffs);
        runs += file_runs;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} correlated statement run(s) vs live rusqlite 3.54.1:\n{}",
            diffs.len(),
            diffs.iter().take(25).cloned().collect::<Vec<_>>().join("\n")
        );
    });
}

/// Opcode-budget proof: on a 2,000-row inner table with 10 groups of 200 rows
/// per `a`-value and 200 outer rows, seeking `(t.a, t.b)` via a nested
/// `BoundOuterValue` expression stays O(N) (< 60,000 opcodes total), whereas
/// walking the 200-row run per outer row exceeds 300,000 opcodes.
#[test]
fn correlated_expr_seek_keys_seek_instead_of_walking_run() {
    const INNER_ROWS: i64 = 2000;
    const OUTER_ROWS: i64 = 200;
    const SEEK_OPCODE_LIMIT: u64 = 25_000;
    const SCAN_OPCODE_FLOOR: u64 = 120_000;

    const SEEK_SHAPES: &[&str] = &[
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a AND t.b = u.b + 1)",
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a = abs(u.neg_a) AND t.b = u.b + 1)",
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a = -u.neg_a AND t.b = u.b + 1)",
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.s = lower(u.su))",
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.s = u.sl || '')",
        "SELECT sum((SELECT t.id FROM t WHERE t.a = u.a AND t.b = u.b + 1 LIMIT 1)) FROM u",
    ];
    const SCAN_CONTROLS: &[&str] = &[
        // `t.b + 0` cannot seek on `b`, so it MUST walk the 200-row `t.a = u.a` run per outer row.
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a AND t.b + 0 = u.b + 1)",
    ];

    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let f = Connection::open(dir.path().join("seek_cost.db").to_str().expect("utf8"))
            .await
            .expect("native open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        let setup = format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT);\
             INSERT INTO t WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < {INNER_ROWS}) \
               SELECT i, i % 10, i, 'v' || i FROM seq;\
             CREATE INDEX t_ab ON t(a, b);\
             CREATE INDEX t_s ON t(s);\
             CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, neg_a INTEGER, b INTEGER, su TEXT, sl TEXT);\
             INSERT INTO u WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < {OUTER_ROWS}) \
               SELECT i, (i * 7) % 10, -((i * 7) % 10), i * 7 - 1, 'V' || (i * 7), 'v' || (i * 7) FROM seq;"
        );
        f.execute_batch(&setup).await.expect("native setup");
        r.execute_batch(&setup).expect("stock setup");

        let mut report = Vec::new();
        let mut violations = Vec::new();
        for sql in SEEK_SHAPES.iter().chain(SCAN_CONTROLS) {
            let expected = rq(&r, sql, 0).expect("stock query");
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let actual = fq(&f, sql, 0, false).await.expect("native query");
            let opcodes = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_enabled(false);
            assert_eq!(actual, expected, "result mismatch on `{sql}`");
            let is_control = SCAN_CONTROLS.contains(sql);
            report.push(format!("  {opcodes:>10} [control={is_control}] `{sql}`"));
            if is_control {
                if opcodes < SCAN_OPCODE_FLOOR {
                    violations.push(format!(
                        "scan control ran only {opcodes} opcodes (expected >= {SCAN_OPCODE_FLOOR}): `{sql}`"
                    ));
                }
            } else if opcodes > SEEK_OPCODE_LIMIT {
                violations.push(format!(
                    "seek shape ran {opcodes} opcodes (limit {SEEK_OPCODE_LIMIT}): `{sql}`"
                ));
            }
        }
        f.close().await.expect("close");
        eprintln!("bd-rnw24 opcode report:\n{}", report.join("\n"));
        assert!(
            violations.is_empty(),
            "bd-rnw24 seek-budget violations:\n{}\nFull report:\n{}",
            violations.join("\n"),
            report.join("\n")
        );
    });
}

/// Same-invocation A/B benchmark against bundled stock SQLite 3.54.1 on the
/// exact 20,000-row `t_ab(a, b)` workload from bd-rnw24.
/// Run with: `cargo test --profile release-perf -p fsqlite-core --test bd_rnw24_correlated_expr_seek_keys -- --ignored --nocapture`
#[test]
#[ignore = "perf benchmark — run explicitly under --profile release-perf"]
fn bd_rnw24_same_invocation_ab_vs_stock() {
    const ROWS: i64 = 20_000;
    const GROUPS: i64 = 100;
    const QUERY: &str =
        "SELECT count(*) FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a AND t.b = u.b + 1)";

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let f_path = dir.path().join("bench_f.db");
        let r_path = dir.path().join("bench_r.db");
        let f = Connection::open(f_path.to_str().expect("utf8"))
            .await
            .expect("native open");
        let r = rusqlite::Connection::open(&r_path).expect("stock open");
        let setup = format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);\
             INSERT INTO t WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < {ROWS}) \
               SELECT i, i % {GROUPS}, i FROM seq;\
             CREATE INDEX t_ab ON t(a, b);\
             CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);\
             INSERT INTO u WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < {ROWS}) \
               SELECT i, (i + 1) % {GROUPS}, i FROM seq;"
        );
        f.execute_batch(&setup).await.expect("native setup");
        r.execute_batch(&setup).expect("stock setup");

        let mut stock_ms = Vec::new();
        let mut frank_ms = Vec::new();
        let mut frank_count = 0_i64;
        for round in 0..3 {
            let t0 = Instant::now();
            let stock_count = r
                .query_row(QUERY, [], |row| row.get::<_, i64>(0))
                .expect("stock query");
            let s_elapsed = t0.elapsed().as_secs_f64() * 1000.0;

            let t1 = Instant::now();
            let rows = f.query(QUERY).await.expect("native query");
            let f_elapsed = t1.elapsed().as_secs_f64() * 1000.0;
            frank_count = match rows[0].values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("unexpected count {other:?}"),
            };
            assert_eq!(frank_count, stock_count, "round {round} count mismatch");
            stock_ms.push(s_elapsed);
            frank_ms.push(f_elapsed);
            eprintln!(
                "bd-rnw24 A/B round {round} ({ROWS} rows, {GROUPS} groups): count={frank_count} fsqlite={f_elapsed:.2}ms stock={s_elapsed:.2}ms ratio={:.2}x",
                f_elapsed / s_elapsed.max(0.001)
            );
        }
        stock_ms.sort_by(f64::total_cmp);
        frank_ms.sort_by(f64::total_cmp);
        eprintln!(
            "bd-rnw24 A/B MEDIAN ({ROWS} rows): fsqlite={:.2}ms stock={:.2}ms ratio={:.2}x count={frank_count}",
            frank_ms[1],
            stock_ms[1],
            frank_ms[1] / stock_ms[1].max(0.001)
        );
        f.close().await.expect("close");
    });
}
