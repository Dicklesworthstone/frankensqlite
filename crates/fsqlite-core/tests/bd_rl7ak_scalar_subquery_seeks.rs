#![recursion_limit = "512"]
#![allow(
    clippy::await_holding_lock,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]

//! bd-rl7ak: the compiled correlated scalar subquery (`emit_scalar_subquery`,
//! used by UPDATE SET, a DML WHERE and compiled SELECTs) scanned its whole
//! table once per outer row whenever it was an aggregate, and whenever its
//! WHERE bounded only the rowid. At 20,000 rows `UPDATE u SET n = (SELECT
//! count(*) FROM t WHERE t.g = u.g AND t.id < 10)` took 111 s; stock takes
//! 0.04 s. The subquery now seeks a rowid equality, an equality on a
//! single-column index, or integer-literal rowid bounds, and visits the rows
//! a scan would accept in the scan's order.
//!
//! A rowid equality seeks with `SeekRowid`, which truncated a key that is not
//! an integer: `t.id = 5.5` and `t.id = x'35'` found row 5, and `t.id = 1e300`
//! the largest rowid, in compiled EXISTS and scalar subqueries before this
//! change too. Like stock, the key now takes NUMERIC affinity and matches no
//! row unless that makes it an integer.
//!
//! `seeking_subqueries_match_stock` compares each statement with rusqlite's
//! bundled SQLite: the change count and the changed tables, or a SELECT's
//! rows. It covers order-sensitive aggregates (`group_concat`, a float `sum`),
//! empty ranges, NULL keys, probe values of every storage class, WITHOUT
//! ROWID and hidden-rowid inner tables, and bound parameters. In memory and in
//! a file, ad hoc and prepared.
//!
//! `seeking_subqueries_visit_few_rows` bounds the opcodes such an UPDATE runs
//! over 1,000 rows, which a scan per row exceeds many times over.

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

fn rq(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(params.iter()), |row| {
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
    params: &[SqliteValue],
    prepared: bool,
) -> Result<Vec<Vec<String>>, String> {
    let rows = match (prepared, params.is_empty()) {
        (false, true) => conn.query(sql).await,
        (false, false) => conn.query_with_params(sql, params).await,
        (true, _) => match conn.prepare(sql).await {
            Ok(stmt) if params.is_empty() => stmt.query().await,
            Ok(stmt) => stmt.query_with_params(params).await,
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

/// `t` has a NULL `g` every 11th row, a float `r`, so a float `sum` depends
/// on the order it adds in, and the largest rowid. `w` and `h` hold the same
/// keys without a rowid and with a hidden one.
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER, r REAL);\
CREATE INDEX t_g ON t(g);\
CREATE INDEX t_v ON t(v);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 80) \
  SELECT i, 'v' || (i % 7), CASE WHEN i % 11 = 0 THEN NULL ELSE i % 9 END, i % 5, i * 0.1 \
  FROM s;\
INSERT INTO t VALUES (9223372036854775807, 'vmax', 4, 4, 0.5);\
CREATE TABLE w(k INTEGER PRIMARY KEY, g INTEGER) WITHOUT ROWID;\
INSERT INTO w SELECT id, g FROM t;\
CREATE TABLE h(a INTEGER, g INTEGER);\
INSERT INTO h SELECT id, g FROM t ORDER BY id;";

// rusqlite's bundled SQLite enforces foreign keys by default; fsqlite does not.
const RESET: &str = "PRAGMA foreign_keys = OFF; DROP TABLE IF EXISTS u; DROP TABLE IF EXISTS p;\
     CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER, r REAL);\
     INSERT INTO u SELECT * FROM t ORDER BY id;\
     CREATE TABLE p(id INTEGER PRIMARY KEY, val, a, b);\
     INSERT INTO p(id, val) VALUES (1, 5), (2, 5.0), (3, '5'), (4, '5.0'), (5, ' 5'), \
       (6, x'35'), (7, NULL), (8, 5.5), (9, -1), (10, 9223372036854775807), (11, '3'), \
       (12, 3.0), (13, 'v3'), (14, 1e300), (15, '0x5'), (16, 9.3e18), (17, '5e0'), \
       (18, ' 5 '), (19, -0.0), (20, '9223372036854775807');";

const CHECKS: &[&str] = &["SELECT * FROM u ORDER BY id", "SELECT * FROM p ORDER BY id"];

const STATEMENTS: &[(&str, &[i64])] = &[
    // Integer-literal rowid bounds: an aggregate seeks the range.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g AND x.id < 10)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id > 70 AND x.n = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id BETWEEN 20 AND 40 \
         AND x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(x.v || x.id) FROM t AS x \
         WHERE x.id >= 10 AND x.id <= 30 AND x.id < 25 AND x.n <> u.n)",
        &[],
    ),
    (
        "UPDATE u SET r = (SELECT sum(x.r) FROM t AS x WHERE x.id > 5 AND x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET r = (SELECT avg(x.r) + count(*) FROM t AS x \
         WHERE x.rowid < 50 AND x.n = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE 30 >= x.id AND 10 < x.id \
         AND x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id > 1000 AND x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT max(x.id) FROM t AS x WHERE x.id > 1000)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id < 40 AND x.id > 50)",
        &[],
    ),
    // Bounds the seek must not take: correlated, negated, under OR, a parameter.
    (
        "UPDATE u SET n = (SELECT max(x.n) FROM t AS x WHERE x.id < u.id AND x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id > -5 AND x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id < 5 OR x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id < ?1 AND x.g = u.g)",
        &[25],
    ),
    // An equality on a single-column index, with and without a residual.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g AND x.n = 1)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(x.id, '.') FROM t AS x \
         WHERE x.g = u.g AND x.n > 1)",
        &[],
    ),
    (
        "UPDATE u SET r = (SELECT sum(x.r) FROM t AS x WHERE x.g = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.v = 'v' || u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.v = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(DISTINCT x.n) FROM t AS x WHERE x.g = u.g \
         AND x.id < 60)",
        &[],
    ),
    // A rowid equality.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id = u.id + 3 AND x.n > 1)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT max(x.v) FROM t AS x WHERE x.id = u.n * 7)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x \
         WHERE x.id = (SELECT max(id) FROM t) - u.n)",
        &[],
    ),
    // The first matching row of a non-aggregate subquery.
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.g = u.g AND x.id > 20)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT x.v || x.id FROM t AS x WHERE x.id >= 30 AND x.n = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.id < 10 AND x.n = u.n \
         ORDER BY x.id DESC)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.n FROM t AS x WHERE x.id = u.id + 1 ORDER BY x.n DESC)",
        &[],
    ),
    // Inner tables without a rowid, and with a hidden one.
    (
        "UPDATE u SET n = (SELECT count(*) FROM w WHERE w.k < 10 AND w.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM h WHERE h.rowid > 60 AND h.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT max(h.a) FROM h WHERE h.rowid = u.id + 2)",
        &[],
    ),
    // Probe values of every storage class.
    (
        "UPDATE p SET a = (SELECT count(*) FROM t WHERE t.id = p.val), \
         b = (SELECT group_concat(t.id) FROM t WHERE t.g = p.val AND t.id < 40)",
        &[],
    ),
    (
        "UPDATE p SET a = (SELECT max(t.v) FROM t WHERE t.v = p.val), \
         b = (SELECT t.id FROM t WHERE t.g = p.val AND t.id > 30)",
        &[],
    ),
    (
        "UPDATE p SET a = (SELECT sum(t.n) FROM t WHERE t.id = p.val AND t.n >= 0), \
         b = (SELECT count(*) FROM t WHERE t.id < 20 AND t.g = p.val)",
        &[],
    ),
    // DELETE decides by such a subquery.
    (
        "DELETE FROM u WHERE (SELECT count(*) FROM t AS x WHERE x.g = u.g AND x.id < 30) > 2",
        &[],
    ),
    (
        "DELETE FROM u WHERE (SELECT sum(x.n) FROM t AS x WHERE x.g = u.g AND x.n > 2) > 10",
        &[],
    ),
    (
        "DELETE FROM u WHERE (SELECT x.id FROM t AS x WHERE x.id > 75 AND x.n = u.n) IS NULL",
        &[],
    ),
    // Bound parameters, numbered and anonymous.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = ?1 AND x.id < 40) \
         WHERE u.id > ?2",
        &[3, 10],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.n = ? AND x.id < 40 \
         AND x.g = u.g) WHERE u.id > ?",
        &[2, 10],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.n = ? AND x.id = u.id + ?)",
        &[1, 4],
    ),
    // SELECT forms.
    (
        "SELECT u.id, (SELECT count(*) FROM t AS x WHERE x.g = u.g AND x.id < 10) \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT group_concat(x.id) FROM t AS x WHERE x.g = u.g AND x.n = 1) \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT x.v FROM t AS x WHERE x.id > 70 AND x.n = u.n) \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT p.id, EXISTS (SELECT 1 FROM t WHERE t.id = p.val), \
         (SELECT t.v FROM t WHERE t.id = p.val), \
         (SELECT count(*) FROM t WHERE t.rowid = p.val AND t.n >= 0) FROM p ORDER BY p.id",
        &[],
    ),
    (
        "SELECT p.id FROM p WHERE EXISTS (SELECT 1 FROM t WHERE t.id = p.val) ORDER BY p.id",
        &[],
    ),
];

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
    for (sql, params) in STATEMENTS {
        let values: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
        for prepared in [false, true] {
            runs += 1;
            f.execute_batch(RESET).await.unwrap();
            r.execute_batch(RESET).unwrap();
            let path = format!("[{db}, prepared={prepared}] `{sql}` {params:?}");
            if sql.starts_with("SELECT") {
                let (fr, rr) = (fq(&f, sql, &values, prepared).await, rq(&r, sql, params));
                if fr != rr {
                    diffs.push(format!("  {path}\n     frank= {fr:?}\n     stock= {rr:?}"));
                }
                continue;
            }
            let stock = r
                .execute(sql, rusqlite::params_from_iter(params.iter()))
                .map_err(|e| e.to_string());
            let frank = match (prepared, values.is_empty()) {
                (false, true) => f.execute(sql).await,
                (false, false) => f.execute_with_params(sql, &values).await,
                (true, _) => match f.prepare(sql).await {
                    Ok(stmt) if values.is_empty() => stmt.execute().await,
                    Ok(stmt) => stmt.execute_with_params(&values).await,
                    Err(e) => Err(e),
                },
            }
            .map_err(|e| e.to_string());
            let changes_match = match (&frank, &stock) {
                (Ok(frank), Ok(stock)) => frank == stock,
                (Err(_), Err(_)) => true,
                _ => false,
            };
            if !changes_match {
                diffs.push(format!(
                    "  {path}\n     changes: frank= {frank:?} stock= {stock:?}"
                ));
            }
            for check in CHECKS {
                let (fr, rr) = (fq(&f, check, &[], false).await, rq(&r, check, &[]));
                if fr != rr {
                    diffs.push(format!(
                        "  {path}\n     `{check}`:\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn seeking_subqueries_match_stock() {
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

/// Rows in `t` and `u` for the cost check.
const COST_ROWS: u64 = 1000;

/// The opcodes one UPDATE of all `COST_ROWS` rows may run. Each seek below
/// visits at most ten inner rows per outer row, about 35 to 215 opcodes per
/// outer row; a scan of `t` per outer row runs about 15,000.
const COST_LIMIT: u64 = COST_ROWS * 1000;

#[test]
fn seeking_subqueries_visit_few_rows() {
    // Each shape seeks; the last is a control that must scan.
    const SHAPES: &[&str] = &[
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g AND x.id < 10)",
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id > 990 AND x.n = u.n)",
        "UPDATE u SET n = (SELECT sum(x.n) FROM t AS x WHERE x.id BETWEEN 500 AND 505 \
         AND x.g <> u.g)",
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g AND x.n = 1)",
        "UPDATE u SET n = (SELECT max(x.id) FROM t AS x WHERE x.g = u.g)",
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.id = u.id + 3 AND x.n > 1)",
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.id > 990 AND x.n = u.n + 7)",
    ];
    const SCAN_CONTROL: &str =
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.n = u.n AND x.id < u.id)";
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("cost.db").to_str().unwrap())
            .await
            .unwrap();
        f.execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, n INTEGER);\
             CREATE INDEX t_g ON t(g);\
             INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
               WHERE i < {COST_ROWS}) SELECT i, i % 250, i % 5 FROM s;\
             CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER, n INTEGER);\
             INSERT INTO u SELECT * FROM t;"
        ))
        .await
        .unwrap();
        let mut report = Vec::new();
        let mut over = Vec::new();
        for sql in SHAPES.iter().chain([&SCAN_CONTROL]) {
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let changed = f.execute(sql).await.unwrap();
            let opcodes = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_enabled(false);
            assert_eq!(changed, usize::try_from(COST_ROWS).unwrap(), "`{sql}`");
            report.push(format!("  {opcodes:>10} `{sql}`"));
            let is_control = *sql == SCAN_CONTROL;
            if (opcodes > COST_LIMIT) != is_control {
                over.push(format!(
                    "  {opcodes} opcodes, control={is_control}: `{sql}`"
                ));
            }
        }
        f.close().await.unwrap();
        eprintln!(
            "opcodes per UPDATE (limit {COST_LIMIT}):\n{}",
            report.join("\n")
        );
        assert!(
            over.is_empty(),
            "the seeks must stay under {COST_LIMIT} opcodes and the scan control over it:\n{}",
            over.join("\n")
        );
    });
}
