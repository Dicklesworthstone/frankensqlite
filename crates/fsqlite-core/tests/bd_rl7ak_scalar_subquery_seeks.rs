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
//! 0.04 s. The subquery now seeks a rowid equality, integer-literal rowid
//! bounds, equalities on an index's leading columns, or bounds on the index
//! column after them. Rowid seeks and a single-column equality visit the rows
//! a scan would accept in the scan's order; other index slices visit them in
//! index order, as stock's walk of the same index does.
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
/// keys without a rowid and with a hidden one. `m` has a three-column index
/// whose last column is not in rowid order, NULL keys, and indexes whose
/// leading term is descending or NOCASE; `q` has a UNIQUE constraint's index.
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER, r REAL);\
CREATE INDEX t_g ON t(g);\
CREATE INDEX t_v ON t(v);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 80) \
  SELECT i, 'v' || (i % 7), CASE WHEN i % 11 = 0 THEN NULL ELSE i % 9 END, i % 5, i * 0.1 \
  FROM s;\
INSERT INTO t VALUES (9223372036854775807, 'vmax', 4, 4, 0.5);\
CREATE INDEX t_r ON t(r);\
CREATE TABLE c(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);\
CREATE INDEX c_s ON c(s);\
INSERT INTO c VALUES (1, 'a'), (2, 'B'), (3, 'b'), (4, 'C'), (5, 'A'), (6, 'd'), (7, 'c'), \
  (8, NULL), (9, 'bb'), (10, 'Ab');\
CREATE TABLE w(k INTEGER PRIMARY KEY, g INTEGER) WITHOUT ROWID;\
INSERT INTO w SELECT id, g FROM t;\
CREATE TABLE h(a INTEGER, g INTEGER);\
INSERT INTO h SELECT id, g FROM t ORDER BY id;\
CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d INTEGER, \
  e TEXT COLLATE NOCASE);\
INSERT INTO m WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 90) \
  SELECT i, CASE WHEN i % 13 = 0 THEN NULL ELSE i % 4 END, \
  CASE WHEN i % 17 = 0 THEN NULL ELSE 'b' || (i % 3) END, ((i * 37) % 91) * 0.25, i % 6, \
  CASE WHEN i % 5 = 0 THEN 'x' ELSE 'X' END || (i % 3) FROM s;\
CREATE INDEX m_abc ON m(a, b, c);\
CREATE INDEX m_da ON m(d DESC, a);\
CREATE INDEX m_bd ON m(b COLLATE NOCASE, d);\
CREATE INDEX m_ea ON m(e, a);\
CREATE TABLE q(a INTEGER, b INTEGER, n INTEGER, UNIQUE(a, b));\
INSERT INTO q SELECT n, g, max(id) FROM t GROUP BY n, g;";

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
    // Bounds on the column of a single-column index.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g BETWEEN u.n AND u.n + 2)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(x.id) FROM t AS x \
         WHERE x.g > u.g AND x.g <= u.g + 1)",
        &[],
    ),
    (
        "UPDATE u SET r = (SELECT sum(x.r) FROM t AS x WHERE x.g < u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.g >= u.g + 3)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE u.n > x.g AND x.n <> u.n)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT max(x.v) || group_concat(x.id) FROM t AS x \
         WHERE x.v > 'v' || u.n AND x.v < 'v6')",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.r BETWEEN u.r - 1 AND u.r + 1)",
        &[],
    ),
    (
        "UPDATE u SET r = (SELECT total(x.r) FROM t AS x WHERE x.r > u.n * 1.5 \
         AND x.r <= u.n * 1.5 + 0.35)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g > '3' AND x.g < 7.5)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.v > u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g >= u.g AND x.g < u.g)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(c.id) FROM c WHERE c.s >= 'b' \
         AND c.s < substr('ABCD', u.n + 1, 1))",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(c.id) FROM c WHERE c.s > 'a' COLLATE BINARY \
         AND u.n > 1)",
        &[],
    ),
    ("UPDATE u SET v = (SELECT c.s FROM c WHERE c.s <= u.v)", &[]),
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
    (
        "UPDATE p SET a = (SELECT count(*) FROM t WHERE t.g >= p.val AND t.g < p.val + 2), \
         b = (SELECT group_concat(t.id) FROM t WHERE t.v <= p.val)",
        &[],
    ),
    (
        "UPDATE p SET a = (SELECT group_concat(c.id) FROM c WHERE c.s > p.val), \
         b = (SELECT count(*) FROM t WHERE t.r < p.val)",
        &[],
    ),
    // Equalities on the leading columns of a multi-column index, then bounds
    // on the next column. The walk follows the index.
    (
        "UPDATE u SET v = (SELECT group_concat(m.id) FROM m WHERE m.a = u.n \
         AND m.b = 'b' || (u.id % 3))",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(m.id) FROM m WHERE m.a = u.g % 4)",
        &[],
    ),
    (
        "UPDATE u SET r = (SELECT total(m.c) FROM m WHERE m.b = 'b1' AND m.a = u.n \
         AND m.c > u.r * 10 AND m.c <= u.r * 10 + 6)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM m WHERE m.a = u.n AND m.b = 'b2' \
         AND m.c = ((u.id * 37) % 91) * 0.25)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(m.id) FROM m WHERE m.a >= u.n \
         AND m.a < u.n + 2)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(m.id) FROM m WHERE m.a = u.n \
         AND m.b BETWEEN 'b0' AND 'b1')",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT m.id || ',' || m.b FROM m WHERE u.n = m.a AND m.b > 'b0')",
        &[],
    ),
    // Terms the index cannot order: a TEXT column under numeric affinity, an
    // explicit collation, a NOCASE or a descending leading key term.
    (
        "UPDATE u SET n = (SELECT count(*) FROM m WHERE m.a = u.n AND m.b = u.g)",
        &[],
    ),
    (
        "UPDATE u SET v = (SELECT group_concat(m.id) FROM m WHERE m.a = u.n \
         AND m.b = 'B1' COLLATE NOCASE)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT max(m.id) FROM m WHERE m.e = 'X' || (u.id % 3) \
         AND m.a = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT max(m.id) FROM m WHERE m.d = u.n AND m.a = 1)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT q.n FROM q WHERE q.a = u.n AND q.b = u.g)",
        &[],
    ),
    (
        "UPDATE p SET a = (SELECT group_concat(m.id) FROM m WHERE m.a = p.val \
         AND m.b = 'b1'), b = (SELECT count(*) FROM m WHERE m.a = 1 AND m.b = p.val)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM m WHERE m.a = ?1 AND m.b = 'b0' \
         AND m.c < ?2) WHERE u.id > ?3",
        &[2, 15, 40],
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
    (
        "DELETE FROM u WHERE (SELECT count(*) FROM m WHERE m.a = u.n AND m.b = 'b1') > 6",
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
    (
        "SELECT u.id, (SELECT count(*) FROM m WHERE m.a = u.n AND m.b = 'b0'), \
         (SELECT max(m.c) FROM m WHERE m.a = u.n AND m.c > u.r) FROM u ORDER BY u.id",
        &[],
    ),
    // bd-c6b16: SELECT-list correlated aggregates walk index ranges and
    // multi-column index equalities in index order, matching stock.
    (
        "SELECT u.id, (SELECT count(*) FROM t AS x WHERE x.g BETWEEN u.n AND u.n + 1), \
         (SELECT group_concat(x.id) FROM t AS x WHERE x.r > u.r AND x.r < u.r + 0.25) \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT group_concat(m.id) FROM m WHERE m.a = u.n), \
         (SELECT group_concat(m.id) FROM m WHERE m.a = u.n AND m.b = 'b0') \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id FROM u WHERE (SELECT group_concat(m.id) FROM m \
         WHERE m.a = u.n AND m.b = 'b0') IS NOT NULL ORDER BY u.id",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g > ?1 AND x.g <= u.g) \
         WHERE u.id > ?2",
        &[2, 10],
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
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g BETWEEN u.g AND u.g + 1)",
        "UPDATE u SET n = (SELECT max(x.n) FROM t AS x WHERE x.g > u.g AND x.g < u.g + 3 \
         AND x.n < u.n)",
        "UPDATE u SET n = (SELECT count(*) FROM k WHERE k.a = u.id % 100 AND k.b = u.id % 7)",
        "UPDATE u SET n = (SELECT max(k.id) FROM k WHERE k.a = u.id % 100)",
        "UPDATE u SET n = (SELECT count(*) FROM k WHERE k.a = u.id % 100 AND k.b > u.n)",
        "UPDATE u SET n = (SELECT count(*) FROM k WHERE k.a BETWEEN u.id % 100 AND u.id % 100)",
        "UPDATE u SET n = (SELECT q.n FROM q WHERE q.a = u.g AND q.b = u.n)",
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
             INSERT INTO u SELECT * FROM t;\
             CREATE TABLE k(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);\
             CREATE INDEX k_ab ON k(a, b);\
             INSERT INTO k SELECT id, id % 100, id % 7 FROM t;\
             CREATE TABLE q(a INTEGER, b INTEGER, n INTEGER, UNIQUE(a, b));\
             INSERT INTO q SELECT g, n, max(id) FROM t GROUP BY g, n;"
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

/// bd-wp9s1: `col = (uncorrelated scalar subquery)` folds the subquery to its
/// value, which carries the result column's affinity. The outer equality
/// seeks the column's index when the comparison converts only that value (a
/// numeric column against a numeric or TEXT value, a TEXT column against a
/// TEXT value), in either operand order, for a row list and a COUNT(*). A
/// numeric value against a TEXT column converts the column side and a BLOB
/// column compares without converting, so those scan, and a seek there would
/// miss rows: each answer is compared with rusqlite's.
#[test]
fn equality_with_an_uncorrelated_scalar_subquery_seeks_the_index() {
    const ROWS: u64 = 2000;
    // A seek visits the 40 rows of one key; a scan steps all 2,000.
    const SEEK_LIMIT: u64 = 2000;
    let setup = format!(
        "CREATE TABLE o(id INTEGER PRIMARY KEY, a INTEGER, s TEXT, r REAL, x);\
         CREATE INDEX o_a ON o(a); CREATE INDEX o_s ON o(s); CREATE INDEX o_r ON o(r);\
         CREATE INDEX o_x ON o(x);\
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {ROWS}) \
         INSERT INTO o SELECT i, i % 50, CAST(i % 50 AS TEXT), (i % 50) * 1.0, i % 50 FROM n;\
         CREATE TABLE p(pid INTEGER PRIMARY KEY, w INTEGER, ws TEXT, wr REAL);\
         INSERT INTO p VALUES (1, 7, '7', 7.0), (2, 9, '09', 9.5), (3, NULL, NULL, NULL);"
    );
    // (sql, Some(seeks), or None where only the answer is pinned: a NULL value or a TEXT value the
    // NUMERIC column converts may take the equality walk's zero-match scan fallback, which is exact)
    let shapes: &[(&str, Option<bool>)] = &[
        ("SELECT id FROM o WHERE a = (SELECT w FROM p WHERE pid = 1)", Some(true)),
        ("SELECT id FROM o WHERE (SELECT w FROM p WHERE pid = 1) = a", Some(true)),
        ("SELECT count(*) FROM o WHERE a = (SELECT w FROM p WHERE pid = 1)", Some(true)),
        ("SELECT id FROM o WHERE s = (SELECT ws FROM p WHERE pid = 1)", Some(true)),
        ("SELECT id FROM o WHERE r = (SELECT wr FROM p WHERE pid = 1)", Some(true)),
        ("SELECT id FROM o WHERE a = (SELECT w FROM p WHERE pid = 1) - 2", None),
        ("SELECT id FROM o WHERE a = (SELECT ws FROM p WHERE pid = 2)", None),
        ("SELECT count(*) FROM o WHERE a = (SELECT ws FROM p WHERE pid = 2)", None),
        ("SELECT a, count(*) FROM o WHERE a = (SELECT ws FROM p WHERE pid = 2) GROUP BY a", None),
        ("SELECT id FROM o WHERE a = (SELECT ws FROM p WHERE pid = 2) AND s IN ('9', '7')", None),
        ("SELECT id FROM o WHERE a = (SELECT w FROM p WHERE pid = 3)", None),
        ("SELECT id FROM o WHERE a = (SELECT w FROM p WHERE pid = 99)", None),
        ("SELECT id FROM o WHERE s = (SELECT w FROM p WHERE pid = 1)", Some(false)),
        ("SELECT id FROM o WHERE x = (SELECT ws FROM p WHERE pid = 1)", Some(false)),
        ("SELECT id FROM o WHERE x = (SELECT w FROM p WHERE pid = 1)", Some(false)),
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("wp9s1.db").to_str().unwrap())
            .await
            .unwrap();
        f.execute_batch(&setup).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch(&setup).unwrap();
        let mut wrong = Vec::new();
        for &(sql, seeks) in shapes {
            let mut expected = rq(&r, sql, &[]).unwrap();
            expected.sort();
            set_vdbe_metrics_enabled(true);
            reset_vdbe_metrics();
            let got = fq(&f, sql, &[], false).await;
            let opcodes = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_enabled(false);
            let mut got = got.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
            got.sort();
            assert_eq!(got, expected, "`{sql}`");
            if let Some(seeks) = seeks
                && (opcodes <= SEEK_LIMIT) != seeks
            {
                wrong.push(format!("  {opcodes} opcodes, seeks={seeks}: `{sql}`"));
            }
        }
        f.close().await.unwrap();
        assert!(
            wrong.is_empty(),
            "the seeks must stay at or under {SEEK_LIMIT} opcodes and the scans over it:\n{}",
            wrong.join("\n")
        );
    });
}

/// bd-c6b16: correlated aggregates (`group_concat`, float `sum`) in a SELECT
/// list and in WHERE over an index range or a multi-column index equality must
/// walk the index in stock SQLite 3.54.1's order rather than falling back to
/// rowid order, on both `:memory:` and file-backed databases, direct and
/// prepared.
#[test]
fn bd_c6b16_correlated_aggregate_index_order_matches_stock() {
    const SETUP: &str = "\
        CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER, r REAL);\
        INSERT INTO t VALUES \
            (5, 'a', 1, 1, 0.5),\
            (6, 'b', 1, 1, 0.6000000000000001),\
            (9223372036854775807, 'm', 4, 4, 0.5),\
            (4, 'c', 1, 1, 0.4),\
            (2, 'z', 2, 1, -0.5),\
            (9, 'y', 2, 2, -0.25),\
            (1, 'n', NULL, 1, NULL);\
        CREATE INDEX t_r ON t(r);\
        CREATE TABLE u AS SELECT * FROM t;\
        CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d INTEGER);\
        INSERT INTO m VALUES \
            (10, 1, 'b2', 3.0, 1),\
            (20, 1, 'b0', 9.0, 1),\
            (30, 1, 'b0', 2.0, 2),\
            (40, 1, 'b1', 1.0, 1),\
            (50, 4, 'b0', 5.0, 4),\
            (60, 2, 'b1', 0.2, 1),\
            (70, 2, 'b0', 0.8, 2),\
            (80, 1, '1', 0.1, 1);\
        CREATE INDEX m_abc ON m(a, b, c);\
        CREATE TABLE td(id INTEGER PRIMARY KEY, g INTEGER, r REAL);\
        INSERT INTO td VALUES (10, 1, 0.2), (20, 1, 0.9), (30, 1, 0.5), (40, 4, 0.1);\
        CREATE INDEX td_gr_desc ON td(g, r DESC);\
        CREATE TABLE c(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE);\
        INSERT INTO c VALUES (1, 'a'), (2, 'B'), (3, 'b'), (4, 'C'), (5, 'A');\
        CREATE INDEX c_s ON c(s);\
        CREATE TABLE p(id INTEGER PRIMARY KEY, val);\
        INSERT INTO p VALUES (1, 1), (2, '1'), (3, 1.0), (4, 'b0'), (5, NULL);";

    const QUERIES: &[&str] = &[
        // Exact bd-c6b16 repro: SELECT-list correlated group_concat over index range `t_r`.
        "SELECT u.id, (SELECT group_concat(x.id) FROM t AS x \
         WHERE x.r > u.r AND x.r < u.r + 0.25) FROM u ORDER BY u.id",
        // Same range in WHERE.
        "SELECT u.id FROM u WHERE (SELECT group_concat(x.id) FROM t AS x \
         WHERE x.r > u.r AND x.r < u.r + 0.25) = '5,9223372036854775807,6' ORDER BY u.id",
        // Index range with residual predicate (`x.n >= 1`) and float `sum` sensitivity.
        "SELECT u.id, (SELECT group_concat(x.id) FROM t AS x \
         WHERE x.r > u.r AND x.r < u.r + 0.25 AND x.n >= 1), \
         (SELECT sum(x.r) FROM t AS x WHERE x.r > u.r AND x.r < u.r + 0.25) \
         FROM u ORDER BY u.id",
        // Reversed operand (`u.r < x.r`), BETWEEN, and negative / one-sided bounds.
        "SELECT u.id, (SELECT group_concat(x.id) FROM t AS x \
         WHERE u.r < x.r AND u.r + 0.25 > x.r), \
         (SELECT group_concat(x.id) FROM t AS x WHERE x.r BETWEEN u.r AND u.r + 0.25) \
         FROM u ORDER BY u.id",
        "SELECT u.id FROM u WHERE (SELECT group_concat(x.id) FROM t AS x \
         WHERE x.r BETWEEN u.r AND u.r + 0.25) = '4,5,9223372036854775807,6' ORDER BY u.id",
        // CTE outer source (affinity=None, collation=Unspecified).
        "WITH w(id, r) AS (VALUES (1, 0.4), (2, -0.6), (3, NULL)) \
         SELECT w.id, (SELECT group_concat(x.id) FROM t AS x \
         WHERE x.r > w.r AND x.r < w.r + 0.25) FROM w ORDER BY w.id",
        // Multi-column index equality in SELECT list: 1-col prefix, 2-col prefix,
        // expression equality, equality + residual, equality prefix + trailing range.
        "SELECT u.id, (SELECT group_concat(m.id) FROM m WHERE m.a = u.g) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT group_concat(m.id) FROM m WHERE m.a = u.g AND m.b = 'b0') \
         FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT group_concat(m.id) FROM m \
         WHERE m.a = u.g AND m.b = 'b' || (u.id % 3)) FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT group_concat(m.id) FROM m WHERE m.a = u.g AND m.d = u.n) \
         FROM u ORDER BY u.id",
        "SELECT u.id, (SELECT group_concat(m.id) FROM m \
         WHERE m.a = u.g AND m.b = 'b0' AND m.c > u.r AND m.c < u.r + 10.0) \
         FROM u ORDER BY u.id",
        // Multi-column index equality in WHERE.
        "SELECT u.id FROM u WHERE (SELECT group_concat(m.id) FROM m \
         WHERE m.a = u.g) = '80,30,20,40,10' ORDER BY u.id",
        "SELECT u.id FROM u WHERE (SELECT group_concat(m.id) FROM m \
         WHERE m.a = u.g AND m.b = 'b0') = '30,20' ORDER BY u.id",
        "SELECT u.id FROM u WHERE (SELECT group_concat(m.id) FROM m \
         WHERE m.a = u.g AND m.d = u.n) = '80,20,40,10' ORDER BY u.id",
        // Multi-column index with DESC trailing key (`td_gr_desc ON td(g, r DESC)`).
        "SELECT u.id, (SELECT group_concat(td.id) FROM td WHERE td.g = u.g) \
         FROM u ORDER BY u.id",
        "SELECT u.id FROM u WHERE (SELECT group_concat(td.id) FROM td \
         WHERE td.g = u.g) = '20,30,10' ORDER BY u.id",
        // Affinity and collation negative controls (must stay identical to stock).
        "SELECT u.id, (SELECT count(*) FROM m WHERE m.a = u.g AND m.b = u.g) \
         FROM u ORDER BY u.id",
        "SELECT p.id, (SELECT group_concat(m.id) FROM m WHERE m.a = 1 AND m.b = p.val), \
         (SELECT group_concat(m.id) FROM m WHERE m.a = p.val) FROM p ORDER BY p.id",
        "SELECT u.id, (SELECT group_concat(c.id) FROM c WHERE u.v < c.s) FROM u ORDER BY u.id",
    ];

    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                (
                    Connection::open(dir.path().join("c6b16_f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("c6b16_r.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            f.execute_batch(SETUP).await.unwrap();
            r.execute_batch(SETUP).unwrap();
            let db = if file_backed { "file" } else { "memory" };

            // Pin the exact stock answer for row 4 of the bd-c6b16 repro so the
            // comparison cannot pass vacuously if stock's index plan ever changed.
            let repro_stock = rq(&r, QUERIES[0], &[]).unwrap();
            assert!(
                repro_stock.contains(&vec![
                    "int:4".to_owned(),
                    "text:5,9223372036854775807,6".to_owned(),
                ]),
                "stock repro row 4 changed: {repro_stock:?}"
            );

            for sql in QUERIES {
                let rr = rq(&r, sql, &[]);
                for prepared in [false, true] {
                    let fr = fq(&f, sql, &[], prepared).await;
                    if fr != rr {
                        diffs.push(format!(
                            "  [{db}, prepared={prepared}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                }
            }
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from rusqlite 3.54.1:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
