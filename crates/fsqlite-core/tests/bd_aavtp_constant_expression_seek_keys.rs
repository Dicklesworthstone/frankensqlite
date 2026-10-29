#![allow(
    clippy::await_holding_lock,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]

//! bd-aavtp: a single-table seek took its key or range bound only from a bare
//! literal or a bind parameter, so `id = -5`, `a = 4 + 1`, `a = ?1 + 1`,
//! `a = abs(-5)` and `id > 300000 - 5` scanned the whole table where stock
//! seeks. A constant expression over literals, parameters and deterministic
//! calls is now a seek key or bound wherever a parameter is, and like a
//! parameter it takes the column's affinity at run time. An expression with
//! an affinity (a CAST at its root) or a collation (a COLLATE anywhere) still
//! scans: it changes how the comparison converts or collates.
//!
//! `constant_expression_keys_match_stock` compares each statement with
//! rusqlite's bundled SQLite. It covers:
//! - columns of every affinity, a NOCASE column, and stored values of every
//!   storage class, including text that converts to a number;
//! - probe expressions of every storage class, NULL, CAST and COLLATE
//!   shapes, and parameters;
//! - rowid, single-column, composite, WITHOUT ROWID and expression-index
//!   keys, equalities and ranges, plain and aggregate SELECTs, UPDATE and
//!   DELETE.
//!
//! Each runs in memory and in a file, ad hoc and prepared.
//!
//! `constant_expression_keys_seek` bounds the opcodes such statements run
//! over 1,000 rows, which a scan exceeds many times over.

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

/// The first `n` of the parameters statements bind: `?1` an integer, `?2`
/// text that converts to a number, `?3` a real.
fn f_params(n: usize) -> Vec<SqliteValue> {
    let mut params = vec![
        SqliteValue::Integer(5),
        SqliteValue::Text("5".into()),
        SqliteValue::Float(5.0),
    ];
    params.truncate(n);
    params
}

fn r_params(n: usize) -> Vec<rusqlite::types::Value> {
    let mut params = vec![
        rusqlite::types::Value::Integer(5),
        rusqlite::types::Value::Text("5".to_owned()),
        rusqlite::types::Value::Real(5.0),
    ];
    params.truncate(n);
    params
}

fn rq(conn: &rusqlite::Connection, sql: &str, bind: usize) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(r_params(bind)), |row| {
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
    let params = f_params(bind);
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

/// `t` has an index on each column. Its rows hold the integer 5 and values
/// that equal it only under some affinity (`'5'`, `' 5'`, `'5.0'`, `5.0`,
/// `x'35'`), their negatives, NULL and text; `f` is NOCASE. `k` and `w` have
/// a two-column key, `w` without a rowid; `x` has an expression index.
const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d NUMERIC, e BLOB, \
  f TEXT COLLATE NOCASE, g, n INTEGER);\
INSERT INTO t VALUES (1, 5, '5', 5, 5, 5, 'abc', 5, 0), \
  (2, -5, '-5', -5.5, '5.0', '5', 'ABC', '5', 0), \
  (3, 5.0, '5.0', 5.0, ' 5', x'35', 'Abc', 5.0, 0), \
  (4, '5', ' 5', '5', '-5', -5, 'abd', -5, 0), \
  (5, 'abc', 'abc', 'abc', 'abc', 'abc', 'x', 'abc', 0), \
  (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0), \
  (7, 3, 'ABC', 3.5, 3, 3.5, '5', x'35', 0), \
  (8, 9223372036854775807, '05', -5, 5.5, '-5', '-5', -5.5, 0), \
  (9, -5, '5', 5.5, -5, ' 5', 'ABD', ' 5', 0), \
  (-5, 5, 'abc', -5.5, 5, '5.0', 'abc', -5, 0);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 10 UNION ALL SELECT i + 1 FROM s WHERE i < 70) \
  SELECT i, i % 11 - 5, (i % 11 - 5) || '', (i % 11 - 5) * 0.5, i % 11 - 5, i % 11 - 5, \
  'ab' || char(97 + i % 4), i % 11 - 5, i FROM s;\
CREATE INDEX t_a ON t(a);\
CREATE INDEX t_b ON t(b);\
CREATE INDEX t_c ON t(c);\
CREATE INDEX t_d ON t(d);\
CREATE INDEX t_e ON t(e);\
CREATE INDEX t_f ON t(f);\
CREATE INDEX t_g ON t(g);\
CREATE TABLE k(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, n INTEGER);\
INSERT INTO k WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 80) \
  SELECT i, i % 7 - 3, (i % 5 - 2) || '', i FROM s;\
INSERT INTO k VALUES (100, -5, '5', 0), (101, 5, '-5', 0), (102, NULL, '5', 0), \
  (103, -5, NULL, 0), (104, '5', ' 5', 0);\
CREATE INDEX k_ab ON k(a, b);\
CREATE TABLE w(a INTEGER, b TEXT, n, PRIMARY KEY(a, b)) WITHOUT ROWID;\
INSERT INTO w SELECT a, b, max(id) FROM k WHERE a IS NOT NULL AND b IS NOT NULL GROUP BY a, b;\
CREATE TABLE x(id INTEGER PRIMARY KEY, s TEXT);\
INSERT INTO x SELECT id, b FROM t;\
CREATE INDEX x_lower ON x(lower(s));";

/// Right-hand sides: constant expressions of every storage class and shape.
/// The CAST, COLLATE and `likely` ones must keep scanning; their results
/// still have to match.
const PROBES: &[&str] = &[
    "-5",
    "- -5",
    "4 + 1",
    "10 / 2",
    "5.0 + 0",
    "0 - 5.5",
    "-5.5",
    "5 / 2.0",
    "-(4 + 1)",
    "'5' || ''",
    "'-' || '5'",
    "' 5' || ''",
    "'5.0' || ''",
    "abs(-5)",
    "abs(-5.0)",
    "lower('ABC')",
    "upper('abd')",
    "NULL + 1",
    "x'35' || ''",
    "+'5'",
    "+5",
    "~-6",
    "coalesce(NULL, -5)",
    "json_extract('{\"a\":5}', '$.a')",
    "length('abcde')",
    "5 = 5",
    "CAST(5 AS TEXT)",
    "CAST('5' AS INTEGER)",
    "CAST('5' AS INTEGER) + 0",
    "'abc' COLLATE NOCASE",
    "('ABC' COLLATE NOCASE) || ''",
    "likely(5)",
    "9223372036854775806 + 1",
    "abs(-9223372036854775807 - 1)",
    "?1 + 0",
    "-?1",
    "?1 || ''",
    "?2 || ''",
    "?2 + 0",
    "?3 * 1",
];

const COLUMNS: &[&str] = &["id", "a", "b", "c", "d", "e", "f", "g"];

/// `{c}` is the column, `{p}` the probe. Forms without ORDER BY compare
/// their rows as a set: a seek and a scan may return them in either order.
const SELECT_FORMS: &[&str] = &[
    "SELECT id FROM t WHERE {c} = {p} ORDER BY id",
    "SELECT id, {c} FROM t WHERE {p} = {c}",
    "SELECT count(*) FROM t WHERE {c} = {p}",
    "SELECT count(*), sum(n), max(id) FROM t WHERE {c} = {p} AND id > 1",
    "SELECT id FROM t WHERE {c} = {p} AND n >= 0 ORDER BY id DESC",
    "SELECT id FROM t WHERE {c} > {p} ORDER BY id",
    "SELECT id, {c} FROM t WHERE {c} <= {p} AND n >= 0",
    "SELECT count(*), max(id) FROM t WHERE {c} BETWEEN {p} AND 7",
    "SELECT id FROM t WHERE {c} >= {p} AND {c} < {p} + 3 ORDER BY id DESC",
];

/// Composite, WITHOUT ROWID and expression-index keys, and the second column
/// of `k_ab` alone (a skip scan candidate).
const KEY_FORMS: &[&str] = &[
    "SELECT id FROM k WHERE a = {p} AND b = {p} || '' ORDER BY id",
    "SELECT id FROM k WHERE a = {p} AND b = '5'",
    "SELECT id FROM k WHERE a = -3 AND b = {p}",
    "SELECT count(*) FROM k WHERE a = {p} AND b = -(-2) || ''",
    "SELECT max(b), min(b) FROM k WHERE a = {p}",
    "SELECT id FROM k WHERE b = {p} ORDER BY id",
    "SELECT n FROM w WHERE a = {p} AND b = {p} || ''",
    "SELECT n FROM w WHERE a = {p}",
    "SELECT id FROM x WHERE lower(s) = {p} ORDER BY id",
    "SELECT id FROM k WHERE a = {p} AND b > '0' ORDER BY id",
    "SELECT id FROM k WHERE a = -3 AND b <= {p} || '' ORDER BY id",
    "SELECT count(*) FROM k WHERE a > {p}",
    "SELECT n FROM w WHERE a = -3 AND b >= {p} || ''",
    "UPDATE k SET n = n + 1 WHERE a = {p} AND b = {p} || ''",
    "DELETE FROM k WHERE a = {p} AND b = '5'",
    "UPDATE w SET n = -n WHERE a = {p} AND b = {p} || ''",
];

const DML_FORMS: &[&str] = &[
    "UPDATE t SET n = n + 1 WHERE {c} = {p}",
    "DELETE FROM t WHERE {c} = {p}",
    "UPDATE t SET n = -n WHERE {c} = {p} AND id > 2",
    "UPDATE t SET n = n + 1 WHERE {c} < {p}",
];

const DML_CHECKS: &[&str] = &[
    "SELECT id, n FROM t ORDER BY id",
    "SELECT id, n FROM k ORDER BY id",
    "SELECT a, b, n FROM w ORDER BY a, b",
];

fn normalize(
    rows: Result<Vec<Vec<String>>, String>,
    ordered: bool,
) -> Result<Vec<Vec<String>>, ()> {
    // An error compares by its presence: the messages differ between engines.
    rows.map(|mut rows| {
        if !ordered {
            rows.sort();
        }
        rows
    })
    .map_err(|_| ())
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
    let mut sqls = Vec::new();
    for probe in PROBES {
        for column in COLUMNS {
            for form in SELECT_FORMS.iter().chain(DML_FORMS) {
                sqls.push(form.replace("{c}", column).replace("{p}", probe));
            }
        }
        for form in KEY_FORMS {
            sqls.push(form.replace("{p}", probe));
        }
    }
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    for sql in &sqls {
        // Bind as many parameters as the statement declares, as rusqlite requires.
        let bind = r.prepare(sql).map_or(0, |st| st.parameter_count());
        for prepared in [false, true] {
            runs += 1;
            let path = format!("[{db}, prepared={prepared}] `{sql}`");
            if sql.starts_with("SELECT") {
                let ordered = sql.contains("ORDER BY");
                let fr = normalize(fq(&f, sql, bind, prepared).await, ordered);
                let rr = normalize(rq(&r, sql, bind), ordered);
                if fr != rr {
                    diffs.push(format!("  {path}\n     frank= {fr:?}\n     stock= {rr:?}"));
                }
                continue;
            }
            f.execute("BEGIN").await.unwrap();
            r.execute_batch("BEGIN").unwrap();
            let stock = r
                .execute(sql, rusqlite::params_from_iter(r_params(bind)))
                .map_err(|_| ());
            let values = f_params(bind);
            let frank = if prepared {
                match f.prepare(sql).await {
                    Ok(stmt) if bind > 0 => stmt.execute_with_params(&values).await,
                    Ok(stmt) => stmt.execute().await,
                    Err(e) => Err(e),
                }
            } else if bind > 0 {
                f.execute_with_params(sql, &values).await
            } else {
                f.execute(sql).await
            }
            .map_err(|_| ());
            if frank != stock {
                diffs.push(format!(
                    "  {path}\n     changes: frank= {frank:?} stock= {stock:?}"
                ));
            }
            for check in DML_CHECKS {
                let fr = normalize(fq(&f, check, 0, false).await, true);
                let rr = normalize(rq(&r, check, 0), true);
                if fr != rr {
                    diffs.push(format!(
                        "  {path}\n     `{check}`: frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
            f.execute("ROLLBACK").await.unwrap();
            r.execute_batch("ROLLBACK").unwrap();
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn constant_expression_keys_match_stock() {
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
fn constant_expression_keys_seek() {
    // Each shape seeks; the last ones are controls that must scan.
    const SHAPES: &[&str] = &[
        "SELECT n FROM t WHERE id = 4 + 1",
        "SELECT n FROM t WHERE id = ?1 + 1",
        "SELECT n FROM t WHERE a = -5",
        "SELECT n FROM t WHERE a = abs(-5) AND b = 6 + 1",
        "SELECT count(*) FROM t WHERE a = 0 - 3",
        "SELECT count(*) FROM t WHERE a = -500",
        "SELECT count(*) FROM t WHERE a = ?1 + 1000",
        "SELECT count(*) FROM t WHERE a = -3 AND b = ?1 + 1",
        "SELECT sum(n) FROM t WHERE a = -(3)",
        "SELECT n FROM t WHERE s = lower('V7')",
        "SELECT n FROM t WHERE id > 1000 - 3",
        "SELECT count(*) FROM t WHERE id BETWEEN ?1 + 10 AND ?1 + 20",
        "SELECT count(*) FROM t WHERE a > 150 - 2",
        "SELECT n FROM t WHERE a = -3 AND b > 2 + 3",
        "UPDATE t SET n = n + 1 WHERE id = 2 + 3",
        "UPDATE t SET n = n + 1 WHERE a = -3 AND b = 1 + 1",
        "DELETE FROM t WHERE a = -499 AND b = abs(-2)",
        "DELETE FROM t WHERE a = ?1 * 100 AND b = 2",
    ];
    const SCAN_CONTROLS: &[&str] = &[
        "SELECT n FROM t WHERE a + 0 = 5",
        "SELECT n FROM t WHERE s = CAST('v7' AS TEXT) COLLATE NOCASE",
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("cost.db").to_str().unwrap())
            .await
            .unwrap();
        f.execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT, n INTEGER);\
             INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
               WHERE i < {COST_ROWS}) SELECT i, i % 250 - 100, i % 7, 'v' || (i % 300), i \
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
                vec![SqliteValue::Integer(-4)]
            } else {
                Vec::new()
            };
            let result = if sql.starts_with("SELECT") {
                f.query_with_params(sql, &params).await.map(|_| ())
            } else {
                f.execute_with_params(sql, &params).await.map(|_| ())
            };
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
            "the seeks must stay under {COST_LIMIT} opcodes and the scan controls over it:\n{}",
            over.join("\n")
        );
    });
}
