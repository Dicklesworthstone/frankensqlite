#![recursion_limit = "512"]

//! bd-5ap77: a FROM-clause subquery under an outer aggregate or expression
//! projection (`SELECT count(*) FROM (SELECT value FROM t)`), over a table
//! function, projecting aliased expressions, or carrying an ORDER BY under an
//! outer count/min/max, now flattens into its source instead of materializing
//! every inner row through the join interpreter. Pinned against stock SQLite
//! (rusqlite), including the shapes that must keep the subquery (unexposed
//! columns, `rowid` through `*`, order-sensitive aggregates, LIMIT, inner
//! function projections) and the errors stock reports for bad references.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Integer(n) => format!("int:{n}"),
        Value::Real(f) => format!("real:{f}"),
        Value::Text(s) => format!("text:{s}"),
        Value::Blob(b) => format!("blob:{b:?}"),
    }
}

/// Rows, or `ERR` (messages are compared separately where they matter).
async fn frank(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

fn stock(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, Value>(i).map(|v| tag_r(&v)))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .map_err(|e| e.to_string())?
    .collect::<rusqlite::Result<Vec<_>>>()
    .map_err(|e| e.to_string())
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(a INTEGER, b TEXT, c REAL, d)",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 500) \
     INSERT INTO t SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE 'b' || (i % 13) END, \
     i * 0.5, CASE i % 3 WHEN 0 THEN '10' WHEN 1 THEN i ELSE NULL END FROM s",
    "CREATE TABLE e(x INTEGER, y TEXT COLLATE NOCASE)",
    "INSERT INTO e VALUES (1, 'Abc'), (2, 'abc'), (3, 'ABD'), (NULL, NULL)",
];

/// Shapes whose results (or error-ness) must match stock exactly.
const SHAPES: &[&str] = &[
    // Outer aggregates over a plain column subquery.
    "SELECT count(*) FROM (SELECT a FROM t)",
    "SELECT count(*), sum(v), min(v), max(v), avg(v) FROM (SELECT a AS v FROM t)",
    "SELECT count(v), total(w) FROM (SELECT b AS v, c AS w FROM t)",
    "SELECT count(*) FROM (SELECT a FROM t WHERE a > 100) WHERE a % 2 = 0",
    "SELECT sum(sq.v) FROM (SELECT a AS v FROM t) AS sq WHERE sq.v BETWEEN 10 AND 20",
    "SELECT group_concat(b, ',') FROM (SELECT b FROM t WHERE a < 20)",
    "SELECT count(DISTINCT b) FROM (SELECT b FROM t)",
    "SELECT max(a) + 1, count(*) * 2 FROM (SELECT * FROM t)",
    "SELECT count(*) FROM (SELECT * FROM t) WHERE d = 10",
    "SELECT count(*) FROM (SELECT a, d FROM t) WHERE d = '10'",
    "SELECT count(*) FROM (SELECT a FROM t) WHERE a = '5'",
    // Aliased expression projections.
    "SELECT count(*), sum(x) FROM (SELECT a * 2 AS x FROM t)",
    "SELECT x + 1 FROM (SELECT a * 2 AS x FROM t WHERE a < 5)",
    "SELECT * FROM (SELECT a * 2 AS x, b FROM t WHERE a < 5)",
    "SELECT count(*) FROM (SELECT a * 2 AS x FROM t) WHERE x = '8'",
    "SELECT max(k) FROM (SELECT CASE WHEN a % 2 THEN b ELSE NULL END AS k FROM t)",
    "SELECT count(*) FROM (SELECT y COLLATE BINARY AS z FROM e) WHERE z = 'abc'",
    "SELECT count(*) FROM (SELECT y AS z FROM e) WHERE z = 'abc'",
    "SELECT max(z), min(z) FROM (SELECT y AS z FROM e)",
    "SELECT count(*) FROM (SELECT CAST(d AS TEXT) AS s FROM t) WHERE s = '10'",
    // A bare ORDER BY identifier names the outer result alias before a column
    // the subquery exposes under the same name (`b` below is text 'b1'..'b12',
    // whose order differs from a's).
    "SELECT x + 0 AS b FROM (SELECT a AS x, b FROM t WHERE a < 13) ORDER BY b",
    "SELECT x AS b FROM (SELECT a AS x, b FROM t WHERE a < 13) ORDER BY b",
    "SELECT x AS b FROM (SELECT a AS x, b FROM t WHERE a < 13) ORDER BY b DESC",
    "SELECT x * 2 AS y FROM (SELECT a AS x, b AS y FROM t WHERE a < 13) ORDER BY y",
    "SELECT *, x AS y FROM (SELECT a AS x, b AS y FROM t WHERE a < 13) ORDER BY y",
    "SELECT x AS y FROM (SELECT a AS x, b AS y FROM t WHERE a < 13) WHERE y > 'b5' ORDER BY x",
    // Inner ORDER BY under an outer count/min/max.
    "SELECT max(v) FROM (SELECT a AS v FROM t ORDER BY a DESC)",
    "SELECT min(v), max(v), count(*) FROM (SELECT b AS v FROM t ORDER BY c)",
    "SELECT count(*) FROM (SELECT a FROM t WHERE a > 10 ORDER BY b)",
    // Shapes that must keep the subquery.
    "SELECT group_concat(v) FROM (SELECT a AS v FROM t WHERE a < 10 ORDER BY a DESC)",
    "SELECT sum(v) FROM (SELECT a AS v FROM t ORDER BY a LIMIT 5)",
    "SELECT max(v, 3) FROM (SELECT a AS v FROM t WHERE a < 6 ORDER BY a DESC)",
    // A bare column beside max() takes the max row's value; this flattens.
    // (The same shape with an inner ORDER BY keeps the subquery, and that
    // materializing path picks the wrong bare-column row today — a separate
    // pre-existing bug, so it is not pinned here.)
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t)",
    "SELECT min(v), b FROM (SELECT a AS v, b FROM t WHERE a > 3)",
    "SELECT count(*), sum(n) FROM (SELECT length(b) AS n FROM t)",
    "SELECT count(*) FROM (SELECT count(*) AS n FROM t)",
    "SELECT sum(v) FROM (SELECT DISTINCT a % 10 AS v FROM t)",
    "SELECT count(*) FROM (SELECT a FROM t) WHERE a IN (SELECT x FROM e)",
    // Bad references: stock errors, so must fsqlite.
    "SELECT count(b) FROM (SELECT a FROM t)",
    "SELECT sum(t.a) FROM (SELECT a FROM t)",
    "SELECT count(rowid) FROM (SELECT * FROM t)",
    "SELECT count(*) FROM (SELECT a FROM t) WHERE b = 'b1'",
    "SELECT sum(a * 2) FROM (SELECT a * 2 AS x FROM t)",
];

#[test]
fn from_subquery_aggregate_flattening_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        for path in [":memory:", "file"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("bd5ap77.db");
            let target = if path == "file" {
                db.to_string_lossy().into_owned()
            } else {
                path.to_owned()
            };
            let fconn = Connection::open(&target).await.unwrap();
            let rconn = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                fconn
                    .execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"));
                rconn.execute_batch(sql).unwrap();
            }
            for sql in SHAPES {
                let f = frank(&fconn, sql).await;
                let r = stock(&rconn, sql);
                match (&f, &r) {
                    (Ok(f), Ok(r)) => assert_eq!(f, r, "[{path}] `{sql}` differs from SQLite"),
                    (Err(_), Err(_)) => {}
                    _ => panic!("[{path}] `{sql}`: FrankenSQLite {f:?} vs SQLite {r:?}"),
                }
            }
        }
    });
}

/// `generate_series` is not in rusqlite's bundled SQLite, so table-function
/// sources are pinned against the same query over an equivalent table.
#[test]
fn from_subquery_over_table_function_matches_table_equivalent() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE g(value INTEGER)").await.unwrap();
        conn.execute("INSERT INTO g SELECT value FROM generate_series(1, 2000)")
            .await
            .unwrap();
        for (tvf, table) in [
            (
                "SELECT count(*) FROM (SELECT value FROM generate_series(1, 2000))",
                "SELECT count(*) FROM (SELECT value FROM g)",
            ),
            (
                "SELECT count(*), sum(x) FROM (SELECT value * 2 AS x FROM generate_series(1, 2000))",
                "SELECT count(*), sum(x) FROM (SELECT value * 2 AS x FROM g)",
            ),
            (
                "SELECT max(v) FROM (SELECT value AS v FROM generate_series(1, 2000) ORDER BY value DESC)",
                "SELECT max(v) FROM (SELECT value AS v FROM g ORDER BY value DESC)",
            ),
            (
                "SELECT count(*) FROM (SELECT value FROM generate_series(1, 2000) AS s WHERE s.value % 3 = 0) WHERE value > 100",
                "SELECT count(*) FROM (SELECT value FROM g AS s WHERE s.value % 3 = 0) WHERE value > 100",
            ),
            (
                "SELECT v FROM (SELECT value AS v FROM generate_series(1, 2000)) WHERE v % 500 = 0",
                "SELECT v FROM (SELECT value AS v FROM g) WHERE v % 500 = 0",
            ),
        ] {
            let left = frank(&conn, tvf).await;
            let right = frank(&conn, table).await;
            assert!(left.is_ok(), "`{tvf}`: {left:?}");
            assert_eq!(left, right, "`{tvf}` differs from `{table}`");
        }
        // Hidden argument columns are not exposed through `*` (stock: no such column).
        assert!(
            frank(&conn, "SELECT max(start) FROM (SELECT * FROM generate_series(1, 5))")
                .await
                .is_err()
        );
    });
}
