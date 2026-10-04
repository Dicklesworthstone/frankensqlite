#![recursion_limit = "512"]

//! bd-xik4y: with a single `min()`/`max()` aggregate, SQLite reads every bare
//! column from the row that produced the extremum. The single-table grouped
//! path did this, but the general join path (also used for FROM subqueries
//! that do not flatten, CTEs and compound sources) read bare columns from
//! each group's first row:
//! `SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a)` returned the
//! NULL row's `b`. A HAVING that repeats the tracked aggregate also dropped
//! the tracking on every path.
//!
//! Unaliased result expressions were named `_cN` by `column_names()`; SQLite
//! names them by their source text (`max(v)`, `x + 1`, `(a)`).
//!
//! Each query is compared with rusqlite, in memory and file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(a, b, c)",
    "INSERT INTO t VALUES (3,'b1',1),(9,'b6',2),(1,'b2',1),(9,'b7',2),(NULL,'bn',3),(5,'b5',3),(2,'b3',1)",
    "CREATE TABLE u(a INTEGER PRIMARY KEY, x)",
    "INSERT INTO u VALUES (1,'x1'),(2,'x2'),(3,'x3'),(5,'x5'),(9,'x9')",
    "CREATE TABLE n(k TEXT COLLATE NOCASE, tag)",
    "INSERT INTO n VALUES ('b','lower-b'),('A','upper-a'),('B','upper-b'),('a','lower-a')",
    "CREATE VIEW vt AS SELECT a AS v, b, c FROM t",
    // Group 1 is all NULL, group 2 has leading NULLs before its maximum.
    "CREATE TABLE z(a, b, g)",
    "INSERT INTO z VALUES (NULL,'z1',1),(NULL,'z2',1),(NULL,'z3',1),\
     (NULL,'y1',2),(4,'y2',2),(NULL,'y3',2),(4,'y4',2)",
];

/// Single min()/max() with bare columns, over every source shape that reaches
/// a different execution path.
const VALUE_QUERIES: &[&str] = &[
    // FROM subqueries that do not flatten (ORDER BY, LIMIT, DISTINCT, WHERE,
    // compound, window).
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT min(v), b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t LIMIT 5)",
    "SELECT max(v), b FROM (SELECT DISTINCT a AS v, b FROM t)",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t WHERE c > 1 ORDER BY a)",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t UNION ALL SELECT 100, 'u100')",
    "SELECT min(v), b FROM (SELECT a AS v, b FROM t UNION ALL SELECT -1, 'neg')",
    "SELECT max(v), b FROM (SELECT a AS v, b, row_number() OVER () AS rn FROM t)",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a LIMIT 3 OFFSET 1)",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a) WHERE v < 9",
    "SELECT max(v + 1), b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT max(v) || ':' || b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT b, max(v) FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a) GROUP BY b IS NULL",
    "SELECT min(v), b, c FROM (SELECT a AS v, b, c FROM t ORDER BY b) GROUP BY c ORDER BY c",
    "SELECT max(v), b, c FROM (SELECT a AS v, b, c FROM t ORDER BY b) GROUP BY c ORDER BY c",
    "SELECT min(v), b FROM (SELECT a AS v, b FROM t WHERE a IS NULL)",
    "SELECT max(v) FILTER (WHERE v < 9), b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT max(k), tag FROM (SELECT k, tag FROM n ORDER BY tag)",
    "SELECT min(k), tag FROM (SELECT k, tag FROM n ORDER BY tag)",
    // CTEs and views.
    "WITH w AS (SELECT a AS v, b FROM t ORDER BY a) SELECT max(v), b FROM w",
    "WITH w AS (SELECT a AS v, b FROM t ORDER BY a LIMIT 4) SELECT min(v), b FROM w",
    "SELECT max(v), b FROM vt",
    "SELECT max(v), b, c FROM vt GROUP BY c ORDER BY c",
    // Joins.
    "SELECT max(t.a), u.x FROM t JOIN u ON u.a = t.a",
    "SELECT min(t.a), u.x FROM t JOIN u ON u.a = t.a",
    "SELECT max(t.a), t.b, u.x FROM t JOIN u ON u.a = t.a",
    "SELECT max(t.a), t.b, u.x, t.c FROM t JOIN u ON u.a = t.a GROUP BY t.c ORDER BY t.c",
    "SELECT max(t.a), u.x FROM t LEFT JOIN u ON u.a = t.a",
    "SELECT min(t.a), u.x FROM t LEFT JOIN u ON u.a = t.a",
    "SELECT max(s.v), s.b FROM (SELECT a AS v, b FROM t ORDER BY a) s JOIN u ON u.a = s.v",
    "SELECT max(u.x), t.b FROM t JOIN u ON u.a = t.a",
    // HAVING that repeats the tracked aggregate (single-table and join paths).
    "SELECT max(a), b FROM t HAVING max(a) > 0",
    "SELECT max(a), b, c FROM t GROUP BY c HAVING max(a) > 1 ORDER BY c",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a) HAVING max(v) > 0",
    "SELECT max(t.a), u.x FROM t JOIN u ON u.a = t.a HAVING max(t.a) > 2",
    // Single-table paths, which already tracked the extremum row.
    "SELECT max(a), b FROM t",
    "SELECT min(a), b FROM t",
    "SELECT max(a), b, c FROM t GROUP BY c ORDER BY c",
    "SELECT max(a), b FROM t WHERE c < 3",
    // A NULL argument row supplies the bare columns until the first non-NULL
    // value (stock's minmaxStep), so an all-NULL group reports its last row.
    "SELECT max(a), b FROM z WHERE g = 1",
    "SELECT g, max(a), b FROM z GROUP BY g ORDER BY g",
    "SELECT g, min(a), b FROM z GROUP BY g ORDER BY g",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM z WHERE g = 1 ORDER BY b)",
    "SELECT g, max(v), b FROM (SELECT a AS v, b, g FROM z ORDER BY b) GROUP BY g ORDER BY g",
    "SELECT max(a) FILTER (WHERE g = 2), b FROM z",
    "SELECT max(a) FILTER (WHERE a IS NULL), b FROM z",
    "SELECT max(v) FILTER (WHERE v IS NULL), b FROM (SELECT a AS v, b FROM z ORDER BY b)",
    "SELECT max(z.a), z.b FROM z JOIN u ON u.a = z.g WHERE z.g = 1",
    "SELECT z.g, max(z.a), z.b FROM z JOIN u ON u.a = z.g GROUP BY z.g ORDER BY z.g",
    // Still a single min()/max(): a repeat of the same call is the same
    // aggregate, count() never decides the bare-column row, and a HAVING alias
    // names the tracked aggregate (single-table, subquery and join paths).
    "SELECT max(a), b, max(a) + 1 FROM t",
    "SELECT max(a), count(*), b FROM t",
    "SELECT count(a), min(a), b FROM t",
    "SELECT max(a), b, c, count(DISTINCT b) FROM t GROUP BY c ORDER BY c",
    "SELECT max(a) AS m, b, c FROM t GROUP BY c HAVING m > 1 ORDER BY c",
    "SELECT max(a), b, c FROM t GROUP BY c HAVING count(*) > 1 ORDER BY c",
    "SELECT max(a) FILTER (WHERE c < 3), count(*) FILTER (WHERE c = 3), b FROM t",
    "SELECT max(v), count(*), b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT max(v) AS m, b FROM (SELECT a AS v, b FROM t ORDER BY a) HAVING m > 0",
    "SELECT max(t.a), count(*), u.x FROM t JOIN u ON u.a = t.a",
    "SELECT max(t.a), u.x, max(t.a) * 2 FROM t JOIN u ON u.a = t.a",
    "SELECT max(t.a) AS m, u.x, t.c FROM t JOIN u ON u.a = t.a GROUP BY t.c HAVING m > 1 ORDER BY t.c",
];

/// Unaliased result expressions are named by their source text.
const NAME_QUERIES: &[&str] = &[
    "SELECT max(a), b FROM t",
    "SELECT count(*), sum(a) AS s, min( a ) FROM t",
    "SELECT a + 1, a+1, (a), (a + 1) * 2, -a, ((a)) FROM t",
    "SELECT 1, 'x', NULL, ?1, :p",
    "SELECT max(v), b FROM (SELECT a AS v, b FROM t ORDER BY a)",
    "SELECT max(v) || ':' || b FROM (SELECT a AS v, b FROM t)",
    "SELECT x+1 FROM (SELECT a AS x FROM t)",
    "SELECT CASE WHEN a > 2 THEN 'big' ELSE 'small' END, CAST(a AS TEXT) FROM t",
    "SELECT a IS NULL, a BETWEEN 1 AND 3, a IN (1, 2), b LIKE 'b%', b COLLATE NOCASE FROM t",
    "SELECT (SELECT max(a) FROM t), EXISTS (SELECT 1 FROM u WHERE u.a = t.a) FROM t",
    "SELECT count(*) OVER (ORDER BY a), max(a) FILTER (WHERE c > 1) FROM t",
    "SELECT t.a, u.x, t.a * u.a FROM t JOIN u ON u.a = t.a",
    "SELECT max(a) /* trailing comment */ , b FROM t",
    "SELECT /* leading */ a + 1, a\n  + 2 -- line comment\n FROM t",
    "SELECT a + 1 FROM t UNION ALL SELECT a FROM t",
    "SELECT max(a) FROM t GROUP BY c HAVING count(*) > 1 ORDER BY 1 LIMIT 2",
    "  SELECT  a  +  1  FROM t;",
];

async fn assert_values_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let ff: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ff, rr, "mismatch on `{sql}`");
}

#[test]
fn single_minmax_bare_columns_come_from_the_extremum_row() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_xik4y_values.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in VALUE_QUERIES {
                assert_values_agree(&f, &r, sql).await;
            }
        });
    }
}

#[test]
fn unaliased_result_expressions_are_named_by_their_source_text() {
    asupersync::test_utils::run_test(|| async move {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        for sql in NAME_QUERIES {
            let prepared = f
                .prepare(sql)
                .await
                .unwrap_or_else(|e| panic!("franken prepare `{sql}`: {e:?}"));
            let franken: Vec<String> = prepared.column_names().to_vec();
            let stmt = r.prepare(sql).expect("rusqlite prepare");
            let stock: Vec<String> = stmt
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            assert_eq!(franken, stock, "column names of `{sql}`");
        }
    });
}
