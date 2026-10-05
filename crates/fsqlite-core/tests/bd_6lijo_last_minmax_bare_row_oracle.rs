#![recursion_limit = "512"]

//! bd-6lijo: with several min()/max() aggregates SQLite reads the bare columns
//! from the row the LAST one (in the order it collects aggregates: result
//! columns, then ORDER BY, then HAVING) last chose, because each min()/max()
//! step resets the shared skip register. `SELECT c, max(a), b ... GROUP BY c
//! HAVING min(a) > 0` reads b from the min() row. fsqlite gave up tracking
//! with a second min()/max() and read the group's first row (or, on the join
//! path, another row), which is neither extremum. Separately, the detector
//! resolved a HAVING name to a result alias even when a FROM column shares the
//! name, while the evaluator (GH#174) resolves the column first, so
//! `SELECT c, max(a) AS b ... HAVING b >= 'y'` lost the tracking.
//!
//! Each query is compared with rusqlite, ad hoc and prepared, in memory and
//! file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(c, a, b)",
    "INSERT INTO t VALUES (1,1,'x1'),(1,5,'y1'),(1,3,'z1'),(2,7,'x2'),(2,9,'y2'),(2,2,'z2'),\
     (3,4,'x3'),(3,4,'y3'),(3,NULL,'z3'),(4,6,'p4'),(4,1,'q4'),(4,6,'r4')",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, c)",
    "INSERT INTO u VALUES (1,1),(2,2),(3,3),(4,4)",
];

const QUERIES: &[&str] = &[
    // The last min()/max() decides, grouped.
    "SELECT c, max(a), b FROM t GROUP BY c HAVING min(a) > 0 ORDER BY c",
    "SELECT c, min(a), max(a), b FROM t GROUP BY c ORDER BY c",
    "SELECT c, max(a), min(a), b FROM t GROUP BY c ORDER BY c",
    "SELECT c, max(a), b FROM t GROUP BY c HAVING max(a) > min(a) ORDER BY c",
    "SELECT c, max(a), b FROM t GROUP BY c ORDER BY min(a), c",
    "SELECT c, max(a) - min(a), b FROM t GROUP BY c ORDER BY c",
    "SELECT c, max(a), b, min(a), max(a) + 0 FROM t GROUP BY c ORDER BY c",
    // Join and non-flattened subquery paths.
    "SELECT t.c, max(t.a), t.b FROM t JOIN u ON u.c = t.c GROUP BY t.c HAVING min(t.a) > 0 \
     ORDER BY t.c",
    "SELECT t.c, min(t.a), max(t.a), t.b FROM t JOIN u ON u.c = t.c GROUP BY t.c ORDER BY t.c",
    "SELECT c, max(a), b FROM (SELECT * FROM t ORDER BY b) GROUP BY c HAVING min(a) > 0 \
     ORDER BY c",
    // Whole table. A prepared statement the bytecode cannot track (an ORDER
    // BY aggregate, a wrapper reading a bare column, DISTINCT) takes the
    // interpreter, as ad hoc execution does.
    "SELECT max(a), b FROM t HAVING min(a) > 0",
    "SELECT max(a), min(a), b FROM t",
    "SELECT min(a), max(a), b FROM t",
    "SELECT max(a) - min(a), b FROM t",
    "SELECT max(a), b FROM t ORDER BY min(a)",
    "SELECT max(a) || ':' || b FROM t",
    "SELECT max(DISTINCT a), b FROM t",
    // A HAVING name that is both a FROM column and a result alias is the column.
    "SELECT c, max(a) AS b FROM t GROUP BY c HAVING b >= 'y' ORDER BY c",
    "SELECT c, min(a) AS b FROM t GROUP BY c HAVING b < 'y' ORDER BY c",
    "SELECT max(a) AS b FROM t HAVING b >= 'y'",
    // Aliases that name no column still stand for the result expression.
    "SELECT c, max(a) AS m, b FROM t GROUP BY c HAVING m > 4 AND b > 'x' ORDER BY c",
    "SELECT c, max(a) AS a, b FROM t GROUP BY c HAVING a > 4 ORDER BY c",
];

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn rows_prepared(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("franken prepare `{sql}`: {e:?}"))
        .query()
        .await
        .unwrap_or_else(|e| panic!("franken prepared query `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn last_minmax_decides_the_bare_column_row() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_6lijo.db")
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
            for sql in QUERIES {
                let stock = rows_r(&r, sql);
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
        });
    }
}
