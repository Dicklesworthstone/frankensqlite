#![recursion_limit = "512"]

//! bd-1ht9p: a parenthesized join as the first FROM source failed with "not
//! implemented: non-table FROM source"; stock returns the rows. SQLite joins
//! associate to the left, so `FROM (a JOIN b) JOIN c` is `FROM a JOIN b JOIN
//! c`. Each statement's rows are compared with stock SQLite (rusqlite,
//! bundled), ad hoc and prepared, on `:memory:` and on a file.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(x, a)",
    "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    "CREATE TABLE u(x, b)",
    "INSERT INTO u VALUES (2, 'u2'), (3, 'u3'), (4, 'u4')",
    "CREATE TABLE w(x, c)",
    "INSERT INTO w VALUES (3, 'w3'), (4, 'w4')",
    "CREATE TABLE tonly(y)",
    "INSERT INTO tonly VALUES (10), (20)",
    "CREATE TEMP TABLE tt(x, d)",
    "INSERT INTO tt VALUES (1, 'tt1'), (3, 'tt3')",
    "CREATE VIEW v AS SELECT t.x, u.b FROM (t JOIN u USING (x))",
];

const QUERIES: &[&str] = &[
    // The bead's shapes.
    "SELECT t.x FROM (t JOIN tonly) ORDER BY 1",
    "SELECT main.t.x, tt.d FROM (main.t JOIN temp.tt USING (x)) ORDER BY 1",
    // ON / USING / NATURAL / comma joins inside the parentheses.
    "SELECT * FROM (t JOIN u ON t.x = u.x) ORDER BY 1",
    "SELECT * FROM (t JOIN u USING (x)) ORDER BY 1",
    "SELECT * FROM (t NATURAL JOIN u) ORDER BY 1",
    "SELECT count(*) FROM (t, u)",
    "SELECT t.a, u.b FROM (t AS t JOIN u AS u ON t.x = u.x) WHERE u.b > 'u2' ORDER BY 1",
    // Outer joins inside and after the parentheses.
    "SELECT * FROM (t LEFT JOIN u USING (x)) ORDER BY 1",
    "SELECT t.x, u.b, w.c FROM (t JOIN u ON t.x = u.x) LEFT JOIN w ON w.x = u.x ORDER BY 1",
    "SELECT t.x, w.c FROM (t LEFT JOIN u ON t.x = u.x) LEFT JOIN w ON w.x = u.x ORDER BY 1",
    // Nested and single-table parentheses.
    "SELECT * FROM ((t JOIN u USING (x))) ORDER BY 1",
    "SELECT * FROM ((t JOIN u USING (x)) JOIN w USING (x)) ORDER BY 1",
    "SELECT * FROM (t) JOIN u USING (x) ORDER BY 1",
    // Aggregates, DISTINCT, compound arms, and nested positions.
    "SELECT u.b, count(*) FROM (t JOIN u USING (x)) GROUP BY u.b ORDER BY 1",
    "SELECT DISTINCT u.x FROM (t JOIN u) ORDER BY 1",
    "SELECT t.x FROM (t JOIN u USING (x)) UNION SELECT x FROM (w) ORDER BY 1",
    "SELECT (SELECT count(*) FROM (t JOIN u USING (x)))",
    "SELECT x FROM (SELECT t.x FROM (t JOIN u USING (x))) ORDER BY 1",
    "SELECT x FROM t WHERE x IN (SELECT u.x FROM (u JOIN w USING (x))) ORDER BY 1",
    "SELECT x FROM t WHERE EXISTS (SELECT 1 FROM (u JOIN w USING (x)) WHERE u.x = t.x) ORDER BY 1",
    "WITH c AS (SELECT t.x FROM (t JOIN u USING (x))) SELECT * FROM c ORDER BY 1",
    "SELECT * FROM v ORDER BY 1",
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = r.prepare(sql).map_err(|e| e.to_string())?;
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .map_err(|e| e.to_string())
}

fn frank_rows(
    result: fsqlite_error::Result<Vec<fsqlite_core::connection::Row>>,
) -> Result<Vec<Vec<String>>, String> {
    result
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("1ht9p.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in SETUP {
        f.execute(sql).await.expect("frank setup");
        r.execute(sql, []).expect("stock setup");
    }
    let mut failures = Vec::new();
    for sql in QUERIES {
        let stock = stock_rows(&r, sql);
        assert!(stock.is_ok(), "stock must accept `{sql}`: {stock:?}");
        let direct = frank_rows(f.query(sql).await);
        if direct != stock {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (ad hoc): frank {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = frank_rows(match f.prepare(sql).await {
            Ok(statement) => statement.query().await,
            Err(e) => Err(e),
        });
        if prepared != stock {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (prepared): frank {prepared:?} vs stock {stock:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn a_leading_parenthesized_join_answers_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            failures.extend(run(file_backed).await);
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
