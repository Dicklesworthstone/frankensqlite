#![recursion_limit = "512"]

//! A CAST inside a correlated subquery converts its operand, as it does
//! everywhere else.
//!
//! The correlated-subquery emitter (`emit_expr_with_fallback`) compiled
//! `CAST(x AS type)` to an `Affinity` op whose P4 was a plain string, which the
//! engine ignores, so the CAST did nothing: on file-backed connections
//! `EXISTS (SELECT 1 FROM b WHERE b.at = CAST(a.n AS TEXT))` seeked the TEXT
//! index with the integer and found nothing. Even a working affinity would be
//! wrong: `CAST('abc' AS INTEGER)` is 0 and `CAST(x'31' AS TEXT)` is '1', which
//! no affinity produces. Results are compared with rusqlite, in memory and
//! file-backed.
//!
//! One in-memory shape is left out: a typeless column against a CAST to TEXT
//! (`b.ax = CAST(a.n AS TEXT)`). SQLite gives both operands an affinity and so
//! compares them raw; the in-memory correlated lane in fsqlite-core treats the
//! typeless column as having none and applies TEXT, so the integer 2 matches
//! '2'. That lane is separate from the codegen fixed here (its TypeAffinity
//! does not tell BLOB from no affinity) and fails the same way before the fix.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i{n}"),
        SqliteValue::Float(f) => format!("r{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("i{n}"),
        rusqlite::types::Value::Real(f) => format!("r{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE a(id INTEGER PRIMARY KEY, t TEXT, n INTEGER, r REAL, x)",
    "INSERT INTO a VALUES (1,'1',1,1.0,1),(2,'2.7',2,2.7,x'31'),(3,'abc',3,3.5,'2'),\
     (4,NULL,NULL,NULL,NULL),(6,' 6',6,6.0,'abc')",
    "CREATE TABLE b(id INTEGER PRIMARY KEY, at TEXT, an INTEGER, ar REAL, ax)",
    "INSERT INTO b VALUES (10,'1',1,1.0,'1'),(11,'2',2,2.0,2),(12,'0',0,0.0,0),\
     (13,'6',6,3.5,'abc'),(14,NULL,NULL,NULL,NULL),(15,'3',3,2.7,x'31')",
    "CREATE INDEX b_at ON b(at)",
    "CREATE INDEX b_an ON b(an)",
    "CREATE INDEX b_ar ON b(ar)",
    "CREATE INDEX b_ax ON b(ax)",
];

fn queries() -> Vec<String> {
    let mut queries = Vec::new();
    for (column, casts) in [
        ("at", ["TEXT", "VARCHAR(5)", "BLOB"]),
        ("an", ["INTEGER", "NUMERIC", "TEXT"]),
        ("ar", ["REAL", "INTEGER", "NUMERIC"]),
        ("ax", ["TEXT", "INTEGER", "BLOB"]),
    ] {
        for cast in casts {
            for outer in ["a.id", "a.t", "a.n", "a.r", "a.x", "a.n + 0"] {
                let probe = format!("CAST({outer} AS {cast})");
                queries.push(format!(
                    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.{column} = {probe}) \
                     ORDER BY a.id"
                ));
                queries.push(format!(
                    "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE {probe} = b.{column} \
                     AND b.id > 0) ORDER BY a.id"
                ));
                queries.push(format!(
                    "SELECT a.id, (SELECT b.id FROM b WHERE b.{column} = {probe} ORDER BY b.id \
                     LIMIT 1) FROM a ORDER BY a.id"
                ));
                queries.push(format!(
                    "SELECT a.id, (SELECT count(*) FROM b WHERE CAST(b.{column} AS TEXT) = \
                     CAST({outer} AS TEXT)) FROM a ORDER BY a.id"
                ));
            }
        }
    }
    queries
}

#[test]
fn correlated_cast_probes_match_sqlite() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("correlated_cast.db");
            let f = if file_backed {
                Connection::open(path.to_str().unwrap()).await.unwrap()
            } else {
                Connection::open(":memory:").await.unwrap()
            };
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            let mut found = Vec::new();
            for sql in queries() {
                let typeless_vs_text_cast = (sql.contains("b.ax = CAST(") || sql.contains("= b.ax"))
                    && sql.contains(" AS TEXT)");
                if !file_backed && typeless_vs_text_cast {
                    continue;
                }
                let ff: Result<Vec<String>, String> = f
                    .query(&sql)
                    .await
                    .map(|rows| {
                        rows.iter()
                            .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                            .collect()
                    })
                    .map_err(|error| error.to_string());
                let rr: Result<Vec<String>, String> = (|| {
                    let mut stmt = r.prepare(&sql)?;
                    let ncol = stmt.column_count();
                    stmt.query_map([], |row| {
                        Ok((0..ncol)
                            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                            .collect::<Vec<_>>()
                            .join("|"))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                })()
                .map_err(|error: rusqlite::Error| error.to_string());
                if ff != rr {
                    found.push(format!(
                        "[file_backed={file_backed}] {sql}\n  fsqlite: {ff:?}\n  stock:   {rr:?}"
                    ));
                }
            }
            assert!(
                found.is_empty(),
                "{} mismatches vs stock:\n{}",
                found.len(),
                found.join("\n")
            );
        });
    }
}
