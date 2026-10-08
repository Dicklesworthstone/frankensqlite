#![recursion_limit = "512"]

//! bd-1jnfu: an UPDATE or DELETE whose WHERE holds a correlated IN subquery on
//! a WITHOUT ROWID table failed in every mode with "IN probe codegen invariant
//! failed: unsupported probe source". The DML lane rewrites such a WHERE to
//! `rowid IN (...)` after evaluating it per row, which a WITHOUT ROWID table
//! cannot use, so the correlated IN reached VDBE codegen. Its matching rows are
//! now frozen by their full primary key (bd-ntt2b's locators). Each case is
//! compared with stock SQLite (rusqlite, bundled) -- changed-row count, then the
//! table -- on `:memory:` and on a file.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const WITHOUT_ROWID: &[&str] = &[
    "CREATE TABLE t(k PRIMARY KEY, v) WITHOUT ROWID",
    "INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c')",
    "CREATE TABLE u(k PRIMARY KEY, v) WITHOUT ROWID",
    "INSERT INTO u VALUES (2,'a'),(4,'x'),(5,'c')",
];
const COMPOSITE: &[&str] = &[
    "CREATE TABLE t(a, b, v, PRIMARY KEY (a, b)) WITHOUT ROWID",
    "INSERT INTO t VALUES (1,'x','p'),(1,'y','p'),(2,'x','q'),(2,'y','r'),(3,'z','r')",
];
const ROWID: &[&str] = &[
    "CREATE TABLE t(k, v)",
    "INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c')",
];

const STATE: &str = "SELECT * FROM t ORDER BY 1, 2";

const CASES: &[(&[&str], &str)] = &[
    (
        WITHOUT_ROWID,
        "DELETE FROM t WHERE k IN (SELECT t2.k FROM t AS t2 WHERE t2.v = t.v AND t2.k > 1)",
    ),
    (
        WITHOUT_ROWID,
        "DELETE FROM t WHERE k NOT IN (SELECT t2.k FROM t AS t2 WHERE t2.v = t.v AND t2.k < t.k)",
    ),
    (
        WITHOUT_ROWID,
        "UPDATE t SET v = v || '!' WHERE k IN (SELECT t2.k FROM t AS t2 WHERE t2.v = t.v AND t2.k <> t.k)",
    ),
    (
        WITHOUT_ROWID,
        "DELETE FROM t WHERE v IN (SELECT u.v FROM u WHERE u.k = t.k)",
    ),
    (
        WITHOUT_ROWID,
        "DELETE FROM t WHERE k IN (SELECT t2.k FROM t AS t2 WHERE t2.v = t.v AND t2.k > 99)",
    ),
    (
        COMPOSITE,
        "DELETE FROM t WHERE a IN (SELECT t2.a FROM t AS t2 WHERE t2.v = t.v AND t2.b <> t.b)",
    ),
    (
        COMPOSITE,
        "UPDATE t SET v = upper(v) WHERE b IN (SELECT t2.b FROM t AS t2 WHERE t2.a = t.a AND t2.v <> t.v)",
    ),
    (
        ROWID,
        "DELETE FROM t WHERE k IN (SELECT t2.k FROM t AS t2 WHERE t2.v = t.v AND t2.k > 1)",
    ),
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

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = r.prepare(sql).expect("stock prepare");
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock query")
}

async fn run(file_backed: bool) -> Vec<String> {
    let mut failures = Vec::new();
    for (index, (setup, statement)) in CASES.iter().enumerate() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = if file_backed {
            dir.path().join("1jnfu.db").to_str().expect("utf-8 path").to_owned()
        } else {
            ":memory:".to_owned()
        };
        let f = Connection::open(&path).await.expect("open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        for sql in *setup {
            f.execute(sql).await.expect("frank setup");
            r.execute(sql, []).expect("stock setup");
        }
        let frank_changes = f.execute(statement).await.map_err(|e| e.to_string());
        let stock_changes = r.execute(statement, []).map_err(|e| e.to_string());
        let frank_state = f
            .query(STATE)
            .await
            .map(|rows| {
                rows.iter()
                    .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            })
            .map_err(|e| e.to_string());
        let stock_state = stock_rows(&r, STATE);
        let label = format!("[file_backed={file_backed} case={index}] `{statement}`");
        if frank_changes.as_ref().ok() != stock_changes.as_ref().ok() {
            failures.push(format!(
                "{label}: changes frank {frank_changes:?} vs stock {stock_changes:?}"
            ));
        }
        if frank_state.as_ref().ok() != Some(&stock_state) {
            failures.push(format!(
                "{label}: table frank {frank_state:?} vs stock {stock_state:?}"
            ));
        }
        f.close().await.expect("close");
    }
    failures
}

#[test]
fn without_rowid_dml_with_a_correlated_in_matches_stock() {
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
