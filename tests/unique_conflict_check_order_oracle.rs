#![recursion_limit = "512"]

//! UNIQUE constraints are checked in stock SQLite's order (found in bd-v9dk8).
//!
//! Stock checks a row against its table's indexes in `Table.pIndex` order:
//! newest index first, except that an index declared `ON CONFLICT REPLACE`
//! comes after every index that is not. FrankenSQLite checked them in creation
//! order. When a row violated two UNIQUE constraints that named the wrong one,
//! and a REPLACE index created before an IGNORE index deleted its conflicting
//! row before the IGNORE skipped the new one: a row silently lost. Each case
//! is compared with rusqlite (outcome, rows, `PRAGMA index_list`) on a fresh
//! file database, and again after the setup is closed and reopened, so the
//! order also holds once the schema is reloaded.
//!
//! Not covered (still differ): an INTEGER PRIMARY KEY declared ON CONFLICT
//! REPLACE, which stock checks after the other UNIQUE constraints; and a
//! WITHOUT ROWID primary key, which stock checks at its place in the index
//! list rather than first.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

const SEED: &str = "INSERT INTO m(a, b, d) VALUES (1, 10, 2), (2, 20, 3), (3, 30, 4)";

/// (table DDL and any extra indexes, statement). Every statement's row
/// conflicts with row 1 on `b` and with row 2 on `d`, unless noted.
const CASES: &[(&str, &str)] = &[
    // Two ABORT constraints: stock names the newer one.
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "INSERT INTO m VALUES (9, 10, 3)"),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "INSERT INTO m SELECT 9, 10, 3"),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "UPDATE m SET b = 10, d = 3 WHERE a = 3"),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "UPDATE m SET b = b + 10, d = d + 1"),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "INSERT OR FAIL INTO m VALUES (9, 10, 3)"),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "INSERT OR ROLLBACK INTO m VALUES (9, 10, 3)"),
    (
        "CREATE TABLE m(a, b UNIQUE, d UNIQUE); CREATE UNIQUE INDEX m_a ON m(a)",
        "INSERT INTO m VALUES (1, 10, 3)",
    ),
    (
        "CREATE TABLE m(a, b, d); CREATE UNIQUE INDEX m_d ON m(d); CREATE UNIQUE INDEX m_b ON m(b)",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE, d UNIQUE, CHECK (a > 0)); CREATE INDEX m_ad ON m(a, d)",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    // A REPLACE index must not delete before an IGNORE or ABORT index decides.
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT IGNORE)",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT IGNORE)",
        "INSERT INTO m SELECT 9, 10, 3",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT IGNORE)",
        "UPDATE m SET b = 10, d = 3 WHERE a = 3",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT ABORT)",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT IGNORE)",
        "INSERT INTO m VALUES (9, 10, 5)",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT IGNORE, d UNIQUE ON CONFLICT REPLACE)",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT REPLACE, \
         c UNIQUE ON CONFLICT IGNORE)",
        "INSERT INTO m(a, b, d, c) VALUES (9, 10, 3, NULL)",
    ),
    // Statement-level conflict clauses keep the declared order.
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE)",
        "INSERT OR ABORT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE, d UNIQUE ON CONFLICT REPLACE)",
        "INSERT OR ABORT INTO m VALUES (9, 10, 3)",
    ),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "INSERT OR REPLACE INTO m VALUES (9, 10, 3)"),
    ("CREATE TABLE m(a, b UNIQUE, d UNIQUE)", "INSERT OR IGNORE INTO m VALUES (9, 10, 3)"),
    // An upsert with no conflict target acts on the first constraint stock checks.
    (
        "CREATE TABLE m(a, b UNIQUE, d UNIQUE)",
        "INSERT INTO m VALUES (9, 10, 3) ON CONFLICT DO UPDATE SET a = a + 100",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE, d UNIQUE)",
        "INSERT INTO m VALUES (9, 10, 3) ON CONFLICT DO NOTHING",
    ),
    (
        "CREATE TABLE m(a, b UNIQUE ON CONFLICT REPLACE, d UNIQUE)",
        "INSERT INTO m VALUES (9, 10, 3) ON CONFLICT DO UPDATE SET a = a + 100",
    ),
    // WITHOUT ROWID secondary UNIQUE constraints (the primary key is not hit).
    (
        "CREATE TABLE m(a PRIMARY KEY, b UNIQUE, d UNIQUE) WITHOUT ROWID",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a PRIMARY KEY, b UNIQUE, d UNIQUE) WITHOUT ROWID",
        "UPDATE m SET b = 10, d = 3 WHERE a = 3",
    ),
    (
        "CREATE TABLE m(a PRIMARY KEY, b UNIQUE ON CONFLICT REPLACE, d UNIQUE ON CONFLICT IGNORE) \
         WITHOUT ROWID",
        "INSERT INTO m VALUES (9, 10, 3)",
    ),
    (
        "CREATE TABLE m(a PRIMARY KEY, b UNIQUE, d UNIQUE) WITHOUT ROWID",
        "INSERT INTO m VALUES (9, 10, 3) ON CONFLICT DO UPDATE SET a = a + 100",
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

fn frank_outcome(result: &Result<(), FrankenError>) -> String {
    match result {
        Ok(()) => "ok".to_owned(),
        Err(e) => format!("error {}: {e}", e.error_code() as i32),
    }
}

fn stock_outcome(result: &rusqlite::Result<()>) -> String {
    match result {
        Ok(()) => "ok".to_owned(),
        Err(rusqlite::Error::SqliteFailure(e, Some(m))) => {
            format!("error {}: {m}", e.extended_code & 0xff)
        }
        Err(e) => format!("error: {e}"),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).expect("stock prepare");
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    })
    .and_then(Iterator::collect)
    .expect("stock query")
}

const CHECKS: &[&str] = &[
    "SELECT * FROM m ORDER BY a",
    "SELECT name FROM pragma_index_list('m')",
];

async fn run_case(setup: &str, stmt: &str, reopen: bool, failures: &mut Vec<String>) {
    let label = format!("[reopen={reopen}] {setup} || {stmt}");
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    let script = format!("{setup}; {SEED}");
    let mut frank = Connection::open(&path).await.expect("open");
    frank.execute_batch(&script).await.expect("frank setup");
    if reopen {
        frank.close().await.expect("close");
        frank = Connection::open(&path).await.expect("reopen");
    }
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    stock.execute_batch(&script).expect("stock setup");

    let fo = frank_outcome(&frank.execute_batch(stmt).await);
    let so = stock_outcome(&stock.execute_batch(stmt));
    if fo != so {
        failures.push(format!("{label}: outcome frank {fo:?} vs stock {so:?}"));
    }
    for sql in CHECKS {
        // `pragma_index_list` as a table-valued function is stock-only syntax
        // here; FrankenSQLite answers the PRAGMA form.
        let fsql = if sql.contains("pragma_index_list") {
            "PRAGMA index_list(m)"
        } else {
            sql
        };
        let mut fv = frank_rows(&frank, fsql).await;
        if fsql.starts_with("PRAGMA") {
            fv = fv.into_iter().map(|row| vec![row[1].clone()]).collect();
        }
        let sv = stock_rows(&stock, sql);
        if fv != sv {
            failures.push(format!("{label}: `{sql}`\n  frank {fv:?}\n  stock {sv:?}"));
        }
    }
    frank.close().await.expect("close");
}

#[test]
fn unique_constraints_are_checked_in_stock_order() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for reopen in [false, true] {
            for (setup, stmt) in CASES {
                run_case(setup, stmt, reopen, &mut failures).await;
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {} cases differ from SQLite:\n{}",
            failures.len(),
            2 * CASES.len(),
            failures.join("\n")
        );
    });
}
