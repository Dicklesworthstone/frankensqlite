//! bd-dda91 — Oracle-parity e2e: UNIQUE / NOT NULL + conflict resolution vs rusqlite.
//!
//! Covers UNIQUE (single + multi-column) and NOT NULL enforcement, the rule
//! that a UNIQUE column permits multiple NULLs, NOT NULL with a DEFAULT, and the
//! statement-level conflict clauses INSERT OR IGNORE / OR REPLACE / OR FAIL plus
//! `REPLACE INTO` and a column-level `ON CONFLICT`. Complements upsert_on_conflict
//! (which covers ON CONFLICT DO UPDATE). DML is autocommit; each scenario runs
//! on both engines, asserting per-statement agreement, then compares state.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn unique_column_violation() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
                "INSERT INTO t VALUES (1,'a@x'),(2,'b@x')",
                "INSERT INTO t VALUES (3,'a@x')", // dup email -> error on both
                "INSERT INTO t VALUES (4,'c@x')", // ok
            ],
            &["SELECT id, email FROM t ORDER BY id"], // 1,2,4
            "unique_column_violation",
        )
        .await;
    });
}

#[test]
fn unique_multi_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a, b))",
                "INSERT INTO t VALUES (1,1,2),(2,1,3),(3,2,2)", // distinct (a,b)
                "INSERT INTO t VALUES (4,1,2)",                 // dup (1,2) -> error
                "INSERT INTO t VALUES (5,2,3)",                 // ok
            ],
            &["SELECT id, a, b FROM t ORDER BY id"], // 1,2,3,5
            "unique_multi_column",
        )
        .await;
    });
}

#[test]
fn unique_allows_multiple_nulls() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT UNIQUE)",
                "INSERT INTO t VALUES (1,NULL),(2,NULL),(3,'x')", // multiple NULLs allowed
                "INSERT INTO t VALUES (4,'x')",                   // dup 'x' -> error
            ],
            &[
                "SELECT id, tag FROM t ORDER BY id", // 1,2,3
                "SELECT count(*) FROM t WHERE tag IS NULL",
            ],
            "unique_allows_multiple_nulls",
        )
        .await;
    });
}

#[test]
fn not_null_violation_and_default() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL DEFAULT 'd')",
                "INSERT INTO t VALUES (1,'x','y')",    // ok
                "INSERT INTO t(id, a) VALUES (2,'z')", // b uses default 'd'
                "INSERT INTO t VALUES (3, NULL, 'q')", // a NULL -> NOT NULL violation
                "INSERT INTO t(id, b) VALUES (4,'w')", // a missing+no default -> NOT NULL violation
            ],
            &["SELECT id, a, b FROM t ORDER BY id"], // 1 and 2
            "not_null_violation_and_default",
        )
        .await;
    });
}

#[test]
fn insert_or_ignore() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x',10)",
                "INSERT OR IGNORE INTO t VALUES (2,'a@x',20)", // conflict -> row skipped, no error
                "INSERT OR IGNORE INTO t VALUES (3,'b@x',30)", // ok
                // PK conflict also ignored.
                "INSERT OR IGNORE INTO t VALUES (1,'c@x',99)",
            ],
            &["SELECT id, email, v FROM t ORDER BY id"], // 1 (unchanged) and 3
            "insert_or_ignore",
        )
        .await;
    });
}

#[test]
fn insert_or_replace_and_replace_into() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x',10),(2,'b@x',20)",
                // OR REPLACE on a UNIQUE(email) conflict deletes the old row, inserts new.
                "INSERT OR REPLACE INTO t VALUES (3,'a@x',99)", // removes id=1, adds id=3
                "REPLACE INTO t VALUES (2,'b@x',77)", // PK conflict -> replaces id=2's row
            ],
            &["SELECT id, email, v FROM t ORDER BY id"], // (2,b,77) and (3,a,99)
            "insert_or_replace",
        )
        .await;
    });
}

#[test]
fn insert_or_fail_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
                "INSERT INTO t VALUES (1,'a@x')",
                "INSERT OR FAIL INTO t VALUES (2,'a@x')", // conflict -> error on both
                "INSERT OR ABORT INTO t VALUES (3,'a@x')", // conflict -> error on both
                "INSERT INTO t VALUES (4,'b@x')",         // ok
            ],
            &["SELECT id, email FROM t ORDER BY id"], // 1 and 4
            "insert_or_fail_errors",
        )
        .await;
    });
}

#[test]
fn column_level_on_conflict_ignore() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                // Column-level ON CONFLICT IGNORE: a plain INSERT that conflicts is
                // silently skipped (the column's conflict clause applies).
                "CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE ON CONFLICT IGNORE)",
                "INSERT INTO t VALUES (1,'a')",
                "INSERT INTO t VALUES (2,'a')", // conflict -> ignored (no OR clause needed)
                "INSERT INTO t VALUES (3,'b')", // ok
            ],
            &["SELECT id, code FROM t ORDER BY id"], // 1 and 3
            "column_level_on_conflict_ignore",
        )
        .await;
    });
}
