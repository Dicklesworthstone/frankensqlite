//! bd-z9sdp — Oracle-parity e2e: multi-constraint column definitions.
//!
//! A single column may carry several constraints at once
//! (`NOT NULL DEFAULT 5 CHECK (a > 0) UNIQUE`); all of them must be enforced
//! together, and an omitted value takes the DEFAULT — which is itself then
//! subject to the column's UNIQUE/CHECK. This also covers a `COLLATE NOCASE
//! UNIQUE` column (case-insensitive uniqueness). Each scenario asserts
//! per-statement agreement with rusqlite, then compares the resulting rows.
#![recursion_limit = "512"]

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
fn all_four_constraints_on_one_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, \
             a INTEGER NOT NULL DEFAULT 5 CHECK (a > 0) UNIQUE)",
                "INSERT INTO t(id,a) VALUES (1,10)",   // ok
                "INSERT INTO t(id,a) VALUES (2,10)",   // UNIQUE(a) violation -> error
                "INSERT INTO t(id,a) VALUES (3,-5)",   // CHECK(a>0) violation -> error
                "INSERT INTO t(id,a) VALUES (4,NULL)", // NOT NULL violation -> error
                "INSERT INTO t(id) VALUES (5)",        // a defaults to 5 -> ok
            ],
            &["SELECT id, a FROM t ORDER BY id"], // (1,10),(5,5)
            "all_four_constraints_on_one_column",
        )
        .await;
    });
}

#[test]
fn default_value_rechecked_against_unique() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, \
             a INTEGER NOT NULL DEFAULT 5 CHECK (a > 0) UNIQUE)",
                "INSERT INTO t(id) VALUES (1)", // a -> default 5
                "INSERT INTO t(id) VALUES (2)", // a -> default 5 again -> UNIQUE violation -> error
                "INSERT INTO t(id,a) VALUES (3,7)",
            ],
            &["SELECT id, a FROM t ORDER BY id"], // (1,5),(3,7)
            "default_value_rechecked_against_unique",
        )
        .await;
    });
}

#[test]
fn collate_nocase_unique_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE UNIQUE)",
                "INSERT INTO t VALUES (1,'Apple')",
                "INSERT INTO t VALUES (2,'apple')", // NOCASE-equal -> UNIQUE violation -> error
                "INSERT INTO t VALUES (3,'Banana')",
            ],
            &[
                "SELECT id, name FROM t ORDER BY id", // (1,'Apple'),(3,'Banana')
                "SELECT id FROM t WHERE name = 'APPLE'", // NOCASE -> 1
            ],
            "collate_nocase_unique_column",
        )
        .await;
    });
}

#[test]
fn check_and_default_expression() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                // DEFAULT is an expression; CHECK references the column.
                "CREATE TABLE t (id INTEGER PRIMARY KEY, \
             qty INTEGER DEFAULT (2 * 5) CHECK (qty BETWEEN 0 AND 100))",
                "INSERT INTO t(id) VALUES (1)", // qty -> 10
                "INSERT INTO t(id,qty) VALUES (2,50)",
                "INSERT INTO t(id,qty) VALUES (3,200)", // CHECK fails -> error
            ],
            &["SELECT id, qty FROM t ORDER BY id"], // (1,10),(2,50)
            "check_and_default_expression",
        )
        .await;
    });
}
