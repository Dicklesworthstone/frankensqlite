//! bd-qnq6r — Oracle-parity e2e: constraints on generated columns vs rusqlite.
//!
//! generated_columns_oracle covers basic generated columns and indexed lookups;
//! this exercises CONSTRAINTS on the computed value: a UNIQUE generated column
//! (two rows whose computed value collides must conflict), a CHECK on a
//! generated column, the re-validation of a UNIQUE generated column when an
//! UPDATE to a base column changes the computed value into a collision, and a
//! VIRTUAL generated column used in a WHERE filter (computed on read). Each
//! scenario asserts per-statement agreement with rusqlite, then compares rows.
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
fn unique_on_generated_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
                 s INTEGER GENERATED ALWAYS AS (a + b) STORED UNIQUE)",
                "INSERT INTO t (id,a,b) VALUES (1,2,3)", // s=5
                "INSERT INTO t (id,a,b) VALUES (2,1,4)", // s=5 -> UNIQUE violation -> error both
                "INSERT INTO t (id,a,b) VALUES (3,10,20)", // s=30 -> ok
            ],
            &["SELECT id, a, b, s FROM t ORDER BY id"], // (1,2,3,5),(3,10,20,30)
            "unique_on_generated_column",
        )
        .await;
    });
}

/// CHECK on a STORED generated column is enforced (the value is materialized).
/// (A VIRTUAL generated column reads NULL — bd-r3303 — so its CHECK would not
/// fire; hence this uses STORED.)
#[test]
fn check_on_stored_generated_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
                 sq INTEGER GENERATED ALWAYS AS (a * a) STORED CHECK (sq < 100))",
                "INSERT INTO t (id,a) VALUES (1,5)",  // sq=25 ok
                "INSERT INTO t (id,a) VALUES (2,10)", // sq=100 -> CHECK fails -> error both
                "INSERT INTO t (id,a) VALUES (3,9)",  // sq=81 ok
            ],
            &["SELECT id, a, sq FROM t ORDER BY id"], // (1,5,25),(3,9,81)
            "check_on_stored_generated_column",
        )
        .await;
    });
}

#[test]
fn unique_generated_revalidated_on_update() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
                 g INTEGER GENERATED ALWAYS AS (a * 10) STORED UNIQUE)",
                "INSERT INTO t (id,a) VALUES (1,1),(2,2)", // g=10,20
                "UPDATE t SET a = 2 WHERE id = 1", // g would become 20 -> collides with id2 -> error
            ],
            &["SELECT id, a, g FROM t ORDER BY id"], // unchanged (1,1,10),(2,2,20)
            "unique_generated_revalidated_on_update",
        )
        .await;
    });
}

/// bd-r3303: a VIRTUAL generated column reads NULL (not computed), so a WHERE
/// filter on it matches nothing. (STORED generated columns work — see the UNIQUE
/// tests above.)
#[test]
fn virtual_generated_in_where() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
                 doubled INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
                "INSERT INTO t (id,a) VALUES (1,5),(2,10),(3,7)",
            ],
            &[
                "SELECT id FROM t WHERE doubled = 20 ORDER BY id", // id2
                "SELECT id, doubled FROM t WHERE doubled > 12 ORDER BY id", // (2,20),(3,14)
            ],
            "virtual_generated_in_where",
        )
        .await;
    });
}

/// bd-r3303: even plain SELECT projection of a VIRTUAL generated column reads
/// NULL — so this is the root of the WHERE/CHECK manifestations above, not a
/// constraint-specific issue. (STORED works, per the UNIQUE / STORED-CHECK tests.)
#[test]
fn virtual_generated_projection_returns_null() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, \
                 d INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
                "INSERT INTO t (id,a) VALUES (1,5),(2,10)",
            ],
            &["SELECT id, d FROM t ORDER BY id"], // expect (1,10),(2,20); frank gives NULLs
            "virtual_generated_projection_returns_null",
        )
        .await;
    });
}
