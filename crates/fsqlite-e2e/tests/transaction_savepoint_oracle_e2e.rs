//! bd-7y3hp — Oracle-parity e2e: transaction + SAVEPOINT semantics vs rusqlite.
//!
//! Runs identical statement sequences on FrankenSQLite and rusqlite, then
//! compares the resulting table state. Covers BEGIN/COMMIT (persist),
//! BEGIN/ROLLBACK (discard), single SAVEPOINT ROLLBACK TO / RELEASE, nested
//! savepoints (ROLLBACK TO an outer savepoint discards the inner ones),
//! continuing after ROLLBACK TO without RELEASE, DDL inside a rolled-back
//! transaction, and SAVEPOINT used outside an explicit BEGIN (implicit txn).
//! All data is fixed and deterministic.

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

/// Run `stmts` (a transaction script) on both engines, asserting they agree on
/// success/failure of each statement, then compare `queries`.
async fn scenario(init: &[&str], stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in init {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("{label} init frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("{label} init rusqlite `{s}`: {e}"));
    }
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

const INIT: [&str; 2] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    "INSERT INTO t VALUES (1,10),(2,20)",
];

#[test]
fn txn_commit_and_rollback() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &INIT,
            &[
                "BEGIN",
                "INSERT INTO t VALUES (3,30)",
                "UPDATE t SET v = 99 WHERE id = 1",
                "COMMIT",
            ],
            &["SELECT id, v FROM t ORDER BY id"],
            "txn_commit",
        )
        .await;
        scenario(
            &INIT,
            &[
                "BEGIN",
                "INSERT INTO t VALUES (3,30)",
                "DELETE FROM t WHERE id = 1",
                "ROLLBACK",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // unchanged
            "txn_rollback",
        )
        .await;
    });
}

#[test]
fn savepoint_rollback_to_and_release() {
    asupersync::test_utils::run_test(|| async {
        // ROLLBACK TO undoes work since the savepoint but keeps the savepoint.
        scenario(
            &INIT,
            &[
                "BEGIN",
                "INSERT INTO t VALUES (3,30)",
                "SAVEPOINT sp",
                "INSERT INTO t VALUES (4,40)",
                "UPDATE t SET v = 0 WHERE id = 1",
                "ROLLBACK TO sp", // undo the (4,40) insert and the update
                "INSERT INTO t VALUES (5,50)",
                "RELEASE sp",
                "COMMIT",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // 1..3 original + (5,50)
            "savepoint_rollback_to_then_continue",
        )
        .await;
        // RELEASE merges the savepoint's work into the enclosing transaction.
        scenario(
            &INIT,
            &[
                "BEGIN",
                "SAVEPOINT sp",
                "INSERT INTO t VALUES (3,30)",
                "RELEASE sp",
                "COMMIT",
            ],
            &["SELECT id, v FROM t ORDER BY id"],
            "savepoint_release_merges",
        )
        .await;
    });
}

#[test]
fn savepoint_nested_rollback_to_outer() {
    asupersync::test_utils::run_test(|| async {
        // ROLLBACK TO an outer savepoint discards inner savepoints' work too.
        scenario(
            &INIT,
            &[
                "BEGIN",
                "SAVEPOINT outer_sp",
                "INSERT INTO t VALUES (3,30)",
                "SAVEPOINT inner_sp",
                "INSERT INTO t VALUES (4,40)",
                "UPDATE t SET v = -1",
                "ROLLBACK TO outer_sp", // discards 3,4 and the update
                "INSERT INTO t VALUES (9,90)",
                "RELEASE outer_sp",
                "COMMIT",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // original + (9,90)
            "savepoint_nested_rollback_outer",
        )
        .await;
        // Release inner, then roll back outer.
        scenario(
            &INIT,
            &[
                "BEGIN",
                "SAVEPOINT a",
                "INSERT INTO t VALUES (3,30)",
                "SAVEPOINT b",
                "INSERT INTO t VALUES (4,40)",
                "RELEASE b",     // b's work folds into a
                "ROLLBACK TO a", // discards both 3 and 4
                "RELEASE a",
                "COMMIT",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // unchanged
            "savepoint_release_inner_rollback_outer",
        )
        .await;
    });
}

#[test]
fn savepoint_implicit_transaction() {
    asupersync::test_utils::run_test(|| async {
        // SAVEPOINT outside an explicit BEGIN starts an implicit transaction.
        scenario(
            &INIT,
            &[
                "SAVEPOINT sp",
                "INSERT INTO t VALUES (3,30)",
                "INSERT INTO t VALUES (4,40)",
                "ROLLBACK TO sp",
                "INSERT INTO t VALUES (5,50)",
                "RELEASE sp", // commits the implicit transaction
            ],
            &["SELECT id, v FROM t ORDER BY id"], // original + (5,50)
            "savepoint_implicit_txn",
        )
        .await;
    });
}

#[test]
fn txn_ddl_rollback() {
    asupersync::test_utils::run_test(|| async {
        // DDL inside a rolled-back transaction is undone (table must not exist).
        scenario(
            &INIT,
            &[
                "BEGIN",
                "CREATE TABLE temp_t (x INTEGER)",
                "INSERT INTO temp_t VALUES (1),(2)",
                "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'z'",
                "ROLLBACK",
            ],
            &[
                "SELECT count(*) FROM sqlite_master WHERE name = 'temp_t'", // 0
                "SELECT id, v FROM t ORDER BY id",                          // no extra column added
            ],
            "txn_ddl_rollback",
        )
        .await;
    });
}

#[test]
fn savepoint_reuse_name_after_release() {
    asupersync::test_utils::run_test(|| async {
        // The same savepoint name can be reused after RELEASE.
        scenario(
            &INIT,
            &[
                "BEGIN",
                "SAVEPOINT sp",
                "INSERT INTO t VALUES (3,30)",
                "RELEASE sp",
                "SAVEPOINT sp",
                "INSERT INTO t VALUES (4,40)",
                "ROLLBACK TO sp",
                "RELEASE sp",
                "COMMIT",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // original + (3,30)
            "savepoint_reuse_name",
        )
        .await;
    });
}
