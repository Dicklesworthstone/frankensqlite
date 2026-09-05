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

async fn indexed_overflow_live_integrity(file_backed: bool, journal_mode: Option<&str>) {
    let directory = tempfile::tempdir().expect("temporary database directory");
    let path = directory.path().join("private-reservations.db");
    let f = Connection::open(if file_backed {
        path.to_str().expect("UTF-8 test path")
    } else {
        ":memory:"
    })
    .await
    .expect("open FrankenSQLite");
    assert!(f.is_concurrent_mode_default());
    if let Some(mode) = journal_mode {
        f.execute(&format!("PRAGMA journal_mode={mode}"))
            .await
            .expect("configure real journal mode");
        assert_eq!(
            frank_rows(&f, "PRAGMA journal_mode").await.unwrap(),
            vec![vec![format!("'{}'", mode.to_lowercase())]]
        );
    }
    let r = rusqlite::Connection::open_in_memory().expect("open stock oracle");
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY,u INTEGER UNIQUE,k INTEGER,v TEXT)",
        "CREATE INDEX idx_t_k ON t(k)",
        "BEGIN",
    ] {
        f.execute(sql).await.expect("FrankenSQLite setup");
        r.execute_batch(sql).expect("stock setup");
    }
    let payload = "x".repeat(9000);
    for id in 1..=512 {
        let sql = format!("INSERT INTO t VALUES({id},{id},{},'{payload}')", id % 13);
        f.execute(&sql)
            .await
            .expect("FrankenSQLite overflow insert");
        r.execute_batch(&sql).expect("stock overflow insert");
    }
    let overflow_insert = format!("INSERT INTO t VALUES(1003,1003,8,'{payload}')");
    for sql in [
        "SAVEPOINT outer_sp",
        &overflow_insert,
        "UPDATE t SET k=99,v='changed' WHERE id<=10",
        "SAVEPOINT inner_sp",
        "DELETE FROM t WHERE id BETWEEN 11 AND 30",
        "RELEASE inner_sp",
        "INSERT INTO t VALUES(1001,1001,7,'first'),(1002,1,8,'duplicate')",
        "ROLLBACK TO outer_sp",
        "UPDATE t SET k=88 WHERE id=512",
        "ROLLBACK TO outer_sp",
        "RELEASE outer_sp",
        "COMMIT",
    ] {
        let actual = f.execute(sql).await;
        let expected = r.execute_batch(sql);
        let should_succeed = !sql.contains("duplicate");
        assert_eq!(actual.is_ok(), should_succeed, "{sql}: {actual:?}");
        assert_eq!(expected.is_ok(), should_succeed, "{sql}: {expected:?}");
        if !should_succeed {
            assert!(
                matches!(
                    &actual,
                    Err(fsqlite_error::FrankenError::UniqueViolation { .. })
                ),
                "the rejected statement must reach UNIQUE enforcement: {actual:?}"
            );
            assert_eq!(
                expected
                    .as_ref()
                    .unwrap_err()
                    .sqlite_error()
                    .expect("stock UNIQUE error")
                    .extended_code,
                rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
            );
        }
        for query in [
            "SELECT id,u,k,v FROM t ORDER BY id",
            "SELECT id FROM t INDEXED BY idx_t_k WHERE k=7 ORDER BY id",
        ] {
            assert_eq!(
                frank_rows(&f, query).await.expect("FrankenSQLite rows"),
                sqlite_rows(&r, query).expect("stock rows"),
                "file_backed={file_backed} after {sql}: {query}"
            );
        }
        assert_eq!(
            frank_rows(&f, "PRAGMA integrity_check")
                .await
                .expect("live integrity query"),
            vec![vec!["'ok'".to_owned()]],
            "file_backed={file_backed} after {sql}"
        );
        assert_eq!(
            sqlite_rows(&r, "PRAGMA integrity_check").expect("stock integrity"),
            vec![vec!["'ok'".to_owned()]]
        );
        eprintln!("event=private_reservation_integrity file_backed={file_backed} step={sql}");
    }
    // A rollback can leave private reservations above the old database bound.
    // Grow past that range in a later transaction on the same live connection:
    // a lost reservation must become a real orphan, not stay hidden at EOF.
    for sql in [
        "BEGIN",
        "SAVEPOINT abort_sp",
        &overflow_insert,
        "ROLLBACK TO abort_sp",
        "RELEASE abort_sp",
        "ROLLBACK",
    ] {
        f.execute(sql).await.expect("whole-transaction rollback");
        r.execute_batch(sql)
            .expect("stock whole-transaction rollback");
    }
    f.execute("BEGIN")
        .await
        .expect("begin growth after rollback");
    r.execute_batch("BEGIN").expect("stock begin growth");
    for id in 513..=640 {
        let sql = format!("INSERT INTO t VALUES({id},{id},{},'{payload}')", id % 13);
        f.execute(&sql).await.expect("growth after rollback");
        r.execute_batch(&sql).expect("stock growth after rollback");
    }
    f.execute("COMMIT").await.expect("commit later growth");
    r.execute_batch("COMMIT")
        .expect("stock commit later growth");
    for query in [
        "SELECT id,u,k,v FROM t ORDER BY id",
        "SELECT id FROM t INDEXED BY idx_t_k WHERE k=7 ORDER BY id",
        "PRAGMA integrity_check",
    ] {
        assert_eq!(
            frank_rows(&f, query)
                .await
                .expect("rows after later growth"),
            sqlite_rows(&r, query).expect("stock rows after later growth"),
            "file_backed={file_backed} growth after rollback: {query}"
        );
    }
    eprintln!("event=private_reservation_integrity file_backed={file_backed} step=later_growth");
    f.close().await.expect("await FrankenSQLite close");
    if file_backed {
        let reopened = rusqlite::Connection::open(&path).expect("stock physical reopen");
        for query in [
            "SELECT id,u,k,v FROM t ORDER BY id",
            "SELECT id FROM t INDEXED BY idx_t_k WHERE k=7 ORDER BY id",
            "PRAGMA integrity_check",
        ] {
            assert_eq!(
                sqlite_rows(&reopened, query).expect("reopened stock rows"),
                sqlite_rows(&r, query).expect("reference stock rows"),
                "stock physical reopen: {query}"
            );
        }
    }
}

#[test]
fn indexed_overflow_live_integrity_memory_pywfi() {
    asupersync::test_utils::run_test(|| indexed_overflow_live_integrity(false, None));
}

#[test]
fn indexed_overflow_live_integrity_file_pywfi() {
    asupersync::test_utils::run_test(|| indexed_overflow_live_integrity(true, Some("WAL")));
}

#[test]
fn indexed_overflow_live_integrity_file_rollback_journal_qkk9h() {
    asupersync::test_utils::run_test(|| indexed_overflow_live_integrity(true, Some("DELETE")));
}
