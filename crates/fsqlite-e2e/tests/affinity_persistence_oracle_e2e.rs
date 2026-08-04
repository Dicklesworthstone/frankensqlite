//! Oracle-parity e2e: type-affinity coercion must persist across a file-backed
//! close/reopen cycle (through the pager / b-tree / WAL), matching rusqlite.
//!
//! The in-memory affinity oracle tests (fsqlite-core
//! conformance_oracle_type_affinity) verify coercion at insert time; these
//! verify the *stored* storage class round-trips correctly after the row image
//! is serialized to disk and read back on a fresh connection. In particular it
//! pins the NUMERIC REAL->INTEGER reduction fix (fix(types) d1cf117d) end-to-end
//! on a file-backed database.
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

/// Render a FrankenSQLite result set as `Vec<Vec<String>>` for comparison.
async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

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

/// Render a rusqlite result set as `Vec<Vec<String>>` for comparison.
fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
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
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

/// Apply DDL/DML to a file-backed FrankenSQLite db (then drop the connection to
/// force a close), and to a fresh rusqlite in-memory db. Returns the db path.
async fn setup(dir: &tempfile::TempDir, stmts: &[&str]) -> (String, rusqlite::Connection) {
    let db_path = dir.path().join("affinity_persist.db");
    let db_str = db_path.to_string_lossy().into_owned();
    {
        let fconn = Connection::open(&db_str).await.expect("open frank");
        for s in stmts {
            fconn
                .execute(s)
                .await
                .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        }
        // Drop closes the connection, flushing the row image to disk.
    }
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        rconn
            .execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (db_str, rconn)
}

async fn assert_parity(db_str: &str, rconn: &rusqlite::Connection, queries: &[&str], label: &str) {
    // Reopen the file-backed db on a fresh connection so the schema + rows are
    // re-hydrated from disk rather than served from the writer's in-memory state.
    let fconn = Connection::open(db_str).await.expect("reopen frank");
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&fconn, q).await, sqlite_rows(rconn, q)) {
            (Ok(f), Ok(s)) if f == s => {}
            (Ok(f), Ok(s)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"))
            }
            (Err(fe), Ok(s)) => {
                mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                ));
            }
            (Ok(f), Err(se)) => {
                mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                ));
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
fn affinity_storage_class_persists_across_reopen() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (db, rconn) = setup(
            &dir,
            &[
                "CREATE TABLE t (i INTEGER, t TEXT, r REAL, n NUMERIC, b BLOB)",
                // '123'->int, 456->text, '78.5'->real, '90'->int, 'hi'->text(blob aff).
                "INSERT INTO t VALUES ('123', 456, '78.5', '90', 'hi')",
            ],
        )
        .await;
        assert_parity(
            &db,
            &rconn,
            &["SELECT typeof(i), i, typeof(t), t, typeof(r), r, typeof(n), n, typeof(b), b FROM t"],
            "affinity_storage_class_persists_across_reopen",
        )
        .await;
    });
}

#[test]
fn numeric_real_reduction_persists_across_reopen() {
    asupersync::test_utils::run_test(|| async {
        // Pins fix(types) d1cf117d on a file-backed db: an integral REAL stored in a
        // NUMERIC column is reduced to INTEGER and survives the disk round-trip.
        let dir = test_tmpdir();
        let (db, rconn) = setup(
            &dir,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n NUMERIC)",
                "INSERT INTO t(n) VALUES (4.0), (5.5), ('3.0e2'), ('123'), ('abc')",
            ],
        )
        .await;
        assert_parity(
            &db,
            &rconn,
            &["SELECT id, typeof(n), n FROM t ORDER BY id"],
            "numeric_real_reduction_persists_across_reopen",
        )
        .await;
    });
}

#[test]
fn comparison_affinity_persists_across_reopen() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (db, rconn) = setup(
            &dir,
            &[
                "CREATE TABLE t (i INTEGER, x TEXT)",
                "INSERT INTO t VALUES (2,'2'),(5,'5'),(10,'10'),(100,'100')",
            ],
        )
        .await;
        assert_parity(
            &db,
            &rconn,
            &[
                // RHS literal acquires the column's affinity, after reopen.
                "SELECT i FROM t WHERE i = '5'",
                "SELECT x FROM t WHERE x = 5",
                "SELECT i FROM t WHERE i > '3' ORDER BY i",
                "SELECT x FROM t WHERE x < '7' ORDER BY x",
                "SELECT i FROM t ORDER BY i",
                "SELECT x FROM t ORDER BY x",
            ],
            "comparison_affinity_persists_across_reopen",
        )
        .await;
    });
}

#[test]
fn integer_real_lossless_persists_across_reopen() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (db, rconn) = setup(
            &dir,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t(v) VALUES (1.0), (1.5), ('1e3'), ('2.0')",
            ],
        )
        .await;
        assert_parity(
            &db,
            &rconn,
            &["SELECT id, typeof(v), v FROM t ORDER BY id"],
            "integer_real_lossless_persists_across_reopen",
        )
        .await;
    });
}
