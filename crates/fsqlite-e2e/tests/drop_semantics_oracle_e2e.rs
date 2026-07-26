//! bd-t5iv0 — Oracle-parity e2e: object DROP semantics vs rusqlite.
//!
//! Covers dropping tables/views/indexes/triggers, the IF EXISTS guard (and the
//! error when dropping a missing object without it), the rule that dropping a
//! table also removes its indexes and triggers, that a view referencing a
//! dropped table survives in the schema but becomes unqueryable, and that
//! sqlite_master is cleaned up. Scenarios assert per-statement agreement with
//! rusqlite, then compare query results. DML/DDL is autocommit.
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
fn drop_table_cleans_schema() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO t VALUES (1,'a')",
                "DROP TABLE t",
            ],
            &[
                // sqlite_master must no longer list the table.
                "SELECT count(*) FROM sqlite_master WHERE name='t'",
                "SELECT count(*) FROM sqlite_master",
            ],
            "drop_table_cleans_schema",
        )
        .await;
    });
}

#[test]
fn drop_if_exists_vs_missing() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                "DROP TABLE IF EXISTS no_such_table", // no error
                "DROP TABLE IF EXISTS t",             // drops it
                "DROP TABLE IF EXISTS t",             // already gone, no error
                "DROP VIEW IF EXISTS no_such_view",
                "DROP INDEX IF EXISTS no_such_index",
                "DROP TRIGGER IF EXISTS no_such_trigger",
                "DROP TABLE no_such_table", // no IF EXISTS -> error on both
            ],
            &["SELECT count(*) FROM sqlite_master WHERE name='t'"],
            "drop_if_exists_vs_missing",
        )
        .await;
    });
}

#[test]
fn drop_table_removes_indexes_and_triggers() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "CREATE INDEX idx_a ON t(a)",
                "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN SELECT 1; END",
                "CREATE TABLE other (x INTEGER)",
                "CREATE INDEX idx_other ON other(x)",
                "DROP TABLE t",
            ],
            &[
                // t's index and trigger are auto-removed; other's index survives.
                "SELECT type, name FROM sqlite_master WHERE name IN ('idx_a','t_ai','t') ORDER BY name",
                "SELECT count(*) FROM sqlite_master WHERE name='idx_other'",
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='t'",
            ],
            "drop_table_removes_indexes_and_triggers",
        )
        .await;
    });
}

#[test]
fn drop_view_index_trigger() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "CREATE VIEW v AS SELECT id FROM t",
                "CREATE INDEX idx_a ON t(a)",
                "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN SELECT 1; END",
                "DROP VIEW v",
                "DROP INDEX idx_a",
                "DROP TRIGGER t_ai",
            ],
            &[
                // Only the base table remains.
                "SELECT type, name FROM sqlite_master ORDER BY name",
                "SELECT id, a FROM t ORDER BY id",
            ],
            "drop_view_index_trigger",
        )
        .await;
    });
}

#[test]
fn drop_table_leaves_dangling_view() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,10)",
            "CREATE VIEW dv AS SELECT id, v FROM t",
            "DROP TABLE t", // SQLite allows this; the view becomes dangling but stays
        ] {
            f.execute(s)
                .await
                .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
            r.execute_batch(s)
                .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
        }
        // The view definition survives in the schema...
        let fr = frank_rows(
            &f,
            "SELECT count(*) FROM sqlite_master WHERE type='view' AND name='dv'",
        )
        .await
        .unwrap();
        let rr = sqlite_rows(
            &r,
            "SELECT count(*) FROM sqlite_master WHERE type='view' AND name='dv'",
        )
        .unwrap();
        assert_eq!(
            fr, rr,
            "dangling view should remain in schema: frank {fr:?} vs csql {rr:?}"
        );
        // ...but querying it now errors on both (its base table is gone).
        let fe = f.query("SELECT * FROM dv").await;
        let re = sqlite_rows(&r, "SELECT * FROM dv");
        assert!(
            fe.is_err() && re.is_err(),
            "querying a dangling view should error: frank ok={:?}, csql ok={:?}",
            fe.is_ok(),
            re.is_ok()
        );
    });
}

#[test]
fn drop_then_recreate() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                "DROP TABLE t",
                // Recreate with a different shape; old data must be gone.
                "CREATE TABLE t (id INTEGER PRIMARY KEY, b TEXT)",
                "INSERT INTO t VALUES (1,'x'),(2,'y')",
            ],
            &["SELECT id, b FROM t ORDER BY id", "SELECT count(*) FROM t"],
            "drop_then_recreate",
        )
        .await;
    });
}
