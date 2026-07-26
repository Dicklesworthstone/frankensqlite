//! bd-8k77i — Oracle-parity e2e: ALTER TABLE ADD/DROP COLUMN vs rusqlite.
//!
//! rename_propagation_oracle covers ALTER ... RENAME; this covers the column
//! add/drop operations: ADD COLUMN backfilling existing rows with NULL, ADD
//! COLUMN with a DEFAULT (existing rows take it), ADD COLUMN NOT NULL DEFAULT,
//! DROP COLUMN (SQLite 3.35+) removing the column and its data, an add-then-drop
//! round-trip, and the error when dropping a PRIMARY KEY column. Each scenario
//! asserts per-statement agreement with rusqlite, then compares the resulting
//! table shape/state (incl. `SELECT *` to catch a wrong column count).
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
fn add_column_backfills_null() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "ALTER TABLE t ADD COLUMN b TEXT",
                "INSERT INTO t (id,a,b) VALUES (3,30,'new')",
            ],
            &[
                "SELECT id, a, b FROM t ORDER BY id", // existing rows b=NULL; new row 'new'
                "SELECT * FROM t ORDER BY id",        // 3 columns now
            ],
            "add_column_backfills_null",
        )
        .await;
    });
}

#[test]
fn add_column_with_default() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 99",
                "INSERT INTO t (id,a) VALUES (3,30)",
            ],
            &["SELECT id, a, c FROM t ORDER BY id"], // all c=99
            "add_column_with_default",
        )
        .await;
    });
}

#[test]
fn add_column_not_null_default() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                "ALTER TABLE t ADD COLUMN d TEXT NOT NULL DEFAULT 'x'",
                "INSERT INTO t (id,a) VALUES (2,20)",
            ],
            &["SELECT id, a, d FROM t ORDER BY id"], // both d='x'
            "add_column_not_null_default",
        )
        .await;
    });
}

/// bd-w50nr: dropping a NON-LAST column updates the schema but does not rewrite
/// stored rows, so columns after the dropped position read stale slots
/// (here `c` returns b's old values). Dropping the last column works
/// (add_then_drop_column_roundtrip), so this is specific to mid-table drops.
#[test]
fn drop_column_removes_data() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
                "INSERT INTO t VALUES (1,10,'x',100),(2,20,'y',200)",
                "ALTER TABLE t DROP COLUMN b",
            ],
            &[
                "SELECT id, a, c FROM t ORDER BY id", // (1,10,100),(2,20,200)
                "SELECT * FROM t ORDER BY id",        // 3 columns
            ],
            "drop_column_removes_data",
        )
        .await;
    });
}

/// bd-w50nr (second manifestation): dropping a CREATE-time column (even the
/// last one) leaves stored rows at their original width; a later read trips
/// frank's payload-width check with "database disk image is malformed". DROP
/// COLUMN only works for columns added via ALTER ADD COLUMN (lazy slots), as in
/// add_then_drop_column_roundtrip.
#[test]
fn drop_create_time_last_column_corrupts() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
                "INSERT INTO t VALUES (1,10,'x'),(2,20,'y')",
                "ALTER TABLE t DROP COLUMN b",
                "INSERT INTO t (id,a) VALUES (3,30)",
            ],
            &[
                "SELECT id, a FROM t ORDER BY id", // (1,10),(2,20),(3,30)
                "SELECT * FROM t ORDER BY id",     // 2 columns
            ],
            "drop_create_time_last_column_corrupts",
        )
        .await;
    });
}

#[test]
fn add_then_drop_column_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                "ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'd'",
                "ALTER TABLE t DROP COLUMN b",
                "INSERT INTO t (id,a) VALUES (2,20)",
            ],
            &["SELECT * FROM t ORDER BY id"], // back to (id,a): (1,10),(2,20)
            "add_then_drop_column_roundtrip",
        )
        .await;
    });
}

#[test]
fn drop_primary_key_column_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                "ALTER TABLE t DROP COLUMN id", // error on both (cannot drop PK column)
            ],
            &["SELECT id, a FROM t ORDER BY id"], // unchanged
            "drop_primary_key_column_errors",
        )
        .await;
    });
}
