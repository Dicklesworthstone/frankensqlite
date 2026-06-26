//! bd-btntk — Oracle-parity e2e: ROWID / WITHOUT ROWID semantics vs rusqlite.
//!
//! Covers the rowid aliases (`rowid`/`_rowid_`/`oid`), INTEGER PRIMARY KEY as
//! the rowid alias, implicit rowid assignment, rowid reuse vs AUTOINCREMENT
//! monotonicity after a delete, explicit-rowid INSERT, last_insert_rowid(), and
//! WITHOUT ROWID tables (no rowid column; ordered by PK; composite PK). All DML
//! is autocommit and data is fixed/deterministic.

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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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

fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s).unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q), sqlite_rows(r, q)) {
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
fn rowid_aliases_and_integer_pk() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO t VALUES (5,'a'),(10,'b'),(20,'c')",
    ]);
    check(
        &f,
        &r,
        &[
            // rowid / _rowid_ / oid all alias the INTEGER PRIMARY KEY.
            "SELECT rowid, _rowid_, oid, id FROM t ORDER BY id",
            "SELECT name FROM t WHERE rowid = 10",
            "SELECT id FROM t WHERE oid > 5 ORDER BY id",
            "SELECT typeof(rowid) FROM t LIMIT 1",
        ],
        "rowid_aliases_and_integer_pk",
    );
}

#[test]
fn implicit_rowid_assignment() {
    let (f, r) = setup(&[
        "CREATE TABLE t (name TEXT)", // no INTEGER PK -> implicit rowid
        "INSERT INTO t VALUES ('a'),('b'),('c')",
    ]);
    check(
        &f,
        &r,
        &[
            "SELECT rowid, name FROM t ORDER BY rowid", // 1,2,3
            "SELECT name FROM t WHERE rowid = 2",
            "SELECT max(rowid) FROM t",
        ],
        "implicit_rowid_assignment",
    );
}

#[test]
fn last_insert_rowid_fn() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (42,'x')",
    ]);
    check(
        &f,
        &r,
        &["SELECT last_insert_rowid()"], // 42
        "last_insert_rowid_after_explicit",
    );
    let (f2, r2) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t(v) VALUES ('a'),('b'),('c')",
    ]);
    check(
        &f2,
        &r2,
        &["SELECT last_insert_rowid()"], // 3
        "last_insert_rowid_after_implicit",
    );
}

#[test]
fn rowid_reuse_without_autoincrement() {
    // Without AUTOINCREMENT, the rowid of a deleted max row can be reused.
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t(v) VALUES ('a'),('b'),('c')", // rowids 1,2,3
        "DELETE FROM t WHERE id = 3",
        "INSERT INTO t(v) VALUES ('d')", // reuses rowid 3 (max remaining 2 + 1)
    ]);
    check(
        &f,
        &r,
        &["SELECT id, v FROM t ORDER BY id"],
        "rowid_reuse_without_autoincrement",
    );
}

#[test]
fn autoincrement_is_monotonic() {
    // With AUTOINCREMENT, a deleted max rowid is NOT reused.
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        "INSERT INTO t(v) VALUES ('a'),('b'),('c')", // 1,2,3
        "DELETE FROM t WHERE id = 3",
        "INSERT INTO t(v) VALUES ('d')", // gets 4, not 3
    ]);
    check(
        &f,
        &r,
        &[
            "SELECT id, v FROM t ORDER BY id",
            "SELECT seq FROM sqlite_sequence WHERE name = 't'",
        ],
        "autoincrement_is_monotonic",
    );
}

#[test]
fn explicit_rowid_insert() {
    let (f, r) = setup(&[
        "CREATE TABLE t (a TEXT)",
        "INSERT INTO t(rowid, a) VALUES (100, 'x')",
        "INSERT INTO t(a) VALUES ('y')", // gets rowid 101
        "INSERT INTO t(rowid, a) VALUES (50, 'z')",
    ]);
    check(
        &f,
        &r,
        &["SELECT rowid, a FROM t ORDER BY rowid"],
        "explicit_rowid_insert",
    );
}

#[test]
fn without_rowid_table() {
    let (f, r) = setup(&[
        "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
        "INSERT INTO wr VALUES ('banana',1),('apple',2),('cherry',3)",
    ]);
    check(
        &f,
        &r,
        &[
            // WITHOUT ROWID rows are stored/ordered by the PK.
            "SELECT k, v FROM wr ORDER BY k",
            "SELECT v FROM wr WHERE k = 'apple'",
            "SELECT count(*) FROM wr",
            // Updating a non-PK column (autocommit).
        ],
        "without_rowid_table",
    );
    // A WITHOUT ROWID table has no rowid column: referencing it errors on both.
    let fe = f.query("SELECT rowid FROM wr");
    let re = sqlite_rows(&r, "SELECT rowid FROM wr");
    assert!(
        fe.is_err() && re.is_err(),
        "WITHOUT ROWID rowid reference: frank ok={:?}, csql ok={:?} (both must error)",
        fe.is_ok(),
        re.is_ok()
    );
}

#[test]
fn without_rowid_composite_pk() {
    let (f, r) = setup(&[
        "CREATE TABLE wr (a INTEGER, b INTEGER, label TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID",
        "INSERT INTO wr VALUES (2,1,'x'),(1,2,'y'),(1,1,'z'),(2,2,'w')",
    ]);
    check(
        &f,
        &r,
        &[
            // Ordered by the composite PK (a, then b).
            "SELECT a, b, label FROM wr ORDER BY a, b",
            "SELECT label FROM wr WHERE a = 1 AND b = 2",
            "SELECT count(*) FROM wr WHERE a = 2",
        ],
        "without_rowid_composite_pk",
    );
}
