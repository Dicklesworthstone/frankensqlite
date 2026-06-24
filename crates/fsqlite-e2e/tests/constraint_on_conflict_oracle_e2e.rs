//! bd-pcdvq — Oracle-parity e2e: constraint-level ON CONFLICT clauses.
//!
//! A constraint can carry its own conflict-resolution algorithm
//! (`UNIQUE ON CONFLICT REPLACE`, `PRIMARY KEY ON CONFLICT IGNORE`,
//! `NOT NULL ON CONFLICT IGNORE`, ...). A plain INSERT that violates such a
//! constraint uses that algorithm without any `INSERT OR` prefix; and when the
//! statement DOES carry an `OR` prefix, the statement-level algorithm overrides
//! the constraint's. unique_conflict_oracle covers one case (UNIQUE ON CONFLICT
//! IGNORE); this fills the matrix. Each scenario asserts per-statement agreement
//! with rusqlite, then compares the resulting rows.

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

fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
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

/// bd-587fx: a plain INSERT violating UNIQUE ON CONFLICT REPLACE should replace
/// the conflicting row; frank errors instead (only UNIQUE+IGNORE is honored).
#[test]
fn unique_on_conflict_replace() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE ON CONFLICT REPLACE, label TEXT)",
            "INSERT INTO t VALUES (1,10,'a'),(2,20,'b')",
            "INSERT INTO t VALUES (3,10,'c')", // u=10 conflicts -> REPLACE removes id1
        ],
        &["SELECT id, u, label FROM t ORDER BY id"], // (2,20,'b'),(3,10,'c')
        "unique_on_conflict_replace",
    );
}

/// bd-587fx: PRIMARY KEY ON CONFLICT IGNORE should skip a duplicate-PK insert;
/// frank errors instead.
#[test]
fn primary_key_on_conflict_ignore() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT IGNORE, v TEXT)",
            "INSERT INTO t VALUES (1,'a'),(2,'b')",
            "INSERT INTO t VALUES (1,'dup')", // PK conflict -> ignored
            "INSERT INTO t VALUES (3,'c')",
        ],
        &["SELECT id, v FROM t ORDER BY id"], // (1,'a'),(2,'b'),(3,'c')
        "primary_key_on_conflict_ignore",
    );
}

/// bd-587fx: NOT NULL ON CONFLICT IGNORE should skip a row with a NULL in that
/// column; frank errors instead.
#[test]
fn not_null_on_conflict_ignore() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL ON CONFLICT IGNORE)",
            "INSERT INTO t VALUES (1,10)",
            "INSERT INTO t VALUES (2,NULL)", // NOT NULL violated -> row ignored
            "INSERT INTO t VALUES (3,30)",
        ],
        &["SELECT id, v FROM t ORDER BY id"], // (1,10),(3,30)
        "not_null_on_conflict_ignore",
    );
}

#[test]
fn statement_or_overrides_constraint_on_conflict() {
    scenario(
        &[
            // Constraint says IGNORE, but INSERT OR REPLACE overrides it.
            "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE ON CONFLICT IGNORE, label TEXT)",
            "INSERT INTO t VALUES (1,10,'a')",
            "INSERT OR REPLACE INTO t VALUES (2,10,'b')", // statement OR REPLACE wins -> replaces id1
        ],
        &["SELECT id, u, label FROM t ORDER BY id"], // (2,10,'b')
        "statement_or_overrides_constraint_on_conflict",
    );
}

#[test]
fn unique_on_conflict_fail_errors() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE ON CONFLICT FAIL)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "INSERT INTO t VALUES (3,10)", // conflict -> FAIL -> error on both
        ],
        &["SELECT id, u FROM t ORDER BY id"], // (1,10),(2,20)
        "unique_on_conflict_fail_errors",
    );
}
