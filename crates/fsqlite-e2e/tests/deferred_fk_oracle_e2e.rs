//! bd-528wq — Oracle-parity e2e: deferred foreign keys vs rusqlite.
//!
//! A `REFERENCES ... DEFERRABLE INITIALLY DEFERRED` constraint is not checked
//! at statement time but at COMMIT, so a transaction may temporarily violate it
//! (e.g. insert a child before its parent) as long as the constraint holds by
//! the time the transaction commits. An ordinary (immediate) FK fails at the
//! statement. These verify both, INSERT-only, against rusqlite. Each scenario is
//! its own test so any failure is isolated.

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

/// bd-do0d6: frank checks the deferred FK at the INSERT (errors) instead of at
/// COMMIT, so this valid children-first transaction fails.
#[test]
fn deferred_fk_temporary_violation_resolved_by_commit() {
    scenario(
        &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER \
             REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
            "BEGIN",
            "INSERT INTO child VALUES (10, 1)", // parent 1 not present yet -> OK (deferred)
            "INSERT INTO parent VALUES (1)",    // now satisfied
            "COMMIT",                           // constraint holds -> commits
        ],
        &[
            "SELECT id, pid FROM child ORDER BY id", // (10,1)
            "SELECT id FROM parent ORDER BY id",     // (1)
        ],
        "deferred_fk_temporary_violation_resolved_by_commit",
    );
}

/// bd-do0d6: the violation should surface at COMMIT, but frank raises it at the
/// INSERT (deferral unimplemented).
#[test]
fn deferred_fk_unresolved_violation_fails_at_commit() {
    scenario(
        &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER \
             REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
            "INSERT INTO parent VALUES (1)",
            "BEGIN",
            "INSERT INTO child VALUES (10, 99)", // parent 99 never added -> OK so far (deferred)
            "COMMIT",                            // violation still present -> COMMIT fails on both
        ],
        &[
            // The failed COMMIT rolls back the child insert.
            "SELECT count(*) FROM child",        // 0
            "SELECT id FROM parent ORDER BY id", // (1)
        ],
        "deferred_fk_unresolved_violation_fails_at_commit",
    );
}

#[test]
fn immediate_fk_fails_at_statement() {
    scenario(
        &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
            "INSERT INTO parent VALUES (1)",
            // Not deferred: this fails immediately on both engines.
            "INSERT INTO child VALUES (10, 99)",
        ],
        &["SELECT count(*) FROM child"], // 0
        "immediate_fk_fails_at_statement",
    );
}

/// bd-do0d6: building a self-referential tree children-first within a txn relies
/// on deferral; frank checks immediately and fails on the first insert.
#[test]
fn deferred_fk_self_reference_within_txn() {
    scenario(
        &[
            "PRAGMA foreign_keys = ON",
            // Self-referential tree with a deferred FK: insert children before
            // parents in one transaction.
            "CREATE TABLE node (id INTEGER PRIMARY KEY, parent INTEGER \
             REFERENCES node(id) DEFERRABLE INITIALLY DEFERRED)",
            "BEGIN",
            "INSERT INTO node VALUES (3, 2)", // 2 not present yet
            "INSERT INTO node VALUES (2, 1)", // 1 not present yet
            "INSERT INTO node VALUES (1, NULL)",
            "COMMIT",
        ],
        &["SELECT id, parent FROM node ORDER BY id"], // (1,NULL),(2,1),(3,2)
        "deferred_fk_self_reference_within_txn",
    );
}
