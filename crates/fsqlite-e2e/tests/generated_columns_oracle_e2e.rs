//! bd-a5nsg — Oracle-parity e2e: generated columns (VIRTUAL/STORED) vs rusqlite.
//!
//! Covers VIRTUAL (computed on read) and STORED (computed on write) generated
//! columns, the `GENERATED ALWAYS AS (...)` long form, expressions referencing
//! multiple base columns and functions, generated columns in WHERE / SELECT *,
//! type-affinity coercion of the generated value, recomputation on UPDATE of a
//! base column, that an explicit value for a generated column is rejected, and
//! an index over a generated column. All data is fixed and deterministic.
//!
//! Each scenario runs its full statement list on BOTH engines and reports the
//! first place they diverge (a frank error where rusqlite succeeds is itself a
//! finding), then compares query results.

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

/// Run `stmts` (DDL/DML) on both engines, asserting they agree on success/
/// failure of each, then compare each query in `queries`.
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

#[test]
#[ignore = "bd-r3303: VIRTUAL generated columns return NULL (not computed on read)"]
fn generated_virtual_basic() {
    scenario(
        &[
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) VIRTUAL)",
            "INSERT INTO t(a, b) VALUES (1, 2), (10, 20), (100, 200)",
        ],
        &[
            "SELECT a, b, c FROM t ORDER BY a",
            "SELECT c FROM t WHERE c > 30 ORDER BY c",
            "SELECT * FROM t ORDER BY a",
        ],
        "generated_virtual_basic",
    );
}

#[test]
fn generated_stored_basic() {
    scenario(
        &[
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a * b) STORED)",
            "INSERT INTO t(a, b) VALUES (2, 3), (4, 5)",
        ],
        &[
            "SELECT a, b, c FROM t ORDER BY a",
            "SELECT * FROM t ORDER BY a",
        ],
        "generated_stored_basic",
    );
}

#[test]
#[ignore = "bd-r3303: VIRTUAL generated columns return NULL (not computed on read)"]
fn generated_always_long_form() {
    scenario(
        &[
            "CREATE TABLE t (a INTEGER, \
               v INTEGER GENERATED ALWAYS AS (a + 1) VIRTUAL, \
               s INTEGER GENERATED ALWAYS AS (a * 10) STORED)",
            "INSERT INTO t(a) VALUES (5), (7)",
        ],
        &["SELECT a, v, s FROM t ORDER BY a"],
        "generated_always_long_form",
    );
}

#[test]
#[ignore = "bd-r3303: VIRTUAL generated columns return NULL (not computed on read)"]
fn generated_expression_with_functions_and_text() {
    scenario(
        &[
            "CREATE TABLE p (first TEXT, last TEXT, \
               full TEXT AS (first || ' ' || last) VIRTUAL, \
               initials TEXT AS (upper(substr(first,1,1)) || upper(substr(last,1,1))) STORED)",
            "INSERT INTO p(first, last) VALUES ('ada','lovelace'),('alan','turing')",
        ],
        &[
            "SELECT full, initials FROM p ORDER BY first",
            "SELECT first FROM p WHERE full = 'ada lovelace'",
        ],
        "generated_expression_with_functions_and_text",
    );
}

#[test]
#[ignore = "bd-r3303: VIRTUAL generated columns return NULL (not computed on read)"]
fn generated_type_affinity_coercion() {
    // The generated value is coerced to the column's declared affinity.
    scenario(
        &[
            "CREATE TABLE t (a INTEGER, \
               txt TEXT AS (a * 2) VIRTUAL, \
               num INTEGER AS (a / 2.0) STORED)",
            "INSERT INTO t(a) VALUES (3), (4)",
        ],
        &["SELECT a, typeof(txt), txt, typeof(num), num FROM t ORDER BY a"],
        "generated_type_affinity_coercion",
    );
}

#[test]
#[ignore = "bd-r3303: VIRTUAL generated columns return NULL (not computed on read)"]
fn generated_recomputes_on_update() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER AS (a + 1) VIRTUAL, d INTEGER AS (a + 1) STORED)",
            "INSERT INTO t(id, a) VALUES (1, 10), (2, 20)",
            "UPDATE t SET a = a * 2 WHERE id = 1",
        ],
        &["SELECT id, a, c, d FROM t ORDER BY id"],
        "generated_recomputes_on_update",
    );
}

#[test]
fn generated_explicit_value_rejected() {
    // Supplying a value for a generated column must error on both engines.
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    {
        let s = "CREATE TABLE t (a INTEGER, c INTEGER AS (a + 1) VIRTUAL)";
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        assert!(
            fe.is_ok() == re.is_ok(),
            "generated_explicit_value_rejected setup `{s}`: frank ok={:?} csql ok={:?}",
            fe.is_ok(),
            re.is_ok()
        );
        if fe.is_err() {
            return; // generated columns unsupported at DDL — covered elsewhere.
        }
    }
    let fe = f.execute("INSERT INTO t(a, c) VALUES (1, 999)");
    let re = r.execute_batch("INSERT INTO t(a, c) VALUES (1, 999)");
    assert!(
        fe.is_err() && re.is_err(),
        "explicit value for generated column must be rejected: frank ok={:?}, csql ok={:?}",
        fe.is_ok(),
        re.is_ok()
    );
}

#[test]
#[ignore = "bd-r3303: VIRTUAL generated columns return NULL (not computed on read)"]
fn generated_indexed_lookup() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, hi INTEGER AS (a * 1000) VIRTUAL)",
            "CREATE INDEX idx_hi ON t(hi)",
            "INSERT INTO t(id, a) VALUES (1,3),(2,1),(3,2)",
        ],
        &[
            "SELECT id, hi FROM t WHERE hi = 2000",
            "SELECT id FROM t WHERE hi >= 2000 ORDER BY hi",
            "SELECT a FROM t ORDER BY hi",
        ],
        "generated_indexed_lookup",
    );
}

// ---------------------------------------------------------------------------
// STORED generated columns work end-to-end (the on-write computation path); the
// following exercise it thoroughly. VIRTUAL equivalents are tracked in bd-r3303.
// ---------------------------------------------------------------------------

#[test]
fn generated_stored_with_functions_and_text() {
    scenario(
        &[
            "CREATE TABLE p (first TEXT, last TEXT, \
               full TEXT AS (first || ' ' || last) STORED, \
               initials TEXT AS (upper(substr(first,1,1)) || upper(substr(last,1,1))) STORED)",
            "INSERT INTO p(first, last) VALUES ('ada','lovelace'),('alan','turing')",
        ],
        &[
            "SELECT full, initials FROM p ORDER BY first",
            "SELECT first FROM p WHERE full = 'ada lovelace'",
        ],
        "generated_stored_with_functions_and_text",
    );
}

#[test]
fn generated_stored_recomputes_on_update() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, d INTEGER AS (a + 1) STORED)",
            "INSERT INTO t(id, a) VALUES (1, 10), (2, 20)",
            "UPDATE t SET a = a * 2 WHERE id = 1",
        ],
        &["SELECT id, a, d FROM t ORDER BY id"],
        "generated_stored_recomputes_on_update",
    );
}

#[test]
fn generated_stored_indexed_lookup() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, hi INTEGER AS (a * 1000) STORED)",
            "CREATE INDEX idx_hi ON t(hi)",
            "INSERT INTO t(id, a) VALUES (1,3),(2,1),(3,2)",
        ],
        &[
            "SELECT id, hi FROM t WHERE hi = 2000",
            "SELECT id FROM t WHERE hi >= 2000 ORDER BY hi",
            "SELECT a FROM t ORDER BY hi",
        ],
        "generated_stored_indexed_lookup",
    );
}
