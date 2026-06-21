//! bd-a4ooq — Oracle-parity e2e: ORDER BY / GROUP BY ordinal range vs rusqlite.
//!
//! A bare integer literal in ORDER BY / GROUP BY is a 1-based reference to an
//! output column, NOT a constant expression. SQLite validates the ordinal: it
//! must be between 1 and the number of result columns, otherwise it raises
//! "...ORDER BY term out of range - should be between 1 and N" (and likewise for
//! GROUP BY). This pins that in-range ordinals sort/group correctly and that an
//! out-of-range, zero, or negative ordinal is rejected (rather than silently
//! treated as a constant). Compared against rusqlite.

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

fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in [
        "CREATE TABLE t (a INTEGER, b INTEGER)",
        "INSERT INTO t VALUES (3,1),(1,2),(2,3)",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
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
fn ordinal_in_range_sorts_and_groups() {
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT a, b FROM t ORDER BY 1",         // by a: (1,2),(2,3),(3,1)
            "SELECT a, b FROM t ORDER BY 2",         // by b: (3,1),(1,2),(2,3)
            "SELECT a, b FROM t ORDER BY 2, 1",      // by b then a
            "SELECT b FROM t GROUP BY 1 ORDER BY 1", // group by output col 1 (b): 1,2,3
        ],
        "ordinal_in_range_sorts_and_groups",
    );
}

#[test]
fn ordinal_out_of_range_rejected() {
    // A bare integer ordinal must be in [1, ncol]; otherwise SQLite errors.
    // The test confirms frank rejects too (not treating the literal as a constant).
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT a, b FROM t ORDER BY 3",  // > ncol
            "SELECT a, b FROM t ORDER BY 0",  // ordinals are 1-based
            "SELECT a, b FROM t ORDER BY -1", // negative
            "SELECT a FROM t GROUP BY 2",     // > ncol (1 output column)
        ],
        "ordinal_out_of_range_rejected",
    );
}
