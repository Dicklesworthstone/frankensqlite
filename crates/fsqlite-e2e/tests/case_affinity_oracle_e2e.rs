//! bd-3uzkp — Oracle-parity e2e: simple-CASE comparison affinity vs rusqlite.
//!
//! The simple form `CASE x WHEN v THEN ...` compares `x = v`, and that
//! comparison applies affinity exactly like a plain `=`: when `x` is a column
//! with INTEGER/REAL/NUMERIC affinity and `v` is a text literal, `v` is coerced
//! to numeric (and vice-versa for a TEXT column). This targets the affinity
//! behaviour of CASE-WHEN specifically — the IN value-list path was found to
//! skip this coercion (bd-56aj2) while scalar `=` applies it (bd-525y0), so
//! CASE-WHEN could go either way. Compared against rusqlite.

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

fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, s TEXT)",
        "INSERT INTO t VALUES (1,5,'5'),(2,10,'x')",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

/// bd-w4r25: simple-CASE `CASE n WHEN '5'` (INTEGER col vs text literal) does not
/// coerce the WHEN value, so it never matches; SQLite applies INTEGER affinity.
#[test]
fn case_integer_column_when_text_numeric() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            // n has INTEGER affinity: '5' coerces to 5 -> id1 hits.
            "SELECT id, CASE n WHEN '5' THEN 'hit' ELSE 'miss' END FROM t ORDER BY id",
        ],
        "case_integer_column_when_text_numeric",
    );
}

/// bd-w4r25: simple-CASE `CASE s WHEN 5` (TEXT col vs numeric literal) does not
/// coerce, so it never matches; SQLite applies TEXT affinity.
#[test]
fn case_text_column_when_numeric() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            // s has TEXT affinity: 5 coerces to '5' -> id1 (s='5') hits.
            "SELECT id, CASE s WHEN 5 THEN 'hit' ELSE 'miss' END FROM t ORDER BY id",
        ],
        "case_text_column_when_numeric",
    );
}

#[test]
fn case_bare_literals_no_affinity() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            // Bare operands -> no affinity: 5 vs '5' are unequal -> 'miss'.
            "SELECT CASE 5 WHEN '5' THEN 'hit' ELSE 'miss' END",
            // Both text -> equal.
            "SELECT CASE '5' WHEN '5' THEN 'hit' ELSE 'miss' END",
            // Numeric int vs real -> equal (5 = 5.0).
            "SELECT CASE 5 WHEN 5.0 THEN 'hit' ELSE 'miss' END",
        ],
        "case_bare_literals_no_affinity",
    );
}

#[test]
fn case_numeric_int_vs_real_in_where() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            // n = 5 matched against 5.0 (numeric equality, no affinity) -> hit.
            "SELECT id FROM t WHERE CASE n WHEN 5.0 THEN 1 ELSE 0 END = 1 ORDER BY id", // 1
        ],
        "case_numeric_int_vs_real_in_where",
    );
}

/// bd-w4r25: multi-branch simple-CASE with text-numeric WHEN values on an
/// INTEGER column — every branch fails to coerce, so all rows fall to ELSE.
#[test]
fn case_multibranch_affinity() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &["SELECT id, CASE n WHEN '5' THEN 'a' WHEN '10' THEN 'b' ELSE 'c' END FROM t ORDER BY id"],
        "case_multibranch_affinity",
    );
}
