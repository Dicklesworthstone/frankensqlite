//! bd-inoka — Oracle-parity e2e: LIMIT/OFFSET value coercion vs rusqlite.
//!
//! orderby_limit_oracle covers negative LIMIT, bounds, the comma form, and ORDER
//! BY ordinals. This pins how the LIMIT/OFFSET *values* are evaluated: they are
//! arbitrary expressions, but the result must be LOSSLESSLY convertible to an
//! integer. Integer-valued text (`'3'`), an arithmetic expression, and a scalar
//! subquery are accepted; a real with a fractional part (`2.9`), non-numeric text
//! (`'abc'`), and NULL are NOT and make SQLite raise "datatype mismatch". frank is
//! currently more lenient on the non-integer cases (bd-1zc9p). Fixed 5-row table;
//! compared against rusqlite.

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
        "CREATE TABLE t (id INTEGER PRIMARY KEY)",
        "INSERT INTO t VALUES (1),(2),(3),(4),(5)",
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
fn limit_offset_lossless_integer_values() {
    // Values that convert losslessly to an integer are accepted by both engines:
    // integer-valued text, an arithmetic expression, and a scalar subquery.
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id FROM t ORDER BY id LIMIT '3'", // 1,2,3 (text -> 3)
            "SELECT id FROM t ORDER BY id LIMIT 1 + 1", // 1,2 (expression)
            "SELECT id FROM t ORDER BY id LIMIT (SELECT 2)", // 1,2 (scalar subquery)
            "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET '1'", // 2,3 (text offset)
            "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET (SELECT 2)", // 3,4,5
        ],
        "limit_offset_lossless_integer_values",
    );
}

#[test]
fn limit_offset_noninteger_rejected() {
    // SQLite requires a LOSSLESS integer conversion; these all raise
    // "datatype mismatch". frank coerces them (truncate / parse-to-0 / NULL->0).
    let (f, r) = setup();
    check(
        &f,
        &r,
        &[
            "SELECT id FROM t ORDER BY id LIMIT 2.9", // real with fraction
            "SELECT id FROM t ORDER BY id LIMIT 'abc'", // non-numeric text
            "SELECT id FROM t ORDER BY id LIMIT NULL", // NULL
            "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1.5", // real offset
        ],
        "limit_offset_noninteger_rejected",
    );
}
