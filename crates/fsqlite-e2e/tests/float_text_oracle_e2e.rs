//! bd-7r4w8 — Oracle-parity e2e: float-to-text formatting vs rusqlite.
//!
//! This compares the ENGINE's float->text rendering (via CAST(... AS TEXT) and
//! `float || ''`), not the test harness's, so it exercises FrankenSQLite's
//! float formatter against SQLite's. SQLite 3.52+ defaults REAL-to-TEXT
//! conversion to 17 significant digits, with SQLite-specific rounding and
//! scientific-notation thresholds. The riskier cases are repeating decimals,
//! integer-valued floats (`3.0`), and large/small magnitudes where exponent
//! formatting (`1.0e+20` vs `1e20`) commonly differs. Those are isolated.

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

fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
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
fn float_text_simple_decimals() {
    assert_scalar(
        &[
            "SELECT CAST(0.5 AS TEXT)",   // '0.5'
            "SELECT CAST(2.5 AS TEXT)",   // '2.5'
            "SELECT CAST(3.14 AS TEXT)",  // '3.14'
            "SELECT CAST(-0.25 AS TEXT)", // '-0.25'
            "SELECT 1.5 || ''",           // '1.5'
        ],
        "float_text_simple_decimals",
    );
}

#[test]
fn float_text_repeating_and_precision() {
    assert_scalar(
        &[
            "SELECT CAST(1.0/3.0 AS TEXT)",   // 0.33333333333333332
            "SELECT CAST(2.0/3.0 AS TEXT)",   // 0.66666666666666663
            "SELECT CAST(0.1 + 0.2 AS TEXT)", // 0.30000000000000004
            "SELECT (10.0/3.0) || ''",
        ],
        "float_text_repeating_and_precision",
    );
}

#[test]
fn float_text_integer_valued() {
    assert_scalar(
        &[
            "SELECT CAST(3.0 AS TEXT)",   // '3.0'
            "SELECT CAST(100.0 AS TEXT)", // '100.0'
            "SELECT CAST(-7.0 AS TEXT)",  // '-7.0'
            "SELECT 42.0 || ''",          // '42.0'
        ],
        "float_text_integer_valued",
    );
}

/// Large/small magnitudes: scientific-notation thresholds and exponent format
/// are the most likely to diverge between frank's and SQLite's formatters.
#[test]
fn float_text_magnitude_scientific() {
    assert_scalar(
        &[
            "SELECT CAST(1e20 AS TEXT)",
            "SELECT CAST(1e-10 AS TEXT)",
            "SELECT CAST(1.5e15 AS TEXT)",
            "SELECT CAST(1.5e16 AS TEXT)",
            "SELECT CAST(1e308 AS TEXT)",
            "SELECT CAST(1e-300 AS TEXT)",
            "SELECT CAST(123456789012345.6 AS TEXT)",
        ],
        "float_text_magnitude_scientific",
    );
}
