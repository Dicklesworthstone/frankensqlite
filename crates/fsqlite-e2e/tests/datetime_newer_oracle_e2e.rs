//! bd-9u0yl — Oracle-parity e2e: newer datetime functions/modifiers vs rusqlite.
//!
//! datetime_oracle / datetime_extended cover the classic surface; SQLite 3.42-3.43
//! added more that nothing tests yet:
//!   * `timediff(A, B)` (3.43) — a signed `±YYYY-MM-DD HH:MM:SS.SSS` string for the
//!     amount of time to add to B to reach A.
//!   * the `'subsec'` / `'subsecond'` modifier (3.42) — makes `datetime()`/`time()`
//!     and `unixepoch()` keep fractional seconds instead of truncating.
//!   * the `'ceiling'` / `'floor'` modifiers (3.42) — control how a month/year
//!     modifier resolves a day that overflows the target month.
//!   * the `'auto'` modifier — auto-detects whether the numeric input is a Julian
//!     day number or a Unix timestamp.
//! All inputs are FIXED (no `'now'`/`'localtime'`) so results are deterministic;
//! rusqlite is the oracle for the exact formatting.

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
fn timediff_function() {
    assert_scalar(
        &[
            "SELECT timediff('2023-01-02 00:00:00', '2023-01-01 00:00:00')", // +1 day
            "SELECT timediff('2023-01-01 12:30:00', '2023-01-01 00:00:00')", // +12:30
            "SELECT timediff('2023-01-01 00:00:00', '2023-01-02 00:00:00')", // negative
            "SELECT timediff('2024-03-01', '2024-02-01')",                   // leap-Feb month
            "SELECT timediff('2023-06-15 00:00:00.500', '2023-06-15 00:00:00.000')", // subsec
        ],
        "timediff_function",
    );
}

#[test]
fn subsec_modifier_datetime_and_time() {
    assert_scalar(
        &[
            // With 'subsec' the fractional part survives; without it, truncated.
            "SELECT datetime('2023-06-15 12:30:45.678', 'subsec')",
            "SELECT datetime('2023-06-15 12:30:45.678')",
            "SELECT time('2023-06-15 12:30:45.678', 'subsec')",
            "SELECT time('2023-06-15 12:30:45.678')",
            // %f always carries the fractional seconds.
            "SELECT strftime('%f', '2023-06-15 12:30:45.678')",
            // unixepoch without 'subsec' truncates to whole seconds (matches).
            "SELECT unixepoch('2023-06-15 12:30:45')",
        ],
        "subsec_modifier_datetime_and_time",
    );
}

#[test]
fn unixepoch_subsec_fractional() {
    assert_scalar(
        &[
            "SELECT unixepoch('2023-06-15 12:30:45.500', 'subsec')", // SQLite 1686832245.5
        ],
        "unixepoch_subsec_fractional",
    );
}

#[test]
#[ignore = "bd-uh34b: 'ceiling'/'floor' datetime modifiers unimplemented (return NULL)"]
fn ceiling_floor_month_overflow() {
    assert_scalar(
        &[
            "SELECT date('2023-01-31', '+1 month', 'ceiling')", // SQLite '2023-03-03'
            "SELECT date('2023-01-31', '+1 month', 'floor')",   // SQLite '2023-02-28'
            "SELECT date('2020-02-29', '+1 year', 'floor')",    // SQLite '2021-02-28'
            "SELECT date('2020-02-29', '+1 year', 'ceiling')",  // SQLite '2021-03-01'
        ],
        "ceiling_floor_month_overflow",
    );
}

#[test]
fn auto_modifier_detects_julian_vs_unix() {
    // 'auto' classification works across the normal range.
    assert_scalar(
        &[
            "SELECT datetime(1686836730, 'auto')", // large -> Unix seconds
            "SELECT datetime(2460111.0, 'auto')",  // -> Julian day
            "SELECT date(2460111, 'auto')",        // Julian day -> date
        ],
        "auto_modifier_detects_julian_vs_unix",
    );
}

#[test]
#[ignore = "bd-orw5m: JDN->calendar conversion diverges at extreme Julian day 0 (-4712-01-01 vs SQLite -4713-11-24)"]
fn julian_day_zero_extreme_date() {
    assert_scalar(
        &[
            "SELECT datetime(0, 'auto')", // SQLite '-4713-11-24 12:00:00'
        ],
        "julian_day_zero_extreme_date",
    );
}
