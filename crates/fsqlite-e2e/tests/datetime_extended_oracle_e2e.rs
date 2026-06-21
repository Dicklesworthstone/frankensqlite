//! bd-wk7d3 — Oracle-parity e2e: extended datetime specifiers/functions.
//!
//! datetime_oracle_e2e covers the common strftime specifiers; this adds the ones
//! it omits: `%J` (Julian day number), `%p` (AM/PM), `%I` (12-hour), `%e`
//! (space-padded day), the `time()` function (incl. a modifier), a julianday
//! round-trip, and the SQLite 3.46 convenience specifiers `%F`/`%R`/`%T` plus
//! the ISO-8601 week-date set `%G`/`%g`/`%u`/`%V`. Compared against rusqlite
//! (bundled ~3.46); the 3.46-era specifiers are isolated so a divergence (e.g. a
//! specifier not yet implemented) is clean. Inputs are fixed timestamps.

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
fn datetime_julian_ampm_12hour_day() {
    assert_scalar(
        &[
            "SELECT strftime('%J', '2000-01-01 12:00:00')", // 2451545.0
            "SELECT strftime('%J', '2000-01-01 00:00:00')", // 2451544.5
            "SELECT strftime('%p', '2023-06-15 13:45:30')", // PM
            "SELECT strftime('%p', '2023-06-15 09:00:00')", // AM
            "SELECT strftime('%I', '2023-06-15 13:45:30')", // 01
            "SELECT strftime('%I', '2023-06-15 00:30:00')", // 12
            "SELECT strftime('%e', '2023-06-05')",          // ' 5'
        ],
        "datetime_julian_ampm_12hour_day",
    );
}

#[test]
fn datetime_time_function() {
    assert_scalar(
        &[
            "SELECT time('2023-06-15 13:45:30')",            // '13:45:30'
            "SELECT time('2023-06-15 13:45:30', '+1 hour')", // '14:45:30'
            "SELECT time('2023-06-15 23:30:00', '+1 hour')", // '00:30:00'
        ],
        "datetime_time_function",
    );
}

#[test]
fn datetime_julianday_roundtrip() {
    assert_scalar(
        &[
            "SELECT datetime(julianday('2023-06-15 12:00:00'))", // '2023-06-15 12:00:00'
            "SELECT date(julianday('2023-06-15'))",              // '2023-06-15'
        ],
        "datetime_julianday_roundtrip",
    );
}

/// SQLite 3.46 convenience specifiers %R/%T (the %F sibling is bd-luvv8).
#[test]
fn datetime_iso_convenience_specifiers() {
    assert_scalar(
        &[
            "SELECT strftime('%R', '2023-06-15 13:45:30')", // '13:45'
            "SELECT strftime('%T', '2023-06-15 13:45:30')", // '13:45:30'
        ],
        "datetime_iso_convenience_specifiers",
    );
}

/// bd-luvv8: %F (== %Y-%m-%d) is not implemented; frank emits the literal '%F'.
/// Its siblings %R and %T (above) work.
#[test]
fn datetime_iso_date_specifier_f() {
    assert_scalar(
        &["SELECT strftime('%F', '2023-06-15 13:45:30')"], // expect '2023-06-15'
        "datetime_iso_date_specifier_F",
    );
}

/// SQLite 3.46 ISO-8601 week-date specifiers. Isolated for the same reason.
#[test]
fn datetime_iso_week_specifiers() {
    assert_scalar(
        &[
            "SELECT strftime('%G', '2023-01-01')", // ISO year (2022 — Jan 1 2023 is in ISO week 52 of 2022)
            "SELECT strftime('%V', '2023-01-01')", // ISO week number
            "SELECT strftime('%u', '2023-06-15')", // ISO weekday (1=Mon..7=Sun)
            "SELECT strftime('%g', '2023-01-01')", // 2-digit ISO year
        ],
        "datetime_iso_week_specifiers",
    );
}
