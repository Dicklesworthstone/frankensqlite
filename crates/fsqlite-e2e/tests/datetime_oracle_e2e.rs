//! bd-95if0 — Oracle-parity e2e: date/time functions vs rusqlite (real SQLite).
//!
//! SQLite's datetime functions (date/time/datetime/julianday/unixepoch/strftime)
//! are a notorious clean-room divergence source: ISO-8601 parsing, day/month
//! overflow normalization, the modifier grammar (+N days, start of month,
//! weekday N, unixepoch, etc.), and strftime format specifiers all have exact
//! semantics. All inputs here are FIXED (no 'now'/'localtime') so the result is
//! deterministic and directly comparable to rusqlite.
#![recursion_limit = "512"]

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

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
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

/// Compare a batch of (independent, table-less) scalar queries against rusqlite.
async fn assert_parity(queries: &[&str], label: &str) {
    let fconn = Connection::open(":memory:").await.expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&fconn, q).await, sqlite_rows(&rconn, q)) {
            (Ok(f), Ok(s)) if f == s => {}
            (Ok(f), Ok(s)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"));
            }
            (Err(fe), Ok(s)) => {
                mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                ));
            }
            (Ok(f), Err(se)) => {
                mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                ));
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
fn datetime_basic_extraction() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "SELECT date('2023-06-15 13:45:30')",
                "SELECT time('2023-06-15 13:45:30')",
                "SELECT datetime('2023-06-15 13:45:30')",
                "SELECT date('2023-06-15T13:45:30')",
                "SELECT datetime('2023-06-15')",
                "SELECT time('13:45:30')",
                "SELECT datetime('2023-06-15 13:45:30.500')",
            ],
            "datetime_basic_extraction",
        )
        .await;
    });
}

#[test]
fn datetime_day_month_overflow_normalization() {
    asupersync::test_utils::run_test(|| async {
        // Day/month overflow normalizes via Julian-day arithmetic.
        assert_parity(
            &[
                "SELECT date('2023-02-29')", // -> 2023-03-01
                "SELECT date('2023-13-01')", // month overflow
                "SELECT date('2024-02-29')", // valid leap day
                "SELECT date('2023-01-32')", // day overflow
                "SELECT date('2023-04-31')", // April has 30 days
            ],
            "datetime_day_month_overflow_normalization",
        )
        .await;
    });
}

#[test]
fn datetime_modifiers() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "SELECT date('2023-06-15', '+1 day')",
                "SELECT date('2023-06-15', '-1 month')",
                "SELECT date('2023-06-15', '+1 year', '+2 months')",
                "SELECT date('2023-06-15', 'start of month')",
                "SELECT date('2023-06-15', 'start of year')",
                "SELECT datetime('2023-06-15 13:45:30', 'start of day')",
                "SELECT date('2023-06-15', 'weekday 0')", // next Sunday
                "SELECT date('2023-06-15', '+45 days')",
                "SELECT datetime('2023-06-15 23:30:00', '+1 hour')",
                "SELECT date('2023-01-31', '+1 month')", // end-of-month rollover
            ],
            "datetime_modifiers",
        )
        .await;
    });
}

#[test]
fn datetime_julianday() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "SELECT julianday('2023-06-15')",
                "SELECT julianday('2023-06-15 12:00:00')",
                "SELECT julianday('2000-01-01')",
                "SELECT julianday('1970-01-01 00:00:00')",
                // Round-trip: julian day number -> calendar date.
                "SELECT date(2451545.0)",
                "SELECT datetime(2451545.0)",
            ],
            "datetime_julianday",
        )
        .await;
    });
}

#[test]
fn datetime_unixepoch() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "SELECT strftime('%s', '2023-06-15 13:45:30')",
                "SELECT datetime(1686836730, 'unixepoch')",
                "SELECT date(1686836730, 'unixepoch')",
                "SELECT datetime(0, 'unixepoch')",
                "SELECT unixepoch('2023-06-15 13:45:30')",
            ],
            "datetime_unixepoch",
        )
        .await;
    });
}

#[test]
fn datetime_strftime_specifiers() {
    asupersync::test_utils::run_test(|| async {
        assert_parity(
            &[
                "SELECT strftime('%Y-%m-%d', '2023-06-15 13:45:30')",
                "SELECT strftime('%H:%M:%S', '2023-06-15 13:45:30')",
                "SELECT strftime('%Y', '2023-06-15')",
                "SELECT strftime('%j', '2023-06-15')", // day of year
                "SELECT strftime('%w', '2023-06-15')", // day of week (0=Sun)
                "SELECT strftime('%W', '2023-06-15')", // week of year
                "SELECT strftime('%d/%m/%Y', '2023-06-15')",
                "SELECT strftime('%Y-%m-%dT%H:%M:%S', '2023-06-15 13:45:30')",
                "SELECT strftime('%f', '2023-06-15 13:45:30.250')",
                "SELECT strftime('%%', '2023-06-15')", // literal percent
            ],
            "datetime_strftime_specifiers",
        )
        .await;
    });
}

#[test]
fn datetime_in_table_and_ordering() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE ev (id INTEGER PRIMARY KEY, ts TEXT)",
            "INSERT INTO ev VALUES (1,'2023-06-15 09:00:00'),(2,'2023-01-02 23:59:59'),(3,'2023-06-15 08:00:00'),(4,'2022-12-31 00:00:00')",
        ] {
            fconn.execute(s).await.unwrap();
            rconn.execute_batch(s).unwrap();
        }
        let mut mismatches = Vec::new();
        for q in [
            "SELECT id, date(ts) FROM ev ORDER BY julianday(ts)",
            "SELECT id FROM ev WHERE ts >= '2023-01-01' ORDER BY ts",
            "SELECT strftime('%Y-%m', ts) AS ym, count(*) FROM ev GROUP BY ym ORDER BY ym",
            "SELECT id, datetime(ts, '+1 day') FROM ev ORDER BY id",
        ] {
            match (frank_rows(&fconn, q).await, sqlite_rows(&rconn, q)) {
                (Ok(f), Ok(s)) if f == s => {}
                (Ok(f), Ok(s)) => {
                    mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"))
                }
                (Err(fe), Ok(s)) => mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                )),
                (Ok(f), Err(se)) => mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                )),
                (Err(_), Err(_)) => {}
            }
        }
        assert!(
            mismatches.is_empty(),
            "datetime_in_table_and_ordering: {} mismatch(es)\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
