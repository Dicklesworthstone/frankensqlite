//! Differential oracle: date/time functions vs rusqlite (bundled SQLite 3.53).
//! A probe sweep found this surface stock-correct across 22 cases; this keeper
//! locks it in. Fixed timestamps only — no now()/wall-clock — so the comparison
//! is deterministic.
//!
//! Notable edges asserted: `+1 month` on Jan 31 overflow-normalizes to Mar 02
//! (2024 is a leap year), `+1 year` on a leap day rolls to Mar 01, `start of
//! month/year/day`, `weekday N` advances to the next given weekday (same day if
//! already there), and strftime specifiers (%j day-of-year, %w weekday, %W
//! week, %s unix epoch).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn date_time_datetime_basic() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT date('2024-01-15')", "date()").await;
        agree("SELECT time('2024-01-15 12:34:56')", "time()").await;
        agree("SELECT datetime('2024-01-15 12:34:56')", "datetime()").await;
    });
}

#[test]
fn month_and_year_overflow() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT date('2024-01-31', '+1 month')", "+1 month on Jan 31 overflows to Mar 02").await;
        agree("SELECT date('2024-01-15', '+1 month')", "+1 month normal").await;
        agree("SELECT date('2024-02-29', '+1 year')", "+1 year on leap day rolls to Mar 01").await;
    });
}

#[test]
fn day_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT date('2024-02-27', '+3 days')", "+3 days across month boundary").await;
        agree("SELECT date('2024-03-01', '-1 day')", "-1 day back across month boundary").await;
    });
}

#[test]
fn start_of_modifiers() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT date('2024-06-17', 'start of month')", "start of month").await;
        agree("SELECT date('2024-06-17', 'start of year')", "start of year").await;
        agree("SELECT datetime('2024-06-17 09:30:00', 'start of day')", "start of day").await;
    });
}

#[test]
fn weekday_modifier() {
    asupersync::test_utils::run_test(|| async {
        // 2024-06-17 is a Monday.
        agree("SELECT date('2024-06-17', 'weekday 0')", "weekday 0 advances to next Sunday").await;
        agree("SELECT date('2024-06-17', 'weekday 1')", "weekday 1 on Monday stays same day").await;
    });
}

#[test]
fn strftime_specifiers() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT strftime('%Y-%m-%d %H:%M:%S', '2024-01-15 12:34:56')", "strftime full").await;
        agree("SELECT strftime('%j', '2024-03-01')", "%j day-of-year (leap)").await;
        agree("SELECT strftime('%w', '2024-06-17')", "%w weekday").await;
        agree("SELECT strftime('%W', '2024-06-17')", "%W week-of-year").await;
        agree("SELECT strftime('%s', '2024-01-01 00:00:00')", "%s unix epoch").await;
    });
}

#[test]
fn time_arithmetic_and_julianday() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT time('12:34:56', '+1 hour')", "time +1 hour").await;
        agree("SELECT datetime('2024-01-15 23:30:00', '+90 minutes')", "+90 minutes across midnight").await;
        agree("SELECT CAST(julianday('2024-03-01') - julianday('2024-02-01') AS INTEGER)", "julianday difference in days").await;
        agree("SELECT date('2024-01-15', 'start of month', '+1 month', '-1 day')", "chained modifiers = last day of month").await;
    });
}
