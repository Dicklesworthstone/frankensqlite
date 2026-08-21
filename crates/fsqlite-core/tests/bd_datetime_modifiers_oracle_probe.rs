#![recursion_limit = "512"]

//! Exhaustive date/time MODIFIER leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over every documented modifier — N units (incl. fractional),
//! start-of-*, weekday N, the 3.46+ ceiling/floor month-overflow controls,
//! subsec, numeric (Julian-day / unixepoch / auto) inputs, and modifier chains.
//! Strictly UTC-deterministic (no now/localtime/utc). Error-tolerant so an
//! unsupported modifier surfaces as a divergence. Pass = coverage keeper.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fval(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) if rows.len() == 1 => tag_f(&rows[0].values()[0]),
        Ok(rows) => format!("ROWS:{}", rows.len()),
        Err(_) => "ERR".to_owned(),
    }
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    }) {
        Ok(s) => s,
        Err(_) => "ERR".to_owned(),
    }
}

#[test]
fn datetime_modifiers_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // N-unit offsets incl fractional + plural/singular
            "SELECT datetime('2024-03-05 10:20:30','+3 days')",
            "SELECT datetime('2024-03-05 10:20:30','-90 minutes')",
            "SELECT datetime('2024-03-05 10:20:30','+1.5 hours')",
            "SELECT datetime('2024-03-05 10:20:30','+0.25 seconds')",
            "SELECT datetime('2024-03-05 10:20:30','+36 hours')",
            "SELECT datetime('2024-03-05','+2 months')",
            "SELECT datetime('2024-03-05','+10 years')",
            "SELECT datetime('2024-03-05','-1 day','-1 hour')",
            // start-of-*
            "SELECT datetime('2024-03-05 10:20:30','start of day')",
            "SELECT datetime('2024-03-05 10:20:30','start of month')",
            "SELECT datetime('2024-03-05 10:20:30','start of year')",
            // weekday N (0=Sun..6=Sat)
            "SELECT date('2024-03-05','weekday 0')",
            "SELECT date('2024-03-05','weekday 1')",
            "SELECT date('2024-03-05','weekday 5')",
            "SELECT date('2024-03-05','weekday 2')",
            // classic last-day-of-month idiom
            "SELECT date('2024-02-10','start of month','+1 month','-1 day')",
            // month-overflow: default clamps; ceiling/floor control it (3.46+)
            "SELECT date('2024-01-31','+1 month')",
            "SELECT date('2024-01-31','+1 month','ceiling')",
            "SELECT date('2024-01-31','+1 month','floor')",
            "SELECT date('2024-03-31','-1 month','ceiling')",
            "SELECT date('2024-03-31','-1 month','floor')",
            // subsec
            "SELECT strftime('%f','2024-03-05 10:20:30.4','subsec')",
            "SELECT strftime('%f','2024-03-05 10:20:30','subsecond')",
            // numeric inputs
            "SELECT datetime(2460000.5)",
            "SELECT datetime(2460000)",
            "SELECT datetime(1709635230,'unixepoch')",
            "SELECT datetime(1709635230,'auto')",
            "SELECT datetime(2460374.5,'auto')",
            "SELECT date(0,'unixepoch')",
            "SELECT time(1709635230,'unixepoch','+30 minutes')",
            // combined chains
            "SELECT datetime('2024-06-15 12:00:00','start of year','+6 months','+14 days')",
            "SELECT datetime('2024-12-31 23:59:59','+1 second')",
            "SELECT julianday('2024-03-05','start of month')",
            "SELECT unixepoch('2024-03-05 00:00:00','+1 day')",
            // leap-second-ish / boundary
            "SELECT datetime('2024-02-28 23:00:00','+2 hours')",
            "SELECT date('2023-02-28','+1 day')",
            "SELECT date('2024-02-28','+1 day')",
        ];

        let mut diffs = Vec::new();
        for e in exprs {
            let fv = fval(&f, e).await;
            let rv = rval(&r, e);
            if fv != rv {
                diffs.push(format!("  `{e}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} date/time modifier divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
