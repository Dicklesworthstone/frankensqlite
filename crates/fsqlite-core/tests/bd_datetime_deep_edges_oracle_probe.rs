#![recursion_limit = "512"]

//! Deep date/time edge leaf-hunt (pane af49, 2026-08-20): frank vs rusqlite over
//! the trickier corners of the date/time function family — chained modifiers,
//! start-of-* anchors, weekday N, month/year day-overflow, fractional seconds,
//! numeric (Julian-day / unixepoch) inputs, and the fuller strftime specifier
//! set (%j/%W/%U/%G/%V/%u/%p/%s/%J/%f). Strictly UTC-deterministic — NO
//! now/localtime/utc (TZ-dependent → would false-diverge). Pass = coverage
//! keeper; a mismatch is a leaf divergence.

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
fn datetime_deep_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // start-of-* anchors + chaining
            "SELECT datetime('2024-05-17 13:45:30','start of day')",
            "SELECT datetime('2024-05-17','start of month')",
            "SELECT datetime('2024-05-17','start of year')",
            "SELECT datetime('2024-05-17','start of month','+1 month','-1 day')",
            "SELECT date('2024-02-15','start of month','-1 day')",
            // weekday N (0=Sun..6=Sat) — next occurrence incl same day
            "SELECT date('2024-03-13','weekday 0')",
            "SELECT date('2024-03-13','weekday 3')",
            "SELECT date('2024-03-13','weekday 6')",
            "SELECT date('2024-03-13','weekday 2')",
            // month/year day-overflow normalization
            "SELECT date('2024-01-31','+1 month')",
            "SELECT date('2024-03-31','-1 month')",
            "SELECT date('2020-02-29','+1 year')",
            "SELECT date('2020-02-29','+4 years')",
            "SELECT date('2024-01-31','+1 month','+1 month')",
            // large / negative offsets
            "SELECT date('2024-06-15','+400 days')",
            "SELECT date('2024-06-15','-1000 days')",
            "SELECT datetime('2024-06-15 00:00:00','+90000 seconds')",
            "SELECT datetime('2024-06-15 12:00:00','-25 hours')",
            // fractional seconds
            "SELECT strftime('%Y-%m-%d %H:%M:%f','2024-06-15 12:00:00.125')",
            "SELECT strftime('%f','2024-06-15 12:00:59.999')",
            "SELECT datetime('2024-06-15 12:00:00','+0.5 seconds')",
            // numeric inputs: Julian day number and unixepoch
            "SELECT datetime(2460000.5)",
            "SELECT datetime(2460000)",
            "SELECT datetime(1718452800,'unixepoch')",
            "SELECT date(1718452800,'unixepoch')",
            "SELECT julianday('2024-06-15 12:00:00')",
            "SELECT strftime('%J','2024-06-15 12:00:00')",
            "SELECT unixepoch('2024-06-15 12:00:00')",
            // fuller strftime specifier set
            "SELECT strftime('%j','2024-03-01')",
            "SELECT strftime('%W %U','2024-01-01')",
            "SELECT strftime('%G-W%V-%u','2024-01-01')",
            "SELECT strftime('%G-W%V-%u','2026-12-31')",
            "SELECT strftime('%p %I','2024-06-15 13:05:00')",
            "SELECT strftime('%p %I','2024-06-15 00:30:00')",
            "SELECT strftime('%s','2024-06-15 12:00:00')",
            "SELECT strftime('%Y%m%d','2024-06-15')",
            // leap-year / boundary dates
            "SELECT date('2024-12-31','+1 day')",
            "SELECT date('2023-02-28','+1 day')",
            "SELECT date('2024-02-28','+1 day')",
            "SELECT strftime('%j','2024-12-31')",
            "SELECT strftime('%j','2023-12-31')",
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
            "{} deep date/time divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
