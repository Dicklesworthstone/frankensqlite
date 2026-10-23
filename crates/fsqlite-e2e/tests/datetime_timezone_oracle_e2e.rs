//! bd-reality-check-1022-llk6g.17 — Oracle-parity e2e: the date.c time-string
//! grammar, its `isUtc`/`isLocal`/`nFloor` state, and 'localtime'/'utc'.
//!
//! The grammar and state tests do not depend on the process timezone. The
//! 'localtime'/'utc' test re-runs this binary once per timezone with `TZ` set,
//! so fsqlite (chrono) and bundled SQLite (localtime_r) read the same zone;
//! each child first checks that its zone took effect. POSIX TZ strings need no
//! tz database; the named zones run where /usr/share/zoneinfo has them.
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
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
    eprintln!(
        "event=datetime_tz_oracle label={label} queries={} mismatches={}",
        queries.len(),
        mismatches.len()
    );
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// date.c parseYyyyMmDd / parseHhMmSs / parseTimezone: separators, hour 24,
/// `±HH:MM` offsets (hours 0..14) with surrounding spaces, `.999` truncation.
#[test]
fn time_string_grammar_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT datetime('2024-01-01 12:00+0100')",
                "SELECT datetime('2024-01-01 12:00+01')",
                "SELECT datetime('2024-01-01 12:00+15:00')",
                "SELECT datetime('2024-01-01 12:00+14:00')",
                "SELECT datetime('2024-01-01 12:00-14:59')",
                "SELECT datetime('2024-01-01 12:00 +01:00')",
                "SELECT datetime('2024-01-01 12:00   -01:30   ')",
                "SELECT datetime('2024-01-01 12:00 Z')",
                "SELECT datetime('2024-01-01 12:00Z ')",
                "SELECT datetime('2024-01-01 12:00z')",
                "SELECT datetime('2024-01-01 12:00 ')",
                "SELECT datetime('2024-01-01 12:00' || char(11) || '+01:00' || char(12))",
                "SELECT datetime('2024-01-01' || char(11))",
                "SELECT datetime('2024-01-01 24:00')",
                "SELECT datetime('2024-01-01 24:59:59')",
                "SELECT datetime('2024-01-01 25:00')",
                "SELECT datetime('24:00')",
                "SELECT time('24:00:00')",
                "SELECT date('2024-01-01 24:00')",
                "SELECT time('2024-01-01 24:00')",
                "SELECT julianday('2024-01-01 24:00')",
                "SELECT datetime('2024-01-31 24:00')",
                "SELECT datetime('2024-01-01 24:00', 'subsec')",
                "SELECT datetime('2024-01-01 24:00Z')",
                "SELECT datetime('2024-01-01 24:00+01:00')",
                "SELECT datetime('24:00', 'subsec')",
                "SELECT strftime('%Y-%m-%d %H:%M:%S %j', '2024-01-01 24:00')",
                "SELECT datetime('2024-01-01T12:00Z')",
                "SELECT datetime('12:00Z')",
                "SELECT datetime('12:00+01:00')",
                "SELECT datetime('12:00 -05:30')",
                "SELECT datetime('2024-01-01 12:00:00.5+01:00', 'subsec')",
                "SELECT datetime('2024-01-01 12:00:00.123456789Z', 'subsec')",
                "SELECT datetime('2024-01-01 12:00:00.9999')",
                "SELECT datetime('2024-01-01 12:00:00.9999', 'subsec')",
                "SELECT datetime('1970-01-01 23:59:59.9995', 'subsec')",
                "SELECT strftime('%H:%M:%f|%s', '1970-01-01 23:59:59.9995', 'subsec')",
                "SELECT datetime('2024-01-01 12:00:00.', 'subsec')",
                "SELECT datetime('2024-01-01 12:00:00.x')",
                "SELECT datetime('2024-01-01 12:00:00 +01:00')",
                "SELECT datetime('2024-01-01 12:00:00.5 Z', 'subsec')",
                "SELECT datetime('2024-01-01 12:00+01:00', '+1 hour')",
                "SELECT datetime('2024-01-01 12:00+01:0')",
                "SELECT datetime('2024-01-01 12:00+1:00')",
                "SELECT datetime('2024-01-01 12:00-')",
                "SELECT datetime('2024-01-01 12:00+')",
                "SELECT datetime('2024-01-01 12:00ZZ')",
                "SELECT datetime('2024-01-01 12:00 x')",
                "SELECT datetime('2024-01-01Z')",
                "SELECT datetime('2024-01-01  12:00')",
                "SELECT datetime('2024-01-01TT12:00')",
                "SELECT datetime('2024-01-01 T 12:00')",
                "SELECT datetime('2024-01-0112:00')",
                "SELECT datetime('2024-01-01 ')",
                "SELECT datetime('2024-01-01T')",
                "SELECT datetime('+123-01-01')",
                "SELECT datetime('2024-+1-01')",
                "SELECT datetime('2024-01-+1')",
                "SELECT datetime('2024-1-01')",
                "SELECT datetime('2024-01-32')",
                "SELECT datetime('2024-02-30')",
                "SELECT datetime('2024-00-10')",
                "SELECT datetime('2024-01-00')",
                "SELECT datetime('0000-01-01')",
                "SELECT datetime('9999-12-31 23:59:59.999', 'subsec')",
                "SELECT datetime('9999-12-31 24:00')",
                "SELECT datetime('2024-01-01 1:00')",
                "SELECT datetime('2024-01-01 01:0')",
                "SELECT datetime('2024-01-01 01:00:5')",
                "SELECT datetime('2024-01-01 01:60')",
                "SELECT datetime('2024-01-01 01:00:60')",
                "SELECT julianday('2024-01-01 12:00+01:00')",
            ],
            "time_string_grammar_matches_stock",
        )
        .await;
    });
}

/// date.c nFloor (seeded by the date as written, kept by named modifiers,
/// cleared by `NNN units` and a converting 'utc', left pending by 'floor')
/// and isDate's day-of-month normalization when there is no modifier.
#[test]
fn floor_state_and_fields_as_written_match_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT date('2024-01-31', '+1 month', 'floor', 'floor')",
                "SELECT date('2024-01-31', '+1 month', 'start of day', 'floor')",
                "SELECT date('2024-01-31', '+1 month', 'weekday 1', 'floor')",
                "SELECT date('2024-01-31', '+1 month', '+1 day', 'floor')",
                "SELECT date('2024-01-31', '+1 month', '+01:00', 'floor')",
                "SELECT date('2024-02-30', 'floor')",
                "SELECT date('2023-02-30', 'floor')",
                "SELECT date('2023-02-30', 'ceiling')",
                "SELECT date('2023-02-30 12:00+01:00', 'floor')",
                "SELECT datetime('2023-02-30 12:00+01:00')",
                "SELECT date('2023-02-30 12:00+01:00', 'subsec')",
                "SELECT datetime('2024-02-30', 'subsec')",
                "SELECT date('2024-02-30', 'subsec')",
                "SELECT date('2024-02-30')",
                "SELECT strftime('%d', '2024-02-30')",
                "SELECT strftime('%d %j', '2024-02-30', 'subsec')",
            ],
            "floor_state_and_fields_as_written_match_stock",
        )
        .await;
    });
}

/// date.c isUtc: 'now', 'subsec' and text with `Z`/`±HH:MM` are UTC already,
/// so 'utc' leaves them (and their fields as written) alone.
#[test]
fn utc_on_values_already_in_utc_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT datetime('2024-01-01 12:00+00:00', 'utc')",
                "SELECT datetime('2024-01-01 12:00-00:00', 'utc')",
                "SELECT datetime('2024-01-01 12:00Z', 'utc')",
                "SELECT datetime('2024-01-01 12:00+02:00', 'utc')",
                "SELECT datetime('2024-01-01 12:00+02:00', 'utc', 'UTC')",
                "SELECT datetime('2024-01-01 24:00Z', 'utc', 'UTC')",
                "SELECT datetime('now', 'utc') = datetime('now')",
                "SELECT datetime('subsec', 'utc') = datetime('subsec')",
                "SELECT datetime() = datetime('now', 'utc')",
                "SELECT datetime(1718452800, 'unixepoch', 'utc', 'utc') = datetime(1718452800, 'unixepoch', 'utc')",
                "SELECT datetime('12:00', 'utc', 'utc') = datetime('12:00', 'utc')",
            ],
            "utc_on_values_already_in_utc_matches_stock",
        )
        .await;
    });
}

/// 'localtime'/'utc' in one zone: DST gaps and folds, the 1970..2037 window
/// stock maps other years into, the isLocal/isUtc no-ops, and how 'utc'
/// clears a pending floor and an earlier 'subsec'.
const ZONE_QUERIES: &[&str] = &[
    "SELECT datetime('2024-06-15 12:00', 'localtime')",
    "SELECT datetime('2024-01-15 12:00', 'localtime')",
    "SELECT datetime('2024-06-15 12:00', 'utc')",
    "SELECT datetime('2024-01-15 12:00', 'utc')",
    "SELECT datetime('2024-03-10 02:30', 'utc')",
    "SELECT datetime('2024-11-03 01:30', 'utc')",
    "SELECT datetime('2024-10-06 02:15', 'utc')",
    "SELECT datetime('2024-04-07 01:45', 'utc')",
    "SELECT datetime('2024-03-31 01:30', 'utc')",
    "SELECT datetime('2024-10-27 01:30', 'utc')",
    "SELECT datetime('2024-03-10 06:59:59', 'localtime')",
    "SELECT datetime('2024-03-10 07:00', 'localtime')",
    "SELECT datetime('2024-11-03 05:30', 'localtime')",
    "SELECT datetime('2024-11-03 06:30', 'localtime')",
    "SELECT datetime('1800-01-01 02:00', 'localtime')",
    "SELECT datetime('1800-03-10 12:00', 'localtime')",
    "SELECT datetime('1945-02-01 12:00', 'localtime')",
    "SELECT datetime('1969-12-31 23:00', 'localtime')",
    "SELECT datetime('1970-01-01 00:00', 'localtime')",
    "SELECT datetime('2038-01-17 12:00', 'localtime')",
    "SELECT datetime('2038-01-19 12:00', 'localtime')",
    "SELECT datetime('2100-03-01 12:00', 'localtime')",
    "SELECT datetime('2400-07-01 12:00', 'localtime')",
    "SELECT datetime('-0100-07-01 12:00', 'localtime')",
    "SELECT datetime('0000-01-01 00:00', 'localtime')",
    "SELECT datetime('9999-12-31 23:00', 'localtime')",
    "SELECT datetime('0000-01-01 01:00', 'utc')",
    "SELECT datetime('1800-07-01 12:00', 'utc')",
    "SELECT datetime('2050-07-01 12:00', 'utc')",
    "SELECT strftime('%s', '1800-07-01 12:00', 'localtime')",
    "SELECT datetime('2024-06-15 12:00', 'localtime', 'localtime')",
    "SELECT datetime('2024-06-15 12:00', 'utc', 'utc')",
    "SELECT datetime('2024-06-15 12:00', 'localtime', 'utc')",
    "SELECT datetime('2024-06-15 12:00', 'utc', 'localtime')",
    "SELECT datetime('2024-06-15 12:00', 'localtime', 'utc', 'localtime')",
    "SELECT datetime('2024-06-15 12:00Z', 'localtime')",
    "SELECT datetime('2024-06-15 12:00+02:00', 'localtime', 'utc')",
    "SELECT datetime('2024-06-15 12:00', 'localtime', '+1 day', 'localtime')",
    "SELECT datetime('2024-06-15 12:00', 'localtime', 'start of day', 'utc')",
    "SELECT datetime('2024-06-15 12:00', 'localtime', 'weekday 0')",
    "SELECT datetime('2024-06-15 12:00', '+5 hours', 'localtime')",
    "SELECT datetime(1718452800, 'unixepoch', 'localtime')",
    "SELECT datetime(1718452800, 'unixepoch', 'utc')",
    "SELECT datetime(2460477.0, 'localtime')",
    "SELECT datetime('now', 'localtime', 'utc') = datetime('now')",
    "SELECT strftime('%Y-%m-%d %H:%M:%f', '2024-06-15 12:00:00.123', 'localtime')",
    "SELECT datetime('2024-06-15 12:00:00.999', 'localtime', 'subsec')",
    "SELECT datetime('2024-06-15 12:00:00.5', 'subsec', 'utc')",
    "SELECT datetime('2024-06-15 12:00:00.5', 'utc', 'subsec')",
    "SELECT unixepoch('2024-06-15 12:00', 'localtime')",
    "SELECT unixepoch('2024-06-15 12:00', 'utc')",
    "SELECT julianday('2024-06-15 12:00', 'localtime')",
    "SELECT time('12:00', 'localtime')",
    "SELECT date('2024-06-15', 'localtime', 'start of month')",
    "SELECT date('2024-01-31', '+1 month', 'localtime', 'floor')",
    "SELECT date('2024-01-31', '+1 month', 'utc', 'floor')",
];

/// The zones, with what stock prints for 2024-01-15 and 2024-07-15 12:00 UTC
/// under 'localtime' (to prove the child's TZ took effect).
const ZONES: &[(&str, &str, &str)] = &[
    ("UTC0", "2024-01-15 12:00:00", "2024-07-15 12:00:00"),
    (
        "EST5EDT,M3.2.0,M11.1.0",
        "2024-01-15 07:00:00",
        "2024-07-15 08:00:00",
    ),
    ("IST-5:30", "2024-01-15 17:30:00", "2024-07-15 17:30:00"),
    ("<-03>3", "2024-01-15 09:00:00", "2024-07-15 09:00:00"),
    (
        "<+1030>-10:30<+11>-11,M10.1.0,M4.1.0",
        "2024-01-15 23:00:00",
        "2024-07-15 22:30:00",
    ),
    (
        "GMT0BST,M3.5.0/1,M10.5.0",
        "2024-01-15 12:00:00",
        "2024-07-15 13:00:00",
    ),
];

const NAMED_ZONES: &[(&str, &str, &str)] = &[
    (
        "America/New_York",
        "2024-01-15 07:00:00",
        "2024-07-15 08:00:00",
    ),
    ("Asia/Kolkata", "2024-01-15 17:30:00", "2024-07-15 17:30:00"),
    (
        "Australia/Lord_Howe",
        "2024-01-15 23:00:00",
        "2024-07-15 22:30:00",
    ),
    (
        "Europe/London",
        "2024-01-15 12:00:00",
        "2024-07-15 13:00:00",
    ),
    (
        "Pacific/Chatham",
        "2024-01-16 01:45:00",
        "2024-07-16 00:45:00",
    ),
];

const CHILD_ENV: &str = "FSQLITE_DATETIME_TZ_CHILD";

/// Runs only as a child of `localtime_and_utc_match_stock_across_timezones`,
/// which sets `TZ` and the two expected 'localtime' renderings.
#[test]
fn localtime_utc_zone_child() {
    let Ok(expected) = std::env::var(CHILD_ENV) else {
        return;
    };
    let tz = std::env::var("TZ").unwrap_or_default();
    let (january, july) = expected.split_once('|').expect("child expectation");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for (input, want) in [("2024-01-15 12:00", january), ("2024-07-15 12:00", july)] {
        let got: String = r
            .query_row(
                &format!("SELECT datetime('{input}', 'localtime')"),
                [],
                |row| row.get(0),
            )
            .expect("stock localtime");
        assert_eq!(got, want, "TZ={tz} did not take effect in stock SQLite");
    }
    asupersync::test_utils::run_test(|| async {
        assert_scalar(ZONE_QUERIES, &format!("TZ={tz}")).await;
    });
    println!("event=datetime_tz_child_done tz={tz}");
}

#[test]
fn localtime_and_utc_match_stock_across_timezones() {
    let exe = std::env::current_exe().expect("test binary path");
    let zoneinfo = std::path::Path::new("/usr/share/zoneinfo");
    let zones = ZONES.iter().chain(
        NAMED_ZONES
            .iter()
            .filter(|(name, _, _)| zoneinfo.join(name).is_file()),
    );
    let mut failures = Vec::new();
    let mut ran = 0;
    for (tz, january, july) in zones {
        let output = std::process::Command::new(&exe)
            .args([
                "--exact",
                "localtime_utc_zone_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, format!("{january}|{july}"))
            .env("TZ", tz)
            .output()
            .expect("spawn child test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let finished = stdout.contains(&format!("event=datetime_tz_child_done tz={tz}"));
        if !output.status.success() || !finished {
            failures.push(format!(
                "TZ={tz} (status {:?}):\n{stdout}\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        ran += 1;
    }
    eprintln!(
        "event=datetime_tz_oracle_zones ran={ran} failed={}",
        failures.len()
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
