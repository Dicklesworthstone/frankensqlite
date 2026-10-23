#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.28: the geopoly SQL functions must give stock's
//! results. v0.4.12 wrote blobs with a made-up header (0x47 and a
//! little-endian count) that stock cannot read, so a stock database's
//! polygons did not decode; accepted unclosed rings and errored on bad input
//! where stock returns NULL; printed JSON with Rust's float format and SVG as
//! a path; returned an unsigned area and 0/1 from geopoly_overlap() and
//! geopoly_within() (stock: signed area, 0-4 and 0-2); and did not register
//! geopoly_bbox, geopoly_contains_point, geopoly_xform, geopoly_regular,
//! geopoly_ccw, geopoly_debug or geopoly_group_bbox at all.
//!
//! Bundled rusqlite is built without geopoly, so the oracle is a recorded run
//! of stock 3.53.2 (the sqlite3.c bundled with libsqlite3-sys 0.38.2) built
//! with `-DSQLITE_ENABLE_GEOPOLY`. `geopoly_stock_oracle.expected` holds each
//! statement as `> SQL` followed by the rows `sqlite3_exec()` printed
//! (`a|b`, NULL as `N`) or `ERROR: message`. The same outputs came from a
//! `-ffp-contract=off` build, so they do not depend on fused multiply-add.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::{SqliteValue, format_sqlite_float};

const STOCK: &str = include_str!("geopoly_stock_oracle.expected");

fn stock_cases() -> Vec<(&'static str, Vec<&'static str>)> {
    let mut cases: Vec<(&str, Vec<&str>)> = Vec::new();
    for line in STOCK.lines() {
        if let Some(sql) = line.strip_prefix("> ") {
            cases.push((sql, Vec::new()));
        } else if let Some((_, rows)) = cases.last_mut() {
            rows.push(line);
        }
    }
    cases
}

/// A value as `sqlite3_exec()` prints it.
fn exec_text(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "N".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format_sqlite_float(*f),
        SqliteValue::Text(text) => String::from_utf8_lossy(text.as_bytes_direct()).into_owned(),
        SqliteValue::Blob(bytes) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

#[test]
fn geopoly_functions_match_recorded_stock_results() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        let cases = stock_cases();
        assert!(cases.len() > 190, "fixture has only {} cases", cases.len());
        let mut failures = Vec::new();
        for (sql, expected) in &cases {
            let actual: Vec<String> = match conn.query(sql).await {
                Ok(rows) => rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(exec_text)
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect(),
                Err(error) => vec![format!("ERROR: {error}")],
            };
            let matches = match expected.as_slice() {
                [only] if only.starts_with("ERROR: ") => {
                    let message = &only["ERROR: ".len()..];
                    matches!(actual.as_slice(), [got] if got.starts_with("ERROR: ") && got.contains(message))
                }
                _ => actual == *expected,
            };
            if !matches {
                failures.push(format!(
                    "{sql}\n  stock:   {expected:?}\n  fsqlite: {actual:?}"
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {} geopoly statements differ from stock:\n{}",
            failures.len(),
            cases.len(),
            failures.join("\n")
        );
        conn.close().await.expect("close");
    });
}
