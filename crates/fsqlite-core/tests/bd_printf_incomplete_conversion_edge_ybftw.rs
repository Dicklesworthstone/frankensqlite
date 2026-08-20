//! bd-printf-incomplete-conversion-edge-ybftw: printf()/format() incomplete
//! conversions must match stock SQLite. Differential vs rusqlite (SQLite 3.53).
//!
//! * A trailing bare `%` at end-of-format stays LITERAL: printf('abc%')=='abc%',
//!   printf('%')=='%', printf('%d%',7)=='7%'.
//! * A `%` that consumed flags/width/precision but then hit EOF with no
//!   conversion char (printf('%5'), '%-', '%.', '%.3', '% ', '%05', '%+', '%#',
//!   '%*', '%.*') NULLs the whole call.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn frank(f: &Connection, sql: &str) -> String {
    match f.query(sql).await {
        Ok(rows) if !rows.is_empty() => match &rows[0].values()[0] {
            SqliteValue::Null => "<null>".to_owned(),
            SqliteValue::Integer(n) => format!("i:{n}"),
            SqliteValue::Float(x) => format!("r:{x}"),
            SqliteValue::Text(s) => format!("t:{s}"),
            SqliteValue::Blob(b) => format!("b:{b:02X?}"),
        },
        Ok(_) => "<norows>".to_owned(),
        Err(e) => format!("<err:{e:?}>"),
    }
}

fn oracle(r: &rusqlite::Connection, sql: &str) -> String {
    match r.query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0)) {
        Ok(rusqlite::types::Value::Null) => "<null>".to_owned(),
        Ok(rusqlite::types::Value::Integer(n)) => format!("i:{n}"),
        Ok(rusqlite::types::Value::Real(x)) => format!("r:{x}"),
        Ok(rusqlite::types::Value::Text(s)) => format!("t:{s}"),
        Ok(rusqlite::types::Value::Blob(b)) => format!("b:{b:02X?}"),
        Err(e) => format!("<err:{e}>"),
    }
}

async fn assert_all_agree(sqls: &[String]) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut diverged = Vec::new();
    for sql in sqls {
        let fv = frank(&f, sql).await;
        let rv = oracle(&r, sql);
        if fv != rv {
            diverged.push(format!("  {sql}\n    frank ={fv}\n    oracle={rv}"));
        }
    }
    assert!(
        diverged.is_empty(),
        "{} incomplete-% divergence(s) vs rusqlite 3.53:\n{}",
        diverged.len(),
        diverged.join("\n")
    );
}

#[test]
fn printf_trailing_bare_percent_stays_literal() {
    asupersync::test_utils::run_test(|| async {
        let sqls: Vec<String> = ["%", "abc%", "a%", "%d%", "x%%y", "100%"]
            .iter()
            .map(|f| format!("SELECT quote(printf('{f}', 7))"))
            .collect();
        assert_all_agree(&sqls).await;
    });
}

#[test]
fn printf_incomplete_conversion_nulls() {
    asupersync::test_utils::run_test(|| async {
        let sqls: Vec<String> = [
            "%5", "%-", "%.", "%.3", "% ", "%05", "%+", "%#", "%*", "%.*", "%,", "%!",
        ]
        .iter()
        .map(|f| format!("SELECT quote(printf('{f}', 7))"))
        .collect();
        assert_all_agree(&sqls).await;
    });
}

#[test]
fn printf_complete_conversions_still_format() {
    asupersync::test_utils::run_test(|| async {
        // Guard: the break-point changes must not disturb normal, complete specs.
        let sqls: Vec<String> = ["%d", "%5d", "%.3d", "%-5d|", "%%", "%x", "%s"]
            .iter()
            .map(|f| format!("SELECT quote(printf('{f}', 7))"))
            .collect();
        assert_all_agree(&sqls).await;
    });
}
