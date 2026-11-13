#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite (bundled
//! SQLite) over historically-tricky date/time modifier arithmetic and printf/
//! format edge cases. A passing run is a parity regression keeper; a failing
//! case is a leaf divergence to fix. Values compared structurally (typed),
//! never display text. Deterministic inputs only (no 'now').

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

async fn fval(conn: &Connection, expr: &str) -> String {
    let sql = format!("SELECT {expr};");
    let rows = conn
        .query(&sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{expr}`: {e:?}"));
    assert_eq!(rows.len(), 1, "frank `{expr}` returned {} rows", rows.len());
    tag_f(&rows[0].values()[0])
}
fn rval(conn: &rusqlite::Connection, expr: &str) -> String {
    let sql = format!("SELECT {expr};");
    conn.query_row(&sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    })
    .unwrap_or_else(|e| panic!("rusqlite `{expr}`: {e:?}"))
}

#[test]
fn datetime_and_printf_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // ── date/time modifier arithmetic ───────────────────────────────
            "datetime('2024-01-31','+1 month')",
            "date('2024-03-31','+1 month')",
            "datetime('2024-01-15','start of month')",
            "datetime('2024-02-29','+1 year')",
            "date('2024-01-01','+45 days')",
            "date('2024-03-15','weekday 0')",
            "date('2024-03-15','weekday 3')",
            "strftime('%Y-%m-%d %H:%M:%f','2024-01-01 12:34:56.789')",
            "strftime('%w %j %W','2024-03-15')",
            "strftime('%s','2024-01-01 00:00:00')",
            "strftime('%f','2024-01-01 00:00:01.5')",
            "julianday('2024-01-01')",
            "unixepoch('2024-01-01 00:00:00')",
            "datetime('2024-06-15','-3 months','+10 days')",
            "date('2024-02-29','-1 year')",
            "datetime(1704067200,'unixepoch')",
            "strftime('%Y-W%W-%w','2024-01-01')",
            "date('2024-12-31','+1 day')",
            "datetime('2024-01-01','+90 minutes')",
            "date('2024-03-01','-1 day')",
            // ── printf / format edge cases ──────────────────────────────────
            "printf('%5.2f', 3.14159)",
            "printf('%+d|% d', 5, 5)",
            "printf('%x|%X|%#x', 255, 255, 255)",
            "printf('%o|%#o', 8, 8)",
            "printf('%e|%E', 12345.678, 12345.678)",
            "printf('%g|%g', 0.0001, 1000000.0)",
            "printf('%c%c', 72, 105)",
            "printf('%*.*f', 8, 2, 3.14159)",
            "printf('%.3s', 'hello')",
            "printf('a%%b')",
            "printf('[%-6d]', 42)",
            "printf('%06.2f', 3.1)",
            "printf('%d %i %u', -5, -5, 5)",
            "printf('%!d', 5)",
            "format('%s=%d', 'x', 7)",
            "printf('%,d', 1234567)",
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
            "{} date/time+printf divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
