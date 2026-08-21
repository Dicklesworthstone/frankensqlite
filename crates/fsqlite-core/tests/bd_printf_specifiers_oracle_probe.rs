#![recursion_limit = "512"]

//! Exhaustive printf()/format() specifier leaf-hunt (pane af49, 2026-08-21):
//! frank vs rusqlite over every conversion, flag, width, and precision combo,
//! plus SQLite-specific %q/%Q/%w and undefined specifiers. Float cases use
//! EXPLICIT precision (deterministic — no shortest-round-trip / oracle-version
//! ambiguity). Pass = coverage keeper; a mismatch is a leaf divergence.

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
fn printf_specifiers_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let calls = [
            // integer conversions + flags/width
            "printf('%d|%i|%u', -42, -42, 42)",
            "printf('[%5d][%-5d][%05d][%+d][% d]', 42, 42, 42, 42, 42)",
            "printf('%x %X %o', 255, 255, 64)",
            "printf('%#x %#X %#o', 255, 255, 64)",
            "printf('%08x', 255)",
            "printf('%d', 9223372036854775807)",
            "printf('%d', -9223372036854775808)",
            "printf('%x', -1)",
            "printf('%c%c%c', 72, 105, 33)",
            "printf('%c', 0x4e16)",
            // float conversions with EXPLICIT precision (deterministic)
            "printf('%.2f|%.0f|%.5f', 3.14159, 3.9, 2.5)",
            "printf('%+.2f|% .2f', 3.1, 3.1)",
            "printf('%08.2f|%-8.2f|', 3.1, 3.1)",
            "printf('%.3e|%.3E', 12345.678, 12345.678)",
            "printf('%.4g|%.4G', 0.00012345, 123450.0)",
            "printf('%.0e', 9.9)",
            "printf('%.2f', -0.0)",
            "printf('%.10f', 1.0/3.0)",
            "printf('%f', 100)",
            "printf('%d', 3.9)",
            // string conversions + precision/width
            "printf('[%s][%10s][%-10s][%.3s]', 'hello', 'hi', 'hi', 'hello')",
            "printf('%s', NULL)",
            "printf('%.0s', 'abc')",
            // star width/precision
            "printf('%*d|%-*d|%.*f|%*.*f', 6, 42, 6, 42, 2, 3.14159, 8, 3, 3.14159)",
            // SQLite-specific
            "printf('%q', 'a''b''c')",
            "printf('%Q', 'x''y')",
            "printf('%Q', NULL)",
            "printf('%w', 'a\"b')",
            "printf('%%|a%%b')",
            "printf('%z', 'zz')",
            // undefined / edge specifiers
            "printf('%y', 5)",
            "printf('%b', 5)",
            "printf('no specifiers at all')",
            "printf('%d and %s and %.1f', 1, 'x', 2.0)",
            "printf('%5.2f%%', 99.9)",
            // arg count mismatches (fewer/more args than specifiers)
            "printf('%d %d', 1)",
            "printf('%d', 1, 2, 3)",
            "printf('%d %s')",
        ];

        let mut diffs = Vec::new();
        for c in calls {
            let sql = format!("SELECT {c}");
            let fv = fval(&f, &sql).await;
            let rv = rval(&r, &sql);
            if fv != rv {
                diffs.push(format!("  `{c}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} printf specifier divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
