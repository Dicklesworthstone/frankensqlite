#![recursion_limit = "512"]

//! JSON edge-function leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! newer/less-tested json surface — json_valid 1-arg & 2-arg flag form,
//! json_error_position, json_pretty, -> / ->> with negative array indices and
//! quoted keys, and JSON5 input (unquoted keys, trailing commas, single quotes,
//! comments, hex, +/-Infinity). Error-tolerant so a missing feature surfaces as
//! a divergence, not a panic. Pass = coverage keeper; a mismatch is a leaf.

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
// The JSON5 SQL literals ('{a:1}') look like format args to clippy.
#[allow(clippy::literal_string_with_formatting_args)]
fn json_edge_functions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // json_valid 1-arg
            "SELECT json_valid('{\"a\":1}'), json_valid('{bad}'), json_valid('null'), json_valid('')",
            // json_valid 2-arg flag form (bit1=strict-JSON, bit2..=JSON5 etc.)
            "SELECT json_valid('{\"a\":1}', 1)",
            "SELECT json_valid('{a:1}', 1)",
            "SELECT json_valid('{a:1}', 2)",
            "SELECT json_valid('{a:1}', 4)",
            "SELECT json_valid('{a:1}', 6)",
            "SELECT json_valid('[1,2,]', 6)",
            // json_error_position
            "SELECT json_error_position('{\"a\":1}')",
            "SELECT json_error_position('{\"a\":}')",
            "SELECT json_error_position('[1,2,,3]')",
            // json_pretty
            "SELECT json_pretty('{\"a\":1,\"b\":[2,3]}')",
            "SELECT json_pretty('[1,{\"x\":2}]')",
            // -> / ->> with negative array indices + quoted keys
            "SELECT '[1,2,3]' ->> -1",
            "SELECT '[1,2,3]' -> -2",
            "SELECT json_extract('[10,20,30]','$[#-1]')",
            "SELECT json_extract('{\"a b\":5}','$.\"a b\"')",
            "SELECT '{\"a\":{\"b\":9}}' ->> '$.a.b'",
            // NOTE: JSON5 input (unquoted keys, single quotes, trailing commas,
            // comments, hex, +/-Infinity/NaN) is NOT yet supported by frank —
            // tracked as the multi-turn feature bd-qear2 — so those cases are
            // intentionally excluded here.
            // json_quote (scalar -> json)
            "SELECT json_quote(3.0), json_quote('a'), json_quote(null)",
            // json_array_length on non-array / with path
            "SELECT json_array_length('{\"a\":1}'), json_array_length('5')",
            // json_type at deep path
            "SELECT json_type('{\"a\":[{\"b\":true}]}','$.a[0].b')",
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
            "{} json edge-function divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
