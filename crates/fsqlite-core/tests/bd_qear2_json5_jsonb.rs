#![recursion_limit = "512"]

//! bd-qear2 (increment 3 probe): do the jsonb-producing functions accept JSON5?
//! Tested via round-trip through json()/json_extract (JSONB bytes are opaque, so
//! we compare the re-textified result vs rusqlite). If frank routes jsonb via
//! the (already-lenient) json_arg_value choke point, JSON5 works for free.
//! Non-finite (+Infinity/-Infinity/NaN) remains a documented follow-up.

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
fn json5_jsonb_functions_bd_qear2() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // jsonb(JSON5) round-tripped back to text via json()
            "SELECT json(jsonb('{a:1, b:2}'))",
            "SELECT json(jsonb('[1, 2, 3,]'))",
            "SELECT json(jsonb('{''x'':''hi''}'))",
            "SELECT json(jsonb('{n: 0xFF, /* c */ m: 2}'))",
            // extract from a jsonb(JSON5)
            "SELECT json_extract(jsonb('{a:{b:7}}'),'$.a.b')",
            // NOTE: jsonb_extract-of-scalar returning JSONB bytes vs the SQL
            // scalar is a separate pre-existing bug (bd-*, filed) unrelated to
            // JSON5 — excluded here.
            "SELECT json_type(jsonb('{a:1}'),'$.a')",
            // jsonb transform round-trips
            "SELECT json(jsonb_set(jsonb('{a:1}'),'$.b',2))",
            "SELECT json(jsonb_remove(jsonb('{a:1, b:2,}'),'$.a'))",
            // standard JSON regression (jsonb of standard JSON)
            "SELECT json(jsonb('{\"a\":9}'))",
            "SELECT json_extract(jsonb('[10,20,30]'),'$[1]')",
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
            "{} jsonb-JSON5 divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
