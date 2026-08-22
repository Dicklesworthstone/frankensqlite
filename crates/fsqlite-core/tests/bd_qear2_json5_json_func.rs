#![recursion_limit = "512"]
// JSON5 SQL literals ('{a:1}') look like format args to clippy.
#![allow(clippy::literal_string_with_formatting_args)]

//! bd-qear2 (increment 1): json() accepts JSON5 input and canonicalizes it to
//! standard JSON, matching stock SQLite 3.42+. Covers the common JSON5 features
//! (unquoted keys, single-quoted strings, trailing commas, // and /* */
//! comments, hex integers, leading/trailing decimal points) plus standard-JSON
//! regression guards. Non-finite (+Infinity/-Infinity/NaN) is a documented
//! follow-up and NOT asserted here. Oracle = rusqlite (bundled SQLite).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn text_f(rows: &[fsqlite_core::connection::Row]) -> String {
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Null => "NULL".to_owned(),
        other => format!("{other:?}"),
    }
}

async fn fjson(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) if rows.len() == 1 => text_f(&rows),
        _ => "ERR".to_owned(),
    }
}
fn rjson(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|_| "ERR".to_owned())
}

#[test]
fn json_func_accepts_json5_bd_qear2() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let calls = [
            // standard JSON — must stay byte-identical (lexical minify preserved)
            "SELECT json('{\"a\": 1, \"b\": [2, 3]}')",
            "SELECT json('  [ 1 , 2.50 , \"x\" ] ')",
            "SELECT json('{\"e\": 1e3}')",
            // JSON5: unquoted keys + trailing comma
            "SELECT json('{a:1, b:2}')",
            "SELECT json('{a:1, b:2,}')",
            "SELECT json('[1, 2, 3,]')",
            // JSON5: single-quoted strings
            "SELECT json('{''x'':''hi''}')",
            "SELECT json('[''a'', ''b'']')",
            // JSON5: comments
            "SELECT json('[1, 2, /* c */ 3]')",
            "SELECT json('{a: 1, // note'||char(10)||' b: 2}')",
            // JSON5: hex integers
            "SELECT json('{x: 0xFF, y: 0x10}')",
            // JSON5: leading/trailing decimal points + leading +
            "SELECT json('{a: .5, b: 2., c: +3}')",
            // JSON5: nested + mixed
            "SELECT json('{list: [1, 2,], obj: {k: ''v'',}}')",
            // invalid under both -> ERR on both
            "SELECT json('{a: }')",
            "SELECT json('not json')",
        ];

        let mut diffs = Vec::new();
        for c in calls {
            let fv = fjson(&f, c).await;
            let rv = rjson(&r, c);
            if fv != rv {
                diffs.push(format!("  `{c}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} json()-JSON5 divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
