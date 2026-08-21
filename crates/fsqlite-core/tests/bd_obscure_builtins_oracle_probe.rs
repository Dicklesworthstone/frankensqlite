#![recursion_limit = "512"]

//! Leaf-hunt differential (pane af49, 2026-08-20): frank vs rusqlite over
//! NEWER / obscure scalar builtins where a MISSING or divergent implementation
//! is plausible — unhex, concat, concat_ws, octet_length, char edge, format
//! variants, iif, likelihood/likely/unlikely, quote of odd types, hex/zeroblob,
//! nullif/coalesce/ifnull corners. Error-TOLERANT: a frank error (e.g. 'no such
//! function') is tagged and compared against stock's value, so a missing
//! function surfaces as a divergence rather than aborting the sweep.

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

/// Error-tolerant: returns "ERR" on failure so a missing function is a
/// comparable value, not a panic.
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
fn obscure_builtins_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // newer string builtins (SQLite 3.44+)
            "SELECT unhex('48656C6C6F')",
            "SELECT unhex('48 65', ' ')",
            "SELECT unhex('xyz')",
            "SELECT concat('a','b','c')",
            "SELECT concat('a',NULL,'c')",
            "SELECT concat(1,2.5,'x')",
            "SELECT concat_ws('-','a','b','c')",
            "SELECT concat_ws('-','a',NULL,'c')",
            "SELECT concat_ws(',',1,2,3)",
            "SELECT octet_length('héllo')",
            "SELECT octet_length(x'0102')",
            "SELECT octet_length(12345)",
            // char / format / printf corners
            "SELECT char(65,0,66)",
            "SELECT format('%d-%s', 5, 'x')",
            "SELECT printf('%w', 5)",
            "SELECT printf('%q', 'a''b')",
            "SELECT printf('%Q', 'a''b')",
            "SELECT printf('%z', 'x')",
            // iif / control
            "SELECT iif(1>0,'y','n')",
            "SELECT iif(NULL,'y','n')",
            // optimizer-hint passthroughs
            "SELECT likelihood(42, 0.5)",
            "SELECT likely(7)",
            "SELECT unlikely(9)",
            // quote of odd types
            "SELECT quote(1.5)",
            "SELECT quote(NULL)",
            "SELECT quote(x'00')",
            // misc corners
            "SELECT nullif(0,0), nullif(0.0,0)",
            "SELECT coalesce(NULL, 2.5)",
            "SELECT ifnull(NULL, x'01')",
            "SELECT hex(unhex('4142'))",
            "SELECT abs(-9223372036854775808)",
            "SELECT sign(NULL)",
            "SELECT substr('abcdef', 2)",
            "SELECT replace('aaa','a','bb')",
            "SELECT typeof(concat(1,2))",
            "SELECT typeof(unhex('41'))",
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
            "{} obscure-builtin divergence(s) vs rusqlite (frank ERR = likely missing fn):\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
