#![recursion_limit = "512"]

//! BLOB-semantics leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! byte-oriented blob behavior — length (byte count), substr byte-subrange
//! (incl negative/zero), instr over blobs, blob comparison ordering, hex/unhex/
//! quote/zeroblob round-trips, blob<->text CAST, and || on blobs. Typed
//! structural compare. Pass = coverage keeper; a mismatch is a leaf divergence.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:02X?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02X?}"),
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
fn blob_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // length = byte count; typeof
            "SELECT length(x'010203'), length(x''), typeof(x'01')",
            // substr byte-subrange (blob result)
            "SELECT substr(x'0102030405', 2, 2)",
            "SELECT substr(x'0102030405', -2)",
            "SELECT substr(x'0102030405', 0, 3)",
            "SELECT substr(x'0102030405', 3)",
            // instr over blobs
            "SELECT instr(x'0102030405', x'0304')",
            "SELECT instr(x'0102030405', x'09')",
            "SELECT instr(x'0102030405', x'')",
            // hex / unhex / quote / zeroblob round-trips
            "SELECT hex(x'00FFaa'), hex(zeroblob(3))",
            "SELECT unhex('48656C6C6F')",
            "SELECT quote(x'00ff10'), quote(x'')",
            "SELECT length(zeroblob(5))",
            // comparison + ordering (blob sorts after text)
            "SELECT (x'0102' = x'0102'), (x'01' < x'02'), (x'0102' < x'0103')",
            "SELECT (x'41' = 'A')",
            "SELECT (x'01' > 'zzz'), (x'01' > 999999)",
            // CAST blob<->text and numeric
            "SELECT CAST(x'414243' AS TEXT)",
            "SELECT CAST('ABC' AS BLOB)",
            "SELECT CAST(x'3132' AS INTEGER)",
            "SELECT hex(CAST(255 AS BLOB))",
            // || on blobs (SQLite converts operands to text)
            "SELECT typeof(x'41' || x'42'), (x'41' || x'42')",
            "SELECT x'41' || 'B'",
            // min/max over blobs
            "SELECT min(x'02', x'01'), max(x'02', x'01')",
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
            "{} blob-semantics divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
