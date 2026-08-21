#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over
//! string builtins (substr/trim/replace/instr/quote/char/unicode/hex) and
//! aggregate edge cases (group_concat separators & ordering, sum/total/avg on
//! mixed types, min/max affinity, count). Passing run = parity coverage keeper;
//! a mismatch is a leaf divergence. Typed structural compare, no float display.

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
    let rows = conn.query(sql).await.unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"));
    assert_eq!(rows.len(), 1, "frank `{sql}` returned {} rows", rows.len());
    tag_f(&rows[0].values()[0])
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    })
    .unwrap_or_else(|e| panic!("rusqlite `{sql}`: {e:?}"))
}

#[test]
fn string_and_aggregate_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        // Shared table for aggregate cases.
        for s in [
            "CREATE TABLE t(k TEXT, n)",
            "INSERT INTO t VALUES ('a',3),('b',1),('a',2),('c',NULL),('b','7x'),('a',10)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let scalar_exprs = [
            "SELECT substr('hello',-2)",
            "SELECT substr('hello',-2,1)",
            "SELECT substr('hello',0,2)",
            "SELECT substr('hello',2,-1)",
            "SELECT trim('xxhelloxx','x')",
            "SELECT ltrim('  hi'),rtrim('hi  ')",
            "SELECT replace('aaa','a','')",
            "SELECT replace('abc','','X')",
            "SELECT instr('hello','l')",
            "SELECT instr('hello','z')",
            "SELECT instr('','x')",
            "SELECT instr('abcabc','bc')",
            "SELECT quote(x'00ff')",
            "SELECT quote('it''s')",
            "SELECT char(72,105)",
            "SELECT unicode('A'),unicode('')",
            "SELECT hex(zeroblob(3))",
            "SELECT hex(cast('AB' as blob))",
            "SELECT length('héllo'),length(x'0102')",
            "SELECT upper('héllo'),lower('HÉLLO')",
            "SELECT 'a'||NULL,NULL||'b'",
            "SELECT typeof(1),typeof(1.0),typeof('x'),typeof(NULL),typeof(x'00')",
            "SELECT nullif('a','a'),nullif('a','b')",
            "SELECT coalesce(NULL,NULL,'z')",
        ];
        // Aggregates over t. group_concat ordering follows scan order on both.
        let agg_exprs = [
            "SELECT group_concat(k) FROM t",
            "SELECT group_concat(k,'-') FROM t",
            "SELECT group_concat(DISTINCT k) FROM t",
            "SELECT count(*),count(n) FROM t",
            "SELECT sum(n),total(n) FROM t",
            "SELECT avg(n) FROM t",
            "SELECT min(n),max(n) FROM t",
            "SELECT min(k),max(k) FROM t",
            "SELECT group_concat(n) FROM (SELECT n FROM t WHERE k='a' ORDER BY n)",
            "SELECT sum(n) FROM t WHERE k='a'",
            "SELECT count(DISTINCT k) FROM t",
        ];

        let mut diffs = Vec::new();
        for e in scalar_exprs.iter().chain(agg_exprs.iter()) {
            let fv = fval(&f, e).await;
            let rv = rval(&r, e);
            if fv != rv {
                diffs.push(format!("  `{e}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} string/aggregate divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
