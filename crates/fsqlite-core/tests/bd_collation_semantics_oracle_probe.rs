#![recursion_limit = "512"]

//! Collation-semantics leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! how BINARY/NOCASE/RTRIM collations drive comparisons, ORDER BY, GROUP BY,
//! DISTINCT, UNION dedup, IN/BETWEEN, and min/max — via column-declared
//! collation, explicit COLLATE overrides, and the interaction rules (the
//! left operand's/column's collation wins). Full ordered result sets compared.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

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

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

#[test]
fn collation_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE c(nc TEXT COLLATE NOCASE, rt TEXT COLLATE RTRIM, bn TEXT)",
            "INSERT INTO c VALUES ('Apple','x  ','Apple')",
            "INSERT INTO c VALUES ('apple','x','apple')",
            "INSERT INTO c VALUES ('BANANA','y ','BANANA')",
            "INSERT INTO c VALUES ('banana','y','banana')",
            "INSERT INTO c VALUES ('Cherry','z','Cherry')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // column-declared collation drives comparison
            "SELECT nc FROM c WHERE nc = 'apple' ORDER BY bn",
            "SELECT bn FROM c WHERE bn = 'apple' ORDER BY bn",
            "SELECT rt FROM c WHERE rt = 'x' ORDER BY bn",
            // DISTINCT / count(DISTINCT) honour column collation
            "SELECT count(DISTINCT nc) FROM c",
            "SELECT count(DISTINCT bn) FROM c",
            "SELECT DISTINCT nc FROM c ORDER BY bn",
            // ORDER BY collation
            "SELECT bn FROM c ORDER BY nc, bn",
            "SELECT bn FROM c ORDER BY bn COLLATE NOCASE, bn",
            "SELECT bn FROM c ORDER BY bn",
            // GROUP BY collation
            "SELECT nc, count(*) FROM c GROUP BY nc ORDER BY nc",
            "SELECT bn COLLATE NOCASE, count(*) FROM c GROUP BY bn COLLATE NOCASE ORDER BY 1",
            // explicit COLLATE override (left operand's explicit collation wins)
            "SELECT 'ABC' = 'abc' COLLATE NOCASE",
            "SELECT 'ABC' COLLATE NOCASE = 'abc'",
            "SELECT 'a ' = 'a' COLLATE RTRIM",
            "SELECT bn FROM c WHERE bn = 'BANANA' COLLATE NOCASE ORDER BY bn",
            // IN / BETWEEN with collation
            "SELECT bn FROM c WHERE nc IN ('apple','cherry') ORDER BY bn",
            "SELECT nc FROM c WHERE nc BETWEEN 'a' AND 'c' ORDER BY bn",
            // min/max honour collation of the argument column
            "SELECT min(nc), max(nc) FROM c",
            "SELECT min(bn), max(bn) FROM c",
            // UNION dedup uses BINARY regardless of column collation
            "SELECT nc FROM c UNION SELECT nc FROM c ORDER BY 1",
            // LIKE is ASCII-case-insensitive independent of collation
            "SELECT bn FROM c WHERE bn LIKE 'a%' ORDER BY bn",
            "SELECT bn FROM c WHERE nc LIKE 'A%' ORDER BY bn",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} collation-semantics divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
