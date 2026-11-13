#![recursion_limit = "512"]

//! ORDER BY NULLS / LIMIT-OFFSET leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over NULL ordering placement (default per ASC/DESC, explicit NULLS
//! FIRST/LAST, 3.30+), multi-key ordering, ORDER BY by ordinal/expression/
//! COLLATE, and LIMIT/OFFSET edges (negative=unlimited, zero, past-end offset,
//! over-count). Full ordered result sets compared. Pass = coverage keeper.

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
fn orderby_nulls_limit_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE o(id INTEGER PRIMARY KEY, x INTEGER, y TEXT)",
            "INSERT INTO o VALUES (1,30,'c'),(2,NULL,'a'),(3,10,NULL),(4,10,'b'),(5,NULL,'d'),(6,20,'a')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // default NULL placement: ASC -> NULLs first, DESC -> NULLs last
            "SELECT id FROM o ORDER BY x, id",
            "SELECT id FROM o ORDER BY x DESC, id",
            // explicit NULLS FIRST / LAST (3.30+)
            "SELECT id FROM o ORDER BY x NULLS LAST, id",
            "SELECT id FROM o ORDER BY x NULLS FIRST, id",
            "SELECT id FROM o ORDER BY x DESC NULLS FIRST, id",
            "SELECT id FROM o ORDER BY x DESC NULLS LAST, id",
            // text column with NULL + collation
            "SELECT id FROM o ORDER BY y NULLS LAST, id",
            "SELECT id FROM o ORDER BY y COLLATE NOCASE NULLS FIRST, id",
            // multi-key + ordinal + expression
            "SELECT id FROM o ORDER BY x, y, id",
            "SELECT x, id FROM o ORDER BY 1 NULLS LAST, 2",
            "SELECT id FROM o ORDER BY (x % 20) NULLS FIRST, id",
            // LIMIT / OFFSET edges
            "SELECT id FROM o ORDER BY id LIMIT 3",
            "SELECT id FROM o ORDER BY id LIMIT -1",
            "SELECT id FROM o ORDER BY id LIMIT 0",
            "SELECT id FROM o ORDER BY id LIMIT 100",
            "SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 2",
            "SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 10",
            "SELECT id FROM o ORDER BY id LIMIT -1 OFFSET 3",
            "SELECT id FROM o ORDER BY x NULLS LAST, id LIMIT 3 OFFSET 1",
            "SELECT id FROM o ORDER BY id DESC LIMIT 2",
            // LIMIT with expression bound
            "SELECT id FROM o ORDER BY id LIMIT 1+1",
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
            "{} order-by-nulls/limit divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
