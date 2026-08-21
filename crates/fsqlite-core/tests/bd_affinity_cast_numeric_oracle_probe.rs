#![recursion_limit = "512"]

//! Adversarial differential sweep (pane af49, 2026-08-20): frank vs rusqlite
//! over the value-semantics CORNERS where divergences hide — column-affinity
//! storage coercion, CAST boundaries (i64 overflow, huge reals, whitespace/hex/
//! blob text), integer-overflow-to-real arithmetic, and division/modulo sign
//! rules. Pass = coverage keeper; a mismatch is a leaf. Typed structural compare.

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
fn affinity_cast_numeric_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        // Column-affinity storage coercion: insert the same literals into
        // columns of every affinity class and read back (typeof, value).
        for s in [
            "CREATE TABLE aff(i INTEGER, t TEXT, r REAL, n NUMERIC, b BLOB, x)",
            "INSERT INTO aff VALUES ('123','123','123','123','123','123')",
            "INSERT INTO aff VALUES (4.0, 4.0, 4, '4.5', 4.0, 4.0)",
            "INSERT INTO aff VALUES ('3.0e2','3.0e2','3.0e2','3.0e2','3.0e2','3.0e2')",
            "INSERT INTO aff VALUES ('abc', 12, 12, 'abc', 12, 12)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let table_queries = [
            "SELECT typeof(i),typeof(t),typeof(r),typeof(n),typeof(b),typeof(x) FROM aff WHERE rowid=1",
            "SELECT i,t,r,n,b,x FROM aff WHERE rowid=1",
            "SELECT typeof(i),typeof(t),typeof(r),typeof(n) FROM aff WHERE rowid=2",
            "SELECT i,r,n FROM aff WHERE rowid=2",
            "SELECT typeof(i),typeof(n),i,n FROM aff WHERE rowid=3",
            "SELECT typeof(i),typeof(n),i,n FROM aff WHERE rowid=4",
            // affinity in comparison: integer column vs text literal
            "SELECT count(*) FROM aff WHERE i = '123'",
        ];

        let scalar_queries = [
            // CAST boundaries
            "SELECT CAST(9223372036854775807 AS REAL)",
            "SELECT CAST('9223372036854775808' AS INTEGER)",
            "SELECT CAST('-9223372036854775809' AS INTEGER)",
            "SELECT CAST(1e19 AS INTEGER)",
            "SELECT CAST(-1e19 AS INTEGER)",
            "SELECT CAST('  3.14  ' AS REAL)",
            "SELECT CAST('0x1A' AS INTEGER)",
            "SELECT CAST('12abc' AS INTEGER)",
            "SELECT CAST('3.9' AS INTEGER)",
            "SELECT CAST(x'41' AS TEXT)",
            "SELECT CAST(3.99 AS INTEGER)",
            "SELECT CAST(-0.0 AS INTEGER)",
            "SELECT typeof(CAST(5 AS REAL)),typeof(CAST(5.0 AS INTEGER))",
            // integer overflow -> real
            "SELECT 9223372036854775807 + 1",
            "SELECT typeof(9223372036854775807 + 1)",
            "SELECT 9223372036854775807 * 2",
            "SELECT -9223372036854775808 - 1",
            "SELECT 9000000000000000000 + 9000000000000000000",
            // division / modulo sign rules
            "SELECT 7/2, -7/2, 7/-2, -7/-2",
            "SELECT 7%3, -7%3, 7%-3, -7%-3",
            "SELECT 7.0/2, 7/2.0, 1/0, 1.0/0, 5%0",
            // hex literals
            "SELECT 0x1F, 0xFF, 0x7FFFFFFFFFFFFFFF",
            // real formatting edges
            "SELECT 0.1+0.2, 1.0/3.0, 2e308",
            "SELECT typeof(1/1), typeof(3/2), typeof(4/2)",
        ];

        let mut diffs = Vec::new();
        for q in table_queries {
            let (fr, rr) = (fq(&f, q).await, rq(&r, q));
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        for q in scalar_queries {
            let (fr, rr) = (fq(&f, q).await, rq(&r, q));
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} affinity/cast/numeric divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
