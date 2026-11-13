#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over
//! query-structure semantics — CTEs (incl. RECURSIVE), set operations
//! (UNION/UNION ALL/INTERSECT/EXCEPT with ORDER BY/LIMIT), and subqueries
//! (scalar, correlated, EXISTS/NOT EXISTS, IN (subquery)). Pass = parity
//! coverage keeper; a mismatch is a leaf divergence. Full ordered sets compared.

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
fn cte_setops_subquery_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE a(id INTEGER, v INTEGER)",
            "CREATE TABLE b(id INTEGER, w INTEGER)",
            "INSERT INTO a VALUES (1,10),(2,20),(3,30),(4,NULL)",
            "INSERT INTO b VALUES (2,200),(3,300),(5,500)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // CTE
            "WITH c AS (SELECT id, v*2 AS d FROM a WHERE v IS NOT NULL) SELECT * FROM c ORDER BY id",
            "WITH c(x) AS (SELECT v FROM a) SELECT sum(x) FROM c",
            // recursive CTE — counter
            "WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n<5) SELECT n FROM cnt ORDER BY n",
            // recursive CTE — factorial-ish accumulation
            "WITH RECURSIVE f(n,acc) AS (SELECT 1,1 UNION ALL SELECT n+1,acc*(n+1) FROM f WHERE n<5) SELECT n,acc FROM f ORDER BY n",
            // recursive CTE — string build
            "WITH RECURSIVE s(i,txt) AS (SELECT 1,'x' UNION ALL SELECT i+1,txt||'x' FROM s WHERE i<4) SELECT txt FROM s ORDER BY i",
            // set operations
            "SELECT id FROM a UNION SELECT id FROM b ORDER BY id",
            "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id",
            "SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id",
            "SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id",
            "SELECT id FROM a UNION SELECT id FROM b ORDER BY id DESC LIMIT 2",
            "SELECT v FROM a UNION SELECT w FROM b ORDER BY 1",
            // subqueries
            "SELECT id, (SELECT max(w) FROM b) AS mx FROM a ORDER BY id",
            "SELECT id, (SELECT w FROM b WHERE b.id=a.id) AS cw FROM a ORDER BY id",
            "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id=a.id) ORDER BY id",
            "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id=a.id) ORDER BY id",
            "SELECT id FROM a WHERE id IN (SELECT id FROM b) ORDER BY id",
            "SELECT id FROM a WHERE id NOT IN (SELECT id FROM b WHERE w<400) ORDER BY id",
            "SELECT id FROM a WHERE v > (SELECT avg(v) FROM a) ORDER BY id",
            // correlated aggregate
            "SELECT a.id, (SELECT count(*) FROM b WHERE b.w > a.v) FROM a ORDER BY a.id",
            // NULL in NOT IN (three-valued)
            "SELECT id FROM a WHERE id NOT IN (SELECT id FROM b UNION SELECT NULL) ORDER BY id",
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
            "{} cte/setop/subquery divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
