#![recursion_limit = "512"]

//! JOIN-semantics leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! INNER / LEFT / RIGHT / FULL OUTER / CROSS / NATURAL joins, USING vs ON,
//! self-joins, 3-table chains, outer-join NULL handling, and the ON-vs-WHERE
//! distinction for outer joins. RIGHT/FULL OUTER (3.39+) are prime leaf
//! candidates. Ordered result sets compared. Pass = coverage keeper.

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
    match conn.query(sql).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn join_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE a(id INTEGER, x TEXT)",
            "CREATE TABLE b(id INTEGER, y TEXT)",
            "CREATE TABLE c(id INTEGER, z TEXT)",
            "INSERT INTO a VALUES (1,'a1'),(2,'a2'),(3,'a3')",
            "INSERT INTO b VALUES (2,'b2'),(3,'b3'),(4,'b4')",
            "INSERT INTO c VALUES (3,'c3'),(5,'c5')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            "SELECT a.id,x,y FROM a JOIN b ON a.id=b.id ORDER BY a.id",
            "SELECT a.id,x,y FROM a LEFT JOIN b ON a.id=b.id ORDER BY a.id",
            "SELECT a.id,x,y FROM a RIGHT JOIN b ON a.id=b.id ORDER BY b.id",
            "SELECT COALESCE(a.id,b.id) AS k,x,y FROM a FULL OUTER JOIN b ON a.id=b.id ORDER BY k",
            "SELECT id,x,y FROM a JOIN b USING(id) ORDER BY id",
            "SELECT id,x,y FROM a LEFT JOIN b USING(id) ORDER BY id",
            "SELECT count(*) FROM a CROSS JOIN b",
            "SELECT a.x,b.y FROM a CROSS JOIN b ORDER BY a.x,b.y",
            // NATURAL join uses the common 'id' column
            "SELECT * FROM a NATURAL JOIN b ORDER BY id",
            "SELECT id,x,y FROM a NATURAL LEFT JOIN b ORDER BY id",
            // 3-table chain
            "SELECT a.id,x,y,z FROM a JOIN b ON a.id=b.id JOIN c ON b.id=c.id ORDER BY a.id",
            "SELECT a.id,x,y,z FROM a LEFT JOIN b ON a.id=b.id LEFT JOIN c ON a.id=c.id ORDER BY a.id",
            // self-join
            "SELECT t1.x, t2.x FROM a t1 JOIN a t2 ON t1.id=t2.id+1 ORDER BY t1.id",
            // ON-vs-WHERE distinction for LEFT JOIN
            "SELECT a.id,y FROM a LEFT JOIN b ON a.id=b.id AND b.y='b3' ORDER BY a.id",
            "SELECT a.id,y FROM a LEFT JOIN b ON a.id=b.id WHERE b.y='b3' OR b.y IS NULL ORDER BY a.id",
            // aggregate over LEFT JOIN counting NULLs correctly
            "SELECT a.id, count(b.id) FROM a LEFT JOIN b ON a.id=b.id GROUP BY a.id ORDER BY a.id",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(diffs.is_empty(), "{} JOIN-semantics divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
