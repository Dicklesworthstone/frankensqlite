#![recursion_limit = "512"]

//! bd-7s1zz: an unqualified USING/NATURAL column inside a correlated subquery
//! in the join's WHERE names the join's coalesced column, as in the outer
//! query. In a self-join both sides contribute a column with that name, so
//! the outer-reference lookup found it ambiguous and left it unsubstituted,
//! and the nested statement failed with "no such column". Pinned
//! differentially against rusqlite.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let frows = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"));
    let ff: Vec<Vec<String>> = frows
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();

    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(ff, rr, "bd-7s1zz mismatch on `{sql}`");
}

#[test]
fn unqualified_using_column_resolves_inside_correlated_subqueries() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for ddl in [
            "CREATE TABLE a(z INTEGER, x TEXT)",
            "INSERT INTO a VALUES (1,'a1'),(2,'a2'),(NULL,'an'),(4,'a4')",
            "CREATE TABLE b(z INTEGER, y TEXT)",
            "INSERT INTO b VALUES (1,'b1'),(3,'b3'),(NULL,'bn'),(4,'b4')",
            "CREATE TABLE k(i INTEGER, t TEXT, z INTEGER)",
            "INSERT INTO k VALUES (1,'a1',10),(3,'30',30),(4,'40',40),(NULL,'50',50)",
        ] {
            f.execute(ddl).await.unwrap();
            r.execute(ddl, []).unwrap();
        }
        for sql in [
            // Self-joins: both sides contribute `z`.
            "SELECT a.x, p.x FROM a JOIN a AS p USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
            "SELECT a.x, p.x FROM a LEFT JOIN a AS p USING (z) WHERE NOT EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
            "SELECT a.x, p.x FROM a FULL JOIN a AS p USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
            "SELECT a.x, p.x FROM a JOIN a AS p USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z OR k.t = p.x) ORDER BY 1, 2",
            "SELECT q.x, p.x FROM a AS q JOIN a AS p USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
            "SELECT a.x FROM a NATURAL JOIN a AS p WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1",
            "SELECT a.x, p.x FROM a JOIN a AS p USING (z) WHERE (SELECT k.t FROM k WHERE k.i = z) IS NOT NULL ORDER BY 1, 2",
            "SELECT a.x, p.x FROM a JOIN a AS p USING (z) WHERE z IN (SELECT i FROM k WHERE k.i = z) ORDER BY 1, 2",
            // The subquery's own `z` still shadows the join's column.
            "SELECT a.x, p.x FROM a JOIN a AS p USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE z = 40) ORDER BY 1, 2",
            // Distinct tables, including outer joins that coalesce NULLs.
            "SELECT x, y FROM a JOIN b USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
            "SELECT x, y FROM a RIGHT JOIN b USING (z) WHERE EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
            "SELECT x, y FROM a FULL JOIN b USING (z) WHERE NOT EXISTS (SELECT 1 FROM k WHERE k.i = z) ORDER BY 1, 2",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
}
