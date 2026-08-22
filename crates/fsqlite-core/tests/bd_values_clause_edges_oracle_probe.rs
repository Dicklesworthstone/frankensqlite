#![recursion_limit = "512"]

//! VALUES-clause / VALUES-as-table leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the VALUES table-constructor in expression position — a bare
//! `VALUES` row set, VALUES in a FROM subquery (default column names
//! `column1`/`column2`…), VALUES as a WITH CTE with explicit column names,
//! ORDER BY / LIMIT over a VALUES set, VALUES feeding INSERT ... SELECT, row
//! affinity mixing (int vs text columns), IN (VALUES ...) membership, and
//! row-value = comparison against a single VALUES row. Ordered result sets and
//! post-insert state compared. Pass = coverage keeper; a mismatch is a leaf.

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
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn values_clause_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // bare VALUES row set (multi-row, multi-column)
        check("bare values", fq(&f, "VALUES (1,'a'),(2,'b'),(3,'c')").await,
              rq(&r, "VALUES (1,'a'),(2,'b'),(3,'c')"), &mut diffs);
        // VALUES in FROM subquery -> default names column1/column2
        check("values in from default names", fq(&f, "SELECT column1, column2 FROM (VALUES (10,'x'),(20,'y')) ORDER BY column1").await,
              rq(&r, "SELECT column1, column2 FROM (VALUES (10,'x'),(20,'y')) ORDER BY column1"), &mut diffs);
        // aggregate over a VALUES-derived table via default column1
        check("agg over values", fq(&f, "SELECT sum(column1), count(*) FROM (VALUES (1),(2),(3),(4))").await,
              rq(&r, "SELECT sum(column1), count(*) FROM (VALUES (1),(2),(3),(4))"), &mut diffs);
        // VALUES as WITH CTE with explicit column names
        check("cte values explicit names", fq(&f, "WITH v(a,b) AS (VALUES (1,'p'),(2,'q'),(3,'r')) SELECT a,b FROM v WHERE a>1 ORDER BY a").await,
              rq(&r, "WITH v(a,b) AS (VALUES (1,'p'),(2,'q'),(3,'r')) SELECT a,b FROM v WHERE a>1 ORDER BY a"), &mut diffs);
        // ORDER BY / LIMIT directly on a bare VALUES set (ordinal ordering)
        check("values order limit", fq(&f, "VALUES (3),(1),(4),(1),(5),(9) ORDER BY 1 LIMIT 3").await,
              rq(&r, "VALUES (3),(1),(4),(1),(5),(9) ORDER BY 1 LIMIT 3"), &mut diffs);
        // VALUES joined against a VALUES CTE
        check("cte values join", fq(&f, "WITH a(k,x) AS (VALUES (1,'a1'),(2,'a2')), b(k,y) AS (VALUES (2,'b2'),(3,'b3')) SELECT a.k,x,y FROM a JOIN b ON a.k=b.k ORDER BY a.k").await,
              rq(&r, "WITH a(k,x) AS (VALUES (1,'a1'),(2,'a2')), b(k,y) AS (VALUES (2,'b2'),(3,'b3')) SELECT a.k,x,y FROM a JOIN b ON a.k=b.k ORDER BY a.k"), &mut diffs);
        // IN (VALUES ...) membership
        check("in values", fq(&f, "SELECT x FROM (VALUES (1),(2),(3),(4),(5)) v(x) WHERE x IN (VALUES (2),(4)) ORDER BY x").await,
              rq(&r, "SELECT x FROM (VALUES (1),(2),(3),(4),(5)) v(x) WHERE x IN (VALUES (2),(4)) ORDER BY x"), &mut diffs);
        // UNION of two VALUES sets
        check("union of values", fq(&f, "VALUES (1),(2) UNION VALUES (2),(3) ORDER BY 1").await,
              rq(&r, "VALUES (1),(2) UNION VALUES (2),(3) ORDER BY 1"), &mut diffs);
        // heterogeneous column types across rows (no declared affinity -> raw storage classes)
        check("values mixed types", fq(&f, "SELECT column1 FROM (VALUES (1),('two'),(3.5),(NULL)) ORDER BY column1").await,
              rq(&r, "SELECT column1 FROM (VALUES (1),('two'),(3.5),(NULL)) ORDER BY column1"), &mut diffs);
        // row-value equality against a single VALUES row
        check("row-value = values", fq(&f, "SELECT (2,'b') = (SELECT column1,column2 FROM (VALUES (2,'b')))").await,
              rq(&r, "SELECT (2,'b') = (SELECT column1,column2 FROM (VALUES (2,'b')))"), &mut diffs);
        // scalar subquery selecting from VALUES
        check("scalar subquery values", fq(&f, "SELECT (SELECT max(column1) FROM (VALUES (7),(2),(9),(4)))").await,
              rq(&r, "SELECT (SELECT max(column1) FROM (VALUES (7),(2),(9),(4)))"), &mut diffs);

        // VALUES feeding INSERT (multi-row) then declared-affinity coercion
        ex(&f, &r, "CREATE TABLE dst(id INTEGER, label TEXT, amount REAL)").await;
        ex(&f, &r, "INSERT INTO dst VALUES (1,'one','1'),(2,42,'2.5'),(3,'three',3)").await;
        check("after values insert affinity", fq(&f, "SELECT id,label,amount FROM dst ORDER BY id").await,
              rq(&r, "SELECT id,label,amount FROM dst ORDER BY id"), &mut diffs);
        // INSERT ... SELECT from a VALUES-derived table
        ex(&f, &r, "INSERT INTO dst(id,label,amount) SELECT column1, column2, column3 FROM (VALUES (4,'four',4.0),(5,'five',5.5))").await;
        check("after insert-select values", fq(&f, "SELECT id,label,amount FROM dst ORDER BY id").await,
              rq(&r, "SELECT id,label,amount FROM dst ORDER BY id"), &mut diffs);

        assert!(diffs.is_empty(), "{} VALUES-clause divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
