#![recursion_limit = "512"]

//! Type-affinity comparison leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over how a column's DECLARED affinity coerces the OTHER operand in a
//! comparison — an INTEGER/NUMERIC/REAL column compared to a text literal
//! coerces the text to a number (so `iaff = '10'` matches a stored 10), a TEXT
//! column compared to a numeric literal coerces the number to text
//! (`taff = 10` matches a stored '10'), and a column with NO affinity (BLOB
//! affinity, declared type e.g. `BLOB`/none) applies NO coercion (so 10 and '10'
//! stay distinct storage classes). Also index-backed lookups must honour the
//! same affinity. Post-query result sets compared. Pass = coverage keeper.

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
fn affinity_comparison_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            // iaff=INTEGER affinity, taff=TEXT affinity, raff=REAL, naff=NUMERIC, baff=BLOB(no) affinity
            "CREATE TABLE t(id INTEGER PRIMARY KEY, iaff INTEGER, taff TEXT, raff REAL, naff NUMERIC, baff BLOB)",
            "INSERT INTO t VALUES (1, 10, '10', 10, 10, 10)",
            "INSERT INTO t VALUES (2, 20, '20', 20.0, 20, '20')",
            "INSERT INTO t VALUES (3, 5, 'abc', 5.5, 5, x'3130')",  // x'3130' = bytes '10'
            "CREATE INDEX ix_iaff ON t(iaff)",
            "CREATE INDEX ix_taff ON t(taff)",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let mut check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // INTEGER-affinity column vs a text literal: text coerces to number -> matches stored 10
        check("iaff = text literal", fq(&f, "SELECT id FROM t WHERE iaff = '10' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE iaff = '10' ORDER BY id"), &mut diffs);
        check("iaff = int literal", fq(&f, "SELECT id FROM t WHERE iaff = 10 ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE iaff = 10 ORDER BY id"), &mut diffs);
        check("iaff range text", fq(&f, "SELECT id FROM t WHERE iaff > '9' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE iaff > '9' ORDER BY id"), &mut diffs);
        // TEXT-affinity column vs a numeric literal: number coerces to text -> matches stored '10'
        check("taff = int literal", fq(&f, "SELECT id FROM t WHERE taff = 10 ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE taff = 10 ORDER BY id"), &mut diffs);
        check("taff = text literal", fq(&f, "SELECT id FROM t WHERE taff = '10' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE taff = '10' ORDER BY id"), &mut diffs);
        // REAL-affinity column
        check("raff = int literal", fq(&f, "SELECT id FROM t WHERE raff = 20 ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE raff = 20 ORDER BY id"), &mut diffs);
        check("raff = text literal", fq(&f, "SELECT id FROM t WHERE raff = '10' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE raff = '10' ORDER BY id"), &mut diffs);
        // NUMERIC-affinity column
        check("naff = text literal", fq(&f, "SELECT id FROM t WHERE naff = '20' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE naff = '20' ORDER BY id"), &mut diffs);
        // BLOB/no-affinity column: NO coercion -> int 10 and text '10' stay distinct
        check("baff = int literal", fq(&f, "SELECT id FROM t WHERE baff = 10 ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE baff = 10 ORDER BY id"), &mut diffs);
        check("baff = text literal", fq(&f, "SELECT id FROM t WHERE baff = '20' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE baff = '20' ORDER BY id"), &mut diffs);
        // typeof of the stored values reflects applied column affinity at insert time
        check("typeof stored", fq(&f, "SELECT id, typeof(iaff), typeof(taff), typeof(raff), typeof(naff), typeof(baff) FROM t ORDER BY id").await,
              rq(&r, "SELECT id, typeof(iaff), typeof(taff), typeof(raff), typeof(naff), typeof(baff) FROM t ORDER BY id"), &mut diffs);
        // column-to-column comparison: affinity of neither side is a literal, so both keep type
        check("iaff = taff col-col", fq(&f, "SELECT id FROM t WHERE iaff = taff ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE iaff = taff ORDER BY id"), &mut diffs);
        // comparison inside IN honours affinity of the LHS column
        check("iaff IN text list", fq(&f, "SELECT id FROM t WHERE iaff IN ('10','20') ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE iaff IN ('10','20') ORDER BY id"), &mut diffs);
        check("taff IN int list", fq(&f, "SELECT id FROM t WHERE taff IN (10,20) ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE taff IN (10,20) ORDER BY id"), &mut diffs);
        // an expression column loses affinity (iaff+0 has no affinity)
        check("expr loses affinity", fq(&f, "SELECT id FROM t WHERE (iaff+0) = '10' ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE (iaff+0) = '10' ORDER BY id"), &mut diffs);
        // CAST forces the comparison type
        check("cast in comparison", fq(&f, "SELECT id FROM t WHERE CAST(taff AS INTEGER) = 10 ORDER BY id").await,
              rq(&r, "SELECT id FROM t WHERE CAST(taff AS INTEGER) = 10 ORDER BY id"), &mut diffs);
        // ORDER BY on a mixed-type BLOB-affinity column (storage-class order)
        check("orderby no-affinity col", fq(&f, "SELECT id, baff FROM t ORDER BY baff, id").await,
              rq(&r, "SELECT id, baff FROM t ORDER BY baff, id"), &mut diffs);

        assert!(diffs.is_empty(), "{} affinity-comparison divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
