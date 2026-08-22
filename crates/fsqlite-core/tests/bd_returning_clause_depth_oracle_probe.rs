#![recursion_limit = "512"]

//! RETURNING-clause depth leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over the RETURNING clause (3.35+) on INSERT / UPDATE / DELETE — an explicit
//! column list, RETURNING *, computed expressions and aliases, RETURNING that
//! references both old-and-new via the post-image, multi-row DML returning one
//! row per affected row, RETURNING on INSERT ... ON CONFLICT DO UPDATE (returns
//! the updated row), and RETURNING rowid / last_insert_rowid-style values. Since
//! RETURNING row order is not guaranteed, result sets are SORTED before compare.
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

// Collect + SORT rows (RETURNING order is unspecified).
async fn fq_sorted(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut rows = match conn.query(sql).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect::<Vec<_>>()).collect::<Vec<_>>(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    };
    rows.sort();
    rows
}
fn rq_sorted(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut rows = {
        let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
        let n = st.column_count();
        match st.query_map([], |row| {
            Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
        }) {
            Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
            Err(_) => vec![vec!["ERR".to_owned()]],
        }
    };
    rows.sort();
    rows
}
async fn exf(conn: &Connection, sql: &str) { let _ = conn.execute(sql).await; }
fn exr(conn: &rusqlite::Connection, sql: &str) { let _ = conn.execute(sql, []); }

#[test]
fn returning_clause_depth_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
        ] {
            exf(&f, s).await; exr(&r, s);
        }
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // INSERT ... RETURNING explicit columns
        check("insert returning cols",
            fq_sorted(&f, "INSERT INTO t(id,a,b) VALUES (1,10,'x') RETURNING id, a, b").await,
            rq_sorted(&r, "INSERT INTO t(id,a,b) VALUES (1,10,'x') RETURNING id, a, b"), &mut diffs);
        // INSERT ... RETURNING * and an expression/alias
        check("insert returning star+expr",
            fq_sorted(&f, "INSERT INTO t(id,a,b) VALUES (2,20,'y') RETURNING *, a*2 AS dbl").await,
            rq_sorted(&r, "INSERT INTO t(id,a,b) VALUES (2,20,'y') RETURNING *, a*2 AS dbl"), &mut diffs);
        // multi-row INSERT ... RETURNING (one row per inserted row)
        check("multi-row insert returning",
            fq_sorted(&f, "INSERT INTO t(id,a,b) VALUES (3,30,'p'),(4,40,'q'),(5,50,'r') RETURNING id, a").await,
            rq_sorted(&r, "INSERT INTO t(id,a,b) VALUES (3,30,'p'),(4,40,'q'),(5,50,'r') RETURNING id, a"), &mut diffs);

        // UPDATE ... RETURNING post-image + computed expr
        check("update returning post-image",
            fq_sorted(&f, "UPDATE t SET a=a+100 WHERE id<=2 RETURNING id, a, a-100 AS was").await,
            rq_sorted(&r, "UPDATE t SET a=a+100 WHERE id<=2 RETURNING id, a, a-100 AS was"), &mut diffs);
        // UPDATE ... RETURNING * touching all rows
        check("update returning star",
            fq_sorted(&f, "UPDATE t SET b=b||'!' RETURNING *").await,
            rq_sorted(&r, "UPDATE t SET b=b||'!' RETURNING *"), &mut diffs);

        // DELETE ... RETURNING the deleted rows
        check("delete returning",
            fq_sorted(&f, "DELETE FROM t WHERE id IN (4,5) RETURNING id, b").await,
            rq_sorted(&r, "DELETE FROM t WHERE id IN (4,5) RETURNING id, b"), &mut diffs);

        // INSERT ... ON CONFLICT DO UPDATE ... RETURNING (returns the updated row)
        check("upsert returning",
            fq_sorted(&f, "INSERT INTO t(id,a,b) VALUES (1,1,'z') ON CONFLICT(id) DO UPDATE SET a=excluded.a RETURNING id, a").await,
            rq_sorted(&r, "INSERT INTO t(id,a,b) VALUES (1,1,'z') ON CONFLICT(id) DO UPDATE SET a=excluded.a RETURNING id, a"), &mut diffs);

        // RETURNING with a function over the row + typeof
        check("returning function",
            fq_sorted(&f, "UPDATE t SET a=a WHERE id=3 RETURNING id, abs(a), typeof(b), length(b)").await,
            rq_sorted(&r, "UPDATE t SET a=a WHERE id=3 RETURNING id, abs(a), typeof(b), length(b)"), &mut diffs);

        // final table state sanity (ordered)
        check("final state",
            fq_sorted(&f, "SELECT id,a,b FROM t").await,
            rq_sorted(&r, "SELECT id,a,b FROM t"), &mut diffs);

        assert!(diffs.is_empty(), "{} RETURNING-clause divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
