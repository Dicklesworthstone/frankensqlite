#![recursion_limit = "512"]

//! CHECK-constraint enforcement-depth leaf-hunt (pane af49, 2026-08-21): frank
//! vs rusqlite over CHECK constraints — column-level and table-level CHECKs with
//! comparisons, IN, LIKE, and function calls; enforcement on INSERT *and*
//! UPDATE; a multi-column table CHECK; and the subtle three-valued rule that a
//! CHECK is satisfied unless it evaluates to FALSE (so NULL and TRUE both pass).
//! Each mutation is attempted on both engines and the surviving/rejected state
//! is compared. Pass = coverage keeper; a mismatch is a leaf divergence.

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
fn check_constraint_depth_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(\
               id INTEGER PRIMARY KEY, \
               score INTEGER CHECK (score >= 0), \
               grade TEXT CHECK (grade IN ('A','B','C','D','F')), \
               code TEXT CHECK (code LIKE 'X%'), \
               name TEXT CHECK (length(name) <= 5), \
               lo INTEGER, hi INTEGER, \
               CHECK (lo IS NULL OR hi IS NULL OR lo <= hi))",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let mut check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // valid insert passes
        ex(&f, &r, "INSERT INTO t VALUES (1,10,'A','Xhi','abc',1,5)").await;
        // NULL satisfies CHECK (three-valued: only FALSE rejects)
        ex(&f, &r, "INSERT INTO t VALUES (2,NULL,NULL,NULL,NULL,NULL,NULL)").await;
        // each of the following violates exactly one CHECK -> rejected on both
        ex(&f, &r, "INSERT INTO t VALUES (3,-1,'A','Xhi','abc',1,5)").await;     // score<0
        ex(&f, &r, "INSERT INTO t VALUES (4,10,'Z','Xhi','abc',1,5)").await;     // grade not in set
        ex(&f, &r, "INSERT INTO t VALUES (5,10,'A','Yhi','abc',1,5)").await;     // code not LIKE X%
        ex(&f, &r, "INSERT INTO t VALUES (6,10,'A','Xhi','toolong',1,5)").await; // name too long
        ex(&f, &r, "INSERT INTO t VALUES (7,10,'A','Xhi','abc',9,2)").await;     // lo>hi table CHECK
        // a row with lo set but hi NULL -> table CHECK passes (NULL branch)
        ex(&f, &r, "INSERT INTO t VALUES (8,0,'F','X','ok',7,NULL)").await;

        check("after inserts", fq(&f, "SELECT id,score,grade,code,name,lo,hi FROM t ORDER BY id").await,
              rq(&r, "SELECT id,score,grade,code,name,lo,hi FROM t ORDER BY id"), &mut diffs);

        // ── CHECK enforced on UPDATE too ──
        ex(&f, &r, "UPDATE t SET score=-5 WHERE id=1").await;   // rejected
        check("update violate score", fq(&f, "SELECT id,score FROM t WHERE id=1").await,
              rq(&r, "SELECT id,score FROM t WHERE id=1"), &mut diffs);
        ex(&f, &r, "UPDATE t SET score=99 WHERE id=1").await;   // allowed
        check("update valid score", fq(&f, "SELECT id,score FROM t WHERE id=1").await,
              rq(&r, "SELECT id,score FROM t WHERE id=1"), &mut diffs);
        ex(&f, &r, "UPDATE t SET grade='Q' WHERE id=1").await;  // rejected (not in set)
        check("update violate grade", fq(&f, "SELECT id,grade FROM t WHERE id=1").await,
              rq(&r, "SELECT id,grade FROM t WHERE id=1"), &mut diffs);
        // UPDATE that would violate the table-level CHECK
        ex(&f, &r, "UPDATE t SET lo=100, hi=1 WHERE id=1").await; // rejected (lo>hi)
        check("update violate table check", fq(&f, "SELECT id,lo,hi FROM t WHERE id=1").await,
              rq(&r, "SELECT id,lo,hi FROM t WHERE id=1"), &mut diffs);
        // UPDATE setting a value to NULL satisfies the table CHECK
        ex(&f, &r, "UPDATE t SET hi=NULL WHERE id=1").await;    // allowed (NULL branch)
        check("update null satisfies check", fq(&f, "SELECT id,lo,hi FROM t WHERE id=1").await,
              rq(&r, "SELECT id,lo,hi FROM t WHERE id=1"), &mut diffs);

        // final full state + count
        check("final state", fq(&f, "SELECT id,score,grade,code,name,lo,hi FROM t ORDER BY id").await,
              rq(&r, "SELECT id,score,grade,code,name,lo,hi FROM t ORDER BY id"), &mut diffs);
        check("row count", fq(&f, "SELECT count(*) FROM t").await,
              rq(&r, "SELECT count(*) FROM t"), &mut diffs);

        assert!(diffs.is_empty(), "{} CHECK-constraint divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
