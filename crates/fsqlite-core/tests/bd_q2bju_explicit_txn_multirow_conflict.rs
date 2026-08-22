#![recursion_limit = "512"]

//! bd-q2bju / bd-01qa9 (explicit-txn facet): inside an explicit `BEGIN`, a
//! multi-row VALUES INSERT must SEE rows written by a PRIOR statement in the same
//! transaction for conflict detection, and its partial rows must be rolled back on
//! an ABORT (statement atomicity). Frank formerly skipped the internal statement
//! savepoint for the whole in-txn INSERT (bd-pktso's skip-optimization, which is
//! only safe for a SINGLE-ROW direct insert), so a multi-row INSERT neither
//! flushed the prior in-txn write before its conflict check (missed the PK
//! conflict, inserting a duplicate) nor undid its partial rows on ABORT. Fixed by
//! gating bd-pktso's skip to the single-row direct lane. Asserted vs rusqlite.

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
fn explicit_txn_multirow_insert_sees_prior_row_and_is_atomic() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE g(id INTEGER PRIMARY KEY, v TEXT)"] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // CASE 1 (bd-q2bju): prior single-row INSERT then a multi-row INSERT whose
        // 2nd tuple duplicates the prior row's PK. Stock aborts the multi-row
        // INSERT on the conflict; the prior row survives (and, since the whole
        // multi-row statement is atomic, NONE of (2,b),(1,dup),(3,c) is applied).
        ex(&f, &r, "BEGIN").await;
        ex(&f, &r, "INSERT INTO g VALUES (1,'a')").await;
        ex(&f, &r, "INSERT INTO g VALUES (2,'b'),(1,'dup'),(3,'c')").await;
        ex(&f, &r, "COMMIT").await;
        check("after conflict txn", fq(&f, "SELECT id,v FROM g ORDER BY id,v").await,
              rq(&r, "SELECT id,v FROM g ORDER BY id,v"), &mut diffs);
        // exactly one row with id=1, holding the prior 'a' (no duplicate slipped in)
        check("id=1 rows", fq(&f, "SELECT v FROM g WHERE id=1").await,
              rq(&r, "SELECT v FROM g WHERE id=1"), &mut diffs);

        // CASE 2 (regression): a no-conflict multi-row INSERT after a prior in-txn
        // single-row INSERT must ALL apply (savepoint gating must not drop rows).
        ex(&f, &r, "DELETE FROM g").await;
        ex(&f, &r, "BEGIN").await;
        ex(&f, &r, "INSERT INTO g VALUES (10,'ten')").await;
        ex(&f, &r, "INSERT INTO g VALUES (11,'k'),(12,'l'),(13,'m')").await;
        ex(&f, &r, "COMMIT").await;
        check("no-conflict multi applies", fq(&f, "SELECT id,v FROM g ORDER BY id").await,
              rq(&r, "SELECT id,v FROM g ORDER BY id"), &mut diffs);

        // CASE 3 (bd-01qa9 explicit-txn atomicity across a later good stmt):
        // conflict in the middle statement must not corrupt a subsequent write.
        ex(&f, &r, "DELETE FROM g").await;
        ex(&f, &r, "BEGIN").await;
        ex(&f, &r, "INSERT INTO g VALUES (20,'x')").await;
        ex(&f, &r, "INSERT INTO g VALUES (21,'y'),(20,'dupe'),(22,'z')").await; // aborts
        ex(&f, &r, "INSERT INTO g VALUES (23,'ok')").await;                     // still good
        ex(&f, &r, "COMMIT").await;
        check("atomic mid-abort then good", fq(&f, "SELECT id,v FROM g ORDER BY id").await,
              rq(&r, "SELECT id,v FROM g ORDER BY id"), &mut diffs);

        // CASE 4: multi-row conflict WITHIN a single statement (no prior stmt) —
        // duplicate PK between two tuples of the SAME INSERT — also aborts atomically.
        ex(&f, &r, "DELETE FROM g").await;
        ex(&f, &r, "BEGIN").await;
        ex(&f, &r, "INSERT INTO g VALUES (30,'p'),(31,'q'),(30,'p2'),(32,'r')").await;
        ex(&f, &r, "COMMIT").await;
        check("intra-stmt dup aborts", fq(&f, "SELECT id,v FROM g ORDER BY id").await,
              rq(&r, "SELECT id,v FROM g ORDER BY id"), &mut diffs);

        assert!(diffs.is_empty(), "{} bd-q2bju divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
