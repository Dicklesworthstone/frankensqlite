#![recursion_limit = "512"]

//! bd-01qa9 regression: INSERT OR ABORT (and the default/implicit ABORT of a
//! plain multi-row INSERT) must undo EVERY row the statement already inserted
//! when a later row violates a constraint — statement atomicity. Frank formerly
//! kept the earlier rows (OR FAIL semantics). This asserts the fixed behavior
//! against the rusqlite oracle. OR FAIL must still preserve earlier rows.

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

// bd-01qa9: a multi-row VALUES INSERT that violates a constraint on a later row
// must, under ABORT (the default) and OR ABORT, undo the rows it already inserted
// — statement atomicity. Fixed by forcing a statement savepoint for a batched
// autocommit multi-row INSERT (the skip-savepoint optimization is single-row-only)
// so the failed statement is rolled back — buffers included — before the retained
// batch of prior good writes is flushed. OR FAIL still preserves.
#[test]
fn insert_abort_statement_atomicity_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // explicit OR ABORT: multi-row insert, 2nd row conflicts -> ALL undone
        for s in ["CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO a VALUES (5,'five')"] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT OR ABORT INTO a VALUES (10,'ten'),(5,'dup'),(11,'eleven')").await;
        check("or abort", fq(&f, "SELECT id,v FROM a ORDER BY id").await,
              rq(&r, "SELECT id,v FROM a ORDER BY id"), &mut diffs);

        // implicit ABORT (plain INSERT) — same statement rollback
        for s in ["CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO b VALUES (5,'five')"] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT INTO b VALUES (12,'twelve'),(5,'dup'),(13,'thirteen')").await;
        check("implicit abort", fq(&f, "SELECT id,v FROM b ORDER BY id").await,
              rq(&r, "SELECT id,v FROM b ORDER BY id"), &mut diffs);

        // UNIQUE (not just PK) violation partway also rolls the statement back
        for s in ["CREATE TABLE c(id INTEGER PRIMARY KEY, u TEXT UNIQUE)", "INSERT INTO c VALUES (1,'x')"] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT INTO c VALUES (2,'y'),(3,'x'),(4,'z')").await; // u='x' dup at row 2
        check("unique abort", fq(&f, "SELECT id,u FROM c ORDER BY id").await,
              rq(&r, "SELECT id,u FROM c ORDER BY id"), &mut diffs);

        // NOT NULL violation partway
        for s in ["CREATE TABLE d(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)"] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT INTO d VALUES (1,10),(2,NULL),(3,30)").await; // row 2 NOT NULL
        check("notnull abort", fq(&f, "SELECT id,v FROM d ORDER BY id").await,
              rq(&r, "SELECT id,v FROM d ORDER BY id"), &mut diffs);

        // OR FAIL must STILL preserve the earlier row (regression guard the other way)
        for s in ["CREATE TABLE e(id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO e VALUES (5,'five')"] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT OR FAIL INTO e VALUES (20,'twenty'),(5,'dup'),(21,'x')").await;
        check("or fail preserves", fq(&f, "SELECT id,v FROM e ORDER BY id").await,
              rq(&r, "SELECT id,v FROM e ORDER BY id"), &mut diffs);

        // ABORT with the conflicting row committed BEFORE the multi-row INSERT
        // (autocommit): the failed statement's earlier rows are undone, the prior
        // committed row survives.
        for s in ["CREATE TABLE g(id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO g VALUES (1,'a')"] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT INTO g VALUES (2,'b'),(1,'dup'),(3,'c')").await; // (1,'dup') conflicts
        check("abort keeps prior committed", fq(&f, "SELECT id,v FROM g ORDER BY id").await,
              rq(&r, "SELECT id,v FROM g ORDER BY id"), &mut diffs);

        // NOTE: the ABORT-inside-an-explicit-BEGIN variant (a prior in-txn INSERT,
        // then a multi-row INSERT that conflicts with it) is tracked separately in
        // bd-q2bju — a distinct explicit-txn bug where the multi-row INSERT does
        // not see the prior in-txn row for conflict detection. Not asserted here.

        assert!(diffs.is_empty(), "{} INSERT-ABORT atomicity divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
