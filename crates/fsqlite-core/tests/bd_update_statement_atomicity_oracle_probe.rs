#![recursion_limit = "512"]

//! UPDATE statement-atomicity leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite — the UPDATE-side analog of bd-01qa9 (INSERT OR ABORT retaining
//! earlier rows). A multi-row UPDATE whose Nth row violates a UNIQUE / NOT NULL
//! / CHECK constraint must, under ABORT (the default) and OR ABORT, undo EVERY
//! row the statement already changed; under OR FAIL the earlier rows are kept.
//! We drive a multi-row UPDATE that collides partway and compare the surviving
//! table state. Also covers OR IGNORE (skip the offending row, keep the rest)
//! and OR REPLACE. Pass = coverage keeper; a mismatch is a leaf divergence.

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
fn update_statement_atomicity_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // ── default (implicit ABORT): multi-row UPDATE colliding partway undoes ALL ──
        for s in [
            "CREATE TABLE a(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
            "INSERT INTO a VALUES (1,'p'),(2,'q'),(3,'r')",
        ] {
            ex(&f, &r, s).await;
        }
        // set u='Z' on ids 1 and 2: id=1 -> 'Z' ok, id=2 -> 'Z' collides -> ABORT undoes id=1 too
        ex(&f, &r, "UPDATE a SET u='Z' WHERE id IN (1,2)").await;
        check("implicit abort undoes update", fq(&f, "SELECT id,u FROM a ORDER BY id").await,
              rq(&r, "SELECT id,u FROM a ORDER BY id"), &mut diffs);

        // ── explicit OR ABORT: same rollback-the-statement behavior ──
        for s in [
            "CREATE TABLE b(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
            "INSERT INTO b VALUES (1,'p'),(2,'q'),(3,'r')",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE OR ABORT b SET u='Z' WHERE id IN (1,2)").await;
        check("or abort undoes update", fq(&f, "SELECT id,u FROM b ORDER BY id").await,
              rq(&r, "SELECT id,u FROM b ORDER BY id"), &mut diffs);

        // ── NOT NULL violation mid-statement also rolls back under ABORT ──
        for s in [
            "CREATE TABLE c(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, w INTEGER)",
            "INSERT INTO c VALUES (1,10,100),(2,20,NULL),(3,30,300)",
        ] {
            ex(&f, &r, s).await;
        }
        // set v = w for all: id=1 v=100 ok, id=2 v=NULL violates NOT NULL -> undo id=1
        ex(&f, &r, "UPDATE c SET v = w").await;
        check("notnull abort undoes", fq(&f, "SELECT id,v FROM c ORDER BY id").await,
              rq(&r, "SELECT id,v FROM c ORDER BY id"), &mut diffs);

        // ── OR FAIL: earlier rows of the UPDATE are KEPT ──
        for s in [
            "CREATE TABLE d(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
            "INSERT INTO d VALUES (1,'p'),(2,'q'),(3,'r')",
        ] {
            ex(&f, &r, s).await;
        }
        // ORDER of rowids: id=1 -> 'Z' kept, id=2 -> 'Z' fails; OR FAIL keeps id=1's change
        ex(&f, &r, "UPDATE OR FAIL d SET u='Z' WHERE id IN (1,2)").await;
        check("or fail keeps earlier update", fq(&f, "SELECT id,u FROM d ORDER BY id").await,
              rq(&r, "SELECT id,u FROM d ORDER BY id"), &mut diffs);

        // ── OR IGNORE: the offending row is skipped, others updated ──
        for s in [
            "CREATE TABLE e(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
            "INSERT INTO e VALUES (1,'p'),(2,'q'),(3,'r')",
        ] {
            ex(&f, &r, s).await;
        }
        // set u='q' on ids 1 and 3: id=1 collides with existing 'q' (id=2) -> ignored;
        // id=3 -> 'q' also collides -> ignored; net: nothing changes
        ex(&f, &r, "UPDATE OR IGNORE e SET u='q' WHERE id IN (1,3)").await;
        check("or ignore skips offending", fq(&f, "SELECT id,u FROM e ORDER BY id").await,
              rq(&r, "SELECT id,u FROM e ORDER BY id"), &mut diffs);

        // ── OR REPLACE on UPDATE: the collided-with row is deleted ──
        for s in [
            "CREATE TABLE g(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
            "INSERT INTO g VALUES (1,'p'),(2,'q'),(3,'r')",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE OR REPLACE g SET u='q' WHERE id=1").await; // collides w/ id=2 -> id=2 removed
        check("or replace update", fq(&f, "SELECT id,u FROM g ORDER BY id").await,
              rq(&r, "SELECT id,u FROM g ORDER BY id"), &mut diffs);

        // ── successful multi-row UPDATE (no conflict) still works ──
        for s in [
            "CREATE TABLE h(id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO h VALUES (1,10),(2,20),(3,30)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE h SET n=n+1 WHERE id IN (1,2,3)").await;
        check("clean multi-row update", fq(&f, "SELECT id,n FROM h ORDER BY id").await,
              rq(&r, "SELECT id,n FROM h ORDER BY id"), &mut diffs);

        assert!(diffs.is_empty(), "{} UPDATE-atomicity divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
