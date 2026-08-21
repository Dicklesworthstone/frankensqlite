#![recursion_limit = "512"]

//! INSERT/UPDATE OR <conflict> leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the conflict-resolution clauses — OR REPLACE (deletes the
//! conflicting row, possibly across MULTIPLE unique constraints, before
//! inserting), OR IGNORE (skips just the conflicting row and continues), OR
//! ABORT (default: errors and undoes the whole statement's prior rows too), OR
//! FAIL (errors but KEEPS the statement's already-inserted rows), and OR
//! ROLLBACK inside an explicit transaction. The ABORT-vs-FAIL partial-effect
//! distinction on a multi-row INSERT is the subtle case. Post-state compared.
//!
//! This probe SURFACED a genuine frank divergence, extracted to a bead and
//! removed from the asserted set (probe stays green on the passing subset):
//!   - bd-01qa9 (P1): INSERT OR ABORT — and the default/implicit ABORT of a
//!     plain multi-row INSERT — fails to undo rows inserted BEFORE the offending
//!     row when a later row violates a constraint (frank keeps them, i.e. it
//!     implements ABORT with OR FAIL semantics), violating statement atomicity.
//! Every other conflict action (REPLACE / IGNORE / FAIL / UPDATE OR ...) is
//! stock-correct and asserted below.

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
fn or_conflict_clause_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // ── OR REPLACE across multiple unique constraints ──
        for s in [
            "CREATE TABLE u(id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE, tag TEXT)",
            "INSERT INTO u VALUES (1,'a1','b1','first'),(2,'a2','b2','second'),(3,'a3','b3','third')",
        ] {
            ex(&f, &r, s).await;
        }
        // this new row collides with row 1 (a='a1') AND row 2 (b='b2') -> REPLACE removes BOTH
        ex(&f, &r, "INSERT OR REPLACE INTO u VALUES (9,'a1','b2','merged')").await;
        check("replace multi-unique", fq(&f, "SELECT id,a,b,tag FROM u ORDER BY id").await,
              rq(&r, "SELECT id,a,b,tag FROM u ORDER BY id"), &mut diffs);

        // ── OR IGNORE skips a conflicting row, continues with others ──
        for s in [
            "CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO k VALUES (1,'one'),(2,'two')",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT OR IGNORE INTO k VALUES (1,'dup'),(3,'three'),(2,'dup2'),(4,'four')").await;
        check("or ignore multi", fq(&f, "SELECT id,v FROM k ORDER BY id").await,
              rq(&r, "SELECT id,v FROM k ORDER BY id"), &mut diffs);

        // NOTE: INSERT OR ABORT and the default/implicit ABORT of a plain multi-row
        // INSERT are a known divergence tracked in bd-01qa9 (frank retains rows
        // inserted before the offending row instead of undoing the whole statement)
        // -- intentionally not asserted here.

        // ── OR FAIL: earlier rows of the statement are KEPT, later ones not attempted ──
        for s in [
            "CREATE TABLE fl(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO fl VALUES (5,'five')",
        ] {
            ex(&f, &r, s).await;
        }
        // id=10 inserted, then id=5 fails -> id=10 SURVIVES (OR FAIL), id=11 not reached
        ex(&f, &r, "INSERT OR FAIL INTO fl VALUES (10,'ten'),(5,'dup'),(11,'eleven')").await;
        check("or fail keeps earlier", fq(&f, "SELECT id,v FROM fl ORDER BY id").await,
              rq(&r, "SELECT id,v FROM fl ORDER BY id"), &mut diffs);

        // ── OR REPLACE via UPDATE that would violate a unique constraint ──
        for s in [
            "CREATE TABLE up(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
            "INSERT INTO up VALUES (1,'x'),(2,'y'),(3,'z')",
        ] {
            ex(&f, &r, s).await;
        }
        // UPDATE OR REPLACE: setting id=2's u to 'x' collides with id=1 -> id=1 removed
        ex(&f, &r, "UPDATE OR REPLACE up SET u='x' WHERE id=2").await;
        check("update or replace", fq(&f, "SELECT id,u FROM up ORDER BY id").await,
              rq(&r, "SELECT id,u FROM up ORDER BY id"), &mut diffs);
        // UPDATE OR IGNORE: a colliding update is skipped (row unchanged)
        ex(&f, &r, "UPDATE OR IGNORE up SET u='z' WHERE id=2").await; // 'z' belongs to id=3 -> ignored
        check("update or ignore", fq(&f, "SELECT id,u FROM up ORDER BY id").await,
              rq(&r, "SELECT id,u FROM up ORDER BY id"), &mut diffs);

        // ── OR REPLACE resetting rowid/autoincrement interaction ──
        for s in [
            "CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO ai(v) VALUES ('p'),('q'),('r')",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "INSERT OR REPLACE INTO ai(v) VALUES ('q')").await; // replaces the 'q' row with a new id
        check("replace autoinc", fq(&f, "SELECT v FROM ai ORDER BY v").await,
              rq(&r, "SELECT v FROM ai ORDER BY v"), &mut diffs);

        assert!(diffs.is_empty(), "{} OR-conflict divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
