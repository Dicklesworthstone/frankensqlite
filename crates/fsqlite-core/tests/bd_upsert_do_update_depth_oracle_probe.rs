#![recursion_limit = "512"]

//! UPSERT / ON CONFLICT DO UPDATE depth leaf-hunt (pane af49, 2026-08-21): frank
//! vs rusqlite over the interaction-heavy corners of upsert — the `excluded`
//! pseudo-table (referencing the would-be-inserted row), a WHERE clause on
//! DO UPDATE (which turns an update into a no-op / keeps the old row without
//! inserting), DO NOTHING, an explicit conflict target on a named unique index
//! vs the PK, a table with TWO independent UNIQUE constraints (conflict picks
//! the matching one), a composite unique conflict target, and a multi-row
//! INSERT where some rows conflict and some don't. Post-state compared.
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
fn upsert_do_update_depth_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let mut check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // ── excluded pseudo-table + accumulation ──
        for s in [
            "CREATE TABLE inv(id INTEGER PRIMARY KEY, qty INTEGER, hits INTEGER DEFAULT 0)",
            "INSERT INTO inv VALUES (1,10,1),(2,20,1)",
        ] {
            ex(&f, &r, s).await;
        }
        // conflict on id=1 -> add excluded.qty to existing qty, bump hits
        ex(&f, &r, "INSERT INTO inv(id,qty) VALUES (1,5) ON CONFLICT(id) DO UPDATE SET qty=inv.qty+excluded.qty, hits=inv.hits+1").await;
        check("excluded accumulate", fq(&f, "SELECT id,qty,hits FROM inv ORDER BY id").await,
              rq(&r, "SELECT id,qty,hits FROM inv ORDER BY id"), &mut diffs);
        // non-conflicting id=3 -> plain insert
        ex(&f, &r, "INSERT INTO inv(id,qty) VALUES (3,30) ON CONFLICT(id) DO UPDATE SET qty=inv.qty+excluded.qty").await;
        check("upsert insert path", fq(&f, "SELECT id,qty,hits FROM inv ORDER BY id").await,
              rq(&r, "SELECT id,qty,hits FROM inv ORDER BY id"), &mut diffs);

        // ── WHERE on DO UPDATE: only update if a predicate holds ──
        // conflict on id=2, but WHERE excluded.qty > inv.qty is false (5 < 20) -> no update, no insert
        ex(&f, &r, "INSERT INTO inv(id,qty) VALUES (2,5) ON CONFLICT(id) DO UPDATE SET qty=excluded.qty WHERE excluded.qty > inv.qty").await;
        check("do-update where false", fq(&f, "SELECT id,qty FROM inv ORDER BY id").await,
              rq(&r, "SELECT id,qty FROM inv ORDER BY id"), &mut diffs);
        // conflict on id=2, WHERE true (100 > 20) -> update applies
        ex(&f, &r, "INSERT INTO inv(id,qty) VALUES (2,100) ON CONFLICT(id) DO UPDATE SET qty=excluded.qty WHERE excluded.qty > inv.qty").await;
        check("do-update where true", fq(&f, "SELECT id,qty FROM inv ORDER BY id").await,
              rq(&r, "SELECT id,qty FROM inv ORDER BY id"), &mut diffs);

        // ── DO NOTHING ──
        ex(&f, &r, "INSERT INTO inv(id,qty) VALUES (1,999) ON CONFLICT(id) DO NOTHING").await;
        check("do nothing keeps old", fq(&f, "SELECT id,qty FROM inv WHERE id=1").await,
              rq(&r, "SELECT id,qty FROM inv WHERE id=1"), &mut diffs);

        // ── two independent UNIQUE constraints: conflict picks the matching column ──
        for s in [
            "CREATE TABLE u(id INTEGER PRIMARY KEY, email TEXT UNIQUE, handle TEXT UNIQUE, cnt INTEGER DEFAULT 0)",
            "INSERT INTO u VALUES (1,'a@x','alice',0),(2,'b@x','bob',0)",
        ] {
            ex(&f, &r, s).await;
        }
        // conflict on email -> bump cnt on the alice row
        ex(&f, &r, "INSERT INTO u(id,email,handle) VALUES (99,'a@x','newh') ON CONFLICT(email) DO UPDATE SET cnt=cnt+1").await;
        check("conflict on email target", fq(&f, "SELECT id,email,handle,cnt FROM u ORDER BY id").await,
              rq(&r, "SELECT id,email,handle,cnt FROM u ORDER BY id"), &mut diffs);
        // conflict on handle -> bump cnt on the bob row
        ex(&f, &r, "INSERT INTO u(id,email,handle) VALUES (98,'z@x','bob') ON CONFLICT(handle) DO UPDATE SET cnt=cnt+10").await;
        check("conflict on handle target", fq(&f, "SELECT id,email,handle,cnt FROM u ORDER BY id").await,
              rq(&r, "SELECT id,email,handle,cnt FROM u ORDER BY id"), &mut diffs);

        // ── named unique index as an explicit conflict target ──
        for s in [
            "CREATE TABLE k(a INTEGER, b INTEGER, note TEXT)",
            "CREATE UNIQUE INDEX uk_ab ON k(a,b)",
            "INSERT INTO k VALUES (1,1,'first'),(1,2,'second')",
        ] {
            ex(&f, &r, s).await;
        }
        // composite conflict target (a,b)
        ex(&f, &r, "INSERT INTO k(a,b,note) VALUES (1,1,'updated') ON CONFLICT(a,b) DO UPDATE SET note=excluded.note").await;
        check("composite unique conflict target", fq(&f, "SELECT a,b,note FROM k ORDER BY a,b").await,
              rq(&r, "SELECT a,b,note FROM k ORDER BY a,b"), &mut diffs);

        // ── target-less ON CONFLICT DO UPDATE is a syntax error (target required for DO UPDATE) ──
        // (frank & stock should both reject) -- compare the rejection as a no-op on state
        for s in [
            "CREATE TABLE m(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO m VALUES (1,1)",
        ] {
            ex(&f, &r, s).await;
        }
        // multi-row upsert: row (1,..) conflicts, row (2,..) inserts
        ex(&f, &r, "INSERT INTO m(id,v) VALUES (1,100),(2,200) ON CONFLICT(id) DO UPDATE SET v=excluded.v").await;
        check("multi-row mixed upsert", fq(&f, "SELECT id,v FROM m ORDER BY id").await,
              rq(&r, "SELECT id,v FROM m ORDER BY id"), &mut diffs);

        assert!(diffs.is_empty(), "{} UPSERT DO-UPDATE divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
