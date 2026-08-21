#![recursion_limit = "512"]

//! WITHOUT ROWID semantics leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over the distinct WITHOUT ROWID storage path (a table stored as a PK-keyed
//! index B-tree rather than an integer-rowid heap) — single-column, composite,
//! and TEXT primary keys; natural PK-ordered scans; that `rowid` is NOT a valid
//! column; PK uniqueness enforcement; UPDATE of a PK column (moves the row);
//! DELETE by PK; UPSERT (ON CONFLICT of the PK); a secondary index on a
//! WITHOUT ROWID table; and range scans over a composite PK. Ordered result
//! sets / post-state compared. Pass = coverage keeper; a mismatch is a leaf.

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
fn without_rowid_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // ── TEXT single-column PK ──
        for s in [
            "CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO kv VALUES ('banana',2),('apple',1),('cherry',3)",
        ] {
            ex(&f, &r, s).await;
        }
        // natural scan is PK (text) order
        check("wr text pk order", fq(&f, "SELECT k,v FROM kv").await,
              rq(&r, "SELECT k,v FROM kv"), &mut diffs);
        check("wr text pk explicit order", fq(&f, "SELECT k FROM kv ORDER BY k").await,
              rq(&r, "SELECT k FROM kv ORDER BY k"), &mut diffs);
        // rowid is NOT a column on WITHOUT ROWID -> both error
        check("wr no rowid column", fq(&f, "SELECT rowid FROM kv").await,
              rq(&r, "SELECT rowid FROM kv"), &mut diffs);
        // PK uniqueness enforced: duplicate insert rejected
        ex(&f, &r, "INSERT INTO kv VALUES ('apple',99)").await;
        check("wr pk unique", fq(&f, "SELECT k,v FROM kv ORDER BY k").await,
              rq(&r, "SELECT k,v FROM kv ORDER BY k"), &mut diffs);
        // point lookup by PK
        check("wr pk lookup", fq(&f, "SELECT v FROM kv WHERE k='cherry'").await,
              rq(&r, "SELECT v FROM kv WHERE k='cherry'"), &mut diffs);
        // UPDATE a non-PK column
        ex(&f, &r, "UPDATE kv SET v=v+10 WHERE k='apple'").await;
        check("wr update nonpk", fq(&f, "SELECT k,v FROM kv ORDER BY k").await,
              rq(&r, "SELECT k,v FROM kv ORDER BY k"), &mut diffs);
        // UPDATE the PK column (moves the row in the tree)
        ex(&f, &r, "UPDATE kv SET k='avocado' WHERE k='apple'").await;
        check("wr update pk", fq(&f, "SELECT k,v FROM kv ORDER BY k").await,
              rq(&r, "SELECT k,v FROM kv ORDER BY k"), &mut diffs);
        // UPSERT on the PK
        ex(&f, &r, "INSERT INTO kv VALUES ('banana',100) ON CONFLICT(k) DO UPDATE SET v=v+1").await;
        check("wr upsert", fq(&f, "SELECT k,v FROM kv ORDER BY k").await,
              rq(&r, "SELECT k,v FROM kv ORDER BY k"), &mut diffs);
        // DELETE by PK
        ex(&f, &r, "DELETE FROM kv WHERE k='cherry'").await;
        check("wr delete", fq(&f, "SELECT k FROM kv ORDER BY k").await,
              rq(&r, "SELECT k FROM kv ORDER BY k"), &mut diffs);

        // ── composite PK ──
        for s in [
            "CREATE TABLE grid(x INTEGER, y INTEGER, label TEXT, PRIMARY KEY(x,y)) WITHOUT ROWID",
            "INSERT INTO grid VALUES (1,2,'a'),(1,1,'b'),(2,1,'c'),(1,3,'d'),(2,2,'e')",
        ] {
            ex(&f, &r, s).await;
        }
        // scan is composite-PK order (x, then y)
        check("wr composite pk order", fq(&f, "SELECT x,y,label FROM grid").await,
              rq(&r, "SELECT x,y,label FROM grid"), &mut diffs);
        // range scan over the leading PK column
        check("wr composite range", fq(&f, "SELECT x,y FROM grid WHERE x=1 AND y>=2 ORDER BY x,y").await,
              rq(&r, "SELECT x,y FROM grid WHERE x=1 AND y>=2 ORDER BY x,y"), &mut diffs);
        // composite PK uniqueness
        ex(&f, &r, "INSERT INTO grid VALUES (1,1,'dup')").await;
        check("wr composite unique", fq(&f, "SELECT count(*) FROM grid").await,
              rq(&r, "SELECT count(*) FROM grid"), &mut diffs);

        // ── secondary index on a WITHOUT ROWID table ──
        ex(&f, &r, "CREATE INDEX ix_grid_label ON grid(label)").await;
        check("wr secondary index", fq(&f, "SELECT x,y FROM grid WHERE label='e'").await,
              rq(&r, "SELECT x,y FROM grid WHERE label='e'"), &mut diffs);
        check("wr secondary index scan", fq(&f, "SELECT label FROM grid ORDER BY label").await,
              rq(&r, "SELECT label FROM grid ORDER BY label"), &mut diffs);

        // ── INTEGER PK WITHOUT ROWID (still index-btree, not a rowid alias) ──
        for s in [
            "CREATE TABLE n(id INTEGER PRIMARY KEY, w TEXT) WITHOUT ROWID",
            "INSERT INTO n VALUES (5,'e'),(1,'a'),(3,'c')",
        ] {
            ex(&f, &r, s).await;
        }
        check("wr int pk order", fq(&f, "SELECT id,w FROM n").await,
              rq(&r, "SELECT id,w FROM n"), &mut diffs);

        assert!(diffs.is_empty(), "{} WITHOUT ROWID divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
