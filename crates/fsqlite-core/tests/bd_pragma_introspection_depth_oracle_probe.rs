#![recursion_limit = "512"]

//! PRAGMA schema-introspection depth leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the introspection PRAGMAs exposed as table-valued functions —
//! pragma_table_info (cid,name,type,notnull,dflt_value,pk), pragma_table_xinfo
//! (adds hidden + generated columns), pragma_index_list (seq,name,unique,origin,
//! partial), pragma_index_info / pragma_index_xinfo, and pragma_foreign_key_list
//! (id,seq,table,from,to,on_update,on_delete,match) — the last exercises the
//! `to`=NULL-for-implicit-PK case. Selected by explicit stable columns and
//! ordered, so opaque/version-specific ordering can't cause false diffs.
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

#[test]
fn pragma_introspection_depth_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE parent(pid INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            "CREATE TABLE child(\
               id INTEGER PRIMARY KEY, \
               name TEXT NOT NULL DEFAULT 'anon', \
               qty INTEGER DEFAULT 0, \
               ppid INTEGER REFERENCES parent(pid) ON DELETE CASCADE ON UPDATE SET NULL, \
               pcode TEXT REFERENCES parent(code), \
               total INTEGER GENERATED ALWAYS AS (qty * 2) VIRTUAL)",
            "CREATE INDEX ix_child_name ON child(name)",
            "CREATE UNIQUE INDEX ux_child_ppid ON child(ppid) WHERE ppid IS NOT NULL",
            "CREATE INDEX ix_child_expr ON child(qty + 1)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // table_info: columns with type/notnull/default/pk
        check("table_info child",
            fq(&f, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('child') ORDER BY cid").await,
            rq(&r, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('child') ORDER BY cid"), &mut diffs);
        check("table_info parent",
            fq(&f, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('parent') ORDER BY cid").await,
            rq(&r, "SELECT cid,name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('parent') ORDER BY cid"), &mut diffs);
        // table_xinfo: includes generated column (hidden flag distinguishes)
        check("table_xinfo child",
            fq(&f, "SELECT cid,name,type,\"notnull\",pk,hidden FROM pragma_table_xinfo('child') ORDER BY cid").await,
            rq(&r, "SELECT cid,name,type,\"notnull\",pk,hidden FROM pragma_table_xinfo('child') ORDER BY cid"), &mut diffs);

        // index_list: user + auto indexes, unique/origin/partial
        check("index_list child",
            fq(&f, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('child') ORDER BY name").await,
            rq(&r, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('child') ORDER BY name"), &mut diffs);
        check("index_list parent",
            fq(&f, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('parent') ORDER BY name").await,
            rq(&r, "SELECT name,\"unique\",origin,partial FROM pragma_index_list('parent') ORDER BY name"), &mut diffs);

        // index_info: columns of a named index (seqno, cid, name)
        check("index_info ix_child_name",
            fq(&f, "SELECT seqno,cid,name FROM pragma_index_info('ix_child_name') ORDER BY seqno").await,
            rq(&r, "SELECT seqno,cid,name FROM pragma_index_info('ix_child_name') ORDER BY seqno"), &mut diffs);
        // index_xinfo: adds desc, coll, key flag
        check("index_xinfo ux_child_ppid",
            fq(&f, "SELECT seqno,cid,name,\"desc\",coll,key FROM pragma_index_xinfo('ux_child_ppid') ORDER BY seqno").await,
            rq(&r, "SELECT seqno,cid,name,\"desc\",coll,key FROM pragma_index_xinfo('ux_child_ppid') ORDER BY seqno"), &mut diffs);
        // expression index: keyed column has cid = -2
        check("index_xinfo expr",
            fq(&f, "SELECT seqno,cid,key FROM pragma_index_xinfo('ix_child_expr') ORDER BY seqno").await,
            rq(&r, "SELECT seqno,cid,key FROM pragma_index_xinfo('ix_child_expr') ORDER BY seqno"), &mut diffs);

        // foreign_key_list: the `to` = NULL for implicit-PK reference case + named-col case
        check("foreign_key_list child",
            fq(&f, "SELECT id,seq,\"table\",\"from\",\"to\",on_update,on_delete,match FROM pragma_foreign_key_list('child') ORDER BY id,seq").await,
            rq(&r, "SELECT id,seq,\"table\",\"from\",\"to\",on_update,on_delete,match FROM pragma_foreign_key_list('child') ORDER BY id,seq"), &mut diffs);

        // counts via the introspection tables
        check("column count",
            fq(&f, "SELECT count(*) FROM pragma_table_info('child')").await,
            rq(&r, "SELECT count(*) FROM pragma_table_info('child')"), &mut diffs);

        assert!(diffs.is_empty(), "{} PRAGMA introspection divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
