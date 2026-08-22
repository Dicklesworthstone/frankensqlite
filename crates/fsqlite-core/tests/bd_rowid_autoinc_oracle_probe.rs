#![recursion_limit = "512"]

//! rowid / AUTOINCREMENT / WITHOUT ROWID leaf-hunt (pane af49, 2026-08-21):
//! frank vs rusqlite over INTEGER PRIMARY KEY rowid aliasing, implicit rowid
//! assignment + reuse, AUTOINCREMENT monotonicity (no reuse after delete),
//! last_insert_rowid(), explicit rowid insert, and WITHOUT ROWID tables. Final
//! state compared. Pass = coverage keeper; a mismatch is a leaf divergence.

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
fn rowid_autoinc_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // INTEGER PRIMARY KEY aliases rowid
        for s in ["CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)",
                  "INSERT INTO a(v) VALUES ('x'),('y')",
                  "INSERT INTO a(id,v) VALUES (50,'z')",
                  "INSERT INTO a(v) VALUES ('w')"] { ex(&f, &r, s).await; }
        check("ipk rowid alias", fq(&f, "SELECT rowid, id, v FROM a ORDER BY id").await, rq(&r, "SELECT rowid, id, v FROM a ORDER BY id"), &mut diffs);
        check("last_insert_rowid a", fq(&f, "SELECT last_insert_rowid()").await, rq(&r, "SELECT last_insert_rowid()"), &mut diffs);

        // implicit rowid assignment + reuse-after-delete-of-max behavior
        for s in ["CREATE TABLE b(v TEXT)",
                  "INSERT INTO b VALUES ('p'),('q'),('r')",
                  "DELETE FROM b WHERE rowid=3",
                  "INSERT INTO b VALUES ('s')"] { ex(&f, &r, s).await; }
        check("implicit rowid reuse", fq(&f, "SELECT rowid, v FROM b ORDER BY rowid").await, rq(&r, "SELECT rowid, v FROM b ORDER BY rowid"), &mut diffs);

        // AUTOINCREMENT never reuses after delete
        for s in ["CREATE TABLE c(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                  "INSERT INTO c(v) VALUES ('a'),('b'),('c')",
                  "DELETE FROM c",
                  "INSERT INTO c(v) VALUES ('d')"] { ex(&f, &r, s).await; }
        check("autoinc no reuse", fq(&f, "SELECT id, v FROM c ORDER BY id").await, rq(&r, "SELECT id, v FROM c ORDER BY id"), &mut diffs);
        check("sqlite_sequence c", fq(&f, "SELECT name, seq FROM sqlite_sequence WHERE name='c'").await, rq(&r, "SELECT name, seq FROM sqlite_sequence WHERE name='c'"), &mut diffs);

        // WITHOUT ROWID
        for s in ["CREATE TABLE d(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                  "INSERT INTO d VALUES ('b',2),('a',1),('c',3)"] { ex(&f, &r, s).await; }
        check("without rowid order", fq(&f, "SELECT k, v FROM d ORDER BY k").await, rq(&r, "SELECT k, v FROM d ORDER BY k"), &mut diffs);
        check("without rowid pk lookup", fq(&f, "SELECT v FROM d WHERE k='b'").await, rq(&r, "SELECT v FROM d WHERE k='b'"), &mut diffs);

        // rowid/_rowid_/oid aliases + typeof
        check("rowid aliases", fq(&f, "SELECT rowid, _rowid_, oid FROM a WHERE id=50").await, rq(&r, "SELECT rowid, _rowid_, oid FROM a WHERE id=50"), &mut diffs);
        check("max/typeof", fq(&f, "SELECT max(rowid), typeof(rowid) FROM a").await, rq(&r, "SELECT max(rowid), typeof(rowid) FROM a"), &mut diffs);

        assert!(diffs.is_empty(), "{} rowid/autoinc divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
