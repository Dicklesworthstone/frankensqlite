#![recursion_limit = "512"]

//! bd-54sjx: an aggregate over a join materializes the joined rows and
//! reorders each left row's right-table matches the way SQLite's automatic
//! covering index visits them. That reorder also sorted the LEFT rows by
//! their column values (BINARY), so with `c(s TEXT COLLATE NOCASE)` holding
//! 'a','B','b','A' in that scan order, `group_concat(c.b)` over
//! `c JOIN u ON u.g = 1` came out `c4,c2,c1,c3`, `min(c.s)` reported the
//! tie 'A' instead of the first-scanned 'a', and the bare column beside a
//! FILTER that rejects every row came from the 'A' row. SQLite keeps the
//! left rows (and every source without an automatic index) in scan order.
//!
//! Each query is compared with rusqlite, in memory and file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE c(s TEXT COLLATE NOCASE, b)",
    "INSERT INTO c VALUES ('a','c1'),('B','c2'),('b','c3'),('A','c4')",
    "CREATE TABLE c2(s TEXT, b)",
    "INSERT INTO c2 SELECT s, b FROM c",
    "CREATE TABLE u(g PRIMARY KEY, x)",
    "INSERT INTO u VALUES (1,'u1'),(2,'u2')",
    // Un-indexed right tables, joined by USING: SQLite builds an automatic
    // index on them, so their matches come out sorted.
    "CREATE TABLE k(id, v)",
    "INSERT INTO k VALUES (2,'k2b'),(1,'k1z'),(2,'k2a'),(1,'k1a'),(3,'k3')",
    "CREATE TABLE p(id, name)",
    "INSERT INTO p VALUES (3,'p3'),(1,'p1'),(2,'p2'),(1,'p1b')",
    "CREATE TABLE q(name, w)",
    "INSERT INTO q VALUES ('p1','qz'),('p2','qy'),('p1','qa'),('p3','qx')",
];

const QUERIES: &[&str] = &[
    // The left rows keep their scan order (the r5 review shapes).
    "SELECT group_concat(c.b) FROM c JOIN u ON u.g = 1",
    "SELECT group_concat(c2.b) FROM c2 JOIN u ON u.g = 1",
    "SELECT group_concat(c.b) FROM c JOIN u ON u.g = c.rowid",
    "SELECT min(c.s), max(c.s) FROM c JOIN u ON u.g = 1",
    "SELECT min(c.s), c.b FROM c JOIN u ON u.g = 1",
    "SELECT max(c.s), c.b FROM c JOIN u ON u.g = 1",
    "SELECT min(c.s), count(*) FROM c JOIN u ON u.g = 1",
    "SELECT min(c.s) FROM c, u WHERE u.g = 1",
    "SELECT group_concat(c.b), u.x FROM c JOIN u ON u.g = 1 GROUP BY u.x",
    "SELECT u.x, group_concat(c.b) FROM c JOIN u GROUP BY u.x ORDER BY u.x",
    "SELECT max(c.b) FILTER (WHERE 0), c.b FROM c JOIN u ON u.g = 1",
    "SELECT group_concat(c.s) FROM c LEFT JOIN u ON u.g = 9",
    // Automatically indexed right tables still sort within each left row.
    "SELECT group_concat(k.v) FROM p JOIN k USING (id)",
    "SELECT group_concat(p.name || ':' || k.v) FROM p JOIN k USING (id)",
    "SELECT p.id, group_concat(k.v) FROM p JOIN k USING (id) GROUP BY p.id ORDER BY p.id",
    "SELECT group_concat(k.v) FROM p LEFT JOIN k USING (id)",
    "SELECT group_concat(q.w) FROM p JOIN q USING (name)",
    "SELECT group_concat(k.v || q.w) FROM p JOIN k USING (id) JOIN q USING (name)",
];

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn grouped_join_rows_keep_the_left_scan_order() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_54sjx.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                assert_eq!(rows_f(&f, sql).await, rows_r(&r, sql), "query `{sql}`");
            }
        });
    }
}
