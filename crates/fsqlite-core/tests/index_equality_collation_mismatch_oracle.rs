#![recursion_limit = "512"]

//! An equality on an indexed column only pins an index seek when the
//! comparison's collation is the key term's collation.
//!
//! `SELECT count(*) FROM t WHERE a = 'a'` (a bare aggregate over equalities
//! only) sought any index whose leading key terms named the WHERE columns,
//! whatever their collation, and counted the index's run of equal keys
//! without a residual check. A NOCASE or RTRIM key term under a BINARY
//! comparison counted extra rows ('A' for 'a', 'y ' for 'y'), and a BINARY
//! key term under a NOCASE column missed rows. count(), sum() and
//! group_concat() all answered from the wrong rows. Found by the randomized
//! seek differential (`index_seek_random_differential`).
//!
//! Each query is compared with rusqlite, ad hoc and prepared, in memory and
//! file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    // A NOCASE key term under a BINARY column: the seek must not pin it.
    "CREATE TABLE n(id INTEGER PRIMARY KEY, a TEXT)",
    "CREATE INDEX n_a ON n(a COLLATE NOCASE)",
    "INSERT INTO n VALUES (1, 'A'), (2, 'a'), (3, 'b')",
    // A BINARY key term under a NOCASE column.
    "CREATE TABLE c(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE)",
    "CREATE INDEX c_a ON c(a COLLATE BINARY)",
    "INSERT INTO c VALUES (1, 'A'), (2, 'a'), (3, 'b')",
    // A key term that inherits the column's NOCASE: still seekable.
    "CREATE TABLE i(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE)",
    "CREATE INDEX i_a ON i(a)",
    "INSERT INTO i VALUES (1, 'A'), (2, 'a'), (3, 'b')",
    // An RTRIM second key term of a composite index.
    "CREATE TABLE r(id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
    "CREATE INDEX r_ab ON r(a, b COLLATE RTRIM)",
    "INSERT INTO r VALUES (1, 'x', 'y'), (2, 'x', 'y '), (3, 'x', 'z')",
    // RTRIM key terms over an INTEGER column holding text (the shape the
    // randomized differential found).
    "CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "CREATE INDEX m_ab ON m(a COLLATE RTRIM, b COLLATE RTRIM)",
    "INSERT INTO m VALUES (1, 'a', '2.0'), (2, 1, '1')",
];

const QUERIES: &[&str] = &[
    "SELECT count(*) FROM n WHERE a = 'a'",
    "SELECT sum(id) FROM n WHERE a = 'a'",
    "SELECT group_concat(id) FROM n WHERE a = 'a'",
    "SELECT min(id) FROM n WHERE a = 'A'",
    "SELECT count(*) FROM n WHERE 'a' = a",
    "SELECT count(*) FROM n WHERE a = 'a' COLLATE NOCASE",
    "SELECT count(*) FROM n WHERE a COLLATE NOCASE = 'a'",
    "SELECT id FROM n WHERE a = 'a' ORDER BY id",
    "SELECT count(*) FROM c WHERE a = 'a'",
    "SELECT sum(id) FROM c WHERE a = 'A'",
    "SELECT count(*) FROM c WHERE a = 'a' COLLATE BINARY",
    "SELECT id FROM c WHERE a = 'a' ORDER BY id",
    "SELECT count(*) FROM i WHERE a = 'a'",
    "SELECT group_concat(id) FROM i WHERE a = 'B'",
    "SELECT count(*) FROM r WHERE a = 'x' AND b = 'y'",
    "SELECT group_concat(id) FROM r WHERE a = 'x' AND b = 'y'",
    "SELECT count(*) FROM r WHERE a = 'x' AND b = 'y ' COLLATE RTRIM",
    "SELECT count(*) FROM r WHERE a = 'x'",
    "SELECT count(*) FROM m WHERE a = 'a ' AND b = '2.0'",
    "SELECT count(*) FROM m WHERE a = 'a ' AND b = '2.0 '",
    "SELECT count(*) FROM m WHERE a = 1 AND b = '1'",
];

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
fn index_equality_seeks_only_under_the_key_terms_collation() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path().join("coll.db").to_string_lossy().into_owned()
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
                let stock = rows_r(&r, sql);
                let ad_hoc: Vec<Vec<String>> = f
                    .query(sql)
                    .await
                    .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                assert_eq!(ad_hoc, stock, "file_backed={file_backed} `{sql}`");
                let prepared: Vec<Vec<String>> = f
                    .prepare(sql)
                    .await
                    .unwrap_or_else(|e| panic!("prepare `{sql}`: {e:?}"))
                    .query()
                    .await
                    .unwrap_or_else(|e| panic!("prepared `{sql}`: {e:?}"))
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                assert_eq!(
                    prepared, stock,
                    "prepared file_backed={file_backed} `{sql}`"
                );
            }
        });
    }
}
