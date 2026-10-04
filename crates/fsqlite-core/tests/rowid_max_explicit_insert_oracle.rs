#![recursion_limit = "512"]

//! Explicit rowids keep working in a table that holds the largest rowid.
//!
//! On file-backed connections the concurrent rowid allocator refused
//! `bump_explicit` once it had wrapped past i64::MAX, so `INSERT ... VALUES
//! (9223372036854775807, ...)` failed with "explicit rowid bump failed: rowid
//! space exhausted", and so did every later explicit insert into the table,
//! even rowid 1. An implicit rowid allocated as MAX (after MAX - 1) wedged the
//! table the same way. SQLite accepts all of these. Compared with rusqlite, in
//! memory and file-backed; implicit rowids after MAX are left out because
//! SQLite then picks a random free rowid.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i{n}"),
        SqliteValue::Float(f) => format!("r{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("i{n}"),
        rusqlite::types::Value::Real(f) => format!("r{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:?}"),
    }
}

/// (statements, state query) — each statement must succeed in both engines,
/// then the state must match.
const CASES: &[(&[&str], &str)] = &[
    (
        &[
            "CREATE TABLE m(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO m VALUES (9223372036854775807, 'max')",
            "INSERT INTO m VALUES (1, 'one')",
            "INSERT INTO m VALUES (-5, 'neg')",
            "INSERT OR REPLACE INTO m VALUES (9223372036854775807, 'max2')",
        ],
        "SELECT id, v FROM m ORDER BY id",
    ),
    (
        &[
            "CREATE TABLE m2(v)",
            "INSERT INTO m2(rowid, v) VALUES (9223372036854775807, 'max')",
            "INSERT INTO m2(rowid, v) VALUES (5, 'five')",
        ],
        "SELECT rowid, v FROM m2 ORDER BY rowid",
    ),
    (
        &[
            "CREATE TABLE m3(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO m3 VALUES (9223372036854775806, 'nearmax')",
            "INSERT INTO m3(v) VALUES ('auto')",
            "INSERT INTO m3 VALUES (7, 'seven')",
            "UPDATE m3 SET id = 8 WHERE id = 7",
            "DELETE FROM m3 WHERE id = 8",
            "INSERT INTO m3 VALUES (9, 'nine')",
        ],
        "SELECT id, v FROM m3 ORDER BY id",
    ),
    (
        &[
            "CREATE TABLE m4(id INTEGER PRIMARY KEY AUTOINCREMENT, v)",
            "INSERT INTO m4 VALUES (9223372036854775807, 'max')",
            "INSERT INTO m4 VALUES (3, 'three')",
        ],
        "SELECT id, v FROM m4 ORDER BY id",
    ),
];

#[test]
fn explicit_rowids_work_beside_the_largest_rowid() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let mut found = Vec::new();
            for (case_index, (statements, state)) in CASES.iter().enumerate() {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join(format!("rowid_max_{case_index}.db"));
                let f = if file_backed {
                    Connection::open(path.to_str().unwrap()).await.unwrap()
                } else {
                    Connection::open(":memory:").await.unwrap()
                };
                let r = rusqlite::Connection::open_in_memory().unwrap();
                for sql in *statements {
                    r.execute_batch(sql).unwrap();
                    if let Err(error) = f.execute(sql).await {
                        found.push(format!("[file_backed={file_backed}] {sql}: {error}"));
                    }
                }
                let ff: Vec<String> = f
                    .query(state)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                    .collect();
                let mut stmt = r.prepare(state).unwrap();
                let rr: Vec<String> = stmt
                    .query_map([], |row| {
                        Ok((0..2)
                            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                            .collect::<Vec<_>>()
                            .join("|"))
                    })
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                if ff != rr {
                    found.push(format!(
                        "[file_backed={file_backed}] {state}\n  fsqlite: {ff:?}\n  stock:   {rr:?}"
                    ));
                }
            }
            assert!(found.is_empty(), "{}", found.join("\n"));
        });
    }
}
