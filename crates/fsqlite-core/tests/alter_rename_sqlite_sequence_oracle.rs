//! bd-gh-alter-rename-sqlite-sequence-gx6ds (GH #150): ALTER TABLE RENAME must
//! rewrite the on-disk sqlite_sequence row, not leave a stale entry.
//!
//! After `ALTER TABLE t RENAME TO t2`, the AUTOINCREMENT bookkeeping row in
//! sqlite_sequence must move from 't' to 't2' (C SQLite), so a later insert
//! continues the same sequence and a DROP removes the entry. fsqlite once left
//! the old 't' row behind (a second row appeared, and DROP orphaned it). This
//! keeper pins both scenarios against the rusqlite oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const RENAME_SCRIPT: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
    "INSERT INTO t(v) VALUES('a'),('b'),('c')",
    "ALTER TABLE t RENAME TO t2",
    "INSERT INTO t2(v) VALUES('d')",
];

const DROP_SCRIPT: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
    "INSERT INTO t(v) VALUES('a'),('b'),('c')",
    "ALTER TABLE t RENAME TO t2",
    "DROP TABLE t2",
];

fn oracle_seq_rows(script: &[&str]) -> Vec<(String, i64)> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in script {
        conn.execute(s, []).unwrap();
    }
    conn.prepare("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        .unwrap()
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

async fn fsqlite_seq_rows(script: &[&str]) -> Vec<(String, i64)> {
    let conn = Connection::open(":memory:").await.unwrap();
    for s in script {
        conn.execute(s)
            .await
            .unwrap_or_else(|e| panic!("`{s}`: {e:?}"));
    }
    let rows = conn
        .query("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        .await
        .expect("select sqlite_sequence");
    rows.iter()
        .map(|r| {
            let name = match r.values()[0] {
                SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                ref other => panic!("name not text: {other:?}"),
            };
            let seq = match r.values()[1] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("seq not int: {other:?}"),
            };
            (name, seq)
        })
        .collect()
}

#[test]
fn alter_rename_sqlite_sequence_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Rename: only ('t2', 4) — the sequence moved and continued.
        let expected_rename = oracle_seq_rows(RENAME_SCRIPT);
        assert_eq!(
            expected_rename,
            vec![("t2".to_owned(), 4)],
            "oracle premise (rename)"
        );
        assert_eq!(
            fsqlite_seq_rows(RENAME_SCRIPT).await,
            expected_rename,
            "ALTER RENAME must move the sqlite_sequence row (no stale 't' entry)"
        );

        // Drop after rename: the entry is gone.
        let expected_drop = oracle_seq_rows(DROP_SCRIPT);
        assert_eq!(expected_drop, Vec::new(), "oracle premise (drop)");
        assert_eq!(
            fsqlite_seq_rows(DROP_SCRIPT).await,
            expected_drop,
            "DROP after RENAME must leave no orphan sqlite_sequence row"
        );
    });
}
