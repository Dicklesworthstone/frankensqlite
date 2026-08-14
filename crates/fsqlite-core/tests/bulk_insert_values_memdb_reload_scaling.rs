//! bd-33sht: a read-free `INSERT ... VALUES` streak must not pay a full O(rows)
//! MemDatabase reload per statement (which made bulk ingest O(rows x statements)).
//! The mirror stays stale through the streak and is rebuilt once at the next
//! actual read boundary. This keeper asserts both correctness (bulk contents,
//! read-after-write, uniqueness, and the still-refreshing INSERT...SELECT path)
//! and that the memdb reload count does not scale with statement count.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

fn int(values: &[SqliteValue]) -> i64 {
    match values[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer, got {other:?}"),
    }
}

#[test]
fn read_free_insert_values_streak_defers_memdb_reload_and_stays_correct() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("bulk.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("open");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");

        conn.execute("BEGIN;").await.expect("begin");
        reset_hot_path_profile();
        const N: i64 = 60; // statements; each a 2-row INSERT VALUES
        for i in 0..N {
            conn.execute(&format!(
                "INSERT INTO t (id, v) VALUES ({}, 'a-{i:04}'), ({}, 'b-{i:04}');",
                i * 2,
                i * 2 + 1
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {i}: {e:?}"));
        }
        // Captured AFTER the streak, BEFORE any read: pre-fix this is ~N (a full
        // reload per statement); post-fix it must be ~0.
        let reloads_during_streak = hot_path_profile_snapshot().memdb_refresh_count;

        // Read-after-write inside the same txn: the mirror rebuilds once here and
        // must see every inserted row.
        let mid = conn.query("SELECT COUNT(*) FROM t;").await.expect("count");
        assert_eq!(int(mid[0].values()), N * 2, "read-after-write must see all inserted rows");
        conn.execute("COMMIT;").await.expect("commit");

        assert!(
            reloads_during_streak <= 2,
            "bd-33sht: a {N}-statement read-free INSERT VALUES streak reloaded the \
             MemDatabase {reloads_during_streak} times — it must not scale with \
             statement count (expected ~0)"
        );

        // Durable correctness after reopen.
        let count = conn.query("SELECT COUNT(*) FROM t;").await.expect("count2");
        assert_eq!(int(count[0].values()), N * 2);
        let ic = conn.query("PRAGMA integrity_check;").await.expect("ic");
        assert!(matches!(ic[0].values()[0], SqliteValue::Text(ref s) if s.as_ref() == "ok"));
    });
}

#[test]
fn read_free_insert_streak_still_enforces_uniqueness() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("uniq.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("open");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        conn.execute("BEGIN;").await.expect("begin");
        conn.execute("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b');")
            .await
            .expect("first insert");
        // A duplicate PK later in the same streak must still be rejected — the
        // insert's own uniqueness check reads the txn b-tree, which stays flushed.
        let dup = conn
            .execute("INSERT INTO t (id, v) VALUES (3, 'c'), (1, 'dup');")
            .await;
        assert!(dup.is_err(), "duplicate primary key must be rejected mid-streak");
        conn.execute("ROLLBACK;").await.expect("rollback");
    });
}

#[test]
fn insert_select_source_is_not_deferred_and_stays_correct() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("insel.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("open");
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create src");
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create dst");
        conn.execute("BEGIN;").await.expect("begin");
        conn.execute("INSERT INTO src (id, v) VALUES (1, 'x'), (2, 'y'), (3, 'z');")
            .await
            .expect("seed src");
        // INSERT ... SELECT reads existing rows — it is NOT read-free, keeps the
        // full refresh, and must copy the just-inserted src rows correctly.
        conn.execute("INSERT INTO dst (id, v) SELECT id, v FROM src;")
            .await
            .expect("insert select");
        let n = conn.query("SELECT COUNT(*) FROM dst;").await.expect("count");
        assert_eq!(int(n[0].values()), 3, "INSERT...SELECT must see and copy all src rows");
        conn.execute("COMMIT;").await.expect("commit");
    });
}
