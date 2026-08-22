//! GH #338 (bd-qfag9): a single connection running ONE explicit transaction
//! that copies rows out of a table and then drops that same source table must
//! not fail `COMMIT` with a `BusySnapshot` self-conflict.
//!
//! There are no peer connections and no other processes: the transaction is the
//! only writer. Reported deterministic on 0.3.1 — the transaction appears to
//! self-conflict on the schema/root pages its own `INSERT..SELECT` read from the
//! table it later drops (`conflicting_pages: "4,5"`, and `"4,5,6,7"` for a
//! larger variant). A transaction cannot conflict with itself, so `COMMIT` must
//! succeed and the data must be correct.
//!
//! These keepers are FILE-BACKED on purpose: the issue is a fresh-file repro and
//! the suspected mechanism is the WAL + first-committer-wins commit-index path,
//! which a `:memory:` database does not exercise the same way.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn query_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"))
        .into_iter()
        .map(|r| r.values().to_vec())
        .collect()
}

fn count(rows: &[Vec<SqliteValue>]) -> i64 {
    match rows.first().and_then(|r| r.first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("expected an integer count, got {other:?}"),
    }
}

/// The minimal bisected repro from the issue: CREATE new + INSERT..SELECT from
/// old + DROP old, inside one explicit transaction, on a single connection.
#[test]
fn insert_select_then_drop_source_in_one_txn_commits_without_self_conflict() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("issues.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.unwrap();

        conn.execute("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
            .await
            .unwrap();
        conn.execute("INSERT INTO issues (id, title) VALUES ('a', 'first')")
            .await
            .unwrap();
        conn.execute("INSERT INTO issues (id, title) VALUES ('b', 'second')")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        conn.execute("CREATE TABLE issues_new (id TEXT PRIMARY KEY, title TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO issues_new (id, title) SELECT id, title FROM issues")
            .await
            .unwrap();
        conn.execute("DROP TABLE issues").await.unwrap();

        conn.execute("COMMIT").await.unwrap_or_else(|e| {
            panic!(
                "COMMIT of a single-connection INSERT..SELECT+DROP txn must not self-conflict: {e}"
            )
        });

        let rows = query_rows(&conn, "SELECT id, title FROM issues_new ORDER BY id").await;
        assert_eq!(rows.len(), 2, "both rows should have been copied");
        assert_eq!(rows[0][0], SqliteValue::Text("a".into()));
        assert_eq!(rows[1][0], SqliteValue::Text("b".into()));

        conn.close().await.unwrap();
    });
}

/// The full table-rebuild dance the issue reports (`+ ALTER RENAME at the end`),
/// under `BEGIN EXCLUSIVE`, which the issue says reproduces identically.
#[test]
fn table_rebuild_dance_under_begin_exclusive_commits_without_self_conflict() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("rebuild.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.unwrap();

        conn.execute("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
            .await
            .unwrap();
        for i in 0..8 {
            conn.execute(&format!(
                "INSERT INTO issues (id, title) VALUES ('id{i}', 'title {i}')"
            ))
            .await
            .unwrap();
        }

        conn.execute("BEGIN EXCLUSIVE").await.unwrap();
        conn.execute("CREATE TABLE issues_new (id TEXT PRIMARY KEY, title TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO issues_new (id, title) SELECT id, title FROM issues")
            .await
            .unwrap();
        conn.execute("DROP TABLE issues").await.unwrap();
        conn.execute("ALTER TABLE issues_new RENAME TO issues")
            .await
            .unwrap();
        conn.execute("COMMIT").await.unwrap_or_else(|e| {
            panic!("COMMIT of the full rebuild dance must not self-conflict: {e}")
        });

        let rows = query_rows(&conn, "SELECT COUNT(*) FROM issues").await;
        assert_eq!(count(&rows), 8, "all 8 rows should survive the rebuild");

        conn.close().await.unwrap();
    });
}

/// The larger real-world variant the issue says conflicts on pages `"4,5,6,7"`:
/// rebuild TWO tables that reference the same source set inside one transaction.
#[test]
fn multi_table_rebuild_in_one_txn_commits_without_self_conflict() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("multi.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.unwrap();

        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        for i in 0..5 {
            conn.execute(&format!("INSERT INTO a (id, v) VALUES ({i}, 'a{i}')"))
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO b (id, v) VALUES ({i}, 'b{i}')"))
                .await
                .unwrap();
        }

        conn.execute("BEGIN").await.unwrap();
        conn.execute("CREATE TABLE a_new (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO a_new SELECT id, v FROM a")
            .await
            .unwrap();
        conn.execute("DROP TABLE a").await.unwrap();
        conn.execute("ALTER TABLE a_new RENAME TO a").await.unwrap();
        conn.execute("CREATE TABLE b_new (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO b_new SELECT id, v FROM b")
            .await
            .unwrap();
        conn.execute("DROP TABLE b").await.unwrap();
        conn.execute("ALTER TABLE b_new RENAME TO b").await.unwrap();
        conn.execute("COMMIT").await.unwrap_or_else(|e| {
            panic!("COMMIT of a two-table rebuild txn must not self-conflict: {e}")
        });

        let a_rows = query_rows(&conn, "SELECT COUNT(*) FROM a").await;
        let b_rows = query_rows(&conn, "SELECT COUNT(*) FROM b").await;
        assert_eq!(count(&a_rows), 5, "table a rows should survive");
        assert_eq!(count(&b_rows), 5, "table b rows should survive");

        conn.close().await.unwrap();
    });
}
