//! bd-76k72 / bd-rwaxp: an FTS5 virtual table used as a JOIN input must expose
//! its non-MATCH columns correctly, so a join on one of them matches stock.
//!
//! Repro (from the failing --lib test
//! `test_sql_fts5_match_can_join_regular_message_metadata`): an FTS5 table
//! `LEFT JOIN`ed to a regular table on a non-MATCH FTS5 column
//! (`fts_messages.message_id = messages.id`) returns NULL for the regular
//! table's column — the join key does not match even though both sides hold 7.
//!
//! This file first PROBES (with prints, no asserts) to isolate whether the
//! non-MATCH FTS5 column reads wrong standalone or only inside the join, then
//! asserts the fixed behavior.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn setup(conn: &Connection) {
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, idx INTEGER)")
        .await
        .unwrap();
    conn.execute(
        "CREATE VIRTUAL TABLE fts_messages USING fts5(\
         content, title, agent, workspace, source_path, created_at, message_id)",
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO messages(id, idx) VALUES (7, 2)")
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at, message_id) \
         VALUES (1, 'auth token failure', 'Auth failure', 'codex', '/ws', '/tmp/session.jsonl', 42, 7)",
    )
    .await
    .unwrap();
}

#[test]
fn bd_76k72_probe_fts5_column_standalone_vs_join() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        setup(&conn).await;

        // Probe 1: standalone read of the non-MATCH column used as the join key.
        let p1 = conn
            .query("SELECT message_id FROM fts_messages WHERE fts_messages MATCH 'auth'")
            .await
            .unwrap();
        eprintln!(
            "PROBE1 standalone message_id (expect Integer(7)): {:?}",
            p1.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // Probe 2: full row (rowid + several columns) to see column alignment.
        let p2 = conn
            .query(
                "SELECT rowid, title, created_at, message_id FROM fts_messages \
                 WHERE fts_messages MATCH 'auth'",
            )
            .await
            .unwrap();
        eprintln!(
            "PROBE2 rowid,title,created_at,message_id (expect 1,'Auth failure',42,7): {:?}",
            p2.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // Probe 3: the two join keys side by side.
        let p3 = conn
            .query(
                "SELECT fts_messages.message_id, messages.id \
                 FROM fts_messages LEFT JOIN messages \
                 ON fts_messages.message_id = messages.id \
                 WHERE fts_messages MATCH 'auth'",
            )
            .await
            .unwrap();
        eprintln!(
            "PROBE3 join keys fts.message_id, messages.id (expect 7,7): {:?}",
            p3.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // Probe 4: the exact failing projection.
        let p4 = conn
            .query(
                "SELECT fts_messages.title, messages.idx \
                 FROM fts_messages LEFT JOIN messages \
                 ON fts_messages.message_id = messages.id \
                 WHERE fts_messages MATCH 'auth' LIMIT 5",
            )
            .await
            .unwrap();
        eprintln!(
            "PROBE4 title, idx (expect 'Auth failure',2): {:?}",
            p4.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // Assertion (the fixed behavior): the join matches and idx is 2.
        assert_eq!(p4.len(), 1, "expected exactly one joined row");
        assert_eq!(
            p4[0].get(0),
            Some(&SqliteValue::Text("Auth failure".into()))
        );
        assert_eq!(p4[0].get(1), Some(&SqliteValue::Integer(2)));
    });
}
