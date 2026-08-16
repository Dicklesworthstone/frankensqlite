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

// bd-76k72 ROOT CAUSE: typeless virtual-table columns (FTS5, table-valued
// functions) are assigned NUMERIC affinity ('C') by
// `parse_virtual_table_column_infos`, but a typeless column has NONE affinity
// ('A') per SQLite. FTS5 stores columns as TEXT, so `fts_messages.message_id`
// reads as `Text("7")`; the predicate `fts_messages.message_id = messages.id`
// compares it against `messages.id` (INTEGER PK). With the column wrongly
// NUMERIC, `join_comparison_affinity_p5('C','D')` returns 0 = no coercion, so
// `Text("7")` is never coerced and the LEFT JOIN yields NULL. The fix gives
// typeless vtab columns NONE affinity, which combines to NUMERIC against an
// integer column and coerces "7" -> 7. (Plain non-vtab TEXT=INTEGER joins were
// always correct — see bd_76k72_affinity_probe.rs.)
//
// The fix lives in TWO sites (there are two copies of the parser):
//   * crates/fsqlite-core/src/compat_persist.rs (reload path) — DONE.
//   * crates/fsqlite-core/src/connection.rs:90181 (`parse_declared_...`) and
//     :90201 (`default_virtual_table_column_info`) — the LIVE `:memory:`
//     CREATE path (FTS5's generic factory doesn't override `column_info`).
// `#[ignore]` stays until the connection.rs half lands; un-ignore then.
#[ignore = "bd-76k72: green once connection.rs:90181/90201 vtab affinity 'C'->'A' lands (compat_persist half done)"]
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

// bd-76k72 MECHANISM EXPERIMENTS (prints, no hard asserts on the vtab arms):
// isolate WHY the vtab-driven inner INTEGER-PK lookup drops numeric coercion.
#[test]
fn bd_76k72_experiment_cast_and_nonpk_inner() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        setup(&conn).await;

        // E1: explicit CAST of the vtab TEXT key to INTEGER. If this matches,
        // the bug is purely comparison-affinity/coercion (value + plan are fine).
        let e1 = conn
            .query(
                "SELECT fts_messages.title, messages.idx \
                 FROM fts_messages LEFT JOIN messages \
                 ON CAST(fts_messages.message_id AS INTEGER) = messages.id \
                 WHERE fts_messages MATCH 'auth'",
            )
            .await
            .unwrap();
        eprintln!(
            "E1 CAST key to INTEGER (expect 'Auth failure',2): {:?}",
            e1.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // E2: INNER JOIN variant (not LEFT) — does the driver being a vtab still
        // drop the match when the join is inner?
        let e2 = conn
            .query(
                "SELECT fts_messages.title, messages.idx \
                 FROM fts_messages JOIN messages \
                 ON fts_messages.message_id = messages.id \
                 WHERE fts_messages MATCH 'auth'",
            )
            .await
            .unwrap();
        eprintln!(
            "E2 INNER JOIN (expect one row 'Auth failure',2): {:?}",
            e2.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );

        // E3: reversed ON operand order (messages.id = fts.message_id).
        let e3 = conn
            .query(
                "SELECT fts_messages.title, messages.idx \
                 FROM fts_messages LEFT JOIN messages \
                 ON messages.id = fts_messages.message_id \
                 WHERE fts_messages MATCH 'auth'",
            )
            .await
            .unwrap();
        eprintln!(
            "E3 reversed operands (expect 'Auth failure',2): {:?}",
            e3.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );
    });
}

// E4: inner join column is a NON-PK indexed/plain INTEGER (not the rowid).
// Distinguishes a rowid-seek-specific coercion gap from a general vtab-join
// comparison gap.
#[test]
fn bd_76k72_experiment_nonpk_integer_inner() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE meta (mid INTEGER, idx INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE fts_m USING fts5(content, message_id)")
            .await
            .unwrap();
        conn.execute("INSERT INTO meta(mid, idx) VALUES (7, 2)")
            .await
            .unwrap();
        conn.execute("INSERT INTO fts_m(rowid, content, message_id) VALUES (1, 'auth token', 7)")
            .await
            .unwrap();

        let e4 = conn
            .query(
                "SELECT fts_m.message_id, meta.idx \
                 FROM fts_m LEFT JOIN meta ON fts_m.message_id = meta.mid \
                 WHERE fts_m MATCH 'auth'",
            )
            .await
            .unwrap();
        eprintln!(
            "E4 non-PK INTEGER inner (expect 7,2): {:?}",
            e4.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>()
        );
    });
}
