//! bd-r82et: a SINGLE connection executing a large multi-statement DDL batch
//! (the downstream mcp_agent_mail base-schema shape: ~50 CREATE TABLE /
//! CREATE INDEX statements submitted as one batch) must never be refused by
//! the bd-gh302 append-gate freelist guard. With one connection and no
//! concurrent peers, every freelist publication is the writer's OWN
//! sequential publication; the gate's double-consumption arm used to treat a
//! pop of an in-memory-only freelist entry (a page that was never serialized
//! into durable page-1/trunk metadata) as a peer double-pop and failed the
//! batch deterministically with "database is busy (snapshot conflict on
//! pages: 51)". Stock sqlite3 executes the same batch trivially.
//!
//! The regression fix classifies freelist pops as durable-origin vs
//! in-memory-only (`PagerInner::durable_freelist_view`) and restricts the
//! double-consumption refusal to durable-origin pops, keeping the real
//! bd-gh302 peer resurrection/erasure protection fail-closed.

use fsqlite::{Connection, SqliteValue};

fn scratch_db_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "r82et-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Synthesize a DDL batch shaped like the downstream base schema: `tables`
/// CREATE TABLE statements, each followed by three CREATE INDEX statements,
/// with interleaved SQL comments (the downstream batch is ~200KB of
/// commented schema text).
fn synthesize_ddl_batch(tables: usize, name_prefix: &str) -> String {
    let mut batch = String::new();
    for t in 0..tables {
        batch.push_str(&format!(
            "-- table {t} of the synthesized base schema\n\
             CREATE TABLE IF NOT EXISTS {name_prefix}_{t} (\n    \
             id INTEGER PRIMARY KEY AUTOINCREMENT,\n    \
             slug TEXT NOT NULL UNIQUE,\n    \
             other_id INTEGER NOT NULL REFERENCES {name_prefix}_0(id),\n    \
             payload TEXT NOT NULL DEFAULT '',\n    \
             created_ts INTEGER NOT NULL,\n    \
             updated_ts INTEGER NOT NULL\n);\n\
             CREATE INDEX IF NOT EXISTS idx_{name_prefix}_{t}_slug ON {name_prefix}_{t}(slug);\n\
             CREATE INDEX IF NOT EXISTS idx_{name_prefix}_{t}_created ON {name_prefix}_{t}(created_ts DESC, id DESC);\n\
             CREATE INDEX IF NOT EXISTS idx_{name_prefix}_{t}_other ON {name_prefix}_{t}(other_id, updated_ts);\n\n"
        ));
    }
    batch
}

fn count_schema_objects(rows: &[fsqlite::Row]) -> i64 {
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("unexpected count value: {other:?}"),
    }
}

/// The failing downstream shape: one connection, one large DDL batch.
/// Before the fix this failed deterministically with
/// `database is busy (snapshot conflict on pages: 51)`.
#[test]
fn single_writer_large_ddl_batch_is_never_snapshot_refused() {
    asupersync::test_utils::run_test(move || async move {
        let path = scratch_db_path("ddl-batch");
        let conn = Connection::open(path.to_str().unwrap())
            .await
            .expect("open single connection");

        let batch = synthesize_ddl_batch(50, "base");
        conn.execute(&batch)
            .await
            .expect("single-connection multi-statement DDL batch must succeed (bd-r82et)");

        let rows = conn
            .query("SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','index');")
            .await
            .expect("read sqlite_master");
        let count = count_schema_objects(&rows);
        assert!(
            count >= 200,
            "expected all 50 tables + 150 indexes (plus autoindexes), got {count}"
        );
        let _ = std::fs::remove_file(&path);
    });
}

/// Same single writer, but with sequential free -> republish -> re-consume
/// churn across statements: drop a slab of tables (freeing their root and
/// index pages into the freelist), then create a fresh slab that re-consumes
/// those pages. Every publication is the one writer's own sequential
/// publication and must never be refused.
#[test]
fn single_writer_ddl_churn_republication_is_never_snapshot_refused() {
    asupersync::test_utils::run_test(move || async move {
        let path = scratch_db_path("ddl-churn");
        let conn = Connection::open(path.to_str().unwrap())
            .await
            .expect("open single connection");

        conn.execute(&synthesize_ddl_batch(30, "gen1"))
            .await
            .expect("initial DDL batch must succeed");

        let mut drop_batch = String::new();
        for t in 1..30 {
            drop_batch.push_str(&format!("DROP TABLE gen1_{t};\n"));
        }
        conn.execute(&drop_batch)
            .await
            .expect("drop batch must succeed (frees pages to the freelist)");

        conn.execute(&synthesize_ddl_batch(30, "gen2"))
            .await
            .expect("re-create batch must succeed (re-consumes freed pages, bd-r82et)");

        let rows = conn
            .query("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'gen2%';")
            .await
            .expect("read sqlite_master");
        assert_eq!(count_schema_objects(&rows), 30, "all gen2 tables must exist");

        let integrity = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("integrity_check");
        match &integrity[0].values()[0] {
            SqliteValue::Text(s) => {
                assert_eq!(s.as_ref(), "ok", "integrity_check must pass: {s:?}");
            }
            other => panic!("unexpected integrity_check value: {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    });
}
