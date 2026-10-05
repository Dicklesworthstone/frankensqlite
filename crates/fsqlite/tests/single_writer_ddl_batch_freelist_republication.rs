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
        assert_eq!(
            count_schema_objects(&rows),
            30,
            "all gen2 tables must exist"
        );

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

/// br-qfvd6: retain the real downstream handoff between independent engines.
/// No connection overlaps the canonical migration/checkpoint interval.
async fn mixed_engine_bootstrap(path: &str, tables: usize) {
    let bootstrap = Connection::open(path).await.expect("bootstrap open");
    bootstrap
        .execute("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .await
        .expect("bootstrap WAL pragmas");
    bootstrap
        .execute(&synthesize_ddl_batch(tables, "base"))
        .await
        .expect("bootstrap schema");
    bootstrap.close().await.expect("close bootstrap connection");

    let canonical = rusqlite::Connection::open(path).expect("canonical migration open");
    canonical
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             BEGIN IMMEDIATE;
             CREATE TABLE migration_ledger (version INTEGER PRIMARY KEY);
             INSERT INTO migration_ledger VALUES (1);
             CREATE INDEX canonical_payload ON base_0(payload);
             COMMIT;",
        )
        .expect("canonical migration");
    let checkpoint: (i64, i64, i64) = canonical
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("canonical checkpoint");
    assert_eq!(checkpoint.0, 0, "checkpoint must complete: {checkpoint:?}");
    canonical.close().expect("close canonical connection");
}

async fn mixed_engine_sequential_alter(seed_transactions: i64) {
    // The small case minimizes the SQL; the 36-table case preserves the
    // downstream bootstrap's roughly 144 schema-commit clock advances.
    for tables in [2, 36] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mixed-bootstrap.db");
        let path = path.to_str().expect("UTF-8 test path");
        mixed_engine_bootstrap(path, tables).await;
        let runtime = Connection::open(path).await.expect("runtime reopen");
        for id in 0..seed_transactions {
            runtime
                .execute(&format!(
                    "BEGIN IMMEDIATE;
                     INSERT INTO base_0(id, slug, other_id, payload, created_ts, updated_ts)
                     VALUES ({id}, 'seed-{id}', {id}, 'payload', 1, 1);
                     COMMIT;"
                ))
                .await
                .expect("sequential committed seed");
        }
        if let Err(error) = runtime
            .execute("ALTER TABLE base_0 RENAME TO saved_base")
            .await
        {
            let events = runtime.query("PRAGMA fsqlite.commit_events").await;
            panic!(
                "sequential ALTER after mixed bootstrap: tables={tables}, \
                 seed_transactions={seed_transactions}, error={error:?}, events={events:?}"
            );
        }
        assert_eq!(
            count_schema_objects(
                &runtime
                    .query("SELECT COUNT(*) FROM saved_base")
                    .await
                    .unwrap()
            ),
            seed_transactions
        );
        runtime.close().await.expect("close runtime");
        let oracle = rusqlite::Connection::open(path).expect("oracle reopen");
        let count: i64 = oracle
            .query_row("SELECT COUNT(*) FROM saved_base", [], |row| row.get(0))
            .expect("oracle sees renamed table and committed rows");
        assert_eq!(count, seed_transactions);
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("oracle integrity check");
        assert_eq!(integrity, "ok");
    }
}

#[test]
fn mixed_engine_ddl_immediately_after_reopen() {
    asupersync::test_utils::run_test(|| mixed_engine_sequential_alter(0));
}

#[test]
fn mixed_engine_ddl_after_three_committed_seeds() {
    asupersync::test_utils::run_test(|| mixed_engine_sequential_alter(3));
}

/// Control: a real intervening data commit must still invalidate DDL's
/// snapshot, even when it writes a different table from the schema change.
#[test]
fn mixed_engine_ddl_rejects_intervening_data_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("concurrent-control.db");
        let path = path.to_str().expect("UTF-8 test path");
        mixed_engine_bootstrap(path, 2).await;
        let ddl = Connection::open(path).await.expect("DDL open");
        let writer = Connection::open(path).await.expect("writer open");
        ddl.execute("BEGIN CONCURRENT").await.expect("DDL begin");
        writer
            .execute("BEGIN IMMEDIATE; INSERT INTO migration_ledger VALUES (2); COMMIT;")
            .await
            .expect("intervening data commit");
        ddl.execute("ALTER TABLE base_0 RENAME TO saved_base")
            .await
            .expect("stage DDL in old snapshot");
        let error = ddl.execute("COMMIT").await.expect_err("stale DDL rejected");
        assert!(
            matches!(error, fsqlite::FrankenError::BusySnapshot { .. }),
            "expected snapshot conflict, got {error:?}"
        );
        let events = ddl.query("PRAGMA fsqlite.commit_events").await.unwrap();
        assert!(
            events.iter().any(|row| matches!(
                row.values().get(7),
                Some(SqliteValue::Text(reason)) if reason.as_ref() == "stale_schema_change_snapshot"
            )),
            "must reject because of intervening data commit: {events:?}"
        );
        ddl.execute("ROLLBACK").await.expect("rollback stale DDL");
        writer.close().await.expect("close writer");
        ddl.close().await.expect("close DDL connection");
    });
}
