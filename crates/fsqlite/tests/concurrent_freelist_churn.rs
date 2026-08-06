//! GH#302 e2e verification: default (concurrent-promoted) transactions must
//! reuse committed freelist pages without breaking an already-pinned reader.
//! Steady-state churn must plateau in `PRAGMA page_count`, and reused pages
//! must still expose their old bytes to an older WAL snapshot.
//!
//! Run: `cargo test -p fsqlite --test concurrent_freelist_churn`

#![allow(clippy::future_not_send, clippy::large_futures)]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

const SNAPSHOT_ROW_COUNT: usize = 160;
const SNAPSHOT_BATCH_SIZE: usize = 16;
const SNAPSHOT_PAYLOAD_BYTES: usize = 1_536;
const OLD_ID_BASE: i64 = 1_000;
const NEW_ID_BASE: i64 = 2_000;

async fn pragma_u64(conn: &Connection, pragma: &str) -> u64 {
    let rows = conn.query(pragma).await.expect("pragma query");
    match rows[0].values().first() {
        Some(SqliteValue::Integer(v)) => u64::try_from(*v).expect("non-negative pragma value"),
        other => panic!("unexpected pragma row shape for {pragma}: {other:?}"),
    }
}

async fn configure_default_wal_connection(conn: &Connection, role: &str) {
    assert!(
        conn.is_concurrent_mode_default(),
        "{role} must retain FrankenSQLite's concurrent-default contract"
    );

    let journal_mode = conn
        .query("PRAGMA journal_mode")
        .await
        .expect("read journal mode");
    assert!(
        matches!(
            journal_mode.first().and_then(|row| row.values().first()),
            Some(SqliteValue::Text(mode)) if mode.as_ref() == "wal"
        ),
        "{role} must report WAL mode, got {journal_mode:?}"
    );

    conn.execute("PRAGMA wal_autocheckpoint = 0")
        .await
        .expect("disable WAL autocheckpoint");
    assert_eq!(
        pragma_u64(conn, "PRAGMA wal_autocheckpoint").await,
        0,
        "{role} WAL autocheckpoint must remain disabled"
    );
}

fn generation_payload(generation: &str, filler: &str, ordinal: usize) -> String {
    let prefix = format!("{generation}-{ordinal:04}-");
    assert!(prefix.len() < SNAPSHOT_PAYLOAD_BYTES);
    format!(
        "{prefix}{}",
        filler.repeat(SNAPSHOT_PAYLOAD_BYTES - prefix.len())
    )
}

async fn insert_generation(conn: &Connection, id_base: i64, generation: &str, filler: &str) {
    for batch_start in (0..SNAPSHOT_ROW_COUNT).step_by(SNAPSHOT_BATCH_SIZE) {
        let batch_end = (batch_start + SNAPSHOT_BATCH_SIZE).min(SNAPSHOT_ROW_COUNT);
        let values = (batch_start..batch_end)
            .map(|ordinal| {
                let id = id_base + i64::try_from(ordinal).expect("snapshot row id fits i64");
                let payload = generation_payload(generation, filler, ordinal);
                format!("({id}, '{payload}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute(&format!(
            "INSERT INTO snapshot_churn(id, payload) VALUES {values}"
        ))
        .await
        .expect("insert snapshot generation batch");
    }
}

fn assert_generation(rows: &[fsqlite::Row], id_base: i64, generation: &str, filler: &str) {
    assert_eq!(
        rows.len(),
        SNAPSHOT_ROW_COUNT,
        "{generation} snapshot row count"
    );
    for (ordinal, row) in rows.iter().enumerate() {
        let expected_id = id_base + i64::try_from(ordinal).expect("snapshot row ordinal fits i64");
        let expected_payload = generation_payload(generation, filler, ordinal);
        match row.values() {
            [
                SqliteValue::Integer(actual_id),
                SqliteValue::Text(actual_payload),
            ] => {
                assert_eq!(
                    *actual_id, expected_id,
                    "{generation} snapshot id at ordinal {ordinal}"
                );
                assert_eq!(
                    actual_payload.as_ref(),
                    expected_payload.as_str(),
                    "{generation} snapshot payload at ordinal {ordinal}"
                );
            }
            values => {
                panic!("unexpected {generation} snapshot row at ordinal {ordinal}: {values:?}")
            }
        }
    }
}

#[test]
fn default_churn_page_count_plateaus_after_warmup() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("churn.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open churn db");

        let payload = "x".repeat(200);
        let mut page_counts = Vec::new();
        for _cycle in 0..6u32 {
            conn.execute(
                "CREATE TABLE churn(id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, payload TEXT NOT NULL)",
            )
            .await
            .expect("create churn table");
            conn.execute("CREATE INDEX churn_grp ON churn(grp, id)")
                .await
                .expect("create churn index");
            for batch in 0..8u32 {
                let values = (0..25u32)
                    .map(|i| format!("({}, '{payload}')", u64::from(batch * 25 + i) % 7))
                    .collect::<Vec<_>>()
                    .join(", ");
                conn.execute(&format!("INSERT INTO churn(grp, payload) VALUES {values}"))
                    .await
                    .expect("insert churn batch");
            }
            conn.execute("DROP TABLE churn").await.expect("drop churn");
            page_counts.push(pragma_u64(&conn, "PRAGMA page_count").await);
        }

        // Warm-up may grow the file; steady state must not. Allow the second
        // cycle as the high-water mark and require every later cycle to stay
        // at or below it.
        let high_water = page_counts[1];
        for (cycle, &count) in page_counts.iter().enumerate().skip(2) {
            assert!(
                count <= high_water,
                "page_count must plateau after warm-up: cycle={cycle} count={count} \
                 high_water={high_water} all={page_counts:?}"
            );
        }

        // The engine's own integrity check must pass on the churned database.
        let verdict = conn
            .query("PRAGMA integrity_check")
            .await
            .expect("integrity_check");
        let ok = matches!(
            verdict[0].values().first(),
            Some(SqliteValue::Text(s)) if s.as_ref() == "ok"
        );
        assert!(
            ok,
            "integrity_check must return ok, got {:?}",
            verdict[0].values()
        );
    });
}

#[test]
fn old_snapshot_survives_committed_freelist_reuse() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let database_path = dir.path().join("snapshot-reuse.db");
        let path = database_path.to_string_lossy().into_owned();

        let writer = Connection::open(&path).await.expect("open writer");
        configure_default_wal_connection(&writer, "writer").await;
        writer
            .execute(
                "CREATE TABLE snapshot_churn(\
                    id INTEGER PRIMARY KEY, \
                    payload TEXT NOT NULL\
                )",
            )
            .await
            .expect("create snapshot churn table");
        writer
            .execute("BEGIN")
            .await
            .expect("begin seed transaction");
        insert_generation(&writer, OLD_ID_BASE, "old", "o").await;
        writer
            .execute("COMMIT")
            .await
            .expect("commit seed transaction");

        let seeded_page_count = pragma_u64(&writer, "PRAGMA page_count").await;
        assert!(
            seeded_page_count > 2,
            "seed must span multiple pages, got page_count={seeded_page_count}"
        );
        let freelist_before_delete = pragma_u64(&writer, "PRAGMA freelist_count").await;

        let reader = Connection::open(&path).await.expect("open old reader");
        configure_default_wal_connection(&reader, "reader").await;
        reader
            .execute("BEGIN")
            .await
            .expect("begin old reader snapshot");
        assert!(
            reader.is_concurrent_transaction(),
            "plain BEGIN must auto-promote on the concurrent-default reader"
        );

        writer
            .execute("BEGIN")
            .await
            .expect("begin delete transaction");
        writer
            .execute("DELETE FROM snapshot_churn")
            .await
            .expect("delete old generation");
        writer
            .execute("COMMIT")
            .await
            .expect("commit old-generation delete");

        let page_count_after_delete = pragma_u64(&writer, "PRAGMA page_count").await;
        let freelist_after_delete = pragma_u64(&writer, "PRAGMA freelist_count").await;
        assert!(
            freelist_after_delete > freelist_before_delete,
            "committed delete must release pages: before={freelist_before_delete} \
             after={freelist_after_delete} page_count={page_count_after_delete}"
        );

        writer
            .execute("BEGIN")
            .await
            .expect("begin replacement transaction");
        insert_generation(&writer, NEW_ID_BASE, "new", "n").await;
        writer
            .execute("COMMIT")
            .await
            .expect("commit replacement generation");

        let page_count_after_reuse = pragma_u64(&writer, "PRAGMA page_count").await;
        let freelist_after_reuse = pragma_u64(&writer, "PRAGMA freelist_count").await;
        assert_eq!(
            page_count_after_reuse, page_count_after_delete,
            "equal-sized replacements must reuse committed free pages instead of growing EOF; \
             freelist before={freelist_before_delete} after_delete={freelist_after_delete} \
             after_reuse={freelist_after_reuse}"
        );
        assert!(
            freelist_after_reuse < freelist_after_delete,
            "replacement commit must consume the committed freelist: \
             after_delete={freelist_after_delete} after_reuse={freelist_after_reuse} \
             page_count={page_count_after_reuse}"
        );

        // This is the old reader's first access to any target-table page. It
        // must still resolve every deleted-and-reused page through its pinned
        // pre-delete snapshot and recover the exact old bytes.
        let old_rows = reader
            .query("SELECT id, payload FROM snapshot_churn ORDER BY id")
            .await
            .expect("read old generation from pinned snapshot");
        assert_generation(&old_rows, OLD_ID_BASE, "old", "o");
        reader
            .execute("COMMIT")
            .await
            .expect("finish old reader snapshot");

        reader
            .execute("BEGIN")
            .await
            .expect("begin fresh reader transaction");
        let fresh_rows = reader
            .query("SELECT id, payload FROM snapshot_churn ORDER BY id")
            .await
            .expect("read replacement generation from fresh snapshot");
        assert_generation(&fresh_rows, NEW_ID_BASE, "new", "n");
        reader
            .execute("COMMIT")
            .await
            .expect("finish fresh reader transaction");

        reader.close().await.expect("close reader");
        writer.close().await.expect("close writer");

        let stock = rusqlite::Connection::open(&database_path).expect("open with stock SQLite");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("stock SQLite integrity_check");
        assert_eq!(integrity, "ok", "stock SQLite must accept the final file");
    });
}
