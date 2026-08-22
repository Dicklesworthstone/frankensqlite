//! bd-yoa57 / bd-2rsuf: `PRAGMA journal_size_limit` must be RECOGNIZED and
//! ENFORCED, not silently ignored (which made the downstream WAL-size cap an
//! illusion).
//!
//! Part A (recognition): the pragma stores and reports its value.
//! Part B (enforcement): when the WAL exceeds the configured byte cap, the
//! post-commit adaptive auto-checkpoint is promoted to a TRUNCATE checkpoint,
//! so the on-disk WAL does not stay grown past the limit.
//!
//! Oracle framing: stock SQLite keeps the WAL bounded once journal_size_limit
//! is set; fsqlite must too. The control (no limit) is allowed to grow the WAL
//! far larger, proving the limit actually does something.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Read the integer result of a scalar pragma/query.
async fn scalar_int(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("query");
    match rows.first().and_then(|r| r.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

/// Drive `rows` single-row autocommit inserts (each a commit, so the
/// post-commit adaptive auto-checkpoint runs) into a fresh WAL database at
/// `path` configured with the given `journal_size_limit`, then return the
/// on-disk `-wal` file size in bytes.
async fn wal_bytes_after_writes(path: &str, journal_size_limit: i64, rows: i64) -> u64 {
    let conn = Connection::open(path).await.expect("open");
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    conn.execute("PRAGMA fsqlite.concurrent_mode = OFF")
        .await
        .expect("concurrent off");
    // Low autocheckpoint threshold so the adaptive checkpoint fires frequently.
    conn.execute("PRAGMA wal_autocheckpoint=4")
        .await
        .expect("autockpt");
    conn.execute(&format!("PRAGMA journal_size_limit={journal_size_limit}"))
        .await
        .expect("jsl");
    // Part A: the pragma is recognized and reports the value we set.
    assert_eq!(
        scalar_int(&conn, "PRAGMA journal_size_limit").await,
        journal_size_limit,
        "journal_size_limit must be recognized and report its value"
    );
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
        .await
        .expect("create");
    for i in 0..rows {
        conn.execute(&format!(
            "INSERT INTO t(id, payload) VALUES ({i}, 'payload-row-{i}-with-some-bulk-text-to-fill-a-frame')"
        ))
        .await
        .expect("insert");
    }
    conn.close().await.expect("close");

    let wal_path = format!("{path}-wal");
    std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0)
}

#[test]
fn bd_yoa57_journal_size_limit_caps_the_wal() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        const ROWS: i64 = 3000;
        // A 16 KiB cap. Without enforcement, 3000 committed frames leave the WAL
        // file at multiple MiB; with it, each over-limit checkpoint truncates,
        // so the WAL stays small.
        const LIMIT: i64 = 16 * 1024;

        let capped_db = dir.path().join("capped.db");
        let capped_wal =
            wal_bytes_after_writes(capped_db.to_string_lossy().as_ref(), LIMIT, ROWS).await;

        // ENFORCEMENT: the WAL must not stay grown far past the limit. Allow a
        // generous window (one checkpoint interval of frames can be in flight),
        // but nowhere near the multi-MiB an uncapped run would reach.
        assert!(
            capped_wal <= 256 * 1024,
            "journal_size_limit={LIMIT} must keep the WAL bounded, but it was \
             {capped_wal} bytes after {ROWS} committed rows"
        );

        // CONTROL: with no limit (-1), the same workload is permitted to leave
        // the WAL substantially larger — proving the cap is what bounded it.
        let uncapped_db = dir.path().join("uncapped.db");
        let uncapped_wal =
            wal_bytes_after_writes(uncapped_db.to_string_lossy().as_ref(), -1, ROWS).await;

        eprintln!(
            "bd-yoa57: capped_wal={capped_wal} bytes (limit={LIMIT}), uncapped_wal={uncapped_wal} bytes"
        );
        assert!(
            capped_wal < uncapped_wal,
            "journal_size_limit must shrink the WAL vs. no limit \
             (capped={capped_wal}, uncapped={uncapped_wal})"
        );
    });
}
