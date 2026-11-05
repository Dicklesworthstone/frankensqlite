#![recursion_limit = "512"]

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

#[cfg(feature = "diagnostic-pragmas")]
#[test]
fn bd_5lw60_journal_size_limit_reclaims_wal_during_write_pressure() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("write-pressure.db");
        let path = path.to_str().expect("UTF-8 path");
        let conn = Connection::open(path).await.expect("open");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("PRAGMA wal_autocheckpoint=0")
            .await
            .expect("disable autocheckpoint during setup");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB)")
            .await
            .expect("create");
        conn.execute("PRAGMA checkpoint_write_pressure_fps=1")
            .await
            .expect("low write-pressure threshold");
        conn.execute("PRAGMA checkpoint_urgent_wal_frames=1000000")
            .await
            .expect("keep generic emergency threshold out of the fixture");
        conn.execute("PRAGMA wal_autocheckpoint=1")
            .await
            .expect("enable autocheckpoint");

        // One large commit supplies hundreds of frames to the pressure sample,
        // rather than depending on a fast loop of tiny host-timed commits.
        conn.execute("INSERT INTO t VALUES (1, zeroblob(1048576))")
            .await
            .expect("large write");
        let wal_path = format!("{path}-wal");
        let pressure_wal = std::fs::metadata(&wal_path).expect("WAL").len();
        assert!(
            pressure_wal > 1024 * 1024,
            "pressure fixture WAL was {pressure_wal}"
        );
        let stats = conn.query("PRAGMA checkpoint_stats").await.expect("stats");
        let local_checkpoints = stats
            .iter()
            .find(|row| {
                row.values().first() == Some(&SqliteValue::Text("local_checkpoint_count".into()))
            })
            .and_then(|row| row.values().get(1));
        assert_eq!(
            local_checkpoints,
            Some(&SqliteValue::Integer(0)),
            "the unlimited control must exercise write-pressure deferral"
        );

        conn.execute("PRAGMA journal_size_limit=16384")
            .await
            .expect("set cap");
        // Another multi-page commit keeps the pressure branch active even
        // when the host is slow, while the cap now requires reclamation.
        conn.execute("INSERT INTO t VALUES (2, zeroblob(1048576))")
            .await
            .expect("capped write");
        let capped_wal = std::fs::metadata(&wal_path).expect("WAL after cap").len();
        assert!(
            capped_wal <= 16 * 1024,
            "write-pressure deferral must not override journal_size_limit: {capped_wal} bytes"
        );
        conn.execute("INSERT INTO t VALUES (3, X'00')")
            .await
            .expect("reuse WAL");
        let reused_wal = std::fs::metadata(&wal_path).expect("reused WAL").len();
        assert!(
            reused_wal <= 16 * 1024,
            "reused WAL retained {reused_wal} bytes"
        );

        // A size cap promotes a scheduled checkpoint; it does not re-enable
        // checkpoints the caller explicitly disabled.
        conn.execute("PRAGMA wal_autocheckpoint=0")
            .await
            .expect("disable autocheckpoint again");
        conn.execute("INSERT INTO t VALUES (4, zeroblob(1048576))")
            .await
            .expect("disabled write");
        assert!(
            std::fs::metadata(&wal_path).expect("disabled WAL").len() > 1024 * 1024,
            "wal_autocheckpoint=0 must remain disabled with a size cap"
        );
        assert_eq!(scalar_int(&conn, "SELECT count(*) FROM t").await, 4);
        conn.close().await.expect("close");

        // Stock applies the size cap when it reuses a fully checkpointed WAL.
        // Inspect it while open: stock removes its WAL on last-close.
        let stock_path = dir.path().join("stock-size-limit.db");
        let stock = rusqlite::Connection::open(&stock_path).expect("stock open");
        stock
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=1; \
                 CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB); \
                 INSERT INTO t VALUES (1, zeroblob(1048576)); \
                 PRAGMA journal_size_limit=16384; \
                 INSERT INTO t VALUES (2, zeroblob(1048576)); \
                 INSERT INTO t VALUES (3, X'00');",
            )
            .expect("stock workload");
        let stock_wal = std::fs::metadata(format!("{}-wal", stock_path.display()))
            .expect("stock WAL")
            .len();
        assert!(stock_wal <= 16 * 1024, "stock retained {stock_wal} bytes");
        eprintln!(
            "bd-5lw60: pressure_wal={pressure_wal}, capped_wal={capped_wal}, reused_wal={reused_wal}, stock_capped_wal={stock_wal}"
        );
    });
}
