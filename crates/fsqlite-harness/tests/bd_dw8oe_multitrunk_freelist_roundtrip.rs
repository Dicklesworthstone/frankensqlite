//! bd-dw8oe: multi-trunk durable freelist round-trip.
//!
//! The large-scale churn keeper dies at BEGIN with
//! `freelist trunk N leaf_count 17823488 exceeds max 1022` once the durable
//! freelist exceeds one trunk (~1022 leaves per 4K trunk). This keeper pins
//! the single-connection half of the discriminator: build a freelist larger
//! than one trunk, commit it, and prove the durable chain round-trips —
//! through our own begin-refresh walk on a fresh connection AND through the
//! stock SQLite oracle. A deterministic failure here pins multi-trunk
//! serialization (or its WAL read-back); a pass pushes the churn failure
//! toward concurrent/torn-read mechanisms.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn query_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.unwrap_or_else(|error| {
        panic!("query failed: {sql}: {error:?}");
    });
    match rows.first().map(|row| row.values()[0].clone()) {
        Some(SqliteValue::Integer(value)) => value,
        other => panic!("expected integer result for {sql}, got {other:?}"),
    }
}

/// Rows sized so each lands on its own overflow-free leaf page region; 1500
/// deleted rows push the freelist well past one trunk's 1022-leaf capacity.
const ROWS: i64 = 1500;
const PAYLOAD_LEN: usize = 3200; // ~one 4K page per row incl. overhead

#[test]
fn multitrunk_freelist_survives_commit_and_reopen() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("dw8oe_multitrunk.db");
    let db = db_path.to_string_lossy().into_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;\n             PRAGMA busy_timeout=5000;\n             CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT);",
        )
        .await
        .expect("schema");

        // Seed enough pages that deleting them frees > one trunk of leaves.
        for chunk in 0..(ROWS / 100) {
            conn.execute("BEGIN CONCURRENT;").await.expect("begin seed");
            for i in 0..100 {
                let id = chunk * 100 + i;
                let payload = format!("{id}_{}", "p".repeat(PAYLOAD_LEN));
                conn.execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
                    .await
                    .expect("insert");
            }
            conn.execute("COMMIT;").await.expect("commit seed");
        }
        let page_count_seeded = query_i64(&conn, "PRAGMA page_count;").await;
        assert!(
            page_count_seeded > 1100,
            "seed must span enough pages to force a multi-trunk freelist: {page_count_seeded}"
        );

        // Free them all in one committed transaction.
        conn.execute("BEGIN CONCURRENT;")
            .await
            .expect("begin delete");
        conn.execute("DELETE FROM t;").await.expect("delete all");
        conn.execute("COMMIT;").await.expect("commit delete");

        let freelist_count = query_i64(&conn, "PRAGMA freelist_count;").await;
        assert!(
            freelist_count > 1022,
            "freelist must exceed one trunk's 1022-leaf capacity: {freelist_count}"
        );

        // Round-trip 1: our own fresh connection must walk the multi-trunk
        // chain at begin-refresh without corruption errors.
        let fresh = Connection::open(&db).await.expect("fresh open");
        let fresh_freelist = query_i64(&fresh, "PRAGMA freelist_count;").await;
        assert_eq!(
            fresh_freelist, freelist_count,
            "fresh connection must reload the same multi-trunk freelist"
        );
        let fresh_rows = query_i64(&fresh, "SELECT COUNT(*) FROM t;").await;
        assert_eq!(fresh_rows, 0);

        // Round-trip 2: reuse from the multi-trunk freelist must work and
        // shrink it consistently.
        fresh
            .execute("BEGIN CONCURRENT;")
            .await
            .expect("begin reuse");
        for id in 0..50 {
            let payload = format!("reuse_{id}_{}", "q".repeat(PAYLOAD_LEN));
            fresh
                .execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
                .await
                .expect("reuse insert");
        }
        fresh.execute("COMMIT;").await.expect("commit reuse");
        let page_count_after_reuse = query_i64(&fresh, "PRAGMA page_count;").await;
        assert!(
            page_count_after_reuse <= page_count_seeded,
            "reuse must come from the freelist, not grow the file: \
             seeded={page_count_seeded} after={page_count_after_reuse}"
        );

        conn.close().await.expect("close conn");
        fresh.close().await.expect("close fresh");
    });

    // Round-trip 3: stock SQLite must accept the multi-trunk chain.
    let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
    let integrity: String = oracle
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("oracle integrity_check");
    assert_eq!(
        integrity, "ok",
        "stock integrity_check must pass the multi-trunk freelist"
    );
    let oracle_freelist: i64 = oracle
        .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
        .expect("oracle freelist_count");
    assert!(
        oracle_freelist > 900,
        "oracle must see the large freelist too: {oracle_freelist}"
    );
}
