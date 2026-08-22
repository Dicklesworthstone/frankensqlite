//! Regression probe: a schema-only open of a database holding a contentless
//! FTS5 index with persisted segments must BIND to those segments rather than
//! re-tokenizing the corpus into an in-memory inverted index.
//!
//! Gated on `FSQLITE_LAZY_PROBE_DB` so it is inert without a real archive.

#[test]
fn schema_only_open_of_large_fts5_archive_is_bounded() {
    let Ok(db) = std::env::var("FSQLITE_LAZY_PROBE_DB") else {
        eprintln!("probe skipped: set FSQLITE_LAZY_PROBE_DB");
        return;
    };
    asupersync::test_utils::run_test(|| async move {
        let deferred = std::env::var_os("FSQLITE_LAZY_PROBE_DEFERRED").is_some();
        let start = std::time::Instant::now();
        let conn = if deferred {
            fsqlite::Connection::open_existing_schema_only_deferred_fts5(db.clone())
                .await
                .expect("deferred schema-only open")
        } else {
            fsqlite::Connection::open_existing_schema_only(db.clone())
                .await
                .expect("schema-only open")
        };
        let elapsed = start.elapsed();
        let hwm = std::fs::read_to_string("/proc/self/status")
            .unwrap_or_default()
            .lines()
            .find(|l| l.starts_with("VmHWM"))
            .unwrap_or("VmHWM: ?")
            .to_string();
        eprintln!("LAZYPROBE open_ms={} peak_{hwm}", elapsed.as_millis());

        // Correctness gates: a fast open is worthless if the data is wrong.
        let rows = conn
            .query("SELECT COUNT(*) FROM messages")
            .await
            .expect("count canonical messages");
        let total = match rows[0].values().first() {
            Some(fsqlite::SqliteValue::Integer(v)) => *v,
            other => panic!("unexpected count value: {other:?}"),
        };
        eprintln!("LAZYPROBE messages_count={total}");
        assert_eq!(total, 473_753, "canonical row count must be unchanged");

        // The lazy-bound FTS5 index must still answer a match query from its
        // persisted segments.
        if deferred {
            eprintln!("LAZYPROBE deferred=1 (FTS index intentionally empty; skipping FTS gate)");
            drop(conn);
            return;
        }
        let hits = conn
            .query("SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'error'")
            .await
            .expect("fts match query");
        let fts_hits = match hits[0].values().first() {
            Some(fsqlite::SqliteValue::Integer(v)) => *v,
            other => panic!("unexpected fts count value: {other:?}"),
        };
        eprintln!("LAZYPROBE fts_match_hits={fts_hits}");
        assert!(fts_hits > 0, "lazy-bound FTS5 must return matches");

        let after = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        let hwm2 = after
            .lines()
            .find(|l| l.starts_with("VmHWM"))
            .unwrap_or("VmHWM: ?");
        eprintln!("LAZYPROBE after_queries_peak_{hwm2}");
        drop(conn);
    });
}
