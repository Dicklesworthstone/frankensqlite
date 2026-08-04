//! GH#302 e2e churn verification: default (concurrent-promoted) transactions
//! must reuse committed freelist pages, so a steady-state create/insert/drop
//! churn workload plateaus in `PRAGMA page_count` instead of growing the file
//! at EOF without bound while `PRAGMA freelist_count` climbs.
//!
//! Run: `cargo test -p fsqlite --test concurrent_freelist_churn`

#![allow(clippy::future_not_send, clippy::large_futures)]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn pragma_u64(conn: &Connection, pragma: &str) -> u64 {
    let rows = conn.query(pragma).await.expect("pragma query");
    match rows[0].values().first() {
        Some(SqliteValue::Integer(v)) => u64::try_from(*v).expect("non-negative pragma value"),
        other => panic!("unexpected pragma row shape for {pragma}: {other:?}"),
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
