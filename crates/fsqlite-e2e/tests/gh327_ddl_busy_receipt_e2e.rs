//! bd-kkqmd — GH #327 release-risk receipt: single-process `create_table`
//! reported blocking 5 s against its own WriteCoordinator (jsm 0.3.16 field
//! report), then surfacing `database is busy` with no other process alive.
//!
//! The field log shows TWO PerConnection regions in one process, so the prime
//! hypothesis is: connection B ran `CREATE TABLE ... AS SELECT` (the parse
//! span shows the subquery-rewrite path) while connection A held an open
//! write transaction, and DDL admission correctly refused to proceed until
//! the busy timeout — correct busy semantics plus a consumer usage pattern,
//! not a livelock. The alternative hypothesis — a standalone same-process
//! livelock on the `create_table` path — would be a direct release blocker.
//!
//! This receipt pins both sides:
//! 1. A single connection running `CREATE TABLE ... AS SELECT` with a
//!    subquery must complete promptly (refutes the standalone-livelock
//!    hypothesis).
//! 2. With a peer connection holding an open write transaction, the DDL must
//!    resolve within the configured busy budget (no unbounded hang), the
//!    database must stay intact, and the DDL must succeed once the peer
//!    resolves its transaction — matching the field observation that the
//!    same operation works after the conflict is cleared out-of-band.

use std::time::{Duration, Instant};

use fsqlite::Connection;

/// Generous wall-clock ceiling for operations that must not hang. The field
/// report's pathological case blocked for exactly the 5 s busy timeout; a
/// healthy standalone DDL completes in milliseconds.
const PROMPT_BUDGET: Duration = Duration::from_secs(3);

/// Busy budget configured for the contended scenario so the receipt bounds
/// its own wall clock instead of inheriting a 5 s default.
const CONTENDED_BUSY_MS: i64 = 1_000;
const CONTENDED_BUDGET: Duration = Duration::from_secs(8);

const SEED_SQL: &[&str] = &[
    "PRAGMA journal_mode = WAL;",
    "CREATE TABLE skills (id INTEGER PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL);",
    "INSERT INTO skills VALUES (1,'alpha',3),(2,'beta',1),(3,'gamma',7),(4,'delta',2);",
];

/// The DDL shape from the field log: `create_table` that goes through the
/// `rewrite_subquery` parse phase.
const DDL_WITH_SUBQUERY: &str = "CREATE TABLE conflicted AS \
     SELECT id, name FROM skills WHERE id IN (SELECT id FROM skills WHERE version > 1)";

async fn seed(conn: &Connection) {
    for sql in SEED_SQL {
        conn.execute(sql).await.expect("seed statement");
    }
}

#[test]
fn gh327_single_connection_create_table_as_select_is_prompt() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().expect("utf8 path").to_owned();

        let conn = Connection::open(&path).await.expect("open");
        seed(&conn).await;

        let started = Instant::now();
        conn.execute(DDL_WITH_SUBQUERY)
            .await
            .expect("standalone CREATE TABLE AS SELECT with subquery must succeed");
        let elapsed = started.elapsed();
        assert!(
            elapsed < PROMPT_BUDGET,
            "standalone create_table took {elapsed:?}; a delay near the busy \
             timeout reproduces the GH #327 livelock hypothesis"
        );

        let rows = conn
            .query("SELECT count(*) FROM conflicted")
            .await
            .expect("count new table");
        assert_eq!(
            rows[0].values()[0],
            fsqlite_types::SqliteValue::Integer(3),
            "subquery-rewritten CTAS must materialize the filtered rows"
        );
        conn.close().await.expect("close");
    });
}

#[test]
fn gh327_ddl_against_open_peer_write_txn_is_bounded_and_recovers() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().expect("utf8 path").to_owned();

        let conn_a = Connection::open(&path).await.expect("open A");
        seed(&conn_a).await;

        let conn_b = Connection::open(&path).await.expect("open B");
        conn_b
            .execute(&format!("PRAGMA busy_timeout = {CONTENDED_BUSY_MS};"))
            .await
            .expect("busy_timeout");

        // Peer connection holds an open write transaction touching the table
        // the DDL selects from — the two-region shape from the field log.
        conn_a.execute("BEGIN").await.expect("A begin");
        conn_a
            .execute("UPDATE skills SET version = version + 1 WHERE id = 2")
            .await
            .expect("A uncommitted write");

        let started = Instant::now();
        let contended = conn_b.execute(DDL_WITH_SUBQUERY).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < CONTENDED_BUDGET,
            "DDL against an open peer write txn must resolve within the busy \
             budget, not hang; took {elapsed:?}"
        );
        // Either outcome is admissible for the receipt: a busy-class refusal
        // (stock-SQLite-like) or a successful MVCC DDL. What is NOT
        // admissible is a hang (asserted above) or corruption (asserted
        // below). Record which arm ran so the receipt is explicit.
        let contended_outcome = match &contended {
            Ok(_) => "ddl-succeeded-concurrently".to_owned(),
            Err(error) => {
                assert!(
                    error.is_transient(),
                    "a refused concurrent DDL must surface a transient \
                     busy-class error, got: {error}"
                );
                format!("busy-class refusal: {error}")
            }
        };
        eprintln!("gh327 receipt: contended DDL outcome = {contended_outcome} in {elapsed:?}");

        // Field report parity: once the conflicting transaction resolves,
        // the same operation must succeed.
        conn_a.execute("COMMIT").await.expect("A commit");
        if contended.is_err() {
            let started_retry = Instant::now();
            conn_b
                .execute(DDL_WITH_SUBQUERY)
                .await
                .expect("DDL retry after peer commit must succeed");
            let retry_elapsed = started_retry.elapsed();
            assert!(
                retry_elapsed < PROMPT_BUDGET,
                "post-commit DDL retry took {retry_elapsed:?}"
            );
        }

        let rows = conn_b
            .query("SELECT count(*) FROM conflicted")
            .await
            .expect("count conflicted");
        assert_eq!(rows[0].values()[0], fsqlite_types::SqliteValue::Integer(3));

        // The peer's committed write must be intact.
        let rows = conn_b
            .query("SELECT version FROM skills WHERE id = 2")
            .await
            .expect("peer write visible");
        assert_eq!(rows[0].values()[0], fsqlite_types::SqliteValue::Integer(2));

        let integrity = conn_b
            .query("PRAGMA integrity_check;")
            .await
            .expect("integrity");
        assert_eq!(
            integrity[0].values()[0],
            fsqlite_types::SqliteValue::Text("ok".to_owned()),
            "database must stay integrity-clean after the contended DDL"
        );

        conn_b.close().await.expect("close B");
        conn_a.close().await.expect("close A");
    });
}

#[test]
fn gh327_ddl_after_peer_rollback_is_prompt() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().expect("utf8 path").to_owned();

        let conn_a = Connection::open(&path).await.expect("open A");
        seed(&conn_a).await;
        let conn_b = Connection::open(&path).await.expect("open B");

        conn_a.execute("BEGIN").await.expect("A begin");
        conn_a
            .execute("DELETE FROM skills WHERE id = 4")
            .await
            .expect("A uncommitted delete");
        conn_a.execute("ROLLBACK").await.expect("A rollback");

        // After the peer fully resolves, DDL must be prompt — the field
        // report's "works after the conflict is resolved out-of-band" arm.
        let started = Instant::now();
        conn_b
            .execute(DDL_WITH_SUBQUERY)
            .await
            .expect("DDL after peer rollback must succeed");
        let elapsed = started.elapsed();
        assert!(
            elapsed < PROMPT_BUDGET,
            "DDL after peer rollback took {elapsed:?}"
        );

        let rows = conn_b
            .query("SELECT count(*) FROM skills")
            .await
            .expect("rolled-back delete restored");
        assert_eq!(rows[0].values()[0], fsqlite_types::SqliteValue::Integer(4));

        conn_b.close().await.expect("close B");
        conn_a.close().await.expect("close A");
    });
}
