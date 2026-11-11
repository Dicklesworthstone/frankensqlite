#![recursion_limit = "512"]

//! bd-ih8ak — a flusher's remembered size floor must not outlive a peer's
//! publication.
//!
//! Before appending, a flusher floors its base database size with the size the
//! latest durable commit certified (bd-vnxjd), so a writer whose view predates a
//! peer's growth does not certify a smaller database. Deriving that floor reads
//! the WAL and the certificate sidecar. A flusher now remembers the floor its
//! own last flush certified and reuses it while the WAL-index header that flush
//! published is still the published one.
//!
//! The case a remembered floor gets wrong if it is ever reused across a peer's
//! publication: writer A begins, a peer grows the file and commits, and A then
//! commits a write to a page that already existed. A's base size is the one from
//! its BEGIN and its remembered floor is older still; only the floor read from
//! the files covers the peer's growth. Reusing the remembered one would make A
//! certify the smaller size, which the append gate refuses (`BusySnapshot`), so
//! a commit that succeeds today would start failing.
//!
//! Every round checks the row count on both connections and `integrity_check`.
//! After both connections close, stock SQLite (rusqlite) reads the file:
//! `integrity_check`, the row count and both counters. Stock is not opened
//! while the connections are live: an in-process stock connection cannot see
//! their locks and would checkpoint the WAL away under them.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SEED_ROWS: i64 = 2_000;
const PEER_ROWS_PER_ROUND: i64 = 400;
const ROUNDS: i64 = 4;

async fn open(path: &str) -> Connection {
    let conn = Connection::open(path).await.expect("open");
    conn.execute("PRAGMA busy_timeout=10000")
        .await
        .expect("busy_timeout");
    conn
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("query");
    match rows[0].values()[0] {
        SqliteValue::Integer(value) => value,
        ref other => panic!("expected an integer from {sql}, got {other:?}"),
    }
}

async fn text(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.expect("query");
    match rows[0].values()[0] {
        SqliteValue::Text(ref value) => value.to_string(),
        ref other => panic!("expected text from {sql}, got {other:?}"),
    }
}

/// What stock SQLite reads from the file: (`integrity_check`, row count, `v` of id 1, `v` of id 2).
fn stock_view(path: &str) -> (String, i64, i64, i64) {
    let stock = rusqlite::Connection::open(path).expect("stock open");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity_check");
    let count: i64 = stock
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("stock count");
    let first: i64 = stock
        .query_row("SELECT v FROM t WHERE id = 1", [], |row| row.get(0))
        .expect("stock id 1");
    let second: i64 = stock
        .query_row("SELECT v FROM t WHERE id = 2", [], |row| row.get(0))
        .expect("stock id 2");
    (integrity, count, first, second)
}

#[test]
fn stale_writer_with_a_remembered_floor_commits_after_peer_growth() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("floor.db");
        let path = path.to_str().expect("utf-8 path").to_owned();

        let a = open(&path).await;
        a.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad TEXT)")
            .await
            .expect("ddl");
        // Enough rows that the leaf holding ids 1 and 2 is far from the right
        // edge, where the peer's inserts land.
        a.execute("BEGIN").await.expect("seed begin");
        for id in 1..=SEED_ROWS {
            a.execute(&format!(
                "INSERT INTO t(id, v, pad) VALUES ({id}, 0, '{}')",
                "x".repeat(200)
            ))
            .await
            .expect("seed row");
        }
        a.execute("COMMIT").await.expect("seed commit");

        // A's own back-to-back commits: from the second one on, A flushes with
        // the floor its previous flush remembered.
        for _ in 0..3 {
            a.execute("UPDATE t SET v = v + 1 WHERE id = 1")
                .await
                .expect("own autocommit update");
        }

        let b = open(&path).await;
        let mut next_peer_id = SEED_ROWS + 1;
        for round in 1..=ROUNDS {
            a.execute("BEGIN CONCURRENT").await.expect("a begin");
            a.execute("UPDATE t SET v = v + 1 WHERE id = 1")
                .await
                .expect("a writes an existing page");

            // The peer grows the file past A's snapshot and publishes.
            b.execute("BEGIN CONCURRENT").await.expect("b begin");
            for _ in 0..PEER_ROWS_PER_ROUND {
                b.execute(&format!(
                    "INSERT INTO t(id, v, pad) VALUES ({next_peer_id}, {round}, '{}')",
                    "y".repeat(200)
                ))
                .await
                .expect("peer row");
                next_peer_id += 1;
            }
            b.execute("COMMIT").await.expect("peer growth commit");

            a.execute("COMMIT")
                .await
                .expect("a disjoint existing-page writer must commit after peer growth");

            // A's follow-up commits run on a remembered floor again, and must
            // keep the peer's growth.
            for _ in 0..2 {
                a.execute("UPDATE t SET v = v + 1 WHERE id = 2")
                    .await
                    .expect("a follow-up autocommit update");
            }

            let expected_rows = SEED_ROWS + round * PEER_ROWS_PER_ROUND;
            assert_eq!(
                scalar(&a, "SELECT count(*) FROM t").await,
                expected_rows,
                "round {round}: A sees every peer row"
            );
            assert_eq!(
                scalar(&b, "SELECT count(*) FROM t").await,
                expected_rows,
                "round {round}: the peer still sees its own rows after A's commits"
            );
            assert_eq!(
                scalar(&a, "SELECT v FROM t WHERE id = 1").await,
                3 + round,
                "round {round}: A's write under the peer's growth landed once"
            );
            assert_eq!(
                scalar(&b, "SELECT v FROM t WHERE id = 2").await,
                2 * round,
                "round {round}: the peer reads A's follow-up commits"
            );
            assert_eq!(
                text(&a, "PRAGMA integrity_check").await,
                "ok",
                "round {round}: integrity_check"
            );
        }

        drop(b);
        drop(a);
        assert_eq!(
            stock_view(&path),
            (
                "ok".to_owned(),
                SEED_ROWS + ROUNDS * PEER_ROWS_PER_ROUND,
                3 + ROUNDS,
                2 * ROUNDS
            ),
            "after both connections close"
        );
    });
}
