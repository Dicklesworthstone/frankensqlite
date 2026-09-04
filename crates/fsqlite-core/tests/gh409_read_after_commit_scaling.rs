//! GH #409 keeper — the first read after a committed write must cost what the
//! read touches, not a pass over the whole database image.
//!
//! Reported against fsqlite 0.3.13: on a connection that had just committed a
//! write — even a single-row `INSERT` into a one-row table — the next `SELECT`
//! took seconds on a 76 MB file (~30–40 ms per MB), while the same statement
//! repeated cost 1–2 ms and the same read issued *inside* the still-open write
//! transaction was cheap. The plan was correct throughout, so the cost was not
//! the read: something re-armed per commit made the next statement boundary
//! walk the whole image.
//!
//! The keeper is a scaling test rather than an absolute-time test: it measures
//! the post-commit read on a database and then on a database with several
//! times more unrelated data, and requires that the cost not grow with the
//! image. A whole-image pass shows up as a multiple; a bounded refresh does
//! not.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::time::{Duration, Instant};

/// Rows of ~1 KB each. The small fixture is deliberately big enough that a
/// whole-image pass is measurable, and the large one is 4x it.
const SMALL_ROWS: i64 = 4_000;
const LARGE_ROWS: i64 = 16_000;

async fn build(path: &str, rows: i64) -> Connection {
    let conn = Connection::open(path).await.unwrap();
    conn.execute("CREATE TABLE t (k TEXT NOT NULL UNIQUE, n INTEGER NOT NULL, pad TEXT);")
        .await
        .unwrap();
    conn.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, v TEXT);")
        .await
        .unwrap();
    conn.execute("INSERT INTO small(id, v) VALUES (1, 'a');")
        .await
        .unwrap();
    let payload = "p".repeat(1000);
    conn.execute("BEGIN;").await.unwrap();
    for n in 0..rows {
        conn.execute(&format!(
            "INSERT INTO t(k, n, pad) VALUES ('k{n}', {n}, '{payload}');"
        ))
        .await
        .unwrap();
    }
    conn.execute("COMMIT;").await.unwrap();
    conn
}

/// Time the first `t` read after a one-row write to an unrelated table,
/// repeated a few times so a single scheduling hiccup cannot decide the test.
async fn post_commit_read_cost(conn: &Connection, tag: char) -> Duration {
    let mut best = Duration::MAX;
    for round in 0..5_i64 {
        conn.execute(&format!(
            "INSERT INTO small(id, v) VALUES ({}, '{tag}');",
            100 + round
        ))
        .await
        .unwrap();
        let started = Instant::now();
        let rows = conn
            .query("SELECT n FROM t WHERE k = 'k7-nonexistent';")
            .await
            .unwrap();
        best = best.min(started.elapsed());
        assert!(rows.is_empty(), "the probe read matches no row by design");
    }
    best
}

#[test]
#[ignore = "GH#409 repro: currently RED. Measured 2026-09-04 on this fixture: \
            4 000 rows -> 3.5 ms, 16 000 rows -> 21 ms for the first read after a \
            one-row commit (ratio ~6 for 4x data). Un-ignore with the fix."]
fn gh409_first_read_after_a_commit_does_not_scale_with_the_database() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();

        let small_path = dir.path().join("gh409_small.db");
        let small = build(&small_path.to_string_lossy(), SMALL_ROWS).await;
        let large_path = dir.path().join("gh409_large.db");
        let large = build(&large_path.to_string_lossy(), LARGE_ROWS).await;

        // Warm both connections: the very first read after the bulk load pays
        // whatever one-time hydration the open owes, which is not what this
        // keeper measures.
        let _ = small.query("SELECT n FROM t WHERE k = 'k1';").await.unwrap();
        let _ = large.query("SELECT n FROM t WHERE k = 'k1';").await.unwrap();

        let small_cost = post_commit_read_cost(&small, 's').await;
        let large_cost = post_commit_read_cost(&large, 'l').await;

        let ratio = large_cost.as_secs_f64() / small_cost.as_secs_f64().max(1e-6);
        assert!(
            ratio < 2.5,
            "the first read after a commit must not scale with the image: \
             {SMALL_ROWS} rows took {small_cost:?}, {LARGE_ROWS} rows took {large_cost:?} \
             (ratio {ratio:.2}, data is 4x)"
        );

        // The read is still correct after all that.
        let rows = small.query("SELECT n FROM t WHERE k = 'k1';").await.unwrap();
        assert!(rows.is_empty() || matches!(rows[0].values()[0], SqliteValue::Integer(_)));
    });
}

/// Diagnostic companion to the keeper above: enable the hot-path profile and
/// print the counter delta for exactly one post-commit read, at two database
/// sizes, so the term that scales with the image can be named rather than
/// guessed at. Not an assertion; run it with `--ignored --nocapture`.
#[test]
#[ignore = "GH#409 diagnostic probe; run with --ignored --nocapture"]
fn gh409_profile_the_first_read_after_a_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for (label, rows) in [("small", SMALL_ROWS), ("large", LARGE_ROWS)] {
            let path = dir.path().join(format!("gh409_probe_{label}.db"));
            let conn = build(&path.to_string_lossy(), rows).await;
            // Warm: pay the one-time post-load hydration outside the window.
            let _ = conn.query("SELECT n FROM t WHERE k = 'k1';").await.unwrap();
            conn.execute("INSERT INTO small(id, v) VALUES (900, 'w');")
                .await
                .unwrap();
            let _ = conn.query("SELECT n FROM t WHERE k = 'k1';").await.unwrap();

            fsqlite_core::connection::set_hot_path_profile_enabled(true);
            conn.execute("INSERT INTO small(id, v) VALUES (901, 'w');")
                .await
                .unwrap();
            // Reset AFTER the write so every counter below describes the read
            // alone, not the commit that re-armed it.
            fsqlite_core::connection::reset_hot_path_profile();
            let started = Instant::now();
            let _ = conn
                .query("SELECT n FROM t WHERE k = 'k7-nonexistent';")
                .await
                .unwrap();
            let elapsed = started.elapsed();
            let snapshot = fsqlite_core::connection::hot_path_profile_snapshot();
            fsqlite_core::connection::set_hot_path_profile_enabled(false);
            println!("--- {label} ({rows} rows): post-commit read took {elapsed:?}");
            println!("{snapshot:#?}");
            conn.close().await.unwrap();
        }
    });
}

/// GH#409 attribution probe: the post-commit read's VDBE opcode profile showed
/// `Next` once per row in the table, i.e. a full table scan, for a predicate on
/// a column carrying a `UNIQUE` constraint. This prints the access path the
/// planner and the emitted program choose for the keeper's exact shape, for an
/// implicit `sqlite_autoindex_*`, an explicit `CREATE INDEX`, and a bound
/// parameter, so the divergence can be named. Not an assertion.
#[test]
#[ignore = "GH#409 access-path probe; run with --ignored --nocapture"]
fn gh409_probe_access_path_for_unique_column_equality() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gh409_access_path.db");
        let conn = Connection::open(&path.to_string_lossy()).await.unwrap();
        conn.execute("CREATE TABLE t (k TEXT NOT NULL UNIQUE, n INTEGER NOT NULL, pad TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE e (k TEXT NOT NULL, n INTEGER NOT NULL);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX e_k ON e(k);").await.unwrap();
        conn.execute("CREATE TABLE i (k INTEGER NOT NULL UNIQUE, n INTEGER NOT NULL);")
            .await
            .unwrap();
        for n in 0..50_i64 {
            conn.execute(&format!("INSERT INTO t(k, n, pad) VALUES ('k{n}', {n}, 'p');"))
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO e(k, n) VALUES ('k{n}', {n});"))
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO i(k, n) VALUES ({n}, {n});"))
                .await
                .unwrap();
        }

        for sql in [
            "SELECT n FROM t WHERE k = 'k7'",
            "SELECT n FROM t WHERE k = 'k7-nonexistent'",
            "SELECT n FROM e WHERE k = 'k7'",
            "SELECT n FROM i WHERE k = 7",
            "SELECT n FROM t",
        ] {
            let eqp = conn
                .query(&format!("EXPLAIN QUERY PLAN {sql}"))
                .await
                .unwrap()
                .into_iter()
                .map(|row| format!("{:?}", row.values()))
                .collect::<Vec<_>>()
                .join(" | ");
            let ops = conn
                .query(&format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .into_iter()
                .map(|row| match &row.values()[1] {
                    SqliteValue::Text(op) => op.to_string(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>();
            println!("--- {sql}\n    eqp: {eqp}\n    ops: {ops:?}");
        }
        conn.close().await.unwrap();
    });
}
