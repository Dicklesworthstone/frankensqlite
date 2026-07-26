//! Issue #111 perf repro: FK-on INSERTs with an explicit `INTEGER PRIMARY KEY`
//! child rowid must not scale super-linearly.
//!
//! Workload: `INSERT INTO child(id, parent_id) VALUES(?,?)` where `id` is an
//! explicit `INTEGER PRIMARY KEY`, `PRAGMA foreign_keys=ON`, in-memory DB, one
//! explicit transaction, one prepared statement, reused N times.
//!
//! Before the fix the per-insert cost grew from ~108us to ~635us over
//! N=1k..8k. The same workload with the FK removed is linear. This is NOT the
//! #110 memdb-reload bug: `memdb_refresh_count` stays 0 for this shape, which
//! this test asserts.

use std::time::Instant;

use fsqlite_core::connection::{Connection, hot_path_profile_snapshot, reset_hot_path_profile};
use fsqlite_types::SqliteValue;

/// Run N explicit-rowid FK-on inserts in a single txn with one prepared stmt.
/// Returns (microseconds-per-insert, memdb_refresh_count observed during the
/// hot loop).
async fn run_fk_insert(n: i64) -> (f64, u64) {
    let conn = Connection::open(":memory:").await.unwrap();
    conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));",
    )
    .await
    .unwrap();

    // Seed N distinct parents so every child row matches a distinct parent
    // (no FK-cache hit short-circuits the work — worst case for the path).
    conn.execute("BEGIN;").await.unwrap();
    let parent_stmt = conn
        .prepare("INSERT INTO parent(id) VALUES(?1)")
        .await
        .unwrap();
    for i in 1..=n {
        conn.execute_prepared_with_params(&parent_stmt, &[SqliteValue::Integer(i)])
            .await
            .unwrap();
    }
    conn.execute("COMMIT;").await.unwrap();

    let stmt = conn
        .prepare("INSERT INTO child(id, parent_id) VALUES(?1, ?2)")
        .await
        .unwrap();

    // Time the hot loop: one txn, one prepared stmt, explicit rowids.
    reset_hot_path_profile();
    conn.execute("BEGIN;").await.unwrap();
    let t0 = Instant::now();
    for i in 1..=n {
        let affected = conn
            .execute_prepared_with_params(
                &stmt,
                &[SqliteValue::Integer(i), SqliteValue::Integer(i)],
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);
    }
    let elapsed = t0.elapsed();
    let memdb_refresh = hot_path_profile_snapshot().memdb_refresh_count;
    conn.execute("COMMIT;").await.unwrap();

    // Sanity: all rows landed.
    let rows = conn.query("SELECT COUNT(*) FROM child;").await.unwrap();
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(n));

    let us_per_insert = elapsed.as_secs_f64() * 1e6 / n as f64;
    (us_per_insert, memdb_refresh)
}

/// Control: identical workload but the child has no FK.
async fn run_no_fk_insert(n: i64) -> f64 {
    let conn = Connection::open(":memory:").await.unwrap();
    conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER);")
        .await
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO child(id, parent_id) VALUES(?1, ?2)")
        .await
        .unwrap();

    conn.execute("BEGIN;").await.unwrap();
    let t0 = Instant::now();
    for i in 1..=n {
        conn.execute_prepared_with_params(
            &stmt,
            &[SqliteValue::Integer(i), SqliteValue::Integer(i)],
        )
        .await
        .unwrap();
    }
    let elapsed = t0.elapsed();
    conn.execute("COMMIT;").await.unwrap();

    elapsed.as_secs_f64() * 1e6 / n as f64
}

#[test]
fn fk_explicit_rowid_insert_scales_flat() {
    asupersync::test_utils::run_test(|| async {
        let sizes = [1000_i64, 2000, 4000, 8000];
        let mut fk = Vec::new();
        let mut nofk = Vec::new();
        for &n in &sizes {
            let (us, refreshes) = run_fk_insert(n).await;
            // Distinct from #110: this shape must not trigger memdb reloads.
            assert_eq!(
                refreshes, 0,
                "FK explicit-rowid insert at N={n} triggered {refreshes} memdb refreshes (expected 0; this would be the #110 bug, not #111)"
            );
            let control = run_no_fk_insert(n).await;
            eprintln!(
                "N={n:>5}  FK-on={us:8.3} us/insert   no-FK={control:8.3} us/insert   ratio={:.2}x",
                us / control
            );
            fk.push(us);
            nofk.push(control);
        }

        // Scaling check: the per-insert cost at N=8000 must not blow up versus
        // N=1000. The bug showed ~6x growth (108 -> 635 us). Flat/linear-per-row
        // should keep the ratio low. Allow generous slack for CI noise but catch
        // the super-linear regression.
        let growth = fk[fk.len() - 1] / fk[0];
        eprintln!(
            "FK per-insert growth N=1k->8k: {:.2}x ({:.3} -> {:.3} us)",
            growth,
            fk[0],
            fk[fk.len() - 1]
        );
        assert!(
            growth < 2.5,
            "FK explicit-rowid insert grew {growth:.2}x from N=1k to N=8k (per-insert {:.3} -> {:.3} us); expected near-flat scaling. Super-linear regression (#111).",
            fk[0],
            fk[fk.len() - 1]
        );
    });
}
