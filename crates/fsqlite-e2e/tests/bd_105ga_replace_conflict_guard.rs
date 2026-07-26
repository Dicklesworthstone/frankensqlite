//! bd-nhc6g — regression guard for the seek-cache / rootless-stack defect that
//! corrupted real databases under `INSERT OR REPLACE` churn (bd-105ga,
//! bd-kwei8).
//!
//! # The defect this pins
//!
//! `crates/fsqlite-btree/src/cursor.rs` documents it: the table seek-cache fast
//! path (`try_table_seek_cache`) rebuilds the cursor stack as *just the landing
//! leaf, with no path from the root*. Mutating from such a rootless stack used
//! to silently disable `balance_for_delete` (depth one was read as "the leaf IS
//! the root"), the pre-delete anchor capture, and separator repair — so a leaf
//! drained to zero cells stayed referenced by its parent interior page, a shape
//! stock SQLite rejects as `database disk image is malformed`. The documented
//! way in is the `INSERT OR REPLACE` conflict path: the failed insert's seek
//! primes the cache, and `native_replace_row`'s follow-up `table_move_to` then
//! lands on the fast path.
//!
//! Observed in the wild on published fsqlite 0.1.12: a real 3,235-issue
//! database was corrupted deterministically, damage landing on the *interior
//! root* pages, with the engine reporting `table rowid seek on root 2 missed
//! scan-visible rowid 986 on successor page 663`.
//!
//! # Why this test is shaped the way it is
//!
//! The existing unit coverage
//! (`test_balance_choke_points_reject_nonroot_single_entry_paths`) hand-builds a
//! three-page tree, pushes a rootless leaf onto the stack, and asserts the
//! balance entry points fail closed. That pins the *choke point*, but it cannot
//! see the original bug, which was mutation paths reaching balance without the
//! guard at all — a regression reintroducing an unguarded path keeps that unit
//! test green.
//!
//! So this test drives the real workload end to end and asks C SQLite for the
//! verdict:
//!
//! 1. a rowid table with a **secondary UNIQUE index** (a `TEXT PRIMARY KEY`,
//!    exactly the `export_hashes` / `issues` shape that corrupted);
//! 2. enough rows that the table root is an **interior** page, since the damage
//!    only manifests on multi-level trees — asserted, not assumed;
//! 3. repeated `INSERT OR REPLACE` on **existing** keys, so each conflicting
//!    insert primes the seek cache before the replace re-seeks;
//! 4. interleaved deletes, to run `balance_for_delete` on drained leaves;
//! 5. an independent `PRAGMA integrity_check` via rusqlite on the closed file,
//!    plus a row-count check (bd-kwei8 recorded silent table emptying).
//!
//! The verdict deliberately comes from C SQLite rather than our own checker:
//! the failure mode is "our engine is happy, stock SQLite says malformed".

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

/// Rows inserted before the churn phase. Chosen so the table is comfortably
/// multi-level at any supported page size; the test asserts this rather than
/// trusting it.
const SEED_ROWS: usize = 2_000;
/// How many existing keys get replaced. Every one is a genuine UNIQUE conflict.
const REPLACE_ROWS: usize = 800;
/// Payload width, to push leaves toward splitting without needing overflow.
const PAYLOAD: usize = 120;

fn key_for(i: usize) -> String {
    // Non-monotonic textual keys so replaces land across the whole key space
    // rather than clustering on one hot leaf.
    format!("k-{:06}-{}", (i * 7919) % 1_000_000, i)
}

#[test]
fn replace_conflict_churn_leaves_a_stock_valid_database() {
    let dir = tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir");
    let db_path = dir.path().join("bd105ga-replace-guard.db");
    let db_str = db_path.to_str().expect("utf-8 temp path").to_owned();

    asupersync::test_utils::run_test(|| {
        let db_str = db_str.clone();
        async move {
            let conn = Connection::open(db_str).await.expect("open db");

            // `id TEXT PRIMARY KEY` on a rowid table means a secondary UNIQUE
            // index (sqlite_autoindex_*), which is what makes REPLACE resolve a
            // victim through the index rather than by rowid.
            conn.execute(
                "CREATE TABLE items (
                     id TEXT PRIMARY KEY,
                     bucket INTEGER NOT NULL,
                     payload TEXT NOT NULL
                 )",
            )
            .await
            .expect("create table");

            let payload = "p".repeat(PAYLOAD);

            conn.execute("BEGIN IMMEDIATE").await.expect("begin seed");
            for i in 0..SEED_ROWS {
                conn.execute_with_params(
                    "INSERT INTO items (id, bucket, payload) VALUES (?, ?, ?)",
                    &[
                        SqliteValue::from(key_for(i).as_str()),
                        SqliteValue::Integer((i % 16) as i64),
                        SqliteValue::from(payload.as_str()),
                    ],
                )
                .await
                .unwrap_or_else(|e| panic!("seed insert {i}: {e}"));
            }
            conn.execute("COMMIT").await.expect("commit seed");

            // The defect only manifests on a multi-level tree, where a wrong
            // separator or child pointer can strand a leaf under an interior
            // root. Prove we built one instead of assuming it.
            let page_rows = conn.query("PRAGMA page_count;").await.expect("page_count");
            let page_count = page_rows
                .first()
                .and_then(|r| r.values().first().and_then(SqliteValue::as_integer))
                .expect("page_count value");
            assert!(
                page_count > 32,
                "table did not grow past a single-level tree (page_count={page_count}); \
                 raise SEED_ROWS/PAYLOAD or this test cannot exercise interior-page balance"
            );

            // The churn phase: every INSERT OR REPLACE below collides with an
            // existing key, so the failed insert seeks (priming the seek cache)
            // and the replace then re-seeks to delete the victim. Deletes are
            // interleaved so drained leaves also go through balance_for_delete.
            conn.execute("BEGIN IMMEDIATE").await.expect("begin churn");
            for i in 0..REPLACE_ROWS {
                conn.execute_with_params(
                    "INSERT OR REPLACE INTO items (id, bucket, payload) VALUES (?, ?, ?)",
                    &[
                        SqliteValue::from(key_for(i).as_str()),
                        SqliteValue::Integer(((i % 16) + 100) as i64),
                        SqliteValue::from(payload.as_str()),
                    ],
                )
                .await
                .unwrap_or_else(|e| panic!("replace {i}: {e}"));

                if i % 5 == 0 {
                    // Delete a key from a different region than the one just
                    // replaced, so deletions drain leaves the replace churn is
                    // not currently sitting on.
                    let victim = SEED_ROWS - 1 - (i % SEED_ROWS);
                    conn.execute_with_params(
                        "DELETE FROM items WHERE id = ?",
                        &[SqliteValue::from(key_for(victim).as_str())],
                    )
                    .await
                    .unwrap_or_else(|e| panic!("delete {victim}: {e}"));
                }
            }
            conn.execute("COMMIT").await.expect("commit churn");

            let engine_check = conn
                .query("PRAGMA integrity_check;")
                .await
                .expect("engine integrity_check");
            let engine_verdict: Vec<String> = engine_check
                .iter()
                .map(|row| match row.values().first() {
                    Some(SqliteValue::Text(s)) => s.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            assert_eq!(
                engine_verdict,
                vec!["ok".to_owned()],
                "engine integrity_check failed after REPLACE-conflict churn"
            );

            conn.close().await.expect("close");
        }
    });

    // Independent verdict. The historical failure is precisely "our engine is
    // satisfied but stock SQLite sees a malformed image", so this assertion —
    // not the engine's own — is the point of the test.
    let c = rusqlite::Connection::open(&db_path).expect("rusqlite open");
    let verdict: String = c
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("rusqlite integrity_check");
    assert_eq!(
        verdict, "ok",
        "C SQLite reported a malformed database after REPLACE-conflict churn \
         (the bd-105ga / bd-kwei8 signature)"
    );

    // bd-kwei8 recorded silent table emptying during repair attempts, so guard
    // against data loss as well as structural damage.
    let live: i64 = c
        .query_row("SELECT COUNT(*) FROM items;", [], |row| row.get(0))
        .expect("count rows");
    let expected_deletes = REPLACE_ROWS.div_ceil(5);
    let expected = (SEED_ROWS - expected_deletes) as i64;
    assert_eq!(
        live, expected,
        "row count drifted after REPLACE churn: expected {expected}, found {live}"
    );
}
