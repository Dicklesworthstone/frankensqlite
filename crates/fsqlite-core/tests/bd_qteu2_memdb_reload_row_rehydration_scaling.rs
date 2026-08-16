//! bd-qteu2 / GH#345 residual: an ingestion loop must not re-hydrate the rows of
//! tables it never touches.
//!
//! bd-420r8 killed the schema-REPARSE term of the per-write memdb reload
//! (`O(rows_written × schema_text)`, memoized per schema generation). The
//! RESIDUAL term this keeper guards is the row-REHYDRATION term: a trigger
//! `WHEN` clause containing a subquery, evaluated during nested-write ingestion,
//! runs a nested `execute_statement` that reaches
//! `refresh_memdb_from_active_txn_if_dirty`. Because a prior write left the memdb
//! dirty, that refresh runs a full `reload_memdb_from_txn_with_mode` which — for
//! an eagerly-hydrated (`:memory:`) connection — re-scans EVERY table's rows into
//! the mirror, even tables the ingestion never reads or writes. So a burst of `N`
//! writes over a store holding `R` stable rows costs `O(N × R)` row hydrations —
//! quadratic, and the dominant term at the reporter's high row counts.
//!
//! The invariant asserted here is machine-independent and robust to the exact
//! fix mechanism (lazy per-table hydration, deferred hydration, or in-place
//! mirror update): the number of rows hydrated during an ingestion burst must be
//! essentially INSENSITIVE to the size of stable tables the ingestion never
//! touches. We run the identical ingestion twice, differing ONLY in how many
//! rows the untouched stable tables hold, and assert the extra hydration is at
//! most a one-time full load of the extra rows — NOT the per-write re-scan the
//! regression performs. Pre-fix the larger store re-scans its extra rows on
//! every write, so the delta scales with `writes × extra_rows` and cannot
//! satisfy the bound. `BD_QTEU2_MEASURE=1` prints both runs' hydration counts.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const STABLE_TABLES: usize = 15;
const INGEST_ROWS: usize = 150;
const SMALL_STABLE_ROWS: usize = 4;
const LARGE_STABLE_ROWS: usize = 200;

/// Build a `:memory:` store with `STABLE_TABLES` populated tables the ingestion
/// never touches, an ingestion target `main`, a `sink` the trigger writes, and a
/// BEFORE-INSERT trigger whose `WHEN` contains a subquery — the exact shape that
/// reaches the per-write memdb reload during nested-write ingestion (bd-420r8).
/// Runs an ingestion burst and returns the number of user-table rows hydrated
/// into the MemDatabase mirror DURING the burst.
async fn hydration_during_ingestion(stable_rows_each: usize) -> u64 {
    let conn = Connection::open(":memory:").await.unwrap();

    let mut ddl = String::new();
    for t in 0..STABLE_TABLES {
        ddl.push_str(&format!(
            "CREATE TABLE stable_{t}(id INTEGER PRIMARY KEY, a TEXT, b TEXT);"
        ));
    }
    ddl.push_str("CREATE TABLE main(id INTEGER PRIMARY KEY, v TEXT);");
    ddl.push_str("CREATE TABLE sink(id INTEGER PRIMARY KEY, v TEXT);");
    ddl.push_str(
        "CREATE TRIGGER main_guard BEFORE INSERT ON main \
         WHEN (SELECT count(*) FROM sink) >= 0 \
         BEGIN INSERT INTO sink(v) VALUES (NEW.v); END;",
    );
    conn.execute_batch(&ddl).await.unwrap();

    // Populate the stable tables. These are NEVER read or written during the
    // ingestion burst below, so a correct engine has no reason to re-hydrate
    // their rows per write.
    for t in 0..STABLE_TABLES {
        let mut batch = String::from("BEGIN;");
        for r in 0..stable_rows_each {
            batch.push_str(&format!(
                "INSERT INTO stable_{t}(a, b) VALUES ('a{r}', 'b{r}');"
            ));
        }
        batch.push_str("COMMIT;");
        conn.execute_batch(&batch).await.unwrap();
    }

    // Measure row hydrations across ONLY the ingestion phase.
    let before = conn.memdb_row_hydration_count();
    let mut batch = String::from("BEGIN;");
    for i in 0..INGEST_ROWS {
        batch.push_str(&format!("INSERT INTO main(v) VALUES ('row-{i}');"));
    }
    batch.push_str("COMMIT;");
    conn.execute_batch(&batch).await.unwrap();
    let delta = conn.memdb_row_hydration_count() - before;

    // Correctness: the trigger fired once per ingested row, and the untouched
    // stable tables still read back exactly what we stored.
    let rows = conn.query("SELECT count(*) FROM sink;").await.unwrap();
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(INGEST_ROWS as i64));
    let main_rows = conn.query("SELECT count(*) FROM main;").await.unwrap();
    assert_eq!(
        main_rows[0].values()[0],
        SqliteValue::Integer(INGEST_ROWS as i64)
    );
    let stable0 = conn
        .query("SELECT count(*) FROM stable_0;")
        .await
        .unwrap();
    assert_eq!(
        stable0[0].values()[0],
        SqliteValue::Integer(stable_rows_each as i64),
        "untouched stable table must still hold all its rows"
    );

    delta
}

#[test]
fn ingestion_does_not_rehydrate_untouched_table_rows_per_write() {
    asupersync::test_utils::run_test(|| async {
        let small = hydration_during_ingestion(SMALL_STABLE_ROWS).await;
        let large = hydration_during_ingestion(LARGE_STABLE_ROWS).await;

        if std::env::var("BD_QTEU2_MEASURE").is_ok() {
            eprintln!(
                "BD_QTEU2: {INGEST_ROWS} inserts through a subquery-WHEN trigger over \
                 {STABLE_TABLES} untouched stable tables — rows hydrated during ingestion: \
                 small({SMALL_STABLE_ROWS}/table)={small}, large({LARGE_STABLE_ROWS}/table)={large}"
            );
        }

        // The two runs differ ONLY in the size of tables the ingestion never
        // touches. The extra rows a correct engine may hydrate is at most a
        // one-time full load of them (× a slack constant for any per-table
        // placeholder work) — NOT the per-write re-scan the regression performs.
        //
        // Pre-fix: each of INGEST_ROWS writes re-scans every stable table, so
        //   large - small ≈ INGEST_ROWS × STABLE_TABLES × (LARGE - SMALL)
        //   ≈ 150 × 15 × 196 ≈ 441,000 — far above the bound.
        // Post-fix (any lazy/deferred/in-place mechanism): the untouched tables
        // are hydrated at most once, so large - small ≤ STABLE_TABLES ×
        //   (LARGE - SMALL) × slack ≈ small.
        let extra_stable_rows = (STABLE_TABLES * (LARGE_STABLE_ROWS - SMALL_STABLE_ROWS)) as u64;
        let slack: u64 = 2;
        let bound = extra_stable_rows * slack;
        assert!(
            large.saturating_sub(small) <= bound,
            "GH#345 / bd-qteu2: ingestion hydration scaled with untouched-table size — \
             small={small}, large={large}, delta={} exceeds one-time bound {bound} \
             (per-write row re-hydration regression: expected O(total_rows + writes), \
             got O(writes × total_rows))",
            large.saturating_sub(small)
        );
    });
}

/// Correctness guard for the ONE dangerous site the lazy-hydration change
/// touches: `fk_parent_rowid_fast_lookup`. Pre-fix it treated a MISS in the
/// mirror as authoritative for `:memory:` (`if self.pager.is_memory() || ...`).
/// Once the per-write active-txn refresh is lazy, a `:memory:` mirror can hold
/// only empty schema placeholders, so that miss reported a SPURIOUS foreign-key
/// violation for a parent that exists on disk. The fix makes a miss authoritative
/// only when the mirror is actually hydrated, falling back to the pager-backed
/// validation SELECT otherwise. This test forces an FK check against an unhydrated
/// mirror (an in-transaction parent write dirties the mirror before the child
/// insert) and asserts both directions: an existing parent is accepted and a
/// genuinely missing parent is still rejected.
#[test]
fn fk_child_insert_is_correct_under_lazy_hydration() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT);\
             CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id));",
        )
        .await
        .unwrap();

        // Populate parents and COMMIT so they live on disk (the pager), not just
        // in a hydrated mirror.
        let mut seed = String::from("BEGIN;");
        for p in 1..=50 {
            seed.push_str(&format!("INSERT INTO parent(id, v) VALUES ({p}, 'p{p}');"));
        }
        seed.push_str("COMMIT;");
        conn.execute_batch(&seed).await.unwrap();

        conn.execute("PRAGMA foreign_keys=ON;").await.unwrap();

        // In one transaction, first write a NEW parent (this dirties the mirror,
        // arming the lazy per-write active-txn refresh), then insert children
        // that reference parents which exist on disk. Under the fix the FK
        // fast-lookup misses the unhydrated mirror and falls back to the
        // pager-backed validation SELECT, so these MUST succeed.
        conn.execute_batch(
            "BEGIN;\
             INSERT INTO parent(id, v) VALUES (51, 'p51');\
             INSERT INTO child(id, pid) VALUES (1, 1);\
             INSERT INTO child(id, pid) VALUES (2, 50);\
             INSERT INTO child(id, pid) VALUES (3, 51);\
             COMMIT;",
        )
        .await
        .expect("bd-qteu2: FK child insert against an on-disk parent must not false-violate");

        let child_count = conn.query("SELECT count(*) FROM child;").await.unwrap();
        assert_eq!(
            child_count[0].values()[0],
            SqliteValue::Integer(3),
            "all three children referencing existing parents must be inserted"
        );

        // Negative: a child referencing a parent that does NOT exist must still
        // be rejected — the lazy fallback preserves FK enforcement.
        let err = conn
            .execute("INSERT INTO child(id, pid) VALUES (4, 9999);")
            .await;
        assert!(
            err.is_err(),
            "bd-qteu2: FK enforcement must still reject a child referencing a missing parent"
        );
        let child_count_after = conn.query("SELECT count(*) FROM child;").await.unwrap();
        assert_eq!(
            child_count_after[0].values()[0],
            SqliteValue::Integer(3),
            "the rejected child must not have been inserted"
        );
    });
}
