#![recursion_limit = "512"]

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
async fn hydration_during_ingestion(stable_rows_each: usize, with_cte: bool) -> u64 {
    let conn = Connection::open(":memory:").await.unwrap();

    let mut ddl = String::new();
    for t in 0..STABLE_TABLES {
        ddl.push_str(&format!(
            "CREATE TABLE stable_{t}(id INTEGER PRIMARY KEY, a TEXT, b TEXT);"
        ));
    }
    ddl.push_str("CREATE TABLE main(id INTEGER PRIMARY KEY, v TEXT);");
    ddl.push_str("CREATE TABLE sink(id INTEGER PRIMARY KEY, v TEXT);");
    if with_cte {
        ddl.push_str("CREATE TABLE source(v TEXT);");
    }
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

    if with_cte {
        // Match bd-1ucvb's persistent SELECT source: materialization may load
        // the initial row image once. Trigger replay must not keep reloading
        // the untouched tables along with that source image.
        let mut seed = String::from("BEGIN;");
        for i in 0..INGEST_ROWS {
            seed.push_str(&format!("INSERT INTO source(v) VALUES ('row-{i}');"));
        }
        seed.push_str("COMMIT;");
        conn.execute_batch(&seed).await.unwrap();
    }

    // Measure row hydrations across ONLY the ingestion phase.
    let before = conn.memdb_row_hydration_count();
    let mut batch = String::from("BEGIN;");
    if with_cte {
        batch.push_str(
            "WITH c(v) AS (SELECT v FROM source) \
             INSERT INTO main(v) SELECT v FROM c;",
        );
    } else {
        for i in 0..INGEST_ROWS {
            batch.push_str(&format!("INSERT INTO main(v) VALUES ('row-{i}');"));
        }
    }
    batch.push_str("COMMIT;");
    conn.execute_batch(&batch).await.unwrap();
    let delta = conn.memdb_row_hydration_count() - before;

    // Correctness: the trigger fired once per ingested row, and the untouched
    // stable tables still read back exactly what we stored.
    let rows = conn.query("SELECT count(*) FROM sink;").await.unwrap();
    assert_eq!(
        rows[0].values()[0],
        SqliteValue::Integer(INGEST_ROWS as i64)
    );
    let main_rows = conn.query("SELECT count(*) FROM main;").await.unwrap();
    assert_eq!(
        main_rows[0].values()[0],
        SqliteValue::Integer(INGEST_ROWS as i64)
    );
    let stable0 = conn.query("SELECT count(*) FROM stable_0;").await.unwrap();
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
        let small = hydration_during_ingestion(SMALL_STABLE_ROWS, false).await;
        let large = hydration_during_ingestion(LARGE_STABLE_ROWS, false).await;

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

/// bd-1ucvb: WITH INSERT's materialization may hydrate the source once, but
/// replay through a trigger must not retain that fallback override and hydrate
/// every untouched table again for every source row. Counters distinguish this
/// from the one-time source load without depending on machine speed.
#[test]
fn with_insert_trigger_replay_does_not_rehydrate_untouched_rows_per_source_row() {
    asupersync::test_utils::run_test(|| async {
        let small = hydration_during_ingestion(SMALL_STABLE_ROWS, true).await;
        let large = hydration_during_ingestion(LARGE_STABLE_ROWS, true).await;
        let extra_stable_rows = (STABLE_TABLES * (LARGE_STABLE_ROWS - SMALL_STABLE_ROWS)) as u64;
        let bound = extra_stable_rows * 2;

        if std::env::var("BD_QTEU2_MEASURE").is_ok() {
            eprintln!(
                "BD_1UCVB: {INGEST_ROWS} CTE source rows replayed through a subquery-WHEN \
                 trigger: small={small}, large={large}, extra-row hydration bound={bound}"
            );
        }

        assert!(
            large.saturating_sub(small) <= bound,
            "bd-1ucvb: WITH INSERT trigger replay hydrated untouched tables per source row: \
             small={small}, large={large}, delta={} exceeds one-time bound {bound}",
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

#[derive(Clone, Copy, Debug)]
enum Rv46nBackend {
    Memory,
    FileDelete,
    FileWal,
}

impl Rv46nBackend {
    const fn label(self) -> &'static str {
        match self {
            Self::Memory => ":memory:",
            Self::FileDelete => "file-delete",
            Self::FileWal => "file-wal",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Rv46nDml {
    UpdateSubquery,
    UpdateFromReturning,
    DeleteSubquery,
    DeleteExistsReturning,
}

impl Rv46nDml {
    const fn label(self) -> &'static str {
        match self {
            Self::UpdateSubquery => "with-update-subquery",
            Self::UpdateFromReturning => "with-update-from-returning",
            Self::DeleteSubquery => "with-delete-subquery",
            Self::DeleteExistsReturning => "with-delete-exists-returning",
        }
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let cols = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(cols);
        for i in 0..cols {
            let v = match row.get_ref_unwrap(i) {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(n) => format!("I:{n}"),
                rusqlite::types::ValueRef::Real(f) => format!("R:{f:?}"),
                rusqlite::types::ValueRef::Text(b) => {
                    format!("T:{}", String::from_utf8_lossy(b))
                }
                rusqlite::types::ValueRef::Blob(b) => format!("B:{b:?}"),
            };
            out.push(v);
        }
        Ok(out)
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn frank_rows_fmt(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => format!("I:{n}"),
                    SqliteValue::Float(f) => format!("R:{f:?}"),
                    SqliteValue::Text(s) => format!("T:{s}"),
                    SqliteValue::Blob(b) => format!("B:{b:?}"),
                })
                .collect()
        })
        .collect()
}

/// bd-rv46n: run a WITH ... UPDATE or WITH ... DELETE over `INGEST_ROWS` rows
/// on a target table with BEFORE and AFTER triggers whose WHEN clauses query a
/// VIEW over `sink` and whose bodies write to `sink`, alongside `STABLE_TABLES`
/// untouched tables. Returns `(hydrated_delta, returning_rows, main_rows, sink_rows)`
/// and verifies parity against live bundled rusqlite (3.54.1).
async fn run_rv46n_with_dml_scaling_case(
    backend: Rv46nBackend,
    dml: Rv46nDml,
    stable_rows_each: usize,
) -> u64 {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let frank = match backend {
        Rv46nBackend::Memory => Connection::open(":memory:").await.unwrap(),
        Rv46nBackend::FileDelete | Rv46nBackend::FileWal => {
            Connection::open(temp.path().to_str().unwrap())
                .await
                .unwrap()
        }
    };
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    if matches!(backend, Rv46nBackend::FileWal) {
        frank.execute("PRAGMA journal_mode=WAL;").await.unwrap();
    }

    let mut ddl = String::new();
    for t in 0..STABLE_TABLES {
        ddl.push_str(&format!(
            "CREATE TABLE stable_{t}(id INTEGER PRIMARY KEY, a TEXT, b TEXT);"
        ));
    }
    ddl.push_str("CREATE TABLE main(id INTEGER PRIMARY KEY, v TEXT);");
    ddl.push_str("CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT);");
    ddl.push_str("CREATE TABLE sink(id INTEGER PRIMARY KEY, msg TEXT);");
    ddl.push_str("CREATE VIEW sink_v AS SELECT count(*) AS n FROM sink;");
    ddl.push_str(
        "CREATE TRIGGER main_bu BEFORE UPDATE ON main \
         WHEN (SELECT n FROM sink_v) >= 0 \
         BEGIN INSERT INTO sink(msg) VALUES ('bu:' || OLD.id || ':' || OLD.v || '->' || NEW.v); END;\
         CREATE TRIGGER main_au AFTER UPDATE ON main \
         WHEN (SELECT count(*) FROM sink) >= 0 \
         BEGIN INSERT INTO sink(msg) VALUES ('au:' || NEW.id || ':' || NEW.v); END;\
         CREATE TRIGGER main_bd BEFORE DELETE ON main \
         WHEN (SELECT n FROM sink_v) >= 0 \
         BEGIN INSERT INTO sink(msg) VALUES ('bd:' || OLD.id || ':' || OLD.v); END;\
         CREATE TRIGGER main_ad AFTER DELETE ON main \
         WHEN (SELECT count(*) FROM sink) >= 0 \
         BEGIN INSERT INTO sink(msg) VALUES ('ad:' || OLD.id || ':' || OLD.v); END;",
    );
    frank.execute_batch(&ddl).await.unwrap();
    stock.execute_batch(&ddl).unwrap();

    for t in 0..STABLE_TABLES {
        let mut batch = String::from("BEGIN;");
        for r in 0..stable_rows_each {
            batch.push_str(&format!(
                "INSERT INTO stable_{t}(a, b) VALUES ('a{r}', 'b{r}');"
            ));
        }
        batch.push_str("COMMIT;");
        frank.execute_batch(&batch).await.unwrap();
        stock.execute_batch(&batch).unwrap();
    }

    let mut seed = String::from("BEGIN;");
    for i in 1..=INGEST_ROWS {
        seed.push_str(&format!(
            "INSERT INTO main(id, v) VALUES ({i}, 'orig-{i}');\
             INSERT INTO src(id, v) VALUES ({i}, 'next-{i}');"
        ));
    }
    seed.push_str("COMMIT;");
    frank.execute_batch(&seed).await.unwrap();
    stock.execute_batch(&seed).unwrap();

    let stmt_sql = match dml {
        Rv46nDml::UpdateSubquery => {
            "WITH c(k, v) AS (SELECT id, v FROM src) \
             UPDATE main SET v = (SELECT v FROM c WHERE c.k = main.id) \
             WHERE id IN (SELECT k FROM c)"
        }
        Rv46nDml::UpdateFromReturning => {
            "WITH c(k, v) AS (SELECT id, v FROM src) \
             UPDATE main SET v = c.v || '-from' FROM c \
             WHERE main.id = c.k \
             RETURNING main.id, main.v"
        }
        Rv46nDml::DeleteSubquery => {
            "WITH c(k) AS (SELECT id FROM src) \
             DELETE FROM main WHERE id IN (SELECT k FROM c)"
        }
        Rv46nDml::DeleteExistsReturning => {
            "WITH c(k) AS (SELECT id FROM src) \
             DELETE FROM main WHERE EXISTS (SELECT 1 FROM c WHERE c.k = main.id) \
             RETURNING id, v"
        }
    };

    let before = frank.memdb_row_hydration_count();
    frank.execute("BEGIN;").await.unwrap();
    stock.execute_batch("BEGIN;").unwrap();
    let (frank_ret, stock_ret) = match dml {
        Rv46nDml::UpdateSubquery | Rv46nDml::DeleteSubquery => {
            let f_changed = frank.execute(stmt_sql).await.unwrap();
            let s_changed = stock.execute(stmt_sql, []).unwrap();
            assert_eq!(
                f_changed,
                s_changed,
                "bd-rv46n change count mismatch ({}, {})",
                backend.label(),
                dml.label()
            );
            (Vec::new(), Vec::new())
        }
        Rv46nDml::UpdateFromReturning | Rv46nDml::DeleteExistsReturning => {
            let f_rows = frank_rows_fmt(&frank.query(stmt_sql).await.unwrap());
            let s_rows = stock_rows(&stock, stmt_sql);
            (f_rows, s_rows)
        }
    };
    frank.execute("COMMIT;").await.unwrap();
    stock.execute_batch("COMMIT;").unwrap();
    let delta = frank.memdb_row_hydration_count() - before;

    assert_eq!(
        frank_ret,
        stock_ret,
        "bd-rv46n RETURNING mismatch ({}, {})",
        backend.label(),
        dml.label()
    );
    for check_sql in [
        "SELECT id, v FROM main ORDER BY id",
        "SELECT id, msg FROM sink ORDER BY id",
        "SELECT count(*) FROM stable_0",
    ] {
        let f_rows = frank_rows_fmt(&frank.query(check_sql).await.unwrap());
        let s_rows = stock_rows(&stock, check_sql);
        assert_eq!(
            f_rows,
            s_rows,
            "bd-rv46n post-state mismatch ({}, {}, `{check_sql}`)",
            backend.label(),
            dml.label()
        );
    }

    delta
}

/// bd-rv46n: WITH ... UPDATE holding the internal mem-fallback override across
/// BEFORE/AFTER UPDATE triggers (including a trigger WHEN reading a VIEW over
/// another table) must not re-hydrate untouched stable tables on each affected
/// row — across `:memory:`, file-backed DELETE, and file-backed WAL.
#[test]
fn with_update_trigger_replay_does_not_rehydrate_untouched_rows_per_affected_row() {
    asupersync::test_utils::run_test(|| async {
        for backend in [
            Rv46nBackend::Memory,
            Rv46nBackend::FileDelete,
            Rv46nBackend::FileWal,
        ] {
            for dml in [Rv46nDml::UpdateSubquery, Rv46nDml::UpdateFromReturning] {
                let small = run_rv46n_with_dml_scaling_case(backend, dml, SMALL_STABLE_ROWS).await;
                let large = run_rv46n_with_dml_scaling_case(backend, dml, LARGE_STABLE_ROWS).await;
                eprintln!(
                    "bd-rv46n receipt [{}/{}]: small={small} large={large}",
                    backend.label(),
                    dml.label()
                );
                assert_eq!(
                    small,
                    0,
                    "bd-rv46n [{}/{}]: WITH UPDATE hydrated rows on small stable set",
                    backend.label(),
                    dml.label()
                );
                assert_eq!(
                    large,
                    0,
                    "bd-rv46n [{}/{}]: WITH UPDATE hydrated rows on large stable set",
                    backend.label(),
                    dml.label()
                );
            }
        }
    });
}

/// bd-rv46n: WITH ... DELETE holding the internal mem-fallback override across
/// BEFORE/AFTER DELETE triggers (including a trigger WHEN reading a VIEW over
/// another table) must not re-hydrate untouched stable tables on each affected
/// row — across `:memory:`, file-backed DELETE, and file-backed WAL.
#[test]
fn with_delete_trigger_replay_does_not_rehydrate_untouched_rows_per_affected_row() {
    asupersync::test_utils::run_test(|| async {
        for backend in [
            Rv46nBackend::Memory,
            Rv46nBackend::FileDelete,
            Rv46nBackend::FileWal,
        ] {
            for dml in [Rv46nDml::DeleteSubquery, Rv46nDml::DeleteExistsReturning] {
                let small = run_rv46n_with_dml_scaling_case(backend, dml, SMALL_STABLE_ROWS).await;
                let large = run_rv46n_with_dml_scaling_case(backend, dml, LARGE_STABLE_ROWS).await;
                eprintln!(
                    "bd-rv46n receipt [{}/{}]: small={small} large={large}",
                    backend.label(),
                    dml.label()
                );
                assert_eq!(
                    small,
                    0,
                    "bd-rv46n [{}/{}]: WITH DELETE hydrated rows on small stable set",
                    backend.label(),
                    dml.label()
                );
                assert_eq!(
                    large,
                    0,
                    "bd-rv46n [{}/{}]: WITH DELETE hydrated rows on large stable set",
                    backend.label(),
                    dml.label()
                );
            }
        }
    });
}

/// bd-rv46n: ATTACHed-database WITH ... UPDATE and WITH ... DELETE with
/// triggers, plus CTE names that shadow a table written by the trigger body,
/// match stock 3.54.1 without hydrating untouched tables.
#[test]
fn with_update_and_delete_shadowed_and_attached_match_stock_with_zero_hydration() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main.db");
        let att_path = dir.path().join("att.db");
        let main_str = main_path.to_str().unwrap();
        let att_str = att_path.to_str().unwrap();

        let frank = Connection::open(main_str).await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let stock_att_path = dir.path().join("stock_att.db");
        let stock_att_str = stock_att_path.to_str().unwrap();

        frank
            .execute(&format!("ATTACH DATABASE '{att_str}' AS att;"))
            .await
            .unwrap();
        stock
            .execute_batch(&format!("ATTACH DATABASE '{stock_att_str}' AS att;"))
            .unwrap();

        let setup = "\
            CREATE TABLE untouched(id INTEGER PRIMARY KEY, pad TEXT);\
            CREATE TABLE target(id INTEGER PRIMARY KEY, v TEXT);\
            CREATE TABLE sink(id INTEGER PRIMARY KEY, msg TEXT);\
            CREATE TRIGGER t_bu BEFORE UPDATE ON target WHEN (SELECT count(*) FROM sink) >= 0 \
            BEGIN INSERT INTO sink(msg) VALUES ('u:' || OLD.id || ':' || NEW.v); END;\
            CREATE TRIGGER t_bd BEFORE DELETE ON target WHEN (SELECT count(*) FROM sink) >= 0 \
            BEGIN INSERT INTO sink(msg) VALUES ('d:' || OLD.id || ':' || OLD.v); END;\
            CREATE TABLE att.untouched(id INTEGER PRIMARY KEY, pad TEXT);\
            CREATE TABLE att.target(id INTEGER PRIMARY KEY, v TEXT);\
            CREATE TABLE att.sink(id INTEGER PRIMARY KEY, msg TEXT);\
            CREATE TRIGGER att.at_bu BEFORE UPDATE ON target WHEN (SELECT count(*) FROM sink) >= 0 \
            BEGIN INSERT INTO sink(msg) VALUES ('au:' || OLD.id || ':' || NEW.v); END;\
            CREATE TRIGGER att.at_bd BEFORE DELETE ON target WHEN (SELECT count(*) FROM sink) >= 0 \
            BEGIN INSERT INTO sink(msg) VALUES ('ad:' || OLD.id || ':' || OLD.v); END;";
        frank.execute_batch(setup).await.unwrap();
        stock.execute_batch(setup).unwrap();

        let mut seed = String::from("BEGIN;");
        for i in 1..=300 {
            seed.push_str(&format!(
                "INSERT INTO untouched(id, pad) VALUES ({i}, 'pad-{i}');\
                 INSERT INTO att.untouched(id, pad) VALUES ({i}, 'apad-{i}');"
            ));
        }
        for i in 1..=25 {
            seed.push_str(&format!(
                "INSERT INTO target(id, v) VALUES ({i}, 'v{i}');\
                 INSERT INTO att.target(id, v) VALUES ({i}, 'av{i}');"
            ));
        }
        seed.push_str("COMMIT;");
        frank.execute_batch(&seed).await.unwrap();
        stock.execute_batch(&seed).unwrap();

        let before = frank.memdb_row_hydration_count();
        for sql in [
            // CTE shadows trigger target `sink`: trigger must write real `sink`.
            "WITH sink(k, nv) AS (SELECT id, 's-' || id FROM target WHERE id <= 15) \
             UPDATE target SET v = (SELECT nv FROM sink WHERE sink.k = target.id) \
             WHERE id IN (SELECT k FROM sink)",
            "WITH sink(k) AS (SELECT id FROM target WHERE id <= 10) \
             DELETE FROM target WHERE id IN (SELECT k FROM sink)",
            // ATTACHed target with triggers + untouched rows.
            "WITH c(k, nv) AS (SELECT id, 'as-' || id FROM att.target WHERE id <= 15) \
             UPDATE att.target SET v = (SELECT nv FROM c WHERE c.k = att.target.id) \
             WHERE id IN (SELECT k FROM c)",
            "WITH c(k) AS (SELECT id FROM att.target WHERE id <= 10) \
             DELETE FROM att.target WHERE id IN (SELECT k FROM c)",
        ] {
            let f_changed = frank.execute(sql).await.unwrap();
            let s_changed = stock.execute(sql, []).unwrap();
            assert_eq!(f_changed, s_changed, "change count mismatch for `{sql}`");
        }
        let delta = frank.memdb_row_hydration_count() - before;
        assert_eq!(
            delta, 0,
            "bd-rv46n: shadowed + ATTACHed WITH UPDATE/DELETE must hydrate 0 rows"
        );

        for check_sql in [
            "SELECT id, v FROM target ORDER BY id",
            "SELECT id, msg FROM sink ORDER BY id",
            "SELECT id, v FROM att.target ORDER BY id",
            "SELECT id, msg FROM att.sink ORDER BY id",
            "SELECT count(*) FROM untouched",
            "SELECT count(*) FROM att.untouched",
        ] {
            let f_rows = frank_rows_fmt(&frank.query(check_sql).await.unwrap());
            let s_rows = stock_rows(&stock, check_sql);
            assert_eq!(f_rows, s_rows, "post-state mismatch for `{check_sql}`");
        }
    });
}
