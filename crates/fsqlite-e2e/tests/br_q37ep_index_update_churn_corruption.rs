//! br-q37ep — regression repro: index B-tree must not corrupt under sustained
//! UPDATE / delete+reinsert churn of an indexed column.
//!
//! PROD incident (css 2026-06-16, ts2 2026-06-05): `storage.sqlite3` corrupted
//! repeatedly under the ATC experience write/UPDATE churn. `PRAGMA quick_check`
//! signature: `Tree NNNN page NNNN cell N: invalid page number <garbage>` on
//! `idx_atc_exp_open` (a plain INDEX on `atc_experiences(state)`). The ATC
//! operator UPDATEs `atc_experiences.state` (planned -> dispatched -> executed
//! -> resolved) at high frequency; an indexed-column UPDATE is a delete of the
//! old index entry + insert of the new one, so sustained churn drives heavy
//! index B-tree splits/merges/rebalances. The engine reportedly wrote garbage
//! child page numbers into the index B-tree's INTERIOR cells, and the version
//! store grew to ~10GB (MVCC page-version accumulation under the default
//! `concurrent_mode = ON` autocommit path).
//!
//! These tests reproduce the workload along every dimension the prior
//! reproductions (GH#113, the bead author) had not combined: the
//! low-cardinality `state` index profile, overflow-page payloads, heavy
//! delete+reinsert freelist churn, high iteration counts, MVCC autocommit, and
//! a canonical-SQLite cross-check of the committed file. Sizes are tunable via
//! `Q37EP_ROWS` / `Q37EP_ROUNDS` so the workload can be cranked without a
//! recompile when hunting a probabilistic trigger.

use fsqlite_types::SqliteValue;
use tempfile::TempDir;

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `PRAGMA quick_check` -> `Ok(())` when the single row is `"ok"`, else the
/// first reported corruption line. This is the exact surface the prod incident
/// tripped (`invalid page number` on an interior cell).
fn quick_check(conn: &fsqlite::Connection) -> Result<(), String> {
    let rows = conn
        .query("PRAGMA quick_check")
        .map_err(|e| e.to_string())?;
    match rows.first().map(|r| r.values().first().cloned()) {
        Some(Some(SqliteValue::Text(s))) => {
            let s = s.to_string();
            if s == "ok" { Ok(()) } else { Err(s) }
        }
        other => Err(format!("unexpected quick_check result: {other:?}")),
    }
}

/// Sustained UPDATE churn on an indexed TEXT column with all four states kept
/// populated. Each round advances ~1/STRIPE of the rows one state forward, so
/// the index always holds four key-regions separated by interior dividers —
/// maximizing interior-cell rebalancing. Pure autocommit (each UPDATE is its
/// own MVCC-versioned commit under the default `concurrent_mode = ON`).
fn churn_indexed_state(rows: i64, rounds: i64, stripe: i64) {
    let conn = fsqlite::Connection::open(":memory:").expect("open frank");
    conn.execute("CREATE TABLE atc_experiences (id INTEGER PRIMARY KEY, state TEXT, payload TEXT)")
        .expect("create table");
    conn.execute("CREATE INDEX idx_atc_exp_open ON atc_experiences(state)")
        .expect("create index");

    let states = ["planned", "dispatched", "executed", "resolved"];
    let mut chunk = String::new();
    for id in 1..=rows {
        if !chunk.is_empty() {
            chunk.push(',');
        }
        let st = states[(id as usize) % states.len()];
        chunk.push_str(&format!("({id},'{st}','payload-row-{id:08}-filler')"));
        if id % 500 == 0 || id == rows {
            conn.execute(&format!(
                "INSERT INTO atc_experiences (id, state, payload) VALUES {chunk}"
            ))
            .expect("seed insert");
            chunk.clear();
        }
    }
    quick_check(&conn).unwrap_or_else(|e| panic!("corrupt right after seed: {e}"));

    let advance = "UPDATE atc_experiences SET state = CASE state \
         WHEN 'planned' THEN 'dispatched' \
         WHEN 'dispatched' THEN 'executed' \
         WHEN 'executed' THEN 'resolved' \
         WHEN 'resolved' THEN 'planned' END \
         WHERE id % ";
    for round in 0..rounds {
        let sql = format!("{advance}{stripe} = {}", round % stripe);
        conn.execute(&sql)
            .unwrap_or_else(|e| panic!("update round {round} failed: {e}"));
        if round % 8 == 0 || round == rounds - 1 {
            if let Err(msg) = quick_check(&conn) {
                panic!(
                    "INDEX CORRUPTION after round {round} (rows={rows}, stripe={stripe}): {msg}"
                );
            }
        }
    }
}

/// Small/fast: a 2-level index B-tree (a few thousand entries) under churn.
#[test]
fn index_update_churn_small_stays_intact() {
    churn_indexed_state(
        env_i64("Q37EP_ROWS", 2_000),
        env_i64("Q37EP_ROUNDS", 120),
        7,
    );
}

/// Larger: enough rows to push the index B-tree to multiple interior levels,
/// with many rounds of churn — the conditions the prod incident hit.
#[test]
fn index_update_churn_large_stays_intact() {
    churn_indexed_state(
        env_i64("Q37EP_ROWS", 20_000),
        env_i64("Q37EP_ROUNDS", 240),
        11,
    );
}

/// Faithful ATC profile against a real file: a low-cardinality `state` index
/// plus a composite index, overflow-page payloads, and heavy delete+reinsert
/// churn (frees whole B-tree/overflow pages to the freelist, then reallocates
/// them — the regime where an `invalid page number` can enter an interior
/// cell). After the churn the COMMITTED file is cross-checked with canonical
/// SQLite (the prod incident was confirmed by canonical sqlite3 reading the
/// fsqlite-written file).
fn churn_delete_reinsert_with_overflow(rows: i64, cycles: i64) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("q37ep_atc.db");
    let path = db_path.to_string_lossy().into_owned();
    {
        let conn = fsqlite::Connection::open(path.clone()).expect("open frank");
        conn.execute("PRAGMA foreign_keys=off").unwrap();
        conn.execute_batch(
            "CREATE TABLE atc_experiences (
                 id INTEGER PRIMARY KEY,
                 state TEXT,
                 method TEXT,
                 payload TEXT
             );
             CREATE INDEX idx_atc_exp_open ON atc_experiences(state);
             CREATE INDEX idx_atc_exp_sm ON atc_experiences(state, method);",
        )
        .expect("schema");

        let states = ["planned", "dispatched", "executed", "resolved"];
        // A payload large enough to spill to overflow pages (> ~1/4 of a 4 KiB
        // page), so deletes free overflow pages to the freelist.
        let big = "x".repeat(900);

        let mut next_id: i64 = 1;
        let seed = |conn: &fsqlite::Connection, from: i64, to: i64| {
            let mut chunk = String::new();
            for id in from..to {
                if !chunk.is_empty() {
                    chunk.push(',');
                }
                let st = states[(id as usize) % states.len()];
                let m = (id % 19).to_string();
                chunk.push_str(&format!("({id},'{st}','m{m}','{big}-{id}')"));
                if chunk.len() > 60_000 || id == to - 1 {
                    conn.execute(&format!(
                        "INSERT INTO atc_experiences (id,state,method,payload) VALUES {chunk}"
                    ))
                    .unwrap_or_else(|e| panic!("seed insert: {e}"));
                    chunk.clear();
                }
            }
        };
        conn.execute("BEGIN IMMEDIATE").unwrap();
        seed(&conn, next_id, next_id + rows);
        conn.execute("COMMIT").unwrap();
        next_id += rows;

        let mut lo: i64 = 1;
        let batch = (rows * 7) / 10; // ~70% churn per cycle
        for cycle in 0..cycles {
            conn.execute("BEGIN IMMEDIATE").unwrap();
            // Free a contiguous id-range (B-tree + overflow pages -> freelist).
            let hi = lo + batch - 1;
            conn.execute(&format!(
                "DELETE FROM atc_experiences WHERE id BETWEEN {lo} AND {hi}"
            ))
            .unwrap_or_else(|e| panic!("delete cycle {cycle}: {e}"));
            lo = hi + 1;
            // Reinsert fresh rows (reallocates freed pages).
            seed(&conn, next_id, next_id + batch);
            next_id += batch;
            // Advance the indexed column for a stripe (index churn within txn).
            conn.execute(
                "UPDATE atc_experiences SET state = CASE state \
                 WHEN 'planned' THEN 'dispatched' WHEN 'dispatched' THEN 'executed' \
                 WHEN 'executed' THEN 'resolved' WHEN 'resolved' THEN 'planned' END \
                 WHERE id % 5 = 0",
            )
            .unwrap_or_else(|e| panic!("update cycle {cycle}: {e}"));
            // Interleaved in-transaction full btree walk (a GH#113 trigger).
            let _ = conn.query("PRAGMA integrity_check");
            conn.execute("COMMIT").unwrap();

            if let Err(msg) = quick_check(&conn) {
                panic!("INDEX CORRUPTION after cycle {cycle} (committed): {msg}");
            }
        }
    }

    // Cross-check the committed file with canonical SQLite.
    let cconn = rusqlite::Connection::open(&path).expect("open canonical");
    let ic: String = cconn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("canonical integrity_check");
    assert_eq!(ic, "ok", "canonical SQLite on fsqlite-written file: {ic}");

    // Every distinct state's indexed count must equal a forced table scan.
    let states: Vec<String> = {
        let mut stmt = cconn
            .prepare("SELECT DISTINCT state FROM atc_experiences ORDER BY state")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    for st in states {
        let via_index: i64 = cconn
            .query_row(
                "SELECT count(*) FROM atc_experiences WHERE state = ?1",
                [&st],
                |r| r.get(0),
            )
            .unwrap();
        let via_scan: i64 = cconn
            .query_row(
                "SELECT count(*) FROM atc_experiences WHERE +state = ?1",
                [&st],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            via_index, via_scan,
            "state={st}: indexed lookup {via_index} != table scan {via_scan} (missing index entries)"
        );
    }
}

/// The faithful ATC profile: overflow payloads + heavy freelist churn + a
/// low-cardinality index, cross-checked by canonical SQLite.
#[test]
fn index_delete_reinsert_overflow_stays_intact() {
    churn_delete_reinsert_with_overflow(env_i64("Q37EP_ROWS", 8_000), env_i64("Q37EP_ROUNDS", 16));
}

/// GH#132 field reports consistently localize committed damage to a composite
/// UNIQUE autoindex after duplicate errors and caller-side typed recovery. This
/// bounded, deterministic workload does not claim to reproduce either private
/// historical artifact; it freezes the public mutation shape against current
/// main, including reopen/checkpoint boundaries and freelist reuse.
fn composite_unique_duplicate_recovery_churn(journal_mode: &str) {
    const SEED: u64 = 0x132D_57A9;
    const CYCLES: i64 = 8;
    const ROWS_PER_CYCLE: i64 = 80;
    const RECYCLE_ROWS: i64 = 20;

    let dir = TempDir::new().expect("create GH#132 tempdir");
    let db_path = dir.path().join(format!("gh132-{journal_mode}.db"));
    let path = db_path.to_string_lossy().into_owned();

    for cycle in 0..CYCLES {
        {
            let conn = fsqlite::Connection::open(path.clone()).unwrap_or_else(|error| {
                panic!("GH#132 seed={SEED:#x} cycle={cycle}: reopen failed: {error}")
            });
            if cycle == 0 {
                conn.execute("PRAGMA page_size=512")
                    .expect("set tiny page size before schema creation");
            }
            conn.execute(&format!("PRAGMA journal_mode='{journal_mode}'"))
                .unwrap_or_else(|error| {
                    panic!("GH#132 seed={SEED:#x} cycle={cycle}: set journal mode failed: {error}")
                });
            if cycle == 0 {
                conn.execute_batch(
                    "CREATE TABLE conversations (
                         id INTEGER PRIMARY KEY,
                         source_id TEXT NOT NULL,
                         agent_id INTEGER NOT NULL,
                         external_id TEXT NOT NULL,
                         title TEXT NOT NULL,
                         UNIQUE(source_id, agent_id, external_id)
                     );",
                )
                .expect("create GH#132 composite UNIQUE schema");
            }

            for offset in 0..ROWS_PER_CYCLE {
                let operation = cycle * ROWS_PER_CYCLE + offset;
                let id = operation + 1;
                let source = (operation * 37 + i64::try_from(SEED & 0xff).unwrap()) % 29;
                let agent = (operation * 17 + 3) % 11;
                let external = format!("external-{operation:06}");
                conn.execute(&format!(
                    "INSERT INTO conversations(id,source_id,agent_id,external_id,title) \
                     VALUES({id},'source-{source:02}',{agent},'{external}','title-{operation:06}')"
                ))
                .unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: insert failed: {error}"
                    )
                });

                if offset % 4 == 0 {
                    let duplicate_id = 900_000 + operation;
                    let duplicate_error = conn
                        .execute(&format!(
                            "INSERT INTO conversations(id,source_id,agent_id,external_id,title) \
                             VALUES({duplicate_id},'source-{source:02}',{agent},'{external}','duplicate')"
                        ))
                        .expect_err("composite UNIQUE duplicate must be rejected");
                    assert!(
                        matches!(
                            duplicate_error,
                            fsqlite::FrankenError::UniqueViolation { .. }
                        ),
                        "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                         duplicate must return typed UniqueViolation, got {duplicate_error:?}"
                    );

                    let recovered = conn
                        .query(&format!(
                            "SELECT id FROM conversations \
                             WHERE source_id='source-{source:02}' \
                               AND agent_id={agent} AND external_id='{external}'"
                        ))
                        .unwrap_or_else(|error| {
                            panic!(
                                "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                                 typed recovery lookup failed: {error}"
                            )
                        });
                    assert_eq!(
                        recovered
                            .first()
                            .and_then(|row| row.values().first())
                            .cloned(),
                        Some(SqliteValue::Integer(id)),
                        "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                         typed recovery must find the pre-existing row"
                    );
                    conn.execute(&format!(
                        "UPDATE conversations SET title='recovered-{operation:06}' WHERE id={id}"
                    ))
                    .unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                             recovery merge failed: {error}"
                        )
                    });
                }
            }

            if cycle >= 2 {
                let retired_cycle = cycle - 2;
                let retired_start = retired_cycle * ROWS_PER_CYCLE + 1;
                let retired_end = retired_start + RECYCLE_ROWS - 1;
                conn.execute(&format!(
                    "DELETE FROM conversations WHERE id BETWEEN {retired_start} AND {retired_end}"
                ))
                .unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle}: freelist-producing delete failed: {error}"
                    )
                });

                for replacement in 0..RECYCLE_ROWS {
                    let replacement_id = 1_000_000 + cycle * RECYCLE_ROWS + replacement;
                    conn.execute(&format!(
                        "INSERT INTO conversations(id,source_id,agent_id,external_id,title) \
                         VALUES({replacement_id},'recycled-{cycle:02}',{},
                                'replacement-{cycle:02}-{replacement:03}','replacement')",
                        replacement % 11
                    ))
                    .unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle} replacement={replacement}: \
                             freelist reuse insert failed: {error}"
                        )
                    });
                }
            }

            if journal_mode == "WAL" && cycle % 2 == 1 {
                conn.query("PRAGMA wal_checkpoint(PASSIVE)")
                    .unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle}: WAL checkpoint failed: {error}"
                        )
                    });
            }
            quick_check(&conn).unwrap_or_else(|error| {
                panic!("GH#132 seed={SEED:#x} cycle={cycle}: fsqlite quick_check failed: {error}")
            });
        }

        let stock = rusqlite::Connection::open(&path).unwrap_or_else(|error| {
            panic!("GH#132 seed={SEED:#x} cycle={cycle}: stock reopen failed: {error}")
        });
        let quick: String = stock
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap_or_else(|error| {
                panic!("GH#132 seed={SEED:#x} cycle={cycle}: stock quick_check failed: {error}")
            });
        assert_eq!(
            quick, "ok",
            "GH#132 seed={SEED:#x} cycle={cycle}: stock quick_check detected committed damage"
        );
    }

    let stock = rusqlite::Connection::open(&path).expect("final stock reopen");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("final stock integrity_check");
    assert_eq!(
        integrity, "ok",
        "GH#132 seed={SEED:#x}: stock integrity_check detected committed damage"
    );

    fn collect_rows(
        conn: &rusqlite::Connection,
        from_clause: &str,
    ) -> Vec<(String, i64, String, i64)> {
        let sql = format!(
            "SELECT source_id,agent_id,external_id,id FROM {from_clause} \
             ORDER BY source_id,agent_id,external_id,id"
        );
        let mut statement = conn.prepare(&sql).expect("prepare stock comparison");
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .expect("query stock comparison")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect stock comparison")
    }

    let via_table = collect_rows(&stock, "conversations NOT INDEXED");
    let via_index = collect_rows(
        &stock,
        "conversations INDEXED BY sqlite_autoindex_conversations_1",
    );
    assert_eq!(
        via_index, via_table,
        "GH#132 seed={SEED:#x}: composite UNIQUE index traversal diverged from table traversal"
    );
}

#[test]
fn composite_unique_duplicate_recovery_wal_stays_stock_canonical() {
    composite_unique_duplicate_recovery_churn("WAL");
}

#[test]
fn composite_unique_duplicate_recovery_rollback_stays_stock_canonical() {
    composite_unique_duplicate_recovery_churn("DELETE");
}

/// Deep-tree variant: a tiny `page_size` (512) under WAL forces the index
/// B-tree to many interior levels with only a few thousand rows, so every
/// churn round drives interior-page splits/merges at multiple levels — the
/// most direct stress on the interior-cell child-pointer handling the prod
/// `invalid page number` signature implicates. Cross-checked by canonical
/// SQLite, which natively parses the 512-byte-page file.
#[test]
fn index_churn_tiny_pages_deep_tree_stays_intact() {
    let rows = env_i64("Q37EP_ROWS", 6_000);
    let rounds = env_i64("Q37EP_ROUNDS", 80);
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("q37ep_deep.db");
    let path = db_path.to_string_lossy().into_owned();
    {
        let conn = fsqlite::Connection::open(path.clone()).expect("open frank");
        // Tiny pages + WAL BEFORE any table is created.
        conn.execute("PRAGMA page_size=512").unwrap();
        conn.execute("PRAGMA journal_mode=WAL").unwrap();
        conn.execute("PRAGMA foreign_keys=off").unwrap();
        conn.execute_batch(
            "CREATE TABLE atc_experiences (id INTEGER PRIMARY KEY, state TEXT, method TEXT);
             CREATE INDEX idx_atc_exp_open ON atc_experiences(state);
             CREATE INDEX idx_atc_exp_sm ON atc_experiences(state, method);",
        )
        .expect("schema");

        let states = ["planned", "dispatched", "executed", "resolved"];
        let mut chunk = String::new();
        for id in 1..=rows {
            if !chunk.is_empty() {
                chunk.push(',');
            }
            let st = states[(id as usize) % states.len()];
            chunk.push_str(&format!("({id},'{st}','m{}')", id % 19));
            if chunk.len() > 20_000 || id == rows {
                conn.execute(&format!(
                    "INSERT INTO atc_experiences (id,state,method) VALUES {chunk}"
                ))
                .expect("seed");
                chunk.clear();
            }
        }
        quick_check(&conn).unwrap_or_else(|e| panic!("corrupt after seed: {e}"));

        for round in 0..rounds {
            conn.execute(&format!(
                "UPDATE atc_experiences SET state = CASE state \
                 WHEN 'planned' THEN 'dispatched' WHEN 'dispatched' THEN 'executed' \
                 WHEN 'executed' THEN 'resolved' WHEN 'resolved' THEN 'planned' END \
                 WHERE id % 7 = {}",
                round % 7
            ))
            .unwrap_or_else(|e| panic!("update round {round}: {e}"));
            if round % 8 == 0 || round == rounds - 1 {
                if let Err(msg) = quick_check(&conn) {
                    panic!("INDEX CORRUPTION (tiny pages) after round {round}: {msg}");
                }
            }
        }
    }

    let cconn = rusqlite::Connection::open(&path).expect("open canonical");
    let ic: String = cconn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("canonical integrity_check");
    assert_eq!(ic, "ok", "canonical SQLite on tiny-page fsqlite file: {ic}");
}

// ============================================================================
// Concurrent shared-index churn (bd-9inpb validation + bd-l9yii concurrent angle).
//
// The single-connection guards above and the separate-table am#152 repros never
// combined this dimension: MANY writers, each on its OWN Connection to the same
// FILE-BACKED db, all churning the SAME low-cardinality `state` index (shared
// index B-tree) with delete+reinsert freelist churn. Each writer owns a disjoint
// id-stripe (no pure rowid write-write conflict on the table) but their `state`
// index entries interleave across the same index leaves/interiors, so the shared
// index B-tree sees concurrent splits/merges and freelist page REUSE.
//
// rollback-journal (DELETE) mode is the regime where bd-9inpb / am#152 reproduced
// (the cross-connection page-alias that produced "invalid page number" in index
// interior cells). With the bd-9inpb fix (commit_journal re-reads the on-disk
// page-1 db size under the EXCLUSIVE lock and aborts BusySnapshot on a
// peer-claimed page) these stay consistent. Heavy/probabilistic stress: #[ignore]d,
// run with --ignored. Env-tunable via Q37EP_WRITERS / Q37EP_ROWS / Q37EP_ROUNDS.
// ============================================================================

fn cc_is_transient(err: &fsqlite::FrankenError) -> bool {
    matches!(
        err,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::DatabaseLocked { .. }
    )
}

fn cc_with_retry<T>(
    label: &str,
    mut f: impl FnMut() -> Result<T, fsqlite::FrankenError>,
) -> Result<T, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut attempt = 0u32;
    loop {
        match f() {
            Ok(v) => return Ok(v),
            Err(err) if cc_is_transient(&err) && std::time::Instant::now() < deadline => {
                attempt = attempt.saturating_add(1);
                let shift = attempt.min(6);
                std::thread::sleep(std::time::Duration::from_millis((1u64 << shift).min(40)));
            }
            Err(err) => return Err(format!("{label}: {err}")),
        }
    }
}

fn cc_writer(
    path: &str,
    writer_id: usize,
    writers: usize,
    rows: i64,
    rounds: i64,
    wal: bool,
    barrier: &std::sync::Barrier,
) -> Result<(), String> {
    let conn = fsqlite::Connection::open(path.to_string()).map_err(|e| format!("open: {e}"))?;
    let _ = conn.execute("PRAGMA busy_timeout=10000;");
    if wal {
        cc_with_retry("journal_mode=WAL", || {
            conn.execute("PRAGMA journal_mode=WAL;").map(|_| ())
        })?;
    }
    assert!(
        conn.is_concurrent_mode_default(),
        "concurrent_mode default must be ON (the whole point of the repro)"
    );
    let states = ["planned", "dispatched", "executed", "resolved"];
    let w = writer_id as i64;
    let n = writers as i64;
    barrier.wait();

    for round in 0..rounds {
        // Advance the SHARED indexed column for this writer's id-stripe
        // (autocommit: each UPDATE is its own MVCC commit).
        let advance = format!(
            "UPDATE atc_experiences SET state = CASE state \
             WHEN 'planned' THEN 'dispatched' WHEN 'dispatched' THEN 'executed' \
             WHEN 'executed' THEN 'resolved' WHEN 'resolved' THEN 'planned' END \
             WHERE id % {n} = {w}"
        );
        cc_with_retry(&format!("advance w{writer_id} r{round}"), || {
            conn.execute(&advance).map(|_| ())
        })?;

        // Every 4th round: free this writer's stripe then reinsert it (freelist
        // page reuse in the shared index B-tree).
        if round % 4 == 3 {
            cc_with_retry(&format!("delete w{writer_id} r{round}"), || {
                conn.execute(&format!("DELETE FROM atc_experiences WHERE id % {n} = {w}"))
                    .map(|_| ())
            })?;
            let mut chunk = String::new();
            for id in 1..=rows {
                if id % n != w {
                    continue;
                }
                if !chunk.is_empty() {
                    chunk.push(',');
                }
                let st = states[(id as usize) % states.len()];
                chunk.push_str(&format!("({id},'{st}','payload-{id:08}-filler')"));
                if chunk.len() > 40_000 {
                    let stmt =
                        format!("INSERT INTO atc_experiences(id,state,payload) VALUES {chunk}");
                    cc_with_retry(&format!("reinsert w{writer_id} r{round}"), || {
                        conn.execute(&stmt).map(|_| ())
                    })?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                let stmt = format!("INSERT INTO atc_experiences(id,state,payload) VALUES {chunk}");
                cc_with_retry(&format!("reinsert tail w{writer_id} r{round}"), || {
                    conn.execute(&stmt).map(|_| ())
                })?;
            }
        }
    }
    Ok(())
}

fn concurrent_shared_index_churn(writers: usize, rows: i64, rounds: i64, wal: bool) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("q37ep_concurrent.db");
    let path = db_path.to_string_lossy().into_owned();

    // Seed rows 1..=rows partitioned by id % writers across the writers.
    {
        let conn = fsqlite::Connection::open(path.clone()).expect("open seed");
        if wal {
            conn.execute("PRAGMA journal_mode=WAL").unwrap();
        }
        conn.execute("PRAGMA foreign_keys=off").unwrap();
        conn.execute(
            "CREATE TABLE atc_experiences (id INTEGER PRIMARY KEY, state TEXT, payload TEXT)",
        )
        .expect("create table");
        conn.execute("CREATE INDEX idx_atc_exp_open ON atc_experiences(state)")
            .expect("create index");
        let states = ["planned", "dispatched", "executed", "resolved"];
        let mut chunk = String::new();
        for id in 1..=rows {
            if !chunk.is_empty() {
                chunk.push(',');
            }
            let st = states[(id as usize) % states.len()];
            chunk.push_str(&format!("({id},'{st}','payload-{id:08}-filler')"));
            if id % 500 == 0 || id == rows {
                conn.execute(&format!(
                    "INSERT INTO atc_experiences(id,state,payload) VALUES {chunk}"
                ))
                .expect("seed");
                chunk.clear();
            }
        }
        quick_check(&conn).unwrap_or_else(|e| panic!("corrupt after seed: {e}"));
    }

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(writers));
    let errors = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let path_arc = std::sync::Arc::new(path.clone());
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let path = std::sync::Arc::clone(&path_arc);
            let barrier = std::sync::Arc::clone(&barrier);
            let errors = std::sync::Arc::clone(&errors);
            std::thread::spawn(move || {
                if let Err(e) = cc_writer(&path, w, writers, rows, rounds, wal, &barrier) {
                    eprintln!("[cc writer {w}] FAILED: {e}");
                    errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }

    // frank quick_check on the committed state.
    let verify = fsqlite::Connection::open(path.clone()).expect("verify open");
    if wal {
        let _ = verify.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    }
    if let Err(msg) = quick_check(&verify) {
        panic!("INDEX CORRUPTION (concurrent, frank quick_check, wal={wal}): {msg}");
    }
    drop(verify);

    // Canonical SQLite cross-check of the committed file: integrity + every
    // state's indexed count must equal a forced table scan (missing index
    // entries == silent data loss on indexed reads, the bd-l9yii / GH#113 claim).
    let cconn = rusqlite::Connection::open(&path).expect("open canonical");
    let _ = cconn.execute_batch("PRAGMA busy_timeout=10000; PRAGMA wal_checkpoint(TRUNCATE);");
    let ic: String = cconn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("canonical integrity_check");
    assert_eq!(
        ic, "ok",
        "canonical SQLite on concurrently-churned file: {ic}"
    );

    let states: Vec<String> = {
        let mut stmt = cconn
            .prepare("SELECT DISTINCT state FROM atc_experiences ORDER BY state")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    for st in states {
        let via_index: i64 = cconn
            .query_row(
                "SELECT count(*) FROM atc_experiences WHERE state = ?1",
                [&st],
                |r| r.get(0),
            )
            .unwrap();
        let via_scan: i64 = cconn
            .query_row(
                "SELECT count(*) FROM atc_experiences WHERE +state = ?1",
                [&st],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            via_index, via_scan,
            "state={st}: indexed lookup {via_index} != table scan {via_scan} (missing index entries)"
        );
    }
}

/// Concurrent shared-index churn in rollback-journal (DELETE) mode — the mode
/// whose commit-time cross-process page-conflict check was empty before bd-9inpb.
#[test]
#[ignore = "bd-9inpb/bd-l9yii concurrent shared-index churn stress harness; run with --ignored"]
fn concurrent_shared_index_churn_rollback_journal_stays_intact() {
    concurrent_shared_index_churn(
        env_i64("Q37EP_WRITERS", 8) as usize,
        env_i64("Q37EP_ROWS", 6_000),
        env_i64("Q37EP_ROUNDS", 160),
        false,
    );
}

/// Concurrent shared-index churn in WAL mode (first-committer-wins page conflict
/// detection) — the harness's normal mode and the bd-9inpb control.
#[test]
#[ignore = "bd-9inpb/bd-l9yii concurrent shared-index churn stress harness; run with --ignored"]
fn concurrent_shared_index_churn_wal_stays_intact() {
    concurrent_shared_index_churn(
        env_i64("Q37EP_WRITERS", 8) as usize,
        env_i64("Q37EP_ROWS", 6_000),
        env_i64("Q37EP_ROUNDS", 160),
        true,
    );
}
