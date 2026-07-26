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
#![recursion_limit = "512"]

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
async fn quick_check(conn: &fsqlite::Connection) -> Result<(), String> {
    let rows = conn
        .query("PRAGMA quick_check")
        .await
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
async fn churn_indexed_state(rows: i64, rounds: i64, stripe: i64) {
    let conn = fsqlite::Connection::open(":memory:")
        .await
        .expect("open frank");
    conn.execute("CREATE TABLE atc_experiences (id INTEGER PRIMARY KEY, state TEXT, payload TEXT)")
        .await
        .expect("create table");
    conn.execute("CREATE INDEX idx_atc_exp_open ON atc_experiences(state)")
        .await
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
            .await
            .expect("seed insert");
            chunk.clear();
        }
    }
    quick_check(&conn)
        .await
        .unwrap_or_else(|e| panic!("corrupt right after seed: {e}"));

    let advance = "UPDATE atc_experiences SET state = CASE state \
         WHEN 'planned' THEN 'dispatched' \
         WHEN 'dispatched' THEN 'executed' \
         WHEN 'executed' THEN 'resolved' \
         WHEN 'resolved' THEN 'planned' END \
         WHERE id % ";
    for round in 0..rounds {
        let sql = format!("{advance}{stripe} = {}", round % stripe);
        conn.execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("update round {round} failed: {e}"));
        if round % 8 == 0 || round == rounds - 1 {
            if let Err(msg) = quick_check(&conn).await {
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
    asupersync::test_utils::run_test(|| async {
        churn_indexed_state(
            env_i64("Q37EP_ROWS", 2_000),
            env_i64("Q37EP_ROUNDS", 120),
            7,
        )
        .await;
    });
}

/// Larger: enough rows to push the index B-tree to multiple interior levels,
/// with many rounds of churn — the conditions the prod incident hit.
#[test]
fn index_update_churn_large_stays_intact() {
    asupersync::test_utils::run_test(|| async {
        churn_indexed_state(
            env_i64("Q37EP_ROWS", 20_000),
            env_i64("Q37EP_ROUNDS", 240),
            11,
        )
        .await;
    });
}

/// Faithful ATC profile against a real file: a low-cardinality `state` index
/// plus a composite index, overflow-page payloads, and heavy delete+reinsert
/// churn (frees whole B-tree/overflow pages to the freelist, then reallocates
/// them — the regime where an `invalid page number` can enter an interior
/// cell). After the churn the COMMITTED file is cross-checked with canonical
/// SQLite (the prod incident was confirmed by canonical sqlite3 reading the
/// fsqlite-written file).
async fn churn_delete_reinsert_with_overflow(rows: i64, cycles: i64) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("q37ep_atc.db");
    let path = db_path.to_string_lossy().into_owned();
    {
        let conn = fsqlite::Connection::open(path.clone())
            .await
            .expect("open frank");
        conn.execute("PRAGMA foreign_keys=off").await.unwrap();
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
        .await
        .expect("schema");

        let states = ["planned", "dispatched", "executed", "resolved"];
        // A payload large enough to spill to overflow pages (> ~1/4 of a 4 KiB
        // page), so deletes free overflow pages to the freelist.
        let big = "x".repeat(900);

        let mut next_id: i64 = 1;
        async fn seed(
            conn: &fsqlite::Connection,
            states: &[&str],
            big: &str,
            from: i64,
            to: i64,
        ) {
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
                    .await
                    .unwrap_or_else(|e| panic!("seed insert: {e}"));
                    chunk.clear();
                }
            }
        }
        conn.execute("BEGIN IMMEDIATE").await.unwrap();
        seed(&conn, &states, &big, next_id, next_id + rows).await;
        conn.execute("COMMIT").await.unwrap();
        next_id += rows;

        let mut lo: i64 = 1;
        let batch = (rows * 7) / 10; // ~70% churn per cycle
        for cycle in 0..cycles {
            conn.execute("BEGIN IMMEDIATE").await.unwrap();
            // Free a contiguous id-range (B-tree + overflow pages -> freelist).
            let hi = lo + batch - 1;
            conn.execute(&format!(
                "DELETE FROM atc_experiences WHERE id BETWEEN {lo} AND {hi}"
            ))
            .await
            .unwrap_or_else(|e| panic!("delete cycle {cycle}: {e}"));
            lo = hi + 1;
            // Reinsert fresh rows (reallocates freed pages).
            seed(&conn, &states, &big, next_id, next_id + batch).await;
            next_id += batch;
            // Advance the indexed column for a stripe (index churn within txn).
            conn.execute(
                "UPDATE atc_experiences SET state = CASE state \
                 WHEN 'planned' THEN 'dispatched' WHEN 'dispatched' THEN 'executed' \
                 WHEN 'executed' THEN 'resolved' WHEN 'resolved' THEN 'planned' END \
                 WHERE id % 5 = 0",
            )
            .await
            .unwrap_or_else(|e| panic!("update cycle {cycle}: {e}"));
            // Interleaved in-transaction full btree walk (a GH#113 trigger).
            drop(conn.query("PRAGMA integrity_check").await);
            conn.execute("COMMIT").await.unwrap();

            if let Err(msg) = quick_check(&conn).await {
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
    asupersync::test_utils::run_test(|| async {
        churn_delete_reinsert_with_overflow(
            env_i64("Q37EP_ROWS", 8_000),
            env_i64("Q37EP_ROUNDS", 16),
        )
        .await;
    });
}

/// GH#132 field reports consistently localize committed damage to a composite
/// UNIQUE autoindex after duplicate errors and caller-side typed recovery. This
/// bounded, deterministic workload does not claim to reproduce either private
/// historical artifact; it freezes the public mutation shape against current
/// main, including reopen/checkpoint boundaries and freelist reuse.
fn collect_gh132_rows(
    connection: &rusqlite::Connection,
    from_clause: &str,
) -> Vec<(String, i64, String, i64, String)> {
    let sql = format!(
        "SELECT source_id,agent_id,external_id,id,title FROM {from_clause} \
         ORDER BY source_id,agent_id,external_id,id,title"
    );
    let mut statement = connection.prepare(&sql).expect("prepare GH#132 comparison");
    statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .expect("query GH#132 comparison")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect GH#132 comparison")
}

async fn composite_unique_duplicate_recovery_churn(journal_mode: &str) {
    const SEED: u64 = 0x132D_57A9;
    const CYCLES: i64 = 8;
    const ROWS_PER_CYCLE: i64 = 80;
    const RECYCLE_ROWS: i64 = 20;

    let dir = TempDir::new().expect("create GH#132 tempdir");
    let db_path = dir.path().join(format!("gh132-{journal_mode}.db"));
    let path = db_path.to_string_lossy().into_owned();
    let oracle_path = dir.path().join(format!("gh132-{journal_mode}-oracle.db"));

    for cycle in 0..CYCLES {
        {
            let conn = fsqlite::Connection::open(path.clone())
                .await
                .unwrap_or_else(|error| {
                    panic!("GH#132 seed={SEED:#x} cycle={cycle}: reopen failed: {error}")
                });
            let oracle = rusqlite::Connection::open(&oracle_path).unwrap_or_else(|error| {
                panic!("GH#132 seed={SEED:#x} cycle={cycle}: oracle reopen failed: {error}")
            });
            if cycle == 0 {
                conn.execute("PRAGMA page_size=512")
                    .await
                    .expect("set tiny page size before schema creation");
                oracle
                    .execute_batch("PRAGMA page_size=512;")
                    .expect("set oracle tiny page size before schema creation");
            }
            conn.execute(&format!("PRAGMA journal_mode='{journal_mode}'"))
                .await
                .unwrap_or_else(|error| {
                    panic!("GH#132 seed={SEED:#x} cycle={cycle}: set journal mode failed: {error}")
                });
            oracle
                .execute_batch(&format!("PRAGMA journal_mode='{journal_mode}';"))
                .unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle}: \
                         set oracle journal mode failed: {error}"
                    )
                });
            if cycle == 0 {
                let schema = "CREATE TABLE conversations (
                         id INTEGER PRIMARY KEY,
                         source_id TEXT NOT NULL,
                         agent_id INTEGER NOT NULL,
                         external_id TEXT NOT NULL,
                         title TEXT NOT NULL,
                         UNIQUE(source_id, agent_id, external_id)
                     );";
                conn.execute_batch(schema)
                    .await
                    .expect("create GH#132 composite UNIQUE schema");
                oracle
                    .execute_batch(schema)
                    .expect("create oracle GH#132 composite UNIQUE schema");
            }

            for offset in 0..ROWS_PER_CYCLE {
                let operation = cycle * ROWS_PER_CYCLE + offset;
                let id = operation + 1;
                let source = (operation * 37 + i64::try_from(SEED & 0xff).unwrap()) % 29;
                let agent = (operation * 17 + 3) % 11;
                let external = format!("external-{operation:06}");
                let insert = format!(
                    "INSERT INTO conversations(id,source_id,agent_id,external_id,title) \
                     VALUES({id},'source-{source:02}',{agent},'{external}','title-{operation:06}')"
                );
                conn.execute(&insert)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: insert failed: {error}"
                    )
                });
                oracle.execute_batch(&insert).unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                         oracle insert failed: {error}"
                    )
                });

                if offset % 4 == 0 {
                    let duplicate_id = 900_000 + operation;
                    let duplicate = format!(
                        "INSERT INTO conversations(id,source_id,agent_id,external_id,title) \
                         VALUES({duplicate_id},'source-{source:02}',{agent},'{external}','duplicate')"
                    );
                    let duplicate_error = conn
                        .execute(&duplicate)
                        .await
                        .expect_err("composite UNIQUE duplicate must be rejected");
                    assert!(
                        matches!(
                            duplicate_error,
                            fsqlite::FrankenError::UniqueViolation { .. }
                        ),
                        "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                         duplicate must return typed UniqueViolation, got {duplicate_error:?}"
                    );
                    oracle
                        .execute_batch(&duplicate)
                        .expect_err("oracle composite UNIQUE duplicate must be rejected");

                    let recovered = conn
                        .query(&format!(
                            "SELECT id FROM conversations \
                             WHERE source_id='source-{source:02}' \
                               AND agent_id={agent} AND external_id='{external}'"
                        ))
                        .await
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
                    let oracle_recovered: i64 = oracle
                        .query_row(
                            "SELECT id FROM conversations \
                             WHERE source_id=?1 AND agent_id=?2 AND external_id=?3",
                            (format!("source-{source:02}"), agent, &external),
                            |row| row.get(0),
                        )
                        .unwrap_or_else(|error| {
                            panic!(
                                "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                                 oracle typed recovery lookup failed: {error}"
                            )
                        });
                    assert_eq!(oracle_recovered, id);
                    let update = format!(
                        "UPDATE conversations SET title='recovered-{operation:06}' WHERE id={id}"
                    );
                    conn.execute(&update).await.unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                             recovery merge failed: {error}"
                        )
                    });
                    oracle.execute_batch(&update).unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle} operation={operation}: \
                             oracle recovery merge failed: {error}"
                        )
                    });
                }
            }

            if cycle >= 2 {
                let retired_cycle = cycle - 2;
                let retired_start = retired_cycle * ROWS_PER_CYCLE + 1;
                let retired_end = retired_start + RECYCLE_ROWS - 1;
                let delete = format!(
                    "DELETE FROM conversations WHERE id BETWEEN {retired_start} AND {retired_end}"
                );
                conn.execute(&delete)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle}: freelist-producing delete failed: {error}"
                    )
                });
                oracle.execute_batch(&delete).unwrap_or_else(|error| {
                    panic!(
                        "GH#132 seed={SEED:#x} cycle={cycle}: \
                         oracle freelist-producing delete failed: {error}"
                    )
                });

                for replacement in 0..RECYCLE_ROWS {
                    let replacement_id = 1_000_000 + cycle * RECYCLE_ROWS + replacement;
                    let insert = format!(
                        "INSERT INTO conversations(id,source_id,agent_id,external_id,title) \
                         VALUES({replacement_id},'recycled-{cycle:02}',{},
                                'replacement-{cycle:02}-{replacement:03}','replacement')",
                        replacement % 11
                    );
                    conn.execute(&insert).await.unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle} replacement={replacement}: \
                             freelist reuse insert failed: {error}"
                        )
                    });
                    oracle.execute_batch(&insert).unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle} replacement={replacement}: \
                             oracle freelist reuse insert failed: {error}"
                        )
                    });
                }
            }

            if journal_mode == "WAL" && cycle % 2 == 1 {
                conn.query("PRAGMA wal_checkpoint(PASSIVE)")
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle}: WAL checkpoint failed: {error}"
                        )
                    });
                oracle
                    .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
                    .unwrap_or_else(|error| {
                        panic!(
                            "GH#132 seed={SEED:#x} cycle={cycle}: \
                             oracle WAL checkpoint failed: {error}"
                        )
                    });
            }
            quick_check(&conn).await.unwrap_or_else(|error| {
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
        let oracle = rusqlite::Connection::open(&oracle_path).unwrap_or_else(|error| {
            panic!("GH#132 seed={SEED:#x} cycle={cycle}: oracle verify open failed: {error}")
        });
        let oracle_quick: String = oracle
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap_or_else(|error| {
                panic!("GH#132 seed={SEED:#x} cycle={cycle}: oracle quick_check failed: {error}")
            });
        assert_eq!(oracle_quick, "ok");
        let produced_table = collect_gh132_rows(&stock, "conversations NOT INDEXED");
        let produced_index = collect_gh132_rows(
            &stock,
            "conversations INDEXED BY sqlite_autoindex_conversations_1",
        );
        let oracle_table = collect_gh132_rows(&oracle, "conversations NOT INDEXED");
        assert_eq!(
            produced_index, produced_table,
            "GH#132 seed={SEED:#x} cycle={cycle}: committed index/table divergence"
        );
        assert_eq!(
            produced_table, oracle_table,
            "GH#132 seed={SEED:#x} cycle={cycle}: FrankenSQLite diverged from stock SQLite"
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

    let via_table = collect_gh132_rows(&stock, "conversations NOT INDEXED");
    let via_index = collect_gh132_rows(
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
    asupersync::test_utils::run_test(|| async {
        composite_unique_duplicate_recovery_churn("WAL").await;
    });
}

#[test]
fn composite_unique_duplicate_recovery_rollback_stays_stock_canonical() {
    asupersync::test_utils::run_test(|| async {
        composite_unique_duplicate_recovery_churn("DELETE").await;
    });
}

// ============================================================================
// GH #289: post-stock-VACUUM packed UNIQUE autoindex corruption fingerprint.
// ============================================================================

fn gh289_mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn gh289_key(ordinal: i64) -> String {
    let first = gh289_mix(ordinal as u64 ^ 0x0289_5eed_cafe_f00d);
    let second = gh289_mix(first);
    let third = gh289_mix(second);
    let tail = gh289_mix(third) & 0x000f_ffff;
    let key = format!("{first:016x}{second:016x}{third:016x}{tail:05x}");
    assert_eq!(key.len(), 53);
    key
}

async fn gh289_insert_target_row(conn: &fsqlite::Connection, ordinal: i64, payload: &str) {
    let key = gh289_key(ordinal);
    conn.execute(&format!(
        "INSERT INTO gh289_objects(object_key,bucket,payload) \
         VALUES('{key}',{},'{payload}-{ordinal:08}')",
        ordinal.rem_euclid(97)
    ))
    .await
    .unwrap_or_else(|error| panic!("GH#289 insert ordinal={ordinal} failed: {error}"));
}

fn gh289_stock_repack(path: &str) {
    let stock = rusqlite::Connection::open(path).expect("GH#289 stock maintenance open");
    stock
        .busy_timeout(std::time::Duration::from_secs(30))
        .expect("GH#289 stock busy timeout");
    stock
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); REINDEX; VACUUM;")
        .expect("GH#289 stock REINDEX + VACUUM");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("GH#289 post-maintenance stock integrity_check");
    assert_eq!(integrity, "ok", "GH#289 stock maintenance must start clean");
    let page_size: i64 = stock
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("GH#289 stock page_size");
    let freelist: i64 = stock
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .expect("GH#289 stock freelist_count");
    assert_eq!(page_size, 512, "GH#289 must exercise tiny packed pages");
    assert_eq!(freelist, 0, "stock VACUUM must leave a fully packed file");
}

fn gh289_assert_stock_canonical(path: &str, expected_rows: i64) {
    let stock = rusqlite::Connection::open(path).expect("GH#289 final stock open");
    stock
        .busy_timeout(std::time::Duration::from_secs(30))
        .expect("GH#289 final stock busy timeout");
    stock
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("GH#289 final stock checkpoint");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("GH#289 final stock integrity_check");
    assert_eq!(integrity, "ok", "GH#289 committed image is corrupt");

    let table_rows: i64 = stock
        .query_row(
            "SELECT COUNT(*) FROM gh289_objects NOT INDEXED",
            [],
            |row| row.get(0),
        )
        .expect("GH#289 table row count");
    let index_rows: i64 = stock
        .query_row(
            "SELECT COUNT(*) FROM gh289_objects \
             INDEXED BY sqlite_autoindex_gh289_objects_1",
            [],
            |row| row.get(0),
        )
        .expect("GH#289 autoindex row count");
    assert_eq!(table_rows, expected_rows, "GH#289 table row count drift");
    assert_eq!(
        index_rows, table_rows,
        "GH#289 wrong number of entries in the UNIQUE autoindex"
    );

    let collect = |from_clause: &str| -> Vec<(String, i64)> {
        let sql = format!("SELECT object_key,rowid FROM {from_clause} ORDER BY object_key,rowid");
        let mut statement = stock.prepare(&sql).expect("GH#289 comparison prepare");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("GH#289 comparison query")
            .collect::<Result<Vec<_>, _>>()
            .expect("GH#289 comparison collect")
    };
    let table_entries = collect("gh289_objects NOT INDEXED");
    let index_entries = collect("gh289_objects INDEXED BY sqlite_autoindex_gh289_objects_1");
    assert_eq!(
        index_entries, table_entries,
        "GH#289 autoindex has duplicated, missing, misplaced, or wrong-rowid cells"
    );

    let duplicate_keys: i64 = stock
        .query_row(
            "SELECT COUNT(*) FROM (
                 SELECT object_key
                 FROM gh289_objects INDEXED BY sqlite_autoindex_gh289_objects_1
                 GROUP BY object_key HAVING COUNT(*) != 1
             )",
            [],
            |row| row.get(0),
        )
        .expect("GH#289 duplicate-key fingerprint");
    assert_eq!(
        duplicate_keys, 0,
        "GH#289 UNIQUE autoindex contains a duplicated key run"
    );
}

fn gh289_post_stock_vacuum_stress(seed_rows: i64, transactions: i64, readers: usize) {
    const ROWS_PER_TRANSACTION: i64 = 15;
    const AUX_TABLES: i64 = 8;

    let dir = TempDir::new().expect("GH#289 tempdir");
    let db_path = dir.path().join("gh289-packed-unique.db");
    let path = db_path.to_string_lossy().into_owned();
    let payload = "p".repeat(1_450);

    {
        let stock = rusqlite::Connection::open(&path).expect("GH#289 stock seed open");
        stock
            .execute_batch(
                "PRAGMA page_size=512;
                 CREATE TABLE gh289_stock_anchor(id INTEGER PRIMARY KEY);
                 VACUUM;",
            )
            .expect("GH#289 create a canonical 512-byte-page database");
        let page_size: i64 = stock
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("GH#289 seed page_size");
        assert_eq!(page_size, 512, "GH#289 stock seed must use tiny pages");
    }

    asupersync::test_utils::run_test(|| async {
        let conn = fsqlite::Connection::open(path.clone())
            .await
            .expect("GH#289 seed open");
        conn.execute("PRAGMA journal_mode=WAL")
            .await
            .expect("GH#289 WAL mode");
        conn.execute_batch(
            "CREATE TABLE gh289_objects (
                 object_key TEXT PRIMARY KEY NOT NULL,
                 bucket INTEGER NOT NULL,
                 payload TEXT NOT NULL
             );
             CREATE INDEX gh289_objects_secondary
                 ON gh289_objects(bucket, object_key);
             CREATE TABLE gh289_blobs (
                 txn_id INTEGER PRIMARY KEY,
                 payload BLOB NOT NULL
             );",
        )
        .await
        .expect("GH#289 core schema");
        for table in 0..AUX_TABLES {
            conn.execute_batch(&format!(
                "CREATE TABLE gh289_aux_{table} (
                     id INTEGER PRIMARY KEY,
                     value TEXT NOT NULL
                 );
                 CREATE INDEX gh289_aux_{table}_value ON gh289_aux_{table}(value);"
            ))
            .await
            .unwrap_or_else(|error| panic!("GH#289 auxiliary schema {table}: {error}"));
        }

        conn.execute("BEGIN IMMEDIATE")
            .await
            .expect("GH#289 seed transaction begin");
        for ordinal in 0..seed_rows {
            gh289_insert_target_row(&conn, ordinal, &payload).await;
        }
        conn.execute("COMMIT")
            .await
            .expect("GH#289 seed transaction commit");
        quick_check(&conn)
            .await
            .unwrap_or_else(|error| panic!("GH#289 seed corrupt: {error}"));
        conn.query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("GH#289 seed checkpoint");
    });

    gh289_stock_repack(&path);

    let stop_readers = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_errors = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let reader_handles = (0..readers)
        .map(|reader_id| {
            let path = path.clone();
            let stop = std::sync::Arc::clone(&stop_readers);
            let errors = std::sync::Arc::clone(&reader_errors);
            std::thread::spawn(move || {
                asupersync::test_utils::run_test(move || async move {
                    let conn = match fsqlite::Connection::open(path).await {
                        Ok(conn) => conn,
                        Err(error) => {
                            errors
                                .lock()
                                .unwrap()
                                .push(format!("reader {reader_id} open failed: {error}"));
                            return;
                        }
                    };
                    drop(conn.execute("PRAGMA busy_timeout=10000").await);
                    while !stop.load(std::sync::atomic::Ordering::Acquire) {
                        match conn
                            .query(
                                "SELECT object_key,rowid
                         FROM gh289_objects
                         INDEXED BY sqlite_autoindex_gh289_objects_1
                         ORDER BY object_key LIMIT 64",
                            )
                            .await
                        {
                            Ok(rows) => {
                                if rows.windows(2).any(|pair| {
                                    let left = match pair[0].values().first() {
                                        Some(SqliteValue::Text(value)) => value.as_ref(),
                                        other => panic!(
                                            "GH#289 reader expected a TEXT autoindex key, got {other:?}"
                                        ),
                                    };
                                    let right = match pair[1].values().first() {
                                        Some(SqliteValue::Text(value)) => value.as_ref(),
                                        other => panic!(
                                            "GH#289 reader expected a TEXT autoindex key, got {other:?}"
                                        ),
                                    };
                                    left > right
                                }) {
                                    errors.lock().unwrap().push(format!(
                                        "reader {reader_id} observed out-of-order autoindex keys"
                                    ));
                                    return;
                                }
                            }
                            Err(error) if cc_is_transient(&error) => {}
                            Err(error) => {
                                errors
                                    .lock()
                                    .unwrap()
                                    .push(format!("reader {reader_id} index scan failed: {error}"));
                                return;
                            }
                        }
                        std::thread::yield_now();
                    }
                });
            })
        })
        .collect::<Vec<_>>();

    let writer_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(path.clone())
                .await
                .expect("GH#289 writer reopen");
            conn.execute("PRAGMA busy_timeout=10000")
                .await
                .expect("GH#289 writer busy timeout");
            conn.execute("PRAGMA journal_mode=WAL")
                .await
                .expect("GH#289 writer WAL mode");

            for transaction in 0..transactions {
                conn.execute("BEGIN IMMEDIATE")
                    .await
                    .unwrap_or_else(|error| panic!("GH#289 txn {transaction} begin: {error}"));
                for offset in 0..ROWS_PER_TRANSACTION {
                    let ordinal = seed_rows + transaction * ROWS_PER_TRANSACTION + offset;
                    gh289_insert_target_row(&conn, ordinal, &payload).await;
                }
                conn.execute(&format!(
                    "INSERT INTO gh289_blobs(txn_id,payload) VALUES({transaction},zeroblob({}))",
                    4_096 + transaction.rem_euclid(13) * 4_096
                ))
                .await
                .unwrap_or_else(|error| panic!("GH#289 txn {transaction} blob write: {error}"));
                for table in 0..AUX_TABLES {
                    conn.execute(&format!(
                        "INSERT INTO gh289_aux_{table}(id,value) \
                         VALUES({transaction},'txn-{transaction:06}-table-{table}')"
                    ))
                    .await
                    .unwrap_or_else(|error| {
                        panic!("GH#289 txn {transaction} auxiliary table {table}: {error}")
                    });
                }
                conn.execute("COMMIT")
                    .await
                    .unwrap_or_else(|error| panic!("GH#289 txn {transaction} commit: {error}"));

                for rollback in 0..3_i64 {
                    let ordinal = 10_000_000 + transaction * 10 + rollback;
                    conn.execute("BEGIN IMMEDIATE").await.unwrap_or_else(|error| {
                        panic!("GH#289 txn {transaction} rollback {rollback} begin: {error}")
                    });
                    gh289_insert_target_row(&conn, ordinal, &payload).await;
                    conn.execute("ROLLBACK").await.unwrap_or_else(|error| {
                        panic!("GH#289 txn {transaction} rollback {rollback}: {error}")
                    });
                }

                if transaction % 5 == 0 || transaction + 1 == transactions {
                    quick_check(&conn).await.unwrap_or_else(|error| {
                        panic!("GH#289 committed corruption after txn {transaction}: {error}")
                    });
                }
                if transaction % 10 == 9 {
                    if let Err(error) = conn.query("PRAGMA wal_checkpoint(PASSIVE)").await {
                        assert!(
                            error.to_string().contains("database is busy"),
                            "GH#289 checkpoint after txn {transaction}: {error}"
                        );
                    }
                }
            }
        });
    }));

    stop_readers.store(true, std::sync::atomic::Ordering::Release);
    for handle in reader_handles {
        handle.join().expect("GH#289 reader thread panicked");
    }
    let reader_errors = reader_errors.lock().unwrap();
    assert!(
        reader_errors.is_empty(),
        "GH#289 concurrent reader failures: {reader_errors:?}"
    );
    drop(reader_errors);
    if let Err(payload) = writer_result {
        std::panic::resume_unwind(payload);
    }

    asupersync::test_utils::run_test(|| async {
        let final_checkpoint = fsqlite::Connection::open(&path)
            .await
            .expect("GH#289 final checkpoint connection");
        final_checkpoint
            .query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("GH#289 final FrankenSQLite checkpoint after readers stopped");
    });

    gh289_assert_stock_canonical(&path, seed_rows + transactions * ROWS_PER_TRANSACTION);
}

#[test]
fn gh289_post_stock_vacuum_packed_unique_autoindex_stays_canonical() {
    gh289_post_stock_vacuum_stress(1_970, 40, 0);
}

#[test]
#[ignore = "GH#289 multi-btree + concurrent-reader packed-leaf stress; run explicitly"]
fn gh289_post_stock_vacuum_with_concurrent_readers_stays_canonical() {
    gh289_post_stock_vacuum_stress(
        env_i64("GH289_SEED_ROWS", 1_970),
        env_i64("GH289_TRANSACTIONS", 80),
        2,
    );
}

/// Deep-tree variant: a tiny `page_size` (512) under WAL forces the index
/// B-tree to many interior levels with only a few thousand rows, so every
/// churn round drives interior-page splits/merges at multiple levels — the
/// most direct stress on the interior-cell child-pointer handling the prod
/// `invalid page number` signature implicates. Cross-checked by canonical
/// SQLite, which natively parses the 512-byte-page file.
#[test]
fn index_churn_tiny_pages_deep_tree_stays_intact() {
    asupersync::test_utils::run_test(|| async {
        let rows = env_i64("Q37EP_ROWS", 6_000);
        let rounds = env_i64("Q37EP_ROUNDS", 80);
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("q37ep_deep.db");
        let path = db_path.to_string_lossy().into_owned();
        {
            let conn = fsqlite::Connection::open(path.clone())
                .await
                .expect("open frank");
            // Tiny pages + WAL BEFORE any table is created.
            conn.execute("PRAGMA page_size=512").await.unwrap();
            conn.execute("PRAGMA journal_mode=WAL").await.unwrap();
            conn.execute("PRAGMA foreign_keys=off").await.unwrap();
            conn.execute_batch(
                "CREATE TABLE atc_experiences (id INTEGER PRIMARY KEY, state TEXT, method TEXT);
             CREATE INDEX idx_atc_exp_open ON atc_experiences(state);
             CREATE INDEX idx_atc_exp_sm ON atc_experiences(state, method);",
            )
            .await
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
                    .await
                    .expect("seed");
                    chunk.clear();
                }
            }
            quick_check(&conn)
                .await
                .unwrap_or_else(|e| panic!("corrupt after seed: {e}"));

            for round in 0..rounds {
                conn.execute(&format!(
                    "UPDATE atc_experiences SET state = CASE state \
                 WHEN 'planned' THEN 'dispatched' WHEN 'dispatched' THEN 'executed' \
                 WHEN 'executed' THEN 'resolved' WHEN 'resolved' THEN 'planned' END \
                 WHERE id % 7 = {}",
                    round % 7
                ))
                .await
                .unwrap_or_else(|e| panic!("update round {round}: {e}"));
                if round % 8 == 0 || round == rounds - 1 {
                    if let Err(msg) = quick_check(&conn).await {
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
    });
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

async fn cc_with_retry<T>(
    label: &str,
    mut f: impl std::ops::AsyncFnMut() -> Result<T, fsqlite::FrankenError>,
) -> Result<T, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut attempt = 0u32;
    loop {
        match f().await {
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

async fn cc_writer(
    path: &str,
    writer_id: usize,
    writers: usize,
    rows: i64,
    rounds: i64,
    wal: bool,
    barrier: &std::sync::Barrier,
) -> Result<(), String> {
    let conn = fsqlite::Connection::open(path.to_string())
        .await
        .map_err(|e| format!("open: {e}"))?;
    drop(conn.execute("PRAGMA busy_timeout=10000;").await);
    if wal {
        cc_with_retry("journal_mode=WAL", async || {
            conn.execute("PRAGMA journal_mode=WAL;").await.map(|_| ())
        })
        .await?;
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
        cc_with_retry(&format!("advance w{writer_id} r{round}"), async || {
            conn.execute(&advance).await.map(|_| ())
        })
        .await?;

        // Every 4th round: free this writer's stripe then reinsert it (freelist
        // page reuse in the shared index B-tree).
        if round % 4 == 3 {
            cc_with_retry(&format!("delete w{writer_id} r{round}"), async || {
                conn.execute(&format!("DELETE FROM atc_experiences WHERE id % {n} = {w}"))
                    .await
                    .map(|_| ())
            })
            .await?;
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
                    cc_with_retry(&format!("reinsert w{writer_id} r{round}"), async || {
                        conn.execute(&stmt).await.map(|_| ())
                    })
                    .await?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                let stmt = format!("INSERT INTO atc_experiences(id,state,payload) VALUES {chunk}");
                cc_with_retry(&format!("reinsert tail w{writer_id} r{round}"), async || {
                    conn.execute(&stmt).await.map(|_| ())
                })
                .await?;
            }
        }
    }
    Ok(())
}

async fn concurrent_shared_index_churn(writers: usize, rows: i64, rounds: i64, wal: bool) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("q37ep_concurrent.db");
    let path = db_path.to_string_lossy().into_owned();

    // Seed rows 1..=rows partitioned by id % writers across the writers.
    {
        let conn = fsqlite::Connection::open(path.clone())
            .await
            .expect("open seed");
        if wal {
            conn.execute("PRAGMA journal_mode=WAL").await.unwrap();
        }
        conn.execute("PRAGMA foreign_keys=off").await.unwrap();
        conn.execute(
            "CREATE TABLE atc_experiences (id INTEGER PRIMARY KEY, state TEXT, payload TEXT)",
        )
        .await
        .expect("create table");
        conn.execute("CREATE INDEX idx_atc_exp_open ON atc_experiences(state)")
            .await
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
                .await
                .expect("seed");
                chunk.clear();
            }
        }
        quick_check(&conn)
            .await
            .unwrap_or_else(|e| panic!("corrupt after seed: {e}"));
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
                asupersync::test_utils::run_test(|| async {
                    if let Err(e) = cc_writer(&path, w, writers, rows, rounds, wal, &barrier).await {
                        eprintln!("[cc writer {w}] FAILED: {e}");
                        errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                });
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }

    // frank quick_check on the committed state.
    let verify = fsqlite::Connection::open(path.clone())
        .await
        .expect("verify open");
    if wal {
        drop(verify.execute("PRAGMA wal_checkpoint(TRUNCATE)").await);
    }
    if let Err(msg) = quick_check(&verify).await {
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
    asupersync::test_utils::run_test(|| async {
        concurrent_shared_index_churn(
            env_i64("Q37EP_WRITERS", 8) as usize,
            env_i64("Q37EP_ROWS", 6_000),
            env_i64("Q37EP_ROUNDS", 160),
            false,
        )
        .await;
    });
}

/// Concurrent shared-index churn in WAL mode (first-committer-wins page conflict
/// detection) — the harness's normal mode and the bd-9inpb control.
#[test]
#[ignore = "bd-9inpb/bd-l9yii concurrent shared-index churn stress harness; run with --ignored"]
fn concurrent_shared_index_churn_wal_stays_intact() {
    asupersync::test_utils::run_test(|| async {
        concurrent_shared_index_churn(
            env_i64("Q37EP_WRITERS", 8) as usize,
            env_i64("Q37EP_ROWS", 6_000),
            env_i64("Q37EP_ROUNDS", 160),
            true,
        )
        .await;
    });
}
