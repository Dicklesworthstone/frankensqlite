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
