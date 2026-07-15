//! INSERT-dominated drift audit for UNIQUE (auto)indexes at the SQL level:
//! under a sustained insert workload of hash-shaped unique keys — including
//! a mid-stream REINDEX and the OR IGNORE / OR REPLACE conflict flows an
//! ingest pipeline produces — every inserted key must remain reachable
//! through the index probe. The decisive oracle is the UNIQUE constraint
//! itself: re-inserting an existing key MUST fail — the uniqueness probe is
//! an index seek, so a key that landed out of b-tree order (unreachable by
//! seek) would let the duplicate through. A full-scan count and
//! `PRAGMA integrity_check` back it up.
//!
//! Companion coverage to the b-tree-level tests
//! `test_unique_index_scattered_insert_only_keys_stay_seek_reachable` and
//! `test_unique_index_monotonic_fast_append_across_splits_stays_seek_reachable`
//! (crates/fsqlite-btree/src/cursor.rs): this exercises the same two cursor
//! primitives through the whole VDBE `IdxInsert` flow, including the
//! rightmost fast-append hint bookkeeping that persists across rows of the
//! statement stream.
//!
//! All data is synthetic (deterministic xorshift), sized so the index b-tree
//! goes multiple levels deep and splits constantly.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

const ROWS: u64 = 3000;

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// 192-hex-char scattered key (hash-shaped), deterministic per ordinal.
fn scattered_key(state: &mut u64) -> String {
    let a = xorshift64(state);
    let b = xorshift64(state);
    let c = xorshift64(state);
    format!("{a:016x}{b:016x}{c:016x}").repeat(4)
}

/// Ascending key with the same footprint, for the fast-append hint path.
fn ascending_key(i: u64) -> String {
    format!("{i:048x}").repeat(4)
}

fn count_one(c: &Connection, sql: &str) -> i64 {
    let rows = c.query(sql).unwrap();
    match rows.first().and_then(|row| row.values().first().cloned()) {
        Some(SqliteValue::Integer(n)) => n,
        other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

fn assert_integrity_ok(c: &Connection, label: &str) {
    let rows = c.query("PRAGMA integrity_check").unwrap();
    let msgs: Vec<SqliteValue> = rows.iter().flat_map(|row| row.values().to_vec()).collect();
    assert_eq!(
        msgs,
        vec![SqliteValue::Text("ok".into())],
        "integrity_check after {label}"
    );
}

fn run_workload(keys: &[String], label: &str) {
    let c = Connection::open(":memory:").unwrap();
    c.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, h TEXT UNIQUE, body BLOB)")
        .unwrap();

    // Phase 1: load the first half, then REINDEX (rebuilds the unique index
    // btree), then keep inserting into the rebuilt index — the sequence that
    // must NOT re-introduce drift.
    let half = keys.len() / 2;
    for (i, h) in keys.iter().take(half).enumerate() {
        let body = format!("{:02X}", i % 251).repeat(120);
        c.execute(&format!(
            "INSERT INTO a VALUES ({}, '{h}', X'{body}')",
            i + 1
        ))
        .unwrap_or_else(|error| panic!("{label}: insert {i} must succeed: {error}"));
    }
    c.execute("REINDEX").unwrap();

    // Phase 2: the remaining inserts, interleaved with the conflict-handling
    // flows an ingest workload produces: INSERT OR IGNORE re-puts of an
    // already-present key (index-conflict rollback path) and INSERT OR
    // REPLACE re-puts (conflict row + index-entry replacement path).
    for (i, h) in keys.iter().enumerate().skip(half) {
        let body = format!("{:02X}", i % 251).repeat(120);
        c.execute(&format!(
            "INSERT INTO a VALUES ({}, '{h}', X'{body}')",
            i + 1
        ))
        .unwrap_or_else(|error| panic!("{label}: insert {i} must succeed: {error}"));
        if i % 8 == 0 {
            let re_put = &keys[i / 2];
            c.execute(&format!(
                "INSERT OR IGNORE INTO a VALUES ({}, '{re_put}', X'AA')",
                2_000_000 + i
            ))
            .unwrap_or_else(|error| {
                panic!("{label}: OR IGNORE re-put {i} must not error: {error}")
            });
        }
        if i % 13 == 0 {
            let re_put = &keys[i / 3];
            let body = format!("{:02X}", (i + 1) % 251).repeat(120);
            c.execute(&format!(
                "INSERT OR REPLACE INTO a VALUES ({}, '{re_put}', X'{body}')",
                (i / 3) + 1
            ))
            .unwrap_or_else(|error| {
                panic!("{label}: OR REPLACE re-put {i} must not error: {error}")
            });
        }
    }
    assert_eq!(
        count_one(&c, "SELECT count(*) FROM a"),
        i64::try_from(keys.len()).unwrap(),
        "{label}: row count after load"
    );

    // Oracle 1: every key must be refused as a duplicate. The uniqueness
    // probe is an index seek; a key that landed out of order would be
    // invisible to it and the duplicate would slip through.
    let mut slipped = Vec::new();
    for (i, h) in keys.iter().enumerate() {
        let dup = c.execute(&format!(
            "INSERT INTO a VALUES ({}, '{h}', X'00')",
            1_000_000 + i
        ));
        if dup.is_ok() {
            slipped.push(i);
        }
    }
    assert!(
        slipped.is_empty(),
        "{label}: {} of {} keys were not visible to the UNIQUE probe \
         (index entry unreachable by seek); first insert ordinals: {:?}",
        slipped.len(),
        keys.len(),
        &slipped[..slipped.len().min(10)]
    );

    // Oracle 2: no duplicate insert may have slipped through and no row may
    // have been lost.
    assert_eq!(
        count_one(&c, "SELECT count(*) FROM a"),
        i64::try_from(keys.len()).unwrap(),
        "{label}: row count after duplicate probes"
    );
    assert_eq!(
        count_one(&c, "SELECT count(DISTINCT h) FROM a"),
        i64::try_from(keys.len()).unwrap(),
        "{label}: distinct key count"
    );

    // Oracle 3: structural verification.
    assert_integrity_ok(&c, label);
}

/// Scattered (hash-shaped) unique keys: the shape of a content-hash digest
/// column. Exercises the canonical unique-probe insert on every row, with
/// constant leaf and interior splits.
#[test]
fn insert_only_unique_index_scattered_keys_probe_every_row() {
    let mut state = 0xDEAD_BEEF_CAFE_F00D_u64;
    let keys: Vec<String> = (0..ROWS).map(|_| scattered_key(&mut state)).collect();
    run_workload(&keys, "scattered");
}

/// Ascending unique keys: enables the VDBE rightmost fast-append hint on
/// most rows (with fallback re-probes right after splits).
#[test]
fn insert_only_unique_index_ascending_keys_probe_every_row() {
    let keys: Vec<String> = (1..=ROWS).map(ascending_key).collect();
    run_workload(&keys, "ascending");
}
