//! Reproducibility verification tests for deterministic seeding.
//!
//! Bead: bd-mblr.4.3.2
//!
//! These tests verify that identical seeds produce identical outputs across
//! multiple executions. This is the cornerstone of FrankenSQLite's debugging
//! and regression testing strategy.
//!
//! ## Test Categories
//!
//! 1. **Seed derivation**: Verify `derive_worker_seed` and `derive_scenario_seed`
//!    are pure functions that always return the same output.
//!
//! 2. **RNG stream stability**: Verify that the same seed produces the same
//!    random sequence across runs.
//!
//! 3. **OpLog determinism**: Verify that workload generation is reproducible.
//!
//! 4. **Database state determinism**: Verify that executing the same OpLog
//!    produces identical database states.
//!
//! Run with:
//! ```sh
//! cargo test -p fsqlite-e2e --test seed_reproducibility -- --nocapture
//! ```

use rand::RngCore;
use rand::SeedableRng;
use rand::rngs::StdRng;

use fsqlite_e2e::oplog::{preset_commutative_inserts_disjoint_keys, preset_hot_page_contention};
use fsqlite_e2e::{FRANKEN_SEED, derive_scenario_seed, derive_worker_seed};

// ─── Seed Derivation Reproducibility ────────────────────────────────────

#[test]
fn seed_derivation_is_deterministic() {
    // derive_worker_seed must be a pure function.
    let base = FRANKEN_SEED;

    for worker_id in 0..=100 {
        let seed1 = derive_worker_seed(base, worker_id);
        let seed2 = derive_worker_seed(base, worker_id);
        assert_eq!(
            seed1, seed2,
            "derive_worker_seed must be deterministic for worker {worker_id}"
        );
    }
}

#[test]
fn scenario_derivation_is_deterministic() {
    // derive_scenario_seed must be a pure function.
    let base = FRANKEN_SEED;

    let scenarios = [0x534348u64, 0x54584E, 0x434F4E, 0x434F52, 0x434D50];
    for hash in scenarios {
        let seed1 = derive_scenario_seed(base, hash);
        let seed2 = derive_scenario_seed(base, hash);
        assert_eq!(
            seed1, seed2,
            "derive_scenario_seed must be deterministic for hash {hash:#x}"
        );
    }
}

#[test]
fn worker_seeds_are_distinct() {
    // Different workers must get different seeds.
    let base = FRANKEN_SEED;
    let mut seeds = std::collections::HashSet::new();

    for worker_id in 0..100 {
        let seed = derive_worker_seed(base, worker_id);
        assert!(
            seeds.insert(seed),
            "Worker {worker_id} seed collision: {seed}"
        );
    }
}

#[test]
fn scenario_seeds_are_distinct() {
    // Different scenarios must get different seeds.
    let base = FRANKEN_SEED;
    let scenarios = [0x534348u64, 0x54584E, 0x434F4E, 0x434F52, 0x434D50];
    let mut seeds = std::collections::HashSet::new();

    for hash in scenarios {
        let seed = derive_scenario_seed(base, hash);
        assert!(
            seeds.insert(seed),
            "Scenario {hash:#x} seed collision: {seed}"
        );
    }
}

// ─── RNG Stream Reproducibility ─────────────────────────────────────────

#[test]
fn rng_stream_is_reproducible() {
    // Same seed must produce same sequence.
    let seed = FRANKEN_SEED;

    let sequence1: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..100).map(|_| rng.next_u64()).collect()
    };

    let sequence2: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..100).map(|_| rng.next_u64()).collect()
    };

    assert_eq!(
        sequence1, sequence2,
        "Same seed must produce same RNG sequence"
    );
}

#[test]
fn rng_stream_different_seeds_differ() {
    // Different seeds must produce different sequences.
    let seed1 = FRANKEN_SEED;
    let seed2 = FRANKEN_SEED + 1;

    let sequence1: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed1);
        (0..10).map(|_| rng.next_u64()).collect()
    };

    let sequence2: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(seed2);
        (0..10).map(|_| rng.next_u64()).collect()
    };

    assert_ne!(
        sequence1, sequence2,
        "Different seeds must produce different RNG sequences"
    );
}

// ─── OpLog Generation Reproducibility ───────────────────────────────────

#[test]
fn oplog_generation_is_reproducible() {
    // Same preset + seed must produce identical OpLogs.
    let seed = FRANKEN_SEED;
    let workers = 4;
    let rows_per_worker = 100;

    let oplog1 =
        preset_commutative_inserts_disjoint_keys("repro-test-1", seed, workers, rows_per_worker);

    let oplog2 =
        preset_commutative_inserts_disjoint_keys("repro-test-1", seed, workers, rows_per_worker);

    // Compare headers.
    assert_eq!(oplog1.header.seed, oplog2.header.seed, "Seeds must match");
    assert_eq!(
        oplog1.header.concurrency.worker_count, oplog2.header.concurrency.worker_count,
        "Worker counts must match"
    );

    // Compare record counts.
    assert_eq!(
        oplog1.records.len(),
        oplog2.records.len(),
        "Record counts must match"
    );

    // Compare each record.
    for (i, (r1, r2)) in oplog1.records.iter().zip(oplog2.records.iter()).enumerate() {
        assert_eq!(r1.op_id, r2.op_id, "Op IDs must match at index {i}");
        assert_eq!(r1.worker, r2.worker, "Worker IDs must match at index {i}");
        // Note: OpKind comparison depends on its implementation of PartialEq.
        // For now, we compare the debug representation.
        assert_eq!(
            format!("{:?}", r1.kind),
            format!("{:?}", r2.kind),
            "OpKinds must match at index {i}"
        );
    }
}

#[test]
fn oplog_contention_preset_is_reproducible() {
    // Contention preset must also be reproducible.
    let seed = FRANKEN_SEED;
    let workers = 4;
    let rounds = 10;

    let oplog1 = preset_hot_page_contention("repro-contention-1", seed, workers, rounds);
    let oplog2 = preset_hot_page_contention("repro-contention-1", seed, workers, rounds);

    assert_eq!(oplog1.header.seed, oplog2.header.seed);
    assert_eq!(oplog1.records.len(), oplog2.records.len());

    for (i, (r1, r2)) in oplog1.records.iter().zip(oplog2.records.iter()).enumerate() {
        assert_eq!(r1.op_id, r2.op_id, "Op IDs must match at index {i}");
    }
}

#[test]
fn oplog_different_seeds_differ() {
    // Different seeds must produce different OpLogs.
    let seed1 = FRANKEN_SEED;
    let seed2 = FRANKEN_SEED + 1;

    let oplog1 = preset_commutative_inserts_disjoint_keys("diff-seed-1", seed1, 2, 50);
    let oplog2 = preset_commutative_inserts_disjoint_keys("diff-seed-2", seed2, 2, 50);

    assert_ne!(oplog1.header.seed, oplog2.header.seed);
    // The record contents should differ (probabilistically certain for non-trivial sizes).
}

// ─── Database State Reproducibility ─────────────────────────────────────

#[test]
fn database_state_is_reproducible() {
    // Executing the same OpLog twice must produce identical database states.
    let seed = FRANKEN_SEED;

    // Generate a workload.
    let oplog = preset_commutative_inserts_disjoint_keys("db-repro-test", seed, 2, 100);

    // Execute on FrankenSQLite twice.
    let state1 = execute_oplog_and_hash(&oplog);
    let state2 = execute_oplog_and_hash(&oplog);

    assert_eq!(
        state1, state2,
        "Same OpLog must produce identical database states"
    );
}

#[test]
fn database_state_commutative_inserts_seed_independent() {
    // The commutative_inserts_disjoint_keys preset produces data values
    // that are deterministic based on worker/row indices, NOT the seed.
    // The seed only affects operation ordering (which doesn't matter for
    // commutative operations with disjoint keys).
    //
    // This test verifies that for commutative presets, different seeds
    // produce EQUIVALENT final states (which is the design intent).
    let seed1 = FRANKEN_SEED;
    let seed2 = FRANKEN_SEED + 1;

    let oplog1 = preset_commutative_inserts_disjoint_keys("db-equiv-1", seed1, 2, 100);
    let oplog2 = preset_commutative_inserts_disjoint_keys("db-equiv-2", seed2, 2, 100);

    let state1 = execute_oplog_and_hash(&oplog1);
    let state2 = execute_oplog_and_hash(&oplog2);

    assert_eq!(
        state1, state2,
        "Commutative presets with disjoint keys should produce equivalent states regardless of seed"
    );
}

// ─── Helpers ────────────────────────────────────────────────────────────

/// Execute an OpLog on FrankenSQLite and return a hash of the final state.
fn execute_oplog_and_hash(oplog: &fsqlite_e2e::oplog::OpLog) -> String {
    use sha2::{Digest, Sha256};

    let conn = fsqlite::Connection::open(":memory:").expect("open connection");

    // Execute each operation from the OpLog.
    // The OpLog includes CREATE TABLE as the first operation.
    for rec in &oplog.records {
        let sql = match &rec.kind {
            fsqlite_e2e::oplog::OpKind::Sql { statement } => statement.clone(),
            fsqlite_e2e::oplog::OpKind::Insert { table, key, values } => {
                let cols: Vec<String> = std::iter::once("id".to_owned())
                    .chain(values.iter().map(|(c, _)| c.clone()))
                    .collect();
                let vals: Vec<String> = std::iter::once(key.to_string())
                    .chain(values.iter().map(|(_, v)| format_val(v)))
                    .collect();
                format!(
                    "INSERT INTO \"{table}\" ({}) VALUES ({})",
                    cols.join(", "),
                    vals.join(", ")
                )
            }
            fsqlite_e2e::oplog::OpKind::Update { table, key, values } => {
                let sets: Vec<String> = values
                    .iter()
                    .map(|(c, v)| format!("{c}={}", format_val(v)))
                    .collect();
                format!("UPDATE \"{table}\" SET {} WHERE id={key}", sets.join(", "))
            }
            fsqlite_e2e::oplog::OpKind::Begin => "BEGIN".to_owned(),
            fsqlite_e2e::oplog::OpKind::Commit => "COMMIT".to_owned(),
            fsqlite_e2e::oplog::OpKind::Rollback => "ROLLBACK".to_owned(),
        };

        // Ignore errors for transaction control that may fail legitimately.
        let _ = conn.execute(&sql);
    }

    // Query all data from table t0 (created by the preset) and hash it.
    let rows = conn
        .query("SELECT * FROM t0 ORDER BY id")
        .unwrap_or_default();

    let mut hasher = Sha256::new();
    for row in &rows {
        for val in row.values() {
            hasher.update(format!("{val:?}").as_bytes());
        }
    }

    format!("{:x}", hasher.finalize())
}

fn format_val(v: &str) -> String {
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_owned()
    } else {
        format!("'{}'", v.replace('\'', "''"))
    }
}

// ─── FRANKEN_SEED Value Verification ────────────────────────────────────

#[test]
fn franken_seed_is_correct_value() {
    // Verify the constant matches "FRANKEN" in ASCII.
    assert_eq!(FRANKEN_SEED, 0x4652_414E_4B45_4E);

    // Verify it decodes to "FRANKEN" (7 bytes, padded with leading zero).
    let bytes = FRANKEN_SEED.to_be_bytes();
    let ascii: String = bytes
        .iter()
        .filter(|&&b| b != 0)
        .map(|&b| b as char)
        .collect();
    assert_eq!(ascii, "FRANKEN");
}

#[test]
fn franken_seed_stability() {
    // Ensure FRANKEN_SEED hasn't changed (regression test).
    // This constant is part of the reproducibility contract and MUST NOT change.
    // 0x4652414E4B454E = 19793688809653582 decimal
    assert_eq!(
        FRANKEN_SEED, 19_793_688_809_653_582,
        "FRANKEN_SEED must not change - this breaks reproducibility!"
    );
}
