//! E2E Test: bd-3plop.5 — SSI serialization correctness under concurrent writers.
//!
//! This test stresses FrankenSQLite's concurrent-writer path and validates:
//! - No deadlock/livelock under concurrent write pressure (bounded retries).
//! - Global balance invariants hold.
//! - No account goes negative.
//! - A conflict graph derived from committed transactions is acyclic.
//! - Abort rate and throughput stay within target bounds for CI scale.

use std::collections::{BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

const CI_WRITERS: usize = 10;
const CI_TXNS_PER_WRITER: usize = 1_000;
const STRESS_WRITERS: usize = 100;
const STRESS_TXNS_PER_WRITER: usize = 10_000;
const ACCOUNT_COUNT: i64 = 128;
const INITIAL_BALANCE: i64 = 1_000;
const MAX_RETRIES_PER_TXN: usize = 64;
const MIX_TRANSFER_PCT: u8 = 70;
const MIX_DEPOSIT_PCT: u8 = 20;
const MIX_BALANCE_CHECK_PCT: u8 = 10;
const MIN_CI_THROUGHPUT_TXN_PER_SEC: f64 = 1_000.0;
const MAX_ABORT_RATE: f64 = 0.20;
const TEST_SEED: u64 = 0xBDBD_3010_05AA_55EE;

#[derive(Clone, Copy)]
enum TxnKind {
    Transfer,
    Deposit,
    BalanceCheck,
}

#[derive(Debug, Clone)]
struct CommittedTxn {
    start_order: u64,
    commit_order: u64,
    read_set: BTreeSet<i64>,
    write_set: BTreeSet<i64>,
}

#[derive(Debug, Default)]
struct WorkerResult {
    committed: u64,
    aborted: u64,
    hard_failures: Vec<String>,
    sum_delta: i64,
    txns: Vec<CommittedTxn>,
}

#[test]
fn ssi_serialization_correctness_ci_scale() {
    let summary = run_ssi_workload(CI_WRITERS, CI_TXNS_PER_WRITER, TEST_SEED, "ci-scale");

    let attempted = summary.committed + summary.aborted;
    assert!(attempted > 0, "expected at least one attempted transaction");

    #[allow(clippy::cast_precision_loss)]
    let abort_rate = summary.aborted as f64 / attempted as f64;
    #[allow(clippy::cast_precision_loss)]
    let throughput = summary.committed as f64 / summary.elapsed_seconds;

    assert!(
        abort_rate < MAX_ABORT_RATE,
        "abort rate too high: {:.3} (max {:.3}); committed={} aborted={}",
        abort_rate,
        MAX_ABORT_RATE,
        summary.committed,
        summary.aborted
    );
    assert!(
        throughput > MIN_CI_THROUGHPUT_TXN_PER_SEC,
        "throughput too low: {:.1} txn/s (min {:.1} txn/s); committed={} elapsed={:.3}s",
        throughput,
        MIN_CI_THROUGHPUT_TXN_PER_SEC,
        summary.committed,
        summary.elapsed_seconds
    );
}

#[test]
#[ignore = "long-running stress profile for bd-3plop.5 acceptance envelope"]
fn ssi_serialization_correctness_stress_profile() {
    let summary = run_ssi_workload(STRESS_WRITERS, STRESS_TXNS_PER_WRITER, TEST_SEED, "stress");
    let attempted = summary.committed + summary.aborted;
    assert!(attempted > 0, "stress run produced zero attempts");

    #[allow(clippy::cast_precision_loss)]
    let abort_rate = summary.aborted as f64 / attempted as f64;
    assert!(
        abort_rate < MAX_ABORT_RATE,
        "stress abort rate too high: {:.3} (max {:.3})",
        abort_rate,
        MAX_ABORT_RATE
    );
}

#[derive(Debug)]
struct WorkloadSummary {
    committed: u64,
    aborted: u64,
    elapsed_seconds: f64,
}

fn run_ssi_workload(
    writers: usize,
    txns_per_writer: usize,
    seed: u64,
    label: &str,
) -> WorkloadSummary {
    let db_dir = tempfile::tempdir().expect("create temp directory for workload");
    let db_path = db_dir.path().join("ssi_serialization.db");
    initialize_db(&db_path);

    let start_counter = Arc::new(AtomicU64::new(1));
    let commit_counter = Arc::new(AtomicU64::new(1));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(writers);
    for worker_id in 0..writers {
        let path = db_path.clone();
        let worker_seed = derive_worker_seed(seed, worker_id);
        let start_ref = Arc::clone(&start_counter);
        let commit_ref = Arc::clone(&commit_counter);
        handles.push(thread::spawn(move || {
            run_worker(
                &path,
                worker_id,
                txns_per_writer,
                worker_seed,
                &start_ref,
                &commit_ref,
            )
        }));
    }

    let mut committed = 0_u64;
    let mut aborted = 0_u64;
    let mut sum_delta = 0_i64;
    let mut committed_txns = Vec::new();
    let mut hard_failures = Vec::new();
    for handle in handles {
        let result = handle
            .join()
            .expect("worker thread should not panic during SSI workload");
        committed += result.committed;
        aborted += result.aborted;
        sum_delta += result.sum_delta;
        committed_txns.extend(result.txns);
        hard_failures.extend(result.hard_failures);
    }
    let elapsed_seconds = started.elapsed().as_secs_f64();

    assert!(
        hard_failures.is_empty(),
        "hard failures in {label} run: {}",
        hard_failures.join(" | ")
    );

    let (final_sum, min_balance) = read_account_invariants(&db_path);
    let initial_sum = ACCOUNT_COUNT * INITIAL_BALANCE;
    let expected_sum = initial_sum + sum_delta;

    assert_eq!(
        final_sum, expected_sum,
        "sum invariant violated in {label}: final_sum={final_sum} expected_sum={expected_sum} initial_sum={initial_sum} sum_delta={sum_delta}"
    );
    assert!(
        min_balance >= 0,
        "negative balance observed in {label}: min_balance={min_balance}"
    );

    let cycle = detect_cycle(&committed_txns);
    assert!(
        !cycle,
        "serialization graph contains a cycle in {label}; committed_txns={}",
        committed_txns.len()
    );

    WorkloadSummary {
        committed,
        aborted,
        elapsed_seconds,
    }
}

fn initialize_db(path: &Path) {
    let conn = fsqlite::Connection::open(path.to_string_lossy().as_ref())
        .expect("open db for initialization");
    conn.execute("PRAGMA journal_mode=WAL;")
        .expect("set WAL mode");
    conn.execute("PRAGMA busy_timeout=5000;")
        .expect("set busy timeout");
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .expect("enable concurrent mode");
    conn.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL
        );",
    )
    .expect("create accounts table");

    for id in 1..=ACCOUNT_COUNT {
        conn.execute(&format!(
            "INSERT INTO accounts (id, balance) VALUES ({id}, {INITIAL_BALANCE});"
        ))
        .expect("seed account row");
    }
}

fn run_worker(
    db_path: &Path,
    worker_id: usize,
    txns_per_worker: usize,
    seed: u64,
    start_counter: &AtomicU64,
    commit_counter: &AtomicU64,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    let mut rng = StdRng::seed_from_u64(seed);
    let conn = fsqlite::Connection::open(db_path.to_string_lossy().as_ref())
        .expect("open worker connection");
    conn.execute("PRAGMA busy_timeout=5000;")
        .expect("set worker busy timeout");
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .expect("enable worker concurrent mode");

    for txn_index in 0..txns_per_worker {
        let mut retries = 0_usize;
        loop {
            let start_order = start_counter.fetch_add(1, Ordering::SeqCst);
            let kind = choose_txn_kind(&mut rng);

            let execute_result: Result<_, FrankenError> = execute_single_txn(&conn, &mut rng, kind);
            match execute_result {
                Ok((read_set, write_set, delta_sum)) => {
                    let commit_order = commit_counter.fetch_add(1, Ordering::SeqCst);
                    result.committed += 1;
                    result.sum_delta += delta_sum;
                    result.txns.push(CommittedTxn {
                        start_order,
                        commit_order,
                        read_set,
                        write_set,
                    });
                    break;
                }
                Err(err) if err.is_transient() => {
                    result.aborted += 1;
                    rollback_best_effort(&conn);
                    retries += 1;
                    if retries > MAX_RETRIES_PER_TXN {
                        result.hard_failures.push(format!(
                            "worker={worker_id} txn_index={txn_index} exceeded retries on transient error: {err}"
                        ));
                        break;
                    }
                }
                Err(err) => {
                    result.aborted += 1;
                    rollback_best_effort(&conn);
                    result.hard_failures.push(format!(
                        "worker={worker_id} txn_index={txn_index} non-transient error: {err}"
                    ));
                    break;
                }
            }
        }
    }

    result
}

fn execute_single_txn(
    conn: &fsqlite::Connection,
    rng: &mut StdRng,
    kind: TxnKind,
) -> Result<(BTreeSet<i64>, BTreeSet<i64>, i64), FrankenError> {
    conn.execute("BEGIN CONCURRENT;")?;

    let mut read_set = BTreeSet::new();
    let mut write_set = BTreeSet::new();
    let mut delta_sum = 0_i64;

    match kind {
        TxnKind::Transfer => {
            let from = random_account(rng);
            let mut to = random_account(rng);
            if to == from {
                to = if to == ACCOUNT_COUNT { 1 } else { to + 1 };
            }
            let amount = i64::from(rng.gen_range(1_u8..=5_u8));

            let from_balance = read_balance(conn, from)?;
            let _to_balance = read_balance(conn, to)?;
            read_set.insert(from);
            read_set.insert(to);

            if from_balance >= amount {
                conn.execute(&format!(
                    "UPDATE accounts SET balance = balance - {amount} WHERE id = {from};"
                ))?;
                conn.execute(&format!(
                    "UPDATE accounts SET balance = balance + {amount} WHERE id = {to};"
                ))?;
                write_set.insert(from);
                write_set.insert(to);
            }
        }
        TxnKind::Deposit => {
            let account = random_account(rng);
            let amount = i64::from(rng.gen_range(1_u8..=3_u8));

            let _before = read_balance(conn, account)?;
            read_set.insert(account);

            conn.execute(&format!(
                "UPDATE accounts SET balance = balance + {amount} WHERE id = {account};"
            ))?;
            write_set.insert(account);
            delta_sum += amount;
        }
        TxnKind::BalanceCheck => {
            let _ = read_sum(conn)?;
        }
    }

    conn.execute("COMMIT;")?;
    Ok((read_set, write_set, delta_sum))
}

fn choose_txn_kind(rng: &mut StdRng) -> TxnKind {
    let bucket = rng.gen_range(0_u8..100_u8);
    if bucket < MIX_TRANSFER_PCT {
        TxnKind::Transfer
    } else if bucket < MIX_TRANSFER_PCT + MIX_DEPOSIT_PCT {
        TxnKind::Deposit
    } else {
        debug_assert_eq!(
            MIX_TRANSFER_PCT + MIX_DEPOSIT_PCT + MIX_BALANCE_CHECK_PCT,
            100
        );
        TxnKind::BalanceCheck
    }
}

fn random_account(rng: &mut StdRng) -> i64 {
    rng.gen_range(1_i64..=ACCOUNT_COUNT)
}

fn rollback_best_effort(conn: &fsqlite::Connection) {
    let _ = conn.execute("ROLLBACK;");
}

fn read_balance(conn: &fsqlite::Connection, account_id: i64) -> Result<i64, FrankenError> {
    let row = conn.query_row(&format!(
        "SELECT balance FROM accounts WHERE id = {account_id};"
    ))?;
    extract_int(&row, 0)
}

fn read_sum(conn: &fsqlite::Connection) -> Result<i64, FrankenError> {
    let row = conn.query_row("SELECT SUM(balance) FROM accounts;")?;
    extract_int(&row, 0)
}

fn extract_int(row: &fsqlite::Row, index: usize) -> Result<i64, FrankenError> {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        Some(other) => Err(FrankenError::Internal(format!(
            "expected integer column at index {index}, got {other:?}"
        ))),
        None => Err(FrankenError::Internal(format!(
            "missing column at index {index}"
        ))),
    }
}

fn read_account_invariants(path: &Path) -> (i64, i64) {
    let conn = fsqlite::Connection::open(path.to_string_lossy().as_ref())
        .expect("open verifier connection");

    let sum_row = conn
        .query_row("SELECT SUM(balance) FROM accounts;")
        .expect("query sum");
    let final_sum = extract_int(&sum_row, 0).expect("extract sum");

    let min_row = conn
        .query_row("SELECT MIN(balance) FROM accounts;")
        .expect("query min balance");
    let min_balance = extract_int(&min_row, 0).expect("extract min balance");

    (final_sum, min_balance)
}

fn detect_cycle(txns: &[CommittedTxn]) -> bool {
    let node_count = txns.len();
    if node_count <= 1 {
        return false;
    }

    let mut edges: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); node_count];
    let mut indegree = vec![0_usize; node_count];

    for left_idx in 0..node_count {
        for right_idx in (left_idx + 1)..node_count {
            let left = &txns[left_idx];
            let right = &txns[right_idx];

            let mut add_edge = |from: usize, to: usize| {
                if edges[from].insert(to) {
                    indegree[to] += 1;
                }
            };

            if intersects(&left.write_set, &right.write_set) {
                if left.commit_order <= right.commit_order {
                    add_edge(left_idx, right_idx);
                } else {
                    add_edge(right_idx, left_idx);
                }
            }

            if intersects(&left.write_set, &right.read_set) {
                orient_read_write_conflict(left, right, left_idx, right_idx, &mut add_edge);
            }
            if intersects(&right.write_set, &left.read_set) {
                orient_read_write_conflict(right, left, right_idx, left_idx, &mut add_edge);
            }
        }
    }

    let mut queue = VecDeque::new();
    for (idx, degree) in indegree.iter().enumerate() {
        if *degree == 0 {
            queue.push_back(idx);
        }
    }

    let mut visited = 0_usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        for &next in &edges[node] {
            indegree[next] -= 1;
            if indegree[next] == 0 {
                queue.push_back(next);
            }
        }
    }

    visited != node_count
}

fn orient_read_write_conflict(
    writer: &CommittedTxn,
    reader: &CommittedTxn,
    writer_idx: usize,
    reader_idx: usize,
    add_edge: &mut impl FnMut(usize, usize),
) {
    if writer.commit_order <= reader.start_order {
        // Writer committed before reader started: WR dependency writer -> reader.
        add_edge(writer_idx, reader_idx);
    } else if reader.commit_order <= writer.start_order {
        // Reader finished before writer started: RW anti-dependency reader -> writer.
        add_edge(reader_idx, writer_idx);
    } else {
        // Concurrent overlap: reader observed a snapshot while writer committed.
        // Model this as anti-dependency reader -> writer.
        add_edge(reader_idx, writer_idx);
    }
}

fn intersects(left: &BTreeSet<i64>, right: &BTreeSet<i64>) -> bool {
    left.iter().any(|item| right.contains(item))
}

fn derive_worker_seed(seed: u64, worker_id: usize) -> u64 {
    let worker = u64::try_from(worker_id).expect("worker id should fit into u64");
    seed ^ worker.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}
