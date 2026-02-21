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
use std::thread;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use fsqlite_error::FrankenError;
use fsqlite_mvcc::{RetryAction, RetryController, RetryCostParams};
use fsqlite_types::value::SqliteValue;

const CI_WRITERS: usize = 10;
const CI_TXNS_PER_WRITER: usize = 1_000;
const SINGLE_WRITER_TXNS: usize = 1_000;
const STRESS_WRITERS: usize = 100;
const STRESS_TXNS_PER_WRITER: usize = 10_000;
// Keep the logical workload hot, but not pathologically single-page hot.
// With 100 concurrent writers, 128 rows collapses onto too few leaf pages and
// measures page-hotspot collapse more than SSI behavior.
const ACCOUNT_COUNT: i64 = 4_096;
const INITIAL_BALANCE: i64 = 1_000;
const MAX_RETRIES_PER_TXN: usize = 16;
const BUSY_TIMEOUT_MS: u64 = 5_000;
const RETRY_SLEEP_JITTER_MS: u64 = 2;
const RETRY_CONTROLLER_FALLBACK_SLEEP_MS: u64 = 20;
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
    retry_conflicts: u64,
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
        "abort rate too high: {:.3} (max {:.3}); committed={} aborted={} retry_conflicts={}",
        abort_rate,
        MAX_ABORT_RATE,
        summary.committed,
        summary.aborted,
        summary.retry_conflicts
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
fn ssi_serialization_correctness_single_writer_smoke() {
    let summary = run_ssi_workload(1, SINGLE_WRITER_TXNS, TEST_SEED, "single-writer");
    let attempted = summary.committed + summary.aborted;
    assert!(
        attempted > 0,
        "expected at least one attempted single-writer transaction"
    );
    assert_eq!(
        summary.aborted, 0,
        "single-writer run should not abort under page-level FCW"
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
        "stress abort rate too high: {:.3} (max {:.3}); retry_conflicts={}",
        abort_rate,
        MAX_ABORT_RATE,
        summary.retry_conflicts
    );
}

#[derive(Debug)]
struct WorkloadSummary {
    committed: u64,
    aborted: u64,
    retry_conflicts: u64,
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

    let started = Instant::now();
    let mut handles = Vec::with_capacity(writers);
    for worker_id in 0..writers {
        let path = db_path.clone();
        let worker_seed = derive_worker_seed(seed, worker_id);
        handles.push(thread::spawn(move || {
            run_worker(&path, worker_id, txns_per_writer, worker_seed)
        }));
    }

    let mut committed = 0_u64;
    let mut aborted = 0_u64;
    let mut retry_conflicts = 0_u64;
    let mut sum_delta = 0_i64;
    let mut committed_txns = Vec::new();
    let mut hard_failures = Vec::new();
    for handle in handles {
        let result = handle
            .join()
            .expect("worker thread should not panic during SSI workload");
        committed += result.committed;
        aborted += result.aborted;
        retry_conflicts += result.retry_conflicts;
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

    if let Some(cycle) = detect_cycle(&committed_txns) {
        let witness = render_cycle_witness(&committed_txns, &cycle);
        panic!(
            "serialization graph contains a cycle in {label}; committed_txns={}; witness={witness}",
            committed_txns.len()
        );
    }

    WorkloadSummary {
        committed,
        aborted,
        retry_conflicts,
        elapsed_seconds,
    }
}

fn initialize_db(path: &Path) {
    let conn = fsqlite::Connection::open(path.to_string_lossy().as_ref())
        .expect("open db for initialization");
    conn.execute("PRAGMA journal_mode=WAL;")
        .expect("set WAL mode");
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"))
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

fn run_worker(db_path: &Path, worker_id: usize, txns_per_worker: usize, seed: u64) -> WorkerResult {
    let mut result = WorkerResult::default();
    let mut rng = StdRng::seed_from_u64(seed);
    // Keep retries adaptive instead of hot-looping under page-level contention.
    let mut retry_controller =
        RetryController::with_candidates(RetryCostParams::default(), vec![1, 2, 5, 10, 20], 8);
    let conn = fsqlite::Connection::open(db_path.to_string_lossy().as_ref())
        .expect("open worker connection");
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"))
        .expect("set worker busy timeout");
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .expect("enable worker concurrent mode");

    for txn_index in 0..txns_per_worker {
        let worker_token = u64::try_from(worker_id).expect("worker id should fit in u64");
        let txn_token = u64::try_from(txn_index).expect("txn index should fit in u64");
        let logical_txn_id = (worker_token << 32) | txn_token;

        let mut retries = 0_usize;
        let mut last_wait_ms = None;
        loop {
            let kind = choose_txn_kind(&mut rng);

            let execute_result: Result<_, FrankenError> = execute_single_txn(&conn, &mut rng, kind);
            match execute_result {
                Ok((start_order, commit_order, read_set, write_set, delta_sum)) => {
                    if let Some(wait_ms) = last_wait_ms.take() {
                        retry_controller.observe(wait_ms, true);
                    }
                    retry_controller.clear_conflict(logical_txn_id);

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
                    if let Some(wait_ms) = last_wait_ms.take() {
                        retry_controller.observe(wait_ms, false);
                    }

                    result.retry_conflicts += 1;
                    rollback_best_effort(&conn);
                    retries += 1;
                    if retries > MAX_RETRIES_PER_TXN {
                        retry_controller.clear_conflict(logical_txn_id);
                        result.aborted += 1;
                        break;
                    }

                    let retry_sleep_ms = match retry_controller.decide_with_cx(
                        logical_txn_id,
                        BUSY_TIMEOUT_MS,
                        0,
                        None,
                        false,
                    ) {
                        RetryAction::RetryAfter { wait_ms } => {
                            let jitter_ceiling = wait_ms.min(RETRY_SLEEP_JITTER_MS);
                            let jitter = if jitter_ceiling == 0 {
                                0
                            } else {
                                rng.gen_range(0_u64..=jitter_ceiling)
                            };
                            last_wait_ms = Some(wait_ms);
                            wait_ms.saturating_add(jitter)
                        }
                        RetryAction::FailNow => {
                            last_wait_ms = Some(RETRY_CONTROLLER_FALLBACK_SLEEP_MS);
                            RETRY_CONTROLLER_FALLBACK_SLEEP_MS
                        }
                    };
                    thread::sleep(Duration::from_millis(retry_sleep_ms));
                }
                Err(err) => {
                    if let Some(wait_ms) = last_wait_ms.take() {
                        retry_controller.observe(wait_ms, false);
                    }
                    result.aborted += 1;
                    rollback_best_effort(&conn);
                    retry_controller.clear_conflict(logical_txn_id);
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

#[allow(clippy::type_complexity)]
fn execute_single_txn(
    conn: &fsqlite::Connection,
    rng: &mut StdRng,
    kind: TxnKind,
) -> Result<(u64, u64, BTreeSet<i64>, BTreeSet<i64>, i64), FrankenError> {
    conn.execute("BEGIN CONCURRENT;")?;
    let start_order = conn.current_concurrent_snapshot_seq().ok_or_else(|| {
        FrankenError::Internal(
            "missing concurrent snapshot sequence after BEGIN CONCURRENT".to_owned(),
        )
    })?;

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
            read_set.insert(from);

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
    let commit_order = conn.last_local_commit_seq().ok_or_else(|| {
        FrankenError::Internal("missing commit sequence after successful COMMIT".to_owned())
    })?;
    Ok((start_order, commit_order, read_set, write_set, delta_sum))
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

fn detect_cycle(txns: &[CommittedTxn]) -> Option<Vec<usize>> {
    let node_count = txns.len();
    if node_count <= 1 {
        return None;
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

    if visited == node_count {
        return None;
    }

    // Extract one concrete cycle from the residual subgraph (nodes with
    // positive indegree after Kahn elimination).
    let mut state = vec![0_u8; node_count]; // 0=unseen, 1=visiting, 2=done
    let mut stack = Vec::new();
    for node in 0..node_count {
        if indegree[node] == 0 || state[node] != 0 {
            continue;
        }
        if let Some(cycle) = dfs_cycle(node, &edges, &indegree, &mut state, &mut stack) {
            return Some(cycle);
        }
    }
    Some(Vec::new())
}

fn dfs_cycle(
    node: usize,
    edges: &[BTreeSet<usize>],
    indegree: &[usize],
    state: &mut [u8],
    stack: &mut Vec<usize>,
) -> Option<Vec<usize>> {
    state[node] = 1;
    stack.push(node);
    for &next in &edges[node] {
        if indegree[next] == 0 {
            continue;
        }
        if state[next] == 0 {
            if let Some(cycle) = dfs_cycle(next, edges, indegree, state, stack) {
                return Some(cycle);
            }
        } else if state[next] == 1 {
            let start = stack
                .iter()
                .position(|&value| value == next)
                .expect("cycle back-edge target should be in DFS stack");
            let mut cycle = stack[start..].to_vec();
            cycle.push(next);
            return Some(cycle);
        }
    }
    stack.pop();
    state[node] = 2;
    None
}

fn render_cycle_witness(txns: &[CommittedTxn], cycle: &[usize]) -> String {
    if cycle.is_empty() {
        return "unresolved-cycle-no-path".to_owned();
    }
    cycle
        .iter()
        .map(|&idx| {
            let txn = &txns[idx];
            format!(
                "#{idx}(start={},commit={},r={:?},w={:?})",
                txn.start_order, txn.commit_order, txn.read_set, txn.write_set
            )
        })
        .collect::<Vec<_>>()
        .join(" -> ")
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
