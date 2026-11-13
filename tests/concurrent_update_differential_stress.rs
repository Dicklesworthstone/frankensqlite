#![recursion_limit = "512"]

//! Concurrent-writer differential stress for the UPDATE rewrite paths.
//!
//! Several threads, each with its own file-backed connection, run
//! `BEGIN CONCURRENT` transactions of random row-local writes and retry them
//! when the commit is refused. This drives the in-place UPDATE overwrite
//! (bd-9ag5r), the delete+insert fallback for size-changing rows, rows that
//! cross the overflow boundary, the cell-slot cache, and the index entries of
//! an indexed column, all under page-level MVCC conflicts.
//!
//! Every operation is commutative, so the expected final table does not depend
//! on the order in which the transactions committed. Seed rows only get
//! `k = k + d, ver = ver + 1`; pad-writing ops also bump `pver` and set a `pad`
//! whose length is a function of the new `pver` (a counter only they touch, so
//! the pad does not depend on how they interleave with the integer-only bumps).
//! Inserted rows use ids private to their thread, and only that thread
//! deletes them. The expected state is built from the transactions whose COMMIT
//! returned Ok. The test then requires:
//! - the table equals the model;
//! - fsqlite's `integrity_check` is ok;
//! - stock SQLite's `integrity_check` (bundled rusqlite) is ok on the file;
//! - no refusal looks like an engine fault (internal, corrupt or malformed).
//!
//! Each seed runs once without and once with an index on `k`.
//! `CONCURRENT_STRESS_SEEDS` (default 2), `CONCURRENT_STRESS_THREADS`
//! (default 4) and `CONCURRENT_STRESS_TXNS` (default 60 per thread) scale it up.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// Run `future` to completion on a fresh current-thread runtime and return
/// its output (`run_test` discards it).
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    RuntimeBuilder::current_thread()
        .build()
        .expect("build runtime")
        .block_on(future)
}

const SEED_ROWS: i64 = 240;
const PAD_MODULUS: i64 = 9_001;
const PAD_STRIDE: i64 = 1_337;
const MAX_ATTEMPTS: usize = 40;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

/// xorshift64*: deterministic per (seed, thread) without a dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The pad a seed row carries after `pver` committed pad writes. Lengths sweep
/// 0..9000 bytes, so rewrites grow, shrink, keep their size, and move across
/// the 4 KiB overflow threshold in both directions.
fn pad_len(pver: i64) -> i64 {
    (pver * PAD_STRIDE) % PAD_MODULUS
}

fn pad_text(len: i64) -> String {
    let len = usize::try_from(len).expect("pad length");
    "abcdefghijklmnopqrstuvwxyz0123456789"
        .chars()
        .cycle()
        .take(len)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    k: i64,
    ver: i64,
    pver: i64,
    pad_len: i64,
}

/// One row-local write. Seed-row ops are commutative; private rows belong to
/// one thread.
#[derive(Clone, Debug)]
enum Op {
    /// Same-size-ish rewrite: integer columns only (varint sizes rarely change).
    Bump { id: i64, d: i64 },
    /// Size-changing rewrite through the pad, which follows the new `pver`.
    BumpPad { id: i64, d: i64 },
    /// Multi-row rewrite over a seed-id range.
    BumpRange { lo: i64, hi: i64, d: i64 },
    /// Insert a private row.
    Insert { id: i64, k: i64, pad: i64 },
    /// Delete a private row (no-op when absent).
    Delete { id: i64 },
}

impl Op {
    fn sql(&self) -> String {
        match self {
            Self::Bump { id, d } => {
                format!("UPDATE t SET k = k + {d}, ver = ver + 1 WHERE id = {id}")
            }
            Self::BumpPad { id, d } => format!(
                "UPDATE t SET k = k + {d}, ver = ver + 1, pver = pver + 1, \
                 pad = substr(?1, 1, ((pver + 1) * {PAD_STRIDE}) % {PAD_MODULUS}) WHERE id = {id}"
            ),
            Self::BumpRange { lo, hi, d } => format!(
                "UPDATE t SET k = k + {d}, ver = ver + 1, pver = pver + 1, \
                 pad = substr(?1, 1, ((pver + 1) * {PAD_STRIDE}) % {PAD_MODULUS}) \
                 WHERE id BETWEEN {lo} AND {hi}"
            ),
            Self::Insert { id, k, pad } => format!(
                "INSERT INTO t(id, k, ver, pver, pad) VALUES ({id}, {k}, 0, 0, substr(?1, 1, {pad}))"
            ),
            Self::Delete { id } => format!("DELETE FROM t WHERE id = {id}"),
        }
    }

    fn apply(&self, model: &mut BTreeMap<i64, Row>) {
        let bump = |row: &mut Row, d: i64, with_pad: bool| {
            row.k += d;
            row.ver += 1;
            if with_pad {
                row.pver += 1;
                row.pad_len = pad_len(row.pver);
            }
        };
        match self {
            Self::Bump { id, d } => {
                if let Some(row) = model.get_mut(id) {
                    bump(row, *d, false);
                }
            }
            Self::BumpPad { id, d } => {
                if let Some(row) = model.get_mut(id) {
                    bump(row, *d, true);
                }
            }
            Self::BumpRange { lo, hi, d } => {
                for (_, row) in model.range_mut(*lo..=*hi) {
                    bump(row, *d, true);
                }
            }
            Self::Insert { id, k, pad } => {
                model.insert(
                    *id,
                    Row {
                        k: *k,
                        ver: 0,
                        pver: 0,
                        pad_len: *pad,
                    },
                );
            }
            Self::Delete { id } => {
                model.remove(id);
            }
        }
    }
}

/// A thread's committed seed-row ops plus its private-row model.
struct ThreadResult {
    committed_seed_ops: Vec<Op>,
    private_rows: BTreeMap<i64, Row>,
    commits: usize,
    refusals: usize,
    dropped: usize,
    errors: Vec<String>,
}

fn gen_txn(rng: &mut Rng, thread: usize, private: &BTreeMap<i64, Row>, next_private: &mut i64) -> Vec<Op> {
    let n_ops = 1 + rng.below(4);
    let mut ops = Vec::new();
    let mut pending_private = private.keys().copied().collect::<Vec<_>>();
    for _ in 0..n_ops {
        let d = i64::try_from(rng.below(5)).expect("d") + 1;
        let choice = rng.below(100);
        let op = if choice < 35 {
            Op::Bump {
                id: 1 + i64::try_from(rng.below(SEED_ROWS as u64)).expect("id"),
                d,
            }
        } else if choice < 65 {
            Op::BumpPad {
                id: 1 + i64::try_from(rng.below(SEED_ROWS as u64)).expect("id"),
                d,
            }
        } else if choice < 75 {
            let lo = 1 + i64::try_from(rng.below(SEED_ROWS as u64)).expect("lo");
            let hi = (lo + i64::try_from(rng.below(12)).expect("span")).min(SEED_ROWS);
            Op::BumpRange { lo, hi, d }
        } else if choice < 90 || pending_private.is_empty() {
            let id = 1_000_000 * (i64::try_from(thread).expect("thread") + 1) + *next_private;
            *next_private += 1;
            pending_private.push(id);
            Op::Insert {
                id,
                k: i64::try_from(rng.below(1_000)).expect("k"),
                pad: i64::try_from(rng.below(6_000)).expect("pad"),
            }
        } else {
            let pick = usize::try_from(rng.below(pending_private.len() as u64)).expect("pick");
            Op::Delete {
                id: pending_private.swap_remove(pick),
            }
        };
        ops.push(op);
    }
    ops
}

fn is_seed_op(op: &Op) -> bool {
    matches!(op, Op::Bump { .. } | Op::BumpPad { .. } | Op::BumpRange { .. })
}

async fn run_txn(conn: &Connection, ops: &[Op], big: &str) -> Result<(), String> {
    conn.execute("BEGIN CONCURRENT")
        .await
        .map_err(|err| err.to_string())?;
    for op in ops {
        let sql = op.sql();
        let result = if sql.contains("?1") {
            conn.execute_with_params(&sql, &[SqliteValue::from(big)]).await
        } else {
            conn.execute(&sql).await
        };
        if let Err(err) = result {
            let _ = conn.execute("ROLLBACK").await;
            return Err(err.to_string());
        }
    }
    match conn.execute("COMMIT").await {
        Ok(_) => Ok(()),
        Err(err) => {
            let _ = conn.execute("ROLLBACK").await;
            Err(err.to_string())
        }
    }
}

fn worker(path: String, seed: u64, thread: usize, txns: usize, big: Arc<String>) -> ThreadResult {
    block_on(async move {
        let conn = Connection::open(&path).await.expect("worker open");
        conn.execute("PRAGMA busy_timeout=2000").await.expect("busy_timeout");
        let mut rng = Rng::new(seed ^ ((thread as u64 + 1) << 32));
        let mut result = ThreadResult {
            committed_seed_ops: Vec::new(),
            private_rows: BTreeMap::new(),
            commits: 0,
            refusals: 0,
            dropped: 0,
            errors: Vec::new(),
        };
        let mut next_private = 0_i64;
        for _ in 0..txns {
            let ops = gen_txn(&mut rng, thread, &result.private_rows, &mut next_private);
            let mut committed = false;
            for _ in 0..MAX_ATTEMPTS {
                match run_txn(&conn, &ops, &big).await {
                    Ok(()) => {
                        committed = true;
                        break;
                    }
                    Err(message) => {
                        result.refusals += 1;
                        result.errors.push(message);
                    }
                }
            }
            if committed {
                result.commits += 1;
                for op in &ops {
                    if is_seed_op(op) {
                        result.committed_seed_ops.push(op.clone());
                    } else {
                        op.apply(&mut result.private_rows);
                    }
                }
            } else {
                result.dropped += 1;
            }
        }
        conn.close().await.expect("worker close");
        result
    })
}

fn seed_database(path: &str, big: &str, with_index: bool) -> BTreeMap<i64, Row> {
    let path = path.to_owned();
    let big = big.to_owned();
    block_on(async move {
        let conn = Connection::open(&path).await.expect("seed open");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, ver INTEGER, pver INTEGER, pad TEXT)")
            .await
            .expect("create");
        if with_index {
            conn.execute("CREATE INDEX t_k ON t(k)").await.expect("index");
        }
        let mut model = BTreeMap::new();
        conn.execute("BEGIN").await.expect("begin seed");
        for id in 1..=SEED_ROWS {
            let pver = id % 7;
            let row = Row {
                k: id * 10,
                ver: id % 5,
                pver,
                pad_len: pad_len(pver),
            };
            conn.execute_with_params(
                &format!(
                    "INSERT INTO t(id, k, ver, pver, pad) VALUES ({id}, {}, {}, {pver}, substr(?1, 1, {}))",
                    row.k, row.ver, row.pad_len
                ),
                &[SqliteValue::from(big.as_str())],
            )
            .await
            .expect("seed insert");
            model.insert(id, row);
        }
        conn.execute("COMMIT").await.expect("commit seed");
        conn.close().await.expect("seed close");
        model
    })
}

fn read_back(path: &str) -> (BTreeMap<i64, Row>, Vec<String>) {
    let path = path.to_owned();
    block_on(async move {
        let conn = Connection::open(&path).await.expect("verify open");
        let rows = conn
            .query("SELECT id, k, ver, pver, length(pad), pad FROM t ORDER BY id")
            .await
            .expect("read back");
        let mut table = BTreeMap::new();
        let mut pad_mismatches = Vec::new();
        for row in &rows {
            let values = row.values();
            let int = |idx: usize| match &values[idx] {
                SqliteValue::Integer(v) => *v,
                other => panic!("column {idx} not an integer: {other:?}"),
            };
            let id = int(0);
            let len = int(4);
            let pad = match &values[5] {
                SqliteValue::Text(text) => text.to_string(),
                other => panic!("pad not text: {other:?}"),
            };
            if pad != pad_text(len) {
                pad_mismatches.push(format!("id {id}: pad content differs (len {len})"));
            }
            table.insert(
                id,
                Row {
                    k: int(1),
                    ver: int(2),
                    pver: int(3),
                    pad_len: len,
                },
            );
        }
        let integrity = conn
            .query("PRAGMA integrity_check")
            .await
            .expect("fsqlite integrity_check");
        let verdict: Vec<String> = integrity
            .iter()
            .map(|row| format!("{:?}", row.values()))
            .collect();
        if verdict.len() != 1 || !verdict[0].contains("ok") {
            pad_mismatches.push(format!("fsqlite integrity_check: {verdict:?}"));
        }
        conn.close().await.expect("verify close");
        (table, pad_mismatches)
    })
}

fn run_seed(seed: u64, threads: usize, txns: usize, with_index: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stress.db").to_string_lossy().into_owned();
    let big = Arc::new(pad_text(PAD_MODULUS));
    let mut model = seed_database(&path, &big, with_index);

    let results = Arc::new(Mutex::new(Vec::new()));
    let handles: Vec<_> = (0..threads)
        .map(|thread| {
            let path = path.clone();
            let big = Arc::clone(&big);
            let results = Arc::clone(&results);
            std::thread::Builder::new()
                .stack_size(64 * 1024 * 1024)
                .spawn(move || {
                    let result = worker(path, seed, thread, txns, big);
                    results.lock().expect("results").push(result);
                })
                .expect("spawn worker")
        })
        .collect();
    for handle in handles {
        handle.join().expect("worker panicked");
    }

    let results = Arc::try_unwrap(results)
        .ok()
        .expect("results still shared")
        .into_inner()
        .expect("results lock");
    let (mut commits, mut refusals, mut dropped) = (0, 0, 0);
    let mut faults = Vec::new();
    let mut error_kinds = BTreeMap::<String, usize>::new();
    for result in &results {
        commits += result.commits;
        refusals += result.refusals;
        dropped += result.dropped;
        for op in &result.committed_seed_ops {
            op.apply(&mut model);
        }
        for (id, row) in &result.private_rows {
            model.insert(*id, row.clone());
        }
        for message in &result.errors {
            let lower = message.to_ascii_lowercase();
            if lower.contains("internal") || lower.contains("corrupt") || lower.contains("malformed") {
                faults.push(message.clone());
            }
            // Group "snapshot conflict on pages: N" by kind, not page number.
            let kind = message.split(" on pages").next().unwrap_or(message).to_owned();
            *error_kinds.entry(kind).or_default() += 1;
        }
    }
    eprintln!(
        "seed {seed} index={with_index}: threads={threads} commits={commits} refusals={refusals} dropped={dropped} kinds={error_kinds:?}"
    );
    assert!(faults.is_empty(), "seed {seed}: engine-fault refusals: {faults:?}");
    assert!(commits > 0, "seed {seed}: nothing committed");

    let (table, mut problems) = read_back(&path);
    for (id, expected) in &model {
        match table.get(id) {
            Some(actual) if actual == expected => {}
            Some(actual) => problems.push(format!("id {id}: expected {expected:?}, got {actual:?}")),
            None => problems.push(format!("id {id}: missing (expected {expected:?})")),
        }
    }
    for id in table.keys() {
        if !model.contains_key(id) {
            problems.push(format!("id {id}: present but not expected"));
        }
    }

    let stock = rusqlite::Connection::open(&path).expect("stock open");
    let stock_verdict: Vec<String> = stock
        .prepare("PRAGMA integrity_check")
        .expect("stock prepare")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("stock integrity")
        .map(|row| row.expect("stock row"))
        .collect();
    if stock_verdict != vec!["ok".to_owned()] {
        problems.push(format!("stock integrity_check: {stock_verdict:?}"));
    }
    if with_index {
        let stock_index_count: i64 = stock
            .query_row("SELECT count(*) FROM t INDEXED BY t_k WHERE k IS NOT NULL", [], |row| {
                row.get(0)
            })
            .expect("stock index count");
        if usize::try_from(stock_index_count).expect("count") != model.len() {
            problems.push(format!(
                "index t_k holds {stock_index_count} rows, model has {}",
                model.len()
            ));
        }
    }

    problems.truncate(40);
    assert!(problems.is_empty(), "seed {seed}: {problems:#?}");
}

#[test]
fn concurrent_row_local_updates_match_the_commit_model() {
    let seeds = env_usize("CONCURRENT_STRESS_SEEDS", 2);
    let threads = env_usize("CONCURRENT_STRESS_THREADS", 4);
    let txns = env_usize("CONCURRENT_STRESS_TXNS", 60);
    for seed in 0..seeds {
        // Without the index, writers to different rows of one leaf only meet
        // on that leaf, so in-place overwrites race each other directly; with
        // it, every `k` change also writes the shared index pages.
        run_seed(0xC0FFEE + seed as u64, threads, txns, false);
        run_seed(0xC0FFEE + seed as u64, threads, txns, true);
    }
}
