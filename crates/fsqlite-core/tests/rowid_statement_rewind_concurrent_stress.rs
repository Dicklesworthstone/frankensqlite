#![recursion_limit = "512"]

//! Review of fc7e91333 (bd-8a8pr): a statement rolled back inside a
//! transaction gives its implicit rowids back to the shared allocator, guarded
//! so another writer's rowid is never reissued. This stress runs several
//! threads, each with its own file-backed connection, through `BEGIN
//! CONCURRENT` transactions of multi-row INSERTs with implicit rowids into a
//! rowid table and an AUTOINCREMENT table. A third of the statements fail
//! part-way on a UNIQUE tag, so their rows roll back while peers keep
//! allocating; some transactions roll back whole. Refused commits retry.
//!
//! Every committed tag must be present exactly once, nothing else may be, the
//! AUTOINCREMENT high-water must cover every id, and stock `integrity_check`
//! must pass on the result.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_core::connection::Connection;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    RuntimeBuilder::current_thread()
        .build()
        .expect("build runtime")
        .block_on(future)
}

const MAX_ATTEMPTS: usize = 60;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

/// xorshift64*: deterministic per (seed, thread).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One INSERT statement: its rows' tags, and whether its last row repeats a
/// tag that is already committed (so the statement fails part-way).
#[derive(Clone)]
struct Stmt {
    table: &'static str,
    tags: Vec<String>,
    collides: bool,
}

impl Stmt {
    fn sql(&self, thread: usize) -> String {
        let rows = self
            .tags
            .iter()
            .map(|tag| format!("('{tag}', {thread})"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("INSERT INTO {}(tag, owner) VALUES {rows}", self.table)
    }
}

struct Txn {
    stmts: Vec<Stmt>,
    rollback: bool,
}

fn gen_txn(rng: &mut Rng, thread: usize, next_tag: &mut u64) -> Txn {
    let count = 1 + rng.below(3);
    let mut stmts = Vec::new();
    for _ in 0..count {
        let table = if rng.below(2) == 0 { "t" } else { "a" };
        let rows = 1 + rng.below(4);
        let mut tags = Vec::new();
        for _ in 0..rows {
            tags.push(format!("{table}-{thread}-{next_tag}"));
            *next_tag += 1;
        }
        let collides = rng.below(3) == 0;
        if collides {
            // The seed row's tag is always committed, so this row always
            // fails the UNIQUE check after the earlier rows were inserted.
            tags.push(format!("{table}-seed"));
        }
        stmts.push(Stmt {
            table,
            tags,
            collides,
        });
    }
    Txn {
        stmts,
        rollback: rng.below(8) == 0,
    }
}

struct ThreadResult {
    committed: BTreeSet<String>,
    commits: usize,
    faults: Vec<String>,
}

/// Run one attempt; `Ok(true)` committed, `Ok(false)` rolled back on purpose.
async fn run_txn(conn: &Connection, txn: &Txn, thread: usize) -> Result<bool, String> {
    conn.execute("BEGIN CONCURRENT")
        .await
        .map_err(|err| err.to_string())?;
    for stmt in &txn.stmts {
        match conn.execute(&stmt.sql(thread)).await {
            Ok(_) if !stmt.collides => {}
            Err(err) if stmt.collides && err.to_string().contains("UNIQUE") => {}
            Ok(_) => {
                let _ = conn.execute("ROLLBACK").await;
                return Err(format!("FAULT: colliding statement succeeded: {}", stmt.sql(thread)));
            }
            Err(err) => {
                let _ = conn.execute("ROLLBACK").await;
                return Err(err.to_string());
            }
        }
    }
    if txn.rollback {
        conn.execute("ROLLBACK").await.map_err(|err| err.to_string())?;
        return Ok(false);
    }
    match conn.execute("COMMIT").await {
        Ok(_) => Ok(true),
        Err(err) => {
            let _ = conn.execute("ROLLBACK").await;
            Err(err.to_string())
        }
    }
}

fn worker(path: String, seed: u64, thread: usize, txns: usize) -> ThreadResult {
    block_on(async move {
        let conn = Connection::open(&path).await.expect("worker open");
        conn.execute("PRAGMA busy_timeout=2000").await.expect("busy_timeout");
        let mut rng = Rng::new(seed ^ ((thread as u64 + 1) << 32));
        let mut next_tag = 0_u64;
        let mut result = ThreadResult {
            committed: BTreeSet::new(),
            commits: 0,
            faults: Vec::new(),
        };
        for _ in 0..txns {
            let txn = gen_txn(&mut rng, thread, &mut next_tag);
            for _ in 0..MAX_ATTEMPTS {
                match run_txn(&conn, &txn, thread).await {
                    Ok(true) => {
                        result.commits += 1;
                        for stmt in txn.stmts.iter().filter(|stmt| !stmt.collides) {
                            result.committed.extend(stmt.tags.iter().cloned());
                        }
                        break;
                    }
                    Ok(false) => break,
                    Err(message) => {
                        let lower = message.to_ascii_lowercase();
                        if message.starts_with("FAULT")
                            || lower.contains("internal")
                            || lower.contains("corrupt")
                            || lower.contains("malformed")
                        {
                            result.faults.push(message);
                            break;
                        }
                    }
                }
            }
        }
        conn.close().await.expect("worker close");
        result
    })
}

fn tags_of(conn: &rusqlite::Connection, table: &str) -> (Vec<String>, i64, i64) {
    let mut stmt = conn
        .prepare(&format!("SELECT tag FROM {table} WHERE tag NOT LIKE '%-seed' ORDER BY id"))
        .expect("stock prepare");
    let tags = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .expect("stock tags")
        .map(|row| row.expect("tag"))
        .collect();
    let (count, distinct_ids): (i64, i64) = conn
        .query_row(&format!("SELECT count(*), count(DISTINCT id) FROM {table}"), [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("stock counts");
    (tags, count, distinct_ids)
}

fn run_seed(seed: u64, threads: usize, txns: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rewind.db").to_string_lossy().into_owned();
    {
        let path = path.clone();
        block_on(async move {
            let conn = Connection::open(&path).await.expect("seed open");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT UNIQUE, owner INTEGER);\
                 CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT UNIQUE, owner INTEGER);\
                 INSERT INTO t(tag, owner) VALUES ('t-seed', -1);\
                 INSERT INTO a(tag, owner) VALUES ('a-seed', -1);",
            )
            .await
            .expect("schema");
            conn.close().await.expect("seed close");
        });
    }
    let results = Arc::new(Mutex::new(Vec::new()));
    let handles: Vec<_> = (0..threads)
        .map(|thread| {
            let path = path.clone();
            let results = Arc::clone(&results);
            std::thread::Builder::new()
                .stack_size(64 * 1024 * 1024)
                .spawn(move || {
                    let result = worker(path, seed, thread, txns);
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
    let mut expected = BTreeSet::new();
    let mut commits = 0;
    let mut faults = Vec::new();
    for result in results {
        commits += result.commits;
        expected.extend(result.committed);
        faults.extend(result.faults);
    }
    eprintln!("seed {seed}: threads={threads} commits={commits} rows={}", expected.len());
    assert!(faults.is_empty(), "seed {seed}: engine faults: {faults:?}");
    assert!(commits > 0, "seed {seed}: nothing committed");

    let stock = rusqlite::Connection::open(&path).expect("stock open");
    let mut problems = Vec::new();
    let mut found = BTreeSet::new();
    for table in ["t", "a"] {
        let (tags, count, distinct_ids) = tags_of(&stock, table);
        if count != distinct_ids {
            problems.push(format!("{table}: {count} rows but {distinct_ids} distinct ids"));
        }
        for tag in tags {
            if !found.insert(tag.clone()) {
                problems.push(format!("{table}: tag {tag} stored twice"));
            }
        }
    }
    for tag in expected.difference(&found) {
        problems.push(format!("committed tag {tag} missing"));
    }
    for tag in found.difference(&expected) {
        problems.push(format!("tag {tag} present but never committed"));
    }
    let (max_id, seq): (i64, i64) = stock
        .query_row(
            "SELECT (SELECT max(id) FROM a), (SELECT seq FROM sqlite_sequence WHERE name = 'a')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("stock sequence");
    if seq < max_id {
        problems.push(format!("sqlite_sequence {seq} below max(id) {max_id}"));
    }
    let verdict: Vec<String> = stock
        .prepare("PRAGMA integrity_check")
        .expect("stock prepare")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("stock integrity")
        .map(|row| row.expect("row"))
        .collect();
    if verdict != vec!["ok".to_owned()] {
        problems.push(format!("stock integrity_check: {verdict:?}"));
    }
    problems.truncate(40);
    assert!(problems.is_empty(), "seed {seed}: {problems:#?}");
}

#[test]
fn statement_rollback_rowid_rewind_under_concurrent_writers() {
    let seeds = env_usize("REWIND_STRESS_SEEDS", 2);
    let threads = env_usize("REWIND_STRESS_THREADS", 4);
    let txns = env_usize("REWIND_STRESS_TXNS", 60);
    for seed in 0..seeds {
        run_seed(0xBD8A_8E00 + seed as u64, threads, txns);
    }
}
