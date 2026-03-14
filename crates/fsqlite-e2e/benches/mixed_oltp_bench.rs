//! Benchmark: mixed OLTP workload (80% read / 20% write).
//!
//! Bead: bd-1fez
//!
//! Simulates a realistic OLTP workload: predominantly reads with occasional
//! writes.  Uses a deterministic xorshift64 PRNG to select operations:
//!
//! - 80% SELECT (point lookups, range scans, aggregates)
//! - 15% INSERT
//! - 3% UPDATE
//! - 2% DELETE
//!
//! The benchmark measures throughput (ops/sec) over a fixed number of
//! operations on both backends.
//!
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite::SqliteValue;

const SEED_ROWS: usize = 500;
const OPS_PER_ITERATION: u64 = 2000;

// ─── Deterministic PRNG ─────────────────────────────────────────────────

struct Rng64 {
    state: u64,
}

impl Rng64 {
    const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[allow(clippy::cast_possible_truncation)]
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % (bound as u64)) as usize
    }
}

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .ok();
}

fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        let _ = conn.execute(pragma);
    }
}

const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);";

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── Setup helpers ──────────────────────────────────────────────────────

fn setup_csqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    apply_pragmas_csqlite(&conn);
    conn.execute_batch(CREATE_TABLE).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
            .unwrap();
        #[allow(clippy::cast_possible_wrap)]
        for i in 1..=SEED_ROWS as i64 {
            stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_fsqlite() -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    apply_pragmas_fsqlite(&conn);
    conn.execute(CREATE_TABLE).unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=SEED_ROWS {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', {})",
            i * 7,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn
}

// ─── C SQLite mixed OLTP ────────────────────────────────────────────────

#[allow(clippy::cast_possible_wrap)]
fn bench_mixed_oltp_csqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_oltp_80r_20w");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.throughput(Throughput::Elements(OPS_PER_ITERATION));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            setup_csqlite,
            |conn| {
                let mut rng = Rng64::new(42);
                let mut next_id = SEED_ROWS as i64 + 1;

                let mut select_pt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
                let mut select_range = conn
                    .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
                    .unwrap();
                let mut select_agg = conn
                    .prepare("SELECT COUNT(*), SUM(score) FROM bench")
                    .unwrap();
                let mut insert = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                    .unwrap();
                let mut update = conn
                    .prepare("UPDATE bench SET score = ?2 WHERE id = ?1")
                    .unwrap();
                let mut delete = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();

                for _ in 0..OPS_PER_ITERATION {
                    let roll = rng.next_usize(100);
                    if roll < 40 {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let _ = select_pt.query_row(rusqlite::params![id], |_| Ok(()));
                    } else if roll < 60 {
                        let start = (rng.next_usize(SEED_ROWS - 50) + 1) as i64;
                        let _: i64 = select_range
                            .query_row(rusqlite::params![start, start + 50], |r| r.get(0))
                            .unwrap();
                    } else if roll < 80 {
                        let _: (i64, i64) = select_agg
                            .query_row([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
                            .unwrap();
                    } else if roll < 95 {
                        let _ = insert.execute(rusqlite::params![next_id]);
                        next_id += 1;
                    } else if roll < 98 {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let _ = update.execute(rusqlite::params![id, id * 99]);
                    } else {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let _ = delete.execute(rusqlite::params![id]);
                    }
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── FrankenSQLite mixed OLTP ───────────────────────────────────────────

#[allow(clippy::cast_possible_wrap)]
fn bench_mixed_oltp_fsqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_oltp_80r_20w");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.throughput(Throughput::Elements(OPS_PER_ITERATION));

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            setup_fsqlite,
            |conn| {
                let mut rng = Rng64::new(42);
                let mut next_id = SEED_ROWS as i64 + 1;
                let select_pt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
                let select_range = conn
                    .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
                    .unwrap();
                let select_agg = conn
                    .prepare("SELECT COUNT(*), SUM(score) FROM bench")
                    .unwrap();
                let insert = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                    .unwrap();
                let update = conn
                    .prepare("UPDATE bench SET score = ?2 WHERE id = ?1")
                    .unwrap();
                let delete = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();

                for _ in 0..OPS_PER_ITERATION {
                    let roll = rng.next_usize(100);
                    if roll < 40 {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let _ = select_pt.query_row_with_params(&[SqliteValue::Integer(id)]);
                    } else if roll < 60 {
                        let start = (rng.next_usize(SEED_ROWS - 50) + 1) as i64;
                        let _ = select_range.query_row_with_params(&[
                            SqliteValue::Integer(start),
                            SqliteValue::Integer(start + 50),
                        ]);
                    } else if roll < 80 {
                        let _ = select_agg.query_row();
                    } else if roll < 95 {
                        let _ = insert.execute_with_params(&[SqliteValue::Integer(next_id)]);
                        next_id += 1;
                    } else if roll < 98 {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let _ = update.execute_with_params(&[
                            SqliteValue::Integer(id),
                            SqliteValue::Integer(id * 99),
                        ]);
                    } else {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let _ = delete.execute_with_params(&[SqliteValue::Integer(id)]);
                    }
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    name = mixed_oltp;
    config = criterion_config();
    targets = bench_mixed_oltp_csqlite, bench_mixed_oltp_fsqlite
);
criterion_main!(mixed_oltp);
