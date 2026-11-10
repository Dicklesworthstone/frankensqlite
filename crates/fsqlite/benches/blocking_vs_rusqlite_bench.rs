//! `fsqlite::blocking` against rusqlite on what a Rust application does: a
//! prepared rowid lookup, a prepared index lookup, prepared INSERTs in one
//! transaction, autocommit INSERTs, and ad hoc UPDATE text. Each workload is
//! written once and compiled against both APIs, so both engines run the same
//! code, in this process, on file-backed WAL databases of the same contents,
//! interleaved per round (the incumbent is live, never a remembered number).
//!
//! The first stdout line is the SHA-256 of the executing ELF. Each row reports
//! nanoseconds per operation (median over rounds) and the median per-round
//! ratio fsqlite/stock; the `A/A` row runs stock against itself to show the
//! noise floor of that ratio.
//!
//! Run:
//! `rch exec -- cargo bench --profile release-perf -p fsqlite --bench blocking_vs_rusqlite_bench`

#![allow(clippy::cast_precision_loss)]

use std::fmt::Write as _;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use sha2::{Digest, Sha256};

const ROWS: i64 = 100_000;
const ROUNDS: usize = 7;

mod stock {
    pub use rusqlite::*;
}

mod ours {
    pub use fsqlite::blocking::*;
}

/// Open `$path` with module `$m` and load the shared table.
macro_rules! open_loaded {
    ($m:ident, $path:expr) => {{
        let mut conn = $m::Connection::open($path).expect("open");
        conn.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
             CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);
             CREATE INDEX t_a ON t(a);
             CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
        )
        .expect("schema");
        let tx = conn.transaction().expect("begin");
        {
            let mut insert = tx
                .prepare("INSERT INTO t VALUES (?1, ?2, ?3)")
                .expect("prepare");
            for id in 1..=ROWS {
                insert
                    .execute($m::params![id, (id * 7_919) % ROWS, format!("row-{id}")])
                    .expect("load");
            }
        }
        tx.commit().expect("commit");
        conn
    }};
}

/// Run workload `$name` once with module `$m` on `$conn`; returns (ops, checksum).
macro_rules! workload {
    ($m:ident, $conn:expr, $name:expr, $round:expr) => {{
        let conn = $conn;
        let round = $round as i64;
        let key = |i: i64| (i * 104_729 + round * 31) % ROWS + 1;
        match $name {
            "point_rowid" => {
                let mut stmt = conn
                    .prepare("SELECT b FROM t WHERE id = ?1")
                    .expect("prepare");
                let mut sum = 0_usize;
                for i in 0..20_000 {
                    let b: String = stmt.query_row([key(i)], |row| row.get(0)).expect("row");
                    sum += b.len();
                }
                (20_000_u64, sum as u64)
            }
            "point_index" => {
                let mut stmt = conn
                    .prepare("SELECT id FROM t WHERE a = ?1")
                    .expect("prepare");
                let mut sum = 0_i64;
                for i in 0..20_000 {
                    let mut rows = stmt.query([key(i) - 1]).expect("query");
                    while let Some(row) = rows.next().expect("next") {
                        sum += row.get::<_, i64>(0).expect("id");
                    }
                }
                (20_000_u64, sum as u64)
            }
            "insert_txn" => {
                conn.execute("DELETE FROM u", []).expect("clear");
                conn.execute_batch("BEGIN").expect("begin");
                {
                    let mut stmt = conn
                        .prepare("INSERT INTO u(a, b) VALUES (?1, ?2)")
                        .expect("prepare");
                    for i in 0..20_000_i64 {
                        stmt.execute($m::params![i, "payload"]).expect("insert");
                    }
                }
                conn.execute_batch("COMMIT").expect("commit");
                (20_000_u64, conn.last_insert_rowid() as u64)
            }
            "insert_autocommit" => {
                let mut stmt = conn
                    .prepare("INSERT INTO u(a, b) VALUES (?1, ?2)")
                    .expect("prepare");
                for i in 0..1_000_i64 {
                    stmt.execute($m::params![i, "one"]).expect("insert");
                }
                (1_000_u64, 1)
            }
            "adhoc_update" => {
                let mut changed = 0_usize;
                for i in 0..2_000 {
                    let sql = format!("UPDATE t SET b = 'u{i}-{round}' WHERE id = {}", key(i));
                    changed += conn.execute(&sql, []).expect("update");
                }
                (2_000_u64, changed as u64)
            }
            other => panic!("unknown workload {other}"),
        }
    }};
}

const WORKLOADS: &[&str] = &[
    "point_rowid",
    "point_index",
    "insert_txn",
    "insert_autocommit",
    "adhoc_update",
];

fn elf_sha256() -> String {
    let exe = std::env::current_exe().expect("current_exe");
    let bytes = std::fs::read(exe).expect("read own ELF");
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn time<T>(f: impl FnOnce() -> (u64, T)) -> (f64, T) {
    let start = Instant::now();
    let (ops, check) = f();
    (start.elapsed().as_nanos() as f64 / ops as f64, check)
}

fn db(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

fn main() {
    println!("elf_sha256 {}", elf_sha256());
    let dir = tempfile::tempdir().expect("tempdir");
    let ours_conn = open_loaded!(ours, db(dir.path(), "fsqlite.db"));
    let stock_conn = open_loaded!(stock, db(dir.path(), "stock.db"));
    let stock_twin = open_loaded!(stock, db(dir.path(), "stock_twin.db"));
    println!(
        "{:18} {:>12} {:>12} {:>9} {:>9}",
        "workload", "fsqlite ns", "stock ns", "F/C", "A/A"
    );
    for name in WORKLOADS {
        let (mut f_ns, mut c_ns, mut ratios, mut nulls) = (vec![], vec![], vec![], vec![]);
        for round in 0..ROUNDS {
            // Alternate which engine goes first.
            let (f, c, twin) = if round % 2 == 0 {
                let f = time(|| workload!(ours, &ours_conn, *name, round));
                let c = time(|| workload!(stock, &stock_conn, *name, round));
                let twin = time(|| workload!(stock, &stock_twin, *name, round));
                (f, c, twin)
            } else {
                let twin = time(|| workload!(stock, &stock_twin, *name, round));
                let c = time(|| workload!(stock, &stock_conn, *name, round));
                let f = time(|| workload!(ours, &ours_conn, *name, round));
                (f, c, twin)
            };
            assert_eq!(f.1, c.1, "{name} round {round}: fsqlite and stock disagree");
            black_box((f.1, twin.1));
            f_ns.push(f.0);
            c_ns.push(c.0);
            ratios.push(f.0 / c.0);
            nulls.push(twin.0 / c.0);
        }
        println!(
            "{:18} {:>12.0} {:>12.0} {:>9.2} {:>9.2}",
            name,
            median(&mut f_ns),
            median(&mut c_ns),
            median(&mut ratios),
            median(&mut nulls)
        );
    }
}
