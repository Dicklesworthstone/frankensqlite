//! bd-2dgf5 A/B: aggregate over a rowid-equality predicate — seek vs full scan.
//!
//! `SELECT SUM(v) FROM t WHERE id = <int literal>` now seeks the single row by rowid
//! (O(log n)); the same query with `NOT INDEXED` declines the seek and full-scans (O(n)).
//! The differential oracle test `rowid_eq_aggregate_matches_sqlite` proves both return
//! identical values, so this isolates the access path.
//!
//! Substrate: ONE binary, seek and scan interleaved WITHIN each measured sample (seek then
//! scan back-to-back), so per-sample drift hits both arms equally. A paired NULL CONTROL
//! (seek vs seek) measures the harness floor. The bound literal VARIES every execution so
//! the retained autocommit count/sum cache (bd-czzlp) cannot serve the answer. Gate on the
//! MEDIAN of the per-sample ratio; report the null median beside it.

use std::hint::black_box;
use std::time::Instant;

use fsqlite_core::connection::Connection;

const ROWS: i64 = 20_000;
const EXECS_PER_SAMPLE: usize = 64;
const SAMPLES: usize = 60;

fn setup() -> Connection {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL);")
        .expect("create");
    conn.execute("BEGIN;").expect("begin");
    for i in 1..=ROWS {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {}, {}.5);",
            i % 100,
            i
        ))
        .expect("insert");
    }
    conn.execute("COMMIT;").expect("commit");
    conn
}

/// Run one arm over `EXECS_PER_SAMPLE` distinct literals and return elapsed nanoseconds.
/// `not_indexed` selects the scan arm; the literal cycles so no cache hit can serve it.
fn time_arm(conn: &Connection, not_indexed: bool, base: i64) -> u128 {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let id = 1 + ((base + j as i64) % ROWS);
        let sql = format!("SELECT SUM(v) FROM t{hint} WHERE id = {id}");
        let rows = conn.query(black_box(&sql)).expect("query");
        black_box(&rows);
    }
    start.elapsed().as_nanos()
}

fn median(mut v: Vec<u128>) -> u128 {
    v.sort_unstable();
    v[v.len() / 2]
}

fn main() {
    let conn = setup();

    // Warm both paths once (JIT-free, but primes caches/allocations symmetrically).
    black_box(time_arm(&conn, false, 0));
    black_box(time_arm(&conn, true, 0));

    let mut seek = Vec::with_capacity(SAMPLES);
    let mut scan = Vec::with_capacity(SAMPLES);
    let mut null_a = Vec::with_capacity(SAMPLES);
    let mut null_b = Vec::with_capacity(SAMPLES);

    for s in 0..SAMPLES {
        let base = (s as i64) * (EXECS_PER_SAMPLE as i64);
        // Interleaved within the sample: seek, then scan, back-to-back.
        seek.push(time_arm(&conn, false, base));
        scan.push(time_arm(&conn, true, base));
        // Null control: seek vs seek, same interleave shape.
        null_a.push(time_arm(&conn, false, base));
        null_b.push(time_arm(&conn, false, base));
    }

    let m_seek = median(seek);
    let m_scan = median(scan);
    let m_na = median(null_a);
    let m_nb = median(null_b);

    let us = |ns: u128| (ns as f64) / (EXECS_PER_SAMPLE as f64) / 1000.0;
    println!("rows={ROWS} execs_per_sample={EXECS_PER_SAMPLE} samples={SAMPLES}");
    println!("seek   median = {:.3} us/query", us(m_seek));
    println!("scan   median = {:.3} us/query", us(m_scan));
    println!(
        "speedup (scan/seek) = {:.3}x",
        (m_scan as f64) / (m_seek as f64)
    );
    println!(
        "NULL control (seek/seek) = {:.3}x  [{:.3} vs {:.3} us/query]",
        (m_nb as f64) / (m_na as f64),
        us(m_na),
        us(m_nb)
    );
}
