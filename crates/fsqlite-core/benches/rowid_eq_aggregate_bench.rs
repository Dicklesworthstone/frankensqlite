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
        // k == id (unique, INTEGER affinity) so an IN-list of a few values is selective and
        // has a secondary index to seek; the id-based equality/range arms are unaffected.
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {i}.5);"))
            .expect("insert");
    }
    conn.execute("COMMIT;").expect("commit");
    conn.execute("CREATE INDEX idx_t_k ON t(k);")
        .expect("create index");
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

/// Range arm: `SUM(v) WHERE id <= <upper>` over a selective upper bound. The bounded scan
/// visits `[1, upper]` and stops; `NOT INDEXED` full-scans all `ROWS`. Upper varies per exec.
fn time_range_arm(conn: &Connection, not_indexed: bool, base: i64) -> u128 {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        // Keep the range selective (~100 rows) so the bounded scan's early-exit dominates.
        let upper = 50 + ((base + j as i64) % 100);
        let sql = format!("SELECT SUM(v) FROM t{hint} WHERE id <= {upper}");
        let rows = conn.query(black_box(&sql)).expect("query");
        black_box(&rows);
    }
    start.elapsed().as_nanos()
}

/// IN-list arm: `SUM(v) WHERE k IN (a,b,c)` on the INTEGER index `idx_t_k`. The seek visits
/// three duplicate runs of one row each; `NOT INDEXED` full-scans all `ROWS`. Values vary.
fn time_in_arm(conn: &Connection, not_indexed: bool, base: i64) -> u128 {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 1) % ROWS);
        let c = 1 + ((base + j as i64 + 2) % ROWS);
        let sql = format!("SELECT SUM(v) FROM t{hint} WHERE k IN ({a}, {b}, {c})");
        let rows = conn.query(black_box(&sql)).expect("query");
        black_box(&rows);
    }
    start.elapsed().as_nanos()
}

/// Non-aggregate IN-list arm: `SELECT id, v WHERE k IN (a,b,c)`, per-value index seek + row
/// projection vs `NOT INDEXED` full-scan. Values vary. Distinct from the aggregate arm: this
/// exercises the `codegen_select_index_in_scan` ResultRow path, not accumulate.
fn time_nonagg_in_arm(conn: &Connection, not_indexed: bool, base: i64) -> u128 {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 1) % ROWS);
        let c = 1 + ((base + j as i64 + 2) % ROWS);
        let sql = format!("SELECT id, v FROM t{hint} WHERE k IN ({a}, {b}, {c})");
        let rows = conn.query(black_box(&sql)).expect("query");
        black_box(&rows);
    }
    start.elapsed().as_nanos()
}

/// Rowid IN-list arm: `SELECT id, v WHERE id IN (a,b,c)`, one SeekRowid per value vs a
/// `NOT INDEXED` full scan. Values vary. Exercises `codegen_select_rowid_in_scan`.
fn time_rowid_in_arm(conn: &Connection, not_indexed: bool, base: i64) -> u128 {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 7) % ROWS);
        let c = 1 + ((base + j as i64 + 13) % ROWS);
        let sql = format!("SELECT id, v FROM t{hint} WHERE id IN ({a}, {b}, {c})");
        let rows = conn.query(black_box(&sql)).expect("query");
        black_box(&rows);
    }
    start.elapsed().as_nanos()
}

/// OR-of-equalities arm: `SELECT id, v WHERE k = a OR k = b OR k = c`, normalized to a
/// per-value index seek vs a `NOT INDEXED` scan. Proves the OR->IN normalization fires.
fn time_or_arm(conn: &Connection, not_indexed: bool, base: i64) -> u128 {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 1) % ROWS);
        let c = 1 + ((base + j as i64 + 2) % ROWS);
        let sql = format!("SELECT id, v FROM t{hint} WHERE k = {a} OR k = {b} OR k = {c}");
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

    // Range arm: SUM(v) WHERE id <= <selective upper>, bounded scan vs NOT INDEXED scan.
    black_box(time_range_arm(&conn, false, 0));
    black_box(time_range_arm(&conn, true, 0));
    let mut rseek = Vec::with_capacity(SAMPLES);
    let mut rscan = Vec::with_capacity(SAMPLES);
    let mut rnull_a = Vec::with_capacity(SAMPLES);
    let mut rnull_b = Vec::with_capacity(SAMPLES);
    for s in 0..SAMPLES {
        let base = (s as i64) * (EXECS_PER_SAMPLE as i64);
        rseek.push(time_range_arm(&conn, false, base));
        rscan.push(time_range_arm(&conn, true, base));
        rnull_a.push(time_range_arm(&conn, false, base));
        rnull_b.push(time_range_arm(&conn, false, base));
    }
    let mr_seek = median(rseek);
    let mr_scan = median(rscan);
    let mr_na = median(rnull_a);
    let mr_nb = median(rnull_b);
    println!("--- range: SUM(v) WHERE id <= <upper ~100> ---");
    println!("range seek median = {:.3} us/query", us(mr_seek));
    println!("range scan median = {:.3} us/query", us(mr_scan));
    println!(
        "range speedup (scan/seek) = {:.3}x",
        (mr_scan as f64) / (mr_seek as f64)
    );
    println!(
        "range NULL control (seek/seek) = {:.3}x  [{:.3} vs {:.3} us/query]",
        (mr_nb as f64) / (mr_na as f64),
        us(mr_na),
        us(mr_nb)
    );

    // IN-list arm: SUM(v) WHERE k IN (a,b,c), per-value index seek vs NOT INDEXED scan.
    black_box(time_in_arm(&conn, false, 0));
    black_box(time_in_arm(&conn, true, 0));
    let mut iseek = Vec::with_capacity(SAMPLES);
    let mut iscan = Vec::with_capacity(SAMPLES);
    let mut inull_a = Vec::with_capacity(SAMPLES);
    let mut inull_b = Vec::with_capacity(SAMPLES);
    for s in 0..SAMPLES {
        let base = (s as i64) * (EXECS_PER_SAMPLE as i64);
        iseek.push(time_in_arm(&conn, false, base));
        iscan.push(time_in_arm(&conn, true, base));
        inull_a.push(time_in_arm(&conn, false, base));
        inull_b.push(time_in_arm(&conn, false, base));
    }
    let mi_seek = median(iseek);
    let mi_scan = median(iscan);
    let mi_na = median(inull_a);
    let mi_nb = median(inull_b);
    println!("--- in-list: SUM(v) WHERE k IN (a,b,c) ---");
    println!("in seek median = {:.3} us/query", us(mi_seek));
    println!("in scan median = {:.3} us/query", us(mi_scan));
    println!(
        "in speedup (scan/seek) = {:.3}x",
        (mi_scan as f64) / (mi_seek as f64)
    );
    println!(
        "in NULL control (seek/seek) = {:.3}x  [{:.3} vs {:.3} us/query]",
        (mi_nb as f64) / (mi_na as f64),
        us(mi_na),
        us(mi_nb)
    );

    // Non-aggregate IN-list arm: SELECT id,v WHERE k IN (a,b,c), seek+ResultRow vs scan.
    black_box(time_nonagg_in_arm(&conn, false, 0));
    black_box(time_nonagg_in_arm(&conn, true, 0));
    let mut nseek = Vec::with_capacity(SAMPLES);
    let mut nscan = Vec::with_capacity(SAMPLES);
    let mut nnull_a = Vec::with_capacity(SAMPLES);
    let mut nnull_b = Vec::with_capacity(SAMPLES);
    for s in 0..SAMPLES {
        let base = (s as i64) * (EXECS_PER_SAMPLE as i64);
        nseek.push(time_nonagg_in_arm(&conn, false, base));
        nscan.push(time_nonagg_in_arm(&conn, true, base));
        nnull_a.push(time_nonagg_in_arm(&conn, false, base));
        nnull_b.push(time_nonagg_in_arm(&conn, false, base));
    }
    let mn_seek = median(nseek);
    let mn_scan = median(nscan);
    let mn_na = median(nnull_a);
    let mn_nb = median(nnull_b);
    println!("--- non-aggregate in-list: SELECT id,v WHERE k IN (a,b,c) ---");
    println!("nonagg in seek median = {:.3} us/query", us(mn_seek));
    println!("nonagg in scan median = {:.3} us/query", us(mn_scan));
    println!(
        "nonagg in speedup (scan/seek) = {:.3}x",
        (mn_scan as f64) / (mn_seek as f64)
    );
    println!(
        "nonagg in NULL control (seek/seek) = {:.3}x  [{:.3} vs {:.3} us/query]",
        (mn_nb as f64) / (mn_na as f64),
        us(mn_na),
        us(mn_nb)
    );

    // Rowid IN-list arm: SELECT id,v WHERE id IN (a,b,c), SeekRowid per value vs scan.
    black_box(time_rowid_in_arm(&conn, false, 0));
    black_box(time_rowid_in_arm(&conn, true, 0));
    let mut rid_seek = Vec::with_capacity(SAMPLES);
    let mut rid_scan = Vec::with_capacity(SAMPLES);
    let mut rid_na = Vec::with_capacity(SAMPLES);
    let mut rid_nb = Vec::with_capacity(SAMPLES);
    for s in 0..SAMPLES {
        let base = (s as i64) * (EXECS_PER_SAMPLE as i64);
        rid_seek.push(time_rowid_in_arm(&conn, false, base));
        rid_scan.push(time_rowid_in_arm(&conn, true, base));
        rid_na.push(time_rowid_in_arm(&conn, false, base));
        rid_nb.push(time_rowid_in_arm(&conn, false, base));
    }
    let mrid_seek = median(rid_seek);
    let mrid_scan = median(rid_scan);
    let mrid_na = median(rid_na);
    let mrid_nb = median(rid_nb);
    println!("--- rowid in-list: SELECT id,v WHERE id IN (a,b,c) ---");
    println!("rowid in seek median = {:.3} us/query", us(mrid_seek));
    println!("rowid in scan median = {:.3} us/query", us(mrid_scan));
    println!(
        "rowid in speedup (scan/seek) = {:.3}x",
        (mrid_scan as f64) / (mrid_seek as f64)
    );
    println!(
        "rowid in NULL control (seek/seek) = {:.3}x  [{:.3} vs {:.3} us/query]",
        (mrid_nb as f64) / (mrid_na as f64),
        us(mrid_na),
        us(mrid_nb)
    );

    // OR-of-equalities arm.
    black_box(time_or_arm(&conn, false, 0));
    black_box(time_or_arm(&conn, true, 0));
    let mut or_seek = Vec::with_capacity(SAMPLES);
    let mut or_scan = Vec::with_capacity(SAMPLES);
    let mut or_na = Vec::with_capacity(SAMPLES);
    let mut or_nb = Vec::with_capacity(SAMPLES);
    for s in 0..SAMPLES {
        let base = (s as i64) * (EXECS_PER_SAMPLE as i64);
        or_seek.push(time_or_arm(&conn, false, base));
        or_scan.push(time_or_arm(&conn, true, base));
        or_na.push(time_or_arm(&conn, false, base));
        or_nb.push(time_or_arm(&conn, false, base));
    }
    let mor_seek = median(or_seek);
    let mor_scan = median(or_scan);
    let mor_na = median(or_na);
    let mor_nb = median(or_nb);
    println!("--- or-of-equalities: SELECT id,v WHERE k = a OR k = b OR k = c ---");
    println!("or seek median = {:.3} us/query", us(mor_seek));
    println!("or scan median = {:.3} us/query", us(mor_scan));
    println!(
        "or speedup (scan/seek) = {:.3}x",
        (mor_scan as f64) / (mor_seek as f64)
    );
    println!(
        "or NULL control (seek/seek) = {:.3}x  [{:.3} vs {:.3} us/query]",
        (mor_nb as f64) / (mor_na as f64),
        us(mor_na),
        us(mor_nb)
    );
}
