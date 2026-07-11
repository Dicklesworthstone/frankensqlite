//! DIAGNOSTIC (bd-zq6dp): what plan does FrankenSQLite choose for a secondary-index
//! range predicate, and does it actually seek at the bytecode + timing level?
//!
//! The 2026-07-10 ledger NON-WIN measured `SELECT id,k WHERE k BETWEEN a AND b`
//! full-scanning (0.985x vs NOT INDEXED) and blamed a connection SimpleFullTableScan
//! bypass. But EQP shows SEARCH USING INDEX. This probe pins the truth via three lenses:
//!   1. EXPLAIN QUERY PLAN (directive-level: SCAN vs SEARCH)
//!   2. EXPLAIN opcodes (bytecode-level: SeekGE/IdxGT vs Rewind/Next)
//!   3. wall-clock A/B (seek arm vs NOT INDEXED scan arm, median us/query)
//!
//! Run with: cargo test -p fsqlite-e2e --test index_range_eqp_diagnostic -- --nocapture

// Timing / row-count casts (u128 elapsed micros, usize exec counts) to f64 for medians;
// precision loss is irrelevant to a wall-clock A/B ratio.
#![allow(clippy::cast_precision_loss)]

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn cell(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Null => "NULL".to_owned(),
        other => format!("{other:?}"),
    }
}

fn eqp(conn: &Connection, sql: &str) -> String {
    match conn.query(&format!("EXPLAIN QUERY PLAN {sql}")) {
        Ok(rows) => rows
            .iter()
            .map(|row| row.values().iter().map(cell).collect::<Vec<_>>().join("|"))
            .collect::<Vec<_>>()
            .join("  ///  "),
        Err(e) => format!("<EQP error: {e}>"),
    }
}

fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    match conn.query(&format!("EXPLAIN {sql}")) {
        Ok(rows) => rows
            .iter()
            .filter_map(|row| row.values().get(1).map(cell))
            .collect(),
        Err(_) => vec![],
    }
}

fn row_count(conn: &Connection, sql: &str) -> usize {
    conn.query(sql).map(|r| r.len()).unwrap_or(usize::MAX)
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

// Reproducible A/B artifact, not a CI gate: 20k inserts + a timing loop. Run explicitly:
//   cargo test --profile release-perf -p fsqlite-e2e --test index_range_eqp_diagnostic \
//     -- --ignored --nocapture
#[test]
#[ignore = "diagnostic/A-B bench; run explicitly under --profile release-perf"]
fn diagnostic_index_range_plan_choice() {
    let f = Connection::open(":memory:").expect("open frank");
    f.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL, w TEXT);")
        .unwrap();
    f.execute("CREATE INDEX idx_t_k ON t(k);").unwrap();
    for i in 1..=20_000_i64 {
        f.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {i}.5, 'r{i}');"))
            .unwrap();
    }

    // --- Lens 1 & 2: directive + bytecode for representative shapes ---
    let shapes: [(&str, &str); 4] = [
        (
            "covering id, selective range",
            "SELECT id FROM t WHERE k BETWEEN 5 AND 55",
        ),
        (
            "covering id,k selective range",
            "SELECT id, k FROM t WHERE k BETWEEN 5 AND 55",
        ),
        (
            "non-covering id,v range",
            "SELECT id, v FROM t WHERE k BETWEEN 5 AND 55",
        ),
        (
            "NOT INDEXED (forced scan)",
            "SELECT id, k FROM t NOT INDEXED WHERE k BETWEEN 5 AND 55",
        ),
    ];
    eprintln!("\n=== bd-zq6dp index-range diagnostic (20000 rows, k==id) ===");
    for (label, sql) in shapes {
        let ops = opcodes(&f, sql);
        let seeks = ops
            .iter()
            .filter(|o| {
                o.contains("Seek")
                    || o.contains("IdxGT")
                    || o.contains("IdxGE")
                    || o.contains("IdxLT")
                    || o.contains("IdxLE")
            })
            .count();
        let rewinds = ops.iter().filter(|o| o.as_str() == "Rewind").count();
        eprintln!(
            "[{label}]\n    {sql}\n    EQP: {}\n    opcodes: {} total, {} seek/idx-cmp, {} Rewind\n    key ops: {}",
            eqp(&f, sql),
            ops.len(),
            seeks,
            rewinds,
            ops.iter()
                .filter(|o| o.contains("Seek")
                    || o.contains("Idx")
                    || o.as_str() == "Rewind"
                    || o.as_str() == "Next"
                    || o.as_str() == "Column"
                    || o.as_str() == "ResultRow")
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    // --- Lens 3: wall-clock A/B, seek vs NOT INDEXED scan, varied bounds, median ---
    // Run under `--profile release-perf` for ledger-grade numbers. Arms interleave per
    // sample (seek, scan, null-control) so sequential drift cancels; bounds vary per sample
    // so no per-statement cache can serve a repeat; the scan arm is forced via NOT INDEXED.
    let samples = 60usize;
    let execs = 40usize;
    let mut seek_us = Vec::with_capacity(samples);
    let mut scan_us = Vec::with_capacity(samples);
    let mut null_us = Vec::with_capacity(samples);
    // Correctness: both arms must return the same 51 rows.
    let a0 = 5000;
    assert_eq!(
        row_count(
            &f,
            &format!("SELECT id, k FROM t WHERE k BETWEEN {a0} AND {}", a0 + 50)
        ),
        row_count(
            &f,
            &format!(
                "SELECT id, k FROM t NOT INDEXED WHERE k BETWEEN {a0} AND {}",
                a0 + 50
            )
        ),
        "seek and scan arms must return identical row counts"
    );

    for s in 0..samples {
        let a = 100 + (s * 379) % 19000; // vary bounds per sample
        let seek_sql = format!("SELECT id, k FROM t WHERE k BETWEEN {a} AND {}", a + 50);
        let scan_sql = format!(
            "SELECT id, k FROM t NOT INDEXED WHERE k BETWEEN {a} AND {}",
            a + 50
        );

        let t = Instant::now();
        for _ in 0..execs {
            let _ = f.query(&seek_sql).unwrap();
        }
        seek_us.push(t.elapsed().as_micros() as f64 / execs as f64);

        let t = Instant::now();
        for _ in 0..execs {
            let _ = f.query(&scan_sql).unwrap();
        }
        scan_us.push(t.elapsed().as_micros() as f64 / execs as f64);

        let t = Instant::now();
        for _ in 0..execs {
            let _ = f.query(&seek_sql).unwrap();
        }
        null_us.push(t.elapsed().as_micros() as f64 / execs as f64);
    }

    let ms = median(seek_us);
    let mc = median(scan_us);
    let mn = median(null_us);
    eprintln!(
        "\n  A/B (51-row selective range, {samples} samples x {execs} execs, varied bounds):\n    \
         seek (WHERE k BETWEEN)          median = {ms:.2} us/query\n    \
         scan (NOT INDEXED k BETWEEN)    median = {mc:.2} us/query\n    \
         speedup (scan/seek)             = {:.2}x\n    \
         null control (seek vs seek)     = {:.3}x  [{mn:.2} vs {ms:.2}]",
        mc / ms,
        mn / ms,
    );

    // bd-u6tbr placeholder A/B: the OLTP prepared-statement shape. Prepare once, bind varying
    // bounds per exec so the seek's runtime Affinity coercion is exercised on the hot path.
    let ph_seek = f
        .prepare("SELECT id, k FROM t WHERE k BETWEEN ?1 AND ?2")
        .unwrap();
    let ph_scan = f
        .prepare("SELECT id, k FROM t NOT INDEXED WHERE k BETWEEN ?1 AND ?2")
        .unwrap();
    let mut ph_seek_us = Vec::with_capacity(samples);
    let mut ph_scan_us = Vec::with_capacity(samples);
    let mut ph_null_us = Vec::with_capacity(samples);
    for s in 0..samples {
        let a = 100 + (s * 379) % 19000;
        let params = [
            SqliteValue::Integer(i64::try_from(a).unwrap()),
            SqliteValue::Integer(i64::try_from(a + 50).unwrap()),
        ];
        let t = Instant::now();
        for _ in 0..execs {
            let _ = ph_seek.query_with_params(&params).unwrap();
        }
        ph_seek_us.push(t.elapsed().as_micros() as f64 / execs as f64);

        let t = Instant::now();
        for _ in 0..execs {
            let _ = ph_scan.query_with_params(&params).unwrap();
        }
        ph_scan_us.push(t.elapsed().as_micros() as f64 / execs as f64);

        let t = Instant::now();
        for _ in 0..execs {
            let _ = ph_seek.query_with_params(&params).unwrap();
        }
        ph_null_us.push(t.elapsed().as_micros() as f64 / execs as f64);
    }
    let pms = median(ph_seek_us);
    let pmc = median(ph_scan_us);
    let pmn = median(ph_null_us);
    eprintln!(
        "\n  Placeholder A/B (prepared, bind varying bounds, {samples}x{execs}):\n    \
         seek (k BETWEEN ?1 AND ?2)      median = {pms:.2} us/query\n    \
         scan (NOT INDEXED ?1 AND ?2)    median = {pmc:.2} us/query\n    \
         speedup (scan/seek)             = {:.2}x\n    \
         null control (seek vs seek)     = {:.3}x  [{pmn:.2} vs {pms:.2}]",
        pmc / pms,
        pmn / pms,
    );
    eprintln!("=== end diagnostic ===\n");
}

/// bd-6x9z0 follow-up A/B: composite DESC (`WHERE a = v AND b <range> ORDER BY b DESC, id DESC`)
/// streams off a reverse index walk with NO sorter, seeking only the `a == v` block. The scan arm
/// (`NOT INDEXED`) must full-scan every row, filter, AND sort. The win is two-fold: fewer row
/// visits (the a-block, not the whole table) and no O(n log n) sorter. Median us/query; null
/// control (seek vs seek) is the sequential-drift floor.
#[test]
#[ignore = "diagnostic/A-B bench; run explicitly under --profile release-perf"]
fn diagnostic_composite_desc_order_by() {
    let f = Connection::open(":memory:").expect("open frank");
    f.execute("CREATE TABLE ct (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, w TEXT);")
        .unwrap();
    f.execute("CREATE INDEX idx_ct_ab ON ct(a, b);").unwrap();
    // 40 distinct `a` values (~500 rows each); `b` spread across [0,1000) per row.
    let n_a = 40i64;
    for i in 1..=20_000_i64 {
        let a = i % n_a;
        let b = (i * 7) % 1000;
        f.execute(&format!("INSERT INTO ct VALUES ({i}, {a}, {b}, 'r{i}');"))
            .unwrap();
    }

    // Bytecode lens: the seek arm must reverse-walk (Prev) with no sorter; the scan arm sorts.
    let seek_ops = opcodes(
        &f,
        "SELECT id, b FROM ct WHERE a = 7 AND b > 500 ORDER BY b DESC, id DESC",
    );
    let scan_ops = opcodes(
        &f,
        "SELECT id, b FROM ct NOT INDEXED WHERE a = 7 AND b > 500 ORDER BY b DESC, id DESC",
    );
    eprintln!("\n=== bd-6x9z0 composite-DESC diagnostic (20000 rows, 40 a-blocks) ===");
    eprintln!(
        "  seek arm: Prev={}, Sorter={}, IdxRowid={}\n  scan arm: Rewind={}, Sorter={}",
        seek_ops.iter().any(|o| o == "Prev"),
        seek_ops.iter().any(|o| o.starts_with("Sorter")),
        seek_ops.iter().any(|o| o == "IdxRowid"),
        scan_ops.iter().any(|o| o == "Rewind"),
        scan_ops.iter().any(|o| o.starts_with("Sorter")),
    );

    let samples = 60usize;
    let execs = 40usize;
    // Correctness sanity (the oracle test is the real gate): both arms return the same rows.
    assert_eq!(
        row_count(
            &f,
            "SELECT id, b FROM ct WHERE a = 7 AND b > 500 ORDER BY b DESC, id DESC"
        ),
        row_count(
            &f,
            "SELECT id, b FROM ct NOT INDEXED WHERE a = 7 AND b > 500 ORDER BY b DESC, id DESC"
        ),
        "composite-DESC seek and scan arms must return identical row counts"
    );

    let mut seek_us = Vec::with_capacity(samples);
    let mut scan_us = Vec::with_capacity(samples);
    let mut null_us = Vec::with_capacity(samples);
    for s in 0..samples {
        let si = i64::try_from(s).unwrap();
        let av = si % n_a;
        let lo = 200 + (si * 13) % 500; // vary bounds per sample
        let seek_sql =
            format!("SELECT id, b FROM ct WHERE a = {av} AND b > {lo} ORDER BY b DESC, id DESC");
        let scan_sql = format!(
            "SELECT id, b FROM ct NOT INDEXED WHERE a = {av} AND b > {lo} ORDER BY b DESC, id DESC"
        );

        let t = Instant::now();
        for _ in 0..execs {
            let _ = f.query(&seek_sql).unwrap();
        }
        seek_us.push(t.elapsed().as_micros() as f64 / execs as f64);

        let t = Instant::now();
        for _ in 0..execs {
            let _ = f.query(&scan_sql).unwrap();
        }
        scan_us.push(t.elapsed().as_micros() as f64 / execs as f64);

        let t = Instant::now();
        for _ in 0..execs {
            let _ = f.query(&seek_sql).unwrap();
        }
        null_us.push(t.elapsed().as_micros() as f64 / execs as f64);
    }

    let ms = median(seek_us);
    let mc = median(scan_us);
    let mn = median(null_us);
    eprintln!(
        "\n  Composite-DESC A/B ({samples} samples x {execs} execs, varied a-block + bound):\n    \
         seek (a=v AND b>lo ORDER BY b DESC, id DESC)   median = {ms:.2} us/query\n    \
         scan (NOT INDEXED, same, + sorter)             median = {mc:.2} us/query\n    \
         speedup (scan/seek)                            = {:.2}x\n    \
         null control (seek vs seek)                    = {:.3}x  [{mn:.2} vs {ms:.2}]",
        mc / ms,
        mn / ms,
    );
    eprintln!("=== end composite-DESC diagnostic ===\n");
}
