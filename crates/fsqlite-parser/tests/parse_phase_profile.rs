//! bd-5310l parse-phase profile: split per-statement parse time into LEX (tokenize) vs PARSE
//! (recursive-descent AST build) so the ONE lever can target the dominant half.
//!
//! Run under release-perf:
//!   RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!     cargo test --profile release-perf -p fsqlite-parser --test parse_phase_profile \
//!     -- --ignored --nocapture
//!
//! `lex` times `Lexer::tokenize` alone; `full parse` times
//! `parse_single_statement_with_scratch` (tokenize + parse, scratch reused); the difference is the
//! recursive-descent / AST-build cost. Same SQL repeated per shape (steady state).

// ns/count -> f64 for the split; precision loss is irrelevant to the ratio.
#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::time::Instant;

use fsqlite_parser::{
    Lexer, StatementParseScratch, parse_single_statement_with_scratch,
    set_force_interner_scan_bench,
};

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// bd-5310l A/B: the interner's `retained_bytes` O(entries) fold (per parse, via
/// `prepare_for_next_parse`) vs the incremental O(1) running sum, measured WITHIN one build via
/// `set_force_interner_scan_bench`. A connection retains interned identifiers across parses, so a
/// warm interner (real app: many distinct table/column names) makes the fold O(N) on every parse.
/// This warms the interner near capacity (~240 distinct identifiers), then measures parse of a
/// fixed statement with the fold FORCED vs the incremental sum vs a null control.
#[test]
#[ignore = "A/B bench; run under --profile release-perf"]
fn interner_retained_bytes_incremental_ab() {
    // Warm the interner near the 256-entry cap with distinct identifiers that persist across parses.
    let mut cols = String::new();
    for i in 0..240 {
        if i > 0 {
            cols.push_str(", ");
        }
        cols.push_str(&format!("c{i}"));
    }
    let warm_sql = format!("SELECT {cols} FROM warm_table");
    let measure_sql = "SELECT id, v FROM t WHERE id = 5";

    let mut scratch = StatementParseScratch::default();
    // Intern ~242 identifiers (c0..c239 + warm_table + id/v/t added by the measure warm-up); stays
    // under the 256-entry / 16KB caps so `prepare_for_next_parse` never resets it.
    let _ = parse_single_statement_with_scratch(&warm_sql, &mut scratch);
    for _ in 0..100 {
        let _ = parse_single_statement_with_scratch(measure_sql, &mut scratch);
    }

    let samples = 40usize;
    let k = 5000usize;
    let run = |force_scan: bool, scratch: &mut StatementParseScratch| -> f64 {
        set_force_interner_scan_bench(force_scan);
        let t = Instant::now();
        for _ in 0..k {
            let _ = black_box(parse_single_statement_with_scratch(
                black_box(measure_sql),
                scratch,
            ));
        }
        t.elapsed().as_nanos() as f64 / k as f64
    };

    let mut scan_ns = Vec::new();
    let mut incr_ns = Vec::new();
    let mut null_ns = Vec::new();
    for _ in 0..samples {
        scan_ns.push(run(true, &mut scratch));
        incr_ns.push(run(false, &mut scratch));
        null_ns.push(run(false, &mut scratch));
    }
    set_force_interner_scan_bench(false);

    let ms = median(scan_ns);
    let mi = median(incr_ns);
    let mn = median(null_ns);
    eprintln!(
        "\n########## bd-5310l interner retained_bytes O(N)-fold vs O(1)-incremental A/B ##########"
    );
    eprintln!(
        "  ~240-entry warm interner, {samples} samples x {k} parses/arm:\n    \
         SCAN arm (O(entries) fold) parse median = {ms:8.1} ns/stmt\n    \
         INCR arm (O(1) running sum) parse median = {mi:8.1} ns/stmt\n    \
         speedup (scan/incr)                      = {:.3}x\n    \
         null control (incr vs incr)              = {:.3}x  [{mn:.1} vs {mi:.1}]\n    \
         per-parse fold cost eliminated           = {:.1} ns",
        ms / mi,
        mn / mi,
        ms - mi,
    );
    eprintln!("########## end interner retained_bytes A/B ##########\n");
}

#[test]
#[ignore = "profile; run under --profile release-perf"]
fn parse_phase_lex_vs_parse_split() {
    let sqls = [
        "SELECT 1",
        "SELECT id, v FROM t WHERE id = 5",
        "SELECT id, v, k FROM t WHERE k BETWEEN 10 AND 20 ORDER BY k, id",
        "INSERT INTO t (a, b, c) VALUES (1, 'hello world', 3.5)",
        "SELECT a.id, b.name FROM users a JOIN accounts b ON a.id = b.uid WHERE a.age > 21",
    ];
    let n = 300_000u32;
    eprintln!("\n########## bd-5310l parse-phase lex-vs-parse split ##########");
    for sql in sqls {
        // Warm.
        for _ in 0..1000 {
            black_box(Lexer::tokenize(black_box(sql)));
        }
        let t = Instant::now();
        for _ in 0..n {
            black_box(Lexer::tokenize(black_box(sql)));
        }
        let lex = t.elapsed().as_nanos() as f64 / f64::from(n);

        let mut scratch = StatementParseScratch::default();
        for _ in 0..1000 {
            let _ = black_box(parse_single_statement_with_scratch(
                black_box(sql),
                &mut scratch,
            ));
        }
        let t = Instant::now();
        for _ in 0..n {
            let _ = black_box(parse_single_statement_with_scratch(
                black_box(sql),
                &mut scratch,
            ));
        }
        let full = t.elapsed().as_nanos() as f64 / f64::from(n);

        eprintln!(
            "[{sql}]\n  lex        = {lex:7.1} ns\n  full parse = {full:7.1} ns\n  parse-only = {:7.1} ns  ({:.0}% lex / {:.0}% parse)",
            full - lex,
            lex / full * 100.0,
            (full - lex) / full * 100.0,
        );
    }
    eprintln!("########## end parse-phase split ##########\n");
}
