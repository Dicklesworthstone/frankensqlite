//! bd-5310l profile-first: split the per-statement ad-hoc overhead (65us/stmt on `SELECT 1`
//! vs C SQLite ~1us) into phases using the built-in hot-path profile counters, so the ONE
//! lever can target the dominant phase rather than guessing.
//!
//! Run under release-perf for ledger-grade numbers:
//!   RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!     cargo test --profile release-perf -p fsqlite-e2e --test adhoc_parse_plan_profile \
//!     -- --ignored --nocapture
//!
//! Each workload runs the SAME SQL repeatedly (steady-state, parse+compile caches warm), so
//! the reported per-statement phase split is what an ad-hoc/ORM/migration workload actually
//! pays on every statement. `gap = wall_clock - sum(instrumented phases)` is the
//! un-instrumented setup overhead (tracing gates, clones, dispatch bookkeeping).

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_core::connection::{
    HotPathProfileSnapshot, compile_subphase_ns, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};

fn ns_per(total_ns: u64, n: u64) -> f64 {
    total_ns as f64 / n as f64
}

fn report(label: &str, sql: &str, n: u64, wall_ns: u128, s: &HotPathProfileSnapshot) {
    let wall_per = wall_ns as f64 / n as f64;
    let parse = ns_per(s.parser.parse_time_ns, n);
    let rewrite = ns_per(s.parser.rewrite_time_ns, n);
    let compile = ns_per(s.parser.compile_time_ns, n);
    let bg = ns_per(s.background_status_time_ns, n);
    let prep_lookup = ns_per(s.prepared_lookup_time_ns, n);
    let begin = ns_per(s.begin_setup_time_ns, n);
    let exec = ns_per(s.execute_body_time_ns, n);
    let commit_rt = ns_per(s.commit_txn_roundtrip_time_ns, n);
    let commit_pre = ns_per(s.commit_pre_txn_time_ns, n);
    let commit_fin = ns_per(s.commit_finalize_seq_time_ns, n);
    let instrumented = parse
        + rewrite
        + compile
        + bg
        + prep_lookup
        + begin
        + exec
        + commit_rt
        + commit_pre
        + commit_fin;
    let gap = wall_per - instrumented;
    eprintln!(
        "\n=== [{label}] `{sql}` (n={n}) ===\n  \
         wall            = {wall_per:9.1} ns/stmt\n  \
         parse           = {parse:9.1} ns   (cache hits {ph}/{pm} miss)\n  \
         rewrite         = {rewrite:9.1} ns\n  \
         compile         = {compile:9.1} ns   (cache hits {ch}/{cm} miss)\n  \
         background_stat = {bg:9.1} ns\n  \
         prepared_lookup = {prep_lookup:9.1} ns\n  \
         begin_setup     = {begin:9.1} ns\n  \
         execute_body    = {exec:9.1} ns\n  \
         commit_pre_txn  = {commit_pre:9.1} ns\n  \
         commit_roundtrip= {commit_rt:9.1} ns\n  \
         commit_finalize = {commit_fin:9.1} ns\n  \
         ---- instrumented sum = {instrumented:9.1} ns\n  \
         ---- UN-INSTR gap     = {gap:9.1} ns   <- setup/dispatch/tracing/clones",
        ph = s.parser.parse_cache_hits,
        pm = s.parser.parse_cache_misses,
        ch = s.parser.compiled_cache_hits,
        cm = s.parser.compiled_cache_misses,
    );
}

fn profile_workload(label: &str, conn: &Connection, sql: &str, n: u64, is_write: bool) {
    // Warm the parse + compile caches so we measure steady-state.
    for _ in 0..50 {
        if is_write {
            let _ = conn.execute(sql);
        } else {
            let _ = conn.query(sql).unwrap();
        }
    }
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    let t = Instant::now();
    for _ in 0..n {
        if is_write {
            let _ = conn.execute(sql).unwrap();
        } else {
            let _ = conn.query(sql).unwrap();
        }
    }
    let wall_ns = t.elapsed().as_nanos();
    let snap = hot_path_profile_snapshot();
    reset_hot_path_profile();
    set_hot_path_profile_enabled(false);
    report(label, sql, n, wall_ns, &snap);
}

/// The bead's real scenario: every statement is textually unique, so parse + compile caches both
/// miss. Generates `SELECT id, v FROM t WHERE id = <i>` (a realistic point read) with a distinct
/// literal each iteration and reports the cache-miss front-end split.
fn profile_unique_sql(conn: &Connection) {
    let n: u64 = 40_000;
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    let t = Instant::now();
    for i in 0..n {
        let sql = format!("SELECT id, v FROM t WHERE id = {}", 1 + (i % 2000));
        let _ = conn.query(&sql).unwrap();
    }
    let wall_ns = t.elapsed().as_nanos();
    let snap = hot_path_profile_snapshot();
    let sub = compile_subphase_ns();
    reset_hot_path_profile();
    set_hot_path_profile_enabled(false);
    report(
        "UNIQUE SQL (cache-miss)",
        "SELECT id, v FROM t WHERE id = <i>",
        n,
        wall_ns,
        &snap,
    );
    let nf = n as f64;
    eprintln!(
        "  COMPILE sub-split (ns/stmt): schema_clone={:.1}  canonicalize+to_string={:.1}  \
         planner={:.1}  codegen={:.1}  finish={:.1}",
        sub[0] as f64 / nf,
        sub[1] as f64 / nf,
        sub[2] as f64 / nf,
        sub[3] as f64 / nf,
        sub[4] as f64 / nf,
    );
}

#[test]
#[ignore = "profile-first diagnostic; run explicitly under --profile release-perf"]
fn adhoc_parse_plan_phase_profile() {
    let conn = Connection::open(":memory:").expect("open frank");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_k ON t(k);").unwrap();
    for i in 1..=2000_i64 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'row{i}', {});",
            i % 100
        ))
        .unwrap();
    }
    // Dedicated auto-rowid table for the INSERT workload: identical SQL every iteration (compile
    // cache warm), auto-assigned rowid, no PK collision.
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v TEXT);")
        .unwrap();

    eprintln!("\n########## bd-5310l ad-hoc per-statement phase profile ##########");

    // A: trivial expression select — pure parse/plan/dispatch overhead, ~zero execution.
    profile_workload("SELECT 1", &conn, "SELECT 1", 200_000, false);

    // B: table point-query by rowid PK — realistic ad-hoc read, cache-warm.
    profile_workload(
        "point rowid",
        &conn,
        "SELECT id, v FROM t WHERE id = 777",
        200_000,
        false,
    );

    // C: indexed range read — a heavier plan.
    profile_workload(
        "indexed range",
        &conn,
        "SELECT id, v FROM t WHERE k BETWEEN 20 AND 30",
        100_000,
        false,
    );

    // D: ad-hoc INSERT inside autocommit — the bulk-load hotspot (bead: ~295x on 10k inserts).
    // Auto rowid, identical SQL every iteration -> compile cache warm, no PK collision.
    profile_workload(
        "insert autocommit",
        &conn,
        "INSERT INTO t2 (v) VALUES ('x')",
        50_000,
        true,
    );

    // E: the ACTUAL bead scenario — UNIQUE SQL every statement (migration / ad-hoc shell / ORM
    // with no statement cache). Both the parse AND compile caches MISS every call, so parse_time
    // and compile_time now reflect the real per-statement front-end cost the bead measured (65us).
    profile_unique_sql(&conn);

    eprintln!("\n########## end bd-5310l phase profile ##########\n");
}
