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

// ns/count -> f64 for medians; precision loss is irrelevant to a timing ratio.
#![allow(clippy::cast_precision_loss)]
#![recursion_limit = "512"]

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_core::connection::{
    HotPathProfileSnapshot, compile_subphase_ns, hot_path_profile_snapshot, planner_dispatch_ns,
    reset_hot_path_profile, set_force_planner_cache_bench, set_force_schema_clone_bench,
    set_hot_path_profile_enabled,
};

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

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

async fn profile_workload(label: &str, conn: &Connection, sql: &str, n: u64, is_write: bool) {
    // Warm the parse + compile caches so we measure steady-state.
    for _ in 0..50 {
        if is_write {
            drop(conn.execute(sql).await);
        } else {
            let _ = conn.query(sql).await.unwrap();
        }
    }
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    let t = Instant::now();
    for _ in 0..n {
        if is_write {
            let _ = conn.execute(sql).await.unwrap();
        } else {
            let _ = conn.query(sql).await.unwrap();
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
async fn profile_unique_sql(conn: &Connection) {
    let n: u64 = 40_000;
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    let t = Instant::now();
    for i in 0..n {
        let sql = format!("SELECT id, v FROM t WHERE id = {}", 1 + (i % 2000));
        let _ = conn.query(&sql).await.unwrap();
    }
    let wall_ns = t.elapsed().as_nanos();
    let snap = hot_path_profile_snapshot();
    let sub = compile_subphase_ns();
    let pd = planner_dispatch_ns();
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
    eprintln!(
        "  PLANNER dispatch split (ns/stmt): to_string={:.1}  plan_compute={:.1}  \
         cache_machinery={:.1}",
        pd[0] as f64 / nf,
        pd[1] as f64 / nf,
        (sub[2] as f64 - pd[0] as f64 - pd[1] as f64) / nf,
    );
}

/// bd-5310l A/B: the per-compile defensive schema deep-clone vs the borrow fast path, measured
/// WITHIN one build via `set_force_schema_clone_bench`. A large schema (many tables/indexes) makes
/// the O(total-schema) clone dominate. Each arm compiles DISTINCT unique-literal SQL of the same
/// shape (so the compile cache always misses and the compared work is identical modulo the literal);
/// the clone arm forces the clone, the borrow arm takes the fast path, and a null control re-runs the
/// borrow arm to expose the sequential-drift floor. Gate on median(clone)/median(borrow).
#[test]
#[ignore = "A/B bench; run explicitly under --profile release-perf"]
fn schema_clone_vs_borrow_compile_ab() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open frank");
        // Large schema: 40 tables x (6 cols + 2 indexes) — the deep clone copies ALL of it per compile.
        for tb in 0..40 {
            conn.execute(&format!(
                "CREATE TABLE big{tb} (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT, d REAL, e INTEGER);"
            ))
            .await
            .unwrap();
            conn.execute(&format!("CREATE INDEX ix_big{tb}_b ON big{tb}(b);"))
                .await
                .unwrap();
            conn.execute(&format!("CREATE INDEX ix_big{tb}_c ON big{tb}(c);"))
                .await
                .unwrap();
        }
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        for i in 1..=500_i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}');"))
                .await
                .unwrap();
        }

        let samples = 40usize;
        let k = 400usize;
        // Globally-unique literal so every compile is a genuine cache MISS (never repeats a shape+literal).
        let mut lit = 1_000_000_i64;
        let run = async |force: bool, lit: &mut i64| -> (f64, f64) {
            set_force_schema_clone_bench(force);
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            for _ in 0..k {
                *lit += 1;
                let sql = format!("SELECT id, v FROM t WHERE id = {lit}");
                let _ = conn.query(&sql).await.unwrap();
            }
            let snap = hot_path_profile_snapshot();
            let sub = compile_subphase_ns();
            reset_hot_path_profile();
            set_hot_path_profile_enabled(false);
            let kf = k as f64;
            // (total compile ns/stmt, schema_clone sub-timer ns/stmt)
            (snap.parser.compile_time_ns as f64 / kf, sub[0] as f64 / kf)
        };

        let mut clone_ns = Vec::new();
        let mut clone_sub = Vec::new();
        let mut borrow_ns = Vec::new();
        let mut borrow_sub = Vec::new();
        let mut null_ns = Vec::new();
        for _ in 0..samples {
            let (c, cs) = run(true, &mut lit).await;
            clone_ns.push(c);
            clone_sub.push(cs);
            let (b, bs) = run(false, &mut lit).await;
            borrow_ns.push(b);
            borrow_sub.push(bs);
            let (n, _) = run(false, &mut lit).await;
            null_ns.push(n);
        }
        set_force_schema_clone_bench(false);

        let mc = median(clone_ns);
        let mb = median(borrow_ns);
        let mn = median(null_ns);
        eprintln!(
            "\n########## bd-5310l schema clone-vs-borrow compile A/B (40-table schema) ##########"
        );
        eprintln!(
            "  {samples} samples x {k} unique compiles/arm:\n    \
             CLONE arm  compile median = {mc:9.1} ns/stmt   (schema_clone sub = {:.1} ns)\n    \
             BORROW arm compile median = {mb:9.1} ns/stmt   (schema_clone sub = {:.1} ns)\n    \
             speedup (clone/borrow)    = {:.3}x\n    \
             null control (borrow vs borrow) = {:.3}x  [{mn:.1} vs {mb:.1}]\n    \
             clone cost eliminated     = {:.1} ns/stmt",
            median(clone_sub),
            median(borrow_sub),
            mc / mb,
            mn / mb,
            mc - mb,
        );
        eprintln!("########## end schema clone-vs-borrow A/B ##########\n");
    });
}

/// bd-5310l A/B: the planner-directive cache path vs the fronted bypass, measured WITHIN one build
/// via `set_force_planner_cache_bench`. On the ad-hoc `query()` path the compiled-program cache
/// fronts each compile, so the planner-directive cache is redundant; the bypass computes the
/// directive directly and skips the `to_string` cache-key derivation + lookup + insert. Each arm
/// compiles DISTINCT unique-literal SQL (compile cache always misses); CACHE arm forces the cache,
/// BYPASS arm takes the fast path, null control re-runs BYPASS. Gate on median(cache)/median(bypass).
#[test]
#[ignore = "A/B bench; run explicitly under --profile release-perf"]
fn planner_cache_bypass_compile_ab() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open frank");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_t_k ON t(k);").await.unwrap();
        for i in 1..=500_i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}', {});", i % 50))
                .await
                .unwrap();
        }

        let samples = 40usize;
        let k = 400usize;
        let mut lit = 1_000_000_i64;
        let run = async |force_cache: bool, lit: &mut i64| -> (f64, f64) {
            set_force_planner_cache_bench(force_cache);
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            for _ in 0..k {
                *lit += 1;
                let sql = format!("SELECT id, v FROM t WHERE id = {lit}");
                let _ = conn.query(&sql).await.unwrap();
            }
            let snap = hot_path_profile_snapshot();
            let pd = planner_dispatch_ns();
            reset_hot_path_profile();
            set_hot_path_profile_enabled(false);
            let kf = k as f64;
            // (total compile ns/stmt, to_string sub-timer ns/stmt — nonzero only on the cache arm)
            (snap.parser.compile_time_ns as f64 / kf, pd[0] as f64 / kf)
        };

        let mut cache_ns = Vec::new();
        let mut cache_ts = Vec::new();
        let mut bypass_ns = Vec::new();
        let mut null_ns = Vec::new();
        for _ in 0..samples {
            let (c, cts) = run(true, &mut lit).await;
            cache_ns.push(c);
            cache_ts.push(cts);
            let (b, _) = run(false, &mut lit).await;
            bypass_ns.push(b);
            let (n, _) = run(false, &mut lit).await;
            null_ns.push(n);
        }
        set_force_planner_cache_bench(false);

        let mc = median(cache_ns);
        let mb = median(bypass_ns);
        let mn = median(null_ns);
        eprintln!("\n########## bd-5310l planner-cache bypass compile A/B ##########");
        eprintln!(
            "  {samples} samples x {k} unique compiles/arm:\n    \
             CACHE arm  compile median = {mc:9.1} ns/stmt   (to_string sub = {:.1} ns)\n    \
             BYPASS arm compile median = {mb:9.1} ns/stmt\n    \
             speedup (cache/bypass)    = {:.3}x\n    \
             null control (bypass vs bypass) = {:.3}x  [{mn:.1} vs {mb:.1}]\n    \
             cache cost eliminated     = {:.1} ns/stmt",
            median(cache_ts),
            mc / mb,
            mn / mb,
            mc - mb,
        );
        eprintln!("########## end planner-cache bypass A/B ##########\n");
    });
}

#[test]
#[ignore = "profile-first diagnostic; run explicitly under --profile release-perf"]
fn adhoc_parse_plan_phase_profile() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open frank");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_t_k ON t(k);").await.unwrap();
        for i in 1..=2000_i64 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'row{i}', {});",
                i % 100
            ))
            .await
            .unwrap();
        }
        // Dedicated auto-rowid table for the INSERT workload: identical SQL every iteration (compile
        // cache warm), auto-assigned rowid, no PK collision.
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();

        eprintln!("\n########## bd-5310l ad-hoc per-statement phase profile ##########");

        // A: trivial expression select — pure parse/plan/dispatch overhead, ~zero execution.
        profile_workload("SELECT 1", &conn, "SELECT 1", 200_000, false).await;

        // B: table point-query by rowid PK — realistic ad-hoc read, cache-warm.
        profile_workload(
            "point rowid",
            &conn,
            "SELECT id, v FROM t WHERE id = 777",
            200_000,
            false,
        )
        .await;

        // C: indexed range read — a heavier plan.
        profile_workload(
            "indexed range",
            &conn,
            "SELECT id, v FROM t WHERE k BETWEEN 20 AND 30",
            100_000,
            false,
        )
        .await;

        // D: ad-hoc INSERT inside autocommit — the bulk-load hotspot (bead: ~295x on 10k inserts).
        // Auto rowid, identical SQL every iteration -> compile cache warm, no PK collision.
        profile_workload(
            "insert autocommit",
            &conn,
            "INSERT INTO t2 (v) VALUES ('x')",
            50_000,
            true,
        )
        .await;

        // E: the ACTUAL bead scenario — UNIQUE SQL every statement (migration / ad-hoc shell / ORM
        // with no statement cache). Both the parse AND compile caches MISS every call, so parse_time
        // and compile_time now reflect the real per-statement front-end cost the bead measured (65us).
        profile_unique_sql(&conn).await;

        eprintln!("\n########## end bd-5310l phase profile ##########\n");
    });
}
