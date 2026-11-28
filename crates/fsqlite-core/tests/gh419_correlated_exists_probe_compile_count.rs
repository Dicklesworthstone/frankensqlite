#![recursion_limit = "512"]

//! GH#419: the SQLite documentation's Sudoku solver (a recursive CTE whose
//! recursive arm filters candidates with a correlated `NOT EXISTS` over a
//! 9-row CTE) ran ~100x slower than stock sqlite3.
//!
//! Root cause: the `NOT EXISTS` probe is evaluated once per candidate row
//! (~42k times for this puzzle) and, because its predicate is an `OR` of
//! expression comparisons rather than one correlated equality, every probe
//! was validated and compiled into a fresh VDBE program from scratch. The fix
//! scans the small probe table directly.
//!
//! This keeper pins the complexity property rather than a wall-clock time:
//! the number of statement compilations for one execution must not grow with
//! the number of probes. It lives in its own test binary because the hot-path
//! profile counters are process-global.

use std::time::{Duration, Instant};

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const SUDOKU: &str = r"
WITH RECURSIVE
  input(sud) AS (
    VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')
  ),
  digits(z, lp) AS (
    VALUES('1', 1)
    UNION ALL SELECT
    CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
  ),
  x(s, ind) AS (
    SELECT sud, instr(sud, '.') FROM input
    UNION ALL
    SELECT
      substr(s, 1, ind-1) || z || substr(s, ind+1),
      instr( substr(s, 1, ind-1) || z || substr(s, ind+1), '.' )
     FROM x, digits AS z
    WHERE ind>0
      AND NOT EXISTS (
            SELECT 1
              FROM digits AS lp
             WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
                OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
                OR z.z = substr(s, (((ind-1)/3) % 3) * 3
                        + ((ind-1)/27) * 27 + lp
                        + ((lp-1) / 3) * 6, 1)
         )
  )
SELECT s FROM x WHERE ind=0;
";

const SOLUTION: &str =
    "534678912672195348198342567859761423426853791713924856961537284287419635345286179";

const SUDOMID3: &str = r"
WITH RECURSIVE
  input(sud) AS (
    VALUES
      ('....754..........8.8.19....3....1.6........34....6817.2.4...6.39......2.53.2.....'),
      ('3.........5.7.3..8....28.7.7......43...........39.41.54..3..8..1...4....968...2..')
  ),
  digits(z, lp) AS (
    VALUES('1', 1)
    UNION ALL SELECT
    CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
  ),
  x(s, ind) AS (
    SELECT sud, instr(sud, '.') FROM input
    UNION ALL
    SELECT
      substr(s, 1, ind-1) || z || substr(s, ind+1),
      instr( substr(s, 1, ind-1) || z || substr(s, ind+1), '.' )
     FROM x, digits AS z
    WHERE ind>0
      AND NOT EXISTS (
            SELECT 1
              FROM digits AS lp
             WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
                OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
                OR z.z = substr(s, (((ind-1)/3) % 3) * 3
                        + ((ind-1)/27) * 27 + lp
                        + ((lp-1) / 3) * 6, 1)
         )
  )
SELECT s FROM x WHERE ind=0;
";

const SUDOMID3_SOLUTIONS: [&str; 2] = [
    "693875412145632798782194356357421869816957234429368175274519683968743521531286947",
    "387419526259763418641528379716285943594631782823974165472396851135842697968157234",
];

/// Stock sqlite3 evaluates 4,632 rows of `x` for this puzzle, so the
/// correlated probe runs tens of thousands of times. A handful of
/// compilations for the CTE bodies and the consumer is expected; anything
/// proportional to the probe count is the GH#419 regression.
const MAX_COMPILATIONS_PER_EXECUTION: u64 = 200;

fn run_stock_sudoku(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    stmt.query_map([], |row| row.get::<_, String>(0))
        .expect("stock query_map")
        .map(|row| row.expect("stock row"))
        .collect()
}

async fn run_fsqlite_sudoku(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(sql)
        .await
        .expect("fsqlite query")
        .into_iter()
        .map(|row| match row.values() {
            [SqliteValue::Text(text)] => text.as_str().to_owned(),
            other => panic!("unexpected sudoku row shape: {other:?}"),
        })
        .collect()
}

fn median_ms(samples: &[Duration]) -> (f64, f64, f64) {
    let mut sorted: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    let median = if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        f64::midpoint(sorted[mid - 1], sorted[mid])
    };
    (median, sorted[0], sorted[sorted.len() - 1])
}

fn proc_status_kib(key: &str) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[test]
fn gh419_sudoku_correlated_not_exists_does_not_compile_per_probe() {
    asupersync::test_utils::run_test(|| async {
        let connection = Connection::open(":memory:").await.expect("open");
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();

        let rows = connection.query(SUDOKU).await.expect("sudoku query");

        let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
        set_hot_path_profile_enabled(false);
        eprintln!("GH#419 Sudoku: {compilations} statement compilations");

        assert_eq!(rows.len(), 1, "exactly one solved grid");
        assert_eq!(rows[0].values(), &[SqliteValue::Text(SOLUTION.into())]);
        assert!(
            compilations <= MAX_COMPILATIONS_PER_EXECUTION,
            "GH#419: {compilations} statement compilations for one Sudoku execution; \
             the correlated NOT EXISTS probe is being compiled once per candidate row"
        );
    });
}

#[test]
fn bd_m2tjm_gh419_sudoku_matches_stock_and_repeats() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock open");
        let fsqlite = Connection::open(":memory:").await.expect("fsqlite open");
        for rep in 1..=2 {
            let expected = run_stock_sudoku(&stock, SUDOKU);
            let actual = run_fsqlite_sudoku(&fsqlite, SUDOKU).await;
            assert_eq!(
                actual, expected,
                "GH#419 sudoku mismatch vs stock sqlite3 on rep {rep}"
            );
            assert_eq!(actual, vec![SOLUTION.to_owned()]);
        }
    });
}

#[test]
fn bd_m2tjm_compiled_scan_exists_and_cte_filter_edge_cases_match_stock() {
    let setup = [
        "CREATE TABLE inner_t(id INTEGER PRIMARY KEY, k INTEGER, t TEXT, nc TEXT COLLATE NOCASE, rt TEXT COLLATE RTRIM, b BLOB)",
        "INSERT INTO inner_t VALUES \
            (1, 1, 'a', 'Alpha', 'hi  ', x'616263'), \
            (2, 2, 'é', 'BETA', 'ok', x'0062'), \
            (3, 0, NULL, NULL, NULL, NULL), \
            (4, 3, 'z', 'gamma', 'end ', x'7a')",
        "CREATE TABLE outer_t(id INTEGER PRIMARY KEY, s TEXT, n INTEGER, z TEXT, b BLOB)",
        "INSERT INTO outer_t VALUES \
            (1, 'aébc', 1, 'é', x'616263'), \
            (2, 'hello' || char(0) || 'world', 6, 'w', x'0062'), \
            (3, 'ALPHA', 9223372036854775807, 'hi', x'7a'), \
            (4, 'xyz', 0, 'z', NULL), \
            (5, NULL, NULL, NULL, NULL)",
    ];
    let queries = [
        // Multibyte UTF-8 substr, negative start/len, and 2-arg substr in compiled scan EXISTS
        "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM inner_t i WHERE i.t = substr(o.s, 2, 1) OR i.t = substr(o.s, -1, 1)) ORDER BY o.id",
        // Embedded NUL terminator in substr
        "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM inner_t i WHERE i.t = substr(o.s, 7, 1) OR i.t = substr(o.s, 1, 1)) ORDER BY o.id",
        // BLOB substr
        "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM inner_t i WHERE i.b = substr(o.b, 1, 2) OR i.b = substr(o.b, 1, 3)) ORDER BY o.id",
        // Integer overflow (i64::MAX + 1 -> REAL), division by zero -> NULL, 3-valued OR/AND
        "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM inner_t i WHERE (o.n / i.k) = 1 OR (o.n + i.k) > 9223372036854775807 OR (o.n IS NULL AND i.k = 0)) ORDER BY o.id",
        // Declared NOCASE and RTRIM collations in compiled scan EXISTS
        "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM inner_t i WHERE i.nc = o.s OR i.rt = o.z) ORDER BY o.id",
        // Fused cross join + leading left-only AND prefix + correlated EXISTS
        "SELECT o.id, i.id FROM outer_t o, inner_t i WHERE o.n > 0 AND o.s IS NOT NULL AND NOT EXISTS (SELECT 1 FROM inner_t j WHERE j.t = substr(o.s, j.id, 1) AND j.id = i.id) ORDER BY o.id, i.id",
        // Empty RHS cross join must not evaluate erroring left filter (or return rows)
        "SELECT o.id FROM outer_t o, (SELECT 1 AS k WHERE 0) e WHERE o.n > 0 ORDER BY o.id",
        // Streaming recursive CTE consumer filter: matching rows also keep generating children
        "WITH RECURSIVE c(n, tag) AS (SELECT 1, 'a' UNION ALL SELECT n+1, tag || 'x' FROM c WHERE n < 6) SELECT n, tag FROM c WHERE n = 3 OR n = 5 ORDER BY n",
        // Streaming recursive CTE consumer filter with alias and aggregate
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 8) SELECT count(*), sum(n) FROM c AS alias WHERE alias.n >= 5",
        // Sibling CTE reading recursive CTE must still see all rows (bypasses consumer filter)
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5), tot(cnt) AS (SELECT count(*) FROM c) SELECT c.n, (SELECT cnt FROM tot) FROM c WHERE c.n = 4",
        // UNION recursive CTE (dedup) with consumer filter
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT CASE WHEN n < 4 THEN n + 1 ELSE 1 END FROM c) SELECT n FROM c WHERE n >= 3 ORDER BY n",
    ];

    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock open");
        let fsqlite = Connection::open(":memory:").await.expect("fsqlite open");
        for sql in setup {
            stock.execute(sql, []).expect("stock setup");
            fsqlite.execute(sql).await.expect("fsqlite setup");
        }
        for sql in queries {
            let mut stmt = stock.prepare(sql).expect("stock prepare");
            let col_count = stmt.column_count();
            let stock_rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut out = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        let v = row.get_ref(i)?;
                        out.push(format!("{v:?}"));
                    }
                    Ok(out)
                })
                .expect("stock query")
                .map(|r| r.expect("stock row"))
                .collect();

            let fsq_rows: Vec<Vec<String>> = fsqlite
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("fsqlite failed on {sql}: {e}"))
                .into_iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(|v| match v {
                            SqliteValue::Null => "Null".to_owned(),
                            SqliteValue::Integer(n) => format!("Integer({n})"),
                            SqliteValue::Float(f) => format!("Real({f:?})"),
                            SqliteValue::Text(t) => format!("Text({:?})", t.as_bytes_direct()),
                            SqliteValue::Blob(b) => format!("Blob({b:?})"),
                        })
                        .collect()
                })
                .collect();

            assert_eq!(fsq_rows, stock_rows, "mismatch vs stock on: {sql}");
        }
    });
}

#[test]
#[ignore = "live release-perf A/B benchmark for bd-m2tjm (GH#419)"]
fn bd_m2tjm_gh419_sudoku_ab_live() {
    asupersync::test_utils::run_test(|| async {
        if let Ok(mode) = std::env::var("BD_M2TJM_PROFILE_ONLY") {
            let conn = Connection::open(":memory:").await.expect("fsqlite open");
            let (sql, expected, reps): (&str, Vec<String>, usize) = match mode.as_str() {
                "fsqlite_sudo" => (SUDOKU, vec![SOLUTION.to_owned()], 20),
                "fsqlite_sudomid3" => (
                    SUDOMID3,
                    SUDOMID3_SOLUTIONS.iter().map(|s| (*s).to_owned()).collect(),
                    2,
                ),
                other => panic!("unknown BD_M2TJM_PROFILE_ONLY={other}"),
            };
            let t0 = Instant::now();
            for _ in 0..reps {
                let got = run_fsqlite_sudoku(&conn, sql).await;
                assert_eq!(got, expected);
            }
            eprintln!(
                "BD_M2TJM_PROFILE_ONLY={mode} reps={reps} total_ms={:.2} vmhwm_kib={:?}",
                t0.elapsed().as_secs_f64() * 1000.0,
                proc_status_kib("VmHWM:"),
            );
            return;
        }

        eprintln!(
            "bd-m2tjm live A/B: bundled stock sqlite3 {}",
            rusqlite::version()
        );

        // 1. Separate connection startup cost (:memory: open).
        let mut stock_open = Vec::with_capacity(50);
        let mut fsqlite_open = Vec::with_capacity(50);
        for i in 0..50 {
            let t1 = Instant::now();
            if i % 2 == 0 {
                let c = rusqlite::Connection::open_in_memory().expect("stock open");
                stock_open.push(t1.elapsed());
                drop(c);

                let t2 = Instant::now();
                let c = Connection::open(":memory:").await.expect("fsqlite open");
                fsqlite_open.push(t2.elapsed());
                drop(c);
            } else {
                let c = Connection::open(":memory:").await.expect("fsqlite open");
                fsqlite_open.push(t1.elapsed());
                drop(c);

                let t2 = Instant::now();
                let c = rusqlite::Connection::open_in_memory().expect("stock open");
                stock_open.push(t2.elapsed());
                drop(c);
            }
        }
        let (s_open_med, s_open_min, s_open_max) = median_ms(&stock_open);
        let (f_open_med, f_open_min, f_open_max) = median_ms(&fsqlite_open);
        eprintln!(
            "startup(:memory:, n=50): fsqlite={f_open_med:.3}ms [{f_open_min:.3}..{f_open_max:.3}] \
             stock={s_open_med:.3}ms [{s_open_min:.3}..{s_open_max:.3}] ratio={:.2}x",
            f_open_med / s_open_med.max(1e-9),
        );

        // 2. sudo.sql: first (cold) vs repeated (warm) on same connection.
        let expected_sudo = vec![SOLUTION.to_owned()];
        let stock_conn = rusqlite::Connection::open_in_memory().expect("stock open");
        let fsqlite_conn = Connection::open(":memory:").await.expect("fsqlite open");

        let t_stock_first = Instant::now();
        let stock_first_rows = run_stock_sudoku(&stock_conn, SUDOKU);
        let stock_first = t_stock_first.elapsed();
        assert_eq!(stock_first_rows, expected_sudo);

        let t_fsq_first = Instant::now();
        let fsq_first_rows = run_fsqlite_sudoku(&fsqlite_conn, SUDOKU).await;
        let fsq_first = t_fsq_first.elapsed();
        assert_eq!(fsq_first_rows, expected_sudo);

        let mut stock_warm = Vec::with_capacity(5);
        let mut fsq_warm = Vec::with_capacity(5);
        for rep in 0..5 {
            let t1 = Instant::now();
            if rep % 2 == 0 {
                assert_eq!(run_stock_sudoku(&stock_conn, SUDOKU), expected_sudo);
                stock_warm.push(t1.elapsed());

                let t2 = Instant::now();
                assert_eq!(run_fsqlite_sudoku(&fsqlite_conn, SUDOKU).await, expected_sudo);
                fsq_warm.push(t2.elapsed());
            } else {
                assert_eq!(run_fsqlite_sudoku(&fsqlite_conn, SUDOKU).await, expected_sudo);
                fsq_warm.push(t1.elapsed());

                let t2 = Instant::now();
                assert_eq!(run_stock_sudoku(&stock_conn, SUDOKU), expected_sudo);
                stock_warm.push(t2.elapsed());
            }
        }
        let (s_med, s_min, s_max) = median_ms(&stock_warm);
        let (f_med, f_min, f_max) = median_ms(&fsq_warm);
        eprintln!(
            "sudo.sql first(cold): fsqlite={:.2}ms stock={:.2}ms ratio={:.2}x",
            fsq_first.as_secs_f64() * 1000.0,
            stock_first.as_secs_f64() * 1000.0,
            fsq_first.as_secs_f64() / stock_first.as_secs_f64().max(1e-9),
        );
        eprintln!(
            "sudo.sql repeated(n=5): fsqlite={f_med:.2}ms [{f_min:.2}..{f_max:.2}] \
             stock={s_med:.2}ms [{s_min:.2}..{s_max:.2}] ratio={:.2}x vmhwm_kib={:?}",
            f_med / s_med.max(1e-9),
            proc_status_kib("VmHWM:"),
        );

        // 3. sudomid3.sql (2 harder GH#419 puzzles): first vs 3 repeated interleaved runs.
        let expected_mid3: Vec<String> = SUDOMID3_SOLUTIONS.iter().map(|s| (*s).to_owned()).collect();
        let stock_mid_conn = rusqlite::Connection::open_in_memory().expect("stock open");
        let fsq_mid_conn = Connection::open(":memory:").await.expect("fsqlite open");

        let t_stock_first = Instant::now();
        let mut stock_mid_rows = run_stock_sudoku(&stock_mid_conn, SUDOMID3);
        let stock_mid_first = t_stock_first.elapsed();
        stock_mid_rows.sort();
        let mut expected_mid3_sorted = expected_mid3.clone();
        expected_mid3_sorted.sort();
        assert_eq!(stock_mid_rows, expected_mid3_sorted);

        let t_fsq_first = Instant::now();
        let mut fsq_mid_rows = run_fsqlite_sudoku(&fsq_mid_conn, SUDOMID3).await;
        let fsq_mid_first = t_fsq_first.elapsed();
        fsq_mid_rows.sort();
        assert_eq!(fsq_mid_rows, expected_mid3_sorted);

        let mut stock_mid_warm = Vec::with_capacity(3);
        let mut fsq_mid_warm = Vec::with_capacity(3);
        for rep in 0..3 {
            let t1 = Instant::now();
            if rep % 2 == 0 {
                let mut rows = run_stock_sudoku(&stock_mid_conn, SUDOMID3);
                stock_mid_warm.push(t1.elapsed());
                rows.sort();
                assert_eq!(rows, expected_mid3_sorted);

                let t2 = Instant::now();
                let mut rows = run_fsqlite_sudoku(&fsq_mid_conn, SUDOMID3).await;
                fsq_mid_warm.push(t2.elapsed());
                rows.sort();
                assert_eq!(rows, expected_mid3_sorted);
            } else {
                let mut rows = run_fsqlite_sudoku(&fsq_mid_conn, SUDOMID3).await;
                fsq_mid_warm.push(t1.elapsed());
                rows.sort();
                assert_eq!(rows, expected_mid3_sorted);

                let t2 = Instant::now();
                let mut rows = run_stock_sudoku(&stock_mid_conn, SUDOMID3);
                stock_mid_warm.push(t2.elapsed());
                rows.sort();
                assert_eq!(rows, expected_mid3_sorted);
            }
        }
        let (sm_med, sm_min, sm_max) = median_ms(&stock_mid_warm);
        let (fm_med, fm_min, fm_max) = median_ms(&fsq_mid_warm);
        eprintln!(
            "sudomid3.sql first(cold): fsqlite={:.2}ms stock={:.2}ms ratio={:.2}x",
            fsq_mid_first.as_secs_f64() * 1000.0,
            stock_mid_first.as_secs_f64() * 1000.0,
            fsq_mid_first.as_secs_f64() / stock_mid_first.as_secs_f64().max(1e-9),
        );
        eprintln!(
            "sudomid3.sql repeated(n=3): fsqlite={fm_med:.2}ms [{fm_min:.2}..{fm_max:.2}] \
             stock={sm_med:.2}ms [{sm_min:.2}..{sm_max:.2}] ratio={:.2}x vmhwm_kib={:?}",
            fm_med / sm_med.max(1e-9),
            proc_status_kib("VmHWM:"),
        );
    });
}
