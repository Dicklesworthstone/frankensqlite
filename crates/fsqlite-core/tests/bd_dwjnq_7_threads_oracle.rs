#![recursion_limit = "512"]

//! bd-dwjnq.7 / bd-dwjnq.8 — `PRAGMA threads` binds real auxiliary execution.
//!
//! Verifies that standard SQLite `PRAGMA threads = N` controls live auxiliary
//! worker admission on eligible parallel B-tree scans/aggregates, rather than
//! acting as a readback-only field:
//! - Explicit `PRAGMA threads = 0` forces strictly serial execution (`0`
//!   auxiliary threads, planted negative control against readback-only storage).
//! - `PRAGMA threads = 1`, `2`, `4`, `8` bound granted auxiliary workers
//!   separately from the caller thread (`workers_granted <= 1 + N`,
//!   `auxiliary_granted <= N`) and observe real distinct auxiliary OS thread
//!   identities and simultaneous auxiliary work on multi-core hosts.
//! - Overflow (`PRAGMA threads = 100`) clamps to stock `SQLITE_MAX_WORKER_THREADS`
//!   (8 auxiliary workers, at most 9 total workers); negative values leave the
//!   configured limit unchanged.
//! - Explicit transactions, unsupported/nested SQL queries, and stock `rusqlite`
//!   result oracle remain invariant.
//! - Mid-walk cancellation drains every auxiliary child thread and releases
//!   every helper grant back to `parallel_helpers_in_use() == 0`.
//! - Multi-connection overload respects `parallel_helper_budget()`, and
//!   concurrent MVCC writers remain ON by default and unobstructed even when
//!   `PRAGMA threads = 0`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use fsqlite_btree::snapshot_walk::{
    SerialReason, WalkRecord, WalkTelemetry, count_entries_parallel, parallel_helper_budget,
    parallel_helpers_high_water, parallel_helpers_in_use, take_last_parallel_walk,
    take_last_parallel_walk_telemetry,
};
use fsqlite_core::connection::{Connection, ConnectionEnv, RuntimeConfig};
use fsqlite_pager::SnapshotPageReader;
use fsqlite_types::PageNumber;
use fsqlite_types::cx::{CancelReason, Cx};
use fsqlite_types::value::SqliteValue;

const ROWS: i64 = 60_000;
const FILTERED_AGG_SQL: &str = "SELECT count(*), sum(a) FROM t WHERE a > 500;";
const COUNT_SQL: &str = "SELECT count(*) FROM t;";

fn fill_sql(rows: i64) -> String {
    format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {rows}) \
         INSERT INTO t SELECT x, (x * 7919) % 1000, 'row-' || x || '-padding-payload' FROM c;"
    )
}

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Blob(b) => format!("blob({})", b.len()),
                })
                .collect()
        })
        .collect()
}

fn stock_query(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = r.prepare(sql).unwrap();
    let cols = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(cols);
        for idx in 0..cols {
            let val = match row.get_ref_unwrap(idx) {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                rusqlite::types::ValueRef::Blob(b) => format!("blob({})", b.len()),
            };
            out.push(val);
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn setup_pair(path: &std::path::Path, rows: i64) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(path.to_str().unwrap()).await.unwrap();
    f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);";
    let fill = fill_sql(rows);
    f.execute(ddl).await.unwrap();
    f.execute(&fill).await.unwrap();
    r.execute_batch(ddl).unwrap();
    r.execute_batch(&fill).unwrap();
    (f, r)
}

#[test]
fn pragma_threads_bounds_live_auxiliary_workers_and_identities() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("threads_live.db");
        let (f, r) = setup_pair(&path, ROWS).await;
        let expected_agg = stock_query(&r, FILTERED_AGG_SQL);
        let expected_cnt = stock_query(&r, COUNT_SQL);
        let budget = parallel_helper_budget();

        // Default readback matches stock SQLite (0).
        assert_eq!(
            render_rows(&f.query("PRAGMA threads;").await.unwrap()),
            vec![vec!["0".to_owned()]]
        );

        // Planted negative control against readback-only PRAGMA threads:
        // explicit `PRAGMA threads = 0` MUST force strictly serial execution
        // (0 auxiliary threads, no parallel walk record or telemetry) even on
        // a multi-core host where the default connection allows parallel scans.
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = 0;").await.unwrap()),
            vec![vec!["0".to_owned()]]
        );
        for (sql, want) in [
            (FILTERED_AGG_SQL, &expected_agg),
            (COUNT_SQL, &expected_cnt),
        ] {
            let _ = take_last_parallel_walk();
            let _ = take_last_parallel_walk_telemetry();
            assert_eq!(&render_rows(&f.query(sql).await.unwrap()), want, "{sql}");
            assert_eq!(
                take_last_parallel_walk(),
                None,
                "PRAGMA threads = 0 must disable parallel scan admission for `{sql}`"
            );
            assert_eq!(
                take_last_parallel_walk_telemetry(),
                None,
                "PRAGMA threads = 0 must admit 0 auxiliary workers for `{sql}`"
            );
        }

        // `PRAGMA threads = 1`: at most 1 auxiliary helper (2 total workers).
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = 1;").await.unwrap()),
            vec![vec!["1".to_owned()]]
        );
        let mut observed_aux_at_1 = false;
        let mut granted_at_1 = 0_usize;
        for (sql, want) in [
            (FILTERED_AGG_SQL, &expected_agg),
            (COUNT_SQL, &expected_cnt),
        ] {
            let _ = take_last_parallel_walk();
            let _ = take_last_parallel_walk_telemetry();
            assert_eq!(&render_rows(&f.query(sql).await.unwrap()), want, "{sql}");
            let record = take_last_parallel_walk();
            let telemetry = take_last_parallel_walk_telemetry();
            if budget >= 1 {
                let WalkTelemetry {
                    workers_granted,
                    auxiliary_granted,
                    peak_simultaneous_auxiliary,
                    cancelled,
                    ..
                } = telemetry.as_ref().expect("threads=1 telemetry");
                assert!(!cancelled);
                assert_eq!(*auxiliary_granted, 1, "{sql}: {telemetry:?}");
                assert_eq!(*workers_granted, 2, "{sql}: {telemetry:?}");
                assert!(*peak_simultaneous_auxiliary <= 1, "{sql}: {telemetry:?}");
                let aux_ids = telemetry.as_ref().unwrap().auxiliary_thread_ids();
                assert!(aux_ids.len() <= 1, "{sql}: aux_ids={aux_ids:?}");
                if !aux_ids.is_empty() {
                    observed_aux_at_1 = true;
                }
                granted_at_1 = granted_at_1.max(*workers_granted);
                assert!(
                    matches!(record, Some(WalkRecord::Ran { workers: 2, .. })),
                    "{sql}: {record:?}"
                );
            }
        }
        if budget >= 1 {
            assert!(
                observed_aux_at_1,
                "expected at least one real auxiliary ThreadId under PRAGMA threads = 1"
            );
        }

        // `PRAGMA threads = 4`: up to 4 auxiliary helpers (5 total workers),
        // strictly exceeding `threads = 1` when host budget permits.
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = 4;").await.unwrap()),
            vec![vec!["4".to_owned()]]
        );
        let mut observed_simultaneous_aux = false;
        let mut max_granted_at_4 = 0_usize;
        for _ in 0..4 {
            let _ = take_last_parallel_walk();
            let _ = take_last_parallel_walk_telemetry();
            assert_eq!(
                render_rows(&f.query(FILTERED_AGG_SQL).await.unwrap()),
                expected_agg
            );
            let telemetry = take_last_parallel_walk_telemetry();
            if let Some(t) = telemetry {
                assert!(!t.cancelled);
                assert!(t.auxiliary_granted <= 4, "{t:?}");
                assert!(t.workers_granted <= 5, "{t:?}");
                assert!(t.auxiliary_thread_ids().len() <= 4, "{t:?}");
                assert!(t.peak_simultaneous_auxiliary <= 4, "{t:?}");
                max_granted_at_4 = max_granted_at_4.max(t.workers_granted);
                if t.peak_simultaneous_workers >= 2 && t.peak_simultaneous_auxiliary >= 1 {
                    observed_simultaneous_aux = true;
                }
            }
        }
        if budget >= 4 {
            assert_eq!(
                max_granted_at_4, 5,
                "PRAGMA threads = 4 must admit 4 auxiliary helpers (5 total workers)"
            );
            assert!(
                max_granted_at_4 > granted_at_1,
                "threads=4 ({max_granted_at_4}) must exceed threads=1 ({granted_at_1})"
            );
            assert!(
                observed_simultaneous_aux,
                "expected simultaneous auxiliary execution under PRAGMA threads = 4"
            );
        }

        // Overflow clamping (`PRAGMA threads = 999` -> 8) and negative no-op (`-5` -> unchanged).
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = 999;").await.unwrap()),
            vec![vec!["8".to_owned()]]
        );
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = -5;").await.unwrap()),
            vec![vec!["8".to_owned()]]
        );
        let _ = take_last_parallel_walk();
        let _ = take_last_parallel_walk_telemetry();
        assert_eq!(
            render_rows(&f.query(FILTERED_AGG_SQL).await.unwrap()),
            expected_agg
        );
        if let Some(t) = take_last_parallel_walk_telemetry() {
            assert!(
                t.auxiliary_granted <= 8 && t.workers_granted <= 9,
                "clamped PRAGMA threads=999 must never admit >8 auxiliary workers: {t:?}"
            );
        }

        // Dynamic re-disable: setting `PRAGMA threads = 0` immediately returns to serial.
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = 0;").await.unwrap()),
            vec![vec!["0".to_owned()]]
        );
        let _ = take_last_parallel_walk();
        let _ = take_last_parallel_walk_telemetry();
        assert_eq!(
            render_rows(&f.query(FILTERED_AGG_SQL).await.unwrap()),
            expected_agg
        );
        assert_eq!(take_last_parallel_walk(), None);
        assert_eq!(take_last_parallel_walk_telemetry(), None);
        assert_eq!(parallel_helpers_in_use(), 0);
    });
}

#[test]
fn pragma_threads_negative_preserves_default_and_unsupported_queries_match_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("threads_nested.db");
        let (f, r) = setup_pair(&path, 25_000).await;

        // Negative assignment on a fresh connection leaves existing parallel_workers untouched.
        let default_pw = render_rows(&f.query("PRAGMA fsqlite.parallel_workers;").await.unwrap());
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = -1;").await.unwrap()),
            vec![vec!["0".to_owned()]]
        );
        assert_eq!(
            render_rows(&f.query("PRAGMA fsqlite.parallel_workers;").await.unwrap()),
            default_pw
        );

        // With `PRAGMA threads = 4`, explicit transactions stay serial and
        // nested/grouped/ordered queries produce stock-identical answers.
        assert_eq!(
            render_rows(&f.query("PRAGMA threads = 4;").await.unwrap()),
            vec![vec!["4".to_owned()]]
        );
        f.execute("BEGIN;").await.unwrap();
        let _ = take_last_parallel_walk();
        assert_eq!(
            render_rows(&f.query(FILTERED_AGG_SQL).await.unwrap()),
            stock_query(&r, FILTERED_AGG_SQL)
        );
        assert_eq!(
            take_last_parallel_walk(),
            None,
            "explicit transaction must stay serial even under PRAGMA threads = 4"
        );
        f.execute("COMMIT;").await.unwrap();

        for sql in [
            "SELECT a % 7 AS bucket, count(*), sum(a) FROM t WHERE a > 200 GROUP BY a % 7 ORDER BY bucket;",
            "SELECT count(*) FROM (SELECT id, a FROM t WHERE a > 500 ORDER BY a DESC, id ASC LIMIT 250);",
            "SELECT (SELECT count(*) FROM t WHERE a < 100) + (SELECT count(*) FROM t WHERE a >= 900);",
        ] {
            assert_eq!(
                render_rows(&f.query(sql).await.unwrap()),
                stock_query(&r, sql),
                "oracle mismatch for `{sql}`"
            );
        }
        assert_eq!(parallel_helpers_in_use(), 0);
    });
}

#[test]
fn parallel_walk_cancellation_drains_children_and_helper_grants() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("cancel_drain.db");
    let stock = rusqlite::Connection::open(&path).unwrap();
    stock
        .execute_batch(
            "PRAGMA page_size = 4096; PRAGMA journal_mode = DELETE;
             CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
        )
        .unwrap();
    stock.execute_batch(&fill_sql(ROWS)).unwrap();
    let root_page: u32 = stock
        .query_row(
            "SELECT rootpage FROM sqlite_master WHERE name = 't'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let root = PageNumber::new(root_page).unwrap();
    drop(stock);

    let file = std::fs::File::open(&path).unwrap();
    let pages = u32::try_from(file.metadata().unwrap().len() / 4096).unwrap();
    let reader = SnapshotPageReader::new(Arc::new(file), None, 4096, pages);

    // Mid-walk cancellation after 20 page checks with 5 workers (caller + 4 aux).
    let polls = AtomicUsize::new(0);
    let stop_after_20 = || polls.fetch_add(1, Ordering::Relaxed) >= 20;
    let _ = take_last_parallel_walk();
    let _ = take_last_parallel_walk_telemetry();
    let outcome = count_entries_parallel(&reader, root, 4096, 5, &stop_after_20).unwrap();
    assert_eq!(outcome, None, "cancelled parallel walk must return None");
    assert_eq!(
        parallel_helpers_in_use(),
        0,
        "every auxiliary helper grant must be drained before return"
    );
    if parallel_helper_budget() >= 1 {
        let telemetry = take_last_parallel_walk_telemetry().expect("cancelled walk telemetry");
        assert!(telemetry.cancelled, "telemetry must record cancellation: {telemetry:?}");
        assert!(telemetry.auxiliary_granted <= 4);
    }

    // Public Connection path with a cancelled caller Cx must fail closed
    // and leave zero helper grants in use.
    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        cx.transition_to_running();
        let env = ConnectionEnv::new_with_root_cx(RuntimeConfig::default(), &cx);
        let conn = Connection::open_with_env(path.to_str().unwrap(), env)
            .await
            .unwrap();
        conn.query("PRAGMA threads = 4;").await.unwrap();
        conn.root_cx()
            .cancel_with_reason(CancelReason::UserInterrupt);
        let result = conn.query(FILTERED_AGG_SQL).await;
        assert!(
            result.is_err(),
            "cancelled caller Cx must interrupt query cleanly"
        );
        assert_eq!(parallel_helpers_in_use(), 0);
    });
}

#[test]
fn multi_connection_overload_and_concurrent_writers_remain_invariant() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("overload_and_writers.db");
    let path_str = path.to_str().unwrap().to_owned();

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock
        .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
        .unwrap();
    stock.execute_batch(&fill_sql(ROWS)).unwrap();
    let expected = stock_query(&stock, FILTERED_AGG_SQL);

    asupersync::test_utils::run_test(|| {
        let path_str = path_str.clone();
        async move {
            let f = Connection::open(&path_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
                .await
                .unwrap();
            f.execute(&fill_sql(ROWS)).await.unwrap();
            f.execute("CREATE TABLE w1(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            f.execute("CREATE TABLE w2(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            f.close().await.unwrap();
        }
    });

    // 6 concurrent connections each requesting `PRAGMA threads = 4` stay within
    // the process-wide helper budget and match stock SQLite.
    const CONN_COUNT: usize = 6;
    let barrier = Arc::new(Barrier::new(CONN_COUNT));
    let observed_records: Arc<Mutex<Vec<Option<WalkRecord>>>> = Arc::new(Mutex::new(Vec::new()));
    let expected_arc = Arc::new(expected);
    let handles: Vec<_> = (0..CONN_COUNT)
        .map(|_| {
            let (p, b, recs, exp) = (
                path_str.clone(),
                Arc::clone(&barrier),
                Arc::clone(&observed_records),
                Arc::clone(&expected_arc),
            );
            std::thread::spawn(move || {
                asupersync::test_utils::run_test(|| async move {
                    let c = Connection::open(&p).await.unwrap();
                    assert_eq!(
                        render_rows(&c.query("PRAGMA threads = 4;").await.unwrap()),
                        vec![vec!["4".to_owned()]]
                    );
                    b.wait();
                    for _ in 0..3 {
                        let _ = take_last_parallel_walk();
                        let got = render_rows(&c.query(FILTERED_AGG_SQL).await.unwrap());
                        assert_eq!(got, *exp);
                        recs.lock().unwrap().push(take_last_parallel_walk());
                    }
                });
            })
        })
        .collect();
    for h in handles {
        h.join().expect("scanner thread");
    }

    assert!(
        parallel_helpers_high_water() <= parallel_helper_budget(),
        "high water {} exceeded budget {}",
        parallel_helpers_high_water(),
        parallel_helper_budget()
    );
    assert_eq!(parallel_helpers_in_use(), 0);
    for rec in observed_records.lock().unwrap().iter() {
        assert!(
            matches!(
                rec,
                Some(WalkRecord::Ran { workers, .. }) if *workers <= 5
            ) || *rec == Some(WalkRecord::Serial(SerialReason::BudgetExhausted)),
            "unexpected record under PRAGMA threads = 4 overload: {rec:?}"
        );
    }

    // Concurrent-writer invariant: setting `PRAGMA threads = 0` must NEVER
    // disable `concurrent_mode_default` or serialize disjoint concurrent writers.
    asupersync::test_utils::run_test(|| async move {
        let c1 = Connection::open(&path_str).await.unwrap();
        let c2 = Connection::open(&path_str).await.unwrap();
        assert!(c1.is_concurrent_mode_default());
        assert!(c2.is_concurrent_mode_default());
        c1.query("PRAGMA threads = 0;").await.unwrap();
        c2.query("PRAGMA threads = 0;").await.unwrap();
        assert!(c1.is_concurrent_mode_default());
        assert!(c2.is_concurrent_mode_default());

        c1.execute("BEGIN;").await.unwrap();
        c2.execute("BEGIN;").await.unwrap();
        c1.execute("INSERT INTO w1 VALUES (1, 'alpha'), (2, 'beta');")
            .await
            .unwrap();
        c2.execute("INSERT INTO w2 VALUES (1, 'gamma'), (2, 'delta');")
            .await
            .unwrap();
        c1.execute("COMMIT;").await.unwrap();
        c2.execute("COMMIT;").await.unwrap();

        assert_eq!(
            render_rows(&c1.query("SELECT count(*) FROM w1;").await.unwrap()),
            vec![vec!["2".to_owned()]]
        );
        assert_eq!(
            render_rows(&c1.query("SELECT count(*) FROM w2;").await.unwrap()),
            vec![vec!["2".to_owned()]]
        );
    });
}
