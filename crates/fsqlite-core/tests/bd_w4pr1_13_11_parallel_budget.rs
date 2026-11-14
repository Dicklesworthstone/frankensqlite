#![recursion_limit = "512"]

//! bd-w4pr1.13.11 — the process-wide helper budget for parallel scans.
//!
//! Eight connections in one process, each allowing 64 workers, scan the same
//! table at once. A 300,000-row table is worth about 50 threads per scan, so
//! together they ask for several hundred helper threads. The most helpers
//! granted at any moment must stay within the budget (the machine's available
//! parallelism); a statement that finds the budget used up runs serially
//! instead of waiting; and every answer equals stock SQLite's.
//!
//! This is its own test binary because the budget and its high-water mark
//! are process-wide: tests elsewhere would share them.

use std::sync::{Arc, Barrier, Mutex};

use fsqlite_btree::snapshot_walk::{
    SerialReason, WalkRecord, parallel_helper_budget, parallel_helpers_high_water,
    take_last_parallel_walk,
};
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const ROWS: i64 = 300_000;
const CONNECTIONS: usize = 8;
const ROUNDS: usize = 6;
const QUERIES: [&str; 2] = [
    "SELECT count(*) FROM t;",
    "SELECT count(*), sum(a) FROM t WHERE a > 500;",
];

fn fill_sql() -> String {
    format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROWS}) \
         INSERT INTO t SELECT x, (x * 7919) % 1000, 'row-' || x FROM c;"
    )
}

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Integer(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

#[test]
fn eight_connections_scanning_at_once_stay_within_the_helper_budget() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("budget.db").to_str().unwrap().to_owned();

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock
        .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
        .unwrap();
    stock.execute_batch(&fill_sql()).unwrap();
    let expected: Vec<Vec<String>> = QUERIES
        .iter()
        .map(|sql| {
            let mut st = stock.prepare(sql).unwrap();
            let n = st.column_count();
            st.query_row([], |row| {
                Ok((0..n)
                    .map(|i| row.get::<_, i64>(i).unwrap().to_string())
                    .collect())
            })
            .unwrap()
        })
        .collect();

    asupersync::test_utils::run_test(|| {
        let path = path.clone();
        async move {
            let f = Connection::open(&path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
                .await
                .unwrap();
            f.execute(&fill_sql()).await.unwrap();
        }
    });

    let barrier = Arc::new(Barrier::new(CONNECTIONS));
    let records: Arc<Mutex<Vec<Option<WalkRecord>>>> = Arc::new(Mutex::new(Vec::new()));
    let expected = Arc::new(expected);
    let scanners: Vec<_> = (0..CONNECTIONS)
        .map(|_| {
            let (path, barrier, records, expected) = (
                path.clone(),
                Arc::clone(&barrier),
                Arc::clone(&records),
                Arc::clone(&expected),
            );
            std::thread::spawn(move || {
                asupersync::test_utils::run_test(|| {
                    let (path, barrier, records, expected) = (
                        path.clone(),
                        Arc::clone(&barrier),
                        Arc::clone(&records),
                        Arc::clone(&expected),
                    );
                    async move {
                        let c = Connection::open(&path).await.unwrap();
                        c.query("PRAGMA fsqlite.parallel_workers = 64;")
                            .await
                            .unwrap();
                        barrier.wait();
                        for _ in 0..ROUNDS {
                            for (sql, want) in QUERIES.iter().zip(expected.iter()) {
                                let _ = take_last_parallel_walk();
                                let rows = c.query(sql).await.unwrap();
                                let got: Vec<String> =
                                    rows[0].values().iter().map(render).collect();
                                assert_eq!(&got, want, "`{sql}`");
                                records.lock().unwrap().push(take_last_parallel_walk());
                            }
                        }
                    }
                });
            })
        })
        .collect();
    for scanner in scanners {
        scanner.join().expect("scanner thread");
    }

    let budget = parallel_helper_budget();
    let high_water = parallel_helpers_high_water();
    assert!(
        high_water <= budget,
        "{high_water} helpers granted at once, budget {budget}"
    );
    assert!(high_water > 0, "no scan got a helper thread");
    let records = records.lock().unwrap();
    assert_eq!(records.len(), CONNECTIONS * ROUNDS * QUERIES.len());
    for record in records.iter() {
        assert!(
            matches!(
                record,
                Some(WalkRecord::Ran { .. } | WalkRecord::Serial(SerialReason::BudgetExhausted))
            ),
            "every scan took the parallel path or found the budget used up: {record:?}"
        );
    }
    // Eight scans each worth about 50 threads ask for far more helpers than
    // any budget below that: some scan ran with fewer workers than it asked
    // for, or serially.
    if budget < CONNECTIONS * 40 {
        assert!(
            records.iter().any(|record| match record {
                Some(WalkRecord::Ran { workers, .. }) => *workers < 40,
                Some(WalkRecord::Serial(SerialReason::BudgetExhausted)) => true,
                _ => false,
            }),
            "no scan was held to the budget: {records:?}"
        );
    }
}
