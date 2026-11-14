#![recursion_limit = "512"]

//! bd-w4pr1.13.11 — scans of one committed snapshot on several threads,
//! checked against stock SQLite (rusqlite) on the same data.
//!
//! `PRAGMA fsqlite.parallel_workers = N` lets an autocommit read of a
//! file-backed WAL database read its snapshot through a
//! `fsqlite_pager::SnapshotPageReader` on N threads. Every answer here must
//! equal stock's at every N, and N = 1 must take the serial path (no
//! parallel walk recorded on the thread). The walk's own record of the
//! threads that did work is the evidence that a parallel plan ran.
//!
//! Two operators use it: `count(*)` of a table or index b-tree (the `Count`
//! opcode), and ungrouped `count` / `sum` / `min` / `max` over a filtered
//! rowid-table scan (a plan codegen attaches to the program). The aggregate
//! tests cover what a naive parallel aggregate gets wrong: SQLite's mixed-type
//! comparison and coercion rules, an integer `sum` that overflows only in
//! rowid order (and one that only rowid order keeps in range), the rowid
//! alias, rows ALTER TABLE left short, payloads on overflow pages, and the
//! shapes that must stay serial.

use fsqlite_btree::snapshot_walk::{SerialReason, WalkRecord, take_last_parallel_walk};
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const WORKERS: [usize; 4] = [1, 2, 4, 8];

/// Whether the statement took the parallel path: it walked, or found the
/// process's helper budget used up by the other tests in this binary and
/// ran serially for that reason only.
fn took_parallel_path(record: Option<WalkRecord>) -> bool {
    matches!(
        record,
        Some(WalkRecord::Ran { .. } | WalkRecord::Serial(SerialReason::BudgetExhausted))
    )
}

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{}", b.len()),
    }
}

async fn frank(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite `{sql}`: {e}"))
        .iter()
        .map(|r| r.values().iter().map(render).collect())
        .collect()
}

fn stock(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = c.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!("x{}", b.len()),
            })
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite `{sql}`: {e}"));
    r.execute_batch(sql).unwrap();
}

async fn set_workers(f: &Connection, workers: usize) {
    let echoed = frank(f, &format!("PRAGMA fsqlite.parallel_workers = {workers};")).await;
    assert_eq!(echoed, vec![vec![workers.to_string()]]);
}

/// Rows `1..=n` of `(id, a, b)`: `a` cycles through 1,000 values, `b` is a
/// short text, so a 4 KiB page holds a little over a hundred rows.
fn fill(table: &str, n: i64) -> String {
    format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) \
         INSERT INTO {table} SELECT x, (x * 7919) % 1000, 'row-' || x FROM c;"
    )
}

#[test]
fn parallel_count_star_matches_stock_on_table_and_index_btrees() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("count.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE big(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
            "CREATE TABLE ix(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
            "CREATE INDEX ix_a ON ix(a);",
            "CREATE TABLE tiny(x);",
            "INSERT INTO tiny VALUES (1), (2), (3);",
            "CREATE TABLE empty(x);",
        ] {
            both(&f, &r, sql).await;
        }
        both(&f, &r, &fill("big", 60_000)).await;
        both(&f, &r, &fill("ix", 40_000)).await;
        // Rows below the split frontier and above it: delete a run so the
        // leaves are uneven.
        both(&f, &r, "DELETE FROM big WHERE id BETWEEN 20000 AND 26000;").await;

        for workers in WORKERS {
            set_workers(&f, workers).await;
            // `big` is a multi-level table b-tree; `ix` counts through its
            // index, whose interior cells are entries too (a count of leaf
            // cells alone would be short); `tiny` and `empty` are one leaf,
            // too small for a second thread.
            for (sql, multi_level) in [
                ("SELECT count(*) FROM big;", true),
                ("SELECT count(*) FROM ix;", true),
                ("SELECT count(*) FROM tiny;", false),
                ("SELECT count(*) FROM empty;", false),
            ] {
                let _ = take_last_parallel_walk();
                assert_eq!(frank(&f, sql).await, stock(&r, sql), "workers={workers} `{sql}`");
                let record = take_last_parallel_walk();
                if workers == 1 {
                    assert_eq!(record, None, "workers=1 must stay serial: `{sql}`");
                } else if multi_level {
                    assert!(
                        took_parallel_path(record),
                        "workers={workers} `{sql}`: walk {record:?}"
                    );
                } else {
                    assert_eq!(
                        record,
                        Some(WalkRecord::Serial(SerialReason::SmallTree)),
                        "workers={workers} `{sql}`"
                    );
                }
            }
        }
    });
}

#[test]
fn parallel_count_stays_serial_inside_a_transaction_and_sees_own_writes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("txn.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        both(&f, &r, "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);").await;
        both(&f, &r, &fill("t", 30_000)).await;
        set_workers(&f, 8).await;
        both(&f, &r, "BEGIN;").await;
        both(&f, &r, "DELETE FROM t WHERE id % 3 = 0;").await;
        let _ = take_last_parallel_walk();
        let sql = "SELECT count(*) FROM t;";
        assert_eq!(frank(&f, sql).await, stock(&r, sql));
        assert_eq!(take_last_parallel_walk(), None, "explicit transaction ran parallel");
        both(&f, &r, "COMMIT;").await;
        assert_eq!(frank(&f, sql).await, stock(&r, sql));
        assert!(took_parallel_path(take_last_parallel_walk()));
    });
}

#[test]
fn parallel_count_reads_one_committed_snapshot_while_a_writer_commits() {
    const BASE: i64 = 30_000;
    const BATCH: i64 = 500;
    const BATCHES: i64 = 40;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("race.db");
    let path = path.to_str().unwrap().to_owned();
    asupersync::test_utils::run_test(|| {
        let path = path.clone();
        async move {
            let f = Connection::open(&path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
                .await
                .unwrap();
            f.execute(&fill("t", BASE)).await.unwrap();
        }
    });
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        asupersync::test_utils::run_test(|| {
            let path = writer_path.clone();
            async move {
                let w = Connection::open(&path).await.unwrap();
                for batch in 0..BATCHES {
                    let first = BASE + batch * BATCH + 1;
                    let last = first + BATCH - 1;
                    w.execute("BEGIN;").await.unwrap();
                    w.execute(&format!(
                        "WITH RECURSIVE c(x) AS (SELECT {first} UNION ALL SELECT x + 1 FROM c \
                         WHERE x < {last}) INSERT INTO t SELECT x, x % 1000, 'w-' || x FROM c;"
                    ))
                    .await
                    .unwrap();
                    w.execute("COMMIT;").await.unwrap();
                }
            }
        });
    });
    asupersync::test_utils::run_test(|| {
        let path = path.clone();
        async move {
            let f = Connection::open(&path).await.unwrap();
            set_workers(&f, 4).await;
            let mut previous = BASE;
            let mut parallel_runs = 0_usize;
            for _ in 0..60 {
                let rows = f.query("SELECT count(*) FROM t;").await.unwrap();
                let SqliteValue::Integer(count) = rows[0].values()[0] else {
                    panic!("count is not an integer");
                };
                // Every answer is one committed state: the base plus whole
                // batches, never fewer than an earlier answer.
                assert_eq!((count - BASE) % BATCH, 0, "partial batch visible: {count}");
                assert!(count >= previous, "count went back from {previous} to {count}");
                assert!(count <= BASE + BATCH * BATCHES);
                previous = count;
                if matches!(take_last_parallel_walk(), Some(WalkRecord::Ran { .. })) {
                    parallel_runs += 1;
                }
            }
            assert!(parallel_runs > 0, "no count ran in parallel");
        }
    });
    writer.join().expect("writer thread");
}

/// The reader and the walk on a file stock SQLite wrote (rollback journal,
/// so every page is in the database file): counts on every thread count
/// equal stock's, and a `stop` that fires mid-walk ends it with no answer.
#[test]
fn snapshot_walk_counts_a_stock_written_file_and_stops_when_asked() {
    use fsqlite_btree::snapshot_walk::{count_entries_parallel, split_subtrees};
    use fsqlite_pager::SnapshotPageReader;
    use fsqlite_types::PageNumber;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stock.db");
    let r = rusqlite::Connection::open(&path).unwrap();
    r.execute_batch(
        "PRAGMA page_size = 4096; PRAGMA journal_mode = DELETE;
         CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);
         CREATE INDEX t_b ON t(b);",
    )
    .unwrap();
    r.execute_batch(&fill("t", 50_000)).unwrap();
    let rows: i64 = r.query_row("SELECT count(*) FROM t", [], |row| row.get(0)).unwrap();
    let root = |name: &str| -> PageNumber {
        let page: u32 = r
            .query_row("SELECT rootpage FROM sqlite_master WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .unwrap();
        PageNumber::new(page).unwrap()
    };
    let (table_root, index_root) = (root("t"), root("t_b"));
    let file = std::fs::File::open(&path).unwrap();
    let pages = u32::try_from(file.metadata().unwrap().len() / 4096).unwrap();
    let reader = SnapshotPageReader::new(Arc::new(file), None, 4096, pages);
    let never = || false;

    // A multi-level split covers the tree with more than one subtree and
    // counts enough leaves for 8 threads; one worker is the serial path.
    let split = split_subtrees(&reader, table_root, 4096, 32).unwrap();
    assert!(split.subtrees.len() >= 32, "split {:?}", split.subtrees.len());
    // 50,000 rows of about 20 bytes: some 250 leaves, one thread per 32.
    assert!((200..400).contains(&split.leaf_pages), "leaf pages {}", split.leaf_pages);
    let by_size = usize::try_from(split.leaf_pages / 32).unwrap();
    assert_eq!(
        (split.threads_for(1), split.threads_for(4), split.threads_for(64)),
        (1, 4, by_size)
    );
    for workers in [1, 2, 4, 8] {
        for root in [table_root, index_root] {
            let counted = count_entries_parallel(&reader, root, 4096, workers, &never).unwrap();
            let record = take_last_parallel_walk();
            let at = format!("workers={workers} root={}", root.get());
            match counted {
                Some(n) => assert_eq!(n, rows, "{at}"),
                None if workers == 1 => assert!(
                    matches!(record, Some(WalkRecord::Serial(_))),
                    "{at}: {record:?}"
                ),
                None => assert_eq!(
                    record,
                    Some(WalkRecord::Serial(SerialReason::BudgetExhausted)),
                    "{at}"
                ),
            }
        }
    }
    // Asked to stop before the first page: no answer.
    assert_eq!(count_entries_parallel(&reader, table_root, 4096, 4, &|| true).unwrap(), None);
    // Asked to stop after 50 pages of a tree of hundreds: no answer either.
    let polls = AtomicUsize::new(0);
    let later = || polls.fetch_add(1, Ordering::Relaxed) >= 50;
    assert_eq!(count_entries_parallel(&reader, table_root, 4096, 4, &later).unwrap(), None);
    // A page past the snapshot's end is an error, not a short count.
    let short = SnapshotPageReader::new(
        Arc::new(std::fs::File::open(&path).unwrap()),
        None,
        4096,
        pages / 2,
    );
    match count_entries_parallel(&short, table_root, 4096, 4, &never) {
        Err(_) => {}
        Ok(None) => assert_eq!(
            take_last_parallel_walk(),
            Some(WalkRecord::Serial(SerialReason::BudgetExhausted))
        ),
        Ok(Some(count)) => panic!("counted {count} rows of a truncated snapshot"),
    }
}

/// bd-w4pr1.13.11 sizing: a tree of one interior level over a few leaves
/// (2,000 rows) is not worth a second thread, so even with 8 workers asked
/// for, count(*) and the filtered aggregate take the serial path; a tree a
/// few times larger gets threads in proportion to its leaves.
#[test]
fn small_trees_stay_serial_and_larger_ones_get_threads_by_leaf_pages() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sizes.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE small(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
            "CREATE TABLE medium(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
        ] {
            both(&f, &r, sql).await;
        }
        both(&f, &r, &fill("small", 2_000)).await;
        both(&f, &r, &fill("medium", 20_000)).await;
        set_workers(&f, 8).await;
        for sql in [
            "SELECT count(*) FROM small;",
            "SELECT count(*), sum(a) FROM small WHERE a > 500;",
        ] {
            let _ = take_last_parallel_walk();
            assert_eq!(frank(&f, sql).await, stock(&r, sql), "`{sql}`");
            assert_eq!(
                take_last_parallel_walk(),
                Some(WalkRecord::Serial(SerialReason::SmallTree)),
                "`{sql}`"
            );
        }
        for sql in [
            "SELECT count(*) FROM medium;",
            "SELECT count(*), sum(a) FROM medium WHERE a > 500;",
        ] {
            let _ = take_last_parallel_walk();
            assert_eq!(frank(&f, sql).await, stock(&r, sql), "`{sql}`");
            let record = take_last_parallel_walk();
            // About 150 leaves: more than one thread, fewer than 8.
            assert!(
                matches!(record, Some(WalkRecord::Ran { workers, .. }) if (2..8).contains(&workers))
                    || record == Some(WalkRecord::Serial(SerialReason::BudgetExhausted)),
                "`{sql}`: {record:?}"
            );
        }
    });
}

/// bd-w4pr1.13.11 default: without the pragma a connection allows
/// min(available parallelism, 8) workers, and an explicit transaction still
/// reads serially (and sees its own writes).
#[test]
fn the_default_worker_count_and_explicit_transactions() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("default.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let expected = std::thread::available_parallelism().map_or(1, usize::from).min(8);
        assert_eq!(
            frank(&f, "PRAGMA fsqlite.parallel_workers;").await,
            vec![vec![expected.to_string()]]
        );
        both(&f, &r, "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);").await;
        both(&f, &r, &fill("t", 30_000)).await;
        let sql = "SELECT count(*), sum(a) FROM t WHERE a > 500;";
        let _ = take_last_parallel_walk();
        assert_eq!(frank(&f, sql).await, stock(&r, sql));
        let record = take_last_parallel_walk();
        if expected > 1 {
            assert!(took_parallel_path(record), "{record:?}");
        } else {
            assert_eq!(record, None);
        }
        both(&f, &r, "BEGIN;").await;
        both(&f, &r, "DELETE FROM t WHERE id % 4 = 0;").await;
        for sql in [sql, "SELECT count(*) FROM t;"] {
            let _ = take_last_parallel_walk();
            assert_eq!(frank(&f, sql).await, stock(&r, sql), "`{sql}`");
            assert_eq!(take_last_parallel_walk(), None, "transaction ran `{sql}` in parallel");
        }
        both(&f, &r, "COMMIT;").await;
    });
}

/// Run `sql` on fsqlite at every worker count and on stock; the answers
/// must be equal. Returns what the last parallel run's walk did.
async fn same_at_every_worker_count(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
) -> Option<WalkRecord> {
    let expected = stock_result(r, sql);
    let mut record = None;
    for workers in WORKERS {
        set_workers(f, workers).await;
        let _ = take_last_parallel_walk();
        assert_eq!(frank_result(f, sql).await, expected, "workers={workers} `{sql}`");
        record = take_last_parallel_walk();
        if workers == 1 {
            assert_eq!(record, None, "workers=1 must stay serial: `{sql}`");
        }
    }
    record
}

/// An answer or the error message, so a raised error compares as well.
async fn frank_result(c: &Connection, sql: &str) -> std::result::Result<Vec<Vec<String>>, String> {
    match c.query(sql).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|r| r.values().iter().map(render).collect())
            .collect()),
        Err(e) => Err(e.to_string()),
    }
}

fn stock_result(c: &rusqlite::Connection, sql: &str) -> std::result::Result<Vec<Vec<String>>, String> {
    let mut st = c.prepare(sql).unwrap();
    let n = st.column_count();
    let rows = st.query_map([], |row| {
        Ok((0..n)
            .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!("x{}", b.len()),
            })
            .collect::<Vec<_>>())
    });
    match rows.and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>()) {
        Ok(rows) => Ok(rows),
        Err(e) => Err(e.to_string()),
    }
}

async fn file_pair(dir: &tempfile::TempDir, name: &str) -> (Connection, rusqlite::Connection) {
    let path = dir.path().join(name);
    let f = Connection::open(path.to_str().unwrap()).await.unwrap();
    f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
    (f, rusqlite::Connection::open_in_memory().unwrap())
}

#[test]
fn parallel_aggregates_match_stock_on_the_scorecard_shape() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (f, r) = file_pair(&dir, "shape.db").await;
        both(&f, &r, "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL);").await;
        both(
            &f,
            &r,
            "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM s WHERE x < 80000) \
             INSERT INTO t SELECT x, (x * 7919) % 1000, 'row-' || x, x * 1.5 FROM s;",
        )
        .await;
        for sql in [
            "SELECT count(*), sum(a) FROM t WHERE a > 500;",
            "SELECT count(*), sum(a), min(a), max(a), count(b) FROM t;",
            "SELECT sum(id), min(id), max(id), count(*) FROM t WHERE a > 100;",
            "SELECT count(*) FROM t WHERE a = 7 OR a = 993;",
            "SELECT min(b), max(b), count(*) FROM t WHERE b > 'row-5' AND a < 100;",
            "SELECT max(c), min(c), count(c) FROM t WHERE c >= 12.5 AND c < 9000;",
            "SELECT sum(a) FROM t WHERE a > 5000;",
            "SELECT count(*) FROM t WHERE a IS NULL;",
            "SELECT count(a) FROM t WHERE b IS NOT NULL AND 500 < a;",
        ] {
            let threads = same_at_every_worker_count(&f, &r, sql).await;
            assert!(took_parallel_path(threads), "`{sql}` did not run in parallel: {threads:?}");
        }
        // A REAL sum depends on addition order: its walk declines at the
        // first row and the serial loop answers.
        same_at_every_worker_count(&f, &r, "SELECT sum(c) FROM t WHERE a > 500;").await;
        // No plan: order-dependent aggregates, and a rowid range, which
        // seeks instead of scanning.
        for sql in [
            "SELECT avg(a) FROM t;",
            "SELECT total(a) FROM t;",
            "SELECT sum(id), min(id), max(id) FROM t WHERE id > 1000 AND id <= 70000;",
        ] {
            let threads = same_at_every_worker_count(&f, &r, sql).await;
            assert_eq!(threads, None, "`{sql}`");
        }
    });
}

#[test]
fn parallel_aggregates_follow_sqlite_type_and_comparison_rules() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (f, r) = file_pair(&dir, "types.db").await;
        // `v` has no affinity, `n` INTEGER, `s` TEXT, `x` REAL: every storage
        // class in each, so comparisons cross classes and coerce by affinity.
        both(&f, &r, "CREATE TABLE m(id INTEGER PRIMARY KEY, v, n INTEGER, s TEXT, x REAL);").await;
        both(
            &f,
            &r,
            "WITH RECURSIVE q(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM q WHERE i < 30000) \
             INSERT INTO m SELECT i, \
               CASE i % 6 WHEN 0 THEN i WHEN 1 THEN i * 0.5 WHEN 2 THEN 'v' || i \
                 WHEN 3 THEN x'00ff' WHEN 4 THEN NULL ELSE '' || (i % 100) END, \
               CASE i % 5 WHEN 0 THEN 'abc' WHEN 1 THEN '42' WHEN 2 THEN 3.0 WHEN 3 THEN NULL \
                 ELSE i END, \
               CASE i % 4 WHEN 0 THEN i WHEN 1 THEN 'k' || i WHEN 2 THEN NULL ELSE 7.5 END, \
               CASE i % 3 WHEN 0 THEN i WHEN 1 THEN '12' ELSE 'zz' END \
             FROM q;",
        )
        .await;
        for sql in [
            "SELECT count(*), count(v), min(v), max(v) FROM m;",
            "SELECT count(*), min(v), max(v) FROM m WHERE v > 10;",
            "SELECT count(*), min(v), max(v) FROM m WHERE v < 'v2';",
            "SELECT count(*), max(n), min(n) FROM m WHERE n > 40;",
            "SELECT count(*) FROM m WHERE n = 42;",
            "SELECT count(*) FROM m WHERE n = '42';",
            "SELECT count(*), min(s), max(s) FROM m WHERE s > 500;",
            "SELECT count(*), min(s) FROM m WHERE s = 7.5;",
            "SELECT count(*), max(x), min(x) FROM m WHERE x < 13;",
            "SELECT count(*) FROM m WHERE x = '12';",
            "SELECT count(*) FROM m WHERE v IS NULL OR n IS NULL;",
            "SELECT sum(id), count(*) FROM m WHERE v = 3.0 OR n = 3;",
        ] {
            let threads = same_at_every_worker_count(&f, &r, sql).await;
            assert!(took_parallel_path(threads), "`{sql}` did not run in parallel: {threads:?}");
        }
        // sum() over mixed classes coerces TEXT and adds REALs: declined.
        for sql in ["SELECT sum(v) FROM m;", "SELECT sum(n) FROM m WHERE n > 0;", "SELECT sum(x) FROM m;"] {
            same_at_every_worker_count(&f, &r, sql).await;
        }
    });
}

#[test]
fn parallel_aggregates_read_every_serial_type_as_the_column_opcode_does() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (f, r) = file_pair(&dir, "serial_types.db").await;
        // The integers 0 and 1 are stored as serial types 8 and 9, with no
        // body bytes; `w` (no affinity) also holds 600, the REAL 600.0, the
        // TEXT '600', 500.5, a BLOB, NULL and integers up to 6 bytes; `z` is
        // INTEGER with 0, 1, negatives and 6-byte values; `x` is REAL, whose
        // integral values are stored as integers and read back as REALs. A
        // cell decoded as the wrong storage class changes one of these answers.
        both(&f, &r, "CREATE TABLE k(id INTEGER PRIMARY KEY, w, z INTEGER, x REAL);").await;
        both(
            &f,
            &r,
            "WITH RECURSIVE q(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM q WHERE i < 30000) \
             INSERT INTO k SELECT i, \
               CASE i % 9 WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 2 THEN 600 WHEN 3 THEN 600.0 \
                 WHEN 4 THEN '600' WHEN 5 THEN 500.5 WHEN 6 THEN x'01' WHEN 7 THEN NULL \
                 ELSE i * 100003 END, \
               CASE i % 4 WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 2 THEN -i ELSE i * 4294967311 END, \
               CASE i % 3 WHEN 0 THEN 0 WHEN 1 THEN 1 ELSE i END \
             FROM q;",
        )
        .await;
        for sql in [
            "SELECT count(*), count(w), min(w), max(w) FROM k;",
            "SELECT count(*), min(w), max(w) FROM k WHERE w > 500;",
            "SELECT count(*), min(w), max(w) FROM k WHERE w = 600;",
            "SELECT count(*), count(w) FROM k WHERE w = 1 OR w = 0;",
            "SELECT count(*), sum(z), min(z), max(z) FROM k;",
            "SELECT sum(z), count(z) FROM k WHERE z < 2;",
            "SELECT count(*), min(x), max(x) FROM k WHERE x <= 1;",
            "SELECT count(*), sum(id) FROM k WHERE z IS NOT NULL AND w IS NULL;",
            // An integer against a TEXT literal is not an integer comparison:
            // `w` (no affinity) matches only the TEXT '600', `z` (INTEGER)
            // compares '5' as the number 5.
            "SELECT count(*), min(w), max(w) FROM k WHERE w = '600';",
            "SELECT count(*), min(z) FROM k WHERE z > '5';",
            "SELECT count(*), max(z) FROM k WHERE '5' < z;",
        ] {
            let threads = same_at_every_worker_count(&f, &r, sql).await;
            assert!(took_parallel_path(threads), "`{sql}` did not run in parallel: {threads:?}");
        }
    });
}

#[test]
fn parallel_sum_raises_integer_overflow_exactly_when_the_serial_sum_does() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (f, r) = file_pair(&dir, "overflow.db").await;
        both(&f, &r, "CREATE TABLE o(id INTEGER PRIMARY KEY, v INTEGER, w INTEGER, z INTEGER);").await;
        // 40,000 rows of zeros across many leaves; then:
        //  v: MAX at row 1, +1 at row 30,000 and -1 at row 30,001. The total
        //     fits, but in rowid order the running sum passes MAX at row
        //     30,000: SQLite raises. Summing per run and adding the totals,
        //     or checking only the final total, answers MAX.
        //  w: +MAX and -MAX alternating every 1,000 rows. In rowid order the
        //     running sum stays in range: the answer is 0. Adding the runs in
        //     any other order can pass MAX.
        //  z: MIN at row 1 then -1 at the last row: the running sum passes MIN.
        both(
            &f,
            &r,
            "WITH RECURSIVE q(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM q WHERE i < 40000) \
             INSERT INTO o SELECT i, \
               CASE i WHEN 1 THEN 9223372036854775807 WHEN 30000 THEN 1 WHEN 30001 THEN -1 ELSE 0 END, \
               CASE WHEN i % 1000 <> 0 THEN 0 WHEN (i / 1000) % 2 = 1 THEN 9223372036854775807 \
                 ELSE -9223372036854775807 END, \
               CASE i WHEN 1 THEN -9223372036854775808 WHEN 40000 THEN -1 ELSE 0 END \
             FROM q;",
        )
        .await;
        for sql in [
            "SELECT sum(v) FROM o;",
            "SELECT count(*), sum(v) FROM o WHERE w IS NOT NULL;",
            "SELECT sum(w) FROM o;",
            "SELECT sum(w), count(w) FROM o WHERE w <> 0;",
            "SELECT sum(z) FROM o;",
            "SELECT sum(v) FROM o WHERE z = 0;",
        ] {
            let threads = same_at_every_worker_count(&f, &r, sql).await;
            assert!(took_parallel_path(threads), "`{sql}` did not run in parallel: {threads:?}");
        }
        // The negative case itself: stock raises, so must every worker count.
        assert!(stock_result(&r, "SELECT sum(v) FROM o;").is_err());
        assert_eq!(stock_result(&r, "SELECT sum(w) FROM o;").unwrap(), vec![vec!["0".to_owned()]]);
    });
}

#[test]
fn parallel_aggregates_read_aliases_short_rows_and_overflow_payloads() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (f, r) = file_pair(&dir, "layout.db").await;
        // `b` is long enough that most rows spill onto overflow pages, and
        // `a` is stored after it, on an overflow page.
        both(&f, &r, "CREATE TABLE p(id INTEGER PRIMARY KEY, b TEXT, a INTEGER);").await;
        both(
            &f,
            &r,
            "WITH RECURSIVE q(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM q WHERE i < 6000) \
             INSERT INTO p SELECT i * 3, replace(hex(zeroblob(1500 + i % 1000)), '0', 'x'), \
               i % 97 FROM q;",
        )
        .await;
        // ALTER TABLE ADD COLUMN: every existing row is one column short and
        // reads the default; later rows store it.
        both(&f, &r, "ALTER TABLE p ADD COLUMN z INTEGER DEFAULT 7;").await;
        both(
            &f,
            &r,
            "WITH RECURSIVE q(i) AS (SELECT 6001 UNION ALL SELECT i + 1 FROM q WHERE i < 6500) \
             INSERT INTO p SELECT i * 3, 'short', i % 97, i % 5 FROM q;",
        )
        .await;
        for sql in [
            "SELECT count(*), sum(a), min(a), max(a) FROM p;",
            "SELECT sum(a), count(*) FROM p WHERE a > 50 AND id > 300;",
            "SELECT sum(id), min(id), max(id) FROM p WHERE a = 3;",
            "SELECT sum(z), count(z), min(z), max(z) FROM p;",
            "SELECT count(*) FROM p WHERE z = 7;",
            "SELECT count(*), sum(a) FROM p WHERE z < 3 OR a > 90;",
            "SELECT max(length(b)) FROM p;",
        ] {
            same_at_every_worker_count(&f, &r, sql).await;
        }
        for sql in [
            "SELECT count(*), sum(a), min(a), max(a) FROM p;",
            "SELECT sum(z), count(z), min(z), max(z) FROM p;",
        ] {
            let threads = same_at_every_worker_count(&f, &r, sql).await;
            assert!(took_parallel_path(threads), "`{sql}` did not run in parallel: {threads:?}");
        }
    });
}

#[test]
fn parallel_aggregates_decline_without_rowid_user_functions_and_transactions() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let (f, r) = file_pair(&dir, "decline.db").await;
        both(&f, &r, "CREATE TABLE w(k INTEGER PRIMARY KEY, a INTEGER) WITHOUT ROWID;").await;
        both(&f, &r, "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER);").await;
        for table in ["w", "t"] {
            both(
                &f,
                &r,
                &format!(
                    "WITH RECURSIVE q(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM q WHERE i < 30000) \
                     INSERT INTO {table} SELECT i, i % 1000 FROM q;"
                ),
            )
            .await;
        }
        // WITHOUT ROWID: no plan.
        let threads = same_at_every_worker_count(&f, &r, "SELECT count(*), sum(a) FROM w WHERE a > 5;").await;
        assert_eq!(threads, None);
        // Explicit transaction: serial, and it sees its own writes.
        set_workers(&f, 8).await;
        both(&f, &r, "BEGIN;").await;
        both(&f, &r, "UPDATE t SET a = a + 1 WHERE id % 2 = 0;").await;
        let sql = "SELECT count(*), sum(a) FROM t WHERE a > 5;";
        let _ = take_last_parallel_walk();
        assert_eq!(frank_result(&f, sql).await, stock_result(&r, sql));
        assert_eq!(take_last_parallel_walk(), None);
        both(&f, &r, "COMMIT;").await;
        assert!(took_parallel_path(same_at_every_worker_count(&f, &r, sql).await));
        // EXPLAIN is the serial program whatever the worker count.
        set_workers(&f, 1).await;
        let serial = frank(&f, "EXPLAIN SELECT count(*), sum(a) FROM t WHERE a > 5;").await;
        set_workers(&f, 8).await;
        assert_eq!(frank(&f, "EXPLAIN SELECT count(*), sum(a) FROM t WHERE a > 5;").await, serial);
        // An application function anywhere in the registry: serial, with
        // stock's answer (stock computes twice(a) as a * 2).
        f.register_deterministic_scalar_function(Twice);
        let plain = "SELECT count(*), sum(a) FROM t WHERE a > 5;";
        assert_eq!(same_at_every_worker_count(&f, &r, plain).await, None);
        for workers in WORKERS {
            set_workers(&f, workers).await;
            assert_eq!(
                frank_result(&f, "SELECT count(*) FROM t WHERE twice(a) > 5;").await,
                stock_result(&r, "SELECT count(*) FROM t WHERE a * 2 > 5;"),
            );
            assert_eq!(take_last_parallel_walk(), None);
        }
    });
}

struct Twice;

impl fsqlite_func::ScalarFunction for Twice {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(args[0].to_integer() * 2))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "twice"
    }
}

#[test]
fn parallel_aggregate_reads_one_committed_snapshot_while_a_writer_commits() {
    const BASE: i64 = 30_000;
    const BATCH: i64 = 500;
    const BATCHES: i64 = 40;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("agg_race.db").to_str().unwrap().to_owned();
    asupersync::test_utils::run_test(|| {
        let path = path.clone();
        async move {
            let f = Connection::open(&path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);")
                .await
                .unwrap();
            f.execute(&fill("t", BASE)).await.unwrap();
        }
    });
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        asupersync::test_utils::run_test(|| {
            let path = writer_path.clone();
            async move {
                let w = Connection::open(&path).await.unwrap();
                for batch in 0..BATCHES {
                    let first = BASE + batch * BATCH + 1;
                    let last = first + BATCH - 1;
                    // Every row of a batch has a = 1001: a committed batch
                    // adds exactly BATCH rows and BATCH * 1001 to the sum.
                    w.execute(&format!(
                        "WITH RECURSIVE c(x) AS (SELECT {first} UNION ALL SELECT x + 1 FROM c \
                         WHERE x < {last}) INSERT INTO t SELECT x, 1001, 'w' FROM c;"
                    ))
                    .await
                    .unwrap();
                }
            }
        });
    });
    asupersync::test_utils::run_test(|| {
        let path = path.clone();
        async move {
            let f = Connection::open(&path).await.unwrap();
            set_workers(&f, 4).await;
            let mut parallel_runs = 0_usize;
            let mut previous = 0;
            for _ in 0..60 {
                // Only the writer's rows have a > 1000 (the base rows hold
                // 0..=999): every answer is a whole number of batches.
                let rows = f
                    .query("SELECT count(*), sum(a) FROM t WHERE a > 1000;")
                    .await
                    .unwrap();
                let (SqliteValue::Integer(count), sum) =
                    (&rows[0].values()[0], &rows[0].values()[1])
                else {
                    panic!("count is not an integer");
                };
                assert_eq!(count % BATCH, 0, "partial batch visible: {count}");
                assert!(*count >= previous, "count went back from {previous} to {count}");
                previous = *count;
                let expected = if *count == 0 {
                    SqliteValue::Null
                } else {
                    SqliteValue::Integer(count * 1001)
                };
                assert_eq!(*sum, expected);
                if matches!(take_last_parallel_walk(), Some(WalkRecord::Ran { .. })) {
                    parallel_runs += 1;
                }
            }
            assert!(parallel_runs > 0, "no aggregate ran in parallel");
        }
    });
    writer.join().expect("writer thread");
}
