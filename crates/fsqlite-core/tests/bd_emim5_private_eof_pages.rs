//! bd-emim5 (b) step 1 keepers: a page a concurrent transaction allocates from
//! the end of the file is private to it until COMMIT.
//!
//! Each connection has its own pager, so two in-process transactions that grow
//! the file from the same committed size both hand out "committed size + 1"
//! for their first new page. Before step 1 the second one's first write of
//! that page waited `busy_timeout` on the first one's in-process page lock and
//! failed with `Busy` (interleaved on one thread the first never commits while
//! the second waits: "insert b id 20: Busy", r18's parent run). Step 1 writes
//! such a page without the page lock; COMMIT still takes the page lock and the
//! commit-index check for every page it writes, so the later of two growers is
//! refused there (`BusySnapshot`) and its retry commits.
//!
//! - K-S1a `second_grower_*`: both connections `BEGIN CONCURRENT` and grow their
//!   own table, interleaved, so both allocate from the same committed size.
//!   Every INSERT succeeds; the first COMMIT succeeds; the second COMMIT is
//!   `BusySnapshot` on a page the first committed past the shared size; a full
//!   retry of the second transaction commits; stock SQLite then reads a clean
//!   file with every row. RED on the parent at the second grower's first
//!   end-of-file page.
//! - K-S1b `a_committed_page_*` (negative case): a page that existed before both
//!   transactions began is still locked at the statement, so a second UPDATE of
//!   the same row waits and fails `Busy`. A version that skips the page lock for
//!   every first write fails this.
//! - K-S1c `a_reused_freelist_page_*` (negative case): a page reused from the
//!   committed freelist is a committed page, not a new one, so two transactions
//!   that reuse the same freelist page still meet at its page lock. A version
//!   that treats every page a transaction allocates as private fails this.
//! - bd-t26cs `a_grower_that_freed_a_fresh_page_*`: freeing a fresh end-of-file
//!   page must also stay off the in-process page lock table until COMMIT. Before
//!   the fix, `free_page` took page locks and ran statement-time FCW checks on
//!   private EOF pages, causing a peer grower's `COMMIT` to fail `Busy` (the
//!   `c1e` failure) and rejecting a `DELETE` mid-statement after a peer grew.
//!   COMMIT still refuses the stale grower with `BusySnapshot`, and its first
//!   retry commits with stock SQLite `integrity_check == "ok"`.
#![cfg(unix)]

use std::path::Path;
use std::time::Duration;

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use rusqlite::OpenFlags;

/// Rows each grower inserts into its own table, in `CHUNKS` interleaved chunks.
const ROWS: i64 = 1_000;
const CHUNKS: i64 = 10;
/// About 18 rows per 4096-byte leaf, so each chunk of 100 rows needs about 5 new
/// pages and each grower allocates past the shared committed size in every chunk.
const PAD: usize = 200;
const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

fn db_str(db: &Path) -> String {
    db.to_string_lossy().into_owned()
}

async fn open(db: &Path, busy_timeout_ms: u32) -> Connection {
    let conn = Connection::open(db_str(db)).await.expect("open");
    conn.execute(&format!("PRAGMA busy_timeout={busy_timeout_ms}"))
        .await
        .expect("busy_timeout");
    conn
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("query");
    match rows[0].values()[0] {
        SqliteValue::Integer(value) => value,
        ref other => panic!("expected an integer from {sql}, got {other:?}"),
    }
}

/// fsqlite's own `integrity_check`, every row of the report joined.
async fn fsqlite_integrity(conn: &Connection) -> String {
    let rows = conn
        .query("PRAGMA integrity_check")
        .await
        .expect("fsqlite integrity_check");
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Text(ref text) => text.to_string(),
            ref other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A WAL database with tables `a`, `b` (empty) and `c` (one row `(1, 0)`), and an
/// empty freelist; returns the committed page count.
async fn create(db: &Path) -> i64 {
    let conn = open(db, 5000).await;
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    for sql in [
        "CREATE TABLE a(id INTEGER PRIMARY KEY, pad TEXT)",
        "CREATE TABLE b(id INTEGER PRIMARY KEY, pad TEXT)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO c(id, v) VALUES (1, 0)",
    ] {
        conn.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let pages = scalar(&conn, "PRAGMA page_count").await;
    assert_eq!(
        scalar(&conn, "PRAGMA freelist_count").await,
        0,
        "precondition: an empty freelist, so every new page comes from the end of the file"
    );
    conn.close().await.expect("close creator");
    pages
}

fn pad() -> String {
    "x".repeat(PAD)
}

async fn insert_chunk(conn: &Connection, table: &str, chunk: i64) {
    let per_chunk = ROWS / CHUNKS;
    let pad = pad();
    for i in 0..per_chunk {
        let id = chunk * per_chunk + i + 1;
        conn.execute(&format!("INSERT INTO {table}(id, pad) VALUES ({id}, '{pad}')"))
            .await
            .unwrap_or_else(|error| panic!("insert {table} id {id}: {error:?}"));
    }
}

#[derive(Debug)]
struct StockReport {
    integrity: String,
    rows_a: i64,
    rows_b: i64,
    c_value: i64,
}

fn stock_read(db: &Path) -> StockReport {
    let stock = rusqlite::Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("stock open");
    stock.busy_timeout(BUSY_TIMEOUT).expect("stock busy");
    let integrity: Vec<String> = {
        let mut stmt = stock
            .prepare("PRAGMA integrity_check(100000)")
            .expect("prepare integrity_check");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("integrity_check")
            .collect::<Result<_, _>>()
            .expect("integrity rows")
    };
    let count = |sql: &str| -> i64 { stock.query_row(sql, [], |r| r.get(0)).expect(sql) };
    StockReport {
        integrity: integrity.join("\n"),
        rows_a: count("SELECT count(*) FROM a"),
        rows_b: count("SELECT count(*) FROM b"),
        c_value: count("SELECT v FROM c WHERE id = 1"),
    }
}

fn pages_of(error: &FrankenError) -> Vec<i64> {
    match error {
        FrankenError::BusySnapshot { conflicting_pages } => conflicting_pages
            .split(',')
            .filter_map(|page| page.trim().parse::<i64>().ok())
            .collect(),
        _ => Vec::new(),
    }
}

/// K-S1a with the given commit order; `late_first` commits the connection whose
/// allocations came second in every chunk first.
async fn second_grower(late_first: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("s1a.db");
    let base_pages = create(&db).await;

    let first = open(&db, 5000).await;
    let second = open(&db, 5000).await;
    first.execute("BEGIN CONCURRENT").await.expect("first begin");
    second
        .execute("BEGIN CONCURRENT")
        .await
        .expect("second begin");
    // Both grow from `base_pages`; the parent fails here (`Busy`) at the second
    // connection's first page past it.
    for chunk in 0..CHUNKS {
        insert_chunk(&first, "a", chunk).await;
        insert_chunk(&second, "b", chunk).await;
    }
    let (winner, loser, loser_table) = if late_first {
        (&second, &first, "a")
    } else {
        (&first, &second, "b")
    };
    winner
        .execute("COMMIT")
        .await
        .expect("the first committer commits");
    let grown = scalar(winner, "PRAGMA page_count").await;
    assert!(
        grown > base_pages + 10,
        "vacuity: the first commit grows the file past the shared committed size \
         ({base_pages} -> {grown})"
    );

    let refused = loser.execute("COMMIT").await;
    let error = match refused {
        Err(error) => error,
        Ok(_) => panic!(
            "bd-emim5 step 1 (late_first={late_first}): the second grower allocated the pages \
             the first just committed, so its COMMIT must be refused until relocation exists"
        ),
    };
    assert!(
        matches!(error, FrankenError::BusySnapshot { .. }),
        "bd-emim5 step 1 (late_first={late_first}): the second grower is refused at COMMIT with \
         BusySnapshot (first committer wins), got {error:?}"
    );
    let pages = pages_of(&error);
    assert!(
        pages
            .iter()
            .any(|&page| page > base_pages && page <= grown),
        "bd-emim5 step 1: the refusal names a page the first committed past the shared size \
         ({base_pages}, {grown}], got {pages:?}"
    );
    loser.execute("ROLLBACK").await.ok();

    // The retry starts from the grown file and commits.
    loser
        .execute("BEGIN CONCURRENT")
        .await
        .expect("retry begin");
    for chunk in 0..CHUNKS {
        insert_chunk(loser, loser_table, chunk).await;
    }
    loser.execute("COMMIT").await.expect("the retry commits");
    for conn in [&first, &second] {
        assert_eq!(scalar(conn, "SELECT count(*) FROM a").await, ROWS);
        assert_eq!(scalar(conn, "SELECT count(*) FROM b").await, ROWS);
    }
    first.close().await.expect("close first");
    second.close().await.expect("close second");

    let stock = stock_read(&db);
    assert_eq!(
        stock.integrity, "ok",
        "K-S1a: stock integrity_check after both close: {stock:?}"
    );
    assert_eq!((stock.rows_a, stock.rows_b), (ROWS, ROWS), "K-S1a: stock rows");
    let reopened = open(&db, 5000).await;
    assert_eq!(
        fsqlite_integrity(&reopened).await,
        "ok",
        "K-S1a: fsqlite reopen"
    );
    reopened.close().await.expect("close reopened");
}

#[test]
fn second_grower_runs_without_waiting_and_is_refused_at_commit_early_allocator_first() {
    asupersync::test_utils::run_test(|| async { second_grower(false).await });
}

#[test]
fn second_grower_runs_without_waiting_and_is_refused_at_commit_late_allocator_first() {
    asupersync::test_utils::run_test(|| async { second_grower(true).await });
}

/// K-S1b: a page both transactions found committed keeps its statement-time
/// page lock.
#[test]
fn a_committed_page_still_waits_for_its_peer_at_the_statement() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("s1b.db");
        create(&db).await;

        let first = open(&db, 5000).await;
        let second = open(&db, 200).await;
        first.execute("BEGIN CONCURRENT").await.expect("first begin");
        first
            .execute("UPDATE c SET v = 1 WHERE id = 1")
            .await
            .expect("first update");
        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second begin");
        let contended = second.execute("UPDATE c SET v = 2 WHERE id = 1").await;
        assert!(
            matches!(contended, Err(FrankenError::Busy)),
            "bd-emim5 step 1 negative case: an UPDATE of a page that existed before both \
             transactions began still waits on the peer's page lock and fails Busy, got \
             {contended:?}"
        );
        second.execute("ROLLBACK").await.ok();
        first.execute("COMMIT").await.expect("first commit");
        assert_eq!(scalar(&second, "SELECT v FROM c WHERE id = 1").await, 1);
        first.close().await.expect("close first");
        second.close().await.expect("close second");

        let stock = stock_read(&db);
        assert_eq!(stock.integrity, "ok", "K-S1b: stock integrity: {stock:?}");
        assert_eq!(stock.c_value, 1, "K-S1b: the first committer's value");
    });
}

/// K-S1c: a page reused from the committed freelist keeps its statement-time
/// page lock.
#[test]
fn a_reused_freelist_page_still_waits_for_its_peer_at_the_statement() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("s1c.db");
        create(&db).await;
        {
            let setup = open(&db, 5000).await;
            setup
                .execute("CREATE TABLE d(id INTEGER PRIMARY KEY, pad TEXT)")
                .await
                .expect("create d");
            setup.execute("BEGIN").await.expect("begin fill");
            let pad = pad();
            for id in 1..=300 {
                setup
                    .execute(&format!("INSERT INTO d(id, pad) VALUES ({id}, '{pad}')"))
                    .await
                    .expect("fill d");
            }
            setup.execute("COMMIT").await.expect("commit fill");
            setup.execute("DELETE FROM d").await.expect("empty d");
            assert!(
                scalar(&setup, "PRAGMA freelist_count").await > 4,
                "precondition: emptying d leaves its leaves on the committed freelist"
            );
            setup.close().await.expect("close setup");
        }

        let first = open(&db, 5000).await;
        let second = open(&db, 200).await;
        let base_pages = scalar(&first, "PRAGMA page_count").await;
        first.execute("BEGIN CONCURRENT").await.expect("first begin");
        insert_chunk(&first, "a", 0).await;
        assert_eq!(
            scalar(&first, "PRAGMA page_count").await,
            base_pages,
            "vacuity: the first transaction's new pages came from the freelist, not the end of \
             the file"
        );
        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second begin");
        let pad = pad();
        let mut contended = None;
        for id in 1..=100 {
            if let Err(error) = second
                .execute(&format!("INSERT INTO b(id, pad) VALUES ({id}, '{pad}')"))
                .await
            {
                contended = Some((id, error));
                break;
            }
        }
        assert!(
            matches!(contended, Some((_, FrankenError::Busy))),
            "bd-emim5 step 1 negative case: the second transaction reuses the freelist page the \
             first one already writes and must wait on its page lock and fail Busy at the \
             statement, got {contended:?}"
        );
        second.execute("ROLLBACK").await.ok();
        first.execute("COMMIT").await.expect("first commit");
        first.close().await.expect("close first");
        second.close().await.expect("close second");

        let stock = stock_read(&db);
        assert_eq!(stock.integrity, "ok", "K-S1c: stock integrity: {stock:?}");
        assert_eq!(stock.rows_a, ROWS / CHUNKS, "K-S1c: the first's rows");
        assert_eq!(stock.rows_b, 0, "K-S1c: the second rolled back");
    });
}

/// bd-t26cs keeper (exact `c1e` freed-fresh-page shape): `second` grows table
/// `b` with fresh EOF pages and deletes rows `> 20` (freeing most of those
/// fresh pages); `first` grows table `a` into overlapping EOF page numbers and
/// commits cleanly; `second`'s first COMMIT is refused `BusySnapshot` (negative
/// case: must not commit over `first`'s growth); `second` rolls back, replays,
/// and commits on its first retry with stock SQLite `integrity_check == "ok"`.
#[test]
fn a_grower_that_freed_a_fresh_page_stays_refused_and_retries() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("t26cs_freed.db");
        let base_pages = create(&db).await;

        let first = open(&db, 5000).await;
        let second = open(&db, 5000).await;
        first.execute("BEGIN CONCURRENT").await.expect("first begin");
        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second begin");

        let large_pad = "y".repeat(600);
        for id in 1..=200_i64 {
            second
                .execute(&format!(
                    "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("second insert b id {id}: {e:?}"));
        }
        second
            .execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("second delete b > 20");

        let first_rows = 100_i64;
        for id in 1..=first_rows {
            first
                .execute(&format!(
                    "INSERT INTO a(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("first insert a id {id}: {e:?}"));
        }
        first
            .execute("COMMIT")
            .await
            .expect("bd-t26cs: peer grower's COMMIT must not fail Busy against freed private EOF pages");
        let grown = scalar(&first, "PRAGMA page_count").await;
        assert!(
            grown > base_pages + 10,
            "vacuity: first must grow past second's surviving prefix ({base_pages} -> {grown})"
        );

        let refused = second.execute("COMMIT").await;
        assert!(
            matches!(refused, Err(FrankenError::BusySnapshot { .. })),
            "bd-t26cs negative case: first attempt of second must still be refused BusySnapshot, \
             got {refused:?}"
        );
        second.execute("ROLLBACK").await.ok();

        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second retry begin");
        for id in 1..=200_i64 {
            second
                .execute(&format!(
                    "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("second retry insert b id {id}: {e:?}"));
        }
        second
            .execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("second retry delete b > 20");
        second
            .execute("COMMIT")
            .await
            .expect("bd-t26cs: retried COMMIT after ROLLBACK must succeed on the first retry");

        first.close().await.expect("close first");
        second.close().await.expect("close second");

        let stock = stock_read(&db);
        assert_eq!(
            stock.integrity, "ok",
            "bd-t26cs stock integrity_check after retry: {stock:?}"
        );
        assert_eq!((stock.rows_a, stock.rows_b), (first_rows, 20));
        let reopened = open(&db, 5000).await;
        assert_eq!(fsqlite_integrity(&reopened).await, "ok");
        reopened.close().await.expect("close reopened");
    });
}

/// bd-t26cs symmetric keeper: when `first` commits overlapping EOF growth
/// BEFORE `second` frees its private EOF pages via `DELETE`, `second`'s
/// `DELETE` must not fail mid-statement against `CommitIndex`; refusal stays
/// at `second`'s `COMMIT` (`BusySnapshot`), and the retry commits cleanly.
#[test]
fn a_grower_freeing_fresh_pages_after_peer_commits_defers_refusal_to_commit_and_retries() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("t26cs_delete_after_peer.db");
        let base_pages = create(&db).await;

        let first = open(&db, 5000).await;
        let second = open(&db, 5000).await;
        first.execute("BEGIN CONCURRENT").await.expect("first begin");
        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second begin");

        let large_pad = "y".repeat(600);
        for id in 1..=200_i64 {
            second
                .execute(&format!(
                    "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("second insert b id {id}: {e:?}"));
        }

        let first_rows = 100_i64;
        for id in 1..=first_rows {
            first
                .execute(&format!(
                    "INSERT INTO a(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("first insert a id {id}: {e:?}"));
        }
        first.execute("COMMIT").await.expect("first commit");
        let grown = scalar(&first, "PRAGMA page_count").await;
        assert!(
            grown > base_pages + 10,
            "vacuity: first must overlap second's private EOF range ({base_pages} -> {grown})"
        );

        second
            .execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("bd-t26cs: freeing own private EOF pages must defer conflict check to COMMIT");

        let refused = second.execute("COMMIT").await;
        assert!(
            matches!(refused, Err(FrankenError::BusySnapshot { .. })),
            "bd-t26cs negative case: COMMIT must still be refused BusySnapshot, got {refused:?}"
        );
        second.execute("ROLLBACK").await.ok();

        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second retry begin");
        for id in 1..=200_i64 {
            second
                .execute(&format!(
                    "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("second retry insert b id {id}: {e:?}"));
        }
        second
            .execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("second retry delete b > 20");
        second
            .execute("COMMIT")
            .await
            .expect("bd-t26cs: retry COMMIT must succeed");

        first.close().await.expect("close first");
        second.close().await.expect("close second");

        let stock = stock_read(&db);
        assert_eq!(stock.integrity, "ok", "bd-t26cs stock integrity: {stock:?}");
        assert_eq!((stock.rows_a, stock.rows_b), (first_rows, 20));
    });
}

/// bd-t26cs no-in-process-peer forced-conflict keeper: refuse `conn`'s first
/// `COMMIT` itself (at the WAL append gate, via an external stock SQLite update
/// to `c` with zero EOF growth), then ROLLBACK and retry. Separates leaked
/// lock state from leaked freelist/pager state.
#[test]
fn a_grower_that_freed_a_fresh_page_refused_via_external_conflict_retries() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("t26cs_forced_external.db");
        create(&db).await;

        let conn = open(&db, 5000).await;
        conn.execute("BEGIN CONCURRENT").await.expect("conn begin");

        let large_pad = "z".repeat(600);
        for id in 1..=200_i64 {
            conn.execute(&format!(
                "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("conn insert b id {id}: {e:?}"));
        }
        conn.execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("conn delete b > 20");
        conn.execute("UPDATE c SET v = 100 WHERE id = 1")
            .await
            .expect("conn update c");

        // External stock SQLite writer updates committed row c(1) without
        // growing the file, forcing conn's COMMIT (not a statement) to fail
        // BusySnapshot at the WAL append gate with no in-process peer.
        {
            let ext = rusqlite::Connection::open(&db).expect("ext open");
            ext.busy_timeout(BUSY_TIMEOUT).expect("ext busy");
            ext.execute("UPDATE c SET v = 99 WHERE id = 1", [])
                .expect("ext update c");
        }

        let refused = conn.execute("COMMIT").await;
        assert!(
            matches!(refused, Err(FrankenError::BusySnapshot { .. })),
            "bd-t26cs forced-conflict: COMMIT must be refused BusySnapshot, got {refused:?}"
        );
        conn.execute("ROLLBACK").await.ok();

        conn.execute("BEGIN CONCURRENT")
            .await
            .expect("conn retry begin");
        for id in 1..=200_i64 {
            conn.execute(&format!(
                "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("conn retry insert b id {id}: {e:?}"));
        }
        conn.execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("conn retry delete b > 20");
        conn.execute("COMMIT")
            .await
            .expect("bd-t26cs forced-conflict retry COMMIT must succeed");

        conn.close().await.expect("close conn");

        let stock = stock_read(&db);
        assert_eq!(
            stock.integrity, "ok",
            "bd-t26cs forced-conflict stock integrity: {stock:?}"
        );
        assert_eq!(stock.rows_b, 20, "bd-t26cs forced-conflict rows_b");
        assert_eq!(stock.c_value, 99, "bd-t26cs forced-conflict c_value");
    });
}

/// bd-t26cs savepoint + secure_delete keeper: freeing private EOF pages under
/// `PRAGMA secure_delete = ON` inside a savepoint and rolling back to that
/// savepoint must restore the pages as live without taking statement-time page
/// locks, so a peer grower still commits and a subsequent retry succeeds.
#[test]
fn a_grower_freeing_fresh_pages_with_secure_delete_and_savepoint_rollback_works() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("t26cs_secure_delete_sp.db");
        create(&db).await;

        let first = open(&db, 5000).await;
        let second = open(&db, 5000).await;
        second
            .execute("PRAGMA secure_delete = ON")
            .await
            .expect("secure_delete on");
        first.execute("BEGIN CONCURRENT").await.expect("first begin");
        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("second begin");

        let large_pad = "q".repeat(600);
        for id in 1..=200_i64 {
            second
                .execute(&format!(
                    "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("second insert b id {id}: {e:?}"));
        }
        second.execute("SAVEPOINT sp").await.expect("savepoint sp");
        second
            .execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("second delete inside savepoint");
        assert_eq!(scalar(&second, "SELECT count(*) FROM b").await, 20);
        second
            .execute("ROLLBACK TO sp")
            .await
            .expect("rollback to sp");
        assert_eq!(
            scalar(&second, "SELECT count(*) FROM b").await,
            200,
            "ROLLBACK TO sp must restore private EOF pages freed under secure_delete=ON"
        );
        // Re-mutate one of the restored private EOF rows to verify
        // try_mutate_staged_page_data / write_page_private_eof still work.
        second
            .execute("UPDATE b SET pad = 'updated' WHERE id = 150")
            .await
            .expect("update restored private EOF row");
        second.execute("RELEASE sp").await.expect("release sp");

        let first_rows = 100_i64;
        for id in 1..=first_rows {
            first
                .execute(&format!(
                    "INSERT INTO a(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("first insert a id {id}: {e:?}"));
        }
        first
            .execute("COMMIT")
            .await
            .expect("first commit must succeed without hitting Busy on second's private pages");

        let refused = second.execute("COMMIT").await;
        assert!(
            matches!(refused, Err(FrankenError::BusySnapshot { .. })),
            "second's COMMIT must still be refused BusySnapshot, got {refused:?}"
        );
        second.execute("ROLLBACK").await.ok();

        second
            .execute("BEGIN CONCURRENT")
            .await
            .expect("retry begin");
        for id in 1..=200_i64 {
            second
                .execute(&format!(
                    "INSERT INTO b(id, pad) VALUES ({id}, '{large_pad}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("retry insert b id {id}: {e:?}"));
        }
        second
            .execute("DELETE FROM b WHERE id > 20")
            .await
            .expect("retry delete");
        second.execute("COMMIT").await.expect("retry commit");

        first.close().await.expect("close first");
        second.close().await.expect("close second");

        let stock = stock_read(&db);
        assert_eq!(stock.integrity, "ok", "stock integrity: {stock:?}");
        assert_eq!((stock.rows_a, stock.rows_b), (first_rows, 20));
    });
}
