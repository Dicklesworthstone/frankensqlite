#![recursion_limit = "512"]

//! GH#492: a present TEXT primary key in a 100-row hot table must not
//! materialize an unrelated bulk table on open or after each commit.
//!
//! The ordinary-open regression is explicitly runnable while the production
//! hydration fix is pending. The schema-only control runs the same workload;
//! it is NOT evidence that ordinary Connection::open has been fixed.
//!
//! Run the ordinary regression with:
//! cargo test -p fsqlite-core --test gh492_bounded_post_commit_hydration \
//!     ordinary_post_commit_reads_are_bounded -- --ignored --nocapture
//!
//! Bounds count actual per-connection hydrated rows, not elapsed time or VDBE
//! opcodes (hydration happens outside the point-read program). A selective
//! implementation may hydrate the hot table; the bulk table must stay out.

use fsqlite_core::connection::{Connection, PreparedStatement};
use fsqlite_types::value::SqliteValue;
use std::path::Path;
use std::time::Instant;

const HOT_ROWS: i64 = 100;
const BULK_SMALL: i64 = 8_192;
const BULK_LARGE: i64 = 32_768;
const POINT: &str = "SELECT v FROM hot WHERE id = ?1";
const LITERAL_POINT: &str = "SELECT v FROM hot WHERE id = 'h7'";

#[derive(Clone, Copy, Debug)]
enum OpenMode {
    Ordinary,
    SchemaOnly,
}

#[derive(Clone, Copy, Debug)]
enum ReadApi {
    Query,
    QueryParams,
    QueryRow,
    Prepared,
}

fn seed(path: &Path, bulk_rows: i64) {
    // Stock SQLite builds the fixture independently of the code under test.
    // No stock handle remains open while FrankenSQLite owns the database.
    let mut stock = rusqlite::Connection::open(path).expect("create stock fixture");
    stock
        .execute_batch(
            "PRAGMA page_size = 4096;\
             CREATE TABLE bulk (id INTEGER PRIMARY KEY, body TEXT NOT NULL);\
             CREATE TABLE hot (id TEXT PRIMARY KEY, v INTEGER NOT NULL);",
        )
        .expect("fixture schema");
    let tx = stock.transaction().expect("fixture transaction");
    {
        let body = "x".repeat(2_048);
        let mut insert = tx
            .prepare("INSERT INTO bulk VALUES (?1, ?2)")
            .expect("bulk insert");
        for id in 0..bulk_rows {
            insert
                .execute(rusqlite::params![id, body.as_str()])
                .expect("bulk row");
        }
        for id in 0..HOT_ROWS {
            tx.execute("INSERT INTO hot VALUES (?1, 0)", [format!("h{id}")])
                .expect("hot row");
        }
    }
    tx.commit().expect("fixture commit");
}

async fn open(path: &str, mode: OpenMode, failures: &mut Vec<String>) -> Connection {
    let start = Instant::now();
    let conn = match mode {
        OpenMode::Ordinary => Connection::open(path).await,
        OpenMode::SchemaOnly => Connection::open_existing_schema_only(path).await,
    }
    .expect("open fixture");
    let hydrated = conn.memdb_row_hydration_count();
    println!(
        "GH492 {mode:?} open: elapsed={:?} hydrated={hydrated}",
        start.elapsed()
    );
    if hydrated > 100 {
        failures.push(format!("{mode:?} open hydrated {hydrated} rows"));
    }
    conn
}

async fn read(
    conn: &Connection,
    prepared: &PreparedStatement<'_>,
    api: ReadApi,
    expected: i64,
    label: &str,
    failures: &mut Vec<String>,
) {
    let before = conn.memdb_row_hydration_count();
    let start = Instant::now();
    let key = [SqliteValue::Text("h7".into())];
    let rows = match api {
        ReadApi::Query => conn.query(LITERAL_POINT).await,
        ReadApi::QueryParams => conn.query_with_params(POINT, &key).await,
        ReadApi::QueryRow => conn.query_row(LITERAL_POINT).await.map(|row| vec![row]),
        ReadApi::Prepared => prepared.query_with_params(&key).await,
    }
    .expect("point read");
    let elapsed = start.elapsed();
    let hydrated = conn.memdb_row_hydration_count() - before;
    assert_eq!(rows.len(), 1, "{label} {api:?}: present key");
    assert_eq!(
        rows[0].values(),
        &[SqliteValue::Integer(expected)],
        "{label} {api:?}"
    );
    println!("GH492 {label} {api:?}: elapsed={elapsed:?} hydrated={hydrated}");
    if hydrated > 100 {
        failures.push(format!("{label} {api:?} hydrated {hydrated} rows"));
    }
}

async fn update(conn: &Connection, failures: &mut Vec<String>) {
    let before = conn.memdb_row_hydration_count();
    conn.execute_with_params(
        "UPDATE hot SET v = v + 1 WHERE id = ?1",
        &[SqliteValue::Text("h7".into())],
    )
    .await
    .expect("update hot row");
    let hydrated = conn.memdb_row_hydration_count() - before;
    // Moving the whole-image pass into the write is not a fix either.
    if hydrated > 100 {
        failures.push(format!("hot update hydrated {hydrated} rows"));
    }
}

// Keep the prepared objects and the entire transaction history in one scope.
#[allow(clippy::too_many_lines)]
async fn exercise(path: &Path, bulk_rows: i64, mode: OpenMode, failures: &mut Vec<String>) {
    seed(path, bulk_rows);
    let bytes = std::fs::metadata(path).expect("fixture metadata").len();
    println!("GH492 {mode:?}: bulk_rows={bulk_rows} db_bytes={bytes}");
    let path_text = path.to_string_lossy();
    let a = open(&path_text, mode, failures).await;
    a.execute("PRAGMA journal_mode = WAL")
        .await
        .expect("WAL permits a writer beside the pinned reader snapshot");
    let b = open(&path_text, mode, failures).await;
    let mut expected = 0;
    {
        let a_before = a.memdb_row_hydration_count();
        let b_before = b.memdb_row_hydration_count();
        // These exact prepared objects survive every commit below.
        let a_stmt = a.prepare(POINT).await.expect("prepare writer read");
        let b_stmt = b.prepare(POINT).await.expect("prepare peer read");
        for (label, hydrated) in [
            ("writer prepare", a.memdb_row_hydration_count() - a_before),
            ("peer prepare", b.memdb_row_hydration_count() - b_before),
        ] {
            if hydrated > 100 {
                failures.push(format!("{label} hydrated {hydrated} rows"));
            }
        }
        read(
            &a,
            &a_stmt,
            ReadApi::Prepared,
            0,
            "first writer read",
            failures,
        )
        .await;
        read(&b, &b_stmt, ReadApi::Prepared, 0, "first peer read", failures).await;
        for api in [
            ReadApi::Query,
            ReadApi::QueryParams,
            ReadApi::QueryRow,
            ReadApi::Prepared,
        ] {
            for _ in 0..2 {
                // EVERY API gets the first read after a fresh commit. Warming
                // one API then measuring the other would hide this defect.
                update(&a, failures).await;
                expected += 1;
                read(&a, &a_stmt, api, expected, "same connection", failures).await;
                read(&b, &b_stmt, api, expected, "cross connection", failures).await;
            }
        }
        a.execute("BEGIN").await.expect("begin rollback probe");
        update(&a, failures).await;
        read(
            &a,
            &a_stmt,
            ReadApi::Prepared,
            expected + 1,
            "own uncommitted write",
            failures,
        )
        .await;
        read(
            &b,
            &b_stmt,
            ReadApi::Prepared,
            expected,
            "uncommitted peer is invisible",
            failures,
        )
        .await;
        a.execute("ROLLBACK").await.expect("rollback hot write");
        read(
            &a,
            &a_stmt,
            ReadApi::Prepared,
            expected,
            "after rollback",
            failures,
        )
        .await;

        b.execute("BEGIN").await.expect("begin reader snapshot");
        read(
            &b,
            &b_stmt,
            ReadApi::Prepared,
            expected,
            "pin reader snapshot",
            failures,
        )
        .await;
        update(&a, failures).await;
        read(
            &b,
            &b_stmt,
            ReadApi::Prepared,
            expected,
            "reader keeps old snapshot",
            failures,
        )
        .await;
        expected += 1;
        b.execute("ROLLBACK").await.expect("release reader snapshot");
        read(
            &b,
            &b_stmt,
            ReadApi::Prepared,
            expected,
            "new snapshot sees commit",
            failures,
        )
        .await;
    }
    a.close().await.expect("close writer");
    b.close().await.expect("close reader");

    let reopened = open(&path_text, mode, failures).await;
    {
        let stmt = reopened.prepare(POINT).await.expect("prepare reopened read");
        read(
            &reopened,
            &stmt,
            ReadApi::Prepared,
            expected,
            "reopen plus read",
            failures,
        )
        .await;
        if reopened.memdb_row_hydration_count() > 100 {
            failures.push("reopen/prepare/read hydrated more than the hot table".to_owned());
        }
    }
    reopened.close().await.expect("close reopened reader");
    let stock = rusqlite::Connection::open(path).expect("stock verification");
    let actual: i64 = stock
        .query_row(LITERAL_POINT, [], |row| row.get(0))
        .expect("stock hot row");
    assert_eq!(actual, expected, "committed value persisted");
    let count: i64 = stock
        .query_row("SELECT count(*) FROM bulk", [], |row| row.get(0))
        .expect("stock bulk count");
    assert_eq!(count, bulk_rows, "unrelated bulk rows remain intact");
}

fn run(mode: OpenMode) {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut failures = Vec::new();
        for (name, bulk_rows) in [("small", BULK_SMALL), ("large", BULK_LARGE)] {
            exercise(
                &dir.path().join(format!("{name}.db")),
                bulk_rows,
                mode,
                &mut failures,
            )
            .await;
        }
        // Collect both fixture receipts before failing, so the size-dependent
        // work is visible even when the first ordinary open regresses.
        assert!(
            failures.is_empty(),
            "GH#492 unbounded row hydration:\n{}",
            failures.join("\n")
        );
    });
}

#[test]
#[ignore = "GH#492 pending production fix; run explicitly with --ignored --nocapture"]
fn ordinary_post_commit_reads_are_bounded() {
    run(OpenMode::Ordinary);
}

#[test]
fn schema_only_control_is_bounded_and_preserves_visibility() {
    run(OpenMode::SchemaOnly);
}

#[test]
fn small_ordinary_database_retains_memdb_acceleration() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("tiny.db");
        seed(&path, 8);
        let conn = Connection::open(path.to_string_lossy().as_ref())
            .await
            .expect("open tiny db");
        let row = conn.query_row(LITERAL_POINT).await.expect("tiny point read");
        assert_eq!(row.values(), &[SqliteValue::Integer(0)]);
        assert!(
            conn.memdb_row_hydration_count() > 0,
            "small databases retain the optional MemDatabase fast path"
        );
        conn.close().await.expect("close tiny db");
    });
}
