//! bd-gfx4q keepers: VACUUM of a file-backed database, in rollback-journal mode
//! and in WAL mode.
//!
//! On main since b1b1a76df (bd-cf0be), a read-only open of a cleanly closed WAL
//! database creates an empty `-wal` and a 32 KiB `-shm`, as stock does. VACUUM
//! validates its rebuild candidate (a WAL-format image when the source is in WAL
//! mode) through such an open, so the candidate grew a `-shm` and publication
//! refused it: "VACUUM candidate is not self-contained: companion ...-shm exists".
//!
//! - `vacuum_succeeds_and_leaves_no_rebuild_artifact_*`: VACUUM succeeds, leaves
//!   no `<db>.fsqlite-vacuum-rebuild-*` entry (main file or companion) while the
//!   connection is open or after it closes, and the result passes stock's
//!   `integrity_check` with an empty freelist, keeps its journal mode, and
//!   reopens with the same rows in both engines. Red on main for the two WAL
//!   cases (VACUUM errors); the rollback case is the control.
//! - `self_contained_guard_still_refuses_a_candidate_with_companions`: the
//!   negative case a naive fix fails. The publication guard is unchanged: it
//!   refuses a candidate whose `-wal` holds a complete frame, and one beside any
//!   `-shm`, while it accepts the bare image and the one exception (a `-wal` too
//!   short for a frame). Red under a mutant that relaxes the guard instead.
#![cfg(unix)]

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_pager::SimplePager;
use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_types::value::SqliteValue;
use fsqlite_vfs::UnixVfs;

const SEED_ROWS: i64 = 240;
const KEPT_FROM: i64 = 161;
const ADDED_ROWS: i64 = 15;
const REBUILD_MARK: &str = ".fsqlite-vacuum-rebuild-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seed {
    /// Created by stock in rollback-journal (DELETE) mode.
    StockRollback,
    /// Created by stock in WAL mode and closed cleanly (no `-wal`, no `-shm`).
    StockWal,
    /// Created by fsqlite with its default (WAL) journal mode.
    FsqliteDefault,
}

impl Seed {
    const fn journal_mode(self) -> &'static str {
        match self {
            Self::StockRollback => "delete",
            Self::StockWal | Self::FsqliteDefault => "wal",
        }
    }
}

fn path_text(path: &Path) -> String {
    path.to_str().expect("utf-8 temp path").to_owned()
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
}

fn rebuild_artifacts(dir: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(dir)
        .expect("list temp dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.contains(REBUILD_MARK))
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn seed_sql() -> String {
    let mut sql = String::from(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
         CREATE INDEX t_payload ON t(payload);
         BEGIN;",
    );
    for id in 1..=SEED_ROWS {
        sql.push_str(&format!(
            "INSERT INTO t VALUES ({id}, printf('%04d', {id}) || '{}');",
            "p".repeat(400)
        ));
    }
    sql.push_str(&format!("COMMIT; DELETE FROM t WHERE id < {KEPT_FROM};"));
    sql
}

fn expected_rows() -> (i64, i64) {
    let last = SEED_ROWS + ADDED_ROWS;
    let count = last - KEPT_FROM + 1;
    let sum = (KEPT_FROM..=last).sum::<i64>();
    (count, sum)
}

async fn fsqlite_rows(conn: &Connection) -> (i64, i64) {
    let rows = conn
        .query("SELECT COUNT(*), SUM(id) FROM t;")
        .await
        .expect("fsqlite row query");
    let values = rows[0].values();
    match (&values[0], &values[1]) {
        (SqliteValue::Integer(count), SqliteValue::Integer(sum)) => (*count, *sum),
        other => panic!("unexpected fsqlite row summary {other:?}"),
    }
}

async fn fsqlite_text(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.expect("fsqlite pragma");
    match &rows[0].values()[0] {
        SqliteValue::Text(text) => text.to_string(),
        other => panic!("{sql}: unexpected value {other:?}"),
    }
}

/// Stock (rusqlite's bundled SQLite) reads the VACUUMed file: integrity ok, no
/// free page, the journal mode kept, every row.
fn stock_accepts_the_vacuumed_database(db: &Path, seed: Seed) {
    let stock = rusqlite::Connection::open(db).expect("stock reopen");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("stock integrity_check");
    assert_eq!(
        integrity,
        "ok",
        "{seed:?}: stock {} integrity_check",
        rusqlite::version()
    );
    let freelist: i64 = stock
        .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
        .expect("stock freelist_count");
    assert_eq!(freelist, 0, "{seed:?}: VACUUM must leave no free page");
    let mode: String = stock
        .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
        .expect("stock journal_mode");
    assert_eq!(mode, seed.journal_mode(), "{seed:?}: VACUUM keeps the mode");
    let rows: (i64, i64) = stock
        .query_row("SELECT COUNT(*), SUM(id) FROM t;", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("stock rows");
    assert_eq!(rows, expected_rows(), "{seed:?}: stock rows after VACUUM");
}

/// Seed, VACUUM through fsqlite, then check both engines.
async fn vacuum_case(seed: Seed) {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join(format!("gfx4q-{seed:?}.db"));
    let db_text = path_text(&db);

    match seed {
        Seed::StockRollback | Seed::StockWal => {
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            let mode: String = stock
                .query_row(
                    &format!("PRAGMA journal_mode={};", seed.journal_mode()),
                    [],
                    |row| row.get(0),
                )
                .expect("stock journal_mode");
            assert_eq!(mode, seed.journal_mode());
            stock.execute_batch(&seed_sql()).expect("stock seed");
            drop(stock);
            assert!(
                !sidecar(&db, "-wal").exists() && !sidecar(&db, "-shm").exists(),
                "stock {} must close cleanly, without companions",
                rusqlite::version()
            );
        }
        Seed::FsqliteDefault => {}
    }

    let conn = Connection::open(&db_text).await.expect("fsqlite open");
    if seed == Seed::FsqliteDefault {
        conn.execute_batch(&seed_sql())
            .await
            .expect("fsqlite seed");
    }
    assert_eq!(
        fsqlite_text(&conn, "PRAGMA journal_mode;").await,
        seed.journal_mode(),
        "the case must run in the journal mode it names"
    );
    // Rows written by this connection right before VACUUM (WAL frames in WAL mode).
    for id in SEED_ROWS + 1..=SEED_ROWS + ADDED_ROWS {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({id}, printf('%04d', {id}) || 'late');"
        ))
        .await
        .expect("fsqlite insert before VACUUM");
    }

    conn.execute("VACUUM;")
        .await
        .unwrap_or_else(|error| panic!("{seed:?}: VACUUM of a file-backed database: {error}"));
    assert_eq!(
        rebuild_artifacts(dir.path()),
        Vec::<String>::new(),
        "{seed:?}: VACUUM left rebuild artifacts while the connection is open"
    );
    assert_eq!(fsqlite_rows(&conn).await, expected_rows(), "{seed:?}");
    conn.close().await.expect("fsqlite close");
    assert_eq!(
        rebuild_artifacts(dir.path()),
        Vec::<String>::new(),
        "{seed:?}: VACUUM left rebuild artifacts after close"
    );

    stock_accepts_the_vacuumed_database(&db, seed);

    let reopened = Connection::open(&db_text).await.expect("fsqlite reopen");
    assert_eq!(fsqlite_rows(&reopened).await, expected_rows(), "{seed:?}");
    assert_eq!(
        fsqlite_text(&reopened, "PRAGMA integrity_check;").await,
        "ok",
        "{seed:?}: fsqlite integrity_check after VACUUM"
    );
    reopened.close().await.expect("fsqlite close after reopen");
    assert_eq!(
        rebuild_artifacts(dir.path()),
        Vec::<String>::new(),
        "{seed:?}"
    );
}

#[test]
fn vacuum_succeeds_and_leaves_no_rebuild_artifact_in_rollback_journal_mode() {
    asupersync::test_utils::run_test(|| vacuum_case(Seed::StockRollback));
}

#[test]
fn vacuum_succeeds_and_leaves_no_rebuild_artifact_in_wal_mode() {
    asupersync::test_utils::run_test(|| vacuum_case(Seed::StockWal));
}

#[test]
fn vacuum_succeeds_and_leaves_no_rebuild_artifact_for_a_default_fsqlite_database() {
    asupersync::test_utils::run_test(|| vacuum_case(Seed::FsqliteDefault));
}

async fn guard(image: &Path) -> Result<(), FrankenError> {
    let cx = Cx::new();
    SimplePager::<UnixVfs>::inspect_self_contained_database_image_in_vfs(
        &UnixVfs::new(),
        &cx,
        image,
        PageSize::DEFAULT,
    )
    .await
    .map(|_receipt| ())
}

fn assert_refused(result: Result<(), FrankenError>, suffix: &str, what: &str) {
    match result {
        Err(FrankenError::DatabaseCorrupt { detail })
            if detail.contains("not self-contained") && detail.ends_with(&format!("{suffix} exists")) => {}
        other => panic!("{what}: the guard must refuse the {suffix} companion, got {other:?}"),
    }
}

#[test]
fn self_contained_guard_still_refuses_a_candidate_with_companions() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("src.db");
        let stock = rusqlite::Connection::open(&src).expect("stock open");
        let mode: String = stock
            .query_row("PRAGMA journal_mode=WAL;", [], |row| row.get(0))
            .expect("stock wal");
        assert_eq!(mode, "wal");
        stock
            .execute_batch("PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES (1);")
            .expect("stock seed");
        stock
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |_| Ok(()))
            .expect("stock checkpoint");
        // A clean WAL-format main file: every row is in it.
        let clean_main = std::fs::read(&src).expect("read clean main");
        stock
            .execute_batch("INSERT INTO t VALUES (2), (3);")
            .expect("stock frames");
        let page_size: i64 = stock
            .query_row("PRAGMA page_size;", [], |row| row.get(0))
            .expect("page size");
        let page_size = usize::try_from(page_size).expect("page size fits usize");
        let framed_wal = std::fs::read(sidecar(&src, "-wal")).expect("read live wal");
        assert!(
            framed_wal.len() >= 32 + 24 + page_size,
            "the stock -wal must hold a complete frame ({} bytes)",
            framed_wal.len()
        );
        drop(stock);

        let image = |name: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, &clean_main).expect("write image");
            path
        };

        // Controls: the bare image, and the narrow exception (a -wal too short
        // for one frame), are self-contained.
        let bare = image("bare.db");
        guard(&bare)
            .await
            .expect("a bare WAL-format image is self-contained");
        let short = image("short.db");
        std::fs::write(sidecar(&short, "-wal"), b"").expect("write empty wal");
        guard(&short)
            .await
            .expect("a -wal too short for one frame holds no state");

        // A companion that holds a complete frame: refused.
        let framed = image("framed.db");
        std::fs::write(sidecar(&framed, "-wal"), &framed_wal).expect("write framed wal");
        assert_refused(guard(&framed).await, "-wal", "framed -wal");

        // Any -shm, even beside no -wal: refused (the guard is not relaxed).
        let shm_only = image("shm_only.db");
        std::fs::write(sidecar(&shm_only, "-shm"), vec![0_u8; 32 * 1024]).expect("write shm");
        assert_refused(guard(&shm_only).await, "-shm", "-shm beside no -wal");

        // The pair b1b1a76df's read-only open builds (empty -wal, 32 KiB -shm).
        let pair = image("pair.db");
        std::fs::write(sidecar(&pair, "-wal"), b"").expect("write empty wal");
        std::fs::write(sidecar(&pair, "-shm"), vec![0_u8; 32 * 1024]).expect("write shm");
        assert_refused(guard(&pair).await, "-shm", "empty -wal plus -shm");
    });
}
