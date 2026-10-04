//! bd-25au0: stock SQLite leaves the header's schema format (bytes 44..48) and
//! often its text encoding (bytes 56..60) at 0 while the schema is empty, e.g.
//! for a file that only ever saw `PRAGMA user_version`, or after the last table
//! is dropped and the file VACUUMed. fsqlite refused every such file with
//! "invalid schema format: 0", a common first step of migration tools.
//!
//! Expected (stock 3.53 via rusqlite is the oracle): fsqlite opens these files
//! read-only and read-write, writes nothing on a read-only open, keeps the
//! empty-schema stamp on non-schema writes, and stamps format 4 plus the
//! encoding exactly as stock does when the first table is created, including
//! through ATTACH. `VACUUM INTO` of such a file is readable by stock.

// The async engine futures nest deeply; match the other integration suites.
#![recursion_limit = "512"]

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Stock-written files whose schema is empty.
#[derive(Clone, Copy, Debug)]
enum Fixture {
    /// `PRAGMA user_version` only: schema format 0, encoding 0.
    UserVersionOnly,
    /// A table created, dropped and VACUUMed away: format 0, encoding 1.
    DroppedAndVacuumed,
    /// WAL mode plus `PRAGMA user_version`: format 0, encoding 0, WAL header bytes.
    WalUserVersionOnly,
}

const FIXTURES: [Fixture; 3] = [
    Fixture::UserVersionOnly,
    Fixture::DroppedAndVacuumed,
    Fixture::WalUserVersionOnly,
];

fn stage(fixture: Fixture, db: &Path) {
    let conn = rusqlite::Connection::open(db).expect("stock open");
    match fixture {
        Fixture::UserVersionOnly => conn.execute_batch("PRAGMA user_version=5;"),
        Fixture::DroppedAndVacuumed => conn
            .execute_batch("PRAGMA user_version=5; CREATE TABLE gone(x); DROP TABLE gone; VACUUM;"),
        Fixture::WalUserVersionOnly => {
            let mode: String = conn
                .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
                .expect("wal");
            assert_eq!(mode, "wal");
            conn.execute_batch("PRAGMA user_version=5;")
        }
    }
    .expect("stage fixture");
    drop(conn);
    let header = header_bytes(db);
    assert_eq!(
        &header[44..48],
        &[0; 4],
        "{fixture:?}: stock leaves schema format 0"
    );
    let expected_encoding: u32 = match fixture {
        Fixture::DroppedAndVacuumed => 1,
        Fixture::UserVersionOnly | Fixture::WalUserVersionOnly => 0,
    };
    assert_eq!(
        &header[56..60],
        &expected_encoding.to_be_bytes(),
        "{fixture:?}: stock text encoding field"
    );
}

/// The authoritative header: stock checkpoints any WAL into the main file and
/// removes it when its last connection closes.
fn checkpointed_header(db: &Path) -> [u8; 100] {
    {
        let conn = rusqlite::Connection::open(db).expect("stock checkpoint open");
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .expect("checkpoint");
    }
    header_bytes(db)
}

fn header_bytes(db: &Path) -> [u8; 100] {
    let bytes = std::fs::read(db).expect("read database file");
    bytes[..100].try_into().expect("100-byte header")
}

fn copy_db(src: &Path, dst: &Path) {
    std::fs::copy(src, dst).expect("copy fixture");
}

fn stock_check(db: &Path) -> (String, i64) {
    let conn = rusqlite::Connection::open(db).expect("stock reopen");
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    let user_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    (check, user_version)
}

fn path_str(path: &Path) -> String {
    path.to_str().expect("utf-8 path").to_owned()
}

fn fixture_paths(dir: &Path, fixture: Fixture, label: &str) -> (PathBuf, PathBuf) {
    let fsqlite_db = dir.join(format!("{fixture:?}-{label}-fsqlite.db"));
    let stock_db = dir.join(format!("{fixture:?}-{label}-stock.db"));
    stage(fixture, &fsqlite_db);
    copy_db(&fsqlite_db, &stock_db);
    (fsqlite_db, stock_db)
}

fn single_integer(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(value) => value,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

#[test]
fn read_only_open_reads_empty_stock_schema_without_writing() {
    let dir = tempfile::tempdir().expect("tempdir");
    for fixture in FIXTURES {
        let db = dir.path().join(format!("{fixture:?}-ro.db"));
        stage(fixture, &db);
        let before = std::fs::read(&db).expect("snapshot");
        let path = path_str(&db);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open_schema_only(&path)
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: read-only open failed: {err}"));
            let version = conn
                .query("PRAGMA user_version")
                .await
                .expect("user_version");
            assert_eq!(single_integer(&version), 5, "{fixture:?}");
            let objects = conn
                .query("SELECT count(*) FROM sqlite_master")
                .await
                .expect("sqlite_master");
            assert_eq!(single_integer(&objects), 0, "{fixture:?}");
            conn.close().await.expect("close");
        });
        assert_eq!(
            std::fs::read(&db).expect("re-read"),
            before,
            "{fixture:?}: a read-only open must not touch the file"
        );
    }
}

#[test]
fn first_create_table_stamps_header_like_stock() {
    let dir = tempfile::tempdir().expect("tempdir");
    for fixture in FIXTURES {
        let (fsqlite_db, stock_db) = fixture_paths(dir.path(), fixture, "create");
        let path = path_str(&fsqlite_db);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path)
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: read-write open failed: {err}"));
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .expect("create");
            conn.execute("INSERT INTO t(v) VALUES ('a'), ('b')")
                .await
                .expect("insert");
            conn.close().await.expect("close");
        });
        {
            let conn = rusqlite::Connection::open(&stock_db).expect("stock open");
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t(v) VALUES ('a'), ('b');",
            )
            .expect("stock create");
        }
        let ours = checkpointed_header(&fsqlite_db);
        let theirs = checkpointed_header(&stock_db);
        assert_eq!(
            &theirs[44..48],
            &4u32.to_be_bytes(),
            "{fixture:?}: stock stamps 4"
        );
        assert_eq!(&ours[44..48], &theirs[44..48], "{fixture:?}: schema format");
        assert_eq!(&ours[56..60], &theirs[56..60], "{fixture:?}: text encoding");
        assert_eq!(&ours[60..64], &theirs[60..64], "{fixture:?}: user_version");
        assert_eq!(
            stock_check(&fsqlite_db),
            ("ok".to_owned(), 5),
            "{fixture:?}"
        );
        let conn = rusqlite::Connection::open(&fsqlite_db).expect("stock read");
        let values: Vec<String> = conn
            .prepare("SELECT v FROM t ORDER BY id")
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        assert_eq!(values, ["a", "b"], "{fixture:?}");
    }
}

#[test]
fn user_version_write_keeps_the_empty_schema_stamp() {
    let dir = tempfile::tempdir().expect("tempdir");
    for fixture in FIXTURES {
        let (fsqlite_db, stock_db) = fixture_paths(dir.path(), fixture, "uv");
        let path = path_str(&fsqlite_db);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path)
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: read-write open failed: {err}"));
            conn.execute("PRAGMA user_version=7")
                .await
                .expect("user_version");
            conn.close().await.expect("close");
        });
        {
            let conn = rusqlite::Connection::open(&stock_db).expect("stock open");
            conn.execute_batch("PRAGMA user_version=7;")
                .expect("stock user_version");
        }
        let ours = checkpointed_header(&fsqlite_db);
        let theirs = checkpointed_header(&stock_db);
        assert_eq!(&ours[44..48], &theirs[44..48], "{fixture:?}: schema format");
        assert_eq!(&ours[56..60], &theirs[56..60], "{fixture:?}: text encoding");
        assert_eq!(
            stock_check(&fsqlite_db),
            ("ok".to_owned(), 7),
            "{fixture:?}"
        );
    }
}

#[test]
fn attach_creates_first_table_like_stock() {
    let dir = tempfile::tempdir().expect("tempdir");
    for fixture in FIXTURES {
        let (aux_db, stock_aux_db) = fixture_paths(dir.path(), fixture, "attach");
        let main_db = dir.path().join(format!("{fixture:?}-attach-main.db"));
        let main_path = path_str(&main_db);
        let aux_path = path_str(&aux_db);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&main_path).await.expect("main open");
            conn.execute(&format!("ATTACH '{aux_path}' AS aux"))
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: ATTACH failed: {err}"));
            conn.execute("CREATE TABLE aux.t(x)").await.expect("create");
            conn.execute("INSERT INTO aux.t VALUES (42)")
                .await
                .expect("insert");
            conn.execute("DETACH aux").await.expect("detach");
            conn.close().await.expect("close");
        });
        {
            let conn =
                rusqlite::Connection::open(dir.path().join("stock-main.db")).expect("stock main");
            conn.execute("ATTACH ?1 AS aux", [stock_aux_db.to_str().expect("utf-8")])
                .expect("stock attach");
            conn.execute_batch("CREATE TABLE aux.t(x); INSERT INTO aux.t VALUES (42); DETACH aux;")
                .expect("stock create");
        }
        let ours = checkpointed_header(&aux_db);
        let theirs = checkpointed_header(&stock_aux_db);
        assert_eq!(&ours[44..48], &theirs[44..48], "{fixture:?}: schema format");
        assert_eq!(&ours[56..60], &theirs[56..60], "{fixture:?}: text encoding");
        assert_eq!(stock_check(&aux_db), ("ok".to_owned(), 5), "{fixture:?}");
        let conn = rusqlite::Connection::open(&aux_db).expect("stock read");
        let x: i64 = conn
            .query_row("SELECT x FROM t", [], |row| row.get(0))
            .expect("row");
        assert_eq!(x, 42, "{fixture:?}");
    }
}

#[test]
fn vacuum_into_of_empty_stock_schema_is_stock_readable() {
    let dir = tempfile::tempdir().expect("tempdir");
    for fixture in FIXTURES {
        let db = dir.path().join(format!("{fixture:?}-vacuum.db"));
        stage(fixture, &db);
        let target = dir.path().join(format!("{fixture:?}-vacuum-into.db"));
        let path = path_str(&db);
        let target_path = path_str(&target);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path)
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: read-write open failed: {err}"));
            conn.execute(&format!("VACUUM INTO '{target_path}'"))
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: VACUUM INTO failed: {err}"));
            conn.close().await.expect("close");
        });
        assert_eq!(stock_check(&target), ("ok".to_owned(), 5), "{fixture:?}");
        let target_path = path_str(&target);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&target_path)
                .await
                .unwrap_or_else(|err| panic!("{fixture:?}: VACUUM INTO output open: {err}"));
            let version = conn
                .query("PRAGMA user_version")
                .await
                .expect("user_version");
            assert_eq!(single_integer(&version), 5, "{fixture:?}");
            conn.close().await.expect("close");
        });
    }
}
