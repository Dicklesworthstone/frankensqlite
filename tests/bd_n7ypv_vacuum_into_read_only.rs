#![recursion_limit = "512"]

//! bd-n7ypv / GH#342: VACUUM INTO must be permitted on a read-only source
//! connection — it writes a NEW target file and only reads the source, so
//! sqlite3 allows it (verified: `sqlite3 'file:src?mode=ro' "VACUUM INTO ..."`
//! succeeds while plain `VACUUM` errors SQLITE_READONLY). Previously the
//! read-only guard rejected all VACUUM with the wrong `ReadOnly` kind.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

/// Set in the stock writer process. It holds the source's WAL open while the
/// parent copies the source, and its value is the scratch directory.
const WRITER_ENV: &str = "FSQLITE_VACUUM_INTO_WRITER";

/// Every header change sits in the WAL; nothing was ever checkpointed.
const FRESH_WAL_HEADER: &[&str] = &[
    "PRAGMA journal_mode=WAL",
    "PRAGMA wal_autocheckpoint=0",
    "PRAGMA user_version=231",
    "PRAGMA application_id=4242",
    "CREATE TABLE t(a)",
    "INSERT INTO t VALUES (1)",
];

/// The main file holds older values, so a copy of its header is wrong rather
/// than empty.
const UNCHECKPOINTED_HEADER_CHANGE: &[&str] = &[
    "PRAGMA journal_mode=WAL",
    "PRAGMA wal_autocheckpoint=0",
    "PRAGMA user_version=7",
    "PRAGMA application_id=77",
    "CREATE TABLE t(a)",
    "PRAGMA wal_checkpoint(TRUNCATE)",
    "PRAGMA user_version=231",
    "PRAGMA application_id=4242",
    "INSERT INTO t VALUES (1)",
];

/// The control: the main file is already current.
const CHECKPOINTED_HEADER: &[&str] = &[
    "PRAGMA journal_mode=WAL",
    "PRAGMA wal_autocheckpoint=0",
    "PRAGMA user_version=231",
    "PRAGMA application_id=4242",
    "CREATE TABLE t(a)",
    "INSERT INTO t VALUES (1)",
    "PRAGMA wal_checkpoint(TRUNCATE)",
];

fn stock_run(conn: &rusqlite::Connection, statements: &[&str]) {
    for sql in statements {
        let mut statement = conn.prepare(sql).expect("prepare");
        let mut rows = statement.query([]).expect("run");
        while rows.next().expect("step").is_some() {}
    }
}

fn stock_writer(dir: &Path, setup: &[&str]) {
    let conn = rusqlite::Connection::open(dir.join("src.db")).expect("stock writer");
    stock_run(&conn, setup);
    std::fs::write(dir.join("ready"), b"").expect("ready");
    wait_for(&dir.join("done"));
    // A clean close would checkpoint what the WAL holds.
    std::mem::forget(conn);
}

fn wait_for(path: &Path) -> bool {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !path.exists() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

/// `user_version`, `application_id` and the row count, as stock reads them.
fn stock_reads_copy(path: &Path) -> (i64, i64, i64) {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("stock opens the copy");
    let read = |sql: &str| {
        conn.query_row(sql, [], |row| row.get::<_, i64>(0))
            .expect("stock reads the copy")
    };
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(integrity, "ok");
    (
        read("PRAGMA user_version"),
        read("PRAGMA application_id"),
        read("SELECT count(*) FROM t"),
    )
}

fn vacuum_into_under_live_writer(test: &str, setup: &[&str]) {
    if let Ok(dir) = std::env::var(WRITER_ENV) {
        stock_writer(Path::new(&dir), setup);
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let mut writer = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(WRITER_ENV, dir.path())
        .spawn()
        .expect("spawn stock writer");
    assert!(wait_for(&dir.path().join("ready")), "stock writer not ready");

    let src = dir.path().join("src.db");
    let fsqlite_copy = dir.path().join("fsqlite-copy.db");
    let stock_copy = dir.path().join("stock-copy.db");
    let vacuum = |target: &Path| {
        format!(
            "VACUUM INTO '{}';",
            target.to_string_lossy().replace('\'', "''")
        )
    };
    asupersync::test_utils::run_test(|| async {
        let ro = Connection::open_schema_only(src.to_str().expect("utf-8 path"))
            .await
            .expect("open read-only source");
        ro.execute(&vacuum(&fsqlite_copy))
            .await
            .expect("VACUUM INTO on a read-only source");
        ro.close().await.expect("close read-only");
    });
    let stock_ro =
        rusqlite::Connection::open_with_flags(&src, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("stock read-only source");
    stock_run(&stock_ro, &[vacuum(&stock_copy).as_str()]);
    drop(stock_ro);

    std::fs::write(dir.path().join("done"), b"").expect("done");
    assert!(writer.wait().expect("stock writer exit").success());

    let copied = stock_reads_copy(&fsqlite_copy);
    assert_eq!(copied, stock_reads_copy(&stock_copy), "stock's own copy");
    assert_eq!(copied, (231, 4242, 1), "the writer's committed state");
}

#[test]
fn read_only_vacuum_into_keeps_a_fresh_wal_header() {
    vacuum_into_under_live_writer(
        "read_only_vacuum_into_keeps_a_fresh_wal_header",
        FRESH_WAL_HEADER,
    );
}

#[test]
fn read_only_vacuum_into_keeps_an_uncheckpointed_header_change() {
    vacuum_into_under_live_writer(
        "read_only_vacuum_into_keeps_an_uncheckpointed_header_change",
        UNCHECKPOINTED_HEADER_CHANGE,
    );
}

#[test]
fn read_only_vacuum_into_keeps_a_checkpointed_header() {
    vacuum_into_under_live_writer(
        "read_only_vacuum_into_keeps_a_checkpointed_header",
        CHECKPOINTED_HEADER,
    );
}

// FIXED (bd-n7ypv/GH#342): sqlite3 permits VACUUM INTO on a read-only source
// connection; fsqlite now matches. The fix: (1) exempt VACUUM INTO (into.is_some)
// from the read-only statement guard; (2) release the read-only connection's
// retained non-mutating read snapshot from active_txn before
// quiesce_pager_export_state (lossless — a read-only pager holds no writes);
// (3) skip the source WAL checkpoint on a read-only source (exported rows come
// WAL-aware from reload_memdb_from_pager, and a read-only source cannot be
// written). Plain VACUUM stays ReadOnly (into.is_none()).
#[test]
fn bd_n7ypv_vacuum_into_allowed_on_read_only_connection_plain_vacuum_still_rejected() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("src.db").to_string_lossy().into_owned();
        let target = dir.path().join("out.db").to_string_lossy().into_owned();
        {
            let conn = Connection::open(&src).await.expect("create source");
            conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create table");
            for i in 0..50 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}');"))
                    .await
                    .expect("insert");
            }
            conn.close().await.expect("close source");
        }

        // VACUUM INTO on a FRESH read-only source connection (open_schema_only
        // opens a read-only pager: is_readonly() == true). It writes a NEW
        // target and only reads the source -> allowed.
        {
            let ro = Connection::open_schema_only(&src)
                .await
                .expect("open read-only source");
            ro.execute(&format!("VACUUM INTO '{}';", target.replace('\'', "''")))
                .await
                .expect("VACUUM INTO must be allowed on a read-only source (GH#342)");
            ro.close().await.expect("close read-only");
        }

        // Plain VACUUM rewrites the source in place -> must stay fail-closed
        // with ReadOnly (matches sqlite3 SQLITE_READONLY).
        {
            let ro = Connection::open_schema_only(&src)
                .await
                .expect("reopen read-only source");
            let err = ro
                .execute("VACUUM;")
                .await
                .expect_err("plain VACUUM on a read-only connection must fail closed");
            assert!(
                matches!(err, FrankenError::ReadOnly),
                "plain VACUUM should be ReadOnly, got {err:?}"
            );
            ro.close().await.expect("close read-only");
        }

        // The vacuumed target is a valid database holding the source's rows.
        let out = Connection::open(&target)
            .await
            .expect("open vacuumed target");
        let rows = out
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("count target rows");
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(50),
            "vacuumed target must hold all source rows"
        );
        out.close().await.expect("close target");
    });
}
