//! GH#354 (bd-gy3a9): the persistent database-header field `default_cache_size`
//! (bytes 48..52, big-endian `i32`) must be `0` ("unset") on a freshly created
//! database, matching stock SQLite. The engine previously stamped its *runtime*
//! default (`-2000`) there, so every fsqlite-created file looked like the client
//! had explicitly requested a cache size it never asked for — a file-format
//! divergence that breaks any consumer pinning/preserving header fields.
//!
//! Both halves the issue requests are verified:
//!   1. (bd-gy3a9) Create a database without touching the pragma -> bytes 48..52
//!      == 0, and stock still reports -2000 at the pragma surface.
//!   2. (bd-n7eih) An explicit `PRAGMA default_cache_size=N` persists to bytes
//!      48..52 (stored as abs(N), matching stock's sqlite3AbsInt32), survives
//!      reopen, and is echoed in-session — cross-checked against the C-SQLite
//!      file-format oracle.

use fsqlite::Connection;

/// Read header bytes 48..52 of a SQLite database file as a big-endian `i32`.
fn header_default_cache_size(path: &str) -> i32 {
    let bytes = std::fs::read(path).expect("read database file");
    assert!(
        bytes.len() >= 52,
        "database file shorter than the 100-byte header: {} bytes",
        bytes.len()
    );
    assert_eq!(
        &bytes[..16],
        b"SQLite format 3\0",
        "file is not a stock SQLite image"
    );
    i32::from_be_bytes([bytes[48], bytes[49], bytes[50], bytes[51]])
}

#[test]
fn fresh_database_leaves_default_cache_size_unset_gh354() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir
            .path()
            .join("hdr_dcs.sqlite")
            .to_string_lossy()
            .into_owned();

        let conn = Connection::open(&db).await.expect("open database");
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
            .await
            .expect("create table");
        conn.execute("INSERT INTO t VALUES (1, 'x')")
            .await
            .expect("insert row");
        conn.close().await.expect("close database");

        let stored = header_default_cache_size(&db);
        assert_eq!(
            stored, 0,
            "fresh database must carry default_cache_size=0 in header bytes 48..52, \
             not the runtime default; stock writes 0 unless PRAGMA default_cache_size is set"
        );

        // The written image must also be readable by the C-SQLite oracle.
        let oracle = rusqlite::Connection::open(&db).expect("oracle opens database");
        let oracle_dcs: i64 = oracle
            .query_row("PRAGMA default_cache_size", [], |row| row.get(0))
            .expect("oracle reads default_cache_size");
        // Stock reports the runtime default (-2000) at the pragma surface even
        // though the persisted field is 0.
        assert_eq!(
            oracle_dcs, -2000,
            "oracle runtime default_cache_size should be the compiled default"
        );
    });
}

/// Read header bytes 48..52 of a stock database written by rusqlite with the
/// given `PRAGMA default_cache_size` assignment, as the file-format oracle.
fn oracle_header_default_cache_size(dir: &std::path::Path, name: &str, assign: &str) -> i32 {
    let path = dir.join(name);
    let conn = rusqlite::Connection::open(&path).expect("oracle open");
    conn.execute_batch(&format!(
        "PRAGMA default_cache_size={assign}; CREATE TABLE t(a);"
    ))
    .expect("oracle set + create");
    drop(conn);
    header_default_cache_size(path.to_str().expect("path utf8"))
}

#[test]
fn explicit_default_cache_size_pragma_persists_gh354() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir
            .path()
            .join("hdr_dcs_explicit.sqlite")
            .to_string_lossy()
            .into_owned();

        let conn = Connection::open(&db).await.expect("open database");
        conn.execute("PRAGMA default_cache_size=5000")
            .await
            .expect("set default_cache_size");
        // In-session the bare query echoes the set value (matching stock).
        let in_session = conn
            .query("PRAGMA default_cache_size")
            .await
            .expect("query default_cache_size in session");
        assert_eq!(
            in_session[0].values()[0].clone(),
            fsqlite::SqliteValue::Integer(5000),
            "in-session PRAGMA default_cache_size must echo the set value"
        );
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY)")
            .await
            .expect("create table");
        conn.close().await.expect("close database");

        let stored = header_default_cache_size(&db);
        assert_eq!(
            stored, 5000,
            "explicit PRAGMA default_cache_size=5000 must persist to header bytes 48..52"
        );
        assert_eq!(
            stored,
            oracle_header_default_cache_size(dir.path(), "oracle_5000.db", "5000"),
            "header value must match the C-SQLite oracle for default_cache_size=5000"
        );

        // And it must survive reopen.
        let reopened = Connection::open(&db).await.expect("reopen database");
        let rows = reopened
            .query("PRAGMA default_cache_size")
            .await
            .expect("query default_cache_size");
        assert_eq!(
            rows[0].values()[0].clone(),
            fsqlite::SqliteValue::Integer(5000),
            "explicit default_cache_size must survive reopen"
        );
        reopened.close().await.expect("close reopened");
    });
}

#[test]
fn default_cache_size_negative_is_stored_as_abs_gh354() {
    asupersync::test_utils::run_test(|| async {
        // Stock stores abs(N) as a page count (legacy sqlite3AbsInt32): a
        // negative assignment persists positive. `-3000` -> header 3000.
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir
            .path()
            .join("hdr_dcs_neg.sqlite")
            .to_string_lossy()
            .into_owned();

        let conn = Connection::open(&db).await.expect("open database");
        conn.execute("PRAGMA default_cache_size=-3000")
            .await
            .expect("set default_cache_size");
        conn.execute("CREATE TABLE t(a)")
            .await
            .expect("create table");
        conn.close().await.expect("close database");

        let stored = header_default_cache_size(&db);
        assert_eq!(
            stored, 3000,
            "PRAGMA default_cache_size=-3000 must persist as abs -> 3000"
        );
        assert_eq!(
            stored,
            oracle_header_default_cache_size(dir.path(), "oracle_neg.db", "-3000"),
            "abs-store behaviour must match the C-SQLite oracle"
        );

        let reopened = Connection::open(&db).await.expect("reopen database");
        let rows = reopened
            .query("PRAGMA default_cache_size")
            .await
            .expect("query default_cache_size");
        assert_eq!(
            rows[0].values()[0].clone(),
            fsqlite::SqliteValue::Integer(3000),
            "abs-stored default_cache_size must survive reopen"
        );
        reopened.close().await.expect("close reopened");
    });
}
