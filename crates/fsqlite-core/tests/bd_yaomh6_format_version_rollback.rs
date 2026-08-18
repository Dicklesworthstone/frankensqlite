//! bd-yaomh.6 — format-version rollback-safety handshake.
//!
//! A future release may change the on-disk `.fsqlite` format (e.g. add a
//! `.fsqlite-history` sidecar). If a user downgrades to an older fsqlite binary
//! it must NOT silently corrupt their database — it must detect a newer on-disk
//! format and refuse to open with a clear error.
//!
//! The format version is a big-endian `u32` stored at header bytes 72..76,
//! inside SQLite's "reserved for expansion" region (bytes 72..=91). Stock C
//! SQLite ignores that region, so stamping a non-zero value keeps the file
//! openable by stock — the make-or-break forward-compat constraint, proven
//! empirically below with `rusqlite` (bundled stock SQLite) + `integrity_check`.
//!
//! Coverage:
//!   (a) a current binary opens a legacy DB (format_version == 0) → ok;
//!   (b) round-trip: write header with format_version == CURRENT, reparse → equal;
//!   (c) a DB whose on-disk format_version > CURRENT refuses cleanly with the
//!       documented `NewerFormat` variant + message text;
//!   (d) stock `rusqlite` opens a format-stamped DB and `integrity_check == ok`
//!       (the forward-compat oracle), including a stamp *newer* than CURRENT.

use fsqlite_core::connection::Connection;
use fsqlite_types::{
    CURRENT_FSQLITE_FORMAT_VERSION, DATABASE_HEADER_SIZE, DatabaseHeader, DatabaseHeaderError,
};

/// Byte offset of the big-endian `u32` format-version field in the 100-byte
/// header (start of SQLite's reserved-for-expansion region).
const FORMAT_VERSION_OFFSET: usize = 72;

/// Read the first [`DATABASE_HEADER_SIZE`] bytes of a file into a fixed array.
fn read_header(path: &std::path::Path) -> [u8; DATABASE_HEADER_SIZE] {
    let raw = std::fs::read(path).expect("read database file");
    assert!(
        raw.len() >= DATABASE_HEADER_SIZE,
        "database file shorter than a header: {} bytes",
        raw.len()
    );
    let mut header = [0_u8; DATABASE_HEADER_SIZE];
    header.copy_from_slice(&raw[..DATABASE_HEADER_SIZE]);
    header
}

/// Stamp a big-endian `u32` at [`FORMAT_VERSION_OFFSET`] of an existing file's
/// page 1, perturbing nothing else — the truest test of whether the chosen
/// reserved offset breaks stock C SQLite.
fn stamp_format_version(path: &std::path::Path, version: u32) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open database file for stamping");
    file.seek(SeekFrom::Start(FORMAT_VERSION_OFFSET as u64))
        .expect("seek to format-version offset");
    file.write_all(&version.to_be_bytes())
        .expect("write format-version stamp");
    file.flush().expect("flush stamp");
}

/// Create a self-contained stock SQLite database (rollback-journal mode, so the
/// committed image lives entirely in the main file — no WAL sidecar) with some
/// real b-tree content, and return its path plus the row count.
fn make_stock_db(path: &std::path::Path) -> i64 {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    // `execute_batch` discards result rows, so it is safe for the journal_mode
    // pragma (which returns the mode). Rollback-journal keeps the committed
    // image in the main file — no WAL sidecar to hide data behind.
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
         CREATE INDEX t_v ON t(v);
         INSERT INTO t(id, v) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');",
    )
    .expect("populate stock db");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("count rows");
    conn.close().expect("close stock db");
    rows
}

/// Open `path` with stock SQLite and assert `PRAGMA integrity_check == 'ok'` and
/// that the planted rows survive. This is the forward-compat oracle.
fn assert_stock_ok(path: &std::path::Path, expected_rows: i64) {
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let verdict: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "stock integrity_check must be ok");
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("count rows after stamp");
    assert_eq!(rows, expected_rows, "stock must still read all rows");
    conn.close().expect("close");
}

/// (a) A current binary opens a legacy database (format_version == 0) → ok.
#[test]
fn legacy_zero_format_opens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("legacy.db");
    let rows = make_stock_db(&path);

    // A stock-created DB leaves the whole reserved region zero → legacy/v1.
    let header_bytes = read_header(&path);
    assert_eq!(
        &header_bytes[FORMAT_VERSION_OFFSET..FORMAT_VERSION_OFFSET + 4],
        &[0, 0, 0, 0],
        "stock leaves the format-version slot zero"
    );
    let header = DatabaseHeader::from_bytes(&header_bytes).expect("legacy header opens");
    assert_eq!(header.format_version, 0, "legacy is treated as v0");

    // And stock still agrees the file is fine.
    assert_stock_ok(&path, rows);
}

/// (b) Round-trip: write a header with format_version == CURRENT, reparse → equal.
#[test]
fn current_format_roundtrips_equal() {
    let mut header = DatabaseHeader::default();
    header.set_format_version(CURRENT_FSQLITE_FORMAT_VERSION);
    let bytes = header.to_bytes().expect("serialize");
    assert_eq!(
        u32::from_be_bytes([
            bytes[FORMAT_VERSION_OFFSET],
            bytes[FORMAT_VERSION_OFFSET + 1],
            bytes[FORMAT_VERSION_OFFSET + 2],
            bytes[FORMAT_VERSION_OFFSET + 3],
        ]),
        CURRENT_FSQLITE_FORMAT_VERSION,
        "format_version lands big-endian at byte 72"
    );
    let reparsed = DatabaseHeader::from_bytes(&bytes).expect("reparse");
    assert_eq!(reparsed, header, "header round-trips unchanged");
    assert_eq!(reparsed.format_version, CURRENT_FSQLITE_FORMAT_VERSION);
}

/// (c) A database whose on-disk format_version > CURRENT refuses cleanly with
/// the documented `NewerFormat` variant and message text.
#[test]
fn newer_format_refuses_to_open() {
    let mut header = DatabaseHeader::default();
    header.set_format_version(CURRENT_FSQLITE_FORMAT_VERSION + 1);
    let bytes = header.to_bytes().expect("serialize newer header");

    let err = DatabaseHeader::from_bytes(&bytes).expect_err("must refuse newer format");
    assert_eq!(
        err,
        DatabaseHeaderError::NewerFormat {
            on_disk: CURRENT_FSQLITE_FORMAT_VERSION + 1,
            supported: CURRENT_FSQLITE_FORMAT_VERSION,
        }
    );

    // The message names the cause and the two remedies (upgrade / restore).
    let msg = err.to_string();
    assert!(
        msg.contains("written by a newer fsqlite"),
        "message must name the cause: {msg}"
    );
    assert!(
        msg.contains("refusing to open to prevent corruption"),
        "message must state the refusal: {msg}"
    );
    assert!(
        msg.contains("upgrade fsqlite") && msg.contains("restore from a pre-upgrade backup"),
        "message must state both remedies: {msg}"
    );

    // Equal-to-CURRENT still opens (strict `>` boundary).
    let mut equal = DatabaseHeader::default();
    equal.set_format_version(CURRENT_FSQLITE_FORMAT_VERSION);
    let equal_bytes = equal.to_bytes().expect("serialize equal header");
    assert!(
        DatabaseHeader::from_bytes(&equal_bytes).is_ok(),
        "format_version == CURRENT must open"
    );
}

/// (d) Forward-compat oracle: stock C SQLite (rusqlite, bundled) opens a
/// format-stamped database and `integrity_check == ok`. This proves the chosen
/// reserved offset (72..76) does not break stock — the make-or-break constraint.
#[test]
fn stock_sqlite_opens_format_stamped_db() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stamped.db");
    let rows = make_stock_db(&path);

    // Baseline: stock is happy before any stamp.
    assert_stock_ok(&path, rows);

    // Stamp the CURRENT version into the reserved region, perturbing nothing
    // else, then confirm stock still opens it with integrity_check == ok.
    stamp_format_version(&path, CURRENT_FSQLITE_FORMAT_VERSION);
    let header_bytes = read_header(&path);
    assert_eq!(
        u32::from_be_bytes([
            header_bytes[FORMAT_VERSION_OFFSET],
            header_bytes[FORMAT_VERSION_OFFSET + 1],
            header_bytes[FORMAT_VERSION_OFFSET + 2],
            header_bytes[FORMAT_VERSION_OFFSET + 3],
        ]),
        CURRENT_FSQLITE_FORMAT_VERSION,
        "stamp survives on disk"
    );
    assert_stock_ok(&path, rows);

    // Our parser accepts the CURRENT stamp on the real stock file.
    let parsed = DatabaseHeader::from_bytes(&header_bytes).expect("parse stamped stock header");
    assert_eq!(parsed.format_version, CURRENT_FSQLITE_FORMAT_VERSION);

    // Stamp a version NEWER than CURRENT: our engine now refuses (the downgrade
    // guard), yet stock C SQLite is STILL unaffected — forward compat holds for
    // any value in the reserved slot.
    stamp_format_version(&path, CURRENT_FSQLITE_FORMAT_VERSION + 1);
    let newer_bytes = read_header(&path);
    let refuse = DatabaseHeader::from_bytes(&newer_bytes)
        .expect_err("engine refuses a newer stamp on a real file");
    assert!(matches!(refuse, DatabaseHeaderError::NewerFormat { .. }));
    assert_stock_ok(&path, rows);
}

/// (e) bd-e9hws: the FULL `Connection::open` path (pager open + first-open
/// migration) must REFUSE a database whose on-disk format is newer than this
/// build understands — not open it, "repair" it, or rewrite it — while stock C
/// SQLite still opens the same bytes. Guards the end-to-end rollback-safety
/// guarantee above the header-parse unit level.
#[test]
fn connection_open_refuses_newer_format_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("newer_open.db");
        let rows = make_stock_db(&path);

        // Stamp a format newer than this build supports; stock is unaffected by
        // the reserved-slot value (forward-compat).
        stamp_format_version(&path, CURRENT_FSQLITE_FORMAT_VERSION + 1);
        assert_stock_ok(&path, rows);

        // fsqlite must refuse to open it.
        let err = Connection::open(path.to_string_lossy().as_ref())
            .await
            .err()
            .expect("Connection::open must refuse a newer-format database");

        // The refusal diagnostic names the newer-format cause regardless of
        // which storage layer (pager open vs integrity walk) rejects it first.
        let msg = err.to_string();
        assert!(
            msg.contains("newer fsqlite"),
            "refusal must name the newer-format cause; got: {msg}"
        );
        // bd-3j2c5: the refusal now surfaces the dedicated
        // SQLITE_OPEN_NEWER_FORMAT extended code (not SQLITE_CORRUPT) across all
        // header-mapping sites (connection.rs / compat_persist.rs / pager.rs), so
        // a downgraded binary can tell "written by a newer fsqlite" apart from a
        // genuinely damaged header regardless of which layer rejects it first.
        assert_eq!(
            err.extended_error_code(),
            fsqlite_error::SQLITE_OPEN_NEWER_FORMAT,
            "newer-format open must surface SQLITE_OPEN_NEWER_FORMAT (32526), not \
             SQLITE_CORRUPT; got extended code {}",
            err.extended_error_code()
        );

        // The refused file's page-1 header is untouched: we rejected it, we did
        // not rewrite or "repair" it (the stamp survives byte-for-byte).
        let after = read_header(&path);
        assert_eq!(
            u32::from_be_bytes([
                after[FORMAT_VERSION_OFFSET],
                after[FORMAT_VERSION_OFFSET + 1],
                after[FORMAT_VERSION_OFFSET + 2],
                after[FORMAT_VERSION_OFFSET + 3],
            ]),
            CURRENT_FSQLITE_FORMAT_VERSION + 1,
            "a refused newer-format file must not be rewritten/repaired"
        );
    });
}
