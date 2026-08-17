//! bd-zywqc.14 adversarial corpus — database-header corruption.
//!
//! Corrupt a critical field of the 100-byte SQLite header (the 16-byte magic
//! string, or the page-size field) and verify the engine REJECTS the file
//! loudly on open — an error, never a panic and never a silent open that serves
//! wrong data. Cross-checked against stock C SQLite (rusqlite): both engines
//! must reject the same corrupted header (stock reports SQLITE_NOTADB).
//!
//! This exercises the open-path header validation, complementary to the
//! b-tree-structure checks in `adversarial_bit_flip` (which rely on
//! integrity_check). It also confirms the first-open repair pass (bd-zywqc.5)
//! never turns an unreadable header into silent success — a header this broken
//! fails before the pass would run.

use fsqlite_core::connection::Connection;

/// Build a clean, self-contained fsqlite database file.
async fn build_clean_db(dir: &std::path::Path, name: &str) -> String {
    let db = dir.join(name).to_string_lossy().into_owned();
    let conn = Connection::open(&db).await.expect("open");
    conn.execute("PRAGMA journal_mode=DELETE;")
        .await
        .expect("journal_mode");
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, v TEXT);")
        .await
        .expect("create");
    conn.execute("INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z');")
        .await
        .expect("insert");
    conn.close().await.expect("close");
    db
}

fn set_byte(db: &str, offset: usize, value: u8) {
    let mut bytes = std::fs::read(db).expect("read image");
    assert!(offset < bytes.len());
    bytes[offset] = value;
    std::fs::write(db, &bytes).expect("write corrupted image");
}

/// Did fsqlite reject the corrupted file? Rejection = a failed open, or a failed
/// first read. A successful open that then reads rows cleanly is a silent
/// accept (the failure mode this corpus exists to forbid).
async fn fsqlite_rejects(db: &str) -> bool {
    let Ok(conn) = Connection::open(db).await else {
        return true; // loud open failure
    };
    let rejected = conn.query("SELECT count(*) FROM t;").await.is_err()
        || conn
            .query("PRAGMA integrity_check;")
            .await
            .map(|rows| {
                !matches!(rows.first().map(|r| &r.values()[0]),
                    Some(fsqlite_types::value::SqliteValue::Text(s)) if s.as_ref() == "ok")
            })
            .unwrap_or(true);
    conn.close().await.ok();
    rejected
}

/// Did stock C SQLite reject it? Rejection = a failed open or a failed query
/// (stock opens lazily and reports SQLITE_NOTADB at first access).
fn stock_rejects(db: &str) -> bool {
    let Ok(conn) = rusqlite::Connection::open(db) else {
        return true;
    };
    conn.query_row("SELECT count(*) FROM t;", [], |r| r.get::<_, i64>(0))
        .is_err()
}

async fn assert_header_corruption_rejected(name: &str, offset: usize, value: u8) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = build_clean_db(dir.path(), &format!("{name}.db")).await;

    // Sanity: the pristine file is readable by stock.
    assert!(
        !stock_rejects(&db),
        "{name}: pristine fixture must be readable before corruption"
    );

    set_byte(&db, offset, value);

    assert!(
        stock_rejects(&db),
        "{name}: fixture invalid — stock accepted header corruption at offset {offset} \
         (value {value:#x}); choose a critical header field"
    );
    assert!(
        fsqlite_rejects(&db).await,
        "{name}: fsqlite SILENTLY ACCEPTED header corruption at offset {offset} \
         (value {value:#x}) that stock rejects — a broken header must fail loudly, not \
         open and serve wrong data"
    );
}

macro_rules! header_scenario {
    ($fn_name:ident, $label:literal, $off:expr, $val:expr) => {
        #[test]
        fn $fn_name() {
            asupersync::test_utils::run_test(|| async {
                assert_header_corruption_rejected($label, $off, $val).await;
            });
        }
    };
}

// The magic string "SQLite format 3\0" occupies offsets 0..16; any byte broken
// makes the file not a database. The page-size is a 2-byte big-endian field at
// offsets 16..18 constrained to a power of two in [512, 65536].
header_scenario!(header_magic_byte_0, "magic0", 0, 0xFF);
header_scenario!(header_magic_byte_8, "magic8", 8, 0x00);
header_scenario!(header_magic_byte_15, "magic15", 15, 0x01);
header_scenario!(header_page_size_high_invalid, "pagesize_hi", 16, 0xFF);
header_scenario!(header_page_size_low_not_pow2, "pagesize_lo", 17, 0x01);
