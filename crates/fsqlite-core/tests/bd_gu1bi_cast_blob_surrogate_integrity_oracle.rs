//! bd-gu1bi (REVIEW3-P1) acceptance keeper: `PRAGMA integrity_check` on a **stock
//! UTF-16** database must agree with stock sqlite3 ("ok") for the two index
//! shapes whose recomputed key needs the DB-ENCODED bytes, not canonical text:
//!
//!  1. **`CAST(y AS BLOB)` expression index (P1).** Stock evaluates the index
//!     expression against the stored value, so `CAST(y AS BLOB)` yields the raw
//!     UTF-16 bytes as the blob key. d17976bd3 made the integrity walker decode
//!     the row payload *canonically* (UTF-16 -> Unicode) — correct for
//!     encoding-insensitive text exprs like `lower(y)`, but for `CAST(y AS BLOB)`
//!     it produces the UTF-8 bytes of the decoded text, which never match the
//!     stored UTF-16 blob -> false "index ... does not match the table row
//!     payload" + a spurious first-open migration.
//!  2. **Stock-legal unpaired surrogate in a plain-indexed column (P2, same
//!     sites).** A UTF-16 TEXT value carrying an unpaired surrogate (e.g. U+D800,
//!     bytes `00 D8` in UTF-16le) round-trips byte-for-byte in stock. The
//!     canonical decode is lossy (U+FFFD, re-encoded `FD FF`) and diverges from
//!     the stored `00 D8`, false-flagging ANY indexed column.
//!
//! The fix recomputes each expected index key TWO ways — canonically (fixes
//! `lower(y)`) and byte-preserving (fixes `CAST(y AS BLOB)` and surrogates) — and
//! accepts the stored key if it matches EITHER. A genuinely corrupt key matches
//! neither, so detection power is preserved.
//!
//! rusqlite (bundled C SQLite) is the oracle. UTF-8 controls guard the common
//! path (the byte-preserving candidate is byte-identical to the canonical one on
//! UTF-8, so this is zero behavior change there).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::path::{Path, PathBuf};

/// Header text-encoding byte (offset 56, big-endian 4 bytes): 1=UTF-8,
/// 2=UTF-16le, 3=UTF-16be.
fn header_text_encoding(path: &Path) -> u32 {
    let bytes = std::fs::read(path).expect("read database file header");
    assert!(
        bytes.len() >= 60,
        "database shorter than its 100-byte header"
    );
    u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]])
}

/// `<db_path>.pre-migration-bak` — the spurious-migration fingerprint.
fn pre_migration_backup_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".pre-migration-bak");
    PathBuf::from(s)
}

/// Run `PRAGMA integrity_check` against fsqlite and return the flattened text
/// rows (stock returns exactly `["ok"]` for a clean DB).
async fn integrity_check(conn: &Connection) -> Vec<String> {
    conn.query("PRAGMA integrity_check")
        .await
        .expect("run PRAGMA integrity_check")
        .iter()
        .flat_map(|r| {
            r.values().iter().map(|v| match v {
                SqliteValue::Text(s) => s.to_string(),
                other => format!("{other:?}"),
            })
        })
        .collect()
}

/// Open the stock-written DB with fsqlite and assert integrity_check == `["ok"]`
/// with no spurious first-open migration.
async fn assert_fsqlite_integrity_ok(path: &Path, label: &str) {
    let fconn = Connection::open(path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite failed to open the DB: {e}"));

    let backup = pre_migration_backup_path(path);
    assert!(
        !backup.exists(),
        "[{label}] spurious first-open migration fired: {} exists",
        backup.display()
    );

    let got = integrity_check(&fconn).await;
    assert_eq!(
        got,
        vec!["ok".to_owned()],
        "[{label}] PRAGMA integrity_check diverged from stock on a clean UTF-16 DB (bd-gu1bi): {got:?}"
    );
}

/// Assert stock's own integrity_check is "ok" (the premise this keeper
/// differentiates against) and the requested encoding persisted.
fn assert_stock_premise(path: &Path, encoding: &str) {
    let conn = rusqlite::Connection::open(path).expect("reopen stock SQLite for validation");
    let got: String = conn
        .query_row("PRAGMA encoding", [], |r| r.get(0))
        .expect("read back PRAGMA encoding");
    assert_eq!(
        got.to_ascii_lowercase(),
        encoding.to_ascii_lowercase(),
        "fixture failed to persist requested encoding"
    );
    let stock_integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("stock integrity_check");
    assert_eq!(
        stock_integrity, "ok",
        "premise: stock integrity_check must be ok for the {encoding} CAST-AS-BLOB fixture"
    );
}

// --- P1: CAST(y AS BLOB) expression index ---------------------------------

/// Schema/rows exactly matching the bead's VERIFY, extended with a Latin-1 row
/// so the recompute is exercised on high bytes (invalid-UTF-8 `C0 00`) as well as
/// pure ASCII.
fn build_stock_cast_blob_db(path: &Path, encoding: &str) {
    {
        let conn = rusqlite::Connection::open(path).expect("open stock SQLite writer");
        conn.execute_batch(&format!("PRAGMA encoding = '{encoding}';"))
            .expect("set PRAGMA encoding on empty DB");
        conn.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, y TEXT);\n\
             INSERT INTO t(id, y) VALUES (1, 'MiXeD'), (2, 'ÀÉÎ'), (3, 'café');\n\
             CREATE INDEX ib ON t(CAST(y AS BLOB));",
        )
        .expect("apply CAST-AS-BLOB fixture");
    }
    assert_stock_premise(path, encoding);
}

async fn run_cast_blob_case(encoding: &str, expect_header: u32, label: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join(format!("bd_gu1bi_castblob_{label}.db"));
    build_stock_cast_blob_db(&path, encoding);
    assert_eq!(
        header_text_encoding(&path),
        expect_header,
        "[{label}] fixture did not persist the requested encoding (byte @56)"
    );
    assert_fsqlite_integrity_ok(&path, label).await;
}

#[test]
fn bd_gu1bi_utf16le_cast_blob_expression_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        run_cast_blob_case("UTF-16le", 2, "utf16le").await;
    });
}

#[test]
fn bd_gu1bi_utf16be_cast_blob_expression_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        run_cast_blob_case("UTF-16be", 3, "utf16be").await;
    });
}

/// UTF-8 control: the byte-preserving candidate is byte-identical to the
/// canonical one on UTF-8, so a clean UTF-8 DB with the same index must stay "ok".
#[test]
fn bd_gu1bi_utf8_cast_blob_expression_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        run_cast_blob_case("UTF-8", 1, "utf8").await;
    });
}

// --- P2: stock-legal unpaired surrogate in a plain-indexed column ----------

/// UTF-16le fixture with an unpaired high surrogate (U+D800) in an indexed TEXT
/// column. The value is `"A" + U+D800` = UTF-16le bytes `41 00 00 D8`, injected
/// via `CAST(X'410000D8' AS TEXT)` (stock relabels the blob bytes as text in the
/// DB encoding, no validation). A plain index stores those bytes verbatim; the
/// canonical recompute would lossily re-encode `41 00 FD FF` and false-flag.
fn build_stock_surrogate_db(path: &Path) {
    {
        let conn = rusqlite::Connection::open(path).expect("open stock SQLite writer");
        conn.execute_batch("PRAGMA encoding = 'UTF-16le';")
            .expect("set PRAGMA encoding on empty DB");
        conn.execute_batch(
            "CREATE TABLE s(id INTEGER PRIMARY KEY, y TEXT);\n\
             INSERT INTO s(id, y) VALUES (1, CAST(X'410000D8' AS TEXT)), (2, 'plain');\n\
             CREATE INDEX iy ON s(y);",
        )
        .expect("apply surrogate fixture");
    }
    assert_stock_premise(path, "UTF-16le");
}

#[test]
fn bd_gu1bi_utf16le_unpaired_surrogate_plain_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bd_gu1bi_surrogate_utf16le.db");
        build_stock_surrogate_db(&path);
        assert_eq!(
            header_text_encoding(&path),
            2,
            "surrogate fixture did not persist UTF-16le (byte @56)"
        );
        assert_fsqlite_integrity_ok(&path, "surrogate-utf16le").await;
    });
}
