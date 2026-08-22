//! bd-1v1pl acceptance keeper: opening a **stock UTF-16** database must NOT
//! trigger a spurious first-open migration/repair pass.
//!
//! Root cause: `read_sqlite_master_rows_in_txn` decoded each `sqlite_master`
//! row payload with the UTF-8-hardcoded `parse_record`. On a UTF-16 database the
//! `sqlite_master` TEXT columns (type/name/tbl_name/sql) are stored as UTF-16
//! bytes, so a named index `idx` read back as `i\0d\0x\0` garbage. That fed
//! `validate_sqlite_master_rows`, whose signature comparison then reported
//! "sqlite_master is missing expected entry" — a spurious `integrity_check`
//! divergence that also fired the first-open migration/repair pass, which writes
//! a `<db>.pre-migration-bak` backup and emits a WARN.
//!
//! rusqlite (bundled C SQLite) is the oracle. We build a *stock* UTF-16LE (and
//! UTF-16BE) DB with a named table + named index + rows, validate the header
//! encoding byte actually persisted (a fixture that fell back to UTF-8 would
//! test nothing), then open it with fsqlite and assert the two fingerprints of
//! the wart are gone:
//!
//!   1. `PRAGMA integrity_check` == `["ok"]` (matches stock) — a UTF-16 DB with
//!      a named table + index must integrity-check clean, NOT report a spurious
//!      "sqlite_master is missing expected entry".
//!   2. No `<db_path>.pre-migration-bak` side-file exists after open — the
//!      spurious-migration fingerprint.
//!
//! On UTF-8 the encoding-aware decode is byte-identical to the old path, so this
//! is zero behavior change for the common case.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::path::{Path, PathBuf};

/// Schema: a named table `t` with an INTEGER PRIMARY KEY and a TEXT column,
/// plus a named secondary index `idx` — exactly the objects whose names live in
/// `sqlite_master` TEXT columns and get compared during integrity validation.
const SCHEMA: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT)",
    "CREATE INDEX idx ON t(k)",
];

/// Rows with non-ASCII BMP keys (whose UTF-16 layout diverges from UTF-8) plus
/// ASCII, so the schema decode is genuinely exercised on UTF-16 storage.
const ROWS: &[&str] = &["INSERT INTO t(id, k) VALUES \
     (1, 'alpha'), \
     (2, 'Ωmega'), \
     (3, 'Борис'), \
     (4, '名前')"];

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

/// `<db_path>.pre-migration-bak` — the spurious-migration fingerprint (mirrors
/// `fsqlite_core::migration::pre_migration_backup_path`, a raw suffix append).
fn pre_migration_backup_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".pre-migration-bak");
    PathBuf::from(s)
}

/// Build a stock DB at `path` with `encoding`, and validate the encoding
/// actually persisted (a fixture that silently fell back to UTF-8 tests nothing).
fn build_stock_db(path: &Path, encoding: &str) {
    {
        let conn = rusqlite::Connection::open(path).expect("open stock SQLite writer");
        conn.execute_batch(&format!("PRAGMA encoding = '{encoding}';"))
            .expect("set PRAGMA encoding on empty DB");
        for stmt in SCHEMA {
            conn.execute_batch(stmt).expect("apply schema");
        }
        for stmt in ROWS {
            conn.execute_batch(stmt).expect("apply rows");
        }
    }
    let conn = rusqlite::Connection::open(path).expect("reopen stock SQLite for validation");
    let got: String = conn
        .query_row("PRAGMA encoding", [], |r| r.get(0))
        .expect("read back PRAGMA encoding");
    assert_eq!(
        got.to_ascii_lowercase(),
        encoding.to_ascii_lowercase(),
        "fixture failed to persist requested encoding"
    );
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

async fn run_case(encoding: &str, expect_header: u32, label: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join(format!("bd_1v1pl_{label}.db"));
    build_stock_db(&path, encoding);
    assert_eq!(
        header_text_encoding(&path),
        expect_header,
        "[{label}] fixture did not persist the requested UTF-16 encoding (byte @56)"
    );

    // Opening runs the first-open migration pass (native feature). With the bug,
    // the UTF-16 `sqlite_master` names decode as UTF-8 garbage → validation
    // fails → the pass writes a `.pre-migration-bak` backup.
    let fconn = Connection::open(path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite failed to open the DB: {e}"));

    // Fingerprint 2: no spurious pre-migration backup was written on open.
    let backup = pre_migration_backup_path(&path);
    assert!(
        !backup.exists(),
        "[{label}] spurious first-open migration fired: {} exists (UTF-16 sqlite_master \
         decoded as UTF-8 garbage → validate_sqlite_master_rows false-flagged)",
        backup.display()
    );

    // Fingerprint 1: integrity_check agrees with stock ("ok"), not a spurious
    // "sqlite_master is missing expected entry" divergence.
    let got = integrity_check(&fconn).await;
    assert_eq!(
        got,
        vec!["ok".to_owned()],
        "[{label}] PRAGMA integrity_check diverged from stock on a clean UTF-16 DB: {got:?}"
    );
}

#[test]
fn bd_1v1pl_utf16le_sqlite_master_integrity_no_spurious_migration() {
    asupersync::test_utils::run_test(|| async {
        run_case("UTF-16le", 2, "utf16le").await;
    });
}

#[test]
fn bd_1v1pl_utf16be_sqlite_master_integrity_no_spurious_migration() {
    asupersync::test_utils::run_test(|| async {
        run_case("UTF-16be", 3, "utf16be").await;
    });
}
