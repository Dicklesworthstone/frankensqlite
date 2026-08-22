//! bd-7c6g7.6 acceptance keeper: `PRAGMA integrity_check` on a **stock UTF-16**
//! database with an **expression index** must agree with stock sqlite3 ("ok"),
//! with no spurious first-open migration.
//!
//! Root cause: the schema-btree integrity walker (`validate_schema_btrees_in_txn`
//! in connection.rs) rebuilds each index's expected key record from the table
//! row and byte-compares it against the stored index key. It decoded the table
//! row with the UTF-8-hardcoded `parse_record` and serialized the rebuilt key
//! with the UTF-8-hardcoded `serialize_record`.
//!
//! On a UTF-16 database an EXPRESSION index (e.g. `lower(k)`) diverged: the
//! UTF-16 TEXT column decoded to a byte-preserving *raw* string, so evaluating
//! the index expression against that mis-decoded lossy text and re-serializing
//! it as UTF-8 produced key bytes that did not match the UTF-16 key the store
//! wrote → "index `X` entry for rowid N does not match the table row payload".
//! (A plain-column index accidentally round-tripped the opaque raw bytes and
//! passed, which is why only expression indexes false-flagged.)
//!
//! The fix threads the database text encoding through the table-row decode and
//! the rebuilt-key serialization. On UTF-8 the encoding-aware helpers are
//! byte-identical to the old path, so this is zero behavior change for the
//! common case (covered by the UTF-8 control below).
//!
//! Scope note: this keeper deliberately exercises EXPRESSION indexes only. The
//! separate question of BINARY/NOCASE index-key *ordering* on UTF-16 (stock
//! orders by encoded-byte order, which diverges from code-point order above
//! U+00FF) is a distinct residual tracked in its own bead; it is intentionally
//! out of scope here. Data is confined to the Latin-1 range (<= U+00FF), where
//! UTF-16 byte order and code-point order coincide, so the residual cannot
//! contaminate this keeper.
//!
//! rusqlite (bundled C SQLite) is the oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::path::{Path, PathBuf};

/// Schema exercising expression indexes: a single-column expression index, a
/// multi-column expression index, and a partial expression index — the shapes
/// whose rebuilt key must byte-match the stored UTF-16 key.
const SCHEMA: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, j TEXT)",
    "CREATE INDEX idx_lower ON t(lower(k))",
    "CREATE INDEX idx_expr2 ON t(substr(k, 1, 2), upper(j))",
    "CREATE INDEX idx_partial ON t(lower(k)) WHERE j IS NOT NULL",
];

/// Rows with non-ASCII Latin-1 text whose UTF-16 layout diverges from UTF-8 and
/// whose `lower()`/`upper()` results are themselves non-ASCII, so the expression
/// re-serialization is genuinely exercised on UTF-16 storage. All characters are
/// <= U+00FF (Latin-1), keeping key ordering identical under UTF-16 byte order
/// and code-point order so the (out-of-scope) ordering residual cannot fire.
const ROWS: &[&str] = &["INSERT INTO t(id, k, j) VALUES \
     (1, 'ÀÉÎ',     'café'), \
     (2, 'GRÜN',    'MÜNCHEN'), \
     (3, 'Ünïcödé', NULL), \
     (4, 'éÀ',      'Bób')"];

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

/// Build a stock DB at `path` with `encoding`, validate the encoding persisted,
/// and assert stock's own integrity_check is "ok" (the premise this keeper
/// differentiates against).
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
    let stock_integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("stock integrity_check");
    assert_eq!(
        stock_integrity, "ok",
        "premise: stock integrity_check must be ok for the {encoding} fixture"
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
    let path = dir.path().join(format!("bd_7c6g7_{label}.db"));
    build_stock_db(&path, encoding);
    assert_eq!(
        header_text_encoding(&path),
        expect_header,
        "[{label}] fixture did not persist the requested encoding (byte @56)"
    );

    let fconn = Connection::open(path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite failed to open the DB: {e}"));

    // No spurious first-open migration/repair was written on open.
    let backup = pre_migration_backup_path(&path);
    assert!(
        !backup.exists(),
        "[{label}] spurious first-open migration fired: {} exists",
        backup.display()
    );

    // integrity_check agrees with stock ("ok"), not a spurious
    // "does not match the table row payload".
    let got = integrity_check(&fconn).await;
    assert_eq!(
        got,
        vec!["ok".to_owned()],
        "[{label}] PRAGMA integrity_check diverged from stock on a clean UTF-16 DB \
         with an expression index (bd-7c6g7.6): {got:?}"
    );
}

#[test]
fn bd_7c6g7_utf16le_expression_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        run_case("UTF-16le", 2, "utf16le").await;
    });
}

#[test]
fn bd_7c6g7_utf16be_expression_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        run_case("UTF-16be", 3, "utf16be").await;
    });
}

/// UTF-8 control: the same schema and rows on a default UTF-8 database must stay
/// "ok" — the encoding-aware decode/serialize is byte-identical to the old path
/// on UTF-8, so this guards against a regression in the common case.
#[test]
fn bd_7c6g7_utf8_expression_index_integrity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        run_case("UTF-8", 1, "utf8").await;
    });
}
