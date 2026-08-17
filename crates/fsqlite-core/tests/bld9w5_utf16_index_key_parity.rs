//! bd-bld9w.5 acceptance keeper: TEXT index-key encode/decode/compare must be
//! consistent with the DB text encoding, so an **index-routed** query returns
//! the SAME rows in the SAME order as the equivalent **full-table-scan** AND
//! matches stock SQLite on a UTF-16 database.
//!
//! rusqlite (bundled C SQLite) is the oracle. We build a *stock* UTF-16LE and a
//! *stock* UTF-16BE DB (`PRAGMA encoding=...` on an empty DB, before any
//! schema), create a secondary index on a TEXT column, and insert NON-ASCII BMP
//! rows (accented Latin, Greek, Cyrillic, CJK) whose UTF-16 byte layout diverges
//! from their UTF-8 byte layout — so an index path that decoded/compared TEXT in
//! the wrong encoding produces a different row set or order. Then, on the same
//! file, for each probe query we assert:
//!
//!   (a) fsqlite forced-index (`INDEXED BY idx`) == stock,
//!   (b) fsqlite forced-scan  (`NOT INDEXED`)    == stock,
//!   (c) fsqlite indexed == fsqlite scan (the literal keeper: "indexed vs
//!       unindexed query parity on UTF-16 DBs").
//!
//! To keep (a)/(c) non-vacuous, a separate battery asserts (via EXPLAIN QUERY
//! PLAN) that the canonical equality-seek, range-seek and ordered-scan queries
//! genuinely route through `idx`.
//!
//! ## Status: read-side index-key encoding is already wired (STALE for reads)
//!
//! The index-key SEEK decode was threaded to the DB text encoding in
//! `076ae8d56` (StorageCursor.text_encoding → `_with_encoding` fallbacks) and
//! the index-key COMPARE orders in the DB storage encoding via `e404a51e2`
//! (bd-bld9w.4 BINARY-in-storage). This keeper passes today; it is retained as a
//! regression guard. (The bld9w.5 WRITE/PRAGMA-setter surface is bd-bld9w.7 and
//! is out of scope here — these fixtures are stock-written UTF-16 files.)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Schema: TEXT column `k` with a secondary index, plus an INTEGER payload.
const SCHEMA: &[&str] = &[
    "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)",
    "CREATE INDEX idx ON t(k)",
];

/// Rows with non-ASCII BMP keys where the UTF-16 byte layout differs from UTF-8,
/// plus ASCII keys and a shared-prefix pair so equality/range/ordered-scan
/// probes are all meaningful. All keys are distinct, so a bare equality match
/// returns one deterministic row without depending on tie-break ordering.
const ROWS: &[&str] = &["INSERT INTO t(id, k, v) VALUES \
     (1, 'Élise', 10), \
     (2, 'Ωmega', 20), \
     (3, 'Борис', 30), \
     (4, '名前', 40), \
     (5, 'Zoë', 50), \
     (6, 'Apple', 60), \
     (7, 'Ápple', 70)"];

/// Header text-encoding byte (offset 56, big-endian 4 bytes): 1=UTF-8,
/// 2=UTF-16le, 3=UTF-16be.
fn header_text_encoding(path: &std::path::Path) -> u32 {
    let bytes = std::fs::read(path).expect("read database file header");
    assert!(bytes.len() >= 60, "database shorter than its 100-byte header");
    u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]])
}

/// Build a stock DB at `path` with `encoding` (`UTF-16le`/`UTF-16be`), and
/// validate the encoding actually persisted (a fixture that silently fell back
/// to UTF-8 would test nothing).
fn build_stock_db(path: &std::path::Path, encoding: &str) {
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

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

/// Run `sql` against a fresh C-SQLite reopen of `path`.
fn oracle(path: &std::path::Path, sql: &str) -> Vec<Vec<String>> {
    let conn = rusqlite::Connection::open(path).expect("reopen stock SQLite oracle");
    let mut stmt = conn.prepare(sql).expect("prepare oracle query");
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    })
    .expect("run oracle query")
    .collect::<std::result::Result<Vec<_>, _>>()
    .expect("collect oracle rows")
}

/// Run `sql` against fsqlite.
async fn frank(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite query error on `{sql}`: {e}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

/// Assert fsqlite's EXPLAIN QUERY PLAN for `sql` actually routes through `idx`,
/// so a "forced index" arm is provably not a silent full scan.
async fn assert_uses_index(conn: &Connection, sql: &str) {
    let rows = frank(conn, &format!("EXPLAIN QUERY PLAN {sql}")).await;
    let plan = rows
        .iter()
        .flat_map(|r| r.iter().cloned())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        plan.to_ascii_lowercase().contains("index idx"),
        "expected `{sql}` to route through index `idx`, but plan was: {plan}"
    );
}

/// Probe templates run in three forms (INDEXED BY / NOT INDEXED / plain) and
/// diffed against stock. `{h}` is the index hint. Every template is chosen so
/// the `INDEXED BY idx` form genuinely routes through the index (see the
/// dedicated `assert_uses_index` battery in each test).
const PROBES: &[&str] = &[
    // Equality seeks on non-ASCII keys — the index seek must find the row.
    // Keys are unique, so no ORDER BY is needed for determinism.
    "SELECT id, k, v FROM t {h} WHERE k = 'Ωmega'",
    "SELECT id, k, v FROM t {h} WHERE k = '名前'",
    "SELECT id, k, v FROM t {h} WHERE k = 'Ápple'",
    "SELECT id, k, v FROM t {h} WHERE k = 'Élise'",
    "SELECT id, k, v FROM t {h} WHERE k = 'Борис'",
    // Range seeks — the index scan must yield the same rows, in the same
    // storage-encoding BINARY order, as a full scan + sort.
    "SELECT id, k FROM t {h} WHERE k > 'Apple' ORDER BY k",
    "SELECT id, k FROM t {h} WHERE k >= 'Ωmega' ORDER BY k",
    // Ordered prefix scan over the whole index (covering) — BINARY order in the
    // DB storage encoding must match stock.
    "SELECT id, k FROM t {h} ORDER BY k",
];

/// Queries whose `INDEXED BY idx` form is asserted to route through the index,
/// pinning that the parity above is non-vacuous.
const INDEX_ROUTED: &[&str] = &[
    "SELECT id, k, v FROM t INDEXED BY idx WHERE k = 'Ωmega'",
    "SELECT id, k FROM t INDEXED BY idx WHERE k > 'Apple' ORDER BY k",
    "SELECT id, k FROM t INDEXED BY idx ORDER BY k",
];

async fn run_parity(encoding: &str, expect_header: u32, label: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join(format!("bld9w5_{label}.db"));
    build_stock_db(&path, encoding);
    assert_eq!(header_text_encoding(&path), expect_header, "[{label}] wrong header encoding");

    let fconn = Connection::open(path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite failed to open the DB: {e}"));

    // Non-vacuity: the forced-index arm truly routes through `idx`.
    for q in INDEX_ROUTED {
        assert_uses_index(&fconn, q).await;
    }

    for template in PROBES {
        let indexed = template.replace(" {h} ", " INDEXED BY idx ");
        let full_scan = template.replace(" {h} ", " NOT INDEXED ");
        let plain = template.replace(" {h} ", " ");

        let want = oracle(&path, &plain);
        let got_indexed = frank(&fconn, &indexed).await;
        let got_full = frank(&fconn, &full_scan).await;

        assert_eq!(
            got_indexed, want,
            "[{label}] INDEXED-vs-stock divergence on `{indexed}`\n  frank: {got_indexed:?}\n  csql:  {want:?}"
        );
        assert_eq!(
            got_full, want,
            "[{label}] NOT-INDEXED-vs-stock divergence on `{full_scan}`\n  frank: {got_full:?}\n  csql:  {want:?}"
        );
        assert_eq!(
            got_indexed, got_full,
            "[{label}] indexed vs unindexed divergence on `{template}`\n  idx:  {got_indexed:?}\n  scan: {got_full:?}"
        );
    }
}

#[test]
fn bld9w5_utf16le_index_key_parity() {
    asupersync::test_utils::run_test(|| async {
        run_parity("UTF-16le", 2, "utf16le").await;
    });
}

#[test]
fn bld9w5_utf16be_index_key_parity() {
    asupersync::test_utils::run_test(|| async {
        run_parity("UTF-16be", 3, "utf16be").await;
    });
}
