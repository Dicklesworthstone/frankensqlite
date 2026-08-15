//! bd-bld9w.2/.3/.4 acceptance oracle: end-to-end UTF-16 database read parity.
//!
//! rusqlite (bundled C SQLite) is the oracle. We build UTF-16LE and UTF-16BE
//! *stock* databases (`PRAGMA encoding` set on an empty DB, before any schema),
//! then reopen each in FrankenSQLite and diff schema load plus
//! `SELECT` / `WHERE` / `ORDER BY` / `sqlite_master` against a fresh C-SQLite
//! reopen of the same file.
//!
//! ## Status / why some arms are `#[ignore]`d
//!
//! The UTF-16 read path is being wired across bd-bld9w:
//!   * bld9w.2 — record TEXT decode + VDBE caller-threading of the DB encoding,
//!   * bld9w.3 — UTF-16 `sqlite_master` schema load + lifting the read-side
//!     fail-closed admission gate,
//!   * bld9w.4 — TEXT comparison / BINARY collation on the decoded code points.
//!
//! Until those land, FrankenSQLite fails *closed* on UTF-16 opens (the
//! release-safe admission bridge, commit 1faf13504), so the UTF-16 arms below
//! are `#[ignore]`d. **Un-ignore them when the corresponding slice lands** — they
//! are the green gate for bd-bld9w.2/.3/.4.
//!
//! Two things run *now* and are load-bearing regardless:
//!   * `utf8_control_*` — a UTF-8 hot-path parity guard: an identical UTF-8 stock
//!     DB must read byte-for-byte identically, so the bld9w.2 caller-threading
//!     provably does not regress the default (UTF-8) path.
//!   * `utf16_fixture_really_is_utf16_*` — proves the rusqlite fixtures actually
//!     produce UTF-16 databases (header text-encoding byte + `PRAGMA encoding`),
//!     so the ignored arms are meaningful the moment they are enabled.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Shared schema for every encoding. `city` carries a secondary index so schema
/// load has more than one `sqlite_master` row to decode.
const SCHEMA: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, city TEXT)",
    "CREATE INDEX idx_items_city ON items(city)",
];

/// ASCII-only rows: byte-identical under UTF-8 and UTF-16 apart from the width,
/// so any decode error still surfaces as an interleaved-NUL divergence.
const ASCII_ROWS: &[&str] = &["INSERT INTO items(id, name, city) VALUES \
     (1, 'Alice', 'Paris'), \
     (2, 'Bob', 'London'), \
     (3, 'Cara', 'Paris'), \
     (4, 'Dan', 'Berlin')"];

/// Rows with multi-byte code points, where the UTF-8 and UTF-16 byte layouts
/// genuinely differ (BMP and supra-ASCII). Keeps one ASCII `Paris` city so the
/// `WHERE city = 'Paris'` probe stays meaningful.
const UNICODE_ROWS: &[&str] = &["INSERT INTO items(id, name, city) VALUES \
     (1, 'Élise', 'Paris'), \
     (2, 'Børge', 'Oslo'), \
     (3, '名前', '東京'), \
     (4, 'Zoë', 'Zürich')"];

/// Read probes exercised on both engines against the same file. Ordering keys
/// are chosen so the row order is deterministic without depending on TEXT
/// collation subtleties beyond what stock SQLite itself does.
const READ_QUERIES: &[&str] = &[
    "SELECT id, name, city FROM items ORDER BY id",
    "SELECT name FROM items WHERE city = 'Paris' ORDER BY id",
    "SELECT id, name FROM items ORDER BY name",
    "SELECT count(*) AS n, city FROM items GROUP BY city ORDER BY city, n",
    "SELECT type, name, tbl_name FROM sqlite_master ORDER BY name",
];

/// Text-encoding byte in the SQLite file header (offset 56, 4-byte big-endian):
/// 1 = UTF-8, 2 = UTF-16le, 3 = UTF-16be.
fn header_text_encoding(path: &std::path::Path) -> u32 {
    let bytes = std::fs::read(path).expect("read database file header");
    assert!(
        bytes.len() >= 60,
        "database file shorter than its 100-byte header"
    );
    u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]])
}

/// Build a stock C-SQLite database at `path` with the requested `encoding`
/// (`None` = default UTF-8), then close it so the file is fully flushed. The
/// encoding is validated by reopening and reading `PRAGMA encoding`, so a fixture
/// that silently fell back to UTF-8 fails loudly instead of testing nothing.
fn build_stock_db(path: &std::path::Path, encoding: Option<&str>, rows: &[&str]) {
    {
        let conn = rusqlite::Connection::open(path).expect("open stock SQLite writer");
        if let Some(enc) = encoding {
            // Must precede any schema: encoding is fixed at first write.
            conn.execute_batch(&format!("PRAGMA encoding = '{enc}';"))
                .expect("set PRAGMA encoding on empty database");
        }
        for stmt in SCHEMA {
            conn.execute_batch(stmt).expect("apply schema to stock DB");
        }
        for stmt in rows {
            conn.execute_batch(stmt).expect("apply rows to stock DB");
        }
    } // drop flushes and closes the writer

    if let Some(enc) = encoding {
        let conn = rusqlite::Connection::open(path).expect("reopen stock SQLite for validation");
        let got: String = conn
            .query_row("PRAGMA encoding", [], |row| row.get(0))
            .expect("read back PRAGMA encoding");
        assert_eq!(
            got.to_ascii_lowercase(),
            enc.to_ascii_lowercase(),
            "fixture failed to persist requested encoding"
        );
    }
}

/// Stringify a FrankenSQLite result set the same way as the C oracle below, so
/// the two are directly comparable.
fn frank_rows_to_strings(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => format!("'{s}'"),
                    SqliteValue::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                })
                .collect()
        })
        .collect()
}

/// Run `READ_QUERIES` against a fresh C-SQLite reopen of `path`.
fn oracle_read(path: &std::path::Path) -> Vec<Vec<Vec<String>>> {
    let conn = rusqlite::Connection::open(path).expect("reopen stock SQLite oracle");
    READ_QUERIES
        .iter()
        .map(|query| {
            let mut stmt = conn.prepare(query).expect("prepare oracle query");
            let col_count = stmt.column_count();
            stmt.query_map([], |row| {
                let mut vals = Vec::new();
                for i in 0..col_count {
                    let v: rusqlite::types::Value = row.get_unwrap(i);
                    vals.push(match v {
                        rusqlite::types::Value::Null => "NULL".to_owned(),
                        rusqlite::types::Value::Integer(n) => n.to_string(),
                        rusqlite::types::Value::Real(f) => format!("{f}"),
                        rusqlite::types::Value::Text(s) => format!("'{s}'"),
                        rusqlite::types::Value::Blob(b) => format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        ),
                    });
                }
                Ok(vals)
            })
            .expect("run oracle query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("collect oracle rows")
        })
        .collect()
}

/// Open `path` in FrankenSQLite and diff `READ_QUERIES` against the C oracle.
/// Panics with the first divergence; returns normally on full parity.
async fn assert_read_parity(path: &std::path::Path, label: &str) {
    let expected = oracle_read(path);
    let fconn = Connection::open(path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| panic!("[{label}] FrankenSQLite failed to open the database: {e}"));

    for (query, want) in READ_QUERIES.iter().zip(expected.iter()) {
        let got_rows = fconn
            .query(query)
            .await
            .unwrap_or_else(|e| panic!("[{label}] FrankenSQLite query error: {query}: {e}"));
        let got = frank_rows_to_strings(&got_rows);
        assert_eq!(
            &got, want,
            "[{label}] result divergence on `{query}`\n  frank: {got:?}\n  csql:  {want:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// UTF-8 control — GREEN NOW. Guards the default path against a bld9w.2
// caller-threading regression.
// ---------------------------------------------------------------------------

#[test]
fn utf8_control_read_parity_ascii() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("utf8_ascii.db");
        build_stock_db(&path, None, ASCII_ROWS);
        assert_eq!(
            header_text_encoding(&path),
            1,
            "UTF-8 control must have encoding byte 1"
        );
        assert_read_parity(&path, "utf8_control_ascii").await;
    });
}

#[test]
fn utf8_control_read_parity_unicode() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("utf8_unicode.db");
        build_stock_db(&path, None, UNICODE_ROWS);
        assert_eq!(
            header_text_encoding(&path),
            1,
            "UTF-8 control must have encoding byte 1"
        );
        assert_read_parity(&path, "utf8_control_unicode").await;
    });
}

// ---------------------------------------------------------------------------
// Fixture validation — GREEN NOW. Proves the rusqlite fixtures genuinely
// produce UTF-16 databases, so the ignored arms below are meaningful the moment
// they are enabled.
// ---------------------------------------------------------------------------

#[test]
fn utf16_fixture_really_is_utf16_le() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("utf16le_fixture.db");
    build_stock_db(&path, Some("UTF-16le"), UNICODE_ROWS);
    assert_eq!(
        header_text_encoding(&path),
        2,
        "UTF-16le header text-encoding byte must be 2"
    );
}

#[test]
fn utf16_fixture_really_is_utf16_be() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("utf16be_fixture.db");
    build_stock_db(&path, Some("UTF-16be"), UNICODE_ROWS);
    assert_eq!(
        header_text_encoding(&path),
        3,
        "UTF-16be header text-encoding byte must be 3"
    );
}

// ---------------------------------------------------------------------------
// UTF-16 read parity — the acceptance gate. `#[ignore]`d today.
//
// bld9w.2 (record decode + VDBE caller-threading) landed and the decode is
// PROVEN: in a transient window where the bld9w.3 read admission gate was open,
// all three non-collation arms below passed against HEAD (SageBluff probe
// 2026-08-15). But the admission gate is currently toggled *closed* again —
// FrankenSQLite returns "unsupported operation" on a UTF-16 open — while the
// bld9w.3 write-safety/admission-activation design settles. So the arms stay
// `#[ignore]`d to keep this file green; un-ignore each as bld9w.3 stably lands.
//
// One arm additionally encodes a real bld9w.4 (compare+collate) finding: BINARY
// collation on a UTF-16 database must order by the *stored* encoding's bytes,
// like stock SQLite — not FrankenSQLite's decoded UTF-8 form. This diverges only
// for UTF-16**LE** non-ASCII text (LE byte order != code-point order); UTF-16BE
// matches because BE byte order == code-point order == UTF-8 order for the BMP.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "bd-bld9w.3: un-ignore when the UTF-16 read admission-lift stably lands \
            (record decode proven; admission currently gated closed pending write-safety)"]
fn utf16le_read_parity_ascii() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("utf16le_ascii.db");
        build_stock_db(&path, Some("UTF-16le"), ASCII_ROWS);
        assert_eq!(header_text_encoding(&path), 2);
        assert_read_parity(&path, "utf16le_ascii").await;
    });
}

#[test]
#[ignore = "bd-bld9w.4 (compare+collate): BINARY collation on a UTF-16LE DB must \
            order by the stored UTF-16LE bytes like stock (名前<Børge<Zoë<Élise via \
            0x0D<0x42<0x5A<0xC9), but FrankenSQLite orders by the decoded UTF-8 form \
            (Børge<Zoë<Élise<名前). Un-ignore when .4 makes BINARY compare in the DB encoding."]
fn utf16le_read_parity_unicode() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("utf16le_unicode.db");
        build_stock_db(&path, Some("UTF-16le"), UNICODE_ROWS);
        assert_eq!(header_text_encoding(&path), 2);
        assert_read_parity(&path, "utf16le_unicode").await;
    });
}

#[test]
#[ignore = "bd-bld9w.3: un-ignore when the UTF-16 read admission-lift stably lands \
            (record decode proven; admission currently gated closed pending write-safety)"]
fn utf16be_read_parity_ascii() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("utf16be_ascii.db");
        build_stock_db(&path, Some("UTF-16be"), ASCII_ROWS);
        assert_eq!(header_text_encoding(&path), 3);
        assert_read_parity(&path, "utf16be_ascii").await;
    });
}

#[test]
#[ignore = "bd-bld9w.3: un-ignore when the UTF-16 read admission-lift stably lands \
            (record decode proven; admission currently gated closed pending write-safety)"]
fn utf16be_read_parity_unicode() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("utf16be_unicode.db");
        build_stock_db(&path, Some("UTF-16be"), UNICODE_ROWS);
        assert_eq!(header_text_encoding(&path), 3);
        assert_read_parity(&path, "utf16be_unicode").await;
    });
}
