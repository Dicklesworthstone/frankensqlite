//! bd-iubwb acceptance oracle: `octet_length(X)` must report the byte length of
//! X's TEXT representation IN THE DATABASE'S text encoding.
//!
//! On a UTF-16 database each code unit is two bytes, so `octet_length('abc')`
//! is 6 (3 code units x 2 bytes) and `octet_length(12345)` is 10 ("12345" = 5
//! code units x 2 bytes). On a UTF-8 database the historical behavior is
//! preserved: `octet_length('abc')` is 3 and `octet_length(12345)` is 5. BLOB
//! length is always the raw byte count regardless of encoding.
//!
//! rusqlite (bundled C SQLite) is the oracle: every FrankenSQLite answer is
//! diffed against a fresh C-SQLite reopen of the same physical database, so the
//! hard-coded expectations below are also independently confirmed against stock.
//!
//! Two lanes are exercised on each encoding:
//!   * `octet_length(<literal>)` with no FROM clause — the expression-only VDBE
//!     lane (`compile_expression_select`), whose engine never adopts a text
//!     encoding from a table page and must rely on the Connection's projected
//!     encoding.
//!   * `octet_length(<column>)` / `octet_length(<literal>) FROM t` — the
//!     table-read VDBE lane, whose engine self-adopts the encoding from the
//!     page-1 header when it opens the cursor.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

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
/// (`None` = default UTF-8). A one-row table forces the encoding into the
/// header (it is fixed at first write) and gives the table-read lane something
/// to read. The encoding is validated by reopening and reading
/// `PRAGMA encoding`, so a fixture that silently fell back to UTF-8 fails loudly.
fn build_encoded_db(path: &std::path::Path, encoding: Option<&str>) {
    {
        let conn = rusqlite::Connection::open(path).expect("open stock SQLite writer");
        if let Some(enc) = encoding {
            // Must precede any schema: encoding is fixed at first write.
            conn.execute_batch(&format!("PRAGMA encoding = '{enc}';"))
                .expect("set PRAGMA encoding on empty database");
        }
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);\
             INSERT INTO t(id, v) VALUES (1, 'abc');",
        )
        .expect("apply schema + row to stock DB");
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

/// Run a single-value scalar query against a fresh C-SQLite reopen of `path`.
fn oracle_scalar_i64(path: &std::path::Path, query: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).expect("reopen stock SQLite oracle");
    conn.query_row(query, [], |row| row.get::<_, i64>(0))
        .unwrap_or_else(|e| panic!("oracle query `{query}` failed: {e}"))
}

/// Run a single-value scalar query against FrankenSQLite and return the integer.
async fn frank_scalar_i64(conn: &Connection, query: &str) -> i64 {
    let rows = conn
        .query(query)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite query error `{query}`: {e}"));
    assert_eq!(rows.len(), 1, "expected exactly one row from `{query}`");
    match rows[0].values().first() {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("expected an INTEGER from `{query}`, got {other:?}"),
    }
}

/// Assert FrankenSQLite matches both the hard-coded expectation and the C oracle
/// for `query` on the database at `path` (already opened as `conn`).
async fn assert_octet_length(
    conn: &Connection,
    path: &std::path::Path,
    query: &str,
    expected: i64,
    label: &str,
) {
    let oracle = oracle_scalar_i64(path, query);
    assert_eq!(
        oracle, expected,
        "[{label}] oracle disagrees with the bead's stated expectation for `{query}`"
    );
    let got = frank_scalar_i64(conn, query).await;
    assert_eq!(
        got, expected,
        "[{label}] FrankenSQLite `{query}` = {got}, want {expected} (oracle {oracle})"
    );
}

/// The complete octet_length matrix for one encoding. `unit` is the number of
/// bytes SQLite uses per UTF-16 code unit (1 for UTF-8, 2 for UTF-16), which is
/// how each expectation is derived from the source string.
async fn run_matrix(path: &std::path::Path, unit: i64, label: &str) {
    let conn = Connection::open(path.to_str().unwrap())
        .await
        .unwrap_or_else(|e| panic!("[{label}] FrankenSQLite failed to open the database: {e}"));

    // FROM-less lane (expression-only VDBE): TEXT literal and rendered numeric.
    assert_octet_length(
        &conn,
        path,
        "SELECT octet_length('abc')",
        3 * unit,
        &format!("{label}/fromless-text"),
    )
    .await;
    assert_octet_length(
        &conn,
        path,
        "SELECT octet_length(12345)",
        5 * unit,
        &format!("{label}/fromless-numeric"),
    )
    .await;

    // BLOB is raw bytes regardless of encoding.
    assert_octet_length(
        &conn,
        path,
        "SELECT octet_length(x'01020304')",
        4,
        &format!("{label}/fromless-blob"),
    )
    .await;

    // Table-read lane (engine self-adopts the header encoding): column TEXT and
    // a rendered numeric evaluated with a cursor open.
    assert_octet_length(
        &conn,
        path,
        "SELECT octet_length(v) FROM t",
        3 * unit,
        &format!("{label}/table-text"),
    )
    .await;
    assert_octet_length(
        &conn,
        path,
        "SELECT octet_length(12345) FROM t",
        5 * unit,
        &format!("{label}/table-numeric"),
    )
    .await;
}

#[test]
fn octet_length_utf8_default_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("octet_utf8.db");
        build_encoded_db(&path, None);
        assert_eq!(
            header_text_encoding(&path),
            1,
            "UTF-8 fixture must have encoding byte 1"
        );
        run_matrix(&path, 1, "utf8").await;
    });
}

#[test]
fn octet_length_utf16le_counts_two_bytes_per_code_unit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("octet_utf16le.db");
        build_encoded_db(&path, Some("UTF-16le"));
        assert_eq!(
            header_text_encoding(&path),
            2,
            "UTF-16le fixture must have encoding byte 2"
        );
        run_matrix(&path, 2, "utf16le").await;
    });
}

#[test]
fn octet_length_utf16be_counts_two_bytes_per_code_unit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("octet_utf16be.db");
        build_encoded_db(&path, Some("UTF-16be"));
        assert_eq!(
            header_text_encoding(&path),
            3,
            "UTF-16be fixture must have encoding byte 3"
        );
        run_matrix(&path, 2, "utf16be").await;
    });
}
