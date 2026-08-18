#![recursion_limit = "512"]

//! GH #263 / #264 (bd-gh-pragma-readback): `PRAGMA user_version` and
//! `PRAGMA application_id` are signed 32-bit header fields.
//!
//! - #263: a persisted value like `-1` (bit pattern 0xFFFFFFFF) must hydrate on
//!   reopen as `-1`, not `4294967295` — the header u32 was zero-extended.
//! - #264: an assignment outside the signed-32-bit range follows stock's
//!   `sqlite3GetInt32` rule and stores `0` (both immediate readback and after
//!   reopen), rather than truncating the low 32 bits.
//!
//! The full matrix (in-range incl. i32::MIN/MAX and negatives, plus several
//! out-of-range magnitudes) is compared live against rusqlite (SQLite 3.46.1)
//! for BOTH the immediate readback and a fresh-reopen readback.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// The value matrix: two in-range negatives, both i32 boundaries, several
/// out-of-i32-range magnitudes (both signs), and 0.
const MATRIX: &[i64] = &[
    -1,
    -5,
    2_147_483_647,  // i32::MAX
    -2_147_483_648, // i32::MIN
    2_147_483_648,  // i32::MAX + 1 -> 0
    4_294_967_295,  // u32::MAX      -> 0
    4_294_967_296,  // 2^32         -> 0
    -4_294_967_296, //              -> 0
    9_999_999_999,  //              -> 0
    0,
];

async fn read_pragma_i64(conn: &Connection, pragma: &str) -> i64 {
    let rows = conn
        .query(&format!("PRAGMA {pragma}"))
        .await
        .unwrap_or_else(|e| panic!("frank read PRAGMA {pragma}: {e:?}"));
    match rows.first().and_then(|r| r.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("frank PRAGMA {pragma} unexpected value: {other:?}"),
    }
}

/// (immediate readback, fresh-reopen readback) for frank.
async fn frank_roundtrip(dir: &std::path::Path, pragma: &str, val: i64) -> (i64, i64) {
    let path = dir.join(format!("frank_{pragma}_{val}.db"));
    let p = path.to_str().unwrap();
    let immediate = {
        let conn = Connection::open(p).await.unwrap();
        conn.execute(&format!("PRAGMA {pragma}={val}"))
            .await
            .unwrap();
        // A committed schema change forces the header write so the value
        // survives reopen.
        conn.execute("CREATE TABLE t(x)").await.unwrap();
        read_pragma_i64(&conn, pragma).await
    };
    let reopen = {
        let conn = Connection::open(p).await.unwrap();
        read_pragma_i64(&conn, pragma).await
    };
    (immediate, reopen)
}

/// (immediate readback, fresh-reopen readback) for stock SQLite via rusqlite.
fn stock_roundtrip(dir: &std::path::Path, pragma: &str, val: i64) -> (i64, i64) {
    let path = dir.join(format!("stock_{pragma}_{val}.db"));
    let p = path.to_str().unwrap();
    let immediate = {
        let conn = rusqlite::Connection::open(p).unwrap();
        conn.execute_batch(&format!("PRAGMA {pragma}={val}; CREATE TABLE t(x);"))
            .unwrap();
        conn.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get::<_, i64>(0))
            .unwrap()
    };
    let reopen = {
        let conn = rusqlite::Connection::open(p).unwrap();
        conn.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get::<_, i64>(0))
            .unwrap()
    };
    (immediate, reopen)
}

fn run_field(pragma: &str) {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        for &val in MATRIX {
            let frank = frank_roundtrip(dir.path(), pragma, val).await;
            let stock = stock_roundtrip(dir.path(), pragma, val);
            assert_eq!(
                frank, stock,
                "PRAGMA {pragma}={val}: frank (immediate, reopen)={frank:?} vs stock={stock:?}"
            );
        }
    });
}

/// GH #263 + #264: user_version signed-32 round-trip and overflow-to-0.
#[test]
fn user_version_signed32_roundtrip() {
    run_field("user_version");
}

/// GH #263 + #264: application_id signed-32 round-trip and overflow-to-0.
#[test]
fn application_id_signed32_roundtrip() {
    run_field("application_id");
}
