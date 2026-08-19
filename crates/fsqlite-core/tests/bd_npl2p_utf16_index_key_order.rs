//! bd-utf16-index-key-order-encoded-byte-npl2p (bd-7c6g7 residual).
//!
//! Stock SQLite orders BINARY (and NOCASE) TEXT index keys on a UTF-16 database
//! by the STORED ENCODED-BYTE order (UTF-16LE/BE byte order), which diverges from
//! code-point/UTF-8 order for any character above U+00FF. Example (stock 3.46.1,
//! UTF-16LE): `Ω` = U+03A9 -> LE bytes `A9 03`, `À` = U+00C0 -> LE bytes `C0 00`;
//! memcmp `A9 < C0` so `Ω` sorts BEFORE `À`, the opposite of code-point order.
//!
//! frank's integrity check (`validate_schema_btrees_in_txn`) and its B-tree store
//! comparator (`cmp_index_values_collated`) BOTH decode index-key payloads with
//! the byte-preserving `parse_record` and compare `SmallText` values, whose `cmp`
//! falls back to raw-byte comparison for the (invalid-UTF-8) UTF-16 payload bytes
//! -- i.e. encoded-byte order. So the physical store order and the integrity
//! order agree (no self-inconsistency false-flag) AND match stock's on-disk order.
//!
//! This keeper pins that behavior against a rusqlite oracle so a future agent does
//! not "fix" the two ordering spots by threading `parse_record_with_encoding`
//! there, which would decode to canonical UTF-8 and REGRESS BINARY ordering on
//! UTF-16 above U+00FF. It also asserts a clean UTF-16 image passes
//! `integrity_check` and that reopening it fires no spurious first-open
//! migration/repair (no `<db>.pre-migration-bak` side-file).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Cross-range BINARY data: ASCII, Latin-1 (>U+007F, <=U+00FF), and Greek
/// (>U+00FF) so encoded-byte vs code-point order genuinely diverge.
const BINARY_ROWS: &[&str] = &["ascii", "Zebra", "apple", "ÀÉÎ", "Ωμέγα", "ï", "€uro"];

/// NOCASE data mixing ASCII case pairs with non-ASCII so we exercise both the
/// ASCII case-fold and the non-ASCII raw tail.
const NOCASE_ROWS: &[&str] = &["ABC", "abc", "Zeta", "zeta", "Àbc", "àbc", "Ωμέγα"];

fn frank_order(rows: &[SqliteValue]) -> Vec<String> {
    rows.iter()
        .map(|v| match v {
            SqliteValue::Text(s) => s.as_ref().to_owned(),
            other => panic!("expected TEXT, got {other:?}"),
        })
        .collect()
}

async fn frank_build(
    dir: &std::path::Path,
    encoding: &str,
) -> (String, Vec<String>, Vec<String>) {
    let db = dir.join("u16.db").to_string_lossy().into_owned();
    let conn = Connection::open(&db).await.unwrap();

    // Encoding must be chosen before any table exists. DELETE journal mode gives
    // a stable rollback-mode on-disk image (the bounded whole-image gate refuses
    // WAL); this is a test-local setting, not the concurrent default.
    conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
        .await
        .unwrap();
    conn.execute("PRAGMA journal_mode = DELETE;").await.unwrap();

    conn.execute("CREATE TABLE tb(k TEXT);").await.unwrap();
    conn.execute("CREATE INDEX ib ON tb(k);").await.unwrap();
    conn.execute("CREATE TABLE tn(k TEXT);").await.unwrap();
    conn.execute("CREATE INDEX inc ON tn(k COLLATE NOCASE);")
        .await
        .unwrap();

    for (i, k) in BINARY_ROWS.iter().enumerate() {
        conn.execute(&format!("INSERT INTO tb VALUES({}, '{}');", i + 1, k))
            .await
            .unwrap();
    }
    for (i, k) in NOCASE_ROWS.iter().enumerate() {
        conn.execute(&format!("INSERT INTO tn VALUES({}, '{}');", i + 1, k))
            .await
            .unwrap();
    }

    // Guard against a silent UTF-8 fallback that would make the test vacuous.
    let enc = conn.query("PRAGMA encoding;").await.unwrap();
    let enc_text = match &enc[0].values()[0] {
        SqliteValue::Text(s) => s.as_ref().to_owned(),
        other => panic!("PRAGMA encoding returned non-text {other:?}"),
    };
    assert!(
        enc_text.eq_ignore_ascii_case(encoding),
        "fixture must be {encoding}, got {enc_text}"
    );

    // The integrity check must ACCEPT this clean image (no false out-of-order).
    let ic = conn.query("PRAGMA integrity_check;").await.unwrap();
    let ic_text = match &ic[0].values()[0] {
        SqliteValue::Text(s) => s.as_ref().to_owned(),
        other => panic!("integrity_check returned non-text {other:?}"),
    };
    assert_eq!(
        ic_text, "ok",
        "integrity_check false-flagged a clean {encoding} image: {ic_text}"
    );

    // frank must serve the index scan in encoded-byte order (the index is the
    // access path for the ORDER BY on the collated column).
    let tb = conn.query("SELECT k FROM tb ORDER BY k;").await.unwrap();
    let tn = conn
        .query("SELECT k FROM tn ORDER BY k COLLATE NOCASE;")
        .await
        .unwrap();
    let tb_order = frank_order(&tb.iter().map(|r| r.values()[0].clone()).collect::<Vec<_>>());
    let tn_order = frank_order(&tn.iter().map(|r| r.values()[0].clone()).collect::<Vec<_>>());

    (db, tb_order, tn_order)
}

/// rusqlite oracle: build the SAME UTF-16 schema + data and read stock's order.
fn stock_order(encoding: &str) -> (Vec<String>, Vec<String>) {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.pragma_update(None, "encoding", encoding).unwrap();
    conn.execute_batch(
        "CREATE TABLE tb(k TEXT);
         CREATE INDEX ib ON tb(k);
         CREATE TABLE tn(k TEXT);
         CREATE INDEX inc ON tn(k COLLATE NOCASE);",
    )
    .unwrap();
    for (i, k) in BINARY_ROWS.iter().enumerate() {
        conn.execute("INSERT INTO tb VALUES(?1, ?2);", rusqlite::params![i as i64 + 1, k])
            .unwrap();
    }
    for (i, k) in NOCASE_ROWS.iter().enumerate() {
        conn.execute("INSERT INTO tn VALUES(?1, ?2);", rusqlite::params![i as i64 + 1, k])
            .unwrap();
    }
    // Confirm the oracle really is UTF-16.
    let enc: String = conn
        .query_row("PRAGMA encoding;", [], |r| r.get(0))
        .unwrap();
    assert!(
        enc.eq_ignore_ascii_case(encoding),
        "rusqlite oracle must be {encoding}, got {enc}"
    );

    let read = |sql: &str| -> Vec<String> {
        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        rows
    };
    (
        read("SELECT k FROM tb ORDER BY k;"),
        read("SELECT k FROM tn ORDER BY k COLLATE NOCASE;"),
    )
}

fn run_for_encoding(encoding: &str) {
    asupersync::test_utils::run_test(|| async move {
        let dir = tempfile::tempdir().unwrap();
        let (db, frank_tb, frank_tn) = frank_build(dir.path(), encoding).await;

        // Reopening the clean image must fire NO spurious first-open
        // migration/repair: no `<db>.pre-migration-bak*` side-file appears.
        {
            let reopened = Connection::open(&db).await.unwrap();
            let ic = reopened.query("PRAGMA integrity_check;").await.unwrap();
            let ic_text = match &ic[0].values()[0] {
                SqliteValue::Text(s) => s.as_ref().to_owned(),
                other => panic!("reopen integrity_check non-text {other:?}"),
            };
            assert_eq!(ic_text, "ok", "reopen integrity_check false-flagged: {ic_text}");
        }
        let bak = format!("{db}.pre-migration-bak");
        assert!(
            !std::path::Path::new(&bak).exists(),
            "spurious first-open migration produced {bak} for a clean {encoding} DB"
        );

        let (stock_tb, stock_tn) = stock_order(encoding);
        assert_eq!(
            frank_tb, stock_tb,
            "BINARY index-key order diverges from stock on {encoding}"
        );
        assert_eq!(
            frank_tn, stock_tn,
            "NOCASE index-key order diverges from stock on {encoding}"
        );
    });
}

#[test]
fn utf16le_index_key_order_matches_stock_encoded_byte_order() {
    run_for_encoding("UTF-16le");
}

#[test]
fn utf16be_index_key_order_matches_stock_encoded_byte_order() {
    run_for_encoding("UTF-16be");
}
