//! bd-zywqc.14 adversarial corpus — single-bit structural corruption.
//!
//! Flip exactly one bit at a structural offset of a committed database file and
//! verify the engine CATCHES it — `integrity_check` reports corruption, or the
//! open fails loudly — rather than silently returning wrong data. Every fixture
//! is cross-checked against stock C SQLite (rusqlite): both engines must catch
//! the same planted corruption, so the corpus can never certify a fixture that
//! is secretly still "ok" (the corruption-fixture gotcha).
//!
//! Structural fields are targeted deliberately: SQLite's `integrity_check`
//! validates b-tree structure (page type, cell count, cell pointers), not the
//! bytes of a payload, so a flip in free space or inside a TEXT value reads
//! "ok" in both engines and is not a valid corruption fixture.
//!
//! The fixtures use `journal_mode=DELETE` so the committed image is
//! self-contained in the main file — no WAL sidecar where the real data (and
//! thus the real corruption target) could hide.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Build a clean, self-contained fsqlite database whose page 2 is a populated
/// table b-tree leaf. Returns `(db_path, page_size)`. Because the current code
/// stamps a migration marker at birth, reopening this file skips the first-open
/// repair pass — the corruption verdicts below observe a plain `integrity_check`.
async fn build_clean_db(dir: &std::path::Path, name: &str) -> (String, usize) {
    let db = dir.join(name).to_string_lossy().into_owned();
    {
        let conn = Connection::open(&db).await.expect("open");
        conn.execute("PRAGMA journal_mode=DELETE;")
            .await
            .expect("journal_mode");
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("create");
        // Enough rows to densely populate page 2's cell pointer array + content.
        for i in 0..24 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row-value-{i}');"))
                .await
                .expect("insert");
        }
        conn.close().await.expect("close");
    }
    let bytes = std::fs::read(&db).expect("read header");
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    let page_size = if raw == 1 { 65_536 } else { raw as usize };
    (db, page_size)
}

/// Flip a single bit (`mask`) at `offset` of the file at `db`.
fn flip_bit(db: &str, offset: usize, mask: u8) {
    let mut bytes = std::fs::read(db).expect("read image");
    assert!(offset < bytes.len(), "offset {offset} beyond file len {}", bytes.len());
    bytes[offset] ^= mask;
    std::fs::write(db, &bytes).expect("write corrupted image");
}

/// Whether an engine caught the corruption. `SilentlyAccepted` carries the
/// rows returned so a failure message can show what leaked through.
#[derive(Debug)]
enum Verdict {
    Caught,
    SilentlyAccepted(Vec<String>),
}

/// fsqlite's verdict: an open failure or a non-`ok` `integrity_check` counts as
/// caught; a clean `ok` on a structurally-corrupt image is a silent accept.
async fn fsqlite_verdict(db: &str) -> Verdict {
    let Ok(conn) = Connection::open(db).await else {
        return Verdict::Caught;
    };
    let verdict = match conn.query("PRAGMA integrity_check;").await {
        Err(_) => Verdict::Caught,
        Ok(rows) => {
            let lines: Vec<String> = rows
                .iter()
                .filter_map(|r| match &r.values()[0] {
                    SqliteValue::Text(s) => Some(s.as_ref().to_owned()),
                    _ => None,
                })
                .collect();
            if lines == vec!["ok".to_owned()] {
                Verdict::SilentlyAccepted(lines)
            } else {
                Verdict::Caught
            }
        }
    };
    conn.close().await.ok();
    verdict
}

/// Stock C SQLite's verdict on the same file — the oracle.
fn stock_verdict(db: &str) -> Verdict {
    let Ok(conn) = rusqlite::Connection::open(db) else {
        return Verdict::Caught;
    };
    match conn.query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0)) {
        Ok(line) if line == "ok" => Verdict::SilentlyAccepted(vec![line]),
        _ => Verdict::Caught,
    }
}

/// The shared scenario body: plant a one-bit structural flip and require BOTH
/// engines to catch it.
async fn assert_bit_flip_caught(name: &str, offset_in_page2: usize, mask: u8) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (db, page_size) = build_clean_db(dir.path(), &format!("{name}.db")).await;
    let offset = page_size + offset_in_page2;

    // Sanity: the pristine image is clean in both engines (fixture is valid).
    assert!(
        matches!(stock_verdict(&db), Verdict::SilentlyAccepted(l) if l == vec!["ok".to_owned()]),
        "{name}: pristine fixture must be clean in stock before the flip"
    );

    flip_bit(&db, offset, mask);

    let fsq = fsqlite_verdict(&db).await;
    let stock = stock_verdict(&db);
    assert!(
        matches!(stock, Verdict::Caught),
        "{name}: fixture invalid — stock did not catch the flip at page2+{offset_in_page2} \
         (mask {mask:#x}); pick a structural offset. Verdict: {stock:?}"
    );
    assert!(
        matches!(fsq, Verdict::Caught),
        "{name}: fsqlite SILENTLY ACCEPTED a structural corruption stock caught \
         (page2+{offset_in_page2}, mask {mask:#x}) — integrity_check must not certify a \
         corrupt image as ok. Verdict: {fsq:?}"
    );
}

macro_rules! bit_flip_scenario {
    ($fn_name:ident, $label:literal, $off:expr, $mask:expr) => {
        #[test]
        fn $fn_name() {
            asupersync::test_utils::run_test(|| async {
                assert_bit_flip_caught($label, $off, $mask).await;
            });
        }
    };
}

// Page 2 layout (offsets relative to the page start): byte 0 = b-tree page
// type; bytes 3..5 = cell count; bytes 8.. = the 2-byte cell pointer array.
bit_flip_scenario!(bit_flip_page2_btree_page_type, "pg2_type", 0, 0x08);
bit_flip_scenario!(bit_flip_page2_cell_count_high, "pg2_cellcount_hi", 3, 0x01);
bit_flip_scenario!(bit_flip_page2_cell_count_low, "pg2_cellcount_lo", 4, 0x02);
bit_flip_scenario!(bit_flip_page2_first_cell_pointer, "pg2_cellptr0", 8, 0x08);
bit_flip_scenario!(bit_flip_page2_second_cell_pointer, "pg2_cellptr1", 10, 0x08);
bit_flip_scenario!(bit_flip_page2_cell_content_area, "pg2_cellcontent", 5, 0x10);
