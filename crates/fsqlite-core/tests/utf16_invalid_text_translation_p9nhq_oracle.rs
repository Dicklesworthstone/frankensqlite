//! bd-p9nhq (GH#180 residual) differential oracle — REFUTES the bead premise.
//!
//! GH#180's CORE bug (BLOB->TEXT->BLOB dropping raw bytes to U+FFFD) was fixed by
//! 84ebdf4b3: a `SmallText::Raw` representation preserves arbitrary TEXT bytes
//! byte-for-byte on the UTF-8 path (guarded by `cast_from_blob_oracle_e2e`).
//!
//! bd-p9nhq was filed as a NARROW RESIDUAL: "match stock's lossy UTF-8 -> UTF-16
//! translation for invalid TEXT sequences", on the theory that when an
//! invalid-UTF-8 TEXT value is stored into a `PRAGMA encoding` = UTF-16 database,
//! stock SQLite substitutes U+FFFD per ill-formed sequence during an encoding
//! conversion, and FrankenSQLite's `String::from_utf8_lossy` -> `encode_utf16`
//! transcode (in `fsqlite-types::value::SmallText::to_record_text_bytes`) would
//! diverge at the substitution boundaries.
//!
//! ## What the rusqlite oracle actually shows (see the UTF-16 arms below)
//!
//! Stock does **NOT** do a lossy UTF-8 -> UTF-16 translation here at all. In a
//! UTF-16 database, `CAST(blob AS TEXT)` is a pure **byte-relabel**: the blob
//! bytes are re-tagged as the database's *native* encoding (stock carries a
//! per-`Mem` encoding tag), WITHOUT any transcode. Consequences, all confirmed
//! by the oracle:
//!   * `CAST(X'41' AS TEXT)` ("A") stored in a UTF-16 DB reads back `hex()` = ""
//!     — a lone odd byte is dropped on the UTF-16 decode, not padded/translated.
//!   * `CAST(X'4142' AS TEXT)` reads back `4142` (relabeled UTF-16LE unit U+4241),
//!     NOT `41004200` (a UTF-8 -> UTF-16 translation of "AB").
//!   * `CAST(X'E28082' AS TEXT)` — a **VALID** UTF-8 char U+2002 — reads back
//!     `E280` (relabel + odd-byte drop), NOT `2002` (its UTF-16 translation).
//!
//! That last row is decisive: a perfectly valid UTF-8 sequence diverges too, so
//! the divergence is not about "invalid TEXT sequences" and is not a lossy-
//! translation substitution-boundary issue. The bead premise is **refuted**.
//!
//! ## True root cause (out of scope for this residual)
//!
//! FrankenSQLite normalizes every TEXT value to canonical UTF-8 / raw-bytes with
//! NO per-value encoding tag, and applies the database encoding only at the
//! storage boundary. So `CAST(blob AS TEXT)` interprets the bytes as canonical
//! UTF-8 and later transcodes to UTF-16 on store. Stock instead relabels the
//! bytes as already-UTF-16. Matching stock would require either a per-value
//! encoding tag (the architectural limitation of the original GH#180, mirrored in
//! the UTF-16 direction) OR a scoped change to the VDBE `sql_cast` BLOB->TEXT arm
//! (`fsqlite-vdbe/src/engine.rs`) so that, on a non-UTF-8 database, it decodes the
//! blob via the DB encoding (`SmallText::from_record_text_bytes(bytes, db_enc)`)
//! rather than `from_arc_bytes` (canonical UTF-8). The prescribed
//! transcode-site U+FFFD substitution would NOT reproduce the oracle (it would
//! yield `8000`/`FDFF`, not stock's ""/`C0AF`) and risks regressing the
//! byte-preserving raw representation, so it is deliberately NOT applied.
//!
//! ## What this file guards
//!
//! * `utf8_control_byte_preservation` — ACTIVE parity guard: the 84ebdf4b3
//!   byte-preserving path (default UTF-8 DB) must keep the exact invalid bytes and
//!   must never reintroduce U+FFFD. FrankenSQLite matches stock byte-for-byte.
//! * `utf16{le,be}_documents_cast_relabel_divergence` — ACTIVE characterization:
//!   pins stock's byte-relabel oracle AND FrankenSQLite's current transcode
//!   output, and asserts they differ. If future architectural work aligns
//!   `CAST(blob AS TEXT)` with stock, these fire and must be updated to parity —
//!   the correct signal that the encoding-model gap has closed.

use fsqlite_core::connection::Connection;

/// `(name, blob_hex)` — each turned into TEXT via `CAST(X'..' AS TEXT)`, stored,
/// then observed through `hex(CAST(x AS BLOB))` (exact bytes, ASCII-safe). The
/// four `invalid_*` rows mirror `cast_from_blob_oracle_e2e`; the `ascii_*` and
/// `valid_*` rows prove the UTF-16 divergence is not specific to invalid UTF-8.
const CASES: &[(&str, &str)] = &[
    ("ascii_A_41", "41"),                 // "A"      valid 1-byte
    ("ascii_AB_4142", "4142"),            // "AB"     valid 2-byte
    ("valid_u2002_e28082", "E28082"),     // U+2002   VALID 3-byte UTF-8
    ("valid_u0080_c280", "C280"),         // U+0080   valid 2-byte UTF-8
    ("invalid_bare_cont_80", "80"),       // bare continuation
    ("invalid_overlong_c0af", "C0AF"),    // overlong '/'
    ("invalid_surrogate_eda080", "EDA080"), // lone high surrogate
    ("invalid_overlong4_f08080af", "F08080AF"), // overlong 4-byte
];

const CREATE: &str = "CREATE TABLE t(x TEXT)";
const READBACK: &str = "SELECT hex(CAST(x AS BLOB)) FROM t ORDER BY rowid";

fn insert_sql() -> String {
    let values: Vec<String> = CASES
        .iter()
        .map(|(_, hex)| format!("(CAST(X'{hex}' AS TEXT))"))
        .collect();
    format!("INSERT INTO t VALUES {}", values.join(", "))
}

/// Read `READBACK` (hex of exact stored bytes) from a stock rusqlite DB.
fn stock_read_hex(path: &std::path::Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let mut stmt = conn.prepare(READBACK).expect("prepare stock readback");
    stmt.query_map([], |row| row.get::<_, String>(0))
        .expect("run stock readback")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect stock readback")
}

/// Stock builds a DB (optionally UTF-16), inserts, and reads it back: the oracle.
fn stock_build_and_read_hex(path: &std::path::Path, encoding: Option<&str>) -> Vec<String> {
    {
        let conn = rusqlite::Connection::open(path).expect("stock writer open");
        if let Some(enc) = encoding {
            conn.execute_batch(&format!("PRAGMA encoding = '{enc}';"))
                .expect("stock PRAGMA encoding");
        }
        conn.execute_batch(CREATE).expect("stock CREATE");
        conn.execute_batch(&insert_sql()).expect("stock INSERT");
    }
    stock_read_hex(path)
}

/// FrankenSQLite builds a DB (optionally UTF-16) and inserts, exercising its
/// UTF-8 -> DB-encoding transcode, then closes/flushes.
async fn frank_build(path: &std::path::Path, encoding: Option<&str>) {
    let db = path.to_str().unwrap();
    let conn = Connection::open(db).await.expect("frank open");
    if let Some(enc) = encoding {
        conn.execute(&format!("PRAGMA encoding = '{enc}';"))
            .await
            .expect("frank PRAGMA encoding");
    }
    conn.execute(CREATE).await.expect("frank CREATE");
    conn.execute(&insert_sql()).await.expect("frank INSERT");
    conn.close().await.expect("frank close/flush");
}

fn header_text_encoding(path: &std::path::Path) -> u32 {
    let bytes = std::fs::read(path).expect("read header");
    u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]])
}

/// Build in stock and in frank, read both back through *stock* (isolating the
/// encode path), print the table, and return `(oracle, frank_via_stock)`.
async fn run_pair(encoding: Option<&str>, expect_header: u32) -> (Vec<String>, Vec<String>) {
    let label = encoding.unwrap_or("UTF-8(default)");
    let dir = tempfile::TempDir::new().unwrap();
    let stock_path = dir.path().join("stock.db");
    let frank_path = dir.path().join("frank.db");

    let oracle = stock_build_and_read_hex(&stock_path, encoding);
    assert_eq!(
        header_text_encoding(&stock_path),
        expect_header,
        "[{label}] stock fixture header encoding"
    );

    frank_build(&frank_path, encoding).await;
    assert_eq!(
        header_text_encoding(&frank_path),
        expect_header,
        "[{label}] frank image header encoding"
    );
    let frank_via_stock = stock_read_hex(&frank_path);

    for (i, (name, hex)) in CASES.iter().enumerate() {
        eprintln!(
            "[{label}] {name:<26} X'{hex:<8}': stock={:<18} frank={}",
            oracle[i], frank_via_stock[i]
        );
    }
    (oracle, frank_via_stock)
}

/// ACTIVE parity guard for the 84ebdf4b3 byte-preserving UTF-8 path. Every case,
/// invalid included, must round-trip byte-for-byte with no U+FFFD substitution.
#[test]
fn utf8_control_byte_preservation() {
    asupersync::test_utils::run_test(|| async {
        let (oracle, frank_via_stock) = run_pair(None, 1).await;
        assert_eq!(
            frank_via_stock, oracle,
            "UTF-8 default DB: FrankenSQLite must preserve exact TEXT bytes (84ebdf4b3)"
        );
        // Concrete anchors: raw invalid bytes survive intact, not as EFBFBD.
        assert_eq!(oracle[4], "80", "stock UTF-8 keeps X'80'");
        assert_eq!(frank_via_stock[4], "80", "frank UTF-8 keeps X'80' (no U+FFFD)");
    });
}

fn cases_index(name: &str) -> usize {
    CASES.iter().position(|(n, _)| *n == name).expect("known case")
}

/// ACTIVE characterization of the UTF-16LE divergence (bd-p9nhq refutation).
#[test]
fn utf16le_documents_cast_relabel_divergence() {
    asupersync::test_utils::run_test(|| async {
        let (oracle, frank_via_stock) = run_pair(Some("UTF-16le"), 2).await;

        // Stock = pure byte-relabel into the native UTF-16LE encoding.
        let expected_stock = [
            ("ascii_A_41", ""),
            ("ascii_AB_4142", "4142"),
            ("valid_u2002_e28082", "E280"),
            ("valid_u0080_c280", "C280"),
            ("invalid_bare_cont_80", ""),
            ("invalid_overlong_c0af", "C0AF"),
            ("invalid_surrogate_eda080", "EDA0"),
            ("invalid_overlong4_f08080af", "F08080AF"),
        ];
        for (name, want) in expected_stock {
            assert_eq!(
                oracle[cases_index(name)], want,
                "stock UTF-16le byte-relabel for {name}"
            );
        }

        // Frank = canonical-UTF-8 interpretation then UTF-8 -> UTF-16LE transcode
        // (from_utf8_lossy for invalid bytes: one U+FFFD => FDFF per subpart).
        let expected_frank = [
            ("ascii_A_41", "4100"),
            ("ascii_AB_4142", "41004200"),
            ("valid_u2002_e28082", "0220"),
            ("valid_u0080_c280", "8000"),
            ("invalid_bare_cont_80", "FDFF"),
            ("invalid_overlong_c0af", "FDFFFDFF"),
            ("invalid_surrogate_eda080", "FDFFFDFFFDFF"),
            ("invalid_overlong4_f08080af", "FDFFFDFFFDFFFDFF"),
        ];
        for (name, want) in expected_frank {
            assert_eq!(
                frank_via_stock[cases_index(name)], want,
                "frank UTF-16le transcode for {name}"
            );
        }

        assert_ne!(
            frank_via_stock, oracle,
            "KNOWN GAP (bd-p9nhq): CAST(blob AS TEXT) byte-relabel vs canonical-UTF-8 transcode"
        );
    });
}

/// ACTIVE characterization of the UTF-16BE divergence (bd-p9nhq refutation).
#[test]
fn utf16be_documents_cast_relabel_divergence() {
    asupersync::test_utils::run_test(|| async {
        let (oracle, frank_via_stock) = run_pair(Some("UTF-16be"), 3).await;

        let expected_stock = [
            ("ascii_A_41", ""),
            ("ascii_AB_4142", "4142"),
            ("valid_u2002_e28082", "E280"),
            ("valid_u0080_c280", "C280"),
            ("invalid_bare_cont_80", ""),
            ("invalid_overlong_c0af", "C0AF"),
            ("invalid_surrogate_eda080", "EDA0"),
            ("invalid_overlong4_f08080af", "F08080AF"),
        ];
        for (name, want) in expected_stock {
            assert_eq!(
                oracle[cases_index(name)], want,
                "stock UTF-16be byte-relabel for {name}"
            );
        }

        let expected_frank = [
            ("ascii_A_41", "0041"),
            ("ascii_AB_4142", "00410042"),
            ("valid_u2002_e28082", "2002"),
            ("valid_u0080_c280", "0080"),
            ("invalid_bare_cont_80", "FFFD"),
            ("invalid_overlong_c0af", "FFFDFFFD"),
            ("invalid_surrogate_eda080", "FFFDFFFDFFFD"),
            ("invalid_overlong4_f08080af", "FFFDFFFDFFFDFFFD"),
        ];
        for (name, want) in expected_frank {
            assert_eq!(
                frank_via_stock[cases_index(name)], want,
                "frank UTF-16be transcode for {name}"
            );
        }

        assert_ne!(
            frank_via_stock, oracle,
            "KNOWN GAP (bd-p9nhq): CAST(blob AS TEXT) byte-relabel vs canonical-UTF-8 transcode"
        );
    });
}
