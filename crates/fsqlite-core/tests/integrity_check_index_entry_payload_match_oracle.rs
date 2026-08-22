//! bd-o499g: fsqlite `PRAGMA integrity_check` must agree with stock sqlite3 on a
//! valid, actively-written multi-column index.
//!
//! Split from bd-y2aog symptom 2. The checker must not false-flag "index `X`
//! entry for rowid N does not match the table row payload" on an index stock
//! validates clean.
//!
//! The checker (connection.rs) compares the index entry against a key record it
//! rebuilds from the table row with a RAW BYTE comparison. If the rebuild
//! serializes a key value differently than the live writer did (serial-type
//! width, TEXT/BLOB affinity, collation, NULLs, REAL), a semantically-valid
//! index false-flags while stock — which compares semantically — passes.
//!
//! This keeper drives several serial-type/affinity/collation shapes through
//! INSERT + UPDATE churn on multi-column plain / UNIQUE / partial indexes and
//! asserts fsqlite's integrity_check agrees with rusqlite ("ok").

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &str = "\
CREATE TABLE facts (
    id TEXT NOT NULL,
    period TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    amount REAL,
    tag TEXT COLLATE NOCASE,
    note TEXT
);
CREATE INDEX idx_identity ON facts(id, period, ordinal);
CREATE UNIQUE INDEX idx_unique_identity ON facts(id, ordinal);
CREATE INDEX idx_tag_nocase ON facts(tag);
CREATE INDEX idx_partial ON facts(amount) WHERE amount IS NOT NULL;";

// Serial-type variance: integers spanning every SQLite serial-type width, reals,
// texts (incl. NOCASE-collated), and NULLs — the values whose on-disk encoding
// is most likely to diverge from a naive rebuild.
const INSERTS: &[&str] = &[
    "INSERT INTO facts VALUES ('acct-1','2026-Q1',0,   0.0,      'Alpha', 'a')",
    "INSERT INTO facts VALUES ('acct-1','2026-Q1',1,   127.0,    'ALPHA', NULL)",
    "INSERT INTO facts VALUES ('acct-1','2026-Q2',128, 32767.5,  'beta',  'b')",
    "INSERT INTO facts VALUES ('acct-2','2026-Q1',32768, NULL,   'BETA',  NULL)",
    "INSERT INTO facts VALUES ('acct-2','2026-Q2',8388608, -1.0, NULL,    'c')",
    "INSERT INTO facts VALUES ('acct-3','2026-Q1',2147483647, 1e300, 'g', 'd')",
    "INSERT INTO facts VALUES ('acct-3','2026-Q3',9223372036854775807, -0.0, '', 'e')",
];

// Rewrite index entries: UPDATE key columns (moves entries), non-key columns
// (rewrites payloads), and toggle the partial-index predicate.
const UPDATES: &[&str] = &[
    "UPDATE facts SET ordinal = ordinal + 1000 WHERE id = 'acct-1'",
    "UPDATE facts SET amount = amount * 2 WHERE amount IS NOT NULL",
    "UPDATE facts SET amount = NULL WHERE ordinal = 128 + 1000",
    "UPDATE facts SET amount = 42.0 WHERE amount IS NULL AND id = 'acct-2'",
    "UPDATE facts SET tag = 'ReTagged' WHERE tag = 'g'",
    "UPDATE facts SET period = '2026-Q4' WHERE id = 'acct-3'",
];

fn stock_integrity() -> String {
    let c = rusqlite::Connection::open_in_memory().expect("open stock");
    c.execute_batch(SETUP).expect("stock setup");
    for stmt in INSERTS.iter().chain(UPDATES.iter()) {
        c.execute(stmt, [])
            .unwrap_or_else(|e| panic!("stock `{stmt}`: {e}"));
    }
    c.query_row("PRAGMA integrity_check;", [], |r| r.get::<_, String>(0))
        .expect("stock integrity_check")
}

#[test]
fn integrity_check_agrees_with_stock_on_serial_type_variant_indexes() {
    // Premise: stock sqlite3 finds this actively-written shape consistent.
    assert_eq!(
        stock_integrity(),
        "ok",
        "premise: stock integrity_check is ok"
    );

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        for stmt in SETUP.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            conn.execute(stmt).await.expect("fsqlite setup");
        }
        for stmt in INSERTS.iter().chain(UPDATES.iter()) {
            conn.execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("fsqlite `{stmt}`: {e:?}"));
        }

        let rows = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("fsqlite integrity_check");
        let report = match &rows[0].values()[0] {
            SqliteValue::Text(s) => s.as_ref().to_owned(),
            other => panic!("integrity_check did not return text: {other:?}"),
        };
        assert_eq!(
            report, "ok",
            "fsqlite integrity_check must agree with stock (bd-o499g); \
             a byte-strict index entry/payload check false-flags a valid index"
        );
    });
}
