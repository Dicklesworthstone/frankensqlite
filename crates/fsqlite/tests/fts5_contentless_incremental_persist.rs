//! Regression for bd-sf8dx (asymptotic): a contentless FTS5 table
//! (`content=''`) must persist each INSERT statement as an INCREMENTAL segment
//! append rather than re-encoding the entire inverted index from scratch on
//! every insert.
//!
//! Root cause (cass#301 `index --full` wedge): for a contentless rootpage-zero
//! FTS5 table, every INSERT used to rebuild the whole `_data` segment from the
//! complete in-memory index (`persist_rootpage_zero_fts5_shadow_rows` ->
//! `encode_data_rows` -> `build_pending_hash`, which re-dumps every posting).
//! A batch rebuild was therefore O(statements x table) = O(N^2) and wedged
//! `cass index --full` above ~15-30 MB of content (40 MB synthetic: 900s+
//! timeouts, never completes).
//!
//! The fix appends one new segment per INSERT statement (containing only that
//! statement's new rows) onto the existing on-disk structure, so the work per
//! insert is O(new rows), not O(table). Live MATCH still reads the in-memory
//! index; reopen hydrates from the multi-segment structure.
//!
//! "Fails on old / passes on new" gate: the `_data` shadow row count. The old
//! full-re-encode path replaced `_data` wholesale, so it held a constant ~3
//! rows (averages + structure + one merged leaf) regardless of how many INSERT
//! statements ran. The incremental path appends one leaf per statement, so
//! `_data` grows with the number of inserts.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_contentless_incremental_persist -- --nocapture`

#![cfg(feature = "fts5")]

use fsqlite::{Connection, SqliteValue};

const CREATE_SQL: &str = "CREATE VIRTUAL TABLE idx USING fts5(body, content='', tokenize='porter')";

fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    conn.query(&format!(
        "SELECT rowid FROM idx WHERE idx MATCH '{term}' ORDER BY rowid"
    ))
    .expect("MATCH query")
    .iter()
    .map(|r| match &r.values()[0] {
        SqliteValue::Integer(i) => *i,
        other => panic!("unexpected rowid value: {other:?}"),
    })
    .collect()
}

fn insert_doc(conn: &Connection, rowid: i64, body: &str) {
    conn.execute_with_params(
        "INSERT INTO idx(rowid, body) VALUES (?1, ?2)",
        &[SqliteValue::Integer(rowid), SqliteValue::Text(body.into())],
    )
    .expect("insert contentless fts row");
}

fn data_row_count(conn: &Connection) -> i64 {
    match &conn
        .query("SELECT count(*) FROM idx_data")
        .expect("count _data rows")
        .first()
        .expect("one count row")
        .values()[0]
    {
        SqliteValue::Integer(i) => *i,
        other => panic!("unexpected count value: {other:?}"),
    }
}

#[test]
fn contentless_fts_inserts_append_incremental_segments() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    const N: i64 = 24;

    // --- Phase 1: N inserts, each its own autocommit INSERT statement. ---
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").unwrap();
        conn.execute(CREATE_SQL).expect("create contentless fts5");

        for rowid in 1..=N {
            // Each doc has a unique token `docK` plus the shared token `common`.
            insert_doc(&conn, rowid, &format!("doc{rowid} common alpha beta gamma"));
        }

        // Correctness: every unique token is findable, and the shared token
        // returns every row.
        for rowid in 1..=N {
            assert_eq!(
                match_rowids(&conn, &format!("doc{rowid}")),
                vec![rowid],
                "unique token doc{rowid} not searchable"
            );
        }
        assert_eq!(
            match_rowids(&conn, "common"),
            (1..=N).collect::<Vec<_>>(),
            "shared token did not return every inserted row"
        );

        // Incremental gate: `_data` must have grown well past the constant ~3
        // rows the old full-re-encode path produced. One leaf per insert means
        // at least N leaves plus averages + structure.
        let data_rows = data_row_count(&conn);
        assert!(
            data_rows >= N,
            "expected incremental segment growth (>= {N} _data rows), got {data_rows}; \
             the old full-re-encode path holds a constant ~3 rows"
        );
    }

    // --- Phase 2: reopen — multi-segment hydration must reconstruct all rows. ---
    {
        let conn = Connection::open(&path).expect("reopen contentless fts5 db");
        for rowid in 1..=N {
            assert_eq!(
                match_rowids(&conn, &format!("doc{rowid}")),
                vec![rowid],
                "doc{rowid} lost after reopen across {N} appended segments"
            );
        }
        assert_eq!(
            match_rowids(&conn, "common"),
            (1..=N).collect::<Vec<_>>(),
            "shared token lost rows after reopen"
        );

        // --- Phase 3: incremental catch-up insert into the reopened table. ---
        insert_doc(&conn, N + 1, "doc999 common catchup");
        assert_eq!(match_rowids(&conn, "doc999"), vec![N + 1]);
        assert_eq!(
            match_rowids(&conn, "common"),
            (1..=N + 1).collect::<Vec<_>>(),
            "catch-up insert dropped previously persisted rows"
        );
    }

    // --- Phase 4: final reopen — everything persisted. ---
    {
        let conn = Connection::open(&path).expect("second reopen");
        assert_eq!(match_rowids(&conn, "doc999"), vec![N + 1]);
        assert_eq!(
            match_rowids(&conn, "common"),
            (1..=N + 1).collect::<Vec<_>>(),
        );
    }
}
