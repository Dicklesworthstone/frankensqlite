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
//! "Fails on old / passes on new" gate: the maximum allocated segid in the
//! `_data` shadow. The old full-re-encode path replaced `_data` wholesale and
//! always rebuilt a single segment with segid 1, regardless of how many
//! INSERT statements ran. The incremental path allocates `next_segid = max +
//! 1` per appending statement (and merges allocate still-higher segids for
//! their outputs), so the maximum segid grows with the number of inserts.
//! The raw `_data` ROW count is deliberately not the gate any more: the lazy
//! automerge (bd-fts5-lazy-shadow-reads-itcc4.3, 654621ac5) exists precisely
//! to keep that count bounded, which had left the original >= N row gate
//! stale-red while every correctness assertion still passed.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_contentless_incremental_persist -- --nocapture`

#![cfg(feature = "fts5")]

use fsqlite::{Connection, SqliteValue};

const CREATE_SQL: &str = "CREATE VIRTUAL TABLE idx USING fts5(body, content='', tokenize='porter')";

async fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    conn.query(&format!(
        "SELECT rowid FROM idx WHERE idx MATCH '{term}' ORDER BY rowid"
    ))
    .await
    .expect("MATCH query")
    .iter()
    .map(|r| match &r.values()[0] {
        SqliteValue::Integer(i) => *i,
        other => panic!("unexpected rowid value: {other:?}"),
    })
    .collect()
}

async fn insert_doc(conn: &Connection, rowid: i64, body: &str) {
    conn.execute_with_params(
        "INSERT INTO idx(rowid, body) VALUES (?1, ?2)",
        &[SqliteValue::Integer(rowid), SqliteValue::Text(body.into())],
    )
    .await
    .expect("insert contentless fts row");
}

/// Maximum segid allocated in the `_data` shadow: segment-leaf rowids encode
/// `(segid << 37) | pgno`, and ids 1 (averages) / 10 (structure) sit below
/// the shift, so filtering to `id > 10` leaves only segment-owned rows.
async fn max_data_segid(conn: &Connection) -> i64 {
    match &conn
        .query("SELECT max(id >> 37) FROM idx_data WHERE id > 10")
        .await
        .expect("max _data segid")
        .first()
        .expect("one max row")
        .values()[0]
    {
        SqliteValue::Integer(i) => *i,
        other => panic!("unexpected max segid value: {other:?}"),
    }
}

#[test]
fn contentless_fts_inserts_append_incremental_segments() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        const N: i64 = 24;

        // --- Phase 1: N inserts, each its own autocommit INSERT statement. ---
        {
            let conn = Connection::open(&path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute(CREATE_SQL)
                .await
                .expect("create contentless fts5");

            for rowid in 1..=N {
                // Each doc has a unique token `docK` plus the shared token `common`.
                insert_doc(&conn, rowid, &format!("doc{rowid} common alpha beta gamma")).await;
            }

            // Correctness: every unique token is findable, and the shared token
            // returns every row.
            for rowid in 1..=N {
                assert_eq!(
                    match_rowids(&conn, &format!("doc{rowid}")).await,
                    vec![rowid],
                    "unique token doc{rowid} not searchable"
                );
            }
            assert_eq!(
                match_rowids(&conn, "common").await,
                (1..=N).collect::<Vec<_>>(),
                "shared token did not return every inserted row"
            );

            // Incremental gate: each appending statement allocates
            // `next_segid = max + 1`, and automerge outputs allocate
            // still-higher segids, so after N single-statement inserts the
            // maximum segid is >= N. The old full-re-encode path rebuilt one
            // segment with segid 1 no matter how many statements ran. (The
            // `_data` ROW count is automerge-bounded by design and is no
            // longer a valid incremental observable.)
            let max_segid = max_data_segid(&conn).await;
            assert!(
                max_segid >= N,
                "expected incremental segid growth (max segid >= {N}), got {max_segid}; \
             the old full-re-encode path pins the single rebuilt segment at segid 1"
            );
        }

        // --- Phase 2: reopen — multi-segment hydration must reconstruct all rows. ---
        {
            let conn = Connection::open(&path)
                .await
                .expect("reopen contentless fts5 db");
            for rowid in 1..=N {
                assert_eq!(
                    match_rowids(&conn, &format!("doc{rowid}")).await,
                    vec![rowid],
                    "doc{rowid} lost after reopen across {N} appended segments"
                );
            }
            assert_eq!(
                match_rowids(&conn, "common").await,
                (1..=N).collect::<Vec<_>>(),
                "shared token lost rows after reopen"
            );

            // --- Phase 3: incremental catch-up insert into the reopened table. ---
            insert_doc(&conn, N + 1, "doc999 common catchup").await;
            assert_eq!(match_rowids(&conn, "doc999").await, vec![N + 1]);
            assert_eq!(
                match_rowids(&conn, "common").await,
                (1..=N + 1).collect::<Vec<_>>(),
                "catch-up insert dropped previously persisted rows"
            );
        }

        // --- Phase 4: final reopen — everything persisted. ---
        {
            let conn = Connection::open(&path).await.expect("second reopen");
            assert_eq!(match_rowids(&conn, "doc999").await, vec![N + 1]);
            assert_eq!(
                match_rowids(&conn, "common").await,
                (1..=N + 1).collect::<Vec<_>>(),
            );
        }
    });
}
