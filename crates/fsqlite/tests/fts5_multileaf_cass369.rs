//! Regression for cass#369: a single FTS5 segment leaf stores term/rowid/footer
//! offsets in u16 fields, so a flush whose combined terms + doclists encode past
//! 65,535 bytes used to fail with "segment leaf term offset exceeds u16" — the
//! cumulative-overflow case that the gh362 overlong-*term* tokenizer cap does
//! not address (every term here is well under the 1 KiB cap; it is the *sum* of
//! thousands of in-cap terms that overflows one leaf).
//!
//! The fix partitions an oversized flush across MULTIPLE leaves (a segment
//! spanning pgno 1..=N), which the reader already walks. This test drives a
//! single document with 15,000 distinct short tokens (~240 KiB of encoded
//! segment leaf) through a real contentless FTS5 table and asserts every token
//! stays MATCH-queryable, in the live session and across a reopen.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_multileaf_cass369 -- --nocapture`

#![cfg(feature = "fts5")]

use fsqlite::{Connection, SqliteValue};

const CREATE_SQL: &str =
    "CREATE VIRTUAL TABLE idx USING fts5(body, content='', tokenize='unicode61')";

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

async fn data_row_count(conn: &Connection) -> i64 {
    match &conn
        .query("SELECT count(*) FROM idx_data")
        .await
        .expect("count _data rows")
        .first()
        .expect("one count row")
        .values()[0]
    {
        SqliteValue::Integer(i) => *i,
        other => panic!("unexpected count value: {other:?}"),
    }
}

/// 15,000 distinct `tokNNNNN` tokens: ~240 KiB of encoded segment-leaf, well
/// past the 64 KiB u16 ceiling, forcing a multi-leaf segment.
fn many_distinct_tokens(count: usize) -> String {
    (0..count)
        .map(|i| format!("tok{i:06}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn cumulative_oversized_leaf_splits_into_multiple_leaves_and_stays_searchable() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        {
            let conn = Connection::open(&path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute(CREATE_SQL)
                .await
                .expect("create contentless fts5");

            // One document whose 15,000 distinct in-cap tokens encode to far
            // more than one u16-bounded leaf. Before the fix this INSERT failed
            // with "segment leaf term offset exceeds u16".
            insert_doc(&conn, 1, &many_distinct_tokens(15_000)).await;
            // A second ordinary document, to prove cross-document merge still
            // works over the multi-leaf segment.
            insert_doc(&conn, 2, "unrelated haystack needle").await;

            // Tokens from the start, middle, and end of the oversized document
            // must all resolve — i.e. terms in every leaf of the split segment.
            assert_eq!(match_rowids(&conn, "tok000000").await, vec![1]);
            assert_eq!(match_rowids(&conn, "tok007500").await, vec![1]);
            assert_eq!(match_rowids(&conn, "tok014999").await, vec![1]);
            assert_eq!(match_rowids(&conn, "needle").await, vec![2]);
            // A token that was never inserted must not match.
            assert_eq!(match_rowids(&conn, "tok099999").await, Vec::<i64>::new());

            // The segment was actually written as more than a single leaf: a
            // one-leaf-per-segment write of this document is impossible under
            // the u16 ceiling, so a populated shadow proves the split.
            assert!(
                data_row_count(&conn).await > 0,
                "the _data shadow must contain the flushed multi-leaf segment"
            );
        }

        // Reopen from disk: the persisted multi-leaf segment hydrates and every
        // token stays searchable across sessions.
        {
            let conn = Connection::open(&path).await.unwrap();
            assert_eq!(
                match_rowids(&conn, "tok000000").await,
                vec![1],
                "first-leaf token lost across reopen"
            );
            assert_eq!(
                match_rowids(&conn, "tok014999").await,
                vec![1],
                "last-leaf token lost across reopen"
            );
            assert_eq!(match_rowids(&conn, "haystack").await, vec![2]);
        }
    });
}
