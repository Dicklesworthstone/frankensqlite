//! Regression for cass#362: FTS5 must survive terms longer than the u16
//! segment-leaf offset space instead of failing the INSERT.
//!
//! Real coding-agent corpora routinely contain single whitespace-delimited
//! tokens far beyond 64 KiB (minified JS bundles and base64 blobs pasted into
//! tool outputs — a survey of 8,261 real session files found 173 with a token
//! over 60 KB, the observed worst case 91,548 bytes). Before the fix,
//! `Fts5SegmentLeaf::encode` failed with "fts5: corrupt %_data record:
//! segment leaf term offset exceeds u16" on the first batch containing such a
//! token, which poisoned every repair path: the failing INSERT aborted
//! `index --full`, `doctor --rebuild-canonical-fts`, and even a from-scratch
//! rebuild after dropping the shadow tables.
//!
//! The fix skips terms larger than `FTS5_MAX_TERM_BYTES` at the shared
//! tokenizer decorator (index- and query-side symmetric), mirroring stock C
//! FTS5's *observable* behavior — C writes the oversized term with a silently
//! wrapped `szLeaf` header, leaving it unqueryable — while keeping the write
//! path total. Porter additionally passes tokens >64 bytes through unstemmed,
//! matching C's `FTS5_PORTER_MAX_TOKEN`.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_overlong_term -- --nocapture`

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
    .expect("insert contentless fts row with overlong term");
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

#[test]
fn overlong_term_does_not_fail_insert_and_neighbors_stay_searchable() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        // The observed cass#362 shape: one giant token amid ordinary ones.
        let giant = "a".repeat(91_548);

        {
            let conn = Connection::open(&path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute(CREATE_SQL)
                .await
                .expect("create contentless fts5");

            insert_doc(&conn, 1, &format!("before {giant} needle")).await;
            insert_doc(&conn, 2, "unrelated haystack").await;

            // Ordinary tokens from the same document (before and after the
            // giant token) remain searchable in the live session.
            assert_eq!(match_rowids(&conn, "needle").await, vec![1]);
            assert_eq!(match_rowids(&conn, "before").await, vec![1]);

            // The segment write happened: the shadow is populated, not the
            // empty structure the old silent-wipe fall-through produced.
            assert!(
                data_row_count(&conn).await > 0,
                "the _data shadow must contain the flushed segment"
            );
        }

        // Reopen from disk: the persisted segments hydrate and the document
        // stays searchable across sessions.
        {
            let conn = Connection::open(&path).await.unwrap();
            assert_eq!(
                match_rowids(&conn, "needle").await,
                vec![1],
                "overlong-term document lost across reopen"
            );
            assert_eq!(match_rowids(&conn, "haystack").await, vec![2]);
        }
    });
}

#[test]
fn overlong_term_batch_rebuild_shape_survives() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        // The cass repair-path shape: a multi-row batch whose middle row
        // carries the pathological token. Before the fix the whole batch
        // failed ("inserting N rows into fts_messages during streaming FTS
        // maintenance"), and with it every rebuild attempt.
        let giant = "z".repeat(70_000);

        let conn = Connection::open(&path).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        conn.execute(CREATE_SQL)
            .await
            .expect("create contentless fts5");

        for rowid in 1..=8_i64 {
            let body = if rowid == 4 {
                format!("row{rowid} {giant} marker")
            } else {
                format!("row{rowid} marker")
            };
            insert_doc(&conn, rowid, &body).await;
        }

        assert_eq!(
            match_rowids(&conn, "marker").await,
            (1..=8).collect::<Vec<_>>(),
            "every row of the batch must be indexed, including the one \
             carrying the overlong token"
        );
        assert_eq!(match_rowids(&conn, "row4").await, vec![4]);
    });
}
