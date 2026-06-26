//! Regression for bd-sf8dx / cass y8n3i: a contentless FTS5 table
//! (`content=''`) must survive a close/reopen cycle and then accept further
//! mutations (incremental catch-up), exactly as cass's `fts_messages` derived
//! search index does.
//!
//! Two defects this guards against (both surfaced by cass `index --full` +
//! incremental FTS catch-up against frankensqlite 0.1.11):
//!   1. OPEN: reopening a `content=''` table wrongly demanded a `_content`
//!      shadow table → `database disk image is malformed: FTS5 table
//!      fts_messages is missing required content shadow table
//!      fts_messages_content`.
//!   2. MUTATE: inserting a new row into a reopened contentless table with
//!      persisted segments errored → `fts5: cannot mutate a lazily opened
//!      contentless table with persisted segments`.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_contentless_reopen_mutate -- --nocapture`

#![cfg(feature = "fts5")]

use fsqlite::{Connection, SqliteValue};

/// cass's canonical contentless `fts_messages` schema (src/storage/sqlite.rs
/// `FTS5_REGISTER_SQL`): first column is literally named `content`, plus the
/// `content=''` table option that makes it contentless.
const CASS_FTS_SQL: &str = "CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(\
    content, title, agent, workspace, source_path, \
    created_at UNINDEXED, \
    content='', tokenize='porter')";

fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    conn.query(&format!(
        "SELECT rowid FROM fts_messages WHERE fts_messages MATCH '{term}' ORDER BY rowid"
    ))
    .expect("MATCH query")
    .iter()
    .map(|r| match &r.values()[0] {
        SqliteValue::Integer(i) => *i,
        other => panic!("unexpected rowid value: {other:?}"),
    })
    .collect()
}

fn insert_doc(conn: &Connection, rowid: i64, content: &str, title: &str) {
    conn.execute_with_params(
        "INSERT INTO fts_messages(rowid, content, title, agent, workspace, source_path, created_at) \
         VALUES (?1, ?2, ?3, 'codex', '/ws', '/ws/s.jsonl', '0')",
        &[
            SqliteValue::Integer(rowid),
            SqliteValue::Text(content.into()),
            SqliteValue::Text(title.into()),
        ],
    )
    .expect("insert contentless fts row");
}

#[test]
fn contentless_fts_reopen_then_incremental_insert() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    // --- Phase 1: create + seed, then close (persist segments to disk). ---
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute("PRAGMA journal_mode = WAL;").unwrap();
        conn.execute(CASS_FTS_SQL).expect("create contentless fts5");
        insert_doc(&conn, 1, "authentication error in login flow", "auth bug");
        insert_doc(&conn, 2, "database migration applied cleanly", "db notes");
        assert_eq!(match_rowids(&conn, "authentication"), vec![1]);
    }

    // --- Phase 2: reopen (Bug 1 — open-validation must accept content=''). ---
    {
        let conn = Connection::open(&path).expect("reopen contentless fts5 db");
        // Reads must still work against the persisted segments.
        assert_eq!(
            match_rowids(&conn, "authentication"),
            vec![1],
            "reopened contentless table lost its persisted postings"
        );
        assert_eq!(match_rowids(&conn, "migration"), vec![2]);

        // --- Phase 3: incremental catch-up insert (Bug 2 — reopen-mutate). ---
        insert_doc(&conn, 3, "authentication catchup after reopen", "catchup");
        assert_eq!(
            match_rowids(&conn, "catchup"),
            vec![3],
            "new row not searchable after incremental insert into reopened table"
        );
        // Old rows must remain searchable (the append must not drop segments).
        assert_eq!(match_rowids(&conn, "authentication"), vec![1, 3]);
        assert_eq!(match_rowids(&conn, "migration"), vec![2]);
    }

    // --- Phase 4: reopen again; everything persisted and searchable. ---
    {
        let conn = Connection::open(&path).expect("second reopen");
        assert_eq!(match_rowids(&conn, "authentication"), vec![1, 3]);
        assert_eq!(match_rowids(&conn, "migration"), vec![2]);
        assert_eq!(match_rowids(&conn, "catchup"), vec![3]);
    }
}
