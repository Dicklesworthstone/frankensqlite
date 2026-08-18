//! Match stock's contentless FTS5 UPDATE admission rules (found by the GH#358
//! lazy-default blast-radius audit):
//! - `contentless_delete`: an UPDATE must assign EVERY column (a full-row replace);
//!   a subset-of-columns UPDATE is rejected. Before the fix frank accepted it,
//!   deleting the old row and inserting a partial one — corrupting the index
//!   (count(*) dropped 3 -> 1 in the repro).
//! - plain contentless (`content=''` without `contentless_delete`): UPDATE is
//!   forbidden entirely.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn scalar_i64(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer, got {other:?}"),
    }
}

fn rowids(rows: &[fsqlite_core::connection::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn bd_fts5_contentless_delete_update_requires_all_columns() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_str = dir.path().join("cd.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE cd USING fts5(a, b, content='', contentless_delete=1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO cd(rowid,a,b) VALUES (1,'alpha','one'),(2,'beta','two'),(3,'gamma','three');")
            .await
            .unwrap();

        // Subset-of-columns UPDATE is rejected.
        let err = conn
            .execute("UPDATE cd SET a='changed' WHERE rowid=1;")
            .await
            .expect_err("subset-of-columns UPDATE must be rejected");
        assert!(
            err.to_string().contains("subset of columns"),
            "unexpected error: {err}"
        );

        // The rejection left the table intact — no corruption.
        assert_eq!(
            scalar_i64(&conn.query("SELECT count(*) FROM cd;").await.unwrap()),
            3,
            "count intact after a rejected subset UPDATE"
        );
        assert_eq!(
            rowids(&conn.query("SELECT rowid FROM cd WHERE cd MATCH 'alpha';").await.unwrap()),
            vec![1],
            "old row untouched after rejected UPDATE"
        );

        // A full-row UPDATE (every column assigned) succeeds and reindexes.
        conn.execute("UPDATE cd SET a='fresh', b='text' WHERE rowid=1;")
            .await
            .unwrap();
        assert_eq!(
            scalar_i64(&conn.query("SELECT count(*) FROM cd;").await.unwrap()),
            3,
            "count preserved by a full-row UPDATE"
        );
        assert!(
            conn.query("SELECT rowid FROM cd WHERE cd MATCH 'alpha';").await.unwrap().is_empty(),
            "old term gone after full UPDATE"
        );
        assert_eq!(
            rowids(&conn.query("SELECT rowid FROM cd WHERE cd MATCH 'fresh';").await.unwrap()),
            vec![1],
            "new term indexed by full UPDATE"
        );
    });
}

#[test]
fn bd_fts5_plain_contentless_update_forbidden() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_str = dir.path().join("pc.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE pc USING fts5(a, b, content='');")
            .await
            .unwrap();
        conn.execute("INSERT INTO pc(rowid,a,b) VALUES (1,'alpha','one');")
            .await
            .unwrap();
        let err = conn
            .execute("UPDATE pc SET a='z', b='y' WHERE rowid=1;")
            .await
            .expect_err("UPDATE on a plain contentless table must be rejected");
        assert!(
            err.to_string().contains("contentless fts5 table"),
            "unexpected error: {err}"
        );
    });
}
