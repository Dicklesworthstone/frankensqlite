//! bd-fts5-lazy-shadow-reads-itcc4.3 special commands: `'delete-all'`.
//!
//! `INSERT INTO t(t) VALUES('delete-all')` on a lazy `contentless_delete` FTS5
//! table clears the whole index WITHOUT hydrating the corpus, re-insert works
//! afterwards, and bundled stock SQLite reads the cleared image identically.
//! `'delete-all'` on a stored-content table is rejected, matching stock.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn rowids(rows: &[fsqlite_core::connection::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn bd_fts5_lazy_delete_all_clears_without_promote() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_delete_all.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Write a contentless_delete corpus, then reopen so it binds lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=6 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        let before = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&before), vec![1, 2, 3, 4, 5, 6], "reopened corpus");

        // delete-all clears every row.
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();
        let after = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common';")
            .await
            .unwrap();
        assert!(after.is_empty(), "delete-all removed every row");

        // Re-insert works after the reset.
        conn.execute("INSERT INTO t(rowid, body) VALUES (100, 'fresh start');")
            .await
            .unwrap();
        let fresh = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'fresh';")
            .await
            .unwrap();
        assert_eq!(rowids(&fresh), vec![100], "insert after delete-all");
        // The deleted terms stay gone.
        let common_gone = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common';")
            .await
            .unwrap();
        assert!(common_gone.is_empty(), "old terms did not resurrect");
        conn.close().await.unwrap();

        // Stock reads the cleared + re-inserted image.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check after delete-all");
        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(matched.is_empty(), "stock: delete-all cleared the corpus");
        let fresh_stock: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'fresh'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(fresh_stock, vec![100], "stock reads the post-reset insert");
    });
}

/// bd-zdsh2 regression guard: `BEGIN; INSERT INTO t(t) VALUES('delete-all');
/// ROLLBACK` on a reopened (lazily-bound) contentless fts5 table must restore
/// every row — the index reset is rolled back with the pager. Frank already
/// gets the OBSERVABLE result right here (the query re-reads the rolled-back
/// durable image), so this passes with OR without the bd-zdsh2 instance-txn
/// registration; it locks in the correct rollback behavior. The bd-zdsh2 fix
/// itself is defensive instance-consistency hardening (registering a live-vtab
/// txn for delete-all like the delete-rowids path) whose suspected `count(*)=0`
/// symptom did not reproduce via a normal reopen.
#[test]
fn bd_zdsh2_delete_all_rollback_preserves_lazy_binding() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_delete_all_rollback.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Write a contentless_delete corpus, then reopen so it binds lazily.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=6 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        let before = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&before), vec![1, 2, 3, 4, 5, 6], "reopened corpus");

        // delete-all inside an explicit transaction, then ROLLBACK.
        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();
        conn.execute("ROLLBACK;").await.unwrap();

        // The ROLLBACK must restore every row: the in-memory index reset is undone
        // with the pager, not left applied on the lazy binding.
        let after = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rowids(&after),
            vec![1, 2, 3, 4, 5, 6],
            "ROLLBACK must restore the delete-all on a lazy binding"
        );
        conn.close().await.unwrap();

        // Stock reopens and reads the preserved (rolled-back) corpus.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            matched,
            vec![1, 2, 3, 4, 5, 6],
            "stock reads the rolled-back corpus"
        );
    });
}

#[test]
fn bd_fts5_delete_all_rejects_non_contentless() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_delete_all_reject.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        // Stored-content table (no `content=` option): stock rejects 'delete-all'.
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'hello world');")
            .await
            .unwrap();

        let err = conn
            .execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .expect_err("'delete-all' must be rejected on a stored-content table");
        assert!(
            err.to_string().contains("contentless"),
            "unexpected error: {err}"
        );
        conn.close().await.unwrap();
    });
}

/// bd-i3ldw: `'delete-all'` clears the FTS index but never the content SOURCE,
/// so stock allows it on an EXTERNAL-CONTENT table (content='<table>'), not just
/// a contentless one. Only a regular stored-content table is rejected.
#[test]
fn bd_i3ldw_delete_all_allows_external_content() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ext.db");
        let db_str = db_path.to_string_lossy().into_owned();
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'alpha term'), (2, 'beta term');")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body, content='src', content_rowid='id');")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(t) VALUES('rebuild');")
            .await
            .unwrap();

        // 'delete-all' on an external-content table must SUCCEED (clears index).
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .expect("'delete-all' must be allowed on an external-content fts5 table");

        // The external content source is untouched.
        let src_rows = conn.query("SELECT count(*) FROM src;").await.unwrap();
        assert_eq!(
            src_rows[0].values()[0],
            SqliteValue::Integer(2),
            "'delete-all' must not touch the external content table"
        );
        conn.close().await.unwrap();
    });
}

/// bd-2mzkj (P1): `'delete-all'` on an EXTERNAL-CONTENT fts5 table clears ONLY
/// the index — the documents live in the content source, so `count(*)` and
/// non-MATCH scans still see every row (only MATCH goes empty). f6f864a7c wrongly
/// routed external content through the contentless doc-wipe (`_docsize` clear +
/// `mark_lazy_on_disk(0)`), pinning `count(*)` to 0 and hiding the scan. The
/// landed `bd_i3ldw` keeper only checked the src table, not the vtab.
#[test]
fn bd_2mzkj_external_content_delete_all_preserves_content_visibility() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ext_vis.db");
        let db_str = db_path.to_string_lossy().into_owned();

        fn count_scalar(rows: &[fsqlite_core::connection::Row]) -> i64 {
            match rows[0].values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("expected integer count, got {other:?}"),
            }
        }

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'hello world'), (2, 'foo bar');")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body, content='src', content_rowid='id');")
            .await
            .unwrap();
        // External-content tables are kept in sync by explicit INSERT INTO t (or
        // triggers), which populate the index + the doc count.
        conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'hello world'), (2, 'foo bar');")
            .await
            .unwrap();
        conn.close().await.unwrap();

        // Reopen so the fts5 table binds LAZILY — that is the state `delete-all`
        // targets (persist_rootpage_zero_fts5_contentless_delete_all). Its row
        // count is hydrated from the persisted docsize/segments on open.
        let conn = Connection::open(&db_str).await.unwrap();

        // Before delete-all: two rows visible; MATCH 'hello' finds row 1.
        assert_eq!(
            count_scalar(&conn.query("SELECT count(*) FROM t;").await.unwrap()),
            2,
            "pre count(*)"
        );
        assert_eq!(
            count_scalar(&conn.query("SELECT count(*) FROM (SELECT body FROM t);").await.unwrap()),
            2,
            "pre scan"
        );
        assert_eq!(
            count_scalar(&conn.query("SELECT count(*) FROM t WHERE t MATCH 'hello';").await.unwrap()),
            1,
            "pre MATCH"
        );

        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();

        // After delete-all: the index is cleared (MATCH empty) but the content
        // source is untouched, so count(*) and scans still see both rows.
        // Stock (confirmed): count(*)=2, scan=2, MATCH=0.
        assert_eq!(
            count_scalar(&conn.query("SELECT count(*) FROM t;").await.unwrap()),
            2,
            "bd-2mzkj: external-content delete-all must keep count(*) reading the content source, not 0"
        );
        assert_eq!(
            count_scalar(&conn.query("SELECT count(*) FROM (SELECT body FROM t);").await.unwrap()),
            2,
            "external-content scans still read the content source after delete-all"
        );
        assert_eq!(
            count_scalar(&conn.query("SELECT count(*) FROM t WHERE t MATCH 'hello';").await.unwrap()),
            0,
            "delete-all cleared the index, so MATCH finds nothing"
        );
        conn.close().await.unwrap();

        // Stock oracle reads the same image (count(*)=2, MATCH=0, integrity ok).
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integ: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integ, "ok", "stock integrity_check after external-content delete-all");
        let stock_count: i64 = stock
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stock_count, 2, "stock: count(*) reads the content source after delete-all");
        let stock_match: i64 = stock
            .query_row("SELECT count(*) FROM t WHERE t MATCH 'hello';", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stock_match, 0, "stock: delete-all cleared the index");
    });
}

/// bd-plxob(a): after an external-content `'delete-all'`, a non-MATCH scan must
/// project the REAL column bodies read from the content SOURCE — not NULL. The
/// bd-2mzkj keeper above only asserts row COUNTS/MATCH; a NULL body still counts
/// as a row, so this pins the actual values.
#[test]
fn bd_plxob_external_content_delete_all_projects_real_bodies() {
    asupersync::test_utils::run_test(|| async {
        fn body_values(rows: &[fsqlite_core::connection::Row]) -> Vec<Option<String>> {
            rows.iter()
                .map(|row| match &row.values()[0] {
                    SqliteValue::Text(s) => Some(s.to_string()),
                    SqliteValue::Null => None,
                    other => panic!("unexpected body value: {other:?}"),
                })
                .collect()
        }

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("plxob_bodies.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'hello world'), (2, 'foo bar');")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body, content='src', content_rowid='id');")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'hello world'), (2, 'foo bar');")
            .await
            .unwrap();
        conn.close().await.unwrap();

        // Reopen so t binds lazily — the state delete-all targets.
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();

        let bodies = body_values(&conn.query("SELECT body FROM t ORDER BY rowid;").await.unwrap());
        assert_eq!(
            bodies,
            vec![Some("hello world".to_owned()), Some("foo bar".to_owned())],
            "bd-plxob(a): external-content scan after delete-all must read bodies from the content source, not NULL"
        );
        conn.close().await.unwrap();

        // Stock oracle reads the same image identically.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let mut stmt = stock.prepare("SELECT body FROM t ORDER BY rowid;").unwrap();
        let stock_bodies: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(std::result::Result::unwrap)
            .collect();
        assert_eq!(
            stock_bodies,
            vec!["hello world".to_owned(), "foo bar".to_owned()],
            "stock: external-content bodies after delete-all"
        );
    });
}

/// bd-plxob(b): `INSERT INTO t(t) VALUES('rebuild')` on an external-content table
/// must re-index the content SOURCE (stock behavior), not the empty in-memory
/// documents. Previously frank rebuilt from `all_rows()` and produced count(*)=0.
#[test]
fn bd_plxob_external_content_rebuild_reindexes_source() {
    asupersync::test_utils::run_test(|| async {
        fn scalar(rows: &[fsqlite_core::connection::Row]) -> i64 {
            match rows[0].values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("expected integer, got {other:?}"),
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("plxob_rebuild.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'hello world'), (2, 'foo bar');")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body, content='src', content_rowid='id');")
            .await
            .unwrap();

        // No explicit INSERT INTO t: the index is empty until 'rebuild' pulls the
        // rows from the content source.
        conn.execute("INSERT INTO t(t) VALUES('rebuild');")
            .await
            .unwrap();

        assert_eq!(
            scalar(&conn.query("SELECT count(*) FROM t;").await.unwrap()),
            2,
            "bd-plxob(b): 'rebuild' must index the 2 source rows"
        );
        assert_eq!(
            scalar(
                &conn
                    .query("SELECT count(*) FROM t WHERE t MATCH 'hello';")
                    .await
                    .unwrap()
            ),
            1,
            "bd-plxob(b): 'rebuild' must make MATCH work against source content"
        );
        conn.close().await.unwrap();

        // Stock oracle: the frank-written image is stock-readable and consistent.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integ: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integ, "ok", "stock integrity_check after external-content rebuild");
    });
}
