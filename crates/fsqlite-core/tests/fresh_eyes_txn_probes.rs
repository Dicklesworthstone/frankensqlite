//! Fresh-eyes probes: (1) ROLLBACK of a lazy tombstone DELETE must restore the
//! in-memory row count; (2) frank's monotonically growing segids must stay
//! readable by stock (stock caps segids at FTS5_MAX_SEGMENT=2000 and reuses).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn scalar_i64(rows: &[fsqlite_core::connection::Row]) -> i64 {
    assert_eq!(rows.len(), 1, "expected exactly one row");
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer scalar, got {other:?}"),
    }
}

#[test]
fn fresh_eyes_lazy_delete_rollback_restores_count() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("rollback.db");
        let db_str = db_path.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=3 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }
        // Schema-only open binds lazily (the production cass path).
        let conn = Connection::open_existing_schema_only(&db_str).await.unwrap();
        let c0 = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(scalar_i64(&c0), 3, "reopened count");

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("DELETE FROM t WHERE rowid = 2;").await.unwrap();
        conn.execute("ROLLBACK;").await.unwrap();

        let c1 = conn.query("SELECT count(*) FROM t;").await.unwrap();
        let survivors = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            survivors.len(),
            3,
            "MATCH sees all 3 rows after rollback (disk rolled back)"
        );
        assert_eq!(
            scalar_i64(&c1),
            3,
            "count(*) after ROLLBACK must be 3 (in-memory lazy_doc_count restored)"
        );
        conn.close().await.unwrap();
    });
}

#[test]
fn fresh_eyes_segid_growth_stays_stock_readable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("segids.db");
        let db_str = db_path.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            // Each statement appends one segment (plus merges); max segid grows
            // monotonically past stock's FTS5_MAX_SEGMENT=2000.
            for id in 1..=2100_i64 {
                conn.execute(&format!("INSERT INTO t(rowid, body) VALUES ({id}, 'w{id}');"))
                    .await
                    .unwrap();
            }
            conn.close().await.unwrap();
        }
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check");
        let matched: i64 = stock
            .query_row(
                "SELECT count(*) FROM t WHERE t MATCH 'w2099'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(matched, 1, "stock can query the >2000-segid image");
        let fts_check: std::result::Result<usize, _> =
            stock.execute("INSERT INTO t(t) VALUES('integrity-check')", []);
        assert!(
            fts_check.is_ok(),
            "stock fts5 integrity-check accepts the image: {fts_check:?}"
        );
    });
}
