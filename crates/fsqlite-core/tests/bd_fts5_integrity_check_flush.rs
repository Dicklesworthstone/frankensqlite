//! bd-fts5-lazy-shadow-reads-itcc4.3 special commands: `'integrity-check'` and
//! `'flush'`.
//!
//! `INSERT INTO t(t) VALUES('integrity-check')` validates the persisted index by
//! scanning every segment's leaves through the page-boundary reader (no hydration);
//! a readable index returns Ok. `'flush'` is a no-op (FrankenSQLite persists every
//! statement). A cross-engine case runs integrity-check over a stock-written index
//! whose doclist spills across leaf pages, exercising the GH#360 stitcher.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
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
fn bd_fts5_integrity_check_and_flush_accept_valid_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_integ.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            for id in 1..=5 {
                conn.execute(&format!(
                    "INSERT INTO t(rowid, body) VALUES ({id}, 'common term{id}');"
                ))
                .await
                .unwrap();
            }
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        // integrity-check on a valid index succeeds (no error).
        conn.execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect("integrity-check must pass on a valid index");
        // flush is accepted as a no-op.
        conn.execute("INSERT INTO t(t) VALUES('flush');")
            .await
            .expect("flush must be accepted");
        // Neither command disturbed the corpus.
        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), vec![1, 2, 3, 4, 5], "corpus intact");
        conn.close().await.unwrap();
    });
}

#[test]
fn bd_fts5_integrity_check_passes_on_stock_spilled_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_integ_spill.db");

        // Stock writes a corpus whose 'common' doclist spills across leaf pages
        // (pgsz=64, doc 1 with an 80-position poslist), optimized to one segment.
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE t USING fts5(x, content='', contentless_delete=1);\n\
                     INSERT INTO t(t, rank) VALUES('pgsz', 64);",
                )
                .unwrap();
            for id in 1..=40 {
                stock
                    .execute(
                        "INSERT INTO t(rowid, x) VALUES (?1, ?2)",
                        rusqlite::params![id, format!("common w{id}")],
                    )
                    .unwrap();
            }
            let big = vec!["common"; 80].join(" ");
            stock
                .execute(
                    "UPDATE t SET x = ?1 WHERE rowid = 1",
                    rusqlite::params![big],
                )
                .unwrap();
            stock
                .execute_batch("INSERT INTO t(t) VALUES('optimize');")
                .unwrap();
            let leaf_pages: i64 = stock
                .query_row("SELECT count(*) FROM t_data WHERE id > 100", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert!(leaf_pages > 1, "fixture must spill, got {leaf_pages}");
        }

        // integrity-check scans the stock-split index through the stitcher: Ok.
        let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
        conn.execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect("integrity-check must read the stock-spilled index without error");
        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), (1..=40).collect::<Vec<i64>>());
        conn.close().await.unwrap();

        // Keep the structure record valid so a schema-only open succeeds, but
        // corrupt one persisted leaf. The read-only maintenance command must
        // scan far enough to discover that planted negative itself.
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let leaf_id: i64 = stock
                .query_row("SELECT min(id) FROM t_data WHERE id > 100", [], |row| {
                    row.get(0)
                })
                .unwrap();
            stock
                .execute(
                    "UPDATE t_data SET block = X'FFFFFFFFFFFFFFFFFFFFFFFF' WHERE id = ?1",
                    [leaf_id],
                )
                .unwrap();
        }

        let conn = Connection::open_schema_only(db_path.to_str().unwrap())
            .await
            .expect("leaf corruption must not prevent the read-only schema open");
        let err = conn
            .execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect_err("the read-only integrity command must reject a corrupt leaf");
        assert!(
            !matches!(err, FrankenError::ReadOnly),
            "the semantic integrity result must not be hidden by the SQL write guard"
        );
        assert!(
            err.to_string().contains("malformed") || err.to_string().contains("corrupt"),
            "expected a corruption diagnostic, got {err:?}"
        );
        conn.close().await.unwrap();
    });
}

#[test]
fn bd_fts5_integrity_check_is_the_only_maintenance_insert_allowed_read_only() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_read_only_integrity.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(
                "CREATE VIRTUAL TABLE t USING fts5(body, content='', contentless_delete=1);",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'read only sentinel');")
                .await
                .unwrap();
            conn.close().await.unwrap();
        }

        let conn = Connection::open_schema_only(&db_str).await.unwrap();
        conn.execute("PRAGMA query_only = 1;")
            .await
            .expect("enable the application-level read-only guard");
        conn.execute("INSERT INTO t(t) VALUES('integrity-check');")
            .await
            .expect(
                "the FTS5 integrity decoder must run through a read-only pager with query_only enabled",
            );

        for command in ["flush", "optimize", "rebuild", "delete-all"] {
            let err = conn
                .execute(&format!("INSERT INTO t(t) VALUES('{command}');"))
                .await
                .expect_err("mutating FTS5 maintenance must stay read-only rejected");
            assert!(
                matches!(err, FrankenError::ReadOnly),
                "{command} must require a writable pager, got {err:?}"
            );
        }

        let err = conn
            .execute("INSERT INTO t(rowid, body) VALUES (2, 'forbidden write');")
            .await
            .expect_err("ordinary inserts must stay read-only rejected");
        assert!(matches!(err, FrankenError::ReadOnly));

        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'sentinel';")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), vec![1]);
        conn.close().await.unwrap();
    });
}
