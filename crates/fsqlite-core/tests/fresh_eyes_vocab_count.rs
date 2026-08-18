//! Fresh-eyes probe: `SELECT count(*) FROM <fts5vocab>` must not short-circuit
//! through the generic live-vtab cursor (whose fts5vocab stub is always EOF).

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
fn fresh_eyes_fts5vocab_count_star_matches_full_scan() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'alpha beta'), (2, 'beta gamma');")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE v USING fts5vocab('t', 'row');")
            .await
            .unwrap();

        let full = conn.query("SELECT term FROM v;").await.unwrap();
        assert_eq!(full.len(), 3, "vocab full scan: alpha, beta, gamma");

        let count = conn.query("SELECT count(*) FROM v;").await.unwrap();
        assert_eq!(
            scalar_i64(&count),
            3,
            "count(*) must agree with the full scan (generic cursor stub is empty)"
        );
    });
}

// Probe: which persist branch does INSERT-after-delete-all take on a lazy table?
#[test]
fn fresh_eyes_insert_after_delete_all_probe() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("probe.db");
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
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();
        // After delete-all the table must be lazy with 0 rows.
        let c0 = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(scalar_i64(&c0), 0, "post delete-all count");
        // The re-insert (this is where a lazy full-encode would trip the
        // encode_data_rows debug_assert if that fallback is taken).
        conn.execute("INSERT INTO t(rowid, body) VALUES (100, 'fresh start');")
            .await
            .unwrap();
        let c1 = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(scalar_i64(&c1), 1, "post re-insert count");
        conn.close().await.unwrap();
    });
}

// Probe: same delete-all + re-insert, but inside one explicit transaction so no
// schema reload can reset the lazy flag between the statements.
#[test]
fn fresh_eyes_insert_after_delete_all_in_txn_probe() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("probe_txn.db");
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
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (100, 'fresh start');")
            .await
            .unwrap();
        conn.execute("COMMIT;").await.unwrap();
        let c1 = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(scalar_i64(&c1), 1, "post re-insert count in txn");
        let fresh = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'fresh';")
            .await
            .unwrap();
        assert_eq!(fresh.len(), 1, "fresh row matchable");
        conn.close().await.unwrap();
    });
}
