//! bd-n7ypv / GH#342: VACUUM INTO must be permitted on a read-only source
//! connection — it writes a NEW target file and only reads the source, so
//! sqlite3 allows it (verified: `sqlite3 'file:src?mode=ro' "VACUUM INTO ..."`
//! succeeds while plain `VACUUM` errors SQLITE_READONLY). Previously the
//! read-only guard rejected all VACUUM with the wrong `ReadOnly` kind.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

// FIXED (bd-n7ypv/GH#342): sqlite3 permits VACUUM INTO on a read-only source
// connection; fsqlite now matches. The fix: (1) exempt VACUUM INTO (into.is_some)
// from the read-only statement guard; (2) release the read-only connection's
// retained non-mutating read snapshot from active_txn before
// quiesce_pager_export_state (lossless — a read-only pager holds no writes);
// (3) skip the source WAL checkpoint on a read-only source (exported rows come
// WAL-aware from reload_memdb_from_pager, and a read-only source cannot be
// written). Plain VACUUM stays ReadOnly (into.is_none()).
#[test]
fn bd_n7ypv_vacuum_into_allowed_on_read_only_connection_plain_vacuum_still_rejected() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("src.db").to_string_lossy().into_owned();
        let target = dir.path().join("out.db").to_string_lossy().into_owned();
        {
            let conn = Connection::open(&src).await.expect("create source");
            conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create table");
            for i in 0..50 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}');"))
                    .await
                    .expect("insert");
            }
            conn.close().await.expect("close source");
        }

        // VACUUM INTO on a FRESH read-only source connection (open_schema_only
        // opens a read-only pager: is_readonly() == true). It writes a NEW
        // target and only reads the source -> allowed.
        {
            let ro = Connection::open_schema_only(&src)
                .await
                .expect("open read-only source");
            ro.execute(&format!("VACUUM INTO '{}';", target.replace('\'', "''")))
                .await
                .expect("VACUUM INTO must be allowed on a read-only source (GH#342)");
            ro.close().await.expect("close read-only");
        }

        // Plain VACUUM rewrites the source in place -> must stay fail-closed
        // with ReadOnly (matches sqlite3 SQLITE_READONLY).
        {
            let ro = Connection::open_schema_only(&src)
                .await
                .expect("reopen read-only source");
            let err = ro
                .execute("VACUUM;")
                .await
                .expect_err("plain VACUUM on a read-only connection must fail closed");
            assert!(
                matches!(err, FrankenError::ReadOnly),
                "plain VACUUM should be ReadOnly, got {err:?}"
            );
            ro.close().await.expect("close read-only");
        }

        // The vacuumed target is a valid database holding the source's rows.
        let out = Connection::open(&target)
            .await
            .expect("open vacuumed target");
        let rows = out
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("count target rows");
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(50),
            "vacuumed target must hold all source rows"
        );
        out.close().await.expect("close target");
    });
}
