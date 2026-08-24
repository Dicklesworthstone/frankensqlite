//! bd-dd24l / GH#376: `VACUUM INTO` must produce an output file.
//!
//! Regression: on Windows, fsqlite 0.3.4 → 0.3.7 broke `VACUUM INTO` — it
//! failed with `CannotOpen` on the reserved-empty target and left **no output
//! file at all**. Root cause (d842de357 / bd-mnane): the reserved-bootstrap
//! `PreOpenLockSidecars::snapshot` witness was captured AFTER the VFS open that
//! creates our own `-lock-shared/-reserved/-pending` advisory sidecars, so the
//! `AllowExpected` validation mistook our OWN just-created sidecars for a
//! foreign reservation and rejected the target before page 1 was written.
//!
//! Fix: snapshot the witness BEFORE the VFS open (pager
//! `open_readwrite_with_cx_and_page_buffer_max`), so it captures only genuinely
//! pre-existing (foreign/stale) sidecars while permitting the ones our own open
//! creates. bd-mnane's protection (reject a foreign `-lock-reserved`) is
//! preserved.
//!
//! This test is platform-agnostic: it passes trivially on Linux (no advisory
//! sidecars exist there) and is the load-bearing guard on a Windows runner,
//! where the pre-fix code produced no target file.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn bd_dd24l_vacuum_into_writes_reserved_target_gh376() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("src.db");
        let target = dir.path().join("out.db");
        let src_str = src.to_string_lossy().into_owned();
        let target_str = target.to_string_lossy().into_owned();

        // Build a source database with a few rows.
        {
            let conn = Connection::open(&src_str).await.expect("create source");
            conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create table");
            for i in 0..25 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row{i}');"))
                    .await
                    .expect("insert");
            }
            conn.close().await.expect("close source");
        }

        // The target must not exist yet — VACUUM INTO reserves a fresh 0-byte
        // slot and opens it via the reserved-builder path that regressed.
        assert!(
            !target.exists(),
            "target must not pre-exist so VACUUM INTO takes the reserved-empty path",
        );

        {
            let conn = Connection::open(&src_str).await.expect("reopen source");
            conn.execute(&format!("VACUUM INTO '{}';", target_str.replace('\'', "''")))
                .await
                .expect("VACUUM INTO must succeed and open its reserved target (GH#376)");
            conn.close().await.expect("close source");
        }

        // The exact regression symptom: an output file must exist on disk.
        assert!(
            target.exists(),
            "VACUUM INTO must produce an output file (GH#376: none was produced on Windows)",
        );

        // And it must be a usable copy carrying the source rows.
        {
            let out = Connection::open(&target_str)
                .await
                .expect("open the vacuumed target");
            let rows = out
                .query("SELECT count(*) FROM t;")
                .await
                .expect("query the vacuumed target");
            let count = match rows[0].values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("expected integer count, got {other:?}"),
            };
            assert_eq!(count, 25, "vacuumed target must carry all source rows");
            out.close().await.expect("close target");
        }
    });
}
