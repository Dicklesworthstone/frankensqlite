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
//! Both literal SQL and HFDT's exact bound-parameter call must produce a
//! self-contained, stock-readable image with every source value intact. Only
//! native Windows execution verifies the platform-specific ordering repair.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn bd_dd24l_vacuum_into_writes_reserved_target_gh376() {
    assert_vacuum_into_reserved_target(false);
}

#[test]
fn bd_dd24l_bound_vacuum_into_writes_reserved_target_gh376() {
    assert_vacuum_into_reserved_target(true);
}

fn assert_vacuum_into_reserved_target(bound_parameter: bool) {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("src.db");
        // A quote and a space must survive parameter binding verbatim.
        let target = dir.path().join("out ' snapshot.db");
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
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint source as in the reported HFDT call sequence");
            let result = if bound_parameter {
                conn.execute_with_params(
                    "VACUUM INTO ?1;",
                    &[SqliteValue::from(target_str.clone())],
                )
                .await
            } else {
                conn.execute(&format!(
                    "VACUUM INTO '{}';",
                    target_str.replace('\'', "''")
                ))
                .await
            };
            result.expect("VACUUM INTO must succeed and open its reserved target (GH#376)");
            conn.close().await.expect("close source");
        }

        // The exact regression symptom: an output file must exist on disk.
        assert!(
            target.exists(),
            "VACUUM INTO must produce an output file (GH#376: none was produced on Windows)",
        );

        // Check the image before a FrankenSQLite reopen can repair or alter it.
        {
            let stock = rusqlite::Connection::open_with_flags(
                &target,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .expect("stock SQLite must open the vacuumed image read-only");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .expect("stock integrity_check");
            assert_eq!(integrity, "ok", "vacuumed image must be structurally sound");
            let mut stmt = stock
                .prepare("SELECT x, v FROM t ORDER BY x;")
                .expect("prepare stock row query");
            let rows: Vec<(i64, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .expect("query stock rows")
                .collect::<rusqlite::Result<_>>()
                .expect("read stock rows");
            let expected: Vec<_> = (0..25).map(|i| (i, format!("row{i}"))).collect();
            assert_eq!(rows, expected, "stock reader must recover every source value");
        }

        // And FrankenSQLite must reopen the same output with identical values.
        {
            let out = Connection::open(&target_str)
                .await
                .expect("open the vacuumed target");
            let rows = out
                .query("SELECT x, v FROM t ORDER BY x;")
                .await
                .expect("query the vacuumed target");
            assert_eq!(rows.len(), 25, "vacuumed target must carry all source rows");
            for (i, row) in (0_i64..25).zip(&rows) {
                assert_eq!(
                    row.values(),
                    &[SqliteValue::Integer(i), SqliteValue::from(format!("row{i}"))],
                    "vacuumed target row {i}",
                );
            }
            out.close().await.expect("close target");
        }
    });
}
