//! bd-cp3yd: with the VDBE JIT on, a hot full-scan SELECT of a TEMP table
//! must still read the TEMP table. The compiled templates open their cursor
//! by root page alone, so they must not take over a TEMP (OpenRead p3=1)
//! cursor. The JIT switch is process-global, so this lives in its own binary.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{set_vdbe_jit_enabled, set_vdbe_jit_hot_threshold};

#[test]
fn hot_temp_table_full_scan_stays_on_the_temp_table() {
    set_vdbe_jit_enabled(true);
    let _ = set_vdbe_jit_hot_threshold(1);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("cp3yd.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async {
        for target in [":memory:", path.as_str()] {
            let conn = Connection::open(target).await.expect("open");
            conn.execute_batch(
                "CREATE TEMP TABLE local (a INTEGER, b TEXT);
                 INSERT INTO local VALUES (1, 'x'), (2, 'y');",
            )
            .await
            .expect("setup");
            conn.set_reject_mem_fallback(true);
            for _ in 0..4 {
                let rows = conn.query("SELECT a, b FROM local").await.expect("scan");
                let got: Vec<Vec<SqliteValue>> =
                    rows.iter().map(|row| row.values().to_vec()).collect();
                assert_eq!(
                    got,
                    vec![
                        vec![SqliteValue::Integer(1), SqliteValue::Text("x".into())],
                        vec![SqliteValue::Integer(2), SqliteValue::Text("y".into())],
                    ],
                    "{target}"
                );
            }
            conn.set_reject_mem_fallback(false);
            conn.close().await.expect("close");
        }
    });
}
