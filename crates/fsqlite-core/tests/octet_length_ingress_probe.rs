//! bd-rwaxp: `octet_length` over a large overflow payload must read only the
//! record-header serial type — never materialize the overflow chain.
//!
//! This assertion reads the PROCESS-GLOBAL `btree_copy_profile` counters, which
//! any concurrent btree activity pollutes. In the shared `--lib` test binary
//! (thousands of tests in one process) the `== 0` checks flake spuriously — this
//! is why the test showed up as a bd-rwaxp `--lib` red even though the
//! implementation is correct (it passes cleanly single-threaded / in isolation).
//! Moving it to its own integration-test binary gives it a fresh process whose
//! global profile no other test can touch. Mirrors `tests/fast_path_separation.rs`.
//!
//! Run:
//!   cargo test -p fsqlite-core --test octet_length_ingress_probe -- --test-threads=1

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn octet_length_ingress_probe_does_not_materialize_overflow_payloads() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("octet-length-ingress.db");
        let conn = Connection::open(db_path.to_string_lossy().into_owned())
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE ingress (state_id BIGINT NOT NULL PRIMARY KEY, snapshot_json TEXT NOT NULL);",
        )
        .await
        .unwrap();

        let payload_bytes = 1024 * 1024;
        conn.execute("BEGIN EXCLUSIVE").await.unwrap();
        conn.execute_with_params(
            "INSERT INTO ingress VALUES (?1, ?2);",
            &[
                SqliteValue::Blob(std::sync::Arc::from(vec![0xA5; payload_bytes])),
                SqliteValue::Text("x".repeat(payload_bytes).into()),
            ],
        )
        .await
        .unwrap();
        conn.execute("COMMIT").await.unwrap();

        // Isolated process: no other test can perturb the global profile here.
        fsqlite_btree::reset_btree_copy_profile();
        fsqlite_btree::set_btree_copy_profile_enabled(true);
        let rows = conn
            .query(
                "SELECT octet_length(state_id) AS state_id_bytes, \
                        octet_length(snapshot_json) AS snapshot_bytes \
                 FROM ingress LIMIT 2;",
            )
            .await
            .unwrap();
        let profile = fsqlite_btree::btree_copy_profile_snapshot();
        fsqlite_btree::set_btree_copy_profile_enabled(false);

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values(),
            &[
                SqliteValue::Integer(i64::try_from(payload_bytes).unwrap()),
                SqliteValue::Integer(i64::try_from(payload_bytes).unwrap()),
            ]
        );
        assert_eq!(
            profile.owned_payload_materialization_bytes, 0,
            "record-header octet_length must not allocate either hostile payload: {profile:?}"
        );
        assert_eq!(
            profile.overflow_chain_overflow_bytes, 0,
            "record-header octet_length must not copy overflow bytes: {profile:?}"
        );
        assert_eq!(
            profile.overflow_page_reads, 0,
            "record-header octet_length must not read overflow pages: {profile:?}"
        );
    });
}
