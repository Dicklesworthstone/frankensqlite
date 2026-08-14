//! bd-2pmu6 ask #1b: read-only surfaces must be file-system-inert.
//!
//! Downstream (mcp_agent_mail_rust) forensic/read-neutral flows require that
//! opening a database read-only against a CLEAN family (no sidecars) creates
//! no files at all — no `-wal`, no `-shm`, no `-fsqlite-ns-gate`, no
//! `-fsqlite-ns-use` — and mutates no existing byte. a410c2735 delivered this
//! for the RO namespace-admission path; the residual repro was sidecar
//! creation via the schema-only open surface. This keeper pins BOTH surfaces
//! with a directory-level snapshot (file set + digests).

use fsqlite::Connection;
use fsqlite::compat::{OpenFlags, open_with_flags};
use std::collections::BTreeMap;

fn dir_snapshot(dir: &std::path::Path) -> BTreeMap<String, (u64, u64)> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        let data = std::fs::read(entry.path()).unwrap_or_default();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in &data {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        out.insert(name, (data.len() as u64, hash));
    }
    out
}

#[test]
fn readonly_surfaces_create_no_files_and_mutate_no_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pmu6_clean.db");
    let db = db_path.to_string_lossy().into_owned();

    // Build a settled family: write, then CLEAN close (checkpoint allowed —
    // the writer owns its close-time behavior).
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open writer");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;\n             CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT);\n             INSERT INTO t VALUES (1, 'settled');",
        )
        .await
        .expect("setup");
        conn.close().await.expect("clean close");
    });

    let baseline = dir_snapshot(temp_dir.path());
    assert!(
        baseline.keys().any(|name| name == "pmu6_clean.db"),
        "main file must exist in the baseline: {baseline:?}"
    );

    // Surface 1: flags-based read-only open + SELECT + close.
    asupersync::test_utils::run_test(|| async {
        let readonly = open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .expect("read-only open");
        let rows = readonly
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("read-only select");
        assert_eq!(rows.len(), 1);
        drop(readonly);
    });
    let after_ro = dir_snapshot(temp_dir.path());
    assert_eq!(
        baseline, after_ro,
        "flags-based read-only open must create no files and mutate no bytes \
         (bd-2pmu6 ask #1b)"
    );

    // Surface 2: schema-only open + close.
    asupersync::test_utils::run_test(|| async {
        let schema_only = Connection::open_schema_only(db.clone())
            .await
            .expect("schema-only open");
        drop(schema_only);
    });
    let after_schema = dir_snapshot(temp_dir.path());
    assert_eq!(
        baseline, after_schema,
        "schema-only open must create no files and mutate no bytes \
         (bd-2pmu6 ask #1b residual)"
    );
}
