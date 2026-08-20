//! bd-9hp58 ASK#2 — pool-drop / close must be lsof-clean: a connection's
//! namespace-binding advisory-lock sidecar fds (`-fsqlite-ns-use` /
//! `-fsqlite-ns-gate`) must not survive the connection's teardown, even on an
//! unawaited `drop` where a lingering pager/background `Arc` clone would keep
//! the binding alive past its last-`Arc` `Drop`. Verified against the real
//! `/proc/self/fd` table (Linux only).

#![cfg(all(feature = "native", target_os = "linux"))]

use std::path::Path;

use fsqlite_core::connection::Connection;

/// Namespace-sidecar fds currently open by THIS process that live under `dir`.
/// Scoping to the test's unique tempdir isolates it from any sibling test's
/// still-live connections in the same test binary.
fn open_ns_sidecar_fds(dir: &Path) -> Vec<String> {
    let dir = dir.to_string_lossy().into_owned();
    let mut hits = Vec::new();
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        for entry in entries.flatten() {
            if let Ok(target) = std::fs::read_link(entry.path()) {
                let s = target.to_string_lossy();
                if s.starts_with(&dir)
                    && (s.contains("-fsqlite-ns-use") || s.contains("-fsqlite-ns-gate"))
                {
                    hits.push(s.into_owned());
                }
            }
        }
    }
    hits
}

#[test]
fn namespace_sidecar_fds_released_on_unawaited_drop_bd_9hp58() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pool.db");
        let db_str = db.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute("CREATE TABLE t(x);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            assert!(
                !open_ns_sidecar_fds(dir.path()).is_empty(),
                "a live file-backed connection must retain its -ns sidecar fds"
            );
            // Unawaited DROP — the bd-2pmu6 pool-drop scenario. The Connection
            // Drop impl must shed the binding's sidecar fds synchronously.
            drop(conn);
        }
        let leaked = open_ns_sidecar_fds(dir.path());
        assert!(
            leaked.is_empty(),
            "dropped connection leaked namespace sidecar fds (bd-9hp58): {leaked:?}"
        );
    });
}

#[test]
fn namespace_sidecar_fds_released_on_awaited_close_bd_9hp58() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pool.db");
        let db_str = db.to_string_lossy().into_owned();
        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE TABLE t(x);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
        assert!(
            !open_ns_sidecar_fds(dir.path()).is_empty(),
            "a live file-backed connection must retain its -ns sidecar fds"
        );
        conn.close().await.unwrap();
        let leaked = open_ns_sidecar_fds(dir.path());
        assert!(
            leaked.is_empty(),
            "closed connection leaked namespace sidecar fds (bd-9hp58): {leaked:?}"
        );
    });
}

/// A sibling connection to the SAME path must keep its own sidecar fds after a
/// peer closes — quiesce is per-connection and must never release a sibling's
/// locks.
#[test]
fn sibling_connection_retains_its_own_ns_fds_after_peer_close_bd_9hp58() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("pool.db");
        let db_str = db.to_string_lossy().into_owned();
        let first = Connection::open(&db_str).await.unwrap();
        first.execute("CREATE TABLE t(x);").await.unwrap();
        let second = Connection::open(&db_str).await.unwrap();
        second.execute("INSERT INTO t VALUES (1);").await.unwrap();
        // Close the first; the second is still live and must retain fds.
        first.close().await.unwrap();
        assert!(
            !open_ns_sidecar_fds(dir.path()).is_empty(),
            "a live sibling must keep its own -ns fds after a peer closes"
        );
        second.close().await.unwrap();
        let leaked = open_ns_sidecar_fds(dir.path());
        assert!(
            leaked.is_empty(),
            "both connections closed but -ns fds leaked (bd-9hp58): {leaked:?}"
        );
    });
}
