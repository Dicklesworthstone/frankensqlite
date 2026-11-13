#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.29: stock's unix VFS creates `-wal`, `-journal`
//! and `-shm` with the permission bits of the database they belong to
//! (`findCreateFileMode` + `robust_open`, which re-applies the bits the umask
//! removed). fsqlite 0.4.12 created them with the ambient umask, so a 0600
//! database got a group/world-readable 0664 or 0644 WAL holding its pages.
//!
//! The oracle is bundled stock SQLite writing a copy of the same database at
//! the same mode. The modes are chosen so that every common umask (002, 022,
//! 077) yields a different default for at least one of them.

#![cfg(all(unix, feature = "native"))]

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;

const MODES: [u32; 4] = [0o600, 0o640, 0o660, 0o604];

/// Sidecars stock names, plus fsqlite's WAL certificate pair, which share the
/// WAL's confidentiality.
const SIDECARS: [&str; 5] = ["-wal", "-shm", "-journal", "-wal-cert", "-wal-cert-head"];

fn companion(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
}

fn mode_of(path: &Path) -> Option<u32> {
    std::fs::metadata(path)
        .ok()
        .map(|metadata| metadata.permissions().mode() & 0o777)
}

/// A WAL database with one table, closed cleanly (no sidecars left), at `mode`.
fn seed_wal_database(path: &Path, mode: u32) {
    let stock = rusqlite::Connection::open(path).expect("stock create");
    stock
        .execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t(x);")
        .expect("seed");
    drop(stock);
    for suffix in SIDECARS {
        let _ = std::fs::remove_file(companion(path, suffix));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

fn sidecar_modes(path: &Path) -> Vec<(&'static str, u32)> {
    SIDECARS
        .iter()
        .filter_map(|suffix| mode_of(&companion(path, suffix)).map(|mode| (*suffix, mode)))
        .collect()
}

#[test]
fn wal_and_shm_take_the_database_mode_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let root = tempfile::tempdir().expect("tempdir");
        for mode in MODES {
            let case = format!("mode={mode:o}");

            let stock_path = root.path().join(format!("stock-{mode:o}.db"));
            seed_wal_database(&stock_path, mode);
            let stock = rusqlite::Connection::open(&stock_path).expect("stock reopen");
            stock
                .execute("INSERT INTO t VALUES (1)", [])
                .expect("stock insert");
            let stock_modes = sidecar_modes(&stock_path);
            drop(stock);
            assert!(
                stock_modes.iter().any(|(suffix, _)| *suffix == "-wal"),
                "{case}: stock left a live WAL while open: {stock_modes:?}"
            );
            for (suffix, sidecar_mode) in &stock_modes {
                assert_eq!(
                    *sidecar_mode, mode,
                    "{case}: oracle check: stock {suffix} has the database mode"
                );
            }

            let frank_path = root.path().join(format!("frank-{mode:o}.db"));
            seed_wal_database(&frank_path, mode);
            let conn = Connection::open(frank_path.to_str().expect("utf8 path").to_owned())
                .await
                .unwrap_or_else(|error| panic!("{case}: fsqlite open failed: {error}"));
            conn.execute("INSERT INTO t VALUES (1)")
                .await
                .unwrap_or_else(|error| panic!("{case}: fsqlite insert failed: {error}"));
            let frank_modes = sidecar_modes(&frank_path);
            eprintln!(
                "event=sidecar_file_mode case={case} stock={:?} frank={:?}",
                stock_modes
                    .iter()
                    .map(|(suffix, mode)| format!("{suffix}:{mode:o}"))
                    .collect::<Vec<_>>(),
                frank_modes
                    .iter()
                    .map(|(suffix, mode)| format!("{suffix}:{mode:o}"))
                    .collect::<Vec<_>>(),
            );
            assert!(
                frank_modes.iter().any(|(suffix, _)| *suffix == "-wal"),
                "{case}: fsqlite left a live WAL while open: {frank_modes:?}"
            );
            for (suffix, sidecar_mode) in &frank_modes {
                assert_eq!(
                    format!("{sidecar_mode:o}"),
                    format!("{mode:o}"),
                    "{case}: fsqlite {suffix} must have the database mode, as stock's does"
                );
            }
            assert_eq!(
                mode_of(&frank_path),
                Some(mode),
                "{case}: the database file keeps its mode"
            );
            drop(conn);
        }
    });
}
