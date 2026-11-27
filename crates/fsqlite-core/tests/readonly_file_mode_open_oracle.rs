#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.20: a database file the process cannot write
//! (mode 0444, another owner's file, a read-only mount) must open the way stock
//! SQLite opens it. Stock's `unixOpen` retries a failed read-write open
//! read-only, so reads work and writes fail with SQLITE_READONLY. fsqlite
//! 0.4.12 failed every such open with "I/O error: Permission denied".
//!
//! The oracle is bundled stock SQLite opening a byte-identical copy of each
//! image with its default read-write flags. Root bypasses file modes, so on
//! Linux the test first drops to the `nobody` ids; where it still runs as root
//! (other platforms) it compares outcomes, because stock then writes too.

#![cfg(all(unix, feature = "native"))]

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

const ROWS: i64 = 3;

/// Build one image shape in `dir`; return the main-file path.
fn build_image(dir: &Path, shape: &str) -> PathBuf {
    let path = dir.join("image.db");
    let seed = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); \
                INSERT INTO t(v) VALUES ('a'), ('b'), ('c');";
    match shape {
        "rollback" | "rollback_stray_readonly_shm" => {
            let stock = rusqlite::Connection::open(&path).expect("stock create");
            stock.execute_batch(seed).expect("seed");
            drop(stock);
            if shape == "rollback_stray_readonly_shm" {
                std::fs::write(dir.join("image.db-shm"), [0_u8; 32]).expect("write stray SHM");
            }
        }
        "wal_checkpointed" => {
            let stock = rusqlite::Connection::open(&path).expect("stock create");
            stock
                .execute_batch(&format!("PRAGMA journal_mode=WAL; {seed}"))
                .expect("seed");
        }
        "wal_live_frames" => {
            // Copy the files while the writer is still open, so the WAL keeps
            // every frame and there is no -shm: a reader must rebuild the
            // WAL index to see the rows.
            let source = dir.join("source.db");
            let stock = rusqlite::Connection::open(&source).expect("stock create");
            stock
                .execute_batch(&format!(
                    "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; {seed}"
                ))
                .expect("seed");
            std::fs::copy(&source, &path).expect("copy main");
            std::fs::copy(dir.join("source.db-wal"), dir.join("image.db-wal")).expect("copy WAL");
            drop(stock);
        }
        "wal_live_frames_readonly_shm"
        | "wal_live_frames_readonly_short_shm"
        | "wal_live_frames_readonly_zero_shm"
        | "wal_live_frames_readonly_stale_shm"
        | "wal_live_frames_readonly_shm_only"
        | "wal_live_frames_readonly_wal_only"
        | "wal_live_frames_readonly_dir_with_shm"
        | "wal_live_frames_readonly_dir_no_shm" => {
            // bd-reality-check-1022-llk6g.35: copy main, live -wal, and -shm
            // while the stock writer is still open so WAL frames stay
            // uncheckpointed. For the stale shape, snapshot -shm after only
            // 1 row before inserting the remaining rows so a naive reader that
            // trusts -shm without rebuilding returns 1 instead of 3.
            let source = dir.join("source.db");
            let stock = rusqlite::Connection::open(&source).expect("stock create");
            stock
                .execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; \
                     CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); \
                     INSERT INTO t(v) VALUES ('a');",
                )
                .expect("seed first row");
            let stale_shm = std::fs::read(dir.join("source.db-shm")).expect("read stale -shm");
            stock
                .execute_batch("INSERT INTO t(v) VALUES ('b'), ('c');")
                .expect("seed remaining rows");
            std::fs::copy(&source, &path).expect("copy main");
            std::fs::copy(dir.join("source.db-wal"), dir.join("image.db-wal")).expect("copy WAL");
            let image_shm = dir.join("image.db-shm");
            match shape {
                "wal_live_frames_readonly_shm"
                | "wal_live_frames_readonly_shm_only"
                | "wal_live_frames_readonly_wal_only"
                | "wal_live_frames_readonly_dir_with_shm" => {
                    std::fs::copy(dir.join("source.db-shm"), &image_shm).expect("copy SHM");
                }
                "wal_live_frames_readonly_short_shm" => {
                    std::fs::write(&image_shm, [0_u8; 3]).expect("write short SHM");
                }
                "wal_live_frames_readonly_zero_shm" => {
                    std::fs::write(&image_shm, vec![0_u8; 32 * 1024]).expect("write zero SHM");
                }
                "wal_live_frames_readonly_stale_shm" => {
                    std::fs::write(&image_shm, &stale_shm).expect("write stale SHM");
                }
                "wal_live_frames_readonly_dir_no_shm" => {}
                _ => unreachable!(),
            }
            drop(stock);
        }
        "wal_zero_byte" | "wal_shorter_than_header" => {
            // GH#515's shape: a WAL shorter than its header and no -shm, so
            // the main file is the whole database. The read-only WAL install
            // binds no WAL here, and the open then retried a writable install
            // whose read-write WAL open failed with "Permission denied".
            {
                let stock = rusqlite::Connection::open(&path).expect("stock create");
                stock
                    .execute_batch(&format!("PRAGMA journal_mode=WAL; {seed}"))
                    .expect("seed");
            }
            let wal: &[u8] = if shape == "wal_zero_byte" {
                &[]
            } else {
                &[0x37; 16]
            };
            std::fs::write(dir.join("image.db-wal"), wal).expect("write WAL");
        }
        other => panic!("unknown shape {other}"),
    }
    path
}

/// Copy the image's main file, WAL, and SHM into `dir` and apply the shape's
/// read-only file/directory modes.
fn place_read_only_copy(image: &Path, dir: &Path, shape: &str) -> PathBuf {
    std::fs::create_dir_all(dir).expect("create copy dir");
    let path = dir.join("t.db");
    std::fs::copy(image, &path).expect("copy main");
    let image_wal = PathBuf::from(format!("{}-wal", image.display()));
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    if image_wal.exists() {
        std::fs::copy(&image_wal, &wal).expect("copy WAL");
    }
    let image_shm = PathBuf::from(format!("{}-shm", image.display()));
    let shm = PathBuf::from(format!("{}-shm", path.display()));
    if image_shm.exists() {
        std::fs::copy(&image_shm, &shm).expect("copy SHM");
    }
    match shape {
        "wal_live_frames_readonly_shm_only" | "rollback_stray_readonly_shm" => {
            std::fs::set_permissions(&shm, std::fs::Permissions::from_mode(0o444))
                .expect("chmod 0444 shm");
        }
        "wal_live_frames_readonly_wal_only" => {
            std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o444))
                .expect("chmod 0444 wal");
        }
        _ => {
            for file in [&path, &wal, &shm] {
                if file.exists() {
                    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o444))
                        .expect("chmod 0444");
                }
            }
        }
    }
    if matches!(
        shape,
        "wal_live_frames_readonly_dir_with_shm" | "wal_live_frames_readonly_dir_no_shm"
    ) {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555))
            .expect("chmod 0555 dir");
    }
    path
}

fn shm_is_readonly_shape(shape: &str) -> bool {
    matches!(
        shape,
        "wal_live_frames_readonly_shm"
            | "wal_live_frames_readonly_short_shm"
            | "wal_live_frames_readonly_zero_shm"
            | "wal_live_frames_readonly_stale_shm"
            | "wal_live_frames_readonly_shm_only"
            | "wal_live_frames_readonly_dir_with_shm"
    )
}

fn restore_dir_permissions(dir: &Path) {
    if dir.exists() {
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755));
    }
}

fn read_bytes(path: &Path) -> Option<Vec<u8>> {
    std::fs::read(path).ok()
}

/// Root bypasses file modes: its open of a 0444 file succeeds read-write, so
/// the fallback under test never runs, and most build workers run tests as
/// root. On Linux, drop this process (its only test is this one) to the
/// `nobody` ids first. Returns whether the process is now unprivileged.
fn drop_root_privileges() -> bool {
    use nix::unistd::geteuid;
    #[cfg(target_os = "linux")]
    if geteuid().is_root() {
        use nix::unistd::{Gid, Uid, setgroups, setresgid, setresuid};
        let (uid, gid) = (Uid::from_raw(65_534), Gid::from_raw(65_534));
        setgroups(&[]).expect("drop supplementary groups");
        setresgid(gid, gid, gid).expect("drop root gid");
        setresuid(uid, uid, uid).expect("drop root uid");
        eprintln!("event=readonly_file_mode_open dropped_root_to_uid=65534");
    }
    !geteuid().is_root()
}

#[test]
fn unwritable_database_file_opens_read_only_like_stock() {
    let unprivileged = drop_root_privileges();
    asupersync::test_utils::run_test(|| async {
        // /tmp, not $TMPDIR: a root-owned TMPDIR is closed to `nobody`.
        let root = if cfg!(target_os = "linux") {
            tempfile::tempdir_in("/tmp")
        } else {
            tempfile::tempdir()
        }
        .expect("tempdir");
        for shape in [
            "rollback",
            "rollback_stray_readonly_shm",
            "wal_checkpointed",
            "wal_live_frames",
            "wal_live_frames_readonly_shm",
            "wal_live_frames_readonly_short_shm",
            "wal_live_frames_readonly_zero_shm",
            "wal_live_frames_readonly_stale_shm",
            "wal_live_frames_readonly_shm_only",
            "wal_live_frames_readonly_wal_only",
            "wal_live_frames_readonly_dir_with_shm",
            "wal_zero_byte",
            "wal_shorter_than_header",
        ] {
            let image_dir = root.path().join(format!("{shape}-image"));
            std::fs::create_dir_all(&image_dir).expect("image dir");
            let image = build_image(&image_dir, shape);
            let expect_readonly_when_unprivileged = shape != "rollback_stray_readonly_shm";

            for opener in ["open", "open_existing"] {
                let case = format!("{shape}/{opener}");

                let stock_dir = root.path().join(format!("{shape}-{opener}-stock"));
                let stock_path = place_read_only_copy(&image, &stock_dir, shape);
                let stock_wal = PathBuf::from(format!("{}-wal", stock_path.display()));
                let stock_shm = PathBuf::from(format!("{}-shm", stock_path.display()));
                let stock = rusqlite::Connection::open(&stock_path)
                    .unwrap_or_else(|error| panic!("{case}: stock open failed: {error}"));
                let stock_rows: i64 = stock
                    .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                    .unwrap_or_else(|error| panic!("{case}: stock read failed: {error}"));
                let stock_write = stock.execute("INSERT INTO t(v) VALUES ('d')", []);
                drop(stock);
                let stock_wal_after = read_bytes(&stock_wal);
                let stock_shm_after = read_bytes(&stock_shm);
                restore_dir_permissions(&stock_dir);
                assert_eq!(stock_rows, ROWS, "{case}: stock sees the committed rows");
                if expect_readonly_when_unprivileged {
                    assert!(
                        !unprivileged || stock_write.is_err(),
                        "{case}: an unprivileged stock write to a read-only shape must fail, \
                         or this test exercises nothing: {stock_write:?}"
                    );
                } else {
                    assert!(
                        stock_write.is_ok(),
                        "{case}: stock write on writable rollback DB with stray 0444 -shm must succeed: {stock_write:?}"
                    );
                }

                let frank_dir = root.path().join(format!("{shape}-{opener}-frank"));
                let frank_path = place_read_only_copy(&image, &frank_dir, shape);
                let frank_wal = PathBuf::from(format!("{}-wal", frank_path.display()));
                let frank_shm = PathBuf::from(format!("{}-shm", frank_path.display()));
                let main_before = read_bytes(&frank_path);
                let wal_before = read_bytes(&frank_wal);
                let shm_before = read_bytes(&frank_shm);
                let path_text = frank_path.to_str().expect("utf8 path").to_owned();
                let opened = if opener == "open" {
                    Connection::open(path_text).await
                } else {
                    Connection::open_existing(path_text).await
                };
                let conn = match opened {
                    Ok(conn) => conn,
                    Err(error) => {
                        restore_dir_permissions(&frank_dir);
                        panic!("{case}: fsqlite open failed where stock opened: {error}");
                    }
                };
                let rows = conn.query_row("SELECT count(*) FROM t").await;
                let write = conn.execute("INSERT INTO t(v) VALUES ('d')").await;
                let close_res = conn.close().await;
                restore_dir_permissions(&frank_dir);
                let rows =
                    rows.unwrap_or_else(|error| panic!("{case}: fsqlite read failed: {error}"));
                assert_eq!(
                    rows.values(),
                    &[SqliteValue::Integer(stock_rows)],
                    "{case}: row count matches stock"
                );
                eprintln!(
                    "event=readonly_file_mode_open case={case} stock_write_ok={} frank_write={write:?}",
                    stock_write.is_ok()
                );
                match (&stock_write, &write) {
                    (Ok(_), Ok(_)) => {}
                    (Err(stock_error), Err(error)) => {
                        assert!(
                            stock_error.to_string().contains("readonly"),
                            "{case}: stock refusal is SQLITE_READONLY: {stock_error}"
                        );
                        assert!(
                            matches!(error, FrankenError::ReadOnly),
                            "{case}: fsqlite refuses the write as read-only, got {error}"
                        );
                    }
                    _ => panic!(
                        "{case}: write outcome differs: stock {stock_write:?}, fsqlite {write:?}"
                    ),
                }
                close_res.unwrap_or_else(|error| panic!("{case}: close failed: {error}"));
                if stock_write.is_err() {
                    assert_eq!(
                        read_bytes(&frank_path),
                        main_before,
                        "{case}: main bytes unchanged"
                    );
                    if wal_before.is_some() {
                        assert_eq!(
                            read_bytes(&frank_wal),
                            wal_before,
                            "{case}: existing WAL bytes unchanged"
                        );
                    }
                    if shm_is_readonly_shape(shape) {
                        assert_eq!(
                            read_bytes(&frank_shm),
                            shm_before,
                            "{case}: existing read-only SHM bytes unchanged"
                        );
                        assert_eq!(
                            read_bytes(&frank_shm),
                            stock_shm_after,
                            "{case}: read-only SHM sidecar matches stock"
                        );
                    }
                    // bd-cf0be: on a cleanly closed WAL database (`wal_checkpointed`)
                    // in a writable directory, stock's read-only first attacher
                    // creates a 0-byte `-wal` (and `-shm`), so compare against
                    // live stock's resulting WAL bytes across every shape.
                    assert_eq!(
                        read_bytes(&frank_wal),
                        stock_wal_after,
                        "{case}: WAL sidecar matches stock"
                    );
                }
            }
        }

        // Control: in a 0555 directory with live WAL frames and NO `-shm`,
        // stock SQLite refuses to read the WAL (`SQLITE_CANTOPEN`) because it
        // cannot open or create `-shm` to coordinate locks. Verify fsqlite
        // refuses cleanly without modifying main or `-wal`.
        if unprivileged {
            let shape = "wal_live_frames_readonly_dir_no_shm";
            let image_dir = root.path().join(format!("{shape}-image"));
            std::fs::create_dir_all(&image_dir).expect("image dir");
            let image = build_image(&image_dir, shape);
            let stock_dir = root.path().join(format!("{shape}-stock"));
            let stock_path = place_read_only_copy(&image, &stock_dir, shape);
            let stock = rusqlite::Connection::open(&stock_path).expect("stock lazy open");
            let stock_read = stock.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0));
            drop(stock);
            restore_dir_permissions(&stock_dir);
            assert!(
                stock_read.is_err(),
                "{shape}: stock must refuse live WAL in 0555 dir with no -shm, got {stock_read:?}"
            );

            for opener in ["open", "open_existing"] {
                let frank_dir = root.path().join(format!("{shape}-{opener}-frank"));
                let frank_path = place_read_only_copy(&image, &frank_dir, shape);
                let frank_wal = PathBuf::from(format!("{}-wal", frank_path.display()));
                let main_before = read_bytes(&frank_path);
                let wal_before = read_bytes(&frank_wal);
                let path_text = frank_path.to_str().expect("utf8 path").to_owned();
                let outcome = match if opener == "open" {
                    Connection::open(path_text).await
                } else {
                    Connection::open_existing(path_text).await
                } {
                    Ok(conn) => {
                        let read = conn.query_row("SELECT count(*) FROM t").await;
                        let _ = conn.close().await;
                        read.map(|_| ())
                    }
                    Err(err) => Err(err),
                };
                restore_dir_permissions(&frank_dir);
                assert!(
                    outcome.is_err(),
                    "{shape}/{opener}: fsqlite must refuse live WAL in 0555 dir with no -shm like stock, got {outcome:?}"
                );
                assert_eq!(read_bytes(&frank_path), main_before, "{shape}/{opener}: main unchanged");
                assert_eq!(read_bytes(&frank_wal), wal_before, "{shape}/{opener}: wal unchanged");
            }
        }
    });
}
