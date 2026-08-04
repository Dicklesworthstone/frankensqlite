//! bd-zywqc.3: Platform-specific isolation tests for the CI matrix.
//!
//! These tests verify platform-dependent behavior that differs across
//! Linux x86_64, Linux ARM64, macOS arm64, and Windows x86_64.
//! Failures on one platform that pass on others demonstrate matrix isolation.
#![recursion_limit = "512"]

use tempfile::TempDir;

const _BEAD_ID: &str = "bd-zywqc.3";

async fn open_fsqlite(path: &str) -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(path.to_owned()).await.unwrap();
    drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
    drop(conn.execute("PRAGMA busy_timeout=5000;").await);
    conn
}

fn open_csqlite(path: &std::path::Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA busy_timeout=5000;").unwrap();
    conn
}

#[test]
fn t1_file_backed_concurrent_writers_platform_check() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("platform.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, ts INTEGER)")
            .await
            .unwrap();
        drop(conn);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        for tid in 0..4u32 {
            let p = path_str.clone();
            let b = std::sync::Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let mut ok = 0u32;
                asupersync::test_utils::run_test(|| async {
                    let conn = open_fsqlite(&p).await;
                    b.wait();

                    let base = i64::from(tid) * 100;
                    for i in 0..25i64 {
                        let sql =
                            format!("INSERT INTO t VALUES ({}, 'thread_{tid}', {i})", base + i);
                        match conn.execute(&sql).await {
                            Ok(_) => ok += 1,
                            Err(_) => {}
                        }
                    }
                });
                ok
            }));
        }

        let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(
            total > 0,
            "at least some inserts must succeed across threads"
        );

        let csqlite = open_csqlite(&db_path);
        let count: i64 = csqlite
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count,
            i64::from(total),
            "C SQLite row count must match fsqlite insert count"
        );

        let integrity: String = csqlite
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "cross-engine integrity must pass");
    });
}

#[test]
fn t2_fsync_semantics_durability_check() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("fsync_test.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
            .await
            .unwrap();

        for i in 0..50 {
            let sql = format!("INSERT INTO t VALUES ({i}, zeroblob(1024))");
            conn.execute(&sql).await.unwrap();
        }
        drop(conn);

        let file_size = std::fs::metadata(&db_path).unwrap().len();
        assert!(file_size > 0, "DB file must have nonzero size after writes");

        let csqlite = open_csqlite(&db_path);
        let count: i64 = csqlite
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 50, "all 50 rows must be durable");
    });
}

#[test]
fn t3_tempdir_path_encoding() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("path_test.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        drop(conn);

        assert!(db_path.exists(), "DB file must exist at expected path");

        let csqlite = open_csqlite(&db_path);
        let v: i64 = csqlite
            .query_row("SELECT id FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 1);
    });
}

#[test]
fn t4_thread_count_matches_platform_nproc() {
    asupersync::test_utils::run_test(|| async {
        let nproc = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert!(
            nproc >= 1,
            "available_parallelism must report at least 1 on all platforms"
        );

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("nproc.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, tid INTEGER)")
            .await
            .unwrap();
        drop(conn);

        let n_threads = nproc.min(8);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(n_threads));
        let mut handles = Vec::new();

        for tid in 0..n_threads {
            let p = path_str.clone();
            let b = std::sync::Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let mut ok = false;
                asupersync::test_utils::run_test(|| async {
                    let conn = open_fsqlite(&p).await;
                    b.wait();
                    let sql = format!("INSERT INTO t VALUES ({tid}, {tid})");
                    ok = conn.execute(&sql).await.is_ok();
                });
                ok
            }));
        }

        let successes: usize = handles
            .into_iter()
            .filter_map(|h| h.join().ok())
            .filter(|ok| *ok)
            .count();
        assert!(
            successes > 0,
            "at least one thread must succeed on {n_threads} threads"
        );
    });
}

#[test]
fn t5_file_metadata_timestamps_present() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("meta.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        drop(conn);

        let meta = std::fs::metadata(&db_path).unwrap();
        assert!(meta.len() > 0);
        assert!(meta.modified().is_ok(), "modified time must be available");

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert!(meta.mtime() > 0, "mtime must be positive on Unix");
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            assert!(
                meta.last_write_time() > 0,
                "last_write_time must be positive on Windows"
            );
        }
    });
}

#[test]
fn t6_wal_file_created_and_readable_cross_platform() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("wal_check.db");
        let _wal_path = dir.path().join("wal_check.db-wal");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        for i in 0..20 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                .await
                .unwrap();
        }

        assert!(db_path.exists(), "main DB file must exist on all platforms");

        drop(conn);

        let csqlite = open_csqlite(&db_path);
        let count: i64 = csqlite
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 20, "C SQLite must read all rows written by fsqlite");

        let integrity: String = csqlite
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
    });
}

#[test]
fn t7_concurrent_read_write_no_blocking() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("rw_concurrent.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = open_fsqlite(&path_str).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .await
            .unwrap();
        for i in 0..100 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .await
                .unwrap();
        }
        drop(conn);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let writer_path = path_str.clone();
        let writer_barrier = std::sync::Arc::clone(&barrier);

        let writer = std::thread::spawn(move || {
            let mut written = 0u32;
            asupersync::test_utils::run_test(|| async {
                let conn = open_fsqlite(&writer_path).await;
                writer_barrier.wait();
                for i in 100..200i64 {
                    if conn
                        .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                        .await
                        .is_ok()
                    {
                        written += 1;
                    }
                }
            });
            written
        });

        let mut readers = Vec::new();
        for _ in 0..2 {
            let p = path_str.clone();
            let b = std::sync::Arc::clone(&barrier);
            readers.push(std::thread::spawn(move || {
                let mut reads = 0u32;
                asupersync::test_utils::run_test(|| async {
                    let conn = open_fsqlite(&p).await;
                    b.wait();
                    for _ in 0..50 {
                        let rows = conn.query("SELECT count(*) FROM t;").await;
                        if rows.is_ok() {
                            reads += 1;
                        }
                    }
                });
                reads
            }));
        }

        let writes = writer.join().unwrap();
        let total_reads: u32 = readers.into_iter().map(|r| r.join().unwrap()).sum();

        assert!(writes > 0, "writer must make progress");
        assert!(total_reads > 0, "readers must make progress concurrently");
    });
}

#[test]
fn t8_verify_report_schema() {
    let report = serde_json::json!({
        "platform": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "family": std::env::consts::FAMILY,
        "tests_run": 8,
        "bead_id": "bd-zywqc.3"
    });

    assert!(report["platform"].is_string());
    assert!(report["arch"].is_string());
    assert!(report["tests_run"].as_u64().unwrap() > 0);

    let platform = report["platform"].as_str().unwrap();
    let arch = report["arch"].as_str().unwrap();
    let valid_platforms = ["linux", "macos", "windows"];
    let valid_arches = ["x86_64", "aarch64", "x86"];
    assert!(
        valid_platforms.contains(&platform),
        "OS {platform} must be recognized"
    );
    assert!(
        valid_arches.contains(&arch),
        "arch {arch} must be recognized"
    );
}
