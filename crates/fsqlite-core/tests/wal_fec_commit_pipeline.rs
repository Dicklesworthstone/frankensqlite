//! Public SQL coverage for durable, caller-runtime-owned WAL-FEC generation.
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use asupersync::runtime::yield_now;
use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use fsqlite_wal::{WalHeader, scan_wal_fec, wal_fec_path_for_wal};

fn run_with_repair_pool<F: std::future::Future<Output = ()>>(test: F) {
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .blocking_threads(1, 1).build().unwrap();
    runtime.block_on(test);
}

async fn open(db: &Path) -> Connection {
    let conn = Connection::open(db.to_str().unwrap()).await.unwrap();
    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
    conn.execute("PRAGMA synchronous = FULL;").await.unwrap();
    conn.execute("PRAGMA wal_autocheckpoint = 0;").await.unwrap();
    assert!(conn.is_concurrent_mode_default());
    conn
}

fn wal_path(db: &Path) -> PathBuf {
    let mut path = db.as_os_str().to_os_string();
    path.push("-wal");
    PathBuf::from(path)
}

async fn hold_sidecar_guard(sidecar: &Path) -> File {
    let mut path = sidecar.as_os_str().to_os_string();
    path.push(".lock");
    let file = OpenOptions::new().create(true).truncate(false).read(true).write(true)
        .open(Path::new(&path)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) => {
                assert!(Instant::now() < deadline, "sidecar guard remained busy");
                yield_now().await;
            }
            Err(error) => panic!("sidecar guard failed: {error}"),
        }
    }
    file
}

async fn wait_for_last_group(db: &Path) {
    let wal = wal_path(db);
    let bytes = fs::read(&wal).unwrap();
    let header = WalHeader::from_bytes(&bytes).unwrap();
    let last = u32::try_from((bytes.len() - 32) / (24 + header.page_size as usize)).unwrap();
    let sidecar = wal_fec_path_for_wal(&wal);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if scan_wal_fec(&sidecar).unwrap().groups.iter().any(|group| {
            group.meta.end_frame_no == last
                && group.meta.wal_salt1 == header.salts.salt1
                && group.meta.wal_salt2 == header.salts.salt2
        }) {
            return;
        }
        assert!(Instant::now() < deadline, "background repair did not reach durable frame {last}");
        yield_now().await;
    }
}

#[test]
fn real_commit_returns_while_sidecar_is_busy_then_close_drains() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("durable.db");
        let conn = open(&db).await;
        assert_eq!(conn.query("PRAGMA raptorq_repair_symbols;").await.unwrap()[0].values(), &[SqliteValue::Integer(2)]);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB);").await.unwrap();
        wait_for_last_group(&db).await;
        let sidecar = wal_fec_path_for_wal(&wal_path(&db));
        let before = fs::read(&sidecar).unwrap();
        let guard = hold_sidecar_guard(&sidecar).await;
        conn.execute("BEGIN;").await.unwrap();
        for id in 1..=5 {
            conn.execute(&format!("INSERT INTO t VALUES ({id}, zeroblob(3000));")).await.unwrap();
        }
        conn.execute("COMMIT;").await.expect("primary COMMIT cannot wait for sidecar ownership");
        assert_eq!(fs::read(&sidecar).unwrap(), before);
        assert_eq!(conn.query("SELECT COUNT(*) FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(5)]);
        drop(guard);
        conn.close_without_checkpoint().await.expect("await worker drain");
        let scan = scan_wal_fec(&sidecar).unwrap();
        assert!(!scan.truncated_tail);
        assert!(scan.groups.iter().any(|group| group.meta.k_source >= 5));
        let after_close = fs::read(&sidecar).unwrap();
        for _ in 0..20 { yield_now().await; }
        assert_eq!(fs::read(&sidecar).unwrap(), after_close, "no worker writes after awaited close");
        let stock = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(stock.query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 5);
    });
}

#[test]
fn checkpoint_rejects_late_generation_and_next_commit_is_repairable() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("retired.db");
        let conn = open(&db).await;
        conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        wait_for_last_group(&db).await;
        let wal = wal_path(&db);
        let sidecar = wal_fec_path_for_wal(&wal);
        let retired = WalHeader::from_bytes(&fs::read(&wal).unwrap()).unwrap().salts;
        let guard = hold_sidecar_guard(&sidecar).await;
        conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);").await.unwrap();
        drop(guard);
        conn.query("PRAGMA wal_checkpoint(PASSIVE);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
        conn.close_without_checkpoint().await.unwrap();
        let scan = scan_wal_fec(&sidecar).unwrap();
        assert!(!scan.groups.is_empty());
        assert!(scan.groups.iter().all(|group| {
            (group.meta.wal_salt1, group.meta.wal_salt2) != (retired.salt1, retired.salt2)
        }), "retired worker must not resurrect a reclaimed generation");
    });
}

#[test]
fn repair_budget_zero_and_changes_apply_to_subsequent_groups() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("budget.db");
        let conn = open(&db).await;
        conn.execute("PRAGMA raptorq_repair_symbols = 0;").await.unwrap();
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
        let sidecar = wal_fec_path_for_wal(&wal_path(&db));
        assert!(scan_wal_fec(&sidecar).unwrap().groups.is_empty());
        conn.execute("PRAGMA raptorq_repair_symbols = 3;").await.unwrap();
        conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
        wait_for_last_group(&db).await;
        assert!(scan_wal_fec(&sidecar).unwrap().groups.iter().all(|group| group.meta.r_repair == 3));
        conn.close_without_checkpoint().await.unwrap();
        let reopened = open(&db).await;
        assert_eq!(reopened.query("PRAGMA raptorq_repair_symbols;").await.unwrap()[0].values(), &[SqliteValue::Integer(3)]);
        reopened.close_without_checkpoint().await.unwrap();
    });
}

#[test]
fn two_connections_keep_each_commit_repair_budget() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        for first_writer_is_a in [true, false] {
            let db = dir.path().join(format!("paired-budgets-{first_writer_is_a}.db"));
            let a = open(&db).await;
            a.execute("PRAGMA raptorq_repair_symbols = 3;").await.unwrap();
            a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY);").await.unwrap();
            wait_for_last_group(&db).await;
            let b = open(&db).await;
            b.execute("PRAGMA raptorq_repair_symbols = 5;").await.unwrap();
            assert_eq!(a.query("PRAGMA raptorq_repair_symbols;").await.unwrap()[0].values(), &[SqliteValue::Integer(3)]);
            assert_eq!(b.query("PRAGMA raptorq_repair_symbols;").await.unwrap()[0].values(), &[SqliteValue::Integer(5)]);

            let wal = wal_path(&db);
            let header = WalHeader::from_bytes(&fs::read(&wal).unwrap()).unwrap();
            let sidecar = wal_fec_path_for_wal(&wal);
            let guard = hold_sidecar_guard(&sidecar).await;
            let before = fs::read(&sidecar).unwrap();
            let mut expected = Vec::new();
            for id in 0..16 {
                let use_a = (id % 2 == 0) == first_writer_is_a;
                let (writer, budget) = if use_a { (&a, 3) } else { (&b, 5) };
                writer.execute(&format!("INSERT INTO t VALUES ({id});")).await.unwrap();
                let end_frame = u32::try_from(
                    (fs::metadata(&wal).unwrap().len() - 32) / (24 + u64::from(header.page_size)),
                ).unwrap();
                expected.push((end_frame, budget));
            }
            assert_eq!(fs::read(&sidecar).unwrap(), before, "durable commits must not wait for sidecar ownership");
            drop(guard);
            a.close_without_checkpoint().await.unwrap();
            b.close_without_checkpoint().await.unwrap();

            let scan = scan_wal_fec(&sidecar).unwrap();
            assert!(!scan.truncated_tail);
            assert!(scan.groups.windows(2).all(|groups| groups[0].meta.end_frame_no < groups[1].meta.start_frame_no));
            for (end_frame, budget) in expected {
                let matching: Vec<_> = scan.groups.iter().filter(|group| group.meta.end_frame_no == end_frame).collect();
                assert_eq!(matching.len(), 1, "commit ending at frame {end_frame} must have exactly one repair group");
                assert_eq!(matching[0].meta.r_repair, budget, "first_writer_is_a={first_writer_is_a}, frame={end_frame}: another connection must not replace the committing connection's repair budget");
                assert_eq!(matching[0].repair_symbols.len(), budget as usize);
            }
            let stock = rusqlite::Connection::open(&db).unwrap();
            assert_eq!(stock.query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 16);
        }
    });
}

#[test]
fn two_connections_keep_surviving_repair_worker_after_close() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("surviving-worker.db");
        let a = open(&db).await;
        a.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        wait_for_last_group(&db).await;
        let b = open(&db).await;
        a.execute("INSERT INTO t VALUES (1);").await.unwrap();
        a.close_without_checkpoint().await.unwrap();
        b.execute("INSERT INTO t VALUES (2);").await.unwrap();
        wait_for_last_group(&db).await;
        b.close_without_checkpoint().await.unwrap();
        let scan = scan_wal_fec(&wal_fec_path_for_wal(&wal_path(&db))).unwrap();
        assert!(!scan.truncated_tail);
        assert!(scan.groups.len() >= 3);
        let stock = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(stock.query_row("SELECT SUM(value) FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 3);
    });
}

#[test]
fn two_connections_keep_surviving_repair_worker_after_drop_with_backlog() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("paired-drop.db");
        let a = open(&db).await;
        a.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        wait_for_last_group(&db).await;
        let b = open(&db).await;
        let sidecar = wal_fec_path_for_wal(&wal_path(&db));
        let guard = hold_sidecar_guard(&sidecar).await;
        let before = fs::read(&sidecar).unwrap();
        for value in 1..=4 {
            a.execute(&format!("INSERT INTO t VALUES ({value});")).await.unwrap();
            b.execute(&format!("INSERT INTO t VALUES ({});", value + 4)).await.unwrap();
        }
        assert_eq!(fs::read(&sidecar).unwrap(), before);
        drop(a); // Cancel both an active repair and queued turns before B drains.
        drop(guard);
        wait_for_last_group(&db).await;
        b.close_without_checkpoint().await.unwrap();
        let scan = scan_wal_fec(&sidecar).unwrap();
        assert!(!scan.truncated_tail);
        assert!(scan.groups.windows(2).all(|pair| pair[0].meta.end_frame_no < pair[1].meta.start_frame_no));
        let stock = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(stock.query_row("SELECT SUM(value) FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 36);
    });
}

#[test]
fn restart_regenerates_missing_and_interrupted_sidecar_suffixes() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        for interrupted in [false, true] {
            let db = dir.path().join(format!("restart-{interrupted}.db"));
            let conn = open(&db).await;
            conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
            for value in 0..5 {
                conn.execute(&format!("INSERT INTO t VALUES ({value});")).await.unwrap();
            }
            conn.close_without_checkpoint().await.unwrap();
            let sidecar = wal_fec_path_for_wal(&wal_path(&db));
            let before = scan_wal_fec(&sidecar).unwrap();
            assert!(before.groups.len() >= 5);
            // Preserve the original artifact, then model the exact bytes left
            // by loss of the sidecar or a crash during its first group append.
            let preserved = sidecar.with_extension("preserved-fec");
            fs::rename(&sidecar, &preserved).unwrap();
            if interrupted {
                let bytes = fs::read(&preserved).unwrap();
                let mut torn = File::create(&sidecar).unwrap();
                torn.write_all(&bytes[..bytes.len().min(47)]).unwrap();
                torn.sync_all().unwrap();
                assert!(scan_wal_fec(&sidecar).unwrap().truncated_tail);
            }
            let reopened = open(&db).await;
            assert_eq!(reopened.query("SELECT COUNT(*) FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(5)]);
            reopened.close_without_checkpoint().await.unwrap();
            let after = scan_wal_fec(&sidecar).unwrap();
            assert!(!after.truncated_tail);
            assert_eq!(after.groups, before.groups, "restart regeneration must be deterministic");
        }
    });
}

#[test]
fn process_exit_after_durable_commit_before_repair_is_recoverable() {
    assert_process_crash_recovery(false);
}

#[cfg(target_os = "linux")]
#[test]
fn process_crash_during_real_sidecar_append_is_recoverable() {
    assert_process_crash_recovery(true);
}

fn assert_process_crash_recovery(crash_during_append: bool) {
    const CHILD_DB: &str = "FSQLITE_WAL_FEC_DURABLE_EXIT_DB";
    const DURABLE_EXIT: i32 = 73;
    const PHASE_MARKER: &str = "wal_fec_phase=durable repair_append=blocked committed_rows=5";
    const PARTIAL_APPEND_BYTES: usize = 47;
    let test_name = if crash_during_append {
        "process_crash_during_real_sidecar_append_is_recoverable"
    } else {
        "process_exit_after_durable_commit_before_repair_is_recoverable"
    };

    if let Some(db) = std::env::var_os(CHILD_DB) {
        let db = PathBuf::from(db);
        run_with_repair_pool(async {
            let conn = open(&db).await;
            conn.execute("PRAGMA raptorq_repair_symbols = 7;").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB);").await.unwrap();
            wait_for_last_group(&db).await;
            let sidecar = wal_fec_path_for_wal(&wal_path(&db));
            let guard = hold_sidecar_guard(&sidecar).await;
            let before = fs::read(&sidecar).unwrap();
            conn.execute("BEGIN;").await.unwrap();
            for id in 1..=5 {
                conn.execute(&format!("INSERT INTO t VALUES ({id}, zeroblob(3000));")).await.unwrap();
            }
            conn.execute("COMMIT;").await.expect("primary COMMIT while repair append is blocked");
            assert_eq!(fs::read(&sidecar).unwrap(), before, "repair must still be incomplete at process exit");
            assert_eq!(conn.query("SELECT COUNT(*) FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(5)]);
            eprintln!("{PHASE_MARKER}");
            std::io::stderr().flush().unwrap();
            if crash_during_append {
                // The parent lowers only this child's file-size limit after
                // durability, then permits the real background append.
                let release = db.with_extension("release-append");
                let deadline = Instant::now() + Duration::from_secs(30);
                while !release.exists() {
                    assert!(Instant::now() < deadline, "parent did not arm the append fault");
                    yield_now().await;
                }
                drop(guard);
                conn.close_without_checkpoint().await.expect("SIGXFSZ must interrupt the actual append");
                panic!("repair unexpectedly survived the file-size limit");
            }
            // No Connection/Runtime/guard destructors: the OS releases the
            // locks while the acknowledged WAL remains unprotected by FEC.
            std::process::exit(DURABLE_EXIT);
        });
        unreachable!("child must exit at the durability boundary");
    }

    let dir = tempfile::tempdir().unwrap().keep();
    let db = dir.join("durable-exit.db");
    let log_path = dir.join("child.log");
    let log = File::create(&log_path).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_DB, &db)
        .stdout(log.try_clone().unwrap()).stderr(log)
        .spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let wal = wal_path(&db);
    let sidecar = wal_fec_path_for_wal(&wal);
    let mut append_prefix = None;
    let mut acknowledged_wal = None;
    if crash_during_append {
        loop {
            let log = fs::read_to_string(&log_path).unwrap();
            if log.lines().any(|line| line == PHASE_MARKER) { break; }
            assert!(child.try_wait().unwrap().is_none(), "child exited before durable phase: {log}");
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child never reached durable phase; artifacts={} log={log}", dir.display());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let prefix = fs::read(&sidecar).unwrap();
        acknowledged_wal = Some(fs::read(&wal).unwrap());
        let limit = prefix.len() + PARTIAL_APPEND_BYTES;
        // Linux prlimit changes only our child. A write up to RLIMIT_FSIZE
        // succeeds partially; write_all's next write receives fatal SIGXFSZ.
        // Disable that child's core dump so the test retains only its fixtures.
        let limited = std::process::Command::new("prlimit")
            .args(["--pid", &child.id().to_string(), &format!("--fsize={limit}:{limit}"), "--core=0:0"])
            .output();
        if !limited.as_ref().is_ok_and(|output| output.status.success()) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("could not arm child append fault; artifacts={} result={limited:?}", dir.display());
        }
        append_prefix = Some(prefix);
        fs::write(db.with_extension("release-append"), b"file-size limit armed").unwrap();
    }
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() { break status; }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("durability child timed out; artifacts={} log={}", dir.display(), fs::read_to_string(&log_path).unwrap());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let log = fs::read_to_string(&log_path).unwrap();
    if crash_during_append {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(25), "child must die from Linux SIGXFSZ, not a panic: {log}");
        }
        #[cfg(not(target_os = "linux"))]
        panic!("the RLIMIT_FSIZE crash keeper requires Linux");
    } else {
        assert_eq!(status.code(), Some(DURABLE_EXIT), "child did not reach the intended exit: {log}");
    }
    assert!(log.lines().any(|line| line == PHASE_MARKER), "missing durable phase receipt: {log}");

    let durable_wal = fs::read(&wal).unwrap();
    if let Some(acknowledged) = acknowledged_wal {
        assert_eq!(durable_wal, acknowledged, "the append fault must not alter the acknowledged WAL");
    }
    let header = WalHeader::from_bytes(&durable_wal).unwrap();
    let last = u32::try_from((durable_wal.len() - 32) / (24 + header.page_size as usize)).unwrap();
    let crashed_bytes = fs::read(&sidecar).unwrap();
    if let Some(prefix) = append_prefix {
        assert_eq!(crashed_bytes.len(), prefix.len() + PARTIAL_APPEND_BYTES);
        assert_eq!(&crashed_bytes[..prefix.len()], &prefix, "short append must preserve complete repair groups");
        fs::copy(&sidecar, db.with_extension("interrupted-fec")).unwrap();
    }
    let before = scan_wal_fec(&sidecar).unwrap();
    assert_eq!(before.truncated_tail, crash_during_append);
    let protected = before.groups.last().expect("schema group was repaired before the child transaction").meta.end_frame_no;
    assert!(last >= protected + 5, "multi-page durable commit must lack a repair group");

    // This oracle runs before FrankenSQLite can regenerate any repair data.
    let stock = rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(stock.query_row("SELECT COUNT(*), SUM(id), SUM(length(payload)) FROM t", [], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
    }).unwrap(), (5, 15, 15_000));
    drop(stock);
    assert_eq!(scan_wal_fec(&sidecar).unwrap().groups, before.groups);

    run_with_repair_pool(async {
        let reopened = open(&db).await;
        assert_eq!(reopened.query("SELECT COUNT(*) FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(5)]);
        assert_eq!(reopened.query("PRAGMA raptorq_repair_symbols;").await.unwrap()[0].values(), &[SqliteValue::Integer(7)]);
        reopened.close_without_checkpoint().await.unwrap();
    });
    let repaired = scan_wal_fec(&sidecar).unwrap();
    assert!(!repaired.truncated_tail);
    assert_eq!(repaired.groups.len(), before.groups.len() + 1);
    assert_eq!(&repaired.groups[..before.groups.len()], &before.groups);
    let recovered = repaired.groups.last().unwrap();
    assert_eq!((recovered.meta.wal_salt1, recovered.meta.wal_salt2), (header.salts.salt1, header.salts.salt2));
    assert_eq!((recovered.meta.start_frame_no, recovered.meta.end_frame_no), (protected + 1, last));
    assert_eq!(recovered.meta.k_source, last - protected);
    assert_eq!(recovered.meta.r_repair, 7);
    assert_eq!(recovered.repair_symbols.len(), 7);
    assert_eq!(fs::read(&wal).unwrap(), durable_wal, "catch-up must preserve the primary WAL");

    let repaired_bytes = fs::read(&sidecar).unwrap();
    assert!(repaired_bytes.starts_with(&crashed_bytes), "regeneration must reproduce the exact interrupted record prefix");
    run_with_repair_pool(async {
        open(&db).await.close_without_checkpoint().await.unwrap();
    });
    assert_eq!(fs::read(&sidecar).unwrap(), repaired_bytes, "second restart must not duplicate repair groups");
    eprintln!("wal_fec_process_exit_verified test={test_name} status={status} partial_append={} durable_end={last} recovered_sources={} repair_symbols=7 artifacts={}",
        if crash_during_append { PARTIAL_APPEND_BYTES } else { 0 }, last - protected, dir.display());
}

#[test]
fn real_commit_overhead_and_hundred_commit_catch_up() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let mut elapsed = Vec::new();
        for budget in [0, 2] {
            let db = dir.path().join(format!("overhead-{budget}.db"));
            let conn = open(&db).await;
            conn.execute(&format!("PRAGMA raptorq_repair_symbols = {budget};")).await.unwrap();
            conn.execute("PRAGMA busy_timeout = 10000;").await.unwrap();
            conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
            let start = Instant::now();
            for value in 0..100 {
                conn.execute("BEGIN;").await.unwrap();
                conn.execute(&format!("INSERT INTO t VALUES ({value});")).await.unwrap();
                conn.execute("COMMIT;").await.unwrap();
            }
            elapsed.push(start.elapsed());
            conn.close_without_checkpoint().await.unwrap();
            if budget != 0 {
                let scan = scan_wal_fec(&wal_fec_path_for_wal(&wal_path(&db))).unwrap();
                assert!(scan.groups.len() >= 100);
                assert!(!scan.truncated_tail);
            }
        }
        eprintln!("real_sql_wal_fec commits=100 baseline={:?} repair={:?} delta_percent={:.3}",
            elapsed[0], elapsed[1], 100.0 * (elapsed[1].as_secs_f64() / elapsed[0].as_secs_f64() - 1.0));
    });
}

#[test]
fn corrupt_repair_configuration_preserves_primary_sql_recovery() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("bad-config.db");
        let conn = open(&db).await;
        conn.execute("PRAGMA raptorq_repair_symbols = 2;").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (9);").await.unwrap();
        conn.close_without_checkpoint().await.unwrap();
        let sidecar = wal_fec_path_for_wal(&wal_path(&db));
        let mut bytes = fs::read(&sidecar).unwrap();
        assert_eq!(&bytes[..8], b"FSQLWFCP");
        bytes[16] ^= 1; // Configuration checksum, not a primary WAL frame.
        fs::write(&sidecar, &bytes).unwrap();
        let reopened = open(&db).await;
        assert_eq!(reopened.query("SELECT value FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(9)]);
        reopened.close_without_checkpoint().await.unwrap();
        assert_eq!(fs::read(&sidecar).unwrap(), bytes, "malformed header must be preserved");
    });
}

#[test]
fn reopen_reclaims_retired_groups_after_checkpoint_owner_exits() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("retired-reopen.db");
        let writer = open(&db).await;
        writer.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        writer.execute("INSERT INTO t VALUES (4);").await.unwrap();
        writer.close_without_checkpoint().await.unwrap();
        let sidecar = wal_fec_path_for_wal(&wal_path(&db));
        let before = fs::read(&sidecar).unwrap();
        assert!(!scan_wal_fec(&sidecar).unwrap().groups.is_empty());
        let guard = hold_sidecar_guard(&sidecar).await;
        let maintenance = open(&db).await;
        maintenance.query("PRAGMA wal_checkpoint(TRUNCATE);").await.unwrap();
        assert_eq!(fs::read(&sidecar).unwrap(), before);
        drop(maintenance); // Lose its connection-local pending-cleanup ledger.
        drop(guard);
        let reopened = open(&db).await;
        assert_eq!(reopened.query("SELECT value FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(4)]);
        reopened.close_without_checkpoint().await.unwrap();
        assert!(scan_wal_fec(&sidecar).unwrap().groups.is_empty(), "startup must reclaim the retired generation even when the WAL is empty");
    });
}

#[test]
fn runtime_without_blocking_pool_preserves_primary_sql() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("no-pool.db");
        let conn = open(&db).await;
        conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (6);").await.unwrap();
        assert_eq!(conn.query("SELECT value FROM t;").await.unwrap()[0].values(), &[SqliteValue::Integer(6)]);
        conn.close_without_checkpoint().await.unwrap();
        assert!(!wal_fec_path_for_wal(&wal_path(&db)).exists());
    });
}

#[test]
fn normal_sync_close_can_checkpoint_after_repair_worker_drains() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("normal-close.db");
        let conn = open(&db).await;
        conn.execute("PRAGMA synchronous = NORMAL;").await.unwrap();
        conn.execute("CREATE TABLE t(value INTEGER);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (8);").await.unwrap();
        conn.close().await.expect("checkpoint after worker shutdown");
        let stock = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(stock.query_row("SELECT value FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 8);
    });
}
