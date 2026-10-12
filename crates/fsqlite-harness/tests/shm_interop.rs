#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use fsqlite_error::FrankenError;
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::shm::{
    SHM_SEGMENT_SIZE, SQLITE_SHM_SHARED, SQLITE_SHM_UNLOCK, wal_read_lock_slot,
};
use fsqlite_vfs::{UnixVfs, Vfs, VfsFile};
use tempfile::tempdir;

fn sqlite3_available() -> bool {
    match Command::new("sqlite3").arg("--version").output() {
        Ok(output) if output.status.success() => true,
        result => {
            eprintln!(
                "SKIP: sqlite3 CLI is unavailable; stock CLI interop was not run: {result:?}"
            );
            false
        }
    }
}

fn sqlite3_exec(db_path: &Path, sql: &str) -> Output {
    Command::new("sqlite3")
        .arg(db_path)
        .arg(sql)
        .output()
        .expect("sqlite3 command should execute")
}

fn setup_wal_db(db_path: &Path) {
    let output = sqlite3_exec(
        db_path,
        "PRAGMA journal_mode=WAL;\
         CREATE TABLE IF NOT EXISTS t(x INTEGER);\
         DELETE FROM t;\
         INSERT INTO t VALUES (1);",
    );
    assert!(
        output.status.success(),
        "sqlite3 setup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_legacy_sqlite_reader_coexists() {
    if !sqlite3_available() {
        return;
    }

    let tmp = tempdir().expect("tempdir should be created");
    let db_path = tmp.path().join("interop_reader.db");
    setup_wal_db(&db_path);

    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
    let (mut coordinator, _) = vfs
        .open(&cx, Some(&db_path), flags)
        .expect("frankensqlite unix vfs open should succeed");

    // Legacy sqlite3 interop is validated through classic DB lock grades.
    // WAL-slot protocol semantics are validated in `fsqlite-vfs` unit tests.
    coordinator
        .lock(&cx, LockLevel::Reserved)
        .expect("coordinator must hold reserved lock");

    let reader_output = sqlite3_exec(&db_path, "SELECT COUNT(*) FROM t;");
    assert!(
        reader_output.status.success(),
        "legacy sqlite3 reader should coexist while coordinator lock held: {}",
        String::from_utf8_lossy(&reader_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&reader_output.stdout);
    assert!(
        stdout.contains('1'),
        "legacy reader should observe consistent data, got: {stdout}"
    );

    coordinator
        .unlock(&cx, LockLevel::None)
        .expect("coordinator must release lock");
}

#[test]
fn test_legacy_sqlite_writer_gets_busy() {
    if !sqlite3_available() {
        return;
    }

    let tmp = tempdir().expect("tempdir should be created");
    let db_path = tmp.path().join("interop_writer_busy.db");
    setup_wal_db(&db_path);

    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
    let (mut coordinator, _) = vfs
        .open(&cx, Some(&db_path), flags)
        .expect("frankensqlite unix vfs open should succeed");

    // Use EXCLUSIVE to force sqlite3 writer `SQLITE_BUSY` from another process.
    coordinator
        .lock(&cx, LockLevel::Exclusive)
        .expect("coordinator must hold exclusive lock");

    let writer_output = sqlite3_exec(
        &db_path,
        "PRAGMA busy_timeout=100;\
         BEGIN IMMEDIATE;\
         INSERT INTO t VALUES (2);\
         COMMIT;",
    );
    assert!(
        !writer_output.status.success(),
        "legacy sqlite3 writer must fail with SQLITE_BUSY while coordinator lock held"
    );
    let stderr = String::from_utf8_lossy(&writer_output.stderr).to_lowercase();
    assert!(
        stderr.contains("locked") || stderr.contains("busy"),
        "expected SQLITE_BUSY/locked error, got stderr: {stderr}"
    );

    coordinator
        .unlock(&cx, LockLevel::None)
        .expect("coordinator must release lock");
}

#[test]
fn test_e2e_hybrid_shm_interop_with_c_sqlite() {
    if !sqlite3_available() {
        return;
    }

    let tmp = tempdir().expect("tempdir should be created");
    let db_path = tmp.path().join("interop_e2e.db");
    setup_wal_db(&db_path);

    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE;
    let (mut coordinator, _) = vfs
        .open(&cx, Some(&db_path), flags)
        .expect("frankensqlite unix vfs open should succeed");
    let (mut reader2, _) = vfs
        .open(&cx, Some(&db_path), flags)
        .expect("second unix vfs open should succeed");

    coordinator
        .lock(&cx, LockLevel::Reserved)
        .expect("coordinator must hold reserved lock");

    let updated = coordinator
        .compat_reader_acquire_wal_read_lock(&cx, 0, 123)
        .expect("reader protocol update should succeed");
    assert!(updated, "first reader must update aReadMark");
    let joined = reader2
        .compat_reader_acquire_wal_read_lock(&cx, 0, 123)
        .expect("second reader protocol join should succeed");
    assert!(
        !joined,
        "second reader must join existing aReadMark with SHARED"
    );

    let reader_output = sqlite3_exec(&db_path, "SELECT COUNT(*) FROM t;");
    assert!(
        reader_output.status.success(),
        "legacy sqlite3 reader should succeed while coordinator is alive: {}",
        String::from_utf8_lossy(&reader_output.stderr)
    );

    coordinator
        .unlock(&cx, LockLevel::None)
        .expect("coordinator should release reserved lock before writer exclusion phase");
    // Escalate only for the writer-exclusion assertion.
    coordinator
        .lock(&cx, LockLevel::Exclusive)
        .expect("coordinator must hold exclusive lock for writer exclusion phase");

    let writer_output = sqlite3_exec(
        &db_path,
        "PRAGMA busy_timeout=100;\
         BEGIN IMMEDIATE;\
         INSERT INTO t VALUES (99);\
         COMMIT;",
    );
    assert!(
        !writer_output.status.success(),
        "legacy sqlite3 writer must observe SQLITE_BUSY while coordinator lock is held"
    );

    let read_slot0 = wal_read_lock_slot(0).expect("reader slot 0 should exist");
    reader2
        .shm_lock(&cx, read_slot0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
        .expect("reader2 shared unlock should succeed");
    coordinator
        .shm_lock(&cx, read_slot0, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
        .expect("coordinator shared unlock should succeed");
    coordinator
        .unlock(&cx, LockLevel::None)
        .expect("coordinator must release lock");
}

const GROWTH_CHILD_TIMEOUT: Duration = Duration::from_secs(45);
const GROWTH_ROWS: i64 = 8_500;

fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

/// Every process owns its descriptors: closing a stock SQLite handle in the
/// coordinator process must not accidentally release the coordinator's fcntl
/// locks. A killed or wedged child is reaped before its fixture is removed.
struct GrowthChild {
    process: Child,
    role: &'static str,
    control: PathBuf,
}

impl GrowthChild {
    fn spawn(role: &'static str, control: &Path, db_path: &Path, limited: bool) -> Self {
        let executable = std::env::current_exe().expect("integration test executable");
        let mut command = if limited {
            let mut shell = Command::new("sh");
            // The limit applies only to this disposable process. Shells report
            // -f in 512- or 1024-byte units; either limit admits region zero
            // (32 KiB) and refuses the requested third region (96 KiB).
            shell
                .args([
                    "-c",
                    "trap '' XFSZ; ulimit -f 64 || exit 90; exec \"$@\"",
                    "sh",
                ])
                .arg(executable);
            shell
        } else {
            Command::new(executable)
        };
        let process = command
            .args([
                "--exact",
                "shm_growth_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("FSQLITE_SHM_INTEROP_ROLE", role)
            .env("FSQLITE_SHM_INTEROP_CONTROL", control)
            .env("FSQLITE_SHM_INTEROP_DB", db_path)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start isolated SHM interop child");
        Self {
            process,
            role,
            control: control.to_path_buf(),
        }
    }

    fn wait_for(&mut self, marker: &str) {
        let deadline = Instant::now() + GROWTH_CHILD_TIMEOUT;
        let path = self.control.join(marker);
        while !path.is_file() {
            if let Some(status) = self.process.try_wait().expect("poll interop child") {
                panic!("{} exited before {marker}: {status}", self.role);
            }
            assert!(
                Instant::now() < deadline,
                "{} timed out at {marker}",
                self.role
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn wait_success(&mut self) {
        let deadline = Instant::now() + GROWTH_CHILD_TIMEOUT;
        loop {
            if let Some(status) = self.process.try_wait().expect("poll interop child") {
                assert!(status.success(), "{} failed: {status}", self.role);
                assert!(
                    self.control.join(format!("{}-ok", self.role)).is_file(),
                    "{} must run its assertions, not merely select zero tests",
                    self.role
                );
                return;
            }
            assert!(Instant::now() < deadline, "{} timed out", self.role);
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for GrowthChild {
    fn drop(&mut self) {
        if !matches!(self.process.try_wait(), Ok(Some(_))) {
            let _ = self.process.kill();
        }
        let _ = self.process.wait();
    }
}

fn signal_growth_child(control: &Path, marker: &str) {
    fs::write(control.join(marker), b"ready\n").expect("publish child rendezvous");
}

fn stock_count(connection: &rusqlite::Connection) -> i64 {
    connection
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .expect("stock SQLite reads its snapshot")
}

fn assert_stock_contents(connection: &rusqlite::Connection, expected_rows: i64) {
    assert_eq!(stock_count(connection), expected_rows);
    let original: String = connection
        .query_row("SELECT hex(payload) FROM t WHERE x = 1", [], |row| {
            row.get(0)
        })
        .expect("read original committed payload");
    assert_eq!(original, "1122334455667788");
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity check");
    assert_eq!(integrity, "ok");
}

fn stock_reader_child(control: &Path, db_path: &Path, expected_rows: i64) {
    let connection = rusqlite::Connection::open(db_path).expect("open stock reader");
    connection
        .busy_timeout(Duration::from_secs(2))
        .expect("bound stock reader lock waits");
    connection
        .execute_batch(
            "PRAGMA page_size=512;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(x INTEGER PRIMARY KEY, payload BLOB);
             INSERT INTO t VALUES(1, X'1122334455667788');
             BEGIN;",
        )
        .expect("create stock WAL family and begin reader transaction");
    assert_eq!(stock_count(&connection), 1);
    signal_growth_child(control, "reader-ready");

    let deadline = Instant::now() + GROWTH_CHILD_TIMEOUT;
    while !control.join("reader-finish").is_file() {
        assert_eq!(stock_count(&connection), 1, "pinned WAL snapshot changed");
        if control.join("check-reader").is_file() && !control.join("reader-after-growth").is_file()
        {
            signal_growth_child(control, "reader-after-growth");
        }
        assert!(
            Instant::now() < deadline,
            "stock reader rendezvous timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(stock_count(&connection), 1, "final pinned snapshot changed");
    connection.execute_batch("COMMIT").expect("finish snapshot");
    assert_stock_contents(&connection, expected_rows);
}

fn stock_writer_child(db_path: &Path) {
    let mut connection = rusqlite::Connection::open(db_path).expect("open stock writer");
    connection
        .busy_timeout(Duration::from_secs(2))
        .expect("bound stock writer lock waits");
    connection
        .execute_batch("PRAGMA wal_autocheckpoint=0")
        .expect("keep WAL frames while foreign reader is pinned");
    let transaction = connection.transaction().expect("begin stock write");
    {
        let mut insert = transaction
            .prepare("INSERT INTO t VALUES(?1, zeroblob(400))")
            .expect("prepare small-page WAL workload");
        for row in 2..=GROWTH_ROWS + 1 {
            insert.execute([row]).expect("append stock row");
        }
    }
    transaction.commit().expect("commit stock WAL workload");
    assert_stock_contents(&connection, GROWTH_ROWS + 1);
}

fn staged_probe_child(db_path: &Path, limited: bool) {
    let database_before = fs::read(db_path).expect("read staged database");
    let wal_before = fs::read(sidecar_path(db_path, "-wal")).expect("read staged WAL");
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let (mut file, _) = vfs
        .open(
            &cx,
            Some(db_path),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
        )
        .expect("open staged database through UnixVfs");
    let first = file
        .shm_map(&cx, 0, SHM_SEGMENT_SIZE, false)
        .expect("map complete staged first SHM region");
    let original = first.lock().to_vec();

    if limited {
        for _ in 0..2 {
            let error = file
                .shm_map(&cx, 2, SHM_SEGMENT_SIZE, true)
                .expect_err("resource limit must reject third region before mmap publication");
            let FrankenError::Io(error) = error else {
                panic!("EFBIG must remain a recoverable I/O error: {error}");
            };
            assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
            assert!(error.raw_os_error().is_some(), "preserve allocation errno");
            assert_eq!(&*first.lock(), original.as_slice());
        }
        assert!(
            file.shm_map(&cx, 2, SHM_SEGMENT_SIZE, false).is_err(),
            "failed extension must not expose a complete unbacked third region"
        );
    } else {
        let third = file
            .shm_map(&cx, 2, SHM_SEGMENT_SIZE, true)
            .expect("staged SHM growth recovers when the file limit is lifted");
        let last_word = third.len() - 4;
        third
            .write_u32_ne(last_word, 0x51_47_42_55)
            .expect("touch recovered backing");
        assert_eq!(
            third
                .read_u32_ne(last_word)
                .expect("read recovered backing"),
            0x51_47_42_55
        );
        third
            .write_u32_ne(last_word, 0)
            .expect("restore unused WAL-index bytes");
        assert_eq!(&*first.lock(), original.as_slice());
    }

    drop(first);
    drop(file);
    // Opening and closing another descriptor while UnixVfs owns fcntl claims
    // would release this process's locks on that inode. Inspect bytes only
    // after all mappings and the canonical descriptor have been released.
    assert_eq!(
        fs::read(db_path).expect("reread staged database"),
        database_before
    );
    assert_eq!(
        fs::read(sidecar_path(db_path, "-wal")).expect("reread staged WAL"),
        wal_before
    );
    if !limited {
        let stock = rusqlite::Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("open recovered family with stock SQLite");
        assert_stock_contents(&stock, 1);
    }
}

#[test]
#[ignore = "subprocess entry point; the SHM growth parent tests run it explicitly"]
fn shm_growth_child() {
    let role = std::env::var("FSQLITE_SHM_INTEROP_ROLE").expect("child role is required");
    let control = PathBuf::from(
        std::env::var_os("FSQLITE_SHM_INTEROP_CONTROL").expect("child control path is required"),
    );
    let db_path = PathBuf::from(
        std::env::var_os("FSQLITE_SHM_INTEROP_DB").expect("child database path is required"),
    );
    match role.as_str() {
        "reader-growth" => stock_reader_child(&control, &db_path, GROWTH_ROWS + 1),
        "reader-staged" => stock_reader_child(&control, &db_path, 1),
        "writer" => stock_writer_child(&db_path),
        "probe-limited" => staged_probe_child(&db_path, true),
        "probe-recovered" => staged_probe_child(&db_path, false),
        _ => panic!("unknown SHM growth child role: {role}"),
    }
    signal_growth_child(&control, &format!("{role}-ok"));
}

#[test]
fn test_shm_growth_preserves_stock_wal_reader_and_mapping_aliases() {
    let tmp = tempdir().expect("isolated stock interop fixture");
    let db_path = tmp.path().join("growth.db");
    let mut reader = GrowthChild::spawn("reader-growth", tmp.path(), &db_path, false);
    reader.wait_for("reader-ready");

    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let (mut file, _) = vfs
        .open(
            &cx,
            Some(&db_path),
            VfsOpenFlags::MAIN_DB | VfsOpenFlags::READWRITE,
        )
        .expect("open foreign reader's database through UnixVfs");
    let first = file
        .shm_map(&cx, 0, SHM_SEGMENT_SIZE, false)
        .expect("map stock WAL index");
    let alias = first.share();
    let original = first.lock().to_vec();
    assert!(first.is_mmap_backed());
    let second = file
        .shm_map(&cx, 1, SHM_SEGMENT_SIZE, true)
        .expect("allocate second region");
    let third = file
        .shm_map(&cx, 2, SHM_SEGMENT_SIZE, true)
        .expect("allocate third region");
    assert_eq!(
        &*alias.lock(),
        original.as_slice(),
        "growth must preserve live stock WAL index"
    );

    let replacement = file
        .shm_map(&cx, 0, SHM_SEGMENT_SIZE * 2, true)
        .expect("larger mapping must preserve its existing aliases");
    assert_eq!(first.len(), usize::try_from(SHM_SEGMENT_SIZE).unwrap());
    assert_eq!(&replacement.lock()[..first.len()], original.as_slice());
    signal_growth_child(tmp.path(), "check-reader");
    reader.wait_for("reader-after-growth");

    let mut writer = GrowthChild::spawn("writer", tmp.path(), &db_path, false);
    writer.wait_success();
    file.shm_barrier();
    // Stock's 512-byte-page workload exceeds both the 4062-frame first hash
    // region and the following 4096-frame region, using the allocated third
    // region. The old 32 KiB alias must observe the new published mxFrame.
    let frames = alias
        .read_u32_ne(16)
        .expect("read stock mxFrame through old alias");
    assert!(
        frames > 8_158,
        "stock writer did not reach the third SHM region: {frames}"
    );
    assert_eq!(replacement.read_u32_ne(16).unwrap(), frames);
    let segment = usize::try_from(SHM_SEGMENT_SIZE).unwrap();
    assert_eq!(&*second.lock(), &replacement.lock()[segment..segment * 2]);
    // aPgno[0] in both extended regions is published by the independent stock
    // writer. Read through the original mappings without opening/closing a
    // second SHM descriptor, which would release this process's fcntl locks.
    assert_ne!(second.read_u32_ne(0).unwrap(), 0);
    assert_ne!(third.read_u32_ne(0).unwrap(), 0);
    drop(file);
    assert_eq!(
        alias.read_u32_ne(16).unwrap(),
        frames,
        "mapping survives coordinator close"
    );
    signal_growth_child(tmp.path(), "reader-finish");
    reader.wait_success();
}

#[test]
fn test_staged_wal_family_shm_growth_failure_and_recovery() {
    let tmp = tempdir().expect("isolated copied-family fixture");
    let source = tmp.path().join("source.db");
    let staged = tmp.path().join("probe.db");
    let mut source_reader = GrowthChild::spawn("reader-staged", tmp.path(), &source, false);
    source_reader.wait_for("reader-ready");

    let mut originals = Vec::new();
    for suffix in ["", "-wal", "-shm"] {
        let source_path = sidecar_path(&source, suffix);
        let bytes = fs::read(&source_path).expect("read live source database family");
        fs::copy(&source_path, sidecar_path(&staged, suffix))
            .expect("stage database family for probe");
        originals.push((suffix, bytes));
    }
    let mut limited = GrowthChild::spawn("probe-limited", tmp.path(), &staged, true);
    limited.wait_success();
    for (suffix, bytes) in &originals {
        let after =
            fs::read(sidecar_path(&staged, suffix)).expect("read family after refused growth");
        if *suffix == "-shm" {
            // Page-at-a-time allocation may leave a backed partial extension.
            // Every byte that was already visible must remain unchanged.
            assert!(
                after.starts_with(bytes),
                "failed growth changed the existing WAL index"
            );
        } else {
            assert_eq!(
                after, *bytes,
                "failed health probe changed staged {suffix} contents"
            );
        }
    }

    let mut recovered = GrowthChild::spawn("probe-recovered", tmp.path(), &staged, false);
    recovered.wait_success();
    for (suffix, bytes) in &originals {
        assert_eq!(
            fs::read(sidecar_path(&source, suffix)).expect("read original family after recovery"),
            *bytes,
            "staged health probe changed live source {suffix} contents"
        );
    }
    signal_growth_child(tmp.path(), "reader-finish");
    source_reader.wait_success();
}
