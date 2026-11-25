//! bd-ioq6x keepers: the hole marker and the first-attacher reclaim of EOF holes.
//!
//! The image every reclaim keeper starts from is the K4 crash shape on a database
//! that carries real rows: a table written through SQL, then, inside ONE pager on
//! the same file, a CONCURRENT holder that allocated EOF pages (two written plus an
//! unused lease) while a CONCURRENT grower allocated past them and committed. A copy
//! of the files taken at that moment (the holder still live) is a crash image whose
//! held pages are below the page count, in no b-tree and not free: stock reports
//! each as "never used". The copy keeps the migration-state marker, as every
//! fsqlite-created database has one, so the migration pass does not repair it.
//!
//! Keepers (each names the planted mutant that turns it red):
//! - R1 `first_attacher_reclaims_the_k4_crash_image`: the grower's commit publishes
//!   the marker, and the next open reclaims the holes; rows and stock integrity hold
//!   (red: reclaim skipped; parent: no marker is ever written).
//! - missing sidecar `a_copy_without_the_marker_keeps_the_pre_marker_behavior`: no
//!   walk, no error (red: walk without the marker).
//! - R4 `single_writer_cycles_never_write_the_marker` (red: marker on every commit).
//! - R2 `an_attached_peer_keeps_the_marker_for_a_later_first_attacher`, in-process,
//!   fsqlite child and stock child (red: the first-attacher gate ignored).
//! - R2b + waiting peer `peers_opening_during_the_walk_wait_and_attach_after_commit`,
//!   fsqlite and stock child processes, and an in-process thread (red: downgrade
//!   before the walk; fsqlite openers that do not wait).
//! - N openers `concurrent_openers_elect_exactly_one_first_attacher`, fsqlite only,
//!   then mixed with stock (red: reclaim skipped).
//!
//! Cross-process peers are this test binary re-run with `IOQ6X_CHILD_ROLE` set; the
//! `ioq6x_child_entry` test is a no-op without it. All keepers serialize on one lock
//! because the reclaim observer is process-wide.
#![cfg(unix)]

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite_core::connection::Connection;
use fsqlite_core::migration::{
    HoleReclaimStage, hole_marker_path, migration_marker_path, set_hole_reclaim_observer,
};
use fsqlite_core::wal_adapter::WalBackendAdapter;
use fsqlite_pager::{JournalMode, MvccPager, SimplePager, TransactionHandle, TransactionMode};
use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::value::SqliteValue;
use fsqlite_vfs::{UnixVfs, Vfs};
use fsqlite_wal::{WalFile, WalSalts};
use rusqlite::OpenFlags;

const PAGE: usize = 4096;
const ROWS: i64 = 300;
/// Long enough that every peer started from the observer is mid-open when it ends.
const WALK_STRETCH: Duration = Duration::from_millis(1500);
/// fsqlite's default busy timeout; the stock children set the same.
const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
}

fn wal_of(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db.display()))
}

fn shm_of(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}-shm", db.display()))
}

fn marker_of(db: &Path) -> PathBuf {
    hole_marker_path(db)
}

fn db_str(db: &Path) -> String {
    db.to_string_lossy().into_owned()
}

// ---------------------------------------------------------------------------
// Reclaim observer
// ---------------------------------------------------------------------------

#[derive(Default)]
struct StageCounts {
    walking: AtomicUsize,
    committed: AtomicUsize,
    failed: AtomicUsize,
    freed: AtomicUsize,
}

/// Install an observer that counts stages and runs `on_walking` / `on_committed`.
fn observe(
    on_walking: impl Fn() + Send + Sync + 'static,
    on_committed: impl Fn() + Send + Sync + 'static,
) -> Arc<StageCounts> {
    let counts = Arc::new(StageCounts::default());
    let seen = Arc::clone(&counts);
    set_hole_reclaim_observer(Some(Arc::new(move |stage| match stage {
        HoleReclaimStage::Walking => {
            seen.walking.fetch_add(1, Ordering::SeqCst);
            on_walking();
        }
        HoleReclaimStage::Committed { freed } => {
            seen.committed.fetch_add(1, Ordering::SeqCst);
            seen.freed.store(freed, Ordering::SeqCst);
            on_committed();
        }
        HoleReclaimStage::Failed => {
            seen.failed.fetch_add(1, Ordering::SeqCst);
        }
    })));
    counts
}

fn counting() -> Arc<StageCounts> {
    observe(|| {}, || {})
}

struct ObserverReset;

impl Drop for ObserverReset {
    fn drop(&mut self) {
        set_hole_reclaim_observer(None);
    }
}

// ---------------------------------------------------------------------------
// Stock view
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StockView {
    page_count: i64,
    freelist_count: i64,
    never_used: BTreeSet<u32>,
    other: Vec<String>,
    rows: i64,
}

fn stock_view(db: &Path) -> StockView {
    let stock = rusqlite::Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("stock open");
    stock
        .busy_timeout(BUSY_TIMEOUT)
        .expect("stock busy timeout");
    let report: Vec<String> = {
        let mut stmt = stock
            .prepare("PRAGMA integrity_check(100000)")
            .expect("prepare integrity_check");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("integrity_check")
            .collect::<Result<_, _>>()
            .expect("integrity rows")
    };
    let mut never_used = BTreeSet::new();
    let mut other = Vec::new();
    // Stock returns its report as one row with embedded newlines.
    for line in report.iter().flat_map(|row| row.lines()) {
        if line.starts_with("*** in database") {
            continue;
        }
        if line.contains("never used") {
            let digits: String = line
                .trim_start_matches(|c: char| !c.is_ascii_digit())
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(page) = digits.parse() {
                never_used.insert(page);
                continue;
            }
        }
        if line != "ok" {
            other.push(line.to_owned());
        }
    }
    let page_count = stock
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .expect("page_count");
    let freelist_count = stock
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .expect("freelist_count");
    let rows = stock
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("row count");
    StockView {
        page_count,
        freelist_count,
        never_used,
        other,
        rows,
    }
}

// ---------------------------------------------------------------------------
// The K4 crash image
// ---------------------------------------------------------------------------

struct HoleImage {
    _dir: tempfile::TempDir,
    /// The crash copy every keeper opens.
    db: PathBuf,
    /// Pages the live holder held: below the page count, in no b-tree, not free.
    holes: BTreeSet<u32>,
    /// Whether the grower's commit published the marker on the ORIGINAL file.
    marker_published: bool,
}

async fn open_raw_pager(cx: &Cx, db: &Path) -> SimplePager<UnixVfs> {
    let pager = SimplePager::open_with_cx(cx, UnixVfs::new(), db, PageSize::DEFAULT)
        .await
        .expect("open raw pager");
    pager.finish_namespace_bootstrap().expect("bootstrap");
    let (file, _) = UnixVfs::new()
        .open(
            cx,
            Some(&wal_of(db)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
        )
        .expect("open wal");
    let wal = WalFile::create(
        cx,
        file,
        4096,
        0,
        WalSalts {
            salt1: 0x1046_0001,
            salt2: 0x1046_0002,
        },
    )
    .await
    .expect("create wal");
    pager
        .set_wal_backend(Box::new(WalBackendAdapter::new(wal)))
        .expect("wal backend");
    pager
        .set_journal_mode(cx, JournalMode::Wal)
        .await
        .expect("wal mode");
    pager
}

async fn populate(db: &Path) {
    let conn = Connection::open(db_str(db)).await.expect("create");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .await
        .expect("create table");
    conn.execute("BEGIN").await.expect("begin");
    for id in 0..ROWS {
        conn.execute(&format!(
            "INSERT INTO t VALUES({id}, '{}')",
            format!("row{id:04}-").repeat(20)
        ))
        .await
        .expect("insert");
    }
    conn.execute("COMMIT").await.expect("commit");
    conn.close().await.expect("close");
    // Leave one plain file with an empty freelist (VACUUM; the WAL folded and
    // removed by stock's last close), so the raw pager below allocates from
    // EOF and starts its own WAL, as the Q1 probe does.
    let stock = rusqlite::Connection::open(db).expect("stock checkpoint open");
    stock.execute("VACUUM", []).expect("vacuum");
    stock
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .expect("truncate checkpoint");
    let free: i64 = stock
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .expect("freelist_count");
    assert_eq!(
        free, 0,
        "precondition: the populated file has no free pages"
    );
    drop(stock);
}

fn copy_if_exists(from: &Path, to: &Path) {
    if from.exists() {
        std::fs::copy(from, to).expect("copy companion");
    }
}

async fn build_hole_image(with_marker: bool) -> HoleImage {
    let dir = tempfile::tempdir().expect("temp dir");
    let orig = dir.path().join("orig.db");
    populate(&orig).await;
    assert!(
        migration_marker_path(&db_str(&orig)).exists(),
        "precondition: fsqlite stamps the migration marker at birth"
    );
    assert!(
        !marker_of(&orig).exists(),
        "precondition: single-writer population writes no hole marker"
    );

    let cx = Cx::new();
    let pager = open_raw_pager(&cx, &orig).await;
    let mut holder = pager
        .begin(&cx, TransactionMode::Concurrent)
        .await
        .expect("holder begin");
    // Allocate (and write) until the holder holds an unused EOF lease.
    let mut written = Vec::new();
    let mut lease = BTreeSet::new();
    for _ in 0..64 {
        let page = holder.allocate_page(&cx).await.expect("holder allocate");
        holder
            .write_page(&cx, page, &[0x52; PAGE])
            .await
            .expect("holder write");
        written.push(page.get());
        lease = holder
            .live_reserved_pages()
            .iter()
            .map(|page| page.get())
            .collect();
        if !lease.is_empty() {
            break;
        }
    }
    assert!(
        !lease.is_empty(),
        "precondition: the holder must hold an unused EOF lease (allocated {written:?})"
    );
    let mut grow = pager
        .begin(&cx, TransactionMode::Concurrent)
        .await
        .expect("grow begin");
    let g = grow.allocate_page(&cx).await.expect("g");
    grow.write_page(&cx, g, &[0x54; PAGE])
        .await
        .expect("g write");
    assert!(
        written.iter().chain(&lease).all(|page| *page < g.get()),
        "precondition: the grower allocates past the holder (written={written:?} lease={lease:?} g={g:?})"
    );
    grow.commit(&cx).await.expect("grow commit");
    drop(grow);
    let marker_published = marker_of(&orig).exists();

    let crash_dir = dir.path().join("crash");
    std::fs::create_dir_all(&crash_dir).expect("crash dir");
    let db = crash_dir.join("crash.db");
    std::fs::copy(&orig, &db).expect("copy db");
    copy_if_exists(&wal_of(&orig), &wal_of(&db));
    copy_if_exists(&shm_of(&orig), &shm_of(&db));
    std::fs::copy(
        migration_marker_path(&db_str(&orig)),
        migration_marker_path(&db_str(&db)),
    )
    .expect("copy migration marker");
    if with_marker && marker_published {
        std::fs::copy(marker_of(&orig), marker_of(&db)).expect("copy hole marker");
    }

    holder.rollback(&cx).await.expect("holder rollback");
    drop(holder);
    drop(pager);
    // The populated file has no free pages (VACUUM), so every page the holder
    // took came from EOF and is held below the grower's committed page count.
    let holes = lease.iter().copied().chain(written).collect();
    HoleImage {
        _dir: dir,
        db,
        holes,
        marker_published,
    }
}

/// The crash image as stock sees it before any fsqlite open: every held page is a
/// never-used hole, and the rows are intact (the keepers' non-vacuity check).
fn assert_holes_present(image: &HoleImage, label: &str) -> StockView {
    let before = stock_view(&image.db);
    let missing: Vec<u32> = image
        .holes
        .iter()
        .copied()
        .filter(|page| !before.never_used.contains(page))
        .collect();
    assert!(
        missing.is_empty() && before.rows == ROWS,
        "bd-ioq6x {label}: precondition, the crash image must show the held pages as holes: \
         missing={missing:?} holes={:?} before={before:?}",
        image.holes
    );
    before
}

fn assert_reclaimed(image: &HoleImage, label: &str) {
    let after = stock_view(&image.db);
    assert!(
        after.never_used.is_empty()
            && after.other.is_empty()
            && after.rows == ROWS
            && after.page_count > after.freelist_count
            && after.freelist_count >= i64::try_from(image.holes.len()).expect("small"),
        "bd-ioq6x {label}: after the first attacher's reclaim, stock integrity must be ok with zero \
         never-used pages, every row intact, and the holes on the freelist: holes={:?} after={after:?}",
        image.holes
    );
    assert!(
        !marker_of(&image.db).exists(),
        "bd-ioq6x {label}: the marker goes once the reclaim is durable"
    );
}

async fn open_and_close(db: &Path) {
    let conn = Connection::open(db_str(db)).await.expect("fsqlite open");
    let rows = conn
        .query("SELECT count(*) FROM t")
        .await
        .expect("count rows");
    assert_eq!(
        rows[0].values()[0],
        SqliteValue::Integer(ROWS),
        "rows readable through fsqlite"
    );
    conn.close().await.expect("fsqlite close");
}

// ---------------------------------------------------------------------------
// Child processes
// ---------------------------------------------------------------------------

struct ChildSpec<'a> {
    role: &'a str,
    db: &'a Path,
    out: PathBuf,
    go: Option<&'a Path>,
    release: Option<&'a Path>,
}

fn spawn_child(spec: &ChildSpec<'_>) -> Child {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary"));
    cmd.args([
        "--exact",
        "ioq6x_child_entry",
        "--nocapture",
        "--test-threads=1",
    ])
    .env("IOQ6X_CHILD_ROLE", spec.role)
    .env("IOQ6X_CHILD_DB", spec.db)
    .env("IOQ6X_CHILD_OUT", &spec.out)
    .env("RUST_MIN_STACK", "8388608")
    .stdin(Stdio::null());
    if let Some(go) = spec.go {
        cmd.env("IOQ6X_CHILD_GO", go);
    }
    if let Some(release) = spec.release {
        cmd.env("IOQ6X_CHILD_RELEASE", release);
    }
    cmd.spawn().expect("spawn child")
}

fn wait_for_file(path: &Path, limit: Duration) -> bool {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() > limit {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    true
}

fn read_report(out: &Path) -> HashMap<String, String> {
    assert!(
        wait_for_file(out, Duration::from_secs(90)),
        "child report {} never appeared (deadlock or crash)",
        out.display()
    );
    let text = std::fs::read_to_string(out).expect("read child report");
    text.split_whitespace()
        .filter_map(|field| field.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn write_report(out: &Path, report: &str) {
    let staging = out.with_extension("tmp");
    std::fs::write(&staging, report).expect("write child report");
    std::fs::rename(&staging, out).expect("publish child report");
}

fn reap(mut child: Child) {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("wait child") {
            assert!(status.success(), "child exited with {status}");
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "child did not exit (deadlock)"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn child_fsqlite(db: &Path, out: &Path, release: Option<&Path>) {
    let counts = counting();
    let _reset = ObserverReset;
    let start = now_ns();
    asupersync::test_utils::run_test(|| async {
        let opened = Connection::open(db_str(db)).await;
        let done = now_ns();
        let (conn, error) = match opened {
            Ok(conn) => (Some(conn), String::from("none")),
            Err(error) => (None, format!("{error:?}").replace(char::is_whitespace, "_")),
        };
        let rows = match &conn {
            Some(conn) => match conn.query("SELECT count(*) FROM t").await {
                Ok(rows) => match rows[0].values()[0] {
                    SqliteValue::Integer(count) => count,
                    _ => -1,
                },
                Err(_) => -2,
            },
            None => -3,
        };
        write_report(
            out,
            &format!(
                "ok={} start_ns={start} done_ns={done} walking={} committed={} rows={rows} err={error}\n",
                conn.is_some(),
                counts.walking.load(Ordering::SeqCst),
                counts.committed.load(Ordering::SeqCst),
            ),
        );
        if let Some(release) = release {
            assert!(wait_for_file(release, Duration::from_secs(90)), "release");
        }
        if let Some(conn) = conn {
            conn.close().await.expect("child close");
        }
    });
}

fn child_stock(db: &Path, out: &Path, release: Option<&Path>) {
    let start = now_ns();
    let opened = rusqlite::Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .and_then(|conn| {
        conn.busy_timeout(BUSY_TIMEOUT)?;
        let rows: i64 = conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))?;
        Ok((conn, rows))
    });
    let done = now_ns();
    let (conn, rows, error) = match opened {
        Ok((conn, rows)) => (Some(conn), rows, String::from("none")),
        Err(error) => (
            None,
            -3,
            error.to_string().replace(char::is_whitespace, "_"),
        ),
    };
    write_report(
        out,
        &format!(
            "ok={} start_ns={start} done_ns={done} walking=0 committed=0 rows={rows} err={error}\n",
            conn.is_some()
        ),
    );
    if let Some(release) = release {
        assert!(wait_for_file(release, Duration::from_secs(90)), "release");
    }
    drop(conn);
}

/// Re-run of this binary as a peer process; a no-op in the normal test run.
#[test]
fn ioq6x_child_entry() {
    let Ok(role) = std::env::var("IOQ6X_CHILD_ROLE") else {
        return;
    };
    let db = PathBuf::from(std::env::var("IOQ6X_CHILD_DB").expect("child db"));
    let out = PathBuf::from(std::env::var("IOQ6X_CHILD_OUT").expect("child out"));
    let release = std::env::var_os("IOQ6X_CHILD_RELEASE").map(PathBuf::from);
    if let Some(go) = std::env::var_os("IOQ6X_CHILD_GO").map(PathBuf::from) {
        assert!(wait_for_file(&go, Duration::from_secs(90)), "go signal");
    }
    match role.as_str() {
        "fsqlite" => child_fsqlite(&db, &out, release.as_deref()),
        "stock" => child_stock(&db, &out, release.as_deref()),
        other => panic!("unknown child role {other}"),
    }
}

fn field<'a>(report: &'a HashMap<String, String>, key: &str) -> &'a str {
    report.get(key).map_or("", String::as_str)
}

fn field_u128(report: &HashMap<String, String>, key: &str) -> u128 {
    field(report, key).parse().unwrap_or(0)
}

fn field_usize(report: &HashMap<String, String>, key: &str) -> usize {
    field(report, key).parse().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Keepers
// ---------------------------------------------------------------------------

/// R1: the grower's commit publishes the marker before the hole is durable, and the
/// next first attacher reclaims the crash image exactly.
#[test]
fn first_attacher_reclaims_the_k4_crash_image() {
    let _serial = serial();
    let _reset = ObserverReset;
    asupersync::test_utils::run_test(|| async {
        let image = build_hole_image(true).await;
        assert!(
            image.marker_published,
            "bd-ioq6x R1: the commit that grew past held EOF pages must publish {}",
            marker_of(&image.db).display()
        );
        assert_holes_present(&image, "R1");
        let counts = counting();
        open_and_close(&image.db).await;
        assert_eq!(
            (
                counts.walking.load(Ordering::SeqCst),
                counts.committed.load(Ordering::SeqCst)
            ),
            (1, 1),
            "bd-ioq6x R1: the first attacher runs one reclaim to a durable commit"
        );
        assert!(
            counts.freed.load(Ordering::SeqCst) >= image.holes.len(),
            "bd-ioq6x R1: the reclaim frees at least the held pages ({:?}), freed {}",
            image.holes,
            counts.freed.load(Ordering::SeqCst)
        );
        assert_reclaimed(&image, "R1");
        // Idempotent and quiet afterwards: a reopen walks nothing.
        let again = counting();
        open_and_close(&image.db).await;
        assert_eq!(
            again.walking.load(Ordering::SeqCst),
            0,
            "no marker, no walk"
        );
    });
}

/// A database copied without its marker degrades to the pre-marker behavior: the
/// open succeeds, walks nothing, and the holes stay for VACUUM, as before.
#[test]
fn a_copy_without_the_marker_keeps_the_pre_marker_behavior() {
    let _serial = serial();
    let _reset = ObserverReset;
    asupersync::test_utils::run_test(|| async {
        let image = build_hole_image(false).await;
        assert!(!marker_of(&image.db).exists(), "precondition: no sidecar");
        assert_holes_present(&image, "missing-sidecar");
        let counts = counting();
        open_and_close(&image.db).await;
        assert_eq!(
            counts.walking.load(Ordering::SeqCst),
            0,
            "bd-ioq6x missing sidecar: no marker, no walk"
        );
        let after = stock_view(&image.db);
        assert!(
            image
                .holes
                .iter()
                .all(|page| after.never_used.contains(page))
                && after.rows == ROWS,
            "bd-ioq6x missing sidecar: the open must leave the file as before (holes for VACUUM), \
             not error and not reclaim: holes={:?} after={after:?}",
            image.holes
        );
    });
}

/// R4: single-writer open/commit/close cycles never write the marker.
#[test]
fn single_writer_cycles_never_write_the_marker() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("r4.db");
        let marker = marker_of(&db);
        let conn = Connection::open(db_str(&db)).await.expect("open");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("create");
        conn.execute("CREATE INDEX t_v ON t(v)")
            .await
            .expect("index");
        for id in 0..40 {
            conn.execute(&format!("INSERT INTO t VALUES({id}, 'auto{id}')"))
                .await
                .expect("autocommit insert");
        }
        assert!(!marker.exists(), "bd-ioq6x R4: autocommit inserts");
        conn.execute("BEGIN").await.expect("begin");
        for id in 40..2000 {
            conn.execute(&format!(
                "INSERT INTO t VALUES({id}, '{}')",
                format!("bulk{id}-").repeat(30)
            ))
            .await
            .expect("bulk insert");
        }
        conn.execute("COMMIT").await.expect("commit");
        assert!(!marker.exists(), "bd-ioq6x R4: a growing bulk transaction");
        conn.execute("UPDATE t SET v = v || 'x' WHERE id % 3 = 0")
            .await
            .expect("update");
        conn.execute("DELETE FROM t WHERE id % 2 = 0")
            .await
            .expect("delete");
        conn.execute("BEGIN CONCURRENT")
            .await
            .expect("begin concurrent");
        for id in 2000..2600 {
            conn.execute(&format!(
                "INSERT INTO t VALUES({id}, '{}')",
                format!("conc{id}-").repeat(30)
            ))
            .await
            .expect("concurrent insert");
        }
        conn.execute("COMMIT").await.expect("commit concurrent");
        conn.execute("BEGIN").await.expect("begin");
        conn.execute("SAVEPOINT s1").await.expect("savepoint");
        for id in 2600..2700 {
            conn.execute(&format!(
                "INSERT INTO t VALUES({id}, '{}')",
                format!("sp{id}-").repeat(30)
            ))
            .await
            .expect("savepoint insert");
        }
        conn.execute("ROLLBACK TO s1").await.expect("rollback to");
        conn.execute("INSERT INTO t VALUES(2700, 'after')")
            .await
            .expect("insert after rollback-to");
        conn.execute("COMMIT").await.expect("commit");
        assert!(
            !marker.exists(),
            "bd-ioq6x R4: update, delete, BEGIN CONCURRENT growth and a savepoint rollback"
        );
        conn.close().await.expect("close");
        let conn = Connection::open(db_str(&db)).await.expect("reopen");
        conn.execute("INSERT INTO t VALUES(5001, 'reopened')")
            .await
            .expect("insert after reopen");
        conn.close().await.expect("close");
        assert!(!marker.exists(), "bd-ioq6x R4: reopen cycle");
        let stock = rusqlite::Connection::open(&db).expect("stock");
        let ok: String = stock
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .expect("integrity");
        assert_eq!(ok, "ok", "single-writer image is stock-clean");
    });
}

/// R2: an open that is NOT the first attacher (a peer already holds the dead-man
/// switch) does not reclaim, and the marker stays for a later first attacher.
#[test]
fn an_attached_peer_keeps_the_marker_for_a_later_first_attacher() {
    let _serial = serial();
    let _reset = ObserverReset;
    for peer in ["in_process", "fsqlite_child", "stock_child"] {
        asupersync::test_utils::run_test(|| async {
            let image = build_hole_image(true).await;
            assert!(image.marker_published, "precondition: marker published");
            assert_holes_present(&image, "R2");
            let marker = marker_of(&image.db);
            let aside = image.db.with_extension("holes-aside");
            // Attach the peer while no marker is visible, so the peer itself is
            // an ordinary (non-reclaiming) attachment.
            std::fs::rename(&marker, &aside).expect("marker aside");
            let release = image.db.with_extension("release");
            let mut child = None;
            let mut in_process_peer = None;
            match peer {
                "in_process" => {
                    in_process_peer = Some(
                        Connection::open(db_str(&image.db))
                            .await
                            .expect("in-process peer"),
                    );
                }
                role => {
                    let out = image.db.with_extension(format!("{role}.out"));
                    let spawned = spawn_child(&ChildSpec {
                        role: if role == "fsqlite_child" {
                            "fsqlite"
                        } else {
                            "stock"
                        },
                        db: &image.db,
                        out: out.clone(),
                        go: None,
                        release: Some(&release),
                    });
                    let report = read_report(&out);
                    assert_eq!(field(&report, "ok"), "true", "peer attached: {report:?}");
                    child = Some(spawned);
                }
            }
            std::fs::rename(&aside, &marker).expect("marker back");
            let counts = counting();
            open_and_close(&image.db).await;
            assert_eq!(
                counts.walking.load(Ordering::SeqCst),
                0,
                "bd-ioq6x R2 ({peer}): an open with a live peer attached must not reclaim"
            );
            assert!(
                marker.exists(),
                "bd-ioq6x R2 ({peer}): the marker stays for a later first attacher"
            );
            if let Some(conn) = in_process_peer {
                conn.close().await.expect("peer close");
            }
            std::fs::write(&release, b"go").expect("release");
            if let Some(child) = child {
                reap(child);
            }
            // Now the next open is the first attacher.
            let later = counting();
            open_and_close(&image.db).await;
            assert_eq!(
                later.committed.load(Ordering::SeqCst),
                1,
                "bd-ioq6x R2 ({peer}): the later first attacher reclaims"
            );
            assert_reclaimed(&image, "R2 later first attacher");
        });
    }
}

/// R2b + condition 2: while the first attacher's walk is stretched, peers that
/// open (an fsqlite process, a stock process, an fsqlite thread of this process)
/// wait within their busy timeout and attach only after the reclaim commit is
/// durable; none fails, none attaches mid-walk.
#[test]
fn peers_opening_during_the_walk_wait_and_attach_after_commit() {
    let _serial = serial();
    let _reset = ObserverReset;
    asupersync::test_utils::run_test(|| async {
        let image = build_hole_image(true).await;
        assert!(image.marker_published, "precondition: marker published");
        assert_holes_present(&image, "R2b");
        let db = image.db.clone();
        let children: Arc<Mutex<Vec<(String, PathBuf, Child)>>> = Arc::new(Mutex::new(Vec::new()));
        let thread_report: Arc<Mutex<Option<(u128, u128, bool)>>> = Arc::new(Mutex::new(None));
        let thread_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> =
            Arc::new(Mutex::new(None));
        let walk_started_ns = Arc::new(Mutex::new(0_u128));
        let committed_ns = Arc::new(Mutex::new(0_u128));
        let counts = {
            let children = Arc::clone(&children);
            let thread_report = Arc::clone(&thread_report);
            let thread_handle = Arc::clone(&thread_handle);
            let walk_started_ns = Arc::clone(&walk_started_ns);
            let committed_ns = Arc::clone(&committed_ns);
            let db = db.clone();
            observe(
                move || {
                    *walk_started_ns.lock().unwrap() = now_ns();
                    for role in ["fsqlite", "stock"] {
                        let out = db.with_extension(format!("{role}.r2b.out"));
                        let child = spawn_child(&ChildSpec {
                            role,
                            db: &db,
                            out: out.clone(),
                            go: None,
                            release: None,
                        });
                        children.lock().unwrap().push((role.to_owned(), out, child));
                    }
                    let peer_db = db.clone();
                    let report = Arc::clone(&thread_report);
                    *thread_handle.lock().unwrap() = Some(std::thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let start = now_ns();
                            let opened = Connection::open(db_str(&peer_db)).await;
                            let done = now_ns();
                            let ok = opened.is_ok();
                            *report.lock().unwrap() = Some((start, done, ok));
                            if let Ok(conn) = opened {
                                conn.close().await.expect("thread peer close");
                            }
                        });
                    }));
                    std::thread::sleep(WALK_STRETCH);
                },
                move || {
                    *committed_ns.lock().unwrap() = now_ns();
                },
            )
        };
        open_and_close(&db).await;
        set_hole_reclaim_observer(None);
        assert_eq!(
            counts.committed.load(Ordering::SeqCst),
            1,
            "bd-ioq6x R2b: the stretched first attacher must reclaim"
        );
        let walk_started = *walk_started_ns.lock().unwrap();
        let committed = *committed_ns.lock().unwrap();
        let mut verdicts = Vec::new();
        let spawned = std::mem::take(&mut *children.lock().unwrap());
        for (role, out, child) in spawned {
            let report = read_report(&out);
            reap(child);
            verdicts.push((
                role,
                field(&report, "ok") == "true",
                field_u128(&report, "start_ns"),
                field_u128(&report, "done_ns"),
                field_usize(&report, "walking"),
                report.get("err").cloned().unwrap_or_default(),
            ));
        }
        let handle = thread_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().expect("thread peer");
        }
        let (start, done, ok) = thread_report.lock().unwrap().expect("thread peer report");
        verdicts.push((
            "in_process_thread".to_owned(),
            ok,
            start,
            done,
            0,
            String::new(),
        ));
        for (role, ok, start, done, walking, err) in &verdicts {
            assert!(
                *ok,
                "bd-ioq6x R2b ({role}): a peer opening during the walk must wait, not fail: err={err} \
                 verdicts={verdicts:?}"
            );
            assert!(
                *start < committed && *start >= walk_started,
                "bd-ioq6x R2b ({role}): non-vacuity, the peer must start its open inside the walk window \
                 (walk_started={walk_started} committed={committed}): {verdicts:?}"
            );
            assert!(
                *done >= committed,
                "bd-ioq6x R2b ({role}): the peer attached before the reclaim commit was durable \
                 (done={done} committed={committed}): {verdicts:?}"
            );
            assert!(
                done - start < BUSY_TIMEOUT.as_nanos(),
                "bd-ioq6x R2b ({role}): the wait must end inside the busy timeout: {verdicts:?}"
            );
            assert_eq!(
                *walking, 0,
                "bd-ioq6x R2b ({role}): a waiting peer is not a first attacher"
            );
        }
        assert_reclaimed(&image, "R2b");
    });
}

/// Condition 3: N processes opening at once never deadlock or fail, and exactly one
/// of them is the first attacher that reclaims; mixed with stock, at most one
/// fsqlite opener reclaims and every opener succeeds.
#[test]
fn concurrent_openers_elect_exactly_one_first_attacher() {
    let _serial = serial();
    for (label, roles) in [
        ("fsqlite_only", vec!["fsqlite"; 6]),
        (
            "mixed",
            vec!["fsqlite", "stock", "fsqlite", "stock", "fsqlite", "fsqlite"],
        ),
    ] {
        asupersync::test_utils::run_test(|| async {
            let image = build_hole_image(true).await;
            assert!(image.marker_published, "precondition: marker published");
            assert_holes_present(&image, label);
            let go = image.db.with_extension("go");
            let release = image.db.with_extension("release");
            let mut spawned = Vec::new();
            for (index, role) in roles.iter().copied().enumerate() {
                let out = image.db.with_extension(format!("{index}.{role}.out"));
                let child = spawn_child(&ChildSpec {
                    role,
                    db: &image.db,
                    out: out.clone(),
                    go: Some(&go),
                    release: Some(&release),
                });
                spawned.push((role, out, child));
            }
            std::fs::write(&go, b"go").expect("go");
            let mut reports = Vec::new();
            for (role, out, _) in &spawned {
                reports.push((*role, read_report(out)));
            }
            std::fs::write(&release, b"go").expect("release");
            for (_, _, child) in spawned {
                reap(child);
            }
            let failures: Vec<_> = reports
                .iter()
                .filter(|(_, report)| field(report, "ok") != "true")
                .collect();
            assert!(
                failures.is_empty(),
                "bd-ioq6x N openers ({label}): no opener may fail: {reports:?}"
            );
            let reclaimers: usize = reports
                .iter()
                .map(|(_, report)| field_usize(report, "committed"))
                .sum();
            if label == "fsqlite_only" {
                assert_eq!(
                    reclaimers, 1,
                    "bd-ioq6x N openers ({label}): exactly one first attacher reclaims: {reports:?}"
                );
                assert_reclaimed(&image, label);
            } else {
                assert!(
                    reclaimers <= 1,
                    "bd-ioq6x N openers ({label}): at most one fsqlite first attacher: {reports:?}"
                );
                assert_eq!(
                    marker_of(&image.db).exists(),
                    reclaimers == 0,
                    "bd-ioq6x N openers ({label}): the marker goes iff a reclaim committed"
                );
                let after = stock_view(&image.db);
                assert!(
                    after.other.is_empty() && after.rows == ROWS,
                    "bd-ioq6x N openers ({label}): no corruption: {after:?}"
                );
                if reclaimers == 1 {
                    assert_reclaimed(&image, label);
                }
            }
        });
    }
}
