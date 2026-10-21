#![cfg(all(unix, feature = "native"))]

//! Native, multiprocess GH#519 progress and ownership regressions.
//!
//! Every adversarial schedule waits for a production trace from an actual
//! failed kernel lock, or for an acknowledged acquired lock. There are no
//! sleeps creating races. Each parent owns, watchdogs, kills, and reaps its
//! exact children. This target runs without compiling the SQL engine:
//! `cargo test -p fsqlite-vfs --test gh519_native_lock_progress -- --nocapture`

use std::io::{BufRead as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use fsqlite_error::{ErrorCode, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_vfs::shm::{
    SHM_SEGMENT_SIZE, SQLITE_SHM_EXCLUSIVE, SQLITE_SHM_LOCK, SQLITE_SHM_UNLOCK, WAL_CKPT_LOCK,
    WAL_WRITE_LOCK,
};
use fsqlite_vfs::{UnixVfs, Vfs, VfsFile};
use tracing_subscriber::prelude::*;

const CHILD_TEST: &str = "main_contention_never_retains_wal_and_all_fences_progress";
const CHILD_DB: &str = "FSQLITE_GH519_NATIVE_DB";
const CHILD_KIND: &str = "FSQLITE_GH519_NATIVE_KIND";
const PREFIX: &str = "GH519-NATIVE|";
const RESERVED: u64 = 0x4000_0001;
const SHARED: u64 = 0x4000_0002;
const WATCHDOG: Duration = Duration::from_secs(30);
const PROMPT_PROGRESS: Duration = Duration::from_secs(5);
static SCENARIOS: Mutex<()> = Mutex::new(());

fn serial_scenario() -> MutexGuard<'static, ()> {
    SCENARIOS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn emit(message: &str) {
    println!("{PREFIX}{message}");
    std::io::stdout().flush().unwrap();
}

fn input() -> String {
    let mut line = String::new();
    assert_ne!(
        std::io::stdin().lock().read_line(&mut line).unwrap(),
        0,
        "parent closed child input"
    );
    line.trim_end().to_owned()
}

#[derive(Default)]
struct Schedule {
    pause: AtomicU64,
    cancel: Mutex<Option<Cx>>,
    sibling: Mutex<Option<mpsc::Sender<bool>>>,
}

struct Witness(Arc<Schedule>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Witness {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "fsqlite_vfs::unix" {
            return;
        }
        #[derive(Default)]
        struct Fields {
            event: String,
            start: Option<u64>,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}

            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "event" {
                    self.event = value.to_owned();
                }
            }

            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "lock_start" {
                    self.start = Some(value);
                }
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.event != "unix_lock_busy" {
            return;
        }
        let Some(start) = fields.start else {
            return;
        };
        if start == RESERVED {
            let sibling = self.0.sibling.lock().unwrap().take();
            if let Some(sibling) = sibling {
                emit("blocked-reserved");
                assert_eq!(input(), "borrow-write");
                sibling.send(false).unwrap();
                assert_eq!(input(), "resume");
                sibling.send(true).unwrap();
                return;
            }
            let cancelled = self.0.cancel.lock().unwrap().take();
            if let Some(cx) = cancelled {
                emit("cancel-at-reserved");
                assert_eq!(input(), "cancel");
                cx.cancel();
                return;
            }
        }
        if self
            .0
            .pause
            .compare_exchange(start, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            emit(if start == RESERVED {
                "blocked-reserved"
            } else {
                "blocked-shared"
            });
            assert_eq!(input(), "resume");
        }
    }
}

#[derive(Clone, Copy)]
enum Fence {
    Append,
    Checkpoint,
    Recovery,
    WholeImage,
}

impl Fence {
    fn parse(kind: &str) -> Self {
        match kind {
            "append" => Self::Append,
            "checkpoint" => Self::Checkpoint,
            "recovery" => Self::Recovery,
            "whole-image" => Self::WholeImage,
            _ => panic!("unknown fence: {kind}"),
        }
    }

    fn acquire(self, file: &mut impl VfsFile, cx: &Cx) -> Result<()> {
        match self {
            Self::Append => file.lock_external_wal_append(cx),
            Self::Checkpoint => file.lock_external_wal_checkpoint(cx),
            Self::Recovery => file.lock_external_wal_recovery(cx),
            Self::WholeImage => file.lock_external_maintenance(cx, true),
        }
    }

    fn restore(self, file: &mut impl VfsFile, cx: &Cx) {
        match self {
            Self::Append => file.restore_external_wal_append_attempt(cx).unwrap(),
            _ => file.restore_external_maintenance_attempt(cx).unwrap(),
        }
    }
}

fn wal_slot(file: &mut impl VfsFile, cx: &Cx, slot: u32, acquire: bool) -> Result<()> {
    let operation = if acquire {
        SQLITE_SHM_LOCK
    } else {
        SQLITE_SHM_UNLOCK
    };
    file.shm_lock(cx, slot, 1, operation | SQLITE_SHM_EXCLUSIVE)
}

fn run_actor(path: &Path, map_shm: bool) {
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
    let (mut file, _) = vfs.open(&cx, Some(path), flags).unwrap();
    let region = if map_shm {
        Some(file.shm_map(&cx, 0, SHM_SEGMENT_SIZE, true).unwrap())
    } else {
        None
    };
    let schedule = Arc::new(Schedule::default());
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Witness(Arc::clone(&schedule))));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    emit("ready");
    loop {
        let command = input();
        let words = command.split_whitespace().collect::<Vec<_>>();
        match words.as_slice() {
            ["reserved"] => file.lock(&cx, LockLevel::Reserved).unwrap(),
            ["shared"] => file.lock(&cx, LockLevel::Shared).unwrap(),
            ["exclusive"] => file.lock(&cx, LockLevel::Exclusive).unwrap(),
            ["release-main"] => file.unlock(&cx, LockLevel::None).unwrap(),
            ["busy-shared"] => {
                let error = file
                    .lock(&cx, LockLevel::Shared)
                    .expect_err("PENDING must exclude new readers");
                assert_eq!(error.error_code(), ErrorCode::Busy);
            }
            ["probe-main"] => {
                file.lock(&cx, LockLevel::Reserved)
                    .expect("failed attempt must restore main prefix");
                file.unlock(&cx, LockLevel::None).unwrap();
            }
            ["timeout", value] => file.set_busy_timeout_ms(value.parse().unwrap()),
            ["pause-reserved"] => schedule.pause.store(RESERVED, Ordering::SeqCst),
            ["pause-shared"] => schedule.pause.store(SHARED, Ordering::SeqCst),
            ["hold-write"] => wal_slot(&mut file, &cx, WAL_WRITE_LOCK, true).unwrap(),
            ["release-write"] => wal_slot(&mut file, &cx, WAL_WRITE_LOCK, false).unwrap(),
            ["hold-checkpoint"] => wal_slot(&mut file, &cx, WAL_CKPT_LOCK, true).unwrap(),
            ["release-checkpoint"] => wal_slot(&mut file, &cx, WAL_CKPT_LOCK, false).unwrap(),
            [probe @ ("probe-write" | "probe-checkpoint" | "busy-write" | "busy-checkpoint")] => {
                let slot = if probe.ends_with("write") {
                    WAL_WRITE_LOCK
                } else {
                    WAL_CKPT_LOCK
                };
                let result = wal_slot(&mut file, &cx, slot, true);
                if probe.starts_with("busy-") {
                    assert_eq!(
                        result
                            .expect_err("borrowed claim must survive restoration")
                            .error_code(),
                        ErrorCode::Busy
                    );
                } else {
                    result.expect("failed attempt must release newly acquired WAL slots");
                    wal_slot(&mut file, &cx, slot, false).unwrap();
                }
            }
            ["fence", kind] => {
                let fence = Fence::parse(kind);
                fence.acquire(&mut file, &cx).unwrap();
                fence.restore(&mut file, &cx);
            }
            ["busy-fence", kind] => {
                let fence = Fence::parse(kind);
                let started = Instant::now();
                let error = fence
                    .acquire(&mut file, &cx)
                    .expect_err("foreign owner must block admission");
                assert_eq!(error.error_code(), ErrorCode::Busy);
                fence.restore(&mut file, &cx);
                fence.restore(&mut file, &cx);
                emit(&format!("busy {}", started.elapsed().as_millis()));
                continue;
            }
            ["borrowed", kind] => {
                let fence = Fence::parse(kind);
                file.set_busy_timeout_ms(30000);
                let started = Instant::now();
                let error = fence
                    .acquire(&mut file, &cx)
                    .expect_err("borrowed WAL forbids waiting on main");
                assert_eq!(error.error_code(), ErrorCode::Busy);
                assert!(
                    started.elapsed() < PROMPT_PROGRESS,
                    "{command} waited while retaining WAL"
                );
                file.set_busy_timeout_ms(0);
                fence.restore(&mut file, &cx);
                fence.restore(&mut file, &cx);
            }
            ["sibling", mapping, kind] => {
                let fence = Fence::parse(kind);
                let (mut sibling, _) = vfs.open(&cx, Some(path), flags).unwrap();
                let sibling_region = if *mapping == "unmapped" {
                    None
                } else {
                    assert_eq!(*mapping, "mapped");
                    Some(sibling.shm_map(&cx, 0, SHM_SEGMENT_SIZE, true).unwrap())
                };
                sibling.set_busy_timeout_ms(30000);
                let started = Instant::now();
                let error = fence
                    .acquire(&mut sibling, &cx)
                    .expect_err("sibling WAL forbids main wait");
                assert_eq!(error.error_code(), ErrorCode::Busy);
                assert!(
                    started.elapsed() < PROMPT_PROGRESS,
                    "{command} waited with sibling WAL"
                );
                fence.restore(&mut sibling, &cx);
                drop(sibling_region);
                sibling.close(&cx).unwrap();
            }
            ["cancel", kind] => {
                let fence = Fence::parse(kind);
                let operation_cx = cx.create_child();
                *schedule.cancel.lock().unwrap() = Some(operation_cx.clone());
                file.set_busy_timeout_ms(30000);
                let error = fence
                    .acquire(&mut file, &operation_cx)
                    .expect_err("cancelled wait must abort");
                assert_eq!(error.error_code(), ErrorCode::Abort);
                assert!(operation_cx.is_cancel_requested());
                assert!(
                    !cx.is_cancel_requested(),
                    "cancellation escaped its child context"
                );
                file.set_busy_timeout_ms(0);
                fence.restore(&mut file, &operation_cx);
                fence.restore(&mut file, &operation_cx);
            }
            ["finish"] => {
                drop(region);
                file.close(&cx).unwrap();
                emit("done");
                return;
            }
            _ => panic!("unknown native child command: {command}"),
        }
        emit("ok");
    }
}

fn run_siblings(path: &Path) {
    let cx = Cx::new();
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB;
    let (mut waiter, _) = vfs.open(&cx, Some(path), flags).unwrap();
    let waiter_region = waiter.shm_map(&cx, 0, SHM_SEGMENT_SIZE, true).unwrap();
    let (mut sibling, _) = vfs.open(&cx, Some(path), flags).unwrap();
    let sibling_region = sibling.shm_map(&cx, 0, SHM_SEGMENT_SIZE, true).unwrap();
    let (sender, commands) = mpsc::channel();
    let sibling_cx = cx.create_child();
    let worker = std::thread::spawn(move || {
        assert!(!commands.recv_timeout(WATCHDOG).unwrap());
        wal_slot(&mut sibling, &sibling_cx, WAL_WRITE_LOCK, true).unwrap();
        emit("sibling-holds-write");
        assert!(commands.recv_timeout(WATCHDOG).unwrap());
        sibling.set_busy_timeout_ms(30000);
        let error = sibling
            .lock_external_wal_append(&sibling_cx)
            .expect_err("borrowed WRITE forbids main wait");
        assert_eq!(error.error_code(), ErrorCode::Busy);
        sibling
            .restore_external_wal_append_attempt(&sibling_cx)
            .unwrap();
        wal_slot(&mut sibling, &sibling_cx, WAL_WRITE_LOCK, false).unwrap();
        emit("sibling-released-write");
        drop(sibling_region);
        sibling.close(&sibling_cx).unwrap();
    });
    let schedule = Arc::new(Schedule::default());
    *schedule.sibling.lock().unwrap() = Some(sender);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Witness(schedule)));
    let _subscriber = tracing::dispatcher::set_default(&dispatch);
    waiter.set_busy_timeout_ms(30000);
    emit("ready");
    assert_eq!(input(), "wait");
    let first = waiter.lock_external_wal_checkpoint(&cx);
    waiter.restore_external_maintenance_attempt(&cx).unwrap();
    if let Err(error) = first {
        assert_eq!(error.error_code(), ErrorCode::Busy);
        emit("waiter-busy");
        assert_eq!(input(), "retry");
        waiter.lock_external_wal_checkpoint(&cx).unwrap();
        waiter.restore_external_maintenance_attempt(&cx).unwrap();
    }
    emit("waiter-acquired");
    worker.join().unwrap();
    drop(waiter_region);
    waiter.close(&cx).unwrap();
    emit("done");
}

struct Actor {
    child: Child,
    output: Receiver<String>,
    reader: Option<JoinHandle<()>>,
    name: String,
}

impl Actor {
    fn spawn(path: &Path, kind: &str, name: &str) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([CHILD_TEST, "--exact", "--nocapture"])
            .env(CHILD_DB, path)
            .env(CHILD_KIND, kind)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let (sender, output) = mpsc::channel();
        let mut actor = Self {
            child,
            output,
            reader: None,
            name: name.to_owned(),
        };
        let stdout = actor.child.stdout.take().unwrap();
        actor.reader = Some(std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some((_, witness)) = line.split_once(PREFIX)
                    && sender.send(witness.to_owned()).is_err()
                {
                    break;
                }
            }
        }));
        actor.expect("ready");
        actor
    }

    fn send(&mut self, command: &str) {
        writeln!(self.child.stdin.as_mut().unwrap(), "{command}")
            .unwrap_or_else(|error| panic!("{} sending {command}: {error}", self.name));
    }

    fn receive_for(&self, timeout: Duration) -> String {
        self.output
            .recv_timeout(timeout)
            .unwrap_or_else(|error| panic!("{} progress watchdog: {error}", self.name))
    }

    fn receive(&self) -> String {
        self.receive_for(WATCHDOG)
    }

    fn expect(&self, expected: &str) {
        assert_eq!(self.receive(), expected, "{} protocol", self.name);
    }

    fn command(&mut self, command: &str) {
        self.send(command);
        self.expect("ok");
    }

    fn finish(&mut self) {
        self.send("finish");
        self.expect("done");
        self.wait_success();
    }

    fn wait_success(&mut self) {
        let deadline = Instant::now() + WATCHDOG;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "{} failed to exit", self.name);
            // Poll completed process exit only, never lock scheduling.
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(status.success(), "{} exited with {status}", self.name);
        if let Some(reader) = self.reader.take() {
            reader.join().unwrap();
        }
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn setup() -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lock-progress.db");
    // The VFS does not parse database pages. A marker lets every scenario
    // also assert that lock admission and cleanup never change file contents.
    std::fs::write(&path, b"GH519 lock progress fixture").unwrap();
    (directory, path)
}

fn assert_unchanged(path: &Path) {
    assert_eq!(std::fs::read(path).unwrap(), b"GH519 lock progress fixture");
}

#[test]
fn main_contention_never_retains_wal_and_all_fences_progress() {
    if let Some(path) = std::env::var_os(CHILD_DB) {
        match std::env::var(CHILD_KIND).unwrap().as_str() {
            "main" => run_actor(Path::new(&path), false),
            "wal" => run_actor(Path::new(&path), true),
            "siblings" => run_siblings(Path::new(&path)),
            other => panic!("unknown actor kind: {other}"),
        }
        return;
    }
    let _scenario = serial_scenario();
    for kind in ["append", "checkpoint", "recovery", "whole-image"] {
        let (_directory, path) = setup();
        let mut holder = Actor::spawn(&path, "wal", "immediate main owner");
        let mut waiter = Actor::spawn(&path, "wal", kind);
        holder.command("reserved");
        waiter.command("timeout 30000");
        waiter.command("pause-reserved");
        waiter.send(&format!("fence {kind}"));
        waiter.expect("blocked-reserved");
        // The waiter cannot release an erroneous WAL prefix until resumed.
        // A zero-timeout physical append must succeed while it stays paused.
        holder.command("fence append");
        holder.finish();
        waiter.send("resume");
        waiter.expect("ok");
        waiter.finish();
        assert_unchanged(&path);
    }
}

#[test]
fn borrowed_and_unmapped_sibling_claims_never_wait_for_main() {
    let _scenario = serial_scenario();
    let (_directory, path) = setup();
    let mut holder = Actor::spawn(&path, "main", "foreign RESERVED owner");
    let mut borrower = Actor::spawn(&path, "wal", "borrowed WAL owner");
    let mut observer = Actor::spawn(&path, "wal", "foreign ownership oracle");
    for slot in ["write", "checkpoint"] {
        holder.command("reserved");
        borrower.command(&format!("hold-{slot}"));
        let other = if slot == "write" {
            "checkpoint"
        } else {
            "write"
        };
        for command in [
            "borrowed append",
            "borrowed checkpoint",
            "borrowed recovery",
            "borrowed whole-image",
            "sibling mapped append",
            "sibling mapped checkpoint",
            "sibling unmapped append",
            "sibling unmapped checkpoint",
        ] {
            borrower.command(command);
            observer.command(&format!("busy-{slot}"));
            observer.command(&format!("probe-{other}"));
        }
        holder.command("release-main");
        observer.command("probe-main");
        borrower.command(&format!("release-{slot}"));
    }
    borrower.command("fence append");
    observer.finish();
    borrower.finish();
    holder.finish();
    assert_unchanged(&path);
}

#[test]
fn new_sibling_wal_owner_can_release_while_another_handle_waits() {
    let _scenario = serial_scenario();
    let (_directory, path) = setup();
    let mut holder = Actor::spawn(&path, "main", "foreign RESERVED owner");
    let mut siblings = Actor::spawn(&path, "siblings", "racing sibling handles");
    holder.command("reserved");
    siblings.send("wait");
    siblings.expect("blocked-reserved");
    // Introduce WRITE only after the other handle already checked ownership
    // and encountered real foreign contention on the main file.
    siblings.send("borrow-write");
    siblings.expect("sibling-holds-write");
    siblings.send("resume");
    let deadline = Instant::now() + PROMPT_PROGRESS;
    let mut waiter_busy = false;
    loop {
        match siblings
            .receive_for(deadline.saturating_duration_since(Instant::now()))
            .as_str()
        {
            "waiter-busy" => waiter_busy = true,
            "sibling-released-write" => break,
            other => panic!("unexpected sibling progress: {other}"),
        }
    }
    // Requiring sibling cleanup before releasing RESERVED detects an inode
    // mutex retained throughout the first waiter's 30-second busy wait.
    holder.command("fence append");
    holder.finish();
    if !waiter_busy {
        match siblings.receive().as_str() {
            "waiter-busy" => waiter_busy = true,
            "waiter-acquired" => {}
            other => panic!("unexpected waiter completion: {other}"),
        }
    }
    if waiter_busy {
        siblings.send("retry");
        siblings.expect("waiter-acquired");
    }
    siblings.expect("done");
    siblings.wait_success();
    assert_unchanged(&path);
}

#[test]
fn exclusive_backoff_retains_pending_until_the_original_reader_exits() {
    let _scenario = serial_scenario();
    let (_directory, path) = setup();
    let mut reader = Actor::spawn(&path, "main", "original SHARED owner");
    let mut writer = Actor::spawn(&path, "main", "exclusive waiter");
    let mut newcomer = Actor::spawn(&path, "main", "new reader");
    reader.command("shared");
    writer.command("timeout 30000");
    writer.command("pause-shared");
    writer.send("exclusive");
    writer.expect("blocked-shared");
    newcomer.command("busy-shared");
    reader.finish();
    writer.send("resume");
    writer.expect("ok");
    newcomer.command("busy-shared");
    writer.command("release-main");
    newcomer.command("shared");
    newcomer.command("release-main");
    newcomer.finish();
    writer.finish();
    assert_unchanged(&path);
}

#[test]
fn cancellation_and_timeouts_restore_every_external_attempt_family() {
    let _scenario = serial_scenario();
    let (_directory, path) = setup();
    let mut holder = Actor::spawn(&path, "main", "foreign RESERVED owner");
    let mut waiter = Actor::spawn(&path, "wal", "cancellable waiter");
    let mut observer = Actor::spawn(&path, "wal", "foreign ownership oracle");
    for kind in ["append", "checkpoint", "recovery", "whole-image"] {
        holder.command("reserved");
        waiter.send(&format!("cancel {kind}"));
        waiter.expect("cancel-at-reserved");
        waiter.send("cancel");
        assert_eq!(waiter.receive_for(PROMPT_PROGRESS), "ok");
        observer.command("probe-write");
        observer.command("probe-checkpoint");
        holder.command("release-main");
        observer.command("probe-main");
        for timeout in [0_u64, 200] {
            holder.command("reserved");
            waiter.command(&format!("timeout {timeout}"));
            waiter.send(&format!("busy-fence {kind}"));
            let response = waiter.receive();
            let elapsed = response
                .strip_prefix("busy ")
                .unwrap()
                .parse::<u128>()
                .unwrap();
            assert!(
                elapsed >= u128::from(timeout),
                "native timeout returned early: {response}"
            );
            assert!(
                elapsed < u128::from(timeout.saturating_mul(4).max(400)),
                "native budget multiplied: {response}"
            );
            observer.command("probe-write");
            observer.command("probe-checkpoint");
            holder.command("release-main");
            observer.command("probe-main");
        }
    }
    waiter.command("timeout 0");
    waiter.command("fence append");
    observer.finish();
    waiter.finish();
    holder.finish();
    assert_unchanged(&path);
}

#[test]
fn checkpoint_slot_contention_restores_the_entire_new_prefix() {
    let _scenario = serial_scenario();
    let (_directory, path) = setup();
    let mut stock_order = Actor::spawn(&path, "wal", "stock-order CKPT owner");
    let mut checkpoint = Actor::spawn(&path, "wal", "checkpoint contender");
    stock_order.command("hold-checkpoint");
    checkpoint.command("timeout 30000");
    checkpoint.send("busy-fence checkpoint");
    let response = checkpoint.receive_for(PROMPT_PROGRESS);
    assert!(response.starts_with("busy "));
    stock_order.command("probe-write");
    stock_order.command("probe-main");
    stock_order.command("release-checkpoint");
    checkpoint.command("fence checkpoint");
    checkpoint.finish();
    stock_order.finish();
    assert_unchanged(&path);
}
