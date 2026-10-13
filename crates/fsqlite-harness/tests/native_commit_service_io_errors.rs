//! Reported sync errors while the underlying VFS remains available (GH#508).
//! The inner sync completes before the error is returned. This is an ambiguous
//! result model, not a MemoryVfs or physical power-loss durability certificate.
#![allow(clippy::future_not_send)] // Native publication is a caller-owned !Send future.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use asupersync::Cx as NativeCx;
use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::native_service::NativeCommitService;
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{CommitSeq, LockLevel, PageNumber};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs, VfsFile, VfsWriteCompletion};
use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
use fsqlite_wal::native_durability::{
    NativeDurabilityLimits, NativeDurabilityLog, NativeDurabilityRecovery,
};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore};

const REPORTED_ERROR: &str = "injected error after inner sync success";
const KEY: [u8; 32] = [19; 32];

#[derive(Default)]
struct Audit {
    events: Vec<(&'static str, &'static str)>,
    attempts: usize,
    inner_successes: usize,
    // One-based attempt ordinal across BOTH streams; never reset on reopen.
    error_after_attempt: Option<usize>,
    failures: Vec<(&'static str, usize)>,
}

type Trace = Arc<Mutex<Audit>>;

struct SyncErrorFile {
    inner: <MemoryVfs as Vfs>::File,
    stream: &'static str,
    trace: Trace,
}

impl SyncErrorFile {
    fn record(&self, operation: &'static str) {
        self.trace
            .lock()
            .unwrap()
            .events
            .push((self.stream, operation));
    }

    fn start_sync(&self, api: &'static str) -> usize {
        let mut audit = self.trace.lock().unwrap();
        audit.events.push((self.stream, api));
        audit.attempts += 1;
        audit.attempts
    }

    fn finish_sync(&self, attempt: usize) -> Result<()> {
        let fail = {
            let mut audit = self.trace.lock().unwrap();
            audit.inner_successes += 1;
            if audit.error_after_attempt == Some(attempt) {
                audit.error_after_attempt = None;
                audit.failures.push((self.stream, attempt));
                true
            } else {
                false
            }
        };
        if fail {
            Err(FrankenError::Io(std::io::Error::other(REPORTED_ERROR)))
        } else {
            Ok(())
        }
    }
}

impl VfsFile for SyncErrorFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.file_identity()
    }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, buf: &'a mut [u8], offset: u64) -> Result<usize> {
        self.record("read");
        self.inner.read(cx, buf, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, buf: &'a [u8], offset: u64) -> Result<()> {
        self.record("write");
        self.inner.write(cx, buf, offset).await
    }
    async fn write_tracked<'a>(
        &'a self,
        cx: &'a Cx,
        buf: &'a [u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> Result<()> {
        self.record("write_tracked");
        // Do not replace a source-owned completion with a wrapper-owned token.
        self.inner.write_tracked(cx, buf, offset, completion).await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.record("truncate");
        self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        let attempt = self.start_sync("sync");
        self.inner.sync(cx, flags)?;
        self.finish_sync(attempt)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        let attempt = self.start_sync("durable_sync");
        self.inner.durable_sync(cx, kind)?;
        self.finish_sync(attempt)
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.record("file_size");
        self.inner.file_size(cx)
    }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }
    fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_wal_append(cx)
    }
    fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> {
        self.inner.owns_external_wal_append_write(cx)
    }
    fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_wal_append_attempt(cx)
    }
    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_shared_snapshot(cx)
    }
    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }
    fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal)
    }
    fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_wal_recovery(cx)
    }
    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
    }
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }
    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }
}

type Store = NativePageStore<SyncErrorFile, SyncErrorFile, RaptorQNativeCodec>;

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .unwrap()
        .block_on(future);
}

fn contexts() -> (Cx, NativeCx) {
    let native = NativeCx::current().expect("caller runtime");
    let cx = Cx::new();
    cx.set_native_cx(native.clone());
    (cx, native)
}

fn page(number: u32) -> PageNumber {
    PageNumber::new(number).unwrap()
}

fn file(
    vfs: &MemoryVfs,
    cx: &Cx,
    stream: &'static str,
    trace: &Trace,
    create: bool,
) -> SyncErrorFile {
    let mut flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
    if create {
        flags |= VfsOpenFlags::CREATE;
    }
    SyncErrorFile {
        inner: vfs.open(cx, Some(Path::new(stream)), flags).unwrap().0,
        stream,
        trace: Arc::clone(trace),
    }
}

fn store(vfs: &MemoryVfs, cx: &Cx, trace: &Trace) -> Store {
    let log = NativeDurabilityLog::create(
        cx,
        file(vfs, cx, "objects", trace, true),
        file(vfs, cx, "markers", trace, true),
        NativeDurabilityLimits::default(),
    )
    .unwrap();
    NativePageStore::new(
        log,
        RaptorQNativeCodec::new(Some(KEY)),
        512,
        NativePageLimits::default(),
    )
    .unwrap()
}

async fn recover(
    vfs: &MemoryVfs,
    cx: &Cx,
    trace: &Trace,
) -> Result<(Store, NativeDurabilityRecovery)> {
    NativePageStore::recover(
        cx,
        file(vfs, cx, "objects", trace, false),
        file(vfs, cx, "markers", trace, false),
        RaptorQNativeCodec::new(Some(KEY)),
        512,
        NativeDurabilityLimits::default(),
        NativePageLimits::default(),
    )
    .await
}

fn arm(trace: &Trace, additional_attempts: usize) -> usize {
    assert!(additional_attempts > 0);
    let mut audit = trace.lock().unwrap();
    assert!(audit.error_after_attempt.is_none());
    let attempt = audit.attempts.checked_add(additional_attempts).unwrap();
    audit.error_after_attempt = Some(attempt);
    attempt
}

fn assert_reported_error(error: &FrankenError) {
    match error {
        FrankenError::Io(error) => assert_eq!(error.to_string(), REPORTED_ERROR),
        other => panic!("expected the injected sync error, got {other:?}"),
    }
}

#[test]
fn reported_error_is_one_shot_and_does_not_disable_either_sync_api_or_data_io() {
    run(async {
        for durable in [false, true] {
            let (cx, _) = contexts();
            let vfs = MemoryVfs::new();
            let trace = Trace::default();
            let mut object = file(&vfs, &cx, "objects", &trace, true);
            object.write(&cx, &[1, 2, 3], 0).await.unwrap();
            arm(&trace, 1);
            let result = if durable {
                object.durable_sync(&cx, SyncKind::FullDurable)
            } else {
                object.sync(&cx, SyncFlags::FULL)
            };
            assert_reported_error(&result.unwrap_err());
            assert_eq!(trace.lock().unwrap().inner_successes, 1);
            let mut bytes = [0; 3];
            assert_eq!(object.read(&cx, &mut bytes, 0).await.unwrap(), 3);
            assert_eq!(bytes, [1, 2, 3]);
            object.write(&cx, &[4, 5, 6], 0).await.unwrap();
            object.durable_sync(&cx, SyncKind::FullDurable).unwrap();
            object.sync(&cx, SyncFlags::FULL).unwrap();
            assert_eq!(object.read(&cx, &mut bytes, 0).await.unwrap(), 3);
            assert_eq!(bytes, [4, 5, 6]);
            let audit = trace.lock().unwrap();
            assert_eq!(audit.attempts, 3);
            assert_eq!(audit.inner_successes, 3);
            assert_eq!(audit.failures, [("objects", 1)]);
            drop(audit);
            object.close(&cx).unwrap();
        }
    });
}

#[derive(Clone, Copy)]
struct Scenario {
    pages: &'static [u32],
    batch: usize,
    committed: usize,
    uncertain: usize,
    prior_syncs: usize,
}

const FIRST: Scenario = Scenario {
    pages: &[2, 3, 4],
    batch: 2,
    committed: 0,
    uncertain: 2,
    prior_syncs: 0,
};
const LATER: Scenario = Scenario {
    pages: &[2, 3, 4, 5, 6],
    batch: 2,
    committed: 2,
    uncertain: 2,
    prior_syncs: 2,
};
const SPLIT_FIRST: Scenario = Scenario {
    pages: &[2, 2, 3, 4],
    batch: 3,
    committed: 0,
    uncertain: 1,
    prior_syncs: 0,
};
const SPLIT_LATER: Scenario = Scenario {
    pages: &[2, 3, 3, 4, 5],
    batch: 4,
    committed: 1,
    uncertain: 1,
    prior_syncs: 2,
};

#[test]
fn reported_objects_error_stops_first_group() {
    run(check_service(FIRST, false));
}

#[test]
fn reported_markers_error_stops_first_group() {
    run(check_service(FIRST, true));
}

#[test]
fn reported_objects_error_preserves_prior_group() {
    run(check_service(LATER, false));
}

#[test]
fn reported_markers_error_preserves_prior_group() {
    run(check_service(LATER, true));
}

#[test]
fn reported_objects_error_stops_first_split_member() {
    run(check_service(SPLIT_FIRST, false));
}

#[test]
fn reported_markers_error_stops_first_split_member() {
    run(check_service(SPLIT_FIRST, true));
}

#[test]
fn reported_objects_error_preserves_prior_split_member() {
    run(check_service(SPLIT_LATER, false));
}

#[test]
fn reported_markers_error_preserves_prior_split_member() {
    run(check_service(SPLIT_LATER, true));
}

async fn check_service(case: Scenario, marker_barrier: bool) {
    let (cx, native) = contexts();
    let vfs = MemoryVfs::new();
    let trace = Trace::default();
    let owner = NativePager::new(store(&vfs, &cx, &trace)).unwrap();
    let (sender, worker) = NativeCommitService::new(&owner, case.pages.len(), case.batch).unwrap();
    let mut tickets = Vec::new();
    for (index, &number) in case.pages.iter().enumerate() {
        let mut txn = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        txn.savepoint(&cx, "private").unwrap();
        txn.write_page(&cx, page(number), &[u8::try_from(index + 2).unwrap(); 512])
            .await
            .unwrap();
        tickets.push(
            sender
                .reserve(&native)
                .await
                .unwrap()
                .send(txn, 100)
                .unwrap(),
        );
    }
    let target = arm(
        &trace,
        case.prior_syncs + if marker_barrier { 2 } else { 1 },
    );
    assert!(matches!(
        worker.run(&cx).await,
        Err(FrankenError::BusyRecovery)
    ));
    let failed_stream = if marker_barrier { "markers" } else { "objects" };
    let stopped = {
        let audit = trace.lock().unwrap();
        assert_eq!(audit.attempts, target, "no retry or later group may run");
        assert_eq!(
            audit.inner_successes, target,
            "the inner sync really returned success"
        );
        assert_eq!(audit.failures, [(failed_stream, target)]);
        assert_eq!(audit.events.last(), Some(&(failed_stream, "durable_sync")));
        audit.events.clone()
    };
    assert!(sender.try_reserve(&native).is_err());
    assert!(owner.needs_recovery().unwrap());
    assert_eq!(
        owner.committed_tip().unwrap().get(),
        u64::try_from(case.committed).unwrap()
    );
    let mut returned = Vec::new();
    for mut ticket in tickets {
        returned.push(ticket.wait(&native).await.unwrap());
    }
    assert_eq!(returned.len(), case.pages.len());
    let attempted_end = case.committed + case.uncertain;
    for (index, completion) in returned.iter_mut().enumerate() {
        let txn = &mut completion.transaction;
        if index < case.committed {
            completion.result.as_ref().unwrap();
            assert_eq!(txn.pager_commit_state(), PagerCommitState::Committed);
            assert_eq!(
                txn.acknowledgement().unwrap().commit_seq.get(),
                u64::try_from(index + 1).unwrap()
            );
            continue;
        }
        assert_eq!(txn.pending_commit_pages().unwrap(), [page(case.pages[index])]);
        assert!(txn.acknowledgement().is_none());
        if index < attempted_end {
            assert_reported_error(completion.result.as_ref().unwrap_err().as_ref());
            assert_eq!(txn.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(matches!(
                txn.settle_commit(&cx).await,
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(
                txn.commit_at(&cx, 101).await,
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(
                txn.rollback(&cx).await,
                Err(FrankenError::BusyRecovery)
            ));
            assert!(matches!(
                txn.rollback_to_savepoint(&cx, "private"),
                Err(FrankenError::BusyRecovery)
            ));
        } else {
            assert!(matches!(
                completion.result.as_ref().unwrap_err().as_ref(),
                FrankenError::BusyRecovery
            ));
            assert_eq!(txn.pager_commit_state(), PagerCommitState::NotCommitted);
            txn.rollback_to_savepoint(&cx, "private").unwrap();
            assert!(!txn.has_pending_writes());
            txn.rollback(&cx).await.unwrap();
        }
    }
    // There is deliberately no power_on/reset call: usable storage does not
    // authorize retrying an uncertain intent or admitting new work to this owner.
    assert!(matches!(
        owner.begin(&cx, TransactionMode::Concurrent),
        Err(FrankenError::BusyRecovery)
    ));
    assert!(matches!(owner.close(&cx), Err(FrankenError::Busy)));
    drop(returned);
    owner.close(&cx).unwrap();
    assert_eq!(
        trace.lock().unwrap().events,
        stopped,
        "local cleanup must not retry I/O"
    );

    let recovered_count = case.committed
        + if marker_barrier {
            case.uncertain
        } else {
            0
        };
    let (mut recovered, report) = recover(&vfs, &cx, &trace).await.unwrap();
    assert_eq!(trace.lock().unwrap().attempts, target + 2);
    assert_eq!(report.markers.len(), recovered_count);
    assert_eq!(
        recovered.committed_tip().get(),
        u64::try_from(recovered_count).unwrap()
    );
    let mut view = recovered.begin(&cx).unwrap();
    for &number in case.pages {
        let actual = recovered.read_page(&cx, &mut view, page(number)).unwrap();
        let expected = case.pages[..recovered_count]
            .iter()
            .position(|&p| p == number)
            .map(|index| vec![u8::try_from(index + 2).unwrap(); 512]);
        assert_eq!(
            actual.as_deref(),
            expected.as_deref(),
            "never replay an unattempted overlapping writer"
        );
    }
    recovered.rollback(&mut view).unwrap();
    resume_service(&vfs, &cx, &native, &trace, recovered, recovered_count).await;
}

async fn resume_service(
    vfs: &MemoryVfs,
    cx: &Cx,
    native: &NativeCx,
    trace: &Trace,
    recovered: Store,
    previous_commits: usize,
) {
    let before = trace.lock().unwrap().attempts;
    let owner = NativePager::new(recovered).unwrap();
    let mut next = owner.begin(cx, TransactionMode::Concurrent).unwrap();
    next.write_page(cx, page(100), &[91; 512]).await.unwrap();
    let (sender, worker) = NativeCommitService::new(&owner, 1, 1).unwrap();
    let mut ticket = sender
        .reserve(native)
        .await
        .unwrap()
        .send(next, 1000)
        .unwrap();
    sender.request_shutdown();
    worker.run(cx).await.unwrap();
    let completed = ticket.wait(native).await.unwrap();
    completed.result.unwrap();
    let next_seq = CommitSeq::new(u64::try_from(previous_commits + 1).unwrap());
    assert_eq!(
        completed.transaction.acknowledgement().unwrap().commit_seq,
        next_seq
    );
    assert_eq!(trace.lock().unwrap().attempts, before + 2);
    owner.close(cx).unwrap();
    let (mut reopened, report) = recover(vfs, cx, trace).await.unwrap();
    assert_eq!(trace.lock().unwrap().attempts, before + 4);
    assert_eq!(report.markers.len(), previous_commits + 1);
    assert_eq!(reopened.committed_tip(), next_seq);
    let mut view = reopened.begin(cx).unwrap();
    assert_eq!(
        reopened.read_page(cx, &mut view, page(100)).unwrap().as_deref(),
        Some(&[91_u8; 512][..])
    );
    reopened.rollback(&mut view).unwrap();
    reopened.close(cx).unwrap();
}

#[test]
fn recovery_objects_sync_error_returns_no_owner_and_can_be_retried() {
    run(check_recovery(false));
}

#[test]
fn recovery_markers_sync_error_returns_no_owner_and_can_be_retried() {
    run(check_recovery(true));
}

async fn check_recovery(marker_barrier: bool) {
    let (cx, native) = contexts();
    let vfs = MemoryVfs::new();
    let trace = Trace::default();
    let owner = NativePager::new(store(&vfs, &cx, &trace)).unwrap();
    let mut seed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
    seed.write_page(&cx, page(2), &[7; 512]).await.unwrap();
    seed.commit_at(&cx, 100).await.unwrap();
    owner.close(&cx).unwrap();
    let before = trace.lock().unwrap().events.len();
    let target = arm(&trace, if marker_barrier { 2 } else { 1 });
    match recover(&vfs, &cx, &trace).await {
        Ok(_) => panic!("a failed recovery barrier must not return an admitted store"),
        Err(error) => assert_reported_error(&error),
    }
    let failed_stream = if marker_barrier { "markers" } else { "objects" };
    {
        let audit = trace.lock().unwrap();
        assert_eq!(audit.attempts, target);
        assert_eq!(audit.inner_successes, target);
        assert_eq!(audit.failures, [(failed_stream, target)]);
        assert_eq!(audit.events.last(), Some(&(failed_stream, "durable_sync")));
        assert!(
            audit.events[before..]
                .iter()
                .all(|(_, operation)| !matches!(*operation, "write" | "write_tracked" | "truncate"))
        );
    }
    // A new recovery attempt verifies the retained streams again. It does not
    // retry an old transaction and cannot reuse a partially returned owner.
    let (mut recovered, report) = recover(&vfs, &cx, &trace).await.unwrap();
    assert_eq!(trace.lock().unwrap().attempts, target + 2);
    assert_eq!(report.markers.len(), 1);
    let mut view = recovered.begin(&cx).unwrap();
    assert_eq!(
        recovered.read_page(&cx, &mut view, page(2)).unwrap().as_deref(),
        Some(&[7_u8; 512][..])
    );
    recovered.rollback(&mut view).unwrap();
    resume_service(&vfs, &cx, &native, &trace, recovered, 1).await;
}
