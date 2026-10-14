//! Deterministic cache/writeback model over real MemoryVfs byte I/O. These
//! tests are not physical power-loss certificates or public SQL admission.

use std::future::{Future, Ready, pending, poll_fn, ready};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    CommitMarker, CommitSeq, LockLevel, Oti, SymbolRecordFlags,
    reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{MemoryVfs, ShmRegion, Vfs};

use super::*;

type MemoryFile = <MemoryVfs as Vfs>::File;
type Log = NativeDurabilityLog<ModelFile, ModelFile>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Write(usize, u64, usize),
    Sync(usize),
    Close(usize),
    Truncate(usize, u64),
}

#[derive(Default)]
struct Cache {
    live: Vec<u8>,
    stable: Vec<u8>,
    dirty: bool,
}

struct PendingSource {
    stream: usize,
    offset: u64,
    bytes: Vec<u8>,
    completion: VfsWriteCompletion,
}

#[derive(Default)]
struct Control {
    files: [Cache; 2],
    events: Vec<Event>,
    reads: usize,
    fail_sync: Option<usize>,
    hold_write: Option<usize>,
    source: Option<PendingSource>,
    cancel_after_sync: Option<usize>,
}

struct ModelFile {
    inner: MemoryFile,
    stream: usize,
    control: Arc<Mutex<Control>>,
}

impl ModelFile {
    fn cache_write(&self, bytes: &[u8], offset: u64) {
        let mut control = self.control.lock().unwrap();
        let cache = &mut control.files[self.stream];
        let start = usize::try_from(offset).unwrap();
        let end = start.checked_add(bytes.len()).unwrap();
        if cache.live.len() < end { cache.live.resize(end, 0); }
        cache.live[start..end].copy_from_slice(bytes);
        cache.dirty = true;
    }
}

impl VfsFile for ModelFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.control.lock().unwrap().events.push(Event::Close(self.stream));
        self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        self.control.lock().unwrap().reads += 1;
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.control.lock().unwrap().events.push(Event::Write(self.stream, offset, bytes.len()));
        self.inner.write(cx, bytes, offset).await?;
        self.cache_write(bytes, offset);
        Ok(())
    }
    async fn write_tracked<'a>(
        &'a self,
        cx: &'a Cx,
        bytes: &'a [u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> Result<()> {
        let hold = {
            let mut control = self.control.lock().unwrap();
            if control.hold_write == Some(self.stream) {
                control.hold_write = None;
                assert!(control.source.is_none());
                control.events.push(Event::Write(self.stream, offset, bytes.len()));
                control.source = Some(PendingSource {
                    stream: self.stream, offset, bytes: bytes.to_vec(), completion: completion.clone(),
                });
                true
            } else {
                false
            }
        };
        if hold {
            // The Control retains the physical source after this waiter drops.
            return pending::<Result<()>>().await;
        }
        let result = self.write(cx, bytes, offset).await;
        if result.is_ok() { completion.complete_success(); } else { completion.complete_error(); }
        result
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.control.lock().unwrap().events.push(Event::Truncate(self.stream, size));
        self.inner.truncate(cx, size)?;
        let mut control = self.control.lock().unwrap();
        control.files[self.stream].live.resize(usize::try_from(size).unwrap(), 0);
        control.files[self.stream].dirty = true;
        Ok(())
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        {
            let mut control = self.control.lock().unwrap();
            control.events.push(Event::Sync(self.stream));
            if control.fail_sync == Some(self.stream) {
                control.fail_sync = None;
                // fsyncgate: readable cache survives, but is no longer dirty;
                // a subsequent successful fsync alone persists NOTHING.
                control.files[self.stream].dirty = false;
                return Err(std::io::Error::other("injected clean-but-lost writeback").into());
            }
        }
        self.inner.sync(cx, flags)?;
        let cancel = {
            let mut control = self.control.lock().unwrap();
            let cache = &mut control.files[self.stream];
            if cache.dirty {
                cache.stable.clone_from(&cache.live);
                cache.dirty = false;
            }
            let cancel = control.cancel_after_sync == Some(self.stream);
            if cancel { control.cancel_after_sync = None; }
            cancel
        };
        if cancel { cx.cancel(); }
        Ok(())
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> { self.inner.file_size(cx) }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.lock(cx, level) }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.unlock(cx, level) }
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
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> { self.inner.check_reserved_lock(cx) }
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }
    fn shm_barrier(&self) { self.inner.shm_barrier(); }
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }
}

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}

fn memory_file(vfs: &MemoryVfs, cx: &Cx, path: &str) -> MemoryFile {
    vfs.open(cx, Some(Path::new(path)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}

fn fixture(cx: &Cx) -> (Log, Arc<Mutex<Control>>) {
    let vfs = MemoryVfs::new();
    let control = Arc::new(Mutex::new(Control::default()));
    let symbols = ModelFile { inner: memory_file(&vfs, cx, "objects"), stream: 0, control: Arc::clone(&control) };
    let markers = ModelFile { inner: memory_file(&vfs, cx, "markers"), stream: 1, control: Arc::clone(&control) };
    let log = NativeDurabilityLog::create(cx, symbols, markers, NativeDurabilityLimits::default()).unwrap();
    (log, control)
}

fn symbol(byte: u8) -> SymbolRecord {
    let bytes = vec![byte; 32];
    SymbolRecord::new(
        ObjectId::derive_from_canonical_bytes(&bytes),
        Oti { f: 32, al: 1, t: 32, z: 1, n: 1 },
        0, bytes, SymbolRecordFlags::SYSTEMATIC_RUN_START,
    )
}

fn marker() -> CommitMarker {
    CommitMarker::new(CommitSeq::new(1), 100, symbol(1).object_id, symbol(2).object_id, None)
}

fn verify(id: ObjectId, records: Vec<SymbolRecord>) -> Ready<Result<()>> {
    ready(reconstruct_systematic_happy_path(&records)
        .map_err(|error| corrupt(&error.to_string()))
        .and_then(|bytes| {
            if ObjectId::derive_from_canonical_bytes(&bytes) == id { Ok(()) }
            else { Err(corrupt("decoded object identity mismatch")) }
        }))
}

async fn seed(cx: &Cx) -> (Log, Arc<Mutex<Control>>) {
    let (mut log, control) = fixture(cx);
    log.append_symbols(cx, &[symbol(1), symbol(2)]).await.unwrap();
    log.publish(cx, &[marker()], verify).await.unwrap();
    control.lock().unwrap().events.clear();
    (log, control)
}

fn assert_ordered_rewrite(events: &[Event], max_chunk: usize) {
    let barrier_one = events.iter().position(|event| *event == Event::Sync(0)).unwrap();
    let barrier_two = events.iter().position(|event| *event == Event::Sync(1)).unwrap();
    assert!(barrier_one < barrier_two);
    assert_eq!(barrier_two, events.len() - 1);
    assert!(events[..barrier_one].iter().all(|event| matches!(event, Event::Write(0, _, len) if *len <= max_chunk)));
    assert!(events[barrier_one + 1..barrier_two].iter().all(|event| matches!(event, Event::Write(1, _, len) if *len <= max_chunk)));
}

#[test]
fn failed_marker_writeback_requires_rewrite_not_a_successful_sync_retry() {
    run(async {
        let cx = Cx::new();
        let (mut log, control) = fixture(&cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)]).await.unwrap();
        control.lock().unwrap().fail_sync = Some(1);
        assert!(log.publish(&cx, &[marker()], verify).await.is_err());
        assert!(log.needs_recovery());
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        // Negative control: a successful second sync does not repair the loss.
        log.markers.durable_sync(&cx, SyncKind::FullDurable).unwrap();
        {
            let mut state = control.lock().unwrap();
            assert!(state.files[1].stable.is_empty());
            assert!(!state.files[1].live.is_empty());
            assert!(!state.files[1].dirty);
            state.events.clear();
        }
        let mut recovery = log.into_recovery();
        recovery.prepare(&cx, verify).await.unwrap();
        {
            let state = control.lock().unwrap();
            assert_ordered_rewrite(&state.events, REWRITE_CHUNK_BYTES);
            for cache in &state.files { assert_eq!(cache.stable, cache.live); }
        }
        let handoff = recovery.finish().unwrap();
        assert_eq!(handoff.report().markers, vec![marker()]);
        let (mut log, _) = handoff.into_log();
        assert!(!log.needs_recovery());
        assert_eq!(log.published_tip(), CommitSeq::new(1));
        log.append_symbols(&cx, &[symbol(3), symbol(4)]).await.unwrap();
        let second = CommitMarker::new(
            CommitSeq::new(2), 101, symbol(3).object_id, symbol(4).object_id,
            Some(ObjectId::derive_from_canonical_bytes(&marker().to_record_bytes())),
        );
        log.publish(&cx, &[second], verify).await.unwrap();
        assert_eq!(log.published_tip(), CommitSeq::new(2));
        log.close(&cx).unwrap();
    });
}

#[test]
fn failed_symbol_barrier_never_rewrites_markers_and_retry_redirties_again() {
    run(async {
        let cx = Cx::new();
        let (log, control) = seed(&cx).await;
        let mut recovery = log.into_recovery();
        control.lock().unwrap().fail_sync = Some(0);
        assert!(recovery.prepare(&cx, verify).await.is_err());
        let mut recovery = recovery.finish().unwrap_err();
        {
            let mut state = control.lock().unwrap();
            assert!(state.events.iter().any(|event| matches!(event, Event::Write(0, _, _))));
            assert_eq!(state.events.last(), Some(&Event::Sync(0)));
            assert!(!state.events.iter().any(|event| matches!(event, Event::Write(1, _, _) | Event::Sync(1))));
            state.events.clear();
        }
        recovery.prepare(&cx, verify).await.unwrap();
        assert_ordered_rewrite(&control.lock().unwrap().events, REWRITE_CHUNK_BYTES);
        let (mut log, _) = recovery.finish().unwrap().into_log();
        log.close(&cx).unwrap();
    });
}

#[test]
fn dropped_overwrite_retains_completion_and_original_bytes_for_both_streams() {
    run(async {
        for stream in [0, 1] {
            let cx = Cx::new();
            let (log, control) = seed(&cx).await;
            let expected = {
                let state = control.lock().unwrap();
                [state.files[0].live.clone(), state.files[1].live.clone()]
            };
            let mut recovery = log.into_recovery();
            control.lock().unwrap().hold_write = Some(stream);
            {
                let mut attempt = std::pin::pin!(recovery.prepare(&cx, verify));
                poll_fn(|task_cx| {
                    assert!(attempt.as_mut().poll(task_cx).is_pending());
                    Poll::Ready(())
                }).await;
            }
            let completion = recovery.outstanding_write().unwrap();
            assert_eq!(completion.state(), VfsWriteCompletionState::Pending);
            let before = {
                let state = control.lock().unwrap(); (state.events.clone(), state.reads)
            };
            assert!(matches!(recovery.prepare(&cx, verify).await, Err(FrankenError::BusyRecovery)));
            assert!(matches!(recovery.close(&cx), Err(FrankenError::BusyRecovery)));
            let mut recovery = recovery.finish().unwrap_err();
            {
                let state = control.lock().unwrap();
                assert_eq!(state.events, before.0);
                assert_eq!(state.reads, before.1);
            }
            // The original source now settles with a damaged overwrite. This
            // makes rescanning before restoring the retained chunk fail.
            let source = control.lock().unwrap().source.take().unwrap();
            assert_eq!(source.stream, stream);
            let file = if stream == 0 { &recovery.log.symbols } else { &recovery.log.markers };
            let damaged = vec![0xFF; source.bytes.len()];
            file.inner.write(&cx, &damaged, source.offset).await.unwrap();
            file.cache_write(&damaged, source.offset);
            source.completion.complete_error();
            assert_eq!(completion.state(), VfsWriteCompletionState::Error);
            assert!(matches!(recovery.close(&cx), Err(FrankenError::BusyRecovery)));
            recovery.prepare(&cx, verify).await.unwrap();
            let (mut log, report) = recovery.finish().unwrap().into_log();
            assert_eq!(report.markers, vec![marker()]);
            {
                let state = control.lock().unwrap();
                for (cache, bytes) in state.files.iter().zip(&expected) {
                    assert_eq!(&cache.live, bytes);
                    assert_eq!(&cache.stable, bytes);
                }
            }
            log.close(&cx).unwrap();
        }
    });
}

#[test]
fn invalid_referent_and_complete_corrupt_marker_do_no_recovery_writes() {
    run(async {
        for corrupt_marker in [false, true] {
            let cx = Cx::new();
            let (log, control) = seed(&cx).await;
            if corrupt_marker { log.markers.write(&cx, &[0xFF], 0).await.unwrap(); }
            control.lock().unwrap().events.clear();
            let mut recovery = log.into_recovery();
            let result = if corrupt_marker {
                recovery.prepare(&cx, verify).await
            } else {
                recovery.prepare(&cx, |_, _| ready(Err(FrankenError::Abort))).await
            };
            assert!(result.is_err());
            assert!(control.lock().unwrap().events.is_empty());
            let mut recovery = recovery.finish().unwrap_err();
            recovery.close(&cx).unwrap();
        }
    });
}

#[test]
fn torn_tails_survive_bounded_rewrites_and_still_block_appends() {
    run(async {
        let cx = Cx::new();
        let (mut log, control) = seed(&cx).await;
        let symbol_end = log.symbols.file_size(&cx).unwrap();
        log.symbols.write(&cx, &symbol(3).to_bytes()[..60], symbol_end).await.unwrap();
        let marker_end = log.markers.file_size(&cx).unwrap();
        log.markers.write(&cx, &[0xAA; 17], marker_end).await.unwrap();
        log.limits.max_batch_bytes = 128;
        let expected = {
            let mut state = control.lock().unwrap();
            state.events.clear();
            [state.files[0].live.clone(), state.files[1].live.clone()]
        };
        let mut recovery = log.into_recovery();
        recovery.prepare(&cx, verify).await.unwrap();
        let handoff = recovery.finish().unwrap();
        assert_eq!(handoff.report().symbol_tail_bytes, 60);
        assert_eq!(handoff.report().marker_tail_bytes, 17);
        let (mut log, report) = handoff.into_log();
        assert_eq!(report.markers.len(), 1);
        assert!(report.append_blocked());
        assert!(log.needs_recovery());
        verify(symbol(1).object_id, log.read_object(&cx, symbol(1).object_id).await.unwrap()).await.unwrap();
        assert!(log.append_symbols(&cx, &[symbol(3)]).await.is_err());
        assert!(log.publish(&cx, &[], verify).await.is_err());
        {
            let state = control.lock().unwrap();
            assert_ordered_rewrite(&state.events, 128);
            assert!(state.events.iter().filter(|event| matches!(event, Event::Write(0, _, _))).count() > 1);
            for (cache, bytes) in state.files.iter().zip(&expected) {
                assert_eq!(&cache.live, bytes);
                assert_eq!(&cache.stable, bytes);
            }
        }
        log.close(&cx).unwrap();
    });
}

#[test]
fn cancellation_during_validation_has_no_writes_but_final_barrier_is_not_revoked() {
    run(async {
        let cx = Cx::new();
        let (log, control) = seed(&cx).await;
        let mut recovery = log.into_recovery();
        assert!(matches!(recovery.prepare(&cx, |id, records| {
            cx.cancel();
            verify(id, records)
        }).await, Err(FrankenError::Interrupt)));
        assert!(control.lock().unwrap().events.is_empty());
        let mut recovery = recovery.finish().unwrap_err();
        let fresh = Cx::new();
        control.lock().unwrap().cancel_after_sync = Some(1);
        recovery.prepare(&fresh, verify).await.unwrap();
        assert!(fresh.is_cancel_requested());
        let (mut log, report) = recovery.finish().unwrap().into_log();
        assert_eq!(report.markers.len(), 1);
        log.close(&Cx::new()).unwrap();
    });
}

#[test]
fn new_recovery_owner_handles_empty_streams_without_manufacturing_records() {
    run(async {
        let cx = Cx::new();
        let (log, control) = fixture(&cx);
        let mut recovery = NativeRecovery::new(&cx, log.symbols, log.markers, log.limits).unwrap();
        recovery.prepare(&cx, |_, _| -> Ready<Result<()>> { panic!("empty stream has no object") }).await.unwrap();
        assert_eq!(control.lock().unwrap().events, vec![Event::Sync(0), Event::Sync(1)]);
        let (mut log, report) = recovery.finish().unwrap().into_log();
        assert!(report.markers.is_empty());
        assert_eq!(report.symbol_records, 0);
        assert!(!report.append_blocked());
        log.close(&cx).unwrap();
    });
}

#[test]
fn reopened_recovery_obeys_record_and_stream_limits_before_mutation() {
    run(async {
        for limit_records in [false, true] {
            let cx = Cx::new();
            let (mut log, control) = seed(&cx).await;
            if limit_records { log.limits.max_records = 1; }
            else { log.limits.max_stream_bytes = 1; }
            let mut recovery = NativeRecovery::new(&cx, log.symbols, log.markers, log.limits).unwrap();
            assert!(matches!(recovery.prepare(&cx, verify).await, Err(FrankenError::TooBig)));
            assert!(control.lock().unwrap().events.is_empty());
            let mut recovery = recovery.finish().unwrap_err();
            recovery.close(&cx).unwrap();
        }
    });
}
