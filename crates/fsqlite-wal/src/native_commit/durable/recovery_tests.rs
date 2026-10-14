//! Recovery handoff through actual MemoryVfs reads, not a synthetic report.
//! The mutation gate remains armed across every semantic-replay boundary.

use std::future::{Future, pending, poll_fn, ready};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{LockLevel, Oti, PageNumber, SymbolRecordFlags, reconstruct_systematic_happy_path};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs};

use super::*;
use crate::native_durability::NativeRecovery;
use crate::native_pages::{
    NativePageCapsule, NativePageLimits, NativePageStore, NativePageTransactionState,
    NativePageWrite,
};

type MemoryFile = <MemoryVfs as Vfs>::File;
type Log = NativeDurabilityLog<AuditedFile, AuditedFile>;
type Driver = DurableWriteCoordinator<AuditedFile, AuditedFile, Codec>;

#[derive(Default)]
struct Control {
    forbid_mutation: AtomicBool,
    mutations: AtomicUsize,
    reads: AtomicUsize,
    hold_reads: AtomicBool,
}

impl Control {
    fn mutation(&self) -> Result<()> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        if self.forbid_mutation.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("mutation during semantic recovery handoff").into());
        }
        Ok(())
    }

    fn freeze(&self) -> usize {
        self.forbid_mutation.store(true, Ordering::SeqCst);
        self.mutations.load(Ordering::SeqCst)
    }

    fn assert_frozen(&self, before: usize) {
        assert_eq!(self.mutations.load(Ordering::SeqCst), before);
    }
}

struct AuditedFile {
    inner: MemoryFile,
    control: Arc<Control>,
}

impl VfsFile for AuditedFile {
    fn close(&mut self, cx: &Cx) -> Result<()> { self.inner.close(cx) }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        self.control.reads.fetch_add(1, Ordering::SeqCst);
        if self.control.hold_reads.load(Ordering::SeqCst) {
            return pending().await;
        }
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.control.mutation()?;
        self.inner.write(cx, bytes, offset).await
    }
    async fn write_tracked<'a>(
        &'a self,
        cx: &'a Cx,
        bytes: &'a [u8],
        offset: u64,
        completion: VfsWriteCompletion,
    ) -> Result<()> {
        if let Err(error) = self.control.mutation() {
            completion.complete_error();
            return Err(error);
        }
        self.inner.write_tracked(cx, bytes, offset, completion).await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.control.mutation()?;
        self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.control.mutation()?;
        self.inner.sync(cx, flags)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        self.control.mutation()?;
        self.inner.durable_sync(cx, kind)
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

struct Codec;
impl NativeObjectCodec for Codec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
            0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records)
            .map_err(|error| corrupt(&error.to_string()))?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(corrupt("handoff test object identity mismatch"));
        }
        Ok(bytes)
    }
}

fn run(future: impl Future<Output = ()>) {
    let _guard = crate::metrics::GLOBAL_GROUP_COMMIT_METRICS_TEST_LOCK.lock().unwrap();
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}

fn raw_file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> MemoryFile {
    vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}

fn file(vfs: &MemoryVfs, cx: &Cx, name: &str, control: &Arc<Control>) -> AuditedFile {
    AuditedFile { inner: raw_file(vfs, cx, name), control: Arc::clone(control) }
}

fn fixture(cx: &Cx) -> (MemoryVfs, Arc<Control>, Log) {
    let vfs = MemoryVfs::new();
    let control = Arc::new(Control::default());
    let log = Log::create(cx, file(&vfs, cx, "objects", &control),
        file(&vfs, cx, "markers", &control), NativeDurabilityLimits::default()).unwrap();
    (vfs, control, log)
}

fn submission(seed: u8, snapshot: u64) -> CommitSubmission {
    let evidence = ObjectId::derive_from_canonical_bytes(&[seed + 64; 8]);
    CommitSubmission {
        capsule_object_id: ObjectId::derive_from_canonical_bytes(&[seed; 16]),
        capsule_digest: *blake3::hash(&[seed; 16]).as_bytes(),
        write_set_pages: vec![PageNumber::new(2).unwrap()],
        witness_refs: vec![evidence], edge_ids: vec![evidence], merge_witness_ids: vec![evidence],
        txn_token: TxnToken::new(TxnId::new(u64::from(seed)).unwrap(), TxnEpoch::new(1)),
        begin_seq: CommitSeq::new(snapshot),
    }
}

#[derive(Clone, Copy)]
enum Shape { Valid, WrongBinding, WrongDigest, MissingEvidence }

async fn seed(log: &mut Log, cx: &Cx, count: u8, shape: Shape) {
    let mut previous = None;
    for n in 1..=count {
        let mut proof = NativeCommitProof {
            commit_seq: CommitSeq::new(u64::from(n)),
            commit_time_unix_ns: u64::from(n) * 100,
            submission: submission(n, u64::from(n - 1)),
        };
        if matches!(shape, Shape::WrongBinding) { proof.commit_time_unix_ns += 1; }
        if matches!(shape, Shape::WrongDigest) { proof.submission.capsule_digest = [0; 32]; }
        let capsule = Codec.encode(cx, &[n; 16]).unwrap();
        let records = Codec.encode(cx, &proof.to_bytes().unwrap()).unwrap();
        log.append_symbols(cx, &capsule).await.unwrap();
        log.append_symbols(cx, &records).await.unwrap();
        if !matches!(shape, Shape::MissingEvidence) {
            log.append_symbols(cx, &Codec.encode(cx, &[n + 64; 8]).unwrap()).await.unwrap();
        }
        let marker = CommitMarker::new(proof.commit_seq, u64::from(n) * 100,
            capsule[0].object_id, records[0].object_id, previous);
        previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
        // The low-level log checks object integrity, not the higher-level
        // proof binding or transitive evidence that the handoff must enforce.
        log.publish(cx, &[marker], |id, records| ready(Codec.decode(cx, id, &records).map(|_| ())))
            .await.unwrap().unwrap();
    }
}

async fn prepared(log: Log, cx: &Cx) -> RecoveredNativeLog<AuditedFile, AuditedFile> {
    let mut recovery = log.into_recovery();
    recovery.prepare(cx, |id, records| ready(Codec.decode(cx, id, &records).map(|_| ())))
        .await.unwrap();
    recovery.finish().unwrap()
}

async fn bytes(vfs: &MemoryVfs, cx: &Cx, name: &str) -> Vec<u8> {
    let mut file = raw_file(vfs, cx, name);
    let mut bytes = vec![0; usize::try_from(file.file_size(cx).unwrap()).unwrap()];
    assert_eq!(file.read(cx, &mut bytes, 0).await.unwrap(), bytes.len());
    file.close(cx).unwrap();
    bytes
}

#[test]
fn recovered_handoff_restores_conflicts_clock_and_next_commit_without_mutation() {
    run(async {
        let cx = Cx::new();
        let (vfs, control, mut log) = fixture(&cx);
        seed(&mut log, &cx, 2, Shape::Valid).await;
        let recovered = prepared(log, &cx).await;
        let before = control.freeze();
        let original = bytes(&vfs, &cx, "markers").await;
        let mut seen = Vec::new();
        let (mut driver, report) = Driver::from_recovered(&cx, recovered, Codec, 8, |candidate, objects| {
            seen.push(candidate.proof.commit_seq.get());
            let seed = u8::try_from(candidate.proof.commit_seq.get()).unwrap();
            assert_eq!(candidate.capsule.as_ref(), &[seed; 16]);
            assert_eq!(objects.len(), 2, "repeated evidence refs must resolve once");
            assert_eq!(objects[&candidate.proof.submission.witness_refs[0]].as_ref(), &[seed + 64; 8]);
            Ok(())
        }).await.unwrap();
        assert_eq!(seen, vec![1, 2]);
        assert_eq!(report.markers.len(), 2);
        assert_eq!(driver.committed_tip(), CommitSeq::new(2));
        assert_eq!(driver.pending_count(), 0);
        assert!(driver.take_committed(CommitSeq::new(1)).is_none());
        assert!(driver.take_committed(CommitSeq::new(2)).is_none());
        assert!(!driver.needs_recovery());
        assert!(driver.outstanding_write().is_none());
        assert!(matches!(driver.queue(&cx, submission(3, 0), 7),
            Err(DurableCommitError::Rejected(CommitResult::ConflictFcw { .. }))));
        let next = driver.queue(&cx, submission(3, 2), 7).unwrap();
        assert_eq!(next, CommitSeq::new(3));
        assert_eq!(bytes(&vfs, &cx, "markers").await, original);
        control.assert_frozen(before);
        control.forbid_mutation.store(false, Ordering::SeqCst);
        driver.stage_symbols(&cx, &Codec.encode(&cx, &[3; 16]).unwrap()).await.unwrap();
        driver.stage_symbols(&cx, &Codec.encode(&cx, &[67; 8]).unwrap()).await.unwrap();
        driver.flush(&cx, |candidates, _| {
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].proof.commit_time_unix_ns, 201);
            Ok(())
        }).await.unwrap().unwrap();
        assert_eq!(driver.take_committed(next).unwrap().commit_time_unix_ns, 201);
        driver.close(&cx).unwrap();
        let (mut reopened, report) = Driver::recover(&cx, file(&vfs, &cx, "objects", &control),
            file(&vfs, &cx, "markers", &control), NativeDurabilityLimits::default(), Codec, 8,
            |_, _| Ok(())).await.unwrap();
        assert_eq!(report.markers.len(), 3);
        assert_eq!(reopened.committed_tip(), next);
        reopened.close(&cx).unwrap();
    });
}

#[test]
fn recovered_handoff_refuses_unbound_proofs_digests_and_missing_evidence() {
    run(async {
        for shape in [Shape::WrongBinding, Shape::WrongDigest, Shape::MissingEvidence] {
            let cx = Cx::new();
            let (vfs, control, mut log) = fixture(&cx);
            seed(&mut log, &cx, 1, shape).await;
            let recovered = prepared(log, &cx).await;
            let original = (bytes(&vfs, &cx, "objects").await, bytes(&vfs, &cx, "markers").await);
            let before = control.freeze();
            let result = Driver::from_recovered(&cx, recovered, Codec, 8,
                |_, _| panic!("invalid bound objects must never reach semantic admission")).await;
            assert!(matches!(result, Err(FrankenError::WalCorrupt { .. })));
            control.assert_frozen(before);
            assert_eq!(bytes(&vfs, &cx, "objects").await, original.0);
            assert_eq!(bytes(&vfs, &cx, "markers").await, original.1);
        }
    });
}

#[test]
fn recovered_handoff_semantic_refusal_never_returns_a_partial_coordinator() {
    run(async {
        let cx = Cx::new();
        let (_, control, mut log) = fixture(&cx);
        seed(&mut log, &cx, 2, Shape::Valid).await;
        let recovered = prepared(log, &cx).await;
        let before = control.freeze();
        let mut seen = Vec::new();
        let result = Driver::from_recovered(&cx, recovered, Codec, 8, |candidate, _| {
            seen.push(candidate.proof.commit_seq.get());
            if seen.len() == 2 { Err(FrankenError::Abort) } else { Ok(()) }
        }).await;
        assert!(matches!(result, Err(FrankenError::Abort)));
        assert_eq!(seen, vec![1, 2]);
        control.assert_frozen(before);
    });
}

#[test]
fn recovered_handoff_retains_torn_tails_and_blocks_new_admission() {
    run(async {
        for (symbol_tail, marker_tail) in [(true, false), (false, true), (true, true)] {
            let cx = Cx::new();
            let (vfs, control, mut log) = fixture(&cx);
            seed(&mut log, &cx, 2, Shape::Valid).await;
            for (path, enabled) in [("objects", symbol_tail), ("markers", marker_tail)] {
                if enabled {
                    let mut raw = raw_file(&vfs, &cx, path);
                    raw.write(&cx, &[0xED; 3], raw.file_size(&cx).unwrap()).await.unwrap();
                    raw.close(&cx).unwrap();
                }
            }
            let recovered = prepared(log, &cx).await;
            let before = control.freeze();
            let (mut driver, report) = Driver::from_recovered(&cx, recovered, Codec, 8,
                |_, _| Ok(())).await.unwrap();
            assert_eq!(report.symbol_tail_bytes, if symbol_tail { 3 } else { 0 });
            assert_eq!(report.marker_tail_bytes, if marker_tail { 3 } else { 0 });
            assert_eq!(driver.committed_tip(), CommitSeq::new(2));
            assert!(driver.needs_recovery());
            assert!(matches!(driver.queue(&cx, submission(3, 2), 300),
                Err(DurableCommitError::Failure(FrankenError::BusyRecovery))));
            assert!(matches!(driver.stage_symbols(&cx, &[]).await, Err(FrankenError::BusyRecovery)));
            assert!(matches!(driver.flush(&cx, |_, _| Ok(())).await, Err(FrankenError::BusyRecovery)));
            control.assert_frozen(before);
            driver.close(&cx).unwrap();
        }
    });
}

#[test]
fn recovered_handoff_checks_empty_prefix_bounds_and_final_callback_cancellation() {
    run(async {
        for max_pending in [0, MAX_PENDING_COMMITS + 1] {
            let cx = Cx::new();
            let (_, control, log) = fixture(&cx);
            let recovered = prepared(log, &cx).await;
            let before = control.freeze();
            assert!(matches!(Driver::from_recovered(&cx, recovered, Codec, max_pending,
                |_, _| Ok(())).await, Err(FrankenError::OutOfRange { .. })));
            control.assert_frozen(before);
        }
        for count in [0, 2] {
            let cx = Cx::new();
            let (_, control, mut log) = fixture(&cx);
            seed(&mut log, &cx, count, Shape::Valid).await;
            let recovered = prepared(log, &cx).await;
            let reads = control.reads.load(Ordering::SeqCst);
            let before = control.freeze();
            cx.cancel();
            assert!(matches!(Driver::from_recovered(&cx, recovered, Codec, 8,
                |_, _| Ok(())).await, Err(FrankenError::Interrupt)));
            assert_eq!(control.reads.load(Ordering::SeqCst), reads);
            control.assert_frozen(before);
        }
        let cx = Cx::new();
        let (_, control, mut log) = fixture(&cx);
        seed(&mut log, &cx, 1, Shape::Valid).await;
        let recovered = prepared(log, &cx).await;
        let before = control.freeze();
        assert!(matches!(Driver::from_recovered(&cx, recovered, Codec, 8, |_, _| {
            cx.cancel(); Ok(())
        }).await, Err(FrankenError::Interrupt)));
        control.assert_frozen(before);
        let cx = Cx::new();
        let (_, control, log) = fixture(&cx);
        let recovered = prepared(log, &cx).await;
        let before = control.freeze();
        let (mut empty, report) = Driver::from_recovered(&cx, recovered, Codec, 8,
            |_, _| panic!("empty prefix has no semantic callbacks")).await.unwrap();
        assert!(report.markers.is_empty());
        assert!(!empty.needs_recovery());
        assert_eq!(empty.committed_tip(), CommitSeq::ZERO);
        control.assert_frozen(before);
        empty.close(&cx).unwrap();
    });
}

#[test]
fn dropped_handoff_read_cannot_start_mutation_or_damage_the_recoverable_prefix() {
    run(async {
        let cx = Cx::new();
        let (vfs, control, mut log) = fixture(&cx);
        seed(&mut log, &cx, 2, Shape::Valid).await;
        let recovered = prepared(log, &cx).await;
        let original = (bytes(&vfs, &cx, "objects").await, bytes(&vfs, &cx, "markers").await);
        let before = control.freeze();
        control.hold_reads.store(true, Ordering::SeqCst);
        let mut handoff = Box::pin(Driver::from_recovered(&cx, recovered, Codec, 8, |_, _| Ok(())));
        poll_fn(|task| {
            assert!(handoff.as_mut().poll(task).is_pending());
            Poll::Ready(())
        }).await;
        drop(handoff);
        control.assert_frozen(before);
        assert_eq!(bytes(&vfs, &cx, "objects").await, original.0);
        assert_eq!(bytes(&vfs, &cx, "markers").await, original.1);
        control.hold_reads.store(false, Ordering::SeqCst);
        control.forbid_mutation.store(false, Ordering::SeqCst);
        let mut recovery = NativeRecovery::new(&cx, file(&vfs, &cx, "objects", &control),
            file(&vfs, &cx, "markers", &control), NativeDurabilityLimits::default()).unwrap();
        recovery.prepare(&cx, |id, records| ready(Codec.decode(&cx, id, &records).map(|_| ())))
            .await.unwrap();
        let recovered = recovery.finish().unwrap();
        let before = control.freeze();
        let (mut driver, report) = Driver::from_recovered(&cx, recovered, Codec, 8,
            |_, _| Ok(())).await.unwrap();
        assert_eq!(report.markers.len(), 2);
        assert_eq!(driver.committed_tip(), CommitSeq::new(2));
        control.assert_frozen(before);
        driver.close(&cx).unwrap();
    });
}

type PageStore = NativePageStore<AuditedFile, AuditedFile, Codec>;

fn page(number: u32) -> PageNumber {
    PageNumber::new(number).unwrap()
}

async fn prepare_stored_pages(
    vfs: &MemoryVfs,
    cx: &Cx,
    control: &Arc<Control>,
) -> RecoveredNativeLog<AuditedFile, AuditedFile> {
    // Fixture admission happens before the handoff mutation gate is armed.
    // There are no unsettled old source writes in this MemoryVfs fixture.
    let mut recovery = NativeRecovery::new(
        cx,
        file(vfs, cx, "objects", control),
        file(vfs, cx, "markers", control),
        NativeDurabilityLimits::default(),
    )
    .unwrap();
    recovery
        .prepare(cx, |id, records| {
            ready(Codec.decode(cx, id, &records).map(|_| ()))
        })
        .await
        .unwrap();
    recovery.finish().unwrap()
}

#[test]
fn page_handoff_restores_transactions_tombstones_allocator_and_snapshot_conflicts() {
    run(async {
        let cx = Cx::new();
        let (vfs, control, log) = fixture(&cx);
        let mut source = PageStore::new(log, Codec, 512, NativePageLimits::default()).unwrap();
        let mut earlier = source.begin(&cx).unwrap();
        let mut later = source.begin(&cx).unwrap();
        source.write_page(&cx, &mut earlier, page(2), Some(&[0x11; 512])).unwrap();
        source.write_page(&cx, &mut later, page(90), Some(&[0x22; 512])).unwrap();
        source.commit(&cx, &mut later, 10).await.unwrap();
        source.commit(&cx, &mut earlier, 11).await.unwrap();
        let mut deletion = source.begin(&cx).unwrap();
        source.write_page(&cx, &mut deletion, page(2), Some(&[0x33; 512])).unwrap();
        source.write_page(&cx, &mut deletion, page(90), None).unwrap();
        source.commit(&cx, &mut deletion, 12).await.unwrap();
        let mut foreign = source.begin(&cx).unwrap();
        source.close(&cx).unwrap();
        let recovered = prepare_stored_pages(&vfs, &cx, &control).await;
        let original = (
            bytes(&vfs, &cx, "objects").await,
            bytes(&vfs, &cx, "markers").await,
        );
        let before = control.freeze();
        let (mut db, report) = PageStore::from_recovered(
            &cx, recovered, Codec, 512, NativePageLimits::default(),
        ).await.unwrap();
        assert_eq!(report.markers.len(), 3);
        assert_eq!(db.committed_tip(), CommitSeq::new(3));
        assert_eq!(db.retained_page_bytes(), 512);
        assert_eq!(db.retained_version_count(), 2);
        assert!(db.outstanding_write().is_none());
        assert!(!db.needs_recovery());
        assert!(matches!(db.read_page(&cx, &mut foreign, page(2)), Err(FrankenError::Abort)));
        let mut old = db.begin(&cx).unwrap();
        assert_eq!(old.token().id.get(), 4, "use the greatest committed ID, not commit order");
        assert_eq!(old.snapshot_db_size(), 90, "highest tombstone is still an address");
        assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[0x33; 512]);
        assert!(db.read_page(&cx, &mut old, page(90)).unwrap().is_none());
        let mut writer = db.begin(&cx).unwrap();
        let mut stale = db.begin(&cx).unwrap();
        db.read_page(&cx, &mut stale, page(2)).unwrap();
        db.write_page(&cx, &mut stale, page(3), Some(&[0x55; 512])).unwrap();
        db.write_page(&cx, &mut writer, page(2), Some(&[0x44; 512])).unwrap();
        let mut allocator = db.begin(&cx).unwrap();
        assert_eq!(db.allocate_page(&cx, &mut allocator).unwrap(), page(91));
        db.rollback(&mut allocator).unwrap();
        control.assert_frozen(before);
        assert_eq!(bytes(&vfs, &cx, "objects").await, original.0);
        assert_eq!(bytes(&vfs, &cx, "markers").await, original.1);
        control.forbid_mutation.store(false, Ordering::SeqCst);
        let ack = db.commit(&cx, &mut writer, 0).await.unwrap().unwrap();
        assert_eq!(ack.commit_seq, CommitSeq::new(4));
        assert_eq!(ack.commit_time_unix_ns, 13);
        assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[0x33; 512]);
        assert!(matches!(db.commit(&cx, &mut stale, 14).await, Err(FrankenError::BusySnapshot { .. })));
        assert_eq!(stale.state(), NativePageTransactionState::Active);
        db.rollback(&mut stale).unwrap();
        db.rollback(&mut old).unwrap();
        db.close(&cx).unwrap();
        let recovered = prepare_stored_pages(&vfs, &cx, &control).await;
        let before = control.freeze();
        let (mut db, report) = PageStore::from_recovered(
            &cx, recovered, Codec, 512, NativePageLimits::default(),
        ).await.unwrap();
        assert_eq!(report.markers.len(), 4);
        let mut view = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[0x44; 512]);
        assert!(db.read_page(&cx, &mut view, page(3)).unwrap().is_none());
        assert!(db.read_page(&cx, &mut view, page(90)).unwrap().is_none());
        control.assert_frozen(before);
        db.rollback(&mut view).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn page_handoff_replays_many_versions_with_one_head_and_enforces_real_limits() {
    run(async {
        let cx = Cx::new();
        let (vfs, control, log) = fixture(&cx);
        let mut source = PageStore::new(log, Codec, 512, NativePageLimits::default()).unwrap();
        for value in 1_u8..=32 {
            let mut txn = source.begin(&cx).unwrap();
            source.write_page(&cx, &mut txn, page(7), Some(&[value; 512])).unwrap();
            source.commit(&cx, &mut txn, u64::from(value)).await.unwrap();
        }
        source.close(&cx).unwrap();
        for (page_size, retained, versions) in [(512, 512, 1), (512, 511, 1), (512, 512, 0), (1024, 1024, 1)] {
            control.forbid_mutation.store(false, Ordering::SeqCst);
            let recovered = prepare_stored_pages(&vfs, &cx, &control).await;
            let before = control.freeze();
            let limits = NativePageLimits {
                max_retained_page_bytes: retained,
                max_versions: versions,
                ..NativePageLimits::default()
            };
            let result = PageStore::from_recovered(&cx, recovered, Codec, page_size, limits).await;
            control.assert_frozen(before);
            if page_size == 1024 {
                assert!(matches!(result, Err(FrankenError::WalCorrupt { .. })));
            } else if retained < 512 || versions == 0 {
                assert!(matches!(result, Err(FrankenError::TooBig)));
            } else {
                let (mut db, report) = result.unwrap();
                assert_eq!(report.markers.len(), 32);
                assert_eq!(db.retained_version_count(), 1);
                assert_eq!(db.retained_page_bytes(), 512);
                let mut view = db.begin(&cx).unwrap();
                assert_eq!(view.token().id.get(), 33);
                assert_eq!(db.read_page(&cx, &mut view, page(7)).unwrap().unwrap().as_ref(), &[32; 512]);
                db.rollback(&mut view).unwrap();
                db.close(&cx).unwrap();
            }
        }
    });
}

#[test]
fn page_handoff_rejects_stale_history_duplicate_tokens_and_forged_write_sets() {
    run(async {
        for invalid in 0..3 {
            let cx = Cx::new();
            let (vfs, control, mut log) = fixture(&cx);
            let mut previous = None;
            for seq in 1_u64..=3 {
                let observed = if seq == 3 && invalid == 0 { 1 } else { seq - 1 };
                let token_id = if seq == 3 && invalid == 1 { 1 } else { seq };
                let capsule = NativePageCapsule {
                    page_size: 512,
                    snapshot: CommitSeq::new(seq - 1),
                    reads: vec![(page(2), CommitSeq::new(observed))],
                    writes: vec![NativePageWrite {
                        page: page(2),
                        data: Some(Arc::from(vec![u8::try_from(seq).unwrap(); 512])),
                    }],
                };
                let payload = capsule.to_bytes().unwrap();
                let capsule_records = Codec.encode(&cx, &payload).unwrap();
                let proof = NativeCommitProof {
                    commit_seq: CommitSeq::new(seq),
                    commit_time_unix_ns: seq,
                    submission: CommitSubmission {
                        capsule_object_id: capsule_records[0].object_id,
                        capsule_digest: *blake3::hash(&payload).as_bytes(),
                        write_set_pages: vec![page(if seq == 3 && invalid == 2 { 3 } else { 2 })],
                        witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
                        txn_token: TxnToken::new(TxnId::new(token_id).unwrap(), TxnEpoch::new(1)),
                        begin_seq: capsule.snapshot,
                    },
                };
                let proof_records = Codec.encode(&cx, &proof.to_bytes().unwrap()).unwrap();
                let marker = CommitMarker::new(
                    proof.commit_seq, seq, capsule_records[0].object_id,
                    proof_records[0].object_id, previous,
                );
                previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
                log.append_symbols(&cx, &capsule_records).await.unwrap();
                log.append_symbols(&cx, &proof_records).await.unwrap();
                log.publish(&cx, &[marker], |id, records| {
                    ready(Codec.decode(&cx, id, &records).map(|_| ()))
                }).await.unwrap();
            }
            let recovered = prepared(log, &cx).await;
            let original = (
                bytes(&vfs, &cx, "objects").await,
                bytes(&vfs, &cx, "markers").await,
            );
            let before = control.freeze();
            let result = PageStore::from_recovered(
                &cx, recovered, Codec, 512,
                NativePageLimits {
                    max_retained_page_bytes: 512, max_versions: 1,
                    ..NativePageLimits::default()
                },
            ).await;
            assert!(matches!(result, Err(FrankenError::WalCorrupt { .. })));
            control.assert_frozen(before);
            assert_eq!(bytes(&vfs, &cx, "objects").await, original.0);
            assert_eq!(bytes(&vfs, &cx, "markers").await, original.1);
        }
    });
}

#[test]
fn page_handoff_retains_each_torn_tail_and_never_opens_transaction_admission() {
    run(async {
        for path in ["objects", "markers"] {
            let cx = Cx::new();
            let (vfs, control, log) = fixture(&cx);
            let mut source = PageStore::new(log, Codec, 512, NativePageLimits::default()).unwrap();
            let mut txn = source.begin(&cx).unwrap();
            source.write_page(&cx, &mut txn, page(2), Some(&[7; 512])).unwrap();
            source.commit(&cx, &mut txn, 100).await.unwrap();
            source.close(&cx).unwrap();
            let mut raw = raw_file(&vfs, &cx, path);
            raw.write(&cx, b"tail", raw.file_size(&cx).unwrap()).await.unwrap();
            raw.close(&cx).unwrap();
            let recovered = prepare_stored_pages(&vfs, &cx, &control).await;
            let original = bytes(&vfs, &cx, path).await;
            let before = control.freeze();
            let (mut db, report) = PageStore::from_recovered(
                &cx, recovered, Codec, 512, NativePageLimits::default(),
            ).await.unwrap();
            assert_eq!(report.markers.len(), 1);
            assert_eq!(report.symbol_tail_bytes, if path == "objects" { 4 } else { 0 });
            assert_eq!(report.marker_tail_bytes, if path == "markers" { 4 } else { 0 });
            assert_eq!(db.committed_tip(), CommitSeq::new(1));
            assert!(db.needs_recovery());
            assert!(matches!(db.begin(&cx), Err(FrankenError::BusyRecovery)));
            assert!(matches!(db.commit_batch(&cx, &mut [], 200).await, Err(FrankenError::BusyRecovery)));
            assert!(matches!(db.reclaim_history(&cx, 1), Err(FrankenError::BusyRecovery)));
            control.assert_frozen(before);
            assert_eq!(bytes(&vfs, &cx, path).await, original);
            db.close(&cx).unwrap();
        }
    });
}

#[test]
fn page_handoff_cancellation_and_dropped_reads_leave_no_usable_partial_store() {
    run(async {
        for cancel_before in [true, false] {
            let cx = Cx::new();
            let (vfs, control, log) = fixture(&cx);
            let mut source = PageStore::new(log, Codec, 512, NativePageLimits::default()).unwrap();
            let mut txn = source.begin(&cx).unwrap();
            source.write_page(&cx, &mut txn, page(2), Some(&[0xA5; 512])).unwrap();
            source.commit(&cx, &mut txn, 10).await.unwrap();
            source.close(&cx).unwrap();
            let recovered = prepare_stored_pages(&vfs, &cx, &control).await;
            let original = bytes(&vfs, &cx, "objects").await;
            let before = control.freeze();
            if cancel_before {
                let cancelled = Cx::new();
                cancelled.cancel();
                assert!(matches!(PageStore::from_recovered(
                    &cancelled, recovered, Codec, 512, NativePageLimits::default(),
                ).await, Err(FrankenError::Interrupt)));
            } else {
                control.hold_reads.store(true, Ordering::SeqCst);
                let mut handoff = Box::pin(PageStore::from_recovered(
                    &cx, recovered, Codec, 512, NativePageLimits::default(),
                ));
                poll_fn(|task| {
                    assert!(handoff.as_mut().poll(task).is_pending());
                    Poll::Ready(())
                }).await;
                drop(handoff);
                control.hold_reads.store(false, Ordering::SeqCst);
            }
            control.assert_frozen(before);
            assert_eq!(bytes(&vfs, &cx, "objects").await, original);
            control.forbid_mutation.store(false, Ordering::SeqCst);
            let recovered = prepare_stored_pages(&vfs, &cx, &control).await;
            let before = control.freeze();
            let (mut db, _) = PageStore::from_recovered(
                &cx, recovered, Codec, 512, NativePageLimits::default(),
            ).await.unwrap();
            let mut view = db.begin(&cx).unwrap();
            assert_eq!(db.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[0xA5; 512]);
            control.assert_frozen(before);
            db.rollback(&mut view).unwrap();
            db.close(&cx).unwrap();
        }
    });
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[test]
fn page_handoff_uses_the_real_authenticated_raptorq_codec_without_new_barriers() {
    use super::codec::RaptorQNativeCodec;

    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        for handoff_key in [[7; 32], [8; 32]] {
            let (vfs, control, log) = fixture(&cx);
            let codec = || RaptorQNativeCodec::new(Some([7; 32]));
            let mut source = NativePageStore::new(
                log, codec(), 512, NativePageLimits::default(),
            ).unwrap();
            let mut txn = source.begin(&cx).unwrap();
            source.write_page(&cx, &mut txn, page(2), Some(&[0xBC; 512])).unwrap();
            source.commit(&cx, &mut txn, 100).await.unwrap();
            source.close(&cx).unwrap();
            let mut recovery = NativeRecovery::new(
                &cx, file(&vfs, &cx, "objects", &control),
                file(&vfs, &cx, "markers", &control), NativeDurabilityLimits::default(),
            ).unwrap();
            recovery.prepare(&cx, |id, records| {
                ready(codec().decode(&cx, id, &records).map(|_| ()))
            }).await.unwrap();
            let recovered = recovery.finish().unwrap();
            let before = control.freeze();
            let result = NativePageStore::from_recovered(
                &cx, recovered, RaptorQNativeCodec::new(Some(handoff_key)),
                512, NativePageLimits::default(),
            ).await;
            control.assert_frozen(before);
            if handoff_key != [7; 32] {
                assert!(result.is_err(), "the seal does not authorize a wrong authentication epoch");
                continue;
            }
            let (mut db, report) = result.unwrap();
            assert_eq!(report.markers.len(), 1);
            let mut next = db.begin(&cx).unwrap();
            assert_eq!(db.read_page(&cx, &mut next, page(2)).unwrap().unwrap().as_ref(), &[0xBC; 512]);
            db.write_page(&cx, &mut next, page(3), Some(&[0xDE; 512])).unwrap();
            control.assert_frozen(before);
            control.forbid_mutation.store(false, Ordering::SeqCst);
            assert_eq!(db.commit(&cx, &mut next, 101).await.unwrap().unwrap().commit_seq, CommitSeq::new(2));
            db.close(&cx).unwrap();
        }
    });
}
