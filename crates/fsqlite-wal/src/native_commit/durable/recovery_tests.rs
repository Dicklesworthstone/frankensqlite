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
