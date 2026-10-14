//! Real MemoryVfs byte I/O with deterministic barrier and short-write faults.
//! These tests are not physical power-loss certificates.

use std::collections::BTreeMap;
use std::future::{Future, pending, poll_fn, ready};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    COMMIT_MARKER_RECORD_V1_SIZE, LockLevel, Oti, PageNumber, SymbolRecord,
    SymbolRecordFlags, TxnEpoch, TxnId, reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs, VfsWriteCompletion};

use super::*;
use crate::native_commit::CommitSubmission;
use crate::native_commit::durable::NativeCommitCandidate;
use crate::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};

#[derive(Default)]
struct Audit {
    reads: AtomicUsize,
    writes: AtomicUsize,
    syncs: AtomicUsize,
    fail_sync: AtomicUsize,
    pause_read: AtomicBool,
    short_write: Mutex<Option<(usize, usize)>>,
}
impl Audit {
    fn mutations(&self) -> (usize, usize) {
        (self.writes.load(Ordering::SeqCst), self.syncs.load(Ordering::SeqCst))
    }
    fn barrier(&self) -> Result<()> {
        let ordinal = self.syncs.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_sync.load(Ordering::SeqCst) == ordinal {
            return Err(std::io::Error::other("injected native outcome barrier failure").into());
        }
        Ok(())
    }
}
struct File {
    inner: <MemoryVfs as Vfs>::File,
    stream: usize,
    audit: Arc<Audit>,
}
impl VfsFile for File {
    fn close(&mut self, cx: &Cx) -> Result<()> { self.inner.close(cx) }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.refresh_file_identity() }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        self.audit.reads.fetch_add(1, Ordering::SeqCst);
        if self.audit.pause_read.load(Ordering::SeqCst) { return pending().await; }
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write(cx, bytes, offset).await
    }
    async fn write_tracked<'a>(
        &'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64, completion: VfsWriteCompletion,
    ) -> Result<()> {
        let short = {
            let mut fault = self.audit.short_write.lock().unwrap();
            if fault.is_some_and(|(stream, _)| stream == self.stream) {
                fault.take().map(|(_, count)| count)
            } else { None }
        };
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        if let Some(count) = short {
            let result = self.inner.write(cx, &bytes[..count.min(bytes.len())], offset).await;
            completion.complete_error();
            result?;
            return Err(std::io::Error::other("injected short native outcome write").into());
        }
        self.inner.write_tracked(cx, bytes, offset, completion).await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.audit.barrier()?;
        self.inner.sync(cx, flags)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        self.audit.barrier()?;
        self.inner.durable_sync(cx, kind)
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> { self.inner.file_size(cx) }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.lock(cx, level) }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.unlock(cx, level) }
    fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_wal_append(cx) }
    fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> { self.inner.owns_external_wal_append_write(cx) }
    fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> { self.inner.restore_external_wal_append_attempt(cx) }
    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_shared_snapshot(cx) }
    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> { self.inner.restore_external_shared_snapshot_attempt(cx) }
    fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> { self.inner.lock_external_maintenance(cx, wal) }
    fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> { self.inner.lock_external_wal_recovery(cx) }
    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> { self.inner.restore_external_maintenance_attempt(cx) }
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> { self.inner.check_reserved_lock(cx) }
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> { self.inner.shm_map(cx, region, size, extend) }
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> { self.inner.shm_lock(cx, offset, n, flags) }
    fn shm_barrier(&self) { self.inner.shm_barrier(); }
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> { self.inner.shm_unmap(cx, delete) }
}
struct Codec;
impl NativeObjectCodec for Codec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
            0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START)])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records).map_err(|error| corrupt(&error.to_string()))?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id { return Err(corrupt("outcome test object identity")); }
        Ok(bytes)
    }
}
type Driver = DurableWriteCoordinator<File, File, Codec>;
type Replay = NativeCoordinatorRecovery<File, File, Codec>;
fn run(future: impl Future<Output = ()>) {
    let _guard = crate::metrics::GLOBAL_GROUP_COMMIT_METRICS_TEST_LOCK.lock().unwrap();
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn raw(vfs: &MemoryVfs, cx: &Cx, name: &str) -> <MemoryVfs as Vfs>::File {
    vfs.open(cx, Some(Path::new(name)), VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn log(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>) -> NativeDurabilityLog<File, File> {
    NativeDurabilityLog::create(cx,
        File { inner: raw(vfs, cx, "objects"), stream: 0, audit: Arc::clone(audit) },
        File { inner: raw(vfs, cx, "markers"), stream: 1, audit: Arc::clone(audit) },
        NativeDurabilityLimits::default()).unwrap()
}
fn token(value: u8) -> TxnToken { TxnToken::new(TxnId::new(u64::from(value)).unwrap(), TxnEpoch::new(1)) }
fn submission(value: u8) -> CommitSubmission {
    CommitSubmission {
        capsule_object_id: ObjectId::derive_from_canonical_bytes(&[value; 32]),
        capsule_digest: *blake3::hash(&[value; 32]).as_bytes(),
        write_set_pages: vec![PageNumber::new(u32::from(value)).unwrap()],
        witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
        txn_token: token(value), begin_seq: CommitSeq::ZERO,
    }
}
fn validate(candidate: &NativeCommitCandidate, _: &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()> {
    let value = u8::try_from(candidate.proof.submission.txn_token.id.get()).map_err(|_| FrankenError::Abort)?;
    if candidate.capsule.as_ref() != [value; 32]
        || candidate.proof.submission.write_set_pages != submission(value).write_set_pages
    { return Err(FrankenError::Abort); }
    Ok(())
}
async fn queue(driver: &mut Driver, cx: &Cx, value: u8, time: u64) -> NativeCommitAttempt {
    driver.stage_symbols(cx, &Codec.encode(cx, &[value; 32]).unwrap()).await.unwrap();
    let seq = driver.queue(cx, submission(value), time).unwrap();
    let attempt = driver.commit_attempt(token(value)).unwrap().unwrap();
    assert_eq!(attempt.commit_seq(), seq);
    attempt
}
async fn flush(driver: &mut Driver, cx: &Cx) -> Result<()> {
    driver.flush(cx, |candidates, objects| {
        for candidate in candidates { validate(candidate, objects)?; }
        Ok(())
    }).await.map(|_| ())
}
async fn replay(driver: Driver, cx: &Cx) -> Replay {
    let (mut physical, codec) = driver.try_into_recovery().unwrap_or_else(|_| panic!("live obligation not drained"));
    physical.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    NativeCoordinatorRecovery::new(physical.finish().unwrap(), codec, 8)
}
fn assert_committed(outcome: NativeCommitOutcome, attempt: &NativeCommitAttempt) {
    assert_eq!(outcome, NativeCommitOutcome::Committed(attempt.expected.clone()));
}

#[test]
fn barrier_failures_resolve_each_exact_member_without_replaying_or_faking_replies() {
    run(async {
        for barrier in [1, 2] {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
            let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
            let first = queue(&mut driver, &cx, 1, 100).await;
            let second = queue(&mut driver, &cx, 2, 100).await;
            audit.fail_sync.store(barrier, Ordering::SeqCst);
            assert!(flush(&mut driver, &cx).await.is_err());
            assert!(driver.needs_recovery());
            assert!(driver.take_committed(first.commit_seq()).is_none());
            let before = (audit.reads.load(Ordering::SeqCst), audit.mutations());
            let retained = driver.commit_attempt(token(2)).unwrap().unwrap();
            assert_eq!(retained.proof_digest, second.proof_digest);
            assert!(Arc::ptr_eq(&retained.epoch, &second.epoch));
            assert_eq!((audit.reads.load(Ordering::SeqCst), audit.mutations()), before);
            audit.fail_sync.store(0, Ordering::SeqCst);
            let mut owner = replay(driver, &cx).await;
            assert!(matches!(owner.resolve_commit(&cx, &first).await, Err(FrankenError::BusyRecovery)));
            owner.prepare(&cx, validate).await.unwrap();
            let before = audit.mutations();
            for attempt in [&first, &second] {
                let outcome = owner.resolve_commit(&cx, attempt).await.unwrap();
                if barrier == 1 { assert_eq!(outcome, NativeCommitOutcome::NotCommitted); }
                else { assert_committed(outcome, attempt); }
            }
            assert_eq!(audit.mutations(), before, "resolving outcomes must not write or sync");
            let (mut driver, report) = owner.finish().unwrap();
            assert_eq!(report.markers.len(), if barrier == 1 { 0 } else { 2 });
            assert_eq!(driver.pending_count(), 0);
            assert!(driver.take_committed(first.commit_seq()).is_none(), "recovery did not send an old reply");
            driver.close(&cx).unwrap();
        }
    });
}

#[test]
fn identical_retry_in_a_new_publisher_epoch_cannot_resolve_an_old_ticket() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
        let old = queue(&mut driver, &cx, 1, 100).await;
        // Fail the first proof write before any of its bytes reach storage.
        // The capsule exists, but no duplicate proof symbols are introduced
        // by the later identical retry; only the publisher epoch distinguishes it.
        *audit.short_write.lock().unwrap() = Some((0, 0));
        assert!(flush(&mut driver, &cx).await.is_err());
        let mut owner = replay(driver, &cx).await;
        owner.prepare(&cx, validate).await.unwrap();
        assert_eq!(owner.resolve_commit(&cx, &old).await.unwrap(), NativeCommitOutcome::NotCommitted);
        let (mut driver, _) = owner.finish().unwrap();
        // The staged capsule survived; deliberately reuse the exact token,
        // sequence, time, and proof bytes to isolate the epoch binding.
        driver.queue(&cx, submission(1), 100).unwrap();
        let new = driver.commit_attempt(token(1)).unwrap().unwrap();
        assert_eq!(old.expected, new.expected);
        assert_eq!(old.proof_digest, new.proof_digest);
        assert_eq!(old.proof_object_id, new.proof_object_id);
        assert!(!Arc::ptr_eq(&old.epoch, &new.epoch));
        flush(&mut driver, &cx).await.unwrap();
        let after_flush = driver.commit_attempt(token(1)).unwrap().unwrap();
        assert_eq!(after_flush.proof_digest, new.proof_digest, "capture after encoded proofs are released");
        assert_eq!(driver.take_committed(new.commit_seq()).unwrap(), new.expected);
        let mut owner = replay(driver, &cx).await;
        owner.prepare(&cx, validate).await.unwrap();
        assert!(matches!(owner.resolve_commit(&cx, &old).await, Err(FrankenError::Abort)));
        assert_committed(owner.resolve_commit(&cx, &new).await.unwrap(), &new);
        owner.close(&cx).unwrap();
    });
}

#[test]
fn a_torn_batch_reports_its_committed_prefix_but_never_guesses_the_missing_outcome() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
        let first = queue(&mut driver, &cx, 1, 100).await;
        flush(&mut driver, &cx).await.unwrap();
        driver.take_committed(first.commit_seq()).unwrap();
        let second = queue(&mut driver, &cx, 2, 200).await;
        let third = queue(&mut driver, &cx, 3, 200).await;
        *audit.short_write.lock().unwrap() = Some((1, COMMIT_MARKER_RECORD_V1_SIZE + 7));
        assert!(flush(&mut driver, &cx).await.is_err());
        let mut owner = replay(driver, &cx).await;
        assert_eq!(owner.report().markers.len(), 2);
        assert_eq!(owner.report().marker_tail_bytes, 7);
        owner.prepare(&cx, validate).await.unwrap();
        let before = audit.mutations();
        assert_committed(owner.resolve_commit(&cx, &first).await.unwrap(), &first);
        assert_committed(owner.resolve_commit(&cx, &second).await.unwrap(), &second);
        assert!(matches!(owner.resolve_commit(&cx, &third).await, Err(FrankenError::BusyRecovery)));
        assert_eq!(audit.mutations(), before);
        let (mut driver, _) = owner.finish().unwrap();
        assert!(driver.needs_recovery());
        driver.close(&cx).unwrap();
    });
}

#[test]
fn failed_or_dropped_replay_cannot_issue_outcomes_and_dropped_lookups_keep_tickets() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
        let attempt = queue(&mut driver, &cx, 1, 100).await;
        flush(&mut driver, &cx).await.unwrap();
        driver.take_committed(attempt.commit_seq()).unwrap();
        let mut owner = replay(driver, &cx).await;
        assert!(owner.prepare(&cx, |_, _| Err(FrankenError::Abort)).await.is_err());
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
        owner.prepare(&cx, validate).await.unwrap();
        let before = audit.mutations();
        audit.pause_read.store(true, Ordering::SeqCst);
        {
            let mut lookup = std::pin::pin!(owner.resolve_commit(&cx, &attempt));
            assert!(poll_fn(|cx| Poll::Ready(lookup.as_mut().poll(cx))).await.is_pending());
        }
        audit.pause_read.store(false, Ordering::SeqCst);
        assert_committed(owner.resolve_commit(&cx, &attempt).await.unwrap(), &attempt);
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(owner.resolve_commit(&cancelled, &attempt).await, Err(FrankenError::Interrupt)));
        audit.pause_read.store(true, Ordering::SeqCst);
        {
            let mut prepare = std::pin::pin!(owner.prepare(&cx, validate));
            assert!(poll_fn(|cx| Poll::Ready(prepare.as_mut().poll(cx))).await.is_pending());
        }
        audit.pause_read.store(false, Ordering::SeqCst);
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
        let last_cancel = Cx::new();
        assert!(matches!(owner.prepare(&last_cancel, |candidate, objects| {
            validate(candidate, objects)?; last_cancel.cancel(); Ok(())
        }).await, Err(FrankenError::Interrupt)));
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
        owner.prepare(&cx, validate).await.unwrap();
        assert_committed(owner.resolve_commit(&cx, &attempt).await.unwrap(), &attempt);
        assert_eq!(audit.mutations(), before);
        owner.close(&cx).unwrap();
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
    });
}

#[test]
fn unrelated_identical_databases_and_raw_log_escape_do_not_inherit_ticket_authority() {
    run(async {
        let cx = Cx::new(); let mut pair = Vec::new();
        for _ in 0..2 {
            let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
            let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
            let attempt = queue(&mut driver, &cx, 1, 100).await;
            flush(&mut driver, &cx).await.unwrap(); driver.take_committed(attempt.commit_seq()).unwrap();
            let mut owner = replay(driver, &cx).await; owner.prepare(&cx, validate).await.unwrap();
            pair.push((owner, attempt));
        }
        let (mut second, second_ticket) = pair.pop().unwrap();
        let (mut first, first_ticket) = pair.pop().unwrap();
        assert_eq!(first_ticket.expected, second_ticket.expected);
        assert_eq!(first_ticket.proof_digest, second_ticket.proof_digest);
        assert!(matches!(second.resolve_commit(&cx, &first_ticket).await, Err(FrankenError::Abort)));
        assert_committed(second.resolve_commit(&cx, &second_ticket).await.unwrap(), &second_ticket);
        // Returning to owned physical recovery preserves the epoch across retry.
        let (mut physical, codec) = first.into_recovery();
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        first = NativeCoordinatorRecovery::new(physical.finish().unwrap(), codec, 8);
        first.prepare(&cx, validate).await.unwrap();
        assert_committed(first.resolve_commit(&cx, &first_ticket).await.unwrap(), &first_ticket);
        let (mut physical, codec) = first.into_recovery();
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let (raw_log, _) = physical.finish().unwrap().into_log();
        let mut physical = raw_log.into_recovery();
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut unbound = NativeCoordinatorRecovery::new(physical.finish().unwrap(), codec, 8);
        unbound.prepare(&cx, validate).await.unwrap();
        assert!(matches!(unbound.resolve_commit(&cx, &first_ticket).await, Err(FrankenError::Abort)));
        unbound.close(&cx).unwrap(); second.close(&cx).unwrap();
    });
}

#[test]
fn symbol_tails_changed_lengths_and_changed_proofs_never_produce_a_false_outcome() {
    run(async {
        // A torn symbol is not enough evidence to resolve absence, even with
        // no marker. Retain the uncertainty until explicit tail reconciliation.
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
        let attempt = queue(&mut driver, &cx, 1, 100).await;
        *audit.short_write.lock().unwrap() = Some((0, 5));
        assert!(flush(&mut driver, &cx).await.is_err());
        let mut owner = replay(driver, &cx).await; owner.prepare(&cx, validate).await.unwrap();
        assert_eq!(owner.report().symbol_tail_bytes, 5);
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
        owner.close(&cx).unwrap();

        let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut driver = Driver::new(log(&vfs, &cx, &audit), Codec, 8).unwrap();
        let attempt = queue(&mut driver, &cx, 1, 100).await;
        // The proof follows exactly one already-staged capsule envelope.
        let mut objects = raw(&vfs, &cx, "objects");
        let proof_offset = objects.file_size(&cx).unwrap();
        flush(&mut driver, &cx).await.unwrap(); driver.take_committed(attempt.commit_seq()).unwrap();
        let mut owner = replay(driver, &cx).await; owner.prepare(&cx, validate).await.unwrap();
        let end = objects.file_size(&cx).unwrap();
        objects.write(&cx, b"x", end).await.unwrap();
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
        objects.truncate(&cx, end).unwrap();
        let mut byte = [0]; objects.read(&cx, &mut byte, proof_offset + 51).await.unwrap();
        objects.write(&cx, &[byte[0] ^ 0x80], proof_offset + 51).await.unwrap();
        assert!(owner.resolve_commit(&cx, &attempt).await.is_err());
        objects.close(&cx).unwrap(); owner.close(&cx).unwrap();
    });
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[test]
fn authenticated_attempt_requires_successful_authenticated_semantic_replay() {
    use crate::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
        let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let codec = RaptorQNativeCodec::new(Some([7; 32]));
        let bytes = vec![0xAB; 4097];
        let records = codec.encode(&cx, &bytes).unwrap();
        let mut driver = DurableWriteCoordinator::new(log(&vfs, &cx, &audit), codec, 8).unwrap();
        driver.stage_symbols(&cx, &records).await.unwrap();
        let mut sub = submission(1);
        sub.capsule_object_id = records[0].object_id;
        sub.capsule_digest = *blake3::hash(&bytes).as_bytes();
        driver.queue(&cx, sub, 100).unwrap();
        let attempt = driver.commit_attempt(token(1)).unwrap().unwrap();
        audit.fail_sync.store(2, Ordering::SeqCst);
        assert!(driver.flush(&cx, |candidates, _| {
            assert_eq!(candidates[0].capsule.as_ref(), bytes.as_slice()); Ok(())
        }).await.is_err());
        audit.fail_sync.store(0, Ordering::SeqCst);
        let (mut physical, codec) = driver.try_into_recovery().unwrap_or_else(|_| panic!("failed native transfer"));
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut owner = NativeCoordinatorRecovery::new(physical.finish().unwrap(), RaptorQNativeCodec::new(Some([8; 32])), 8);
        assert!(owner.prepare(&cx, |_, _| Ok(())).await.is_err());
        assert!(matches!(owner.resolve_commit(&cx, &attempt).await, Err(FrankenError::BusyRecovery)));
        let (mut physical, _) = owner.into_recovery();
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut owner = NativeCoordinatorRecovery::new(physical.finish().unwrap(), codec, 8);
        owner.prepare(&cx, |candidate, _| {
            assert_eq!(candidate.capsule.as_ref(), bytes.as_slice()); Ok(())
        }).await.unwrap();
        let before = audit.mutations();
        assert_committed(owner.resolve_commit(&cx, &attempt).await.unwrap(), &attempt);
        assert_eq!(audit.mutations(), before);
        owner.close(&cx).unwrap();
    });
}
