//! Storage-recovery to transaction handoff, using real native log/page APIs.
//! MemoryVfs and counters establish ownership/order, not physical durability.
use std::collections::BTreeMap;
use std::future::{Future, poll_fn, ready};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    CommitMarker, CommitSeq, LockLevel, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags,
    TxnEpoch, TxnId, TxnToken, reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs, VfsFile, VfsWriteCompletion};
use fsqlite_wal::native_commit::CommitSubmission;
use fsqlite_wal::native_commit::durable::{
    NativeCommitCandidate, NativeCommitProof, NativeCoordinatorRecovery, NativeObjectCodec,
};
use fsqlite_wal::native_durability::{
    NativeDurabilityLimits, NativeDurabilityLog, RecoveredNativeLog,
};

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}

#[derive(Clone, Copy)]
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
        let bytes = reconstruct_systematic_happy_path(records).map_err(|error| {
            FrankenError::WalCorrupt { detail: error.to_string() }
        })?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::WalCorrupt { detail: "test identity mismatch".to_owned() });
        }
        Ok(bytes)
    }
}

#[derive(Default)]
struct Audit {
    opens: AtomicUsize,
    closes: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
    syncs: AtomicUsize,
    pause_read_at: AtomicUsize,
    fail_sync_at: AtomicUsize,
    fail_close: AtomicBool,
}
impl Audit {
    fn edges(&self) -> [usize; 4] {
        [
            self.opens.load(Ordering::SeqCst), self.closes.load(Ordering::SeqCst),
            self.writes.load(Ordering::SeqCst), self.syncs.load(Ordering::SeqCst),
        ]
    }
}

struct File {
    inner: <MemoryVfs as Vfs>::File,
    audit: Arc<Audit>,
}
impl VfsFile for File {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.audit.closes.fetch_add(1, Ordering::SeqCst);
        if self.audit.fail_close.swap(false, Ordering::SeqCst) {
            return Err(FrankenError::Io(std::io::Error::other("injected close error")));
        }
        self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.refresh_file_identity() }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        let ordinal = self.audit.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if self.audit.pause_read_at.load(Ordering::SeqCst) == ordinal {
            std::future::pending::<()>().await;
        }
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write(cx, bytes, offset).await
    }
    async fn write_tracked<'a>(
        &'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64, completion: VfsWriteCompletion,
    ) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write_tracked(cx, bytes, offset, completion).await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> { self.inner.truncate(cx, size) }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        let ordinal = self.audit.syncs.fetch_add(1, Ordering::SeqCst) + 1;
        if self.audit.fail_sync_at.load(Ordering::SeqCst) == ordinal {
            return Err(FrankenError::Io(std::io::Error::other("injected sync error")));
        }
        self.inner.sync(cx, flags)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        let ordinal = self.audit.syncs.fetch_add(1, Ordering::SeqCst) + 1;
        if self.audit.fail_sync_at.load(Ordering::SeqCst) == ordinal {
            return Err(FrankenError::Io(std::io::Error::other("injected sync error")));
        }
        self.inner.durable_sync(cx, kind)
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> { self.inner.file_size(cx) }
    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.lock(cx, level) }
    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> { self.inner.unlock(cx, level) }
    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> { self.inner.check_reserved_lock(cx) }
    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }
    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }
    fn shm_barrier(&self) { self.inner.shm_barrier(); }
    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> { self.inner.shm_unmap(cx, delete) }
}

type Log = NativeDurabilityLog<File, File>;
fn file(vfs: &MemoryVfs, cx: &Cx, name: &str, audit: &Arc<Audit>) -> File {
    let inner = vfs.open(
        cx, Some(Path::new(name)), VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    ).unwrap().0;
    audit.opens.fetch_add(1, Ordering::SeqCst);
    File { inner, audit: Arc::clone(audit) }
}
fn log(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>) -> Log {
    Log::create(
        cx, file(vfs, cx, "objects", audit), file(vfs, cx, "markers", audit),
        NativeDurabilityLimits::default(),
    ).unwrap()
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn submission(seed: u8, begin: u64) -> CommitSubmission {
    CommitSubmission {
        capsule_object_id: ObjectId::derive_from_canonical_bytes(&[seed; 8]),
        capsule_digest: *blake3::hash(&[seed; 8]).as_bytes(),
        write_set_pages: vec![page(u32::from(seed))],
        witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
        txn_token: TxnToken::new(TxnId::new(u64::from(seed)).unwrap(), TxnEpoch::new(1)),
        begin_seq: CommitSeq::new(begin),
    }
}
async fn append(
    log: &mut Log, cx: &Cx, proof: &NativeCommitProof, payload: &[u8], previous: Option<ObjectId>,
) -> Result<ObjectId> {
    let capsule = Codec.encode(cx, payload)?;
    let records = Codec.encode(cx, &proof.to_bytes()?)?;
    let marker = CommitMarker::new(
        proof.commit_seq, proof.commit_time_unix_ns,
        capsule[0].object_id, records[0].object_id, previous,
    );
    log.append_symbols(cx, &capsule).await?;
    log.append_symbols(cx, &records).await?;
    log.publish(cx, std::slice::from_ref(&marker), |id, records| {
        ready(Codec.decode(cx, id, &records).map(|_| ()))
    }).await?;
    Ok(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()))
}
async fn seeded(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>, count: u8) -> Log {
    let mut log = log(vfs, cx, audit);
    let mut previous = None;
    for seed in 1..=count {
        previous = Some(append(&mut log, cx, &NativeCommitProof {
            commit_seq: CommitSeq::new(u64::from(seed)),
            commit_time_unix_ns: 100 * u64::from(seed),
            submission: submission(seed, 0),
        }, &[seed; 8], previous).await.unwrap());
    }
    log
}
async fn recover(log: Log, cx: &Cx) -> RecoveredNativeLog<File, File> {
    let mut owner = log.into_recovery();
    owner.prepare(cx, |id, records| ready(Codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    owner.finish().unwrap()
}
fn validate(candidate: &NativeCommitCandidate, _: &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()> {
    let seed = candidate.capsule[0];
    assert_eq!(candidate.capsule.as_ref(), &[seed; 8]);
    assert_eq!(candidate.proof.submission.write_set_pages, vec![page(u32::from(seed))]);
    Ok(())
}

#[test]
fn recovered_coordinator_preserves_conflicts_clock_and_same_open_handles() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let recovered = recover(seeded(&vfs, &cx, &audit, 2).await, &cx).await;
        let before = audit.edges();
        let replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
        let mut replay = replay.finish().err().expect("unprepared owner is returned");
        replay.prepare(&cx, validate).await.unwrap();
        assert_eq!(audit.edges(), before, "replay only reads the retained handles");
        let (mut driver, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 2);
        assert_eq!(driver.committed_tip(), CommitSeq::new(2));
        assert_eq!(driver.pending_count(), 0);
        assert!(driver.take_committed(CommitSeq::new(1)).is_none());
        assert!(matches!(
            driver.queue(&cx, submission(1, 0), 0),
            Err(fsqlite_wal::native_commit::durable::DurableCommitError::Rejected(
                fsqlite_wal::native_commit::CommitResult::ConflictFcw { .. }
            ))
        ));
        assert!(driver.queue(&cx, submission(3, 3), 0).is_err());
        driver.stage_symbols(&cx, &Codec.encode(&cx, &[3; 8]).unwrap()).await.unwrap();
        let seq = driver.queue(&cx, submission(3, 2), 0).unwrap();
        assert_eq!(seq, CommitSeq::new(3));
        driver.flush(&cx, |candidates, objects| {
            for candidate in candidates { validate(candidate, objects)?; }
            Ok(())
        }).await.unwrap();
        let ack = driver.take_committed(seq).unwrap();
        assert_eq!(ack.commit_time_unix_ns, 201);
        assert_eq!(ack.txn_token, submission(3, 2).txn_token);
        assert_eq!(audit.syncs.load(Ordering::SeqCst), before[3] + 2);
        assert_eq!(audit.opens.load(Ordering::SeqCst), 2);
        assert_eq!(audit.closes.load(Ordering::SeqCst), 0);
        driver.close(&cx).unwrap();
    });
}

#[test]
fn rejected_semantics_retains_the_owner_and_retries_from_genesis() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let recovered = recover(seeded(&vfs, &cx, &audit, 2).await, &cx).await;
        let before = audit.edges();
        let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
        replay.prepare(&cx, validate).await.unwrap();
        let mut visited = Vec::new();
        assert!(matches!(replay.prepare(&cx, |candidate, objects| {
            validate(candidate, objects)?;
            visited.push(candidate.proof.commit_seq.get());
            if visited.len() == 2 { return Err(FrankenError::Abort); }
            Ok(())
        }).await, Err(FrankenError::Abort)));
        assert_eq!(visited, vec![1, 2]);
        let mut replay = replay.finish().err().expect("no partial driver");
        visited.clear();
        replay.prepare(&cx, |candidate, objects| {
            visited.push(candidate.proof.commit_seq.get());
            validate(candidate, objects)
        }).await.unwrap();
        assert_eq!(visited, vec![1, 2], "retry must rebuild all conflict entries");
        assert_eq!(audit.edges(), before);
        let (mut driver, _) = replay.finish().unwrap();
        assert_eq!(driver.committed_tip(), CommitSeq::new(2));
        driver.close(&cx).unwrap();
    });
}

#[test]
fn dropping_replay_after_one_commit_keeps_the_handles_and_no_partial_state() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let recovered = recover(seeded(&vfs, &cx, &audit, 2).await, &cx).await;
        let before = audit.edges();
        let next = audit.reads.load(Ordering::SeqCst) + 3;
        audit.pause_read_at.store(next, Ordering::SeqCst);
        let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
        let mut visited = Vec::new();
        {
            let future = replay.prepare(&cx, |candidate, objects| {
                visited.push(candidate.proof.commit_seq.get());
                validate(candidate, objects)
            });
            let mut future = std::pin::pin!(future);
            assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await.is_pending());
        }
        assert_eq!(visited, vec![1], "drop during the second proof read");
        assert_eq!(audit.edges(), before);
        let mut replay = replay.finish().err().expect("dropped read cannot complete replay");
        audit.pause_read_at.store(0, Ordering::SeqCst);
        visited.clear();
        replay.prepare(&cx, |candidate, objects| {
            visited.push(candidate.proof.commit_seq.get());
            validate(candidate, objects)
        }).await.unwrap();
        assert_eq!(visited, vec![1, 2]);
        assert_eq!(audit.edges(), before);
        let (mut driver, _) = replay.finish().unwrap();
        driver.close(&cx).unwrap();
    });
}

#[test]
fn cancellation_in_the_final_validator_does_not_install_a_driver() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let recovered = recover(seeded(&vfs, &cx, &audit, 1).await, &cx).await;
        let before = audit.edges();
        let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
        assert!(matches!(replay.prepare(&cx, |candidate, objects| {
            validate(candidate, objects)?;
            cx.cancel();
            Ok(())
        }).await, Err(FrankenError::Interrupt)));
        let mut replay = replay.finish().err().expect("cancellation retains ownership");
        let fresh = Cx::new();
        replay.prepare(&fresh, validate).await.unwrap();
        assert_eq!(audit.edges(), before);
        let (mut driver, _) = replay.finish().unwrap();
        driver.close(&fresh).unwrap();
    });
}

#[test]
fn byte_valid_but_unbound_proofs_and_missing_evidence_fail_replay() {
    run(async {
        for case in 0..3 {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            let mut log = log(&vfs, &cx, &audit);
            let mut proof = NativeCommitProof {
                commit_seq: CommitSeq::new(1), commit_time_unix_ns: 100,
                submission: submission(1, 0),
            };
            match case {
                0 => proof.submission.capsule_object_id = ObjectId::derive_from_canonical_bytes(b"wrong"),
                1 => proof.submission.capsule_digest[0] ^= 0x80,
                _ => proof.submission.witness_refs.push(ObjectId::derive_from_canonical_bytes(b"missing")),
            }
            // The low-level fixture authenticates object bytes, not this
            // deliberately invalid proof's application semantics/dependencies.
            append(&mut log, &cx, &proof, &[1; 8], None).await.unwrap();
            let recovered = recover(log, &cx).await;
            let before = audit.edges();
            let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
            let mut callbacks = 0;
            assert!(matches!(replay.prepare(&cx, |_, _| {
                callbacks += 1;
                Ok(())
            }).await, Err(FrankenError::WalCorrupt { .. })));
            assert_eq!(callbacks, 0);
            assert_eq!(audit.edges(), before);
            let mut replay = replay.finish().err().expect("invalid proof cannot create a driver");
            replay.close(&cx).unwrap();
        }
    });
}

#[test]
fn an_uncertain_marker_sync_recovers_without_reopening_or_reacknowledging() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let mut log = log(&vfs, &cx, &audit);
        audit.fail_sync_at.store(2, Ordering::SeqCst);
        let proof = NativeCommitProof {
            commit_seq: CommitSeq::new(1), commit_time_unix_ns: 100,
            submission: submission(1, 0),
        };
        assert!(matches!(append(&mut log, &cx, &proof, &[1; 8], None).await,
            Err(FrankenError::Io(_))));
        assert!(log.needs_recovery());
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        let recovered = recover(log, &cx).await;
        assert_eq!(audit.syncs.load(Ordering::SeqCst), 4);
        let before = audit.edges();
        let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
        replay.prepare(&cx, validate).await.unwrap();
        let (mut driver, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(driver.committed_tip(), CommitSeq::new(1));
        assert_eq!(driver.pending_count(), 0);
        assert!(driver.take_committed(CommitSeq::new(1)).is_none());
        assert_eq!(audit.edges(), before);
        assert_eq!(before[0..2], [2, 0]);
        driver.close(&cx).unwrap();
    });
}

#[test]
fn invalid_capacity_and_failed_close_never_discard_the_recovery_owner() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let recovered = recover(log(&vfs, &cx, &audit), &cx).await;
        let before = audit.edges();
        let reads = audit.reads.load(Ordering::SeqCst);
        let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 0);
        assert!(matches!(replay.prepare(&cx, validate).await,
            Err(FrankenError::OutOfRange { .. })));
        assert_eq!(audit.edges(), before);
        assert_eq!(audit.reads.load(Ordering::SeqCst), reads);
        let replay = replay.finish().err().expect("invalid capacity retains handles");
        let (mut storage, codec) = replay.into_recovery();
        storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut replay = NativeCoordinatorRecovery::new(storage.finish().unwrap(), codec, 4);
        replay.prepare(&cx, validate).await.unwrap();
        audit.fail_close.store(true, Ordering::SeqCst);
        assert!(matches!(replay.close(&cx), Err(FrankenError::Io(_))));
        assert!(matches!(replay.prepare(&cx, validate).await, Err(FrankenError::BusyRecovery)));
        let mut replay = replay.finish().err().expect("closing invalidates even an empty prepared replay");
        replay.close(&cx).unwrap();
        assert_eq!(audit.closes.load(Ordering::SeqCst), 3, "retry only the failed close");
    });
}

#[test]
fn semantic_replay_does_not_turn_a_torn_tail_into_append_permission() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let log = seeded(&vfs, &cx, &audit, 1).await;
        let mut raw = vfs.open(&cx, Some(Path::new("objects")), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap().0;
        raw.write(&cx, b"torn", raw.file_size(&cx).unwrap()).await.unwrap();
        raw.close(&cx).unwrap();
        let recovered = recover(log, &cx).await;
        assert!(recovered.report().append_blocked());
        let before = audit.edges();
        let mut replay = NativeCoordinatorRecovery::new(recovered, Codec, 4);
        replay.prepare(&cx, validate).await.unwrap();
        let (mut driver, report) = replay.finish().unwrap();
        assert_eq!(report.symbol_tail_bytes, 4);
        assert!(driver.needs_recovery());
        assert!(driver.queue(&cx, submission(2, 1), 200).is_err());
        assert_eq!(audit.edges(), before);
        driver.close(&cx).unwrap();
    });
}
