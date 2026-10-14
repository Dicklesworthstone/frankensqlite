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

async fn append_page_commit(
    log: &mut Log,
    cx: &Cx,
    seq: u64,
    token: u64,
    capsule: &fsqlite_wal::native_pages::NativePageCapsule,
    previous: Option<ObjectId>,
) -> ObjectId {
    let bytes = capsule.to_bytes().unwrap();
    let proof = NativeCommitProof {
        commit_seq: CommitSeq::new(seq),
        commit_time_unix_ns: seq * 100,
        submission: CommitSubmission {
            capsule_object_id: ObjectId::derive_from_canonical_bytes(&bytes),
            capsule_digest: *blake3::hash(&bytes).as_bytes(),
            write_set_pages: capsule.writes.iter().map(|write| write.page).collect(),
            witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
            txn_token: TxnToken::new(TxnId::new(token).unwrap(), TxnEpoch::new(1)),
            begin_seq: capsule.snapshot,
        },
    };
    append(log, cx, &proof, &bytes, previous).await.unwrap()
}

fn page_capsule(
    snapshot: u64, observed: u64, number: u32, fill: Option<u8>,
) -> fsqlite_wal::native_pages::NativePageCapsule {
    use fsqlite_wal::native_pages::{NativePageCapsule, NativePageWrite};
    NativePageCapsule {
        page_size: 512,
        snapshot: CommitSeq::new(snapshot),
        reads: vec![(page(number), CommitSeq::new(observed))],
        writes: vec![NativePageWrite {
            page: page(number), data: fill.map(|value| Arc::from(vec![value; 512])),
        }],
    }
}

#[test]
fn page_handoff_restores_sparse_tombstones_tokens_snapshots_and_sealed_pager_commits() {
    use fsqlite_pager::native::NativePager;
    use fsqlite_pager::{TransactionHandle, TransactionMode};
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery};
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let mut log = log(&vfs, &cx, &audit);
        let first = append_page_commit(&mut log, &cx, 1, 9,
            &page_capsule(0, 0, 900_000, Some(0x11)), None).await;
        let second = append_page_commit(&mut log, &cx, 2, 7,
            &page_capsule(1, 1, 900_000, None), Some(first)).await;
        append_page_commit(&mut log, &cx, 3, 11,
            &page_capsule(2, 0, 2, Some(0x22)), Some(second)).await;
        let recovered = recover(log, &cx).await;
        let before = audit.edges();
        let replay = NativePageRecovery::new(recovered, Codec, 512, NativePageLimits::default());
        let mut replay = replay.finish().err().expect("unprepared page owner retained");
        replay.prepare(&cx).await.unwrap();
        let (mut db, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 3);
        assert_eq!(db.committed_tip(), CommitSeq::new(3));
        assert_eq!(db.retained_page_bytes(), 512);
        assert_eq!(db.retained_version_count(), 2, "retain the latest tombstone");
        assert_eq!(audit.edges(), before);
        let mut old = db.begin(&cx).unwrap();
        assert_eq!(old.token().id.get(), 12, "transaction identity is not commit sequence");
        assert_eq!(old.snapshot_db_size(), 900_000);
        assert!(db.read_page(&cx, &mut old, page(900_000)).unwrap().is_none());
        assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[0x22; 512]);
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(2), Some(&[0x33; 512])).unwrap();
        db.write_page(&cx, &mut b, page(2), Some(&[0x44; 512])).unwrap();
        let fresh_page = db.allocate_page(&cx, &mut a).unwrap();
        assert_eq!(fresh_page, page(900_001));
        let ack = db.commit(&cx, &mut a, 0).await.unwrap().unwrap();
        assert_eq!(ack.commit_seq, CommitSeq::new(4));
        assert_eq!(ack.commit_time_unix_ns, 301);
        assert!(matches!(db.commit(&cx, &mut b, 302).await, Err(FrankenError::BusySnapshot { .. })));
        assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[0x22; 512]);
        db.rollback(&mut b).unwrap();
        db.rollback(&mut old).unwrap();
        let pager = NativePager::new(db).unwrap();
        let mut txn = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        let visible = txn.get_page(&cx, page(2)).await.unwrap();
        assert_eq!(visible.as_bytes(), &[0x33; 512]);
        txn.write_page(&cx, page(2), &[0x55; 512]).await.unwrap();
        pager.commit_batch_at(&cx, &mut [&mut txn], 400).await.unwrap();
        assert_eq!(pager.committed_tip().unwrap(), CommitSeq::new(5));
        assert_eq!(audit.edges()[0..2], before[0..2], "no intermediate close/open");
        assert_eq!(audit.syncs.load(Ordering::SeqCst), before[3] + 4);
        pager.close(&cx).unwrap();
    });
}

#[test]
fn page_replay_drop_and_error_discard_partial_history_without_losing_handles() {
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery};
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let mut log = log(&vfs, &cx, &audit);
        let first = append_page_commit(&mut log, &cx, 1, 1,
            &page_capsule(0, 0, 2, Some(1)), None).await;
        append_page_commit(&mut log, &cx, 2, 2,
            &page_capsule(1, 1, 2, Some(2)), Some(first)).await;
        let recovered = recover(log, &cx).await;
        let before = audit.edges();
        let mut replay = NativePageRecovery::new(recovered, Codec, 512, NativePageLimits::default());
        replay.prepare(&cx).await.unwrap();
        // Invalidate a prior success, then drop while reading the next attempt's
        // second proof. The first applied page must not survive into a retry.
        audit.pause_read_at.store(audit.reads.load(Ordering::SeqCst) + 3, Ordering::SeqCst);
        {
            let mut future = std::pin::pin!(replay.prepare(&cx));
            assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await.is_pending());
        }
        let mut replay = replay.finish().err().expect("partial page history is not a store");
        audit.pause_read_at.store(0, Ordering::SeqCst);
        let cancelled = Cx::new();
        cancelled.cancel();
        assert!(matches!(replay.prepare(&cancelled).await, Err(FrankenError::Interrupt)));
        let mut replay = replay.finish().err().expect("cancel retains the exact owner");
        replay.prepare(&cx).await.unwrap();
        assert_eq!(audit.edges(), before);
        let (mut db, _) = replay.finish().unwrap();
        assert_eq!(db.retained_version_count(), 1);
        assert_eq!(db.committed_tip(), CommitSeq::new(2));
        let mut txn = db.begin(&cx).unwrap();
        assert_eq!(txn.token().id.get(), 3);
        assert_eq!(db.read_page(&cx, &mut txn, page(2)).unwrap().unwrap().as_ref(), &[2; 512]);
        db.rollback(&mut txn).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn page_handoff_rejects_duplicate_tokens_and_stale_reads_even_with_valid_objects() {
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery};
    run(async {
        for duplicate in [false, true] {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            let mut log = log(&vfs, &cx, &audit);
            let first = append_page_commit(&mut log, &cx, 1, 1,
                &page_capsule(0, 0, 2, Some(1)), None).await;
            let second = append_page_commit(&mut log, &cx, 2, 2,
                &page_capsule(1, 1, 2, Some(2)), Some(first)).await;
            let token = if duplicate { 1 } else { 3 };
            let observed = if duplicate { 2 } else { 1 };
            append_page_commit(&mut log, &cx, 3, token,
                &page_capsule(2, observed, 2, Some(3)), Some(second)).await;
            let recovered = recover(log, &cx).await;
            let before = audit.edges();
            let limits = NativePageLimits {
                max_retained_page_bytes: 512, max_versions: 1, ..NativePageLimits::default()
            };
            let mut replay = NativePageRecovery::new(recovered, Codec, 512, limits);
            let error = replay.prepare(&cx).await.unwrap_err();
            assert!(matches!(error, FrankenError::WalCorrupt { .. }));
            let expected = if duplicate { "duplicate native page commit" } else { "unserializable" };
            assert!(error.to_string().contains(expected));
            assert_eq!(audit.edges(), before);
            let mut replay = replay.finish().err().expect("no partly reconstructed page owner");
            replay.close(&cx).unwrap();
        }
    });
}

#[test]
fn page_recovery_bounds_live_history_not_the_number_of_committed_overwrites() {
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery};
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let mut log = log(&vfs, &cx, &audit);
        let mut previous = None;
        for seq in 1..=64 {
            previous = Some(append_page_commit(&mut log, &cx, seq, seq,
                &page_capsule(seq - 1, seq - 1, 2, Some(u8::try_from(seq).unwrap())), previous).await);
        }
        let recovered = recover(log, &cx).await;
        let before = audit.edges();
        let limits = NativePageLimits {
            max_retained_page_bytes: 512, max_versions: 1, ..NativePageLimits::default()
        };
        let mut replay = NativePageRecovery::new(recovered, Codec, 512, limits);
        replay.prepare(&cx).await.unwrap();
        let (mut db, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 64);
        assert_eq!(db.retained_page_bytes(), 512);
        assert_eq!(db.retained_version_count(), 1);
        assert_eq!(audit.edges(), before);
        let mut txn = db.begin(&cx).unwrap();
        assert_eq!(txn.token().id.get(), 65);
        assert_eq!(db.read_page(&cx, &mut txn, page(2)).unwrap().unwrap().as_ref(), &[64; 512]);
        db.rollback(&mut txn).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn invalid_page_options_and_insufficient_limits_retain_the_sealed_streams() {
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery};
    run(async {
        for (size, bytes, versions) in [(513, 512, 1), (1024, 2048, 2), (512, 511, 1), (512, 512, 0)] {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            let mut log = log(&vfs, &cx, &audit);
            append_page_commit(&mut log, &cx, 1, 1, &page_capsule(0, 0, 2, Some(1)), None).await;
            let recovered = recover(log, &cx).await;
            let before = audit.edges();
            let limits = NativePageLimits {
                max_retained_page_bytes: bytes, max_versions: versions, ..NativePageLimits::default()
            };
            let mut replay = NativePageRecovery::new(recovered, Codec, size, limits);
            assert!(replay.prepare(&cx).await.is_err());
            assert_eq!(audit.edges(), before);
            let replay = replay.finish().err().expect("bad configuration cannot consume handles");
            let (mut storage, codec) = replay.into_recovery();
            storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
            let after_storage = audit.edges();
            let mut replay = NativePageRecovery::with_shared_codec(
                storage.finish().unwrap(), codec, 512, NativePageLimits::default(),
            );
            replay.prepare(&cx).await.unwrap();
            let (mut db, _) = replay.finish().unwrap();
            assert_eq!(audit.edges(), after_storage);
            assert_eq!(audit.edges()[0..2], before[0..2]);
            let mut txn = db.begin(&cx).unwrap();
            assert_eq!(db.read_page(&cx, &mut txn, page(2)).unwrap().unwrap().as_ref(), &[1; 512]);
            db.rollback(&mut txn).unwrap();
            db.close(&cx).unwrap();
        }
    });
}

#[test]
fn page_handoff_keeps_torn_tail_refusal_and_retryable_close() {
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery};
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let mut log = log(&vfs, &cx, &audit);
        append_page_commit(&mut log, &cx, 1, 1, &page_capsule(0, 0, 2, Some(1)), None).await;
        let mut raw = vfs.open(&cx, Some(Path::new("markers")), VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap().0;
        raw.write(&cx, b"tail", raw.file_size(&cx).unwrap()).await.unwrap();
        raw.close(&cx).unwrap();
        let recovered = recover(log, &cx).await;
        let before = audit.edges();
        let mut replay = NativePageRecovery::new(recovered, Codec, 512, NativePageLimits::default());
        replay.prepare(&cx).await.unwrap();
        let (mut db, report) = replay.finish().unwrap();
        assert_eq!(report.marker_tail_bytes, 4);
        assert!(db.needs_recovery());
        assert!(matches!(db.begin(&cx), Err(FrankenError::BusyRecovery)));
        assert_eq!(audit.edges(), before);
        audit.fail_close.store(true, Ordering::SeqCst);
        assert!(matches!(db.close(&cx), Err(FrankenError::Io(_))));
        db.close(&cx).unwrap();
        assert_eq!(audit.closes.load(Ordering::SeqCst), before[1] + 3);
    });
}

async fn recovered_btree_rows<T: fsqlite_pager::TransactionHandle>(
    txn: &mut T, cx: &Cx, root: PageNumber,
) -> Vec<(i64, Vec<u8>)> {
    use fsqlite_btree::{BtCursor, BtreeCursorOps, TransactionPageIo};
    let mut cursor = BtCursor::new(TransactionPageIo::new(txn), root, 512, true);
    let mut rows = Vec::new();
    if cursor.first(cx).await.unwrap() {
        loop {
            rows.push((cursor.rowid(cx).await.unwrap(), cursor.payload(cx).await.unwrap()));
            if !cursor.next(cx).await.unwrap() { break; }
        }
    }
    rows
}

#[test]
fn authenticated_btree_pages_recover_through_retained_owners_and_continue_real_mutations() {
    use fsqlite_btree::{BtCursor, BtreeCursorOps, TransactionPageIo};
    use fsqlite_btree::traits::SeekResult;
    use fsqlite_pager::native::NativePager;
    use fsqlite_pager::{TransactionHandle, TransactionMode};
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    use fsqlite_wal::native_durability::NativeRecovery;
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery, NativePageStore};
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let vfs = MemoryVfs::new();
        let audit = Arc::new(Audit::default());
        let codec = || RaptorQNativeCodec::new(Some([7; 32]));
        let pager = NativePager::new(NativePageStore::new(
            log(&vfs, &cx, &audit), codec(), 512, NativePageLimits::default(),
        ).unwrap()).unwrap();
        let mut txn = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        let root = txn.allocate_page(&cx).await.unwrap();
        let mut empty_leaf = vec![0_u8; 512];
        empty_leaf[0] = 0x0D;
        empty_leaf[5..7].copy_from_slice(&512_u16.to_be_bytes());
        txn.write_page(&cx, root, &empty_leaf).await.unwrap();
        let overflow = vec![0xEF; 4097];
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut txn), root, 512, true);
            for row in 0_u8..40 {
                cursor.table_insert(&cx, i64::from(row), &vec![row; 80]).await.unwrap();
            }
            cursor.table_insert(&cx, 200, &overflow).await.unwrap();
        }
        assert_eq!(txn.get_page(&cx, root).await.unwrap().as_bytes()[0], 0x05);
        let expected = recovered_btree_rows(&mut txn, &cx, root).await;
        assert_eq!(expected.len(), 41);
        assert_eq!(expected.last().unwrap(), &(200, overflow));
        txn.commit_at(&cx, 100).await.unwrap();
        pager.close(&cx).unwrap();
        // Existing, already closed source owners are settled before these
        // explicit new handles are admitted. Replay itself must not reopen.
        let mut storage = NativeRecovery::new(
            &cx, file(&vfs, &cx, "objects", &audit), file(&vfs, &cx, "markers", &audit),
            NativeDurabilityLimits::default(),
        ).unwrap();
        storage.prepare(&cx, |id, records| ready(codec().decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let before = audit.edges();
        let mut replay = NativePageRecovery::new(
            storage.finish().unwrap(), codec(), 512, NativePageLimits::default(),
        );
        replay.prepare(&cx).await.unwrap();
        let (db, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(audit.edges(), before);
        let pager = NativePager::new(db).unwrap();
        let mut old = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let mut writer = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        assert_eq!(recovered_btree_rows(&mut writer, &cx, root).await, expected);
        {
            let mut cursor = BtCursor::new(TransactionPageIo::new(&mut writer), root, 512, true);
            for row in [1, 200] {
                assert_eq!(cursor.table_move_to(&cx, row).await.unwrap(), SeekResult::Found);
                cursor.delete(&cx).await.unwrap();
            }
            cursor.table_insert(&cx, 300, &[0xAB; 80]).await.unwrap();
        }
        writer.commit_at(&cx, 200).await.unwrap();
        assert_eq!(recovered_btree_rows(&mut old, &cx, root).await, expected);
        old.rollback(&cx).await.unwrap();
        let mut expected: Vec<_> = expected.into_iter().filter(|(row, _)| *row != 1 && *row != 200).collect();
        expected.push((300, vec![0xAB; 80]));
        let mut fresh = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(recovered_btree_rows(&mut fresh, &cx, root).await, expected);
        fresh.rollback(&cx).await.unwrap();
        assert_eq!(pager.committed_tip().unwrap(), CommitSeq::new(2));
        assert_eq!(audit.edges()[0..2], before[0..2]);
        pager.close(&cx).unwrap();
        let mut storage = NativeRecovery::new(
            &cx, file(&vfs, &cx, "objects", &audit), file(&vfs, &cx, "markers", &audit),
            NativeDurabilityLimits::default(),
        ).unwrap();
        storage.prepare(&cx, |id, records| ready(codec().decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut replay = NativePageRecovery::new(storage.finish().unwrap(), codec(), 512, NativePageLimits::default());
        replay.prepare(&cx).await.unwrap();
        let (db, report) = replay.finish().unwrap();
        assert_eq!(report.markers.len(), 2);
        let pager = NativePager::new(db).unwrap();
        let mut fresh = pager.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert_eq!(recovered_btree_rows(&mut fresh, &cx, root).await, expected);
        fresh.rollback(&cx).await.unwrap();
        pager.close(&cx).unwrap();
    });
}
