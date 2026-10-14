#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::future_not_send)]
//! Real native log replay and immutable historical images. The source-only
//! fixture isolates history semantics; the final test uses authenticated RaptorQ.

use std::future::{Future, pending, poll_fn, ready};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    CommitMarker, CommitSeq, LockLevel, ObjectId, Oti, PageNumber, SymbolRecord,
    SymbolRecordFlags, TxnEpoch, TxnId, TxnToken, reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs, VfsFile};
use fsqlite_wal::native_commit::CommitSubmission;
use fsqlite_wal::native_commit::durable::{NativeCommitProof, NativeObjectCodec};
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{
    NativePageCapsule, NativePageLimits, NativePageRecovery, NativePageSnapshot,
    NativePageStore, NativePageWrite,
};

struct Codec;
impl NativeObjectCodec for Codec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
            0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START)])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records)
            .map_err(|error| FrankenError::WalCorrupt { detail: error.to_string() })?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::Abort);
        }
        Ok(bytes)
    }
}

#[derive(Default)]
struct Audit {
    reads: AtomicUsize,
    writes: AtomicUsize,
    syncs: AtomicUsize,
    closes: AtomicUsize,
    metadata: AtomicUsize,
    fail_sync_at: AtomicUsize,
    pause_read: AtomicBool,
}
impl Audit {
    fn mutations(&self) -> [usize; 3] {
        [&self.writes, &self.syncs, &self.closes].map(|v| v.load(Ordering::SeqCst))
    }
    fn all_io(&self) -> [usize; 5] {
        [&self.reads, &self.writes, &self.syncs, &self.closes, &self.metadata]
            .map(|v| v.load(Ordering::SeqCst))
    }
    fn barrier(&self) -> Result<()> {
        let ordinal = self.syncs.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_sync_at.load(Ordering::SeqCst) == ordinal {
            return Err(std::io::Error::other("injected online snapshot barrier failure").into());
        }
        Ok(())
    }
}
struct File { inner: <MemoryVfs as Vfs>::File, audit: Arc<Audit> }
impl VfsFile for File {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.audit.closes.fetch_add(1, Ordering::SeqCst); self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> {
        self.audit.metadata.fetch_add(1, Ordering::SeqCst); self.inner.file_identity()
    }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.audit.metadata.fetch_add(1, Ordering::SeqCst); self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        self.audit.reads.fetch_add(1, Ordering::SeqCst);
        if self.audit.pause_read.swap(false, Ordering::SeqCst) { return pending().await; }
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst); self.inner.write(cx, bytes, offset).await
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.audit.writes.fetch_add(1, Ordering::SeqCst); self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.audit.barrier()?; self.inner.sync(cx, flags)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        self.audit.barrier()?; self.inner.durable_sync(cx, kind)
    }
    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.audit.metadata.fetch_add(1, Ordering::SeqCst); self.inner.file_size(cx)
    }
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

type Store = NativePageStore<File, File, Codec>;
type Recovery = NativePageRecovery<File, File, Codec>;
type Log = NativeDurabilityLog<File, File>;
fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn file(vfs: &MemoryVfs, cx: &Cx, name: &str, audit: &Arc<Audit>) -> File {
    File { inner: vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0,
        audit: Arc::clone(audit) }
}
fn log(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>) -> Log {
    NativeDurabilityLog::create(cx, file(vfs, cx, "objects", audit), file(vfs, cx, "markers", audit),
        NativeDurabilityLimits::default()).unwrap()
}
fn store(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>) -> Store {
    Store::new(log(vfs, cx, audit), Codec, 512, NativePageLimits::default()).unwrap()
}
async fn put(db: &mut Store, cx: &Cx, updates: &[(u32, Option<u8>)], time: u64) {
    let mut tx = db.begin(cx).unwrap();
    for &(n, fill) in updates {
        let bytes = fill.map(|fill| [fill; 512]);
        db.write_page(cx, &mut tx, page(n), bytes.as_ref().map(|bytes| bytes.as_slice())).unwrap();
    }
    db.commit(cx, &mut tx, time).await.unwrap();
}
async fn recover(db: Store, cx: &Cx, limits: NativePageLimits) -> Recovery {
    let (mut physical, codec) = db.try_into_recovery().ok().expect("no live pins");
    physical.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    Recovery::with_shared_codec(physical.finish().unwrap(), codec, 512, limits)
}
fn assert_page(image: &NativePageSnapshot, n: u32, fill: Option<u8>) {
    match (image.read_page(page(n)), fill) {
        (Some(bytes), Some(fill)) => assert_eq!(bytes.as_ref(), &[fill; 512]),
        (None, None) => {}
        other => panic!("unexpected historical page {n}: {other:?}"),
    }
}

#[test]
fn historical_images_preserve_atomic_commits_deletions_and_sparse_extents() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = store(&vfs, &cx, &audit);
        put(&mut db, &cx, &[(1, Some(1)), (900_000, Some(9))], 100).await;
        put(&mut db, &cx, &[(1, Some(2)), (900_000, None)], 200).await;
        put(&mut db, &cx, &[(1, Some(3)), (900_001, Some(7))], 300).await;
        let mut replay = recover(db, &cx, NativePageLimits::default()).await;
        let before = audit.mutations();
        let genesis = replay.prepare_snapshot(&cx, CommitSeq::ZERO).await.unwrap();
        assert_eq!(genesis.page_count(), 0); assert_eq!(genesis.database_size_bound(), 0);
        assert_eq!(genesis.commit_time_unix_ns(), None);
        let first = replay.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap();
        let second = replay.prepare_snapshot(&cx, CommitSeq::new(2)).await.unwrap();
        assert_eq!(first.commit_time_unix_ns(), Some(100));
        assert_eq!(first.database_size_bound(), 900_000);
        assert_eq!(first.page_numbers().collect::<Vec<_>>(), vec![page(1), page(900_000)]);
        assert_eq!(first.retained_page_bytes(), 1024);
        assert_page(&first, 1, Some(1)); assert_page(&first, 900_000, Some(9));
        assert_page(&first, 900_001, None);
        assert_eq!(second.page_count(), 1); assert_eq!(second.database_size_bound(), 900_000);
        assert_page(&second, 1, Some(2)); assert_page(&second, 900_000, None);
        assert_eq!(audit.mutations(), before, "snapshot replay must only read");
        let (mut current, _) = replay.finish().unwrap();
        assert_eq!(current.committed_tip(), CommitSeq::new(3));
        let mut tx = current.begin(&cx).unwrap();
        assert_eq!(current.read_page(&cx, &mut tx, page(1)).unwrap().unwrap().as_ref(), &[3; 512]);
        current.rollback(&mut tx).unwrap();
        put(&mut current, &cx, &[(1, Some(4))], 400).await;
        current.reclaim_history(&cx, 10).unwrap();
        current.close(&cx).unwrap();
        assert_page(&first.clone(), 1, Some(1)); assert_page(&second, 1, Some(2));
    });
}

async fn append(
    log: &mut Log, cx: &Cx, sequence: u64, token: u64, observed: u64,
    time: u64, previous: Option<ObjectId>,
) -> ObjectId {
    let capsule = NativePageCapsule {
        page_size: 512, snapshot: CommitSeq::new(sequence - 1),
        reads: vec![(page(2), CommitSeq::new(observed))],
        writes: vec![NativePageWrite { page: page(2), data: Some(Arc::from(vec![u8::try_from(sequence).unwrap(); 512])) }],
    };
    let bytes = capsule.to_bytes().unwrap();
    let records = Codec.encode(cx, &bytes).unwrap();
    let proof = NativeCommitProof {
        commit_seq: CommitSeq::new(sequence), commit_time_unix_ns: time,
        submission: CommitSubmission {
            capsule_object_id: records[0].object_id, capsule_digest: *blake3::hash(&bytes).as_bytes(),
            write_set_pages: vec![page(2)], witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
            txn_token: TxnToken::new(TxnId::new(token).unwrap(), TxnEpoch::new(1)),
            begin_seq: capsule.snapshot,
        },
    };
    let proof_records = Codec.encode(cx, &proof.to_bytes().unwrap()).unwrap();
    let marker = CommitMarker::new(proof.commit_seq, time, records[0].object_id, proof_records[0].object_id, previous);
    log.append_symbols(cx, &records).await.unwrap();
    log.append_symbols(cx, &proof_records).await.unwrap();
    log.publish(cx, std::slice::from_ref(&marker), |id, records| ready(Codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes())
}
async fn replay_log(log: Log, cx: &Cx, limits: NativePageLimits) -> Recovery {
    let mut physical = log.into_recovery();
    physical.prepare(cx, |id, records| ready(Codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    Recovery::new(physical.finish().unwrap(), Codec, 512, limits)
}

#[test]
fn timestamp_selection_uses_inclusive_floor_and_last_equal_time() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut log = log(&vfs, &cx, &audit);
        let first = append(&mut log, &cx, 1, 9, 0, 100, None).await;
        let second = append(&mut log, &cx, 2, 3, 1, 100, Some(first)).await;
        append(&mut log, &cx, 3, 7, 2, 300, Some(second)).await;
        let mut replay = replay_log(log, &cx, NativePageLimits::default()).await;
        let before = audit.mutations();
        for (time, seq) in [(0, 0), (99, 0), (100, 2), (299, 2), (300, 3), (u64::MAX, 3)] {
            let image = replay.prepare_snapshot_at_time(&cx, time).await.unwrap();
            assert_eq!(image.commit_seq(), CommitSeq::new(seq));
            assert_page(&image, 2, if seq == 0 { None } else { Some(u8::try_from(seq).unwrap()) });
        }
        assert_eq!(audit.mutations(), before);
        replay.close(&cx).unwrap();
    });
}

#[test]
fn historical_capture_still_rejects_semantically_invalid_later_commits() {
    run(async {
        for duplicate in [false, true] {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
            let mut log = log(&vfs, &cx, &audit);
            let first = append(&mut log, &cx, 1, 1, 0, 100, None).await;
            let second = append(&mut log, &cx, 2, 2, 1, 200, Some(first)).await;
            append(&mut log, &cx, 3, if duplicate { 1 } else { 3 }, if duplicate { 2 } else { 1 }, 300, Some(second)).await;
            let mut replay = replay_log(log, &cx, NativePageLimits::default()).await;
            let before = audit.mutations();
            assert!(matches!(replay.prepare_snapshot(&cx, CommitSeq::new(1)).await, Err(FrankenError::WalCorrupt { .. })));
            let mut replay = replay.finish().err().expect("invalid suffix exposes neither image nor store");
            assert_eq!(audit.mutations(), before);
            replay.close(&cx).unwrap();
        }
    });
}

#[test]
fn retries_do_not_reuse_prior_preparation_after_bad_target_cancel_or_dropped_read() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = store(&vfs, &cx, &audit);
        put(&mut db, &cx, &[(2, Some(1))], 100).await;
        put(&mut db, &cx, &[(2, Some(2))], 200).await;
        let mut replay = recover(db, &cx, NativePageLimits::default()).await;
        let image = replay.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap();
        let before = audit.mutations();
        assert!(matches!(replay.prepare_snapshot(&cx, CommitSeq::new(3)).await, Err(FrankenError::OutOfRange { .. })));
        let mut replay = replay.finish().err().expect("bad target invalidates earlier preparation");
        replay.prepare(&cx).await.unwrap();
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(replay.prepare_snapshot(&cancelled, CommitSeq::new(1)).await, Err(FrankenError::Interrupt)));
        let mut replay = replay.finish().err().expect("cancel invalidates preparation");
        replay.prepare(&cx).await.unwrap();
        audit.pause_read.store(true, Ordering::SeqCst);
        {
            let mut future = std::pin::pin!(replay.prepare_snapshot(&cx, CommitSeq::new(1)));
            assert!(poll_fn(|task| Poll::Ready(future.as_mut().poll(task))).await.is_pending());
        }
        let mut replay = replay.finish().err().expect("dropped replay is not prepared");
        let retried = replay.prepare_snapshot(&cx, CommitSeq::new(2)).await.unwrap();
        assert_page(&image, 2, Some(1)); assert_page(&retried, 2, Some(2));
        assert_eq!(audit.mutations(), before);
        replay.close(&cx).unwrap();
    });
}

#[test]
fn bounded_replay_retains_selected_image_without_all_historical_versions() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = store(&vfs, &cx, &audit);
        for value in 1_u8..=64 { put(&mut db, &cx, &[(2, Some(value))], u64::from(value)).await; }
        let limits = NativePageLimits { max_retained_page_bytes: 512, max_versions: 1, ..NativePageLimits::default() };
        let mut replay = recover(db, &cx, limits).await;
        let image = replay.prepare_snapshot(&cx, CommitSeq::new(17)).await.unwrap();
        assert_eq!(image.retained_page_bytes(), 512); assert_page(&image, 2, Some(17));
        let (mut db, _) = replay.finish().unwrap();
        assert_eq!(db.retained_page_bytes(), 512); assert_eq!(db.retained_version_count(), 1);
        let mut view = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[64; 512]);
        db.rollback(&mut view).unwrap();
        let limits = NativePageLimits { max_retained_page_bytes: 511, ..limits };
        let mut replay = recover(db, &cx, limits).await;
        assert!(matches!(replay.prepare_snapshot(&cx, CommitSeq::new(1)).await, Err(FrankenError::TooBig)));
        let mut replay = replay.finish().err().expect("limits are not bypassed for historical reads");
        replay.close(&cx).unwrap(); assert_page(&image, 2, Some(17));
    });
}

#[test]
fn historical_read_of_complete_prefix_does_not_authorize_torn_tail_writes() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = store(&vfs, &cx, &audit);
        put(&mut db, &cx, &[(2, Some(1))], 100).await;
        let mut symbols = file(&vfs, &cx, "objects", &audit);
        let end = symbols.file_size(&cx).unwrap();
        symbols.write(&cx, b"torn", end).await.unwrap(); symbols.close(&cx).unwrap();
        let mut replay = recover(db, &cx, NativePageLimits::default()).await;
        assert!(replay.report().append_blocked());
        let before = audit.mutations();
        let image = replay.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap();
        assert_page(&image, 2, Some(1));
        let (mut db, report) = replay.finish().unwrap();
        assert_eq!(report.symbol_tail_bytes, 4); assert!(db.needs_recovery());
        assert!(matches!(db.begin(&cx), Err(FrankenError::BusyRecovery)));
        let before_capture = audit.all_io();
        assert!(matches!(db.capture_snapshot(&cx), Err(FrankenError::BusyRecovery)));
        assert_eq!(audit.all_io(), before_capture, "live capture cannot bypass a torn tail");
        assert_eq!(audit.mutations(), before); db.close(&cx).unwrap();
    });
}

#[cfg(feature = "native")]
#[test]
fn authenticated_raptorq_snapshots_reject_wrong_keys_and_keep_historical_bytes() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
        let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = NativePageStore::new(log(&vfs, &cx, &audit), RaptorQNativeCodec::new(Some([7; 32])),
            512, NativePageLimits::default()).unwrap();
        let mut live_images = Vec::new();
        for value in 1..=2 {
            let mut tx = db.begin(&cx).unwrap();
            for n in 1..=9 { db.write_page(&cx, &mut tx, page(n), Some(&[value; 512])).unwrap(); }
            db.commit(&cx, &mut tx, u64::from(value) * 100).await.unwrap();
            let before = audit.all_io();
            live_images.push(db.capture_snapshot(&cx).unwrap());
            assert_eq!(audit.all_io(), before, "capture must not run the codec or touch storage");
        }
        let (mut physical, codec) = db.try_into_recovery().ok().unwrap();
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut wrong = NativePageRecovery::new(physical.finish().unwrap(),
            RaptorQNativeCodec::new(Some([8; 32])), 512, NativePageLimits::default());
        let before = audit.mutations();
        assert!(wrong.prepare_snapshot(&cx, CommitSeq::new(1)).await.is_err());
        assert_eq!(audit.mutations(), before);
        let (mut physical, _) = wrong.into_recovery();
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut replay = NativePageRecovery::with_shared_codec(physical.finish().unwrap(), codec,
            512, NativePageLimits::default());
        let image = replay.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap();
        assert_eq!(image.page_count(), 9);
        for n in 1..=9 { assert_page(&image, n, Some(1)); }
        assert_eq!(live_images[0].commit_time_unix_ns(), image.commit_time_unix_ns());
        for n in 1..=9 {
            assert_eq!(live_images[0].read_page(page(n)), image.read_page(page(n)));
            assert_page(&live_images[1], n, Some(2));
        }
        let (mut current, _) = replay.finish().unwrap();
        assert_eq!(current.committed_tip(), CommitSeq::new(2)); current.close(&cx).unwrap();
    });
}

fn capture_without_io(db: &Store, cx: &Cx, audit: &Audit) -> NativePageSnapshot {
    let before = audit.all_io();
    let image = db.capture_snapshot(cx).unwrap();
    assert_eq!(audit.all_io(), before, "capture must perform no data or metadata I/O");
    image
}

#[test]
fn online_capture_excludes_private_writes_and_handles_pinned_old_versions() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let limits = NativePageLimits { max_active_transactions: 2, ..NativePageLimits::default() };
        let mut db = Store::new(log(&vfs, &cx, &audit), Codec, 512, limits).unwrap();
        let genesis = capture_without_io(&db, &cx, &audit);
        assert_eq!(genesis.commit_seq(), CommitSeq::ZERO);
        assert_eq!(genesis.commit_time_unix_ns(), None);
        assert_eq!(genesis.page_count(), 0);
        assert_eq!(genesis.database_size_bound(), 0);
        put(&mut db, &cx, &[(2, Some(2)), (900_000, Some(9))], 100).await;
        let mut pinned = db.begin(&cx).unwrap(); // No read is needed to pin old versions.
        let mut writer = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut writer, page(2), Some(&[0xA5; 512])).unwrap();
        db.write_page(&cx, &mut writer, page(900_001), Some(&[0xEE; 512])).unwrap();
        assert!(db.begin(&cx).is_err(), "the live transaction limit is actually full");
        let first = capture_without_io(&db, &cx, &audit);
        assert_eq!(first.commit_seq(), CommitSeq::new(1));
        assert_eq!(first.commit_time_unix_ns(), Some(100));
        assert_eq!(first.database_size_bound(), 900_000);
        assert_eq!(first.page_count(), 2);
        assert_page(&first, 2, Some(2)); assert_page(&first, 900_001, None);
        let receipt = db.commit(&cx, &mut writer, 50).await.unwrap().unwrap();
        assert_eq!(receipt.commit_time_unix_ns, 101);
        let second = capture_without_io(&db, &cx, &audit);
        assert_eq!(second.commit_seq(), receipt.commit_seq);
        assert_eq!(second.commit_time_unix_ns(), Some(receipt.commit_time_unix_ns));
        assert_eq!(second.retained_page_bytes(), 1536);
        assert!(db.retained_page_bytes() > second.retained_page_bytes(), "old pin still retains payloads");
        assert_page(&second, 2, Some(0xA5)); assert_page(&second, 900_001, Some(0xEE));
        assert_eq!(db.read_page(&cx, &mut pinned, page(2)).unwrap().unwrap().as_ref(), &[2; 512]);
        assert!(db.read_page(&cx, &mut pinned, page(900_001)).unwrap().is_none());
        put(&mut db, &cx, &[(2, Some(3)), (900_000, None)], 0).await;
        let third = capture_without_io(&db, &cx, &audit);
        assert_eq!(third.commit_seq(), CommitSeq::new(3));
        assert_eq!(third.commit_time_unix_ns(), Some(102));
        assert_eq!(third.database_size_bound(), 900_001);
        assert_eq!(third.page_numbers().collect::<Vec<_>>(), vec![page(2), page(900_001)]);
        assert_page(&third, 900_000, None);
        db.rollback(&mut pinned).unwrap();
        assert!(db.reclaim_history(&cx, 10).unwrap() > 0);
        db.close(&cx).unwrap();
        assert_page(&first, 2, Some(2)); assert_page(&first, 900_000, Some(9));
        assert_page(&second, 2, Some(0xA5)); assert_page(&second, 900_000, Some(9));
        assert_page(&third, 2, Some(3)); assert_page(&third, 900_000, None);
    });
}

#[test]
fn online_group_snapshot_matches_cold_replay_and_ignores_read_only_clock_samples() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = store(&vfs, &cx, &audit);
        put(&mut db, &cx, &[(2, Some(1))], 90).await;
        let mut a = db.begin(&cx).unwrap();
        let mut reader = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
        db.write_page(&cx, &mut b, page(3), Some(&[3; 512])).unwrap();
        let receipts = db.commit_batch(&cx, &mut [&mut a, &mut reader, &mut b], 100).await.unwrap();
        assert_eq!(receipts[0].as_ref().unwrap().commit_seq, CommitSeq::new(2));
        assert!(receipts[1].is_none());
        assert_eq!(receipts[2].as_ref().unwrap().commit_seq, CommitSeq::new(3));
        assert_eq!(receipts[2].as_ref().unwrap().commit_time_unix_ns, 101);
        let live = capture_without_io(&db, &cx, &audit);
        assert_eq!(live.commit_seq(), CommitSeq::new(3));
        assert_eq!(live.commit_time_unix_ns(), Some(101));
        assert_page(&live, 2, Some(2)); assert_page(&live, 3, Some(3));
        let mut reader = db.begin(&cx).unwrap();
        assert!(db.commit(&cx, &mut reader, u64::MAX).await.unwrap().is_none());
        assert_eq!(capture_without_io(&db, &cx, &audit).commit_time_unix_ns(), Some(101));
        let mut recovery = recover(db, &cx, NativePageLimits::default()).await;
        let cold = recovery.prepare_snapshot(&cx, live.commit_seq()).await.unwrap();
        assert_eq!(cold.commit_time_unix_ns(), live.commit_time_unix_ns());
        assert_eq!(cold.database_size_bound(), live.database_size_bound());
        assert_eq!(cold.page_numbers().collect::<Vec<_>>(), live.page_numbers().collect::<Vec<_>>());
        for page in live.page_numbers() { assert_eq!(cold.read_page(page), live.read_page(page)); }
        let (mut db, _) = recovery.finish().unwrap();
        assert_eq!(capture_without_io(&db, &cx, &audit).commit_time_unix_ns(), Some(101));
        put(&mut db, &cx, &[(2, Some(4))], 0).await;
        let next = capture_without_io(&db, &cx, &audit);
        assert_eq!(next.commit_seq(), CommitSeq::new(4));
        assert_eq!(next.commit_time_unix_ns(), Some(102));
        assert_page(&live, 2, Some(2)); assert_page(&next, 2, Some(4));
        db.close(&cx).unwrap();
    });
}

#[test]
fn online_images_do_not_pin_live_history_or_consume_transaction_capacity() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let limits = NativePageLimits {
            max_retained_page_bytes: 1024, max_versions: 2, max_active_transactions: 1,
        };
        let mut db = Store::new(log(&vfs, &cx, &audit), Codec, 512, limits).unwrap();
        let mut images = Vec::new();
        for value in 1_u8..=64 {
            put(&mut db, &cx, &[(2, Some(value))], u64::from(value)).await;
            images.push(capture_without_io(&db, &cx, &audit));
            db.reclaim_history(&cx, 1).unwrap();
            assert_eq!(db.retained_page_bytes(), 512);
            assert_eq!(db.retained_version_count(), 1);
        }
        db.close(&cx).unwrap();
        for (index, image) in images.iter().enumerate() {
            let value = u8::try_from(index + 1).unwrap();
            assert_eq!(image.commit_seq(), CommitSeq::new(u64::from(value)));
            assert_eq!(image.commit_time_unix_ns(), Some(u64::from(value)));
            assert_page(image, 2, Some(value));
        }
    });
}

#[test]
fn online_capture_refuses_failed_barriers_and_abandoned_publication_until_recovery() {
    use fsqlite_wal::native_pages::NativePageTransactionState;
    run(async {
        // Zero drops a pending validation read; one/two fail the real barriers.
        for barrier in 0..=2 {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
            let mut db = store(&vfs, &cx, &audit);
            put(&mut db, &cx, &[(2, Some(1))], 100).await;
            let before_failure = capture_without_io(&db, &cx, &audit);
            let mut tx = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut tx, page(2), Some(&[9; 512])).unwrap();
            if barrier == 0 {
                audit.pause_read.store(true, Ordering::SeqCst);
                {
                    let mut attempt = std::pin::pin!(db.commit(&cx, &mut tx, 200));
                    assert!(poll_fn(|task| Poll::Ready(attempt.as_mut().poll(task))).await.is_pending());
                }
                assert!(!audit.pause_read.load(Ordering::SeqCst), "the publication reached the injected read");
            } else {
                let fail_at = audit.syncs.load(Ordering::SeqCst) + barrier;
                audit.fail_sync_at.store(fail_at, Ordering::SeqCst);
                assert!(matches!(db.commit(&cx, &mut tx, 200).await, Err(FrankenError::Io(_))));
                assert_eq!(audit.syncs.load(Ordering::SeqCst), fail_at);
            }
            assert_eq!(tx.state(), NativePageTransactionState::Indeterminate);
            assert!(db.needs_recovery());
            let before_capture = audit.all_io();
            assert!(matches!(db.capture_snapshot(&cx), Err(FrankenError::BusyRecovery)));
            assert_eq!(audit.all_io(), before_capture);
            assert_eq!(tx.state(), NativePageTransactionState::Indeterminate);
            assert_page(&before_failure, 2, Some(1));
            audit.fail_sync_at.store(0, Ordering::SeqCst);
            drop(tx); // Abandoning the old handle does not declare its commit absent.
            let mut recovery = recover(db, &cx, NativePageLimits::default()).await;
            recovery.prepare(&cx).await.unwrap();
            let (mut db, _) = recovery.finish().unwrap();
            let image = capture_without_io(&db, &cx, &audit);
            assert_eq!(image.commit_seq(), CommitSeq::new(if barrier == 2 { 2 } else { 1 }));
            assert_eq!(image.commit_time_unix_ns(), Some(if barrier == 2 { 200 } else { 100 }));
            assert_page(&image, 2, Some(if barrier == 2 { 9 } else { 1 }));
            assert_page(&before_failure, 2, Some(1));
            db.close(&cx).unwrap();
        }
    });
}

#[test]
fn online_capture_cancellation_and_close_never_change_a_returned_image() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Audit::default());
        let mut db = store(&vfs, &cx, &audit);
        put(&mut db, &cx, &[(2, Some(7))], 100).await;
        let image = capture_without_io(&db, &cx, &audit);
        let cancelled = Cx::new(); cancelled.cancel();
        let before = audit.all_io();
        assert!(matches!(db.capture_snapshot(&cancelled), Err(FrankenError::Interrupt)));
        assert_eq!(audit.all_io(), before);
        assert!(!db.needs_recovery());
        assert_eq!(capture_without_io(&db, &cx, &audit).commit_seq(), image.commit_seq());
        db.close(&cx).unwrap();
        let before = audit.all_io();
        assert!(matches!(db.capture_snapshot(&cx), Err(FrankenError::BusyRecovery)));
        assert_eq!(audit.all_io(), before);
        assert_page(&image, 2, Some(7));
    });
}
