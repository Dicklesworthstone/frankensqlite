//! Live-store evacuation through actual byte I/O and source-owned completions.
//! MemoryVfs tests model failure/ownership, not physical power-loss durability.

use std::future::{Future, pending, poll_fn, ready};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_types::{
    CommitSeq, LockLevel, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags,
    reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{
    FileIdentity, MemoryVfs, ShmRegion, SyncKind, Vfs, VfsFile, VfsWriteCompletion,
    VfsWriteCompletionState,
};

use super::{NativeObjectCodec, NativePageLimits, NativePageRecovery, NativePageStore};
use crate::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use crate::native_pages::NativePageTransactionState;

type MemoryFile = <MemoryVfs as Vfs>::File;
type Store = NativePageStore<File, File, Codec>;
const PATHS: [&str; 2] = ["objects", "markers"];

struct HeldWrite {
    stream: usize,
    offset: u64,
    bytes: Vec<u8>,
    completion: VfsWriteCompletion,
}

#[derive(Default)]
struct Audit {
    // Read, write, sync, close and truncate attempts, by stream.
    counts: [[usize; 5]; 2],
    fail_sync: Option<usize>,
    fail_close: Option<usize>,
    hold_write: Option<usize>,
    cancel_after_write: bool,
    held: Option<HeldWrite>,
}

struct File {
    inner: MemoryFile,
    stream: usize,
    audit: Arc<Mutex<Audit>>,
}

impl VfsFile for File {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        let mut audit = self.audit.lock().unwrap();
        audit.counts[self.stream][3] += 1;
        if audit.fail_close == Some(self.stream) {
            audit.fail_close = None;
            return Err(std::io::Error::other("injected close failure").into());
        }
        drop(audit);
        self.inner.close(cx)
    }
    fn file_identity(&self) -> Result<Option<FileIdentity>> { self.inner.file_identity() }
    fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
        self.inner.refresh_file_identity()
    }
    async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
        self.audit.lock().unwrap().counts[self.stream][0] += 1;
        self.inner.read(cx, bytes, offset).await
    }
    async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
        self.audit.lock().unwrap().counts[self.stream][1] += 1;
        self.inner.write(cx, bytes, offset).await?;
        let cancel = {
            let mut audit = self.audit.lock().unwrap();
            std::mem::take(&mut audit.cancel_after_write)
        };
        if cancel { cx.cancel(); }
        Ok(())
    }
    async fn write_tracked<'a>(
        &'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64, completion: VfsWriteCompletion,
    ) -> Result<()> {
        let hold = {
            let mut audit = self.audit.lock().unwrap();
            if audit.hold_write == Some(self.stream) {
                audit.hold_write = None;
                assert!(audit.held.is_none());
                audit.counts[self.stream][1] += 1;
                audit.held = Some(HeldWrite {
                    stream: self.stream, offset, bytes: bytes.to_vec(),
                    completion: completion.clone(),
                });
                true
            } else {
                false
            }
        };
        if hold {
            return pending::<Result<()>>().await;
        }
        let result = self.write(cx, bytes, offset).await;
        if result.is_ok() { completion.complete_success(); } else { completion.complete_error(); }
        result
    }
    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.audit.lock().unwrap().counts[self.stream][4] += 1;
        self.inner.truncate(cx, size)
    }
    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.before_sync()?;
        self.inner.sync(cx, flags)
    }
    fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        self.before_sync()?;
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

impl File {
    fn before_sync(&self) -> Result<()> {
        let mut audit = self.audit.lock().unwrap();
        audit.counts[self.stream][2] += 1;
        if audit.fail_sync == Some(self.stream) {
            audit.fail_sync = None;
            return Err(std::io::Error::other("injected uncertain sync").into());
        }
        Ok(())
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
            .map_err(|error| FrankenError::Internal(error.to_string()))?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::Abort);
        }
        Ok(bytes)
    }
}

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn raw(vfs: &MemoryVfs, cx: &Cx, stream: usize) -> MemoryFile {
    vfs.open(cx, Some(Path::new(PATHS[stream])),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn log(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Mutex<Audit>>) -> NativeDurabilityLog<File, File> {
    let file = |stream| File { inner: raw(vfs, cx, stream), stream, audit: Arc::clone(audit) };
    NativeDurabilityLog::create(cx, file(0), file(1), NativeDurabilityLimits::default()).unwrap()
}
async fn seed(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Mutex<Audit>>) -> Store {
    let mut db = Store::new(log(vfs, cx, audit), Codec, 512, NativePageLimits::default()).unwrap();
    let mut txn = db.begin(cx).unwrap();
    db.write_page(cx, &mut txn, page(2), Some(&[1; 512])).unwrap();
    db.write_page(cx, &mut txn, page(900), None).unwrap();
    db.commit(cx, &mut txn, 100).await.unwrap();
    db
}
async fn bytes(vfs: &MemoryVfs, cx: &Cx, stream: usize) -> Vec<u8> {
    let mut file = raw(vfs, cx, stream);
    let len = usize::try_from(file.file_size(cx).unwrap()).unwrap();
    let mut bytes = vec![0; len];
    assert_eq!(file.read(cx, &mut bytes, 0).await.unwrap(), len);
    file.close(cx).unwrap();
    bytes
}
async fn settle(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Mutex<Audit>>) {
    let held = audit.lock().unwrap().held.take().expect("actual retained source write");
    let mut file = raw(vfs, cx, held.stream);
    file.write(cx, &held.bytes, held.offset).await.unwrap();
    held.completion.complete_success();
    file.close(cx).unwrap();
}
async fn rebuild(
    cx: &Cx,
    mut recovery: crate::native_durability::NativeRecovery<File, File>,
    codec: Arc<Codec>,
) -> (Store, usize) {
    recovery.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ())))
        .await.unwrap();
    let mut pages = NativePageRecovery::with_shared_codec(
        recovery.finish().unwrap(), codec, 512, NativePageLimits::default(),
    );
    pages.prepare(cx).await.unwrap();
    let (db, report) = pages.finish().unwrap();
    (db, report.markers.len())
}

#[test]
fn live_transfer_refuses_unread_snapshots_without_changing_the_healthy_store() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
        let mut db = seed(&vfs, &cx, &audit).await;
        let original_codec = Arc::clone(&db.codec);
        let mut unread = db.begin(&cx).unwrap();
        let mut writer = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut writer, page(2), Some(&[2; 512])).unwrap();
        let before = audit.lock().unwrap().counts;
        let db = db.try_into_recovery().err().expect("both pins must refuse transfer");
        assert_eq!(audit.lock().unwrap().counts, before);
        assert_eq!(db.read_page(&cx, &mut writer, page(2)).unwrap().unwrap().as_ref(), &[2; 512]);
        db.rollback(&mut writer).unwrap();
        let db = db.try_into_recovery().err().expect("unread snapshot still pins its owner");
        assert_eq!(db.read_page(&cx, &mut unread, page(2)).unwrap().unwrap().as_ref(), &[1; 512]);
        db.rollback(&mut unread).unwrap();
        let before = audit.lock().unwrap().counts;
        let (recovery, codec) = db.try_into_recovery().ok().expect("terminal handles release pins");
        assert_eq!(audit.lock().unwrap().counts, before);
        assert!(Arc::ptr_eq(&original_codec, &codec));
        let (mut db, count) = rebuild(&cx, recovery, codec).await;
        assert_eq!(count, 1);
        assert_eq!(db.committed_tip(), CommitSeq::new(1));
        assert!(db.read_page(&cx, &mut unread, page(2)).is_err());
        assert!(db.write_page(&cx, &mut writer, page(2), Some(&[3; 512])).is_err());
        let mut next = db.begin(&cx).unwrap();
        assert_eq!(db.allocate_page(&cx, &mut next).unwrap(), page(901));
        db.rollback(&mut next).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn both_uncertain_barriers_recover_on_original_handles_then_accept_new_commits() {
    run(async {
        for failed_stream in 0..2 {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
            let mut db = seed(&vfs, &cx, &audit).await;
            let mut txn = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut txn, page(2), Some(&[2; 512])).unwrap();
            audit.lock().unwrap().fail_sync = Some(failed_stream);
            assert!(db.commit(&cx, &mut txn, 200).await.is_err());
            assert_eq!(txn.state(), NativePageTransactionState::Indeterminate);
            assert!(db.needs_recovery());
            assert!(matches!(db.rollback(&mut txn), Err(FrankenError::BusyRecovery)));
            let db = db.try_into_recovery().err().expect("uncertain handle still owns its pin");
            drop(txn);
            let before = audit.lock().unwrap().counts;
            let (recovery, codec) = db.try_into_recovery().ok().expect("quiesced uncertain store");
            assert_eq!(audit.lock().unwrap().counts, before, "transfer is not recovery I/O");
            let (mut db, commits) = rebuild(&cx, recovery, codec).await;
            assert_eq!(commits, failed_stream + 1, "only a written marker can be recovered");
            let mut old = db.begin(&cx).unwrap();
            let expected = if failed_stream == 0 { 1 } else { 2 };
            assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[expected; 512]);
            assert!(db.read_page(&cx, &mut old, page(900)).unwrap().is_none());
            let mut a = db.begin(&cx).unwrap();
            let mut b = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut a, page(2), Some(&[3; 512])).unwrap();
            db.write_page(&cx, &mut b, page(2), Some(&[4; 512])).unwrap();
            let ack = db.commit(&cx, &mut a, 0).await.unwrap().unwrap();
            assert_eq!(ack.commit_seq.get(), u64::try_from(commits).unwrap() + 1);
            assert!(ack.commit_time_unix_ns > if failed_stream == 0 { 100 } else { 200 });
            assert!(matches!(db.commit(&cx, &mut b, 500).await, Err(FrankenError::BusySnapshot { .. })));
            assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[expected; 512]);
            db.rollback(&mut b).unwrap(); db.rollback(&mut old).unwrap();
            assert_eq!(audit.lock().unwrap().counts[0][3], 0);
            assert_eq!(audit.lock().unwrap().counts[1][3], 0);
            db.close(&cx).unwrap();
        }
    });
}

#[test]
fn dropped_source_writes_keep_their_completion_through_live_store_evacuation() {
    run(async {
        for held_stream in 0..2 {
            let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
            let mut db = seed(&vfs, &cx, &audit).await;
            let mut txn = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut txn, page(2), Some(&[2; 512])).unwrap();
            audit.lock().unwrap().hold_write = Some(held_stream);
            {
                let mut commit = std::pin::pin!(db.commit(&cx, &mut txn, 200));
                assert!(poll_fn(|cx| Poll::Ready(commit.as_mut().poll(cx))).await.is_pending());
            }
            assert_eq!(txn.state(), NativePageTransactionState::Indeterminate);
            let original = db.outstanding_write().expect("source token survives waiter drop");
            assert_eq!(original.state(), VfsWriteCompletionState::Pending);
            drop(txn);
            let before = audit.lock().unwrap().counts;
            let (mut recovery, codec) = db.try_into_recovery().ok().expect("move, never settle, the source");
            let transferred = recovery.outstanding_write().unwrap();
            assert_eq!(transferred.state(), VfsWriteCompletionState::Pending);
            assert!(matches!(recovery.prepare(&cx, |id, records| {
                ready(codec.decode(&cx, id, &records).map(|_| ()))
            }).await, Err(FrankenError::BusyRecovery)));
            assert!(matches!(recovery.close(&cx), Err(FrankenError::BusyRecovery)));
            assert_eq!(audit.lock().unwrap().counts, before, "no I/O before source settlement");
            settle(&vfs, &cx, &audit).await;
            assert_eq!(original.state(), VfsWriteCompletionState::Success);
            assert_eq!(transferred.state(), original.state(), "same source-owned completion");
            let (mut db, count) = rebuild(&cx, recovery, codec).await;
            assert_eq!(count, held_stream + 1);
            let mut view = db.begin(&cx).unwrap();
            let expected = if held_stream == 0 { 1 } else { 2 };
            assert_eq!(db.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[expected; 512]);
            db.rollback(&mut view).unwrap(); db.close(&cx).unwrap();
        }
    });
}

#[test]
fn cancellation_after_successful_staging_retains_page_owned_admission() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
        let mut db = seed(&vfs, &cx, &audit).await;
        let mut txn = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut txn, page(2), Some(&[2; 512])).unwrap();
        audit.lock().unwrap().cancel_after_write = true;
        assert!(matches!(db.commit(&cx, &mut txn, 200).await, Err(FrankenError::Interrupt)));
        assert_eq!(txn.state(), NativePageTransactionState::Indeterminate);
        assert!(db.needs_recovery());
        // The log accepted its stage, but no publication flush started.
        // Its healthy queue must not mask the page owner's failed attempt.
        drop(txn);
        let before = audit.lock().unwrap().counts;
        let (recovery, codec) = db.try_into_recovery().ok().expect("propagate page-owned uncertainty");
        assert_eq!(audit.lock().unwrap().counts, before);
        let fresh = Cx::new();
        let (mut db, count) = rebuild(&fresh, recovery, codec).await;
        assert_eq!(count, 1);
        let mut txn = db.begin(&fresh).unwrap();
        assert_eq!(db.read_page(&fresh, &mut txn, page(2)).unwrap().unwrap().as_ref(), &[1; 512]);
        db.rollback(&mut txn).unwrap(); db.close(&fresh).unwrap();
    });
}

#[test]
fn live_transfer_cannot_forget_the_original_acknowledged_prefix() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
        let db = seed(&vfs, &cx, &audit).await;
        let original = bytes(&vfs, &cx, 1).await;
        let (mut recovery, codec) = db.try_into_recovery().ok().unwrap();
        let mut markers = raw(&vfs, &cx, 1);
        markers.truncate(&cx, 0).unwrap();
        let before = audit.lock().unwrap().counts;
        for _ in 0..2 {
            let error = recovery.prepare(&cx, |id, records| {
                ready(codec.decode(&cx, id, &records).map(|_| ()))
            }).await.unwrap_err();
            assert!(matches!(error, FrankenError::WalCorrupt { .. }));
            assert!(error.to_string().contains("acknowledged commit prefix"));
            let now = audit.lock().unwrap().counts;
            assert_eq!(now[0][1..], before[0][1..]);
            assert_eq!(now[1][1..], before[1][1..]);
        }
        markers.write(&cx, &original, 0).await.unwrap();
        markers.close(&cx).unwrap();
        let (mut db, count) = rebuild(&cx, recovery, codec).await;
        assert_eq!(count, 1);
        db.close(&cx).unwrap();
    });
}

#[test]
fn started_close_is_not_reclassified_as_recovery_and_its_failures_remain_retryable() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
        let mut db = seed(&vfs, &cx, &audit).await;
        audit.lock().unwrap().fail_close = Some(0);
        assert!(db.close(&cx).is_err());
        let before = audit.lock().unwrap().counts;
        let mut db = db.try_into_recovery().err().expect("retain partially closed owner");
        assert_eq!(audit.lock().unwrap().counts, before);
        db.close(&cx).unwrap();
        let now = audit.lock().unwrap().counts;
        assert_eq!(now[0][3], 2, "retry failed symbols close");
        assert_eq!(now[1][3], 1, "do not repeat successful markers close");
        assert!(db.try_into_recovery().is_err());
    });
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
#[test]
fn authenticated_codec_survives_live_evacuation_without_reopening_or_copying_keys() {
    use crate::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let vfs = MemoryVfs::new(); let audit = Arc::new(Mutex::new(Audit::default()));
        let mut db = NativePageStore::new(
            log(&vfs, &cx, &audit), RaptorQNativeCodec::new(Some([7; 32])),
            512, NativePageLimits::default(),
        ).unwrap();
        let mut txn = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut txn, page(2), Some(&[0xA5; 512])).unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let original_codec = Arc::clone(&db.codec);
        let (mut recovery, codec) = db.try_into_recovery().ok().unwrap();
        assert!(Arc::ptr_eq(&original_codec, &codec));
        recovery.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
            .await.unwrap();
        let mut replay = NativePageRecovery::with_shared_codec(
            recovery.finish().unwrap(), codec, 512, NativePageLimits::default(),
        );
        replay.prepare(&cx).await.unwrap();
        let (mut db, _) = replay.finish().unwrap();
        let mut next = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut next, page(2)).unwrap().unwrap().as_ref(), &[0xA5; 512]);
        db.write_page(&cx, &mut next, page(3), Some(&[0x5A; 512])).unwrap();
        db.commit(&cx, &mut next, 101).await.unwrap();
        assert_eq!(db.committed_tip(), CommitSeq::new(2));
        assert_eq!(audit.lock().unwrap().counts[0][3], 0);
        assert_eq!(audit.lock().unwrap().counts[1][3], 0);
        db.close(&cx).unwrap();
    });
}
