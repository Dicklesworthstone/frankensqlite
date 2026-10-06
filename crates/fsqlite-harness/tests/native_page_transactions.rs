//! Native page-store integration, not public SQL or power-loss certification.
use std::future::Future;
use std::path::Path;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{CommitSeq, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore, NativePageTransaction, NativePageTransactionState};

// The deterministic test codec is only for fault/state tests. The native file
// test below uses the real authenticated RaptorQ implementation.
#[derive(Clone, Copy)]
struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let t = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(t), al: 1, t, z: 1, n: 1 }, 0, bytes.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START)])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records)
            .map_err(|error| FrankenError::WalCorrupt { detail: error.to_string() })?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::WalCorrupt { detail: "test object identity mismatch".to_owned() });
        }
        Ok(bytes)
    }
}
fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn image(seed: u8) -> Vec<u8> { vec![seed; 512] }
fn open<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(cx, Some(Path::new(name)), VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn store<V: Vfs>(vfs: &V, cx: &Cx, limits: NativePageLimits) -> NativePageStore<V::File, V::File, TestCodec> {
    let log = NativeDurabilityLog::create(cx, open(vfs, cx, "objects"), open(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap();
    NativePageStore::new(log, TestCodec, 512, limits).unwrap()
}
fn read<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(store: &NativePageStore<S, M, C>, cx: &Cx, txn: &mut NativePageTransaction, p: u32) -> Option<Vec<u8>> {
    store.read_page(cx, txn, page(p)).unwrap().map(|bytes| bytes.to_vec())
}

#[test]
fn private_overlays_disjoint_writers_and_old_snapshots_survive_publication() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        let mut observer = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(1), Some(&image(11))).unwrap();
        db.write_page(&cx, &mut a, page(3), Some(&image(33))).unwrap();
        db.write_page(&cx, &mut b, page(2), Some(&image(22))).unwrap();
        assert_eq!(read(&db, &cx, &mut a, 1), Some(image(11)));
        assert_eq!(read(&db, &cx, &mut observer, 1), None);
        let receipt = db.commit(&cx, &mut a, 100).await.unwrap().unwrap();
        assert_eq!(receipt.commit_seq, CommitSeq::new(1));
        assert_eq!(vfs.sync_count(), 2);
        assert_eq!(read(&db, &cx, &mut observer, 1), None);
        assert_eq!(db.commit(&cx, &mut b, 200).await.unwrap().unwrap().commit_seq, CommitSeq::new(2));
        assert_eq!(read(&db, &cx, &mut observer, 2), None);
        let mut fresh = db.begin(&cx).unwrap();
        for (p, value) in [(1, 11), (2, 22), (3, 33)] {
            assert_eq!(read(&db, &cx, &mut fresh, p), Some(image(value)));
        }
        assert!(db.commit(&cx, &mut observer, 300).await.unwrap().is_none());
        assert_eq!(vfs.sync_count(), 4, "read-only completion must not append a marker");
        db.rollback(&mut fresh).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn write_skew_is_rejected_even_when_the_two_write_sets_are_disjoint() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut seed = db.begin(&cx).unwrap();
        for p in [1, 2] { db.write_page(&cx, &mut seed, page(p), Some(&image(1))).unwrap(); }
        db.commit(&cx, &mut seed, 100).await.unwrap();
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        for txn in [&mut a, &mut b] {
            assert_eq!(read(&db, &cx, txn, 1), Some(image(1)));
            assert_eq!(read(&db, &cx, txn, 2), Some(image(1)));
        }
        db.write_page(&cx, &mut a, page(1), Some(&image(0))).unwrap();
        db.write_page(&cx, &mut b, page(2), Some(&image(0))).unwrap();
        db.commit(&cx, &mut a, 200).await.unwrap();
        let syncs = vfs.sync_count();
        assert!(matches!(db.commit(&cx, &mut b, 201).await, Err(FrankenError::BusySnapshot { .. })));
        assert_eq!(b.state(), NativePageTransactionState::Active);
        assert_eq!(vfs.sync_count(), syncs);
        assert!(!db.needs_recovery());
        db.rollback(&mut b).unwrap();
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(read(&db, &cx, &mut fresh, 1), Some(image(0)));
        assert_eq!(read(&db, &cx, &mut fresh, 2), Some(image(1)));
        db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn missing_page_observation_detects_an_insert_delete_cycle() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut old = db.begin(&cx).unwrap();
        assert_eq!(read(&db, &cx, &mut old, 3), None);
        let mut insert = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut insert, page(3), Some(&image(7))).unwrap();
        db.commit(&cx, &mut insert, 100).await.unwrap();
        let mut delete = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut delete, page(3), None).unwrap();
        db.commit(&cx, &mut delete, 200).await.unwrap();
        assert_eq!(read(&db, &cx, &mut old, 3), None);
        db.write_page(&cx, &mut old, page(5), Some(&image(9))).unwrap();
        assert!(matches!(db.commit(&cx, &mut old, 300).await, Err(FrankenError::BusySnapshot { .. })));
        assert_eq!(vfs.sync_count(), 4);
        db.rollback(&mut old).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn deleting_a_page_does_not_destroy_a_retained_snapshot_image() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut seed = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut seed, page(1), Some(&image(1))).unwrap();
        db.commit(&cx, &mut seed, 100).await.unwrap();
        let mut old = db.begin(&cx).unwrap();
        let mut update = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut update, page(1), Some(&image(2))).unwrap();
        db.commit(&cx, &mut update, 200).await.unwrap();
        let mut delete = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut delete, page(1), None).unwrap();
        assert_eq!(read(&db, &cx, &mut delete, 1), None);
        db.commit(&cx, &mut delete, 300).await.unwrap();
        assert_eq!(read(&db, &cx, &mut old, 1), Some(image(1)));
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(read(&db, &cx, &mut fresh, 1), None);
        db.rollback(&mut old).unwrap(); db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn rollback_is_private_and_foreign_or_completed_handles_are_rejected() {
    run(async {
        let cx = Cx::new(); let va = MemoryVfs::new(); let vb = MemoryVfs::new();
        let mut a = store(&va, &cx, NativePageLimits::default());
        let mut b = store(&vb, &cx, NativePageLimits::default());
        let mut txn = a.begin(&cx).unwrap();
        a.write_page(&cx, &mut txn, page(1), Some(&image(9))).unwrap();
        assert!(b.read_page(&cx, &mut txn, page(1)).is_err());
        assert!(b.commit(&cx, &mut txn, 100).await.is_err());
        assert!(b.rollback(&mut txn).is_err());
        assert_eq!(txn.state(), NativePageTransactionState::Active);
        a.rollback(&mut txn).unwrap(); a.rollback(&mut txn).unwrap();
        assert!(a.read_page(&cx, &mut txn, page(1)).is_err());
        let mut fresh = a.begin(&cx).unwrap();
        assert_eq!(read(&a, &cx, &mut fresh, 1), None);
        assert!(a.write_page(&cx, &mut fresh, page(1), Some(&[0; 511])).is_err());
        a.write_page(&cx, &mut fresh, page(1), Some(&image(2))).unwrap();
        a.commit(&cx, &mut fresh, 100).await.unwrap();
        assert!(a.commit(&cx, &mut fresh, 101).await.is_err());
        assert!(a.rollback(&mut fresh).is_err());
        a.close(&cx).unwrap(); b.close(&cx).unwrap();
    });
}

#[test]
fn active_session_slots_are_released_on_rollback_and_drop() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx, NativePageLimits { max_active_transactions: 1, ..NativePageLimits::default() });
        let mut a = db.begin(&cx).unwrap();
        assert!(matches!(db.begin(&cx), Err(FrankenError::Busy)));
        db.rollback(&mut a).unwrap();
        let b = db.begin(&cx).unwrap(); drop(b);
        let mut c = db.begin(&cx).unwrap();
        assert!(c.token().id.get() > a.token().id.get());
        db.rollback(&mut c).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn retained_payload_and_version_limits_refuse_before_storage_mutation() {
    run(async {
        for limits in [
            NativePageLimits { max_retained_page_bytes: 512, ..NativePageLimits::default() },
            NativePageLimits { max_versions: 1, ..NativePageLimits::default() },
        ] {
            let cx = Cx::new(); let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut db = store(&vfs, &cx, limits);
            let mut seed = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut seed, page(1), Some(&image(1))).unwrap();
            db.commit(&cx, &mut seed, 100).await.unwrap();
            let mut rejected = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut rejected, page(2), Some(&image(2))).unwrap();
            let mut objects = open(&vfs, &cx, "objects");
            let before = objects.file_size(&cx).unwrap();
            assert!(matches!(db.commit(&cx, &mut rejected, 200).await, Err(FrankenError::TooBig)));
            assert_eq!(objects.file_size(&cx).unwrap(), before);
            assert_eq!(vfs.sync_count(), 2);
            assert_eq!(db.committed_tip(), CommitSeq::new(1));
            assert_eq!(rejected.state(), NativePageTransactionState::Active);
            db.rollback(&mut rejected).unwrap(); objects.close(&cx).unwrap(); db.close(&cx).unwrap();
        }
    });
}

#[test]
fn both_sync_failures_have_indeterminate_not_rolled_back_outcomes() {
    run(async {
        for (path, sync, recovered_seq) in [("objects", 1, 0), ("markers", 2, 1)] {
            let cx = Cx::new(); let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut db = store(&vfs, &cx, NativePageLimits::default());
            let mut txn = db.begin(&cx).unwrap();
            for p in [1, 2] { db.write_page(&cx, &mut txn, page(p), Some(&image(9))).unwrap(); }
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(sync).build());
            assert!(db.commit(&cx, &mut txn, 100).await.is_err());
            assert!(vfs.is_powered_off());
            assert!(db.needs_recovery());
            assert_eq!(txn.state(), NativePageTransactionState::Indeterminate);
            assert!(db.rollback(&mut txn).is_err());
            assert_eq!(db.committed_tip(), CommitSeq::ZERO);
            vfs.power_on(); db.close(&cx).unwrap();
            let (mut recovered, report) = NativePageStore::recover(&cx,
                open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec, 512,
                NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
            assert_eq!(recovered.committed_tip(), CommitSeq::new(recovered_seq));
            assert_eq!(report.markers.len(), usize::try_from(recovered_seq).unwrap());
            let mut reader = recovered.begin(&cx).unwrap();
            for p in [1, 2] {
                assert_eq!(read(&recovered, &cx, &mut reader, p), if recovered_seq == 0 { None } else { Some(image(9)) });
            }
            recovered.rollback(&mut reader).unwrap(); recovered.close(&cx).unwrap();
        }
    });
}

#[test]
fn torn_marker_bytes_remain_retained_and_block_new_page_transactions() {
    run(async {
        let cx = Cx::new(); let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut txn = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut txn, page(1), Some(&image(1))).unwrap();
        vfs.inject_fault(FaultSpec::torn_write("markers").valid_bytes(17).build());
        assert!(db.commit(&cx, &mut txn, 100).await.is_err()); db.close(&cx).unwrap();
        let (mut recovered, report) = NativePageStore::recover(&cx,
            open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec, 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert_eq!(report.marker_tail_bytes, 17);
        assert_eq!(recovered.committed_tip(), CommitSeq::ZERO);
        assert!(recovered.begin(&cx).is_err());
        let mut markers = open(&vfs, &cx, "markers");
        assert_eq!(markers.file_size(&cx).unwrap(), 17);
        markers.close(&cx).unwrap(); recovered.close(&cx).unwrap();
    });
}

#[test]
fn recovery_rejects_a_stored_history_that_bypassed_page_read_validation() {
    use fsqlite_types::{TxnEpoch, TxnId, TxnToken};
    use fsqlite_wal::native_commit::CommitSubmission;
    use fsqlite_wal::native_commit::durable::DurableWriteCoordinator;
    use fsqlite_wal::native_pages::{NativePageCapsule, NativePageWrite};
    use std::sync::Arc;
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        let log = NativeDurabilityLog::create(&cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), NativeDurabilityLimits::default()).unwrap();
        let mut unchecked = DurableWriteCoordinator::new(log, TestCodec, 1).unwrap();
        for p in [1_u32, 2] {
            let capsule = NativePageCapsule {
                page_size: 512, snapshot: CommitSeq::ZERO,
                reads: (1..=p).map(|p| (page(p), CommitSeq::ZERO)).collect(),
                writes: vec![NativePageWrite { page: page(p), data: Some(Arc::from(image(9))) }],
            };
            let bytes = capsule.to_bytes().unwrap();
            let symbols = TestCodec.encode(&cx, &bytes).unwrap();
            let sub = CommitSubmission {
                capsule_object_id: symbols[0].object_id, capsule_digest: *blake3::hash(&bytes).as_bytes(),
                write_set_pages: vec![page(p)], witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
                txn_token: TxnToken::new(TxnId::new(u64::from(p)).unwrap(), TxnEpoch::new(1)), begin_seq: CommitSeq::ZERO,
            };
            unchecked.stage_symbols(&cx, &symbols).await.unwrap();
            let seq = unchecked.queue(&cx, sub, u64::from(p)).unwrap();
            unchecked.flush(&cx, |_, _| Ok(())).await.unwrap();
            unchecked.take_committed(seq).unwrap();
        }
        unchecked.close(&cx).unwrap();
        let result = NativePageStore::recover(&cx,
            open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec, 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await;
        assert!(matches!(result, Err(FrankenError::WalCorrupt { .. })), "a committed stale read is corrupt history, not a retryable conflict");
    });
}

#[test]
fn cancellation_before_staging_preserves_the_private_overlay() {
    run(async {
        let cx = Cx::new(); let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut txn = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut txn, page(1), Some(&image(3))).unwrap();
        cx.cancel();
        assert!(matches!(db.commit(&cx, &mut txn, 100).await, Err(FrankenError::Interrupt)));
        assert_eq!(txn.state(), NativePageTransactionState::Active);
        assert_eq!(vfs.sync_count(), 0);
        assert!(!db.needs_recovery());
        db.rollback(&mut txn).unwrap(); db.close(&Cx::new()).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn native_files_recover_page_images_and_deletions_after_source_symbol_erasure() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let objects = dir.path().join("objects"); let markers = dir.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default()).unwrap();
        let mut db = NativePageStore::new(log, RaptorQNativeCodec::new(Some([7; 32])), 512, NativePageLimits::default()).unwrap();
        let mut txn = db.begin(&cx).unwrap();
        for (p, seed) in [(1, 11), (2, 22)] { db.write_page(&cx, &mut txn, page(p), Some(&image(seed))).unwrap(); }
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        // Damage the first stored capsule symbol permanently. Recovery must use
        // the authentic surviving repair symbols, not re-read a source buffer.
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let mut file = vfs.open(&cx, Some(&objects), flags).unwrap().0;
        file.write(&cx, &[0xFF], 51).await.unwrap(); file.close(&cx).unwrap();
        let (mut recovered, report) = NativePageStore::recover(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert_eq!(report.erased_symbols, 1);
        let mut old = recovered.begin(&cx).unwrap();
        assert_eq!(read(&recovered, &cx, &mut old, 1), Some(image(11)));
        assert_eq!(read(&recovered, &cx, &mut old, 2), Some(image(22)));
        let mut delete = recovered.begin(&cx).unwrap();
        recovered.write_page(&cx, &mut delete, page(1), None).unwrap();
        recovered.commit(&cx, &mut delete, 200).await.unwrap();
        assert_eq!(read(&recovered, &cx, &mut old, 1), Some(image(11)));
        recovered.rollback(&mut old).unwrap(); recovered.close(&cx).unwrap();
        let (mut again, _) = NativePageStore::recover(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert_eq!(again.committed_tip(), CommitSeq::new(2));
        let mut latest = again.begin(&cx).unwrap();
        assert_eq!(read(&again, &cx, &mut latest, 1), None);
        assert_eq!(read(&again, &cx, &mut latest, 2), Some(image(22)));
        again.rollback(&mut latest).unwrap(); again.close(&cx).unwrap();
    });
}
