//! Explicit native page groups: real VFS barrier counts and failure boundaries.
//! These tests are not public SQL, cross-process or hardware power-loss proofs.
#![allow(clippy::future_not_send)] // The storage stack uses caller-driven !Send futures.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{
    COMMIT_MARKER_RECORD_V1_SIZE, CommitSeq, ObjectId, Oti, PageNumber, SymbolRecord,
    SymbolRecordFlags, reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_commit::durable::{NativeCommitProof, NativeObjectCodec};
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{
    NativePageLimits, NativePageStore, NativePageTransaction, NativePageTransactionState,
};

// Deliberately simple codec for state/ordering tests, not a fountain-code substitute.
// The Unix test uses the production authenticated RaptorQ implementation instead.
#[derive(Clone, Default)]
struct TestCodec {
    fail_second_proof: Arc<AtomicUsize>,
}
impl NativeObjectCodec for TestCodec {
    fn encode(&self, cx: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        if bytes.starts_with(b"FNCP") && NativeCommitProof::from_bytes(bytes)?.commit_seq.get() == 2 {
            match self.fail_second_proof.swap(0, Ordering::Relaxed) {
                1 => return Err(FrankenError::Abort),
                2 => cx.cancel(),
                _ => {}
            }
        }
        let t = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(t), al: 1, t, z: 1, n: 1 },
            0,
            bytes.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }

    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records).map_err(|error| {
            FrankenError::WalCorrupt { detail: error.to_string() }
        })?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id {
            return Err(FrankenError::WalCorrupt { detail: "test object identity mismatch".to_owned() });
        }
        Ok(bytes)
    }
}

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn page(number: u32) -> PageNumber { PageNumber::new(number).unwrap() }
fn open<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn store<V: Vfs>(vfs: &V, cx: &Cx, limits: NativePageLimits)
    -> NativePageStore<V::File, V::File, TestCodec>
{
    let log = NativeDurabilityLog::create(cx, open(vfs, cx, "objects"),
        open(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap();
    NativePageStore::new(log, TestCodec::default(), 512, limits).unwrap()
}
fn read<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    store: &NativePageStore<S, M, C>, cx: &Cx, txn: &mut NativePageTransaction, number: u32,
) -> Option<Vec<u8>> {
    store.read_page(cx, txn, page(number)).unwrap().map(|data| data.to_vec())
}
fn lengths<V: Vfs>(vfs: &V, cx: &Cx) -> (u64, u64) {
    let mut objects = open(vfs, cx, "objects");
    let mut markers = open(vfs, cx, "markers");
    let result = (objects.file_size(cx).unwrap(), markers.file_size(cx).unwrap());
    objects.close(cx).unwrap();
    markers.close(cx).unwrap();
    result
}

#[test]
fn independent_writers_share_exactly_two_syncs_and_keep_identity_and_old_snapshots() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut old = db.begin(&cx).unwrap(); // Pin before any read.
        let mut transactions = Vec::new();
        for number in 2..=13 {
            let mut txn = db.begin(&cx).unwrap();
            if number != 6 { // Include a read-only handle in the middle.
                db.write_page(&cx, &mut txn, page(number), Some(&[u8::try_from(number).unwrap(); 512])).unwrap();
            }
            transactions.push(txn);
        }
        let tokens: Vec<_> = transactions.iter().map(NativePageTransaction::token).collect();
        assert_eq!(db.committed_tip(), CommitSeq::ZERO);
        assert_eq!(vfs.sync_count(), 0);
        let replies = db.commit_batch(&cx, &mut transactions.iter_mut().collect::<Vec<_>>(), 100)
            .await.unwrap();
        assert_eq!(replies.len(), transactions.len());
        assert_eq!(vfs.sync_count(), 2, "the whole writer group must share both barriers");
        let mut sequence = 0_u64;
        for (index, reply) in replies.iter().enumerate() {
            assert_eq!(transactions[index].state(), NativePageTransactionState::Committed);
            if index == 4 {
                assert!(reply.is_none());
            } else {
                sequence += 1;
                let reply = reply.as_ref().unwrap();
                assert_eq!(reply.txn_token, tokens[index]);
                assert_eq!(reply.commit_seq, CommitSeq::new(sequence));
                assert_eq!(reply.commit_time_unix_ns, 99 + sequence);
            }
        }
        assert_eq!(db.committed_tip(), CommitSeq::new(11));
        assert_eq!(lengths(&vfs, &cx).1, u64::try_from(11 * COMMIT_MARKER_RECORD_V1_SIZE).unwrap());
        let mut fresh = db.begin(&cx).unwrap();
        for number in 2..=13 {
            assert_eq!(read(&db, &cx, &mut old, number), None);
            let expected = (number != 6).then(|| vec![u8::try_from(number).unwrap(); 512]);
            assert_eq!(read(&db, &cx, &mut fresh, number), expected);
        }
        db.rollback(&mut old).unwrap();
        db.rollback(&mut fresh).unwrap();
        assert!(db.commit_batch(&cx, &mut [], 999).await.unwrap().is_empty());
        assert_eq!(vfs.sync_count(), 2);
        db.close(&cx).unwrap();
    });
}

#[test]
fn intra_group_write_skew_or_overlap_rejects_every_member_before_storage_changes() {
    run(async {
        for overlap in [false, true] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut db = store(&vfs, &cx, NativePageLimits::default());
            let mut a = db.begin(&cx).unwrap();
            let mut b = db.begin(&cx).unwrap();
            for txn in [&mut a, &mut b] {
                assert_eq!(read(&db, &cx, txn, 2), None);
                assert_eq!(read(&db, &cx, txn, 3), None);
            }
            db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
            db.write_page(&cx, &mut b, page(if overlap { 2 } else { 3 }), Some(&[3; 512])).unwrap();
            let before = lengths(&vfs, &cx);
            assert!(matches!(db.commit_batch(&cx, &mut [&mut a, &mut b], 100).await,
                Err(FrankenError::BusySnapshot { .. })));
            assert_eq!(lengths(&vfs, &cx), before);
            assert_eq!(vfs.sync_count(), 0);
            assert_eq!(a.state(), NativePageTransactionState::Active);
            assert_eq!(b.state(), NativePageTransactionState::Active);
            assert_eq!(db.retained_version_count(), 0);
            assert!(!db.needs_recovery());
            assert_eq!(db.commit(&cx, &mut a, 10).await.unwrap().unwrap().commit_seq, CommitSeq::new(1));
            assert!(matches!(db.commit(&cx, &mut b, 11).await, Err(FrankenError::BusySnapshot { .. })));
            db.rollback(&mut b).unwrap();
            db.close(&cx).unwrap();
        }
    });
}

#[test]
fn one_direction_read_dependency_is_admitted_only_in_the_valid_commit_order() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut reader_writer = db.begin(&cx).unwrap();
        let mut later_writer = db.begin(&cx).unwrap();
        assert_eq!(read(&db, &cx, &mut reader_writer, 3), None);
        db.write_page(&cx, &mut reader_writer, page(2), Some(&[2; 512])).unwrap();
        db.write_page(&cx, &mut later_writer, page(3), Some(&[3; 512])).unwrap();
        assert!(matches!(db.commit_batch(&cx, &mut [&mut later_writer, &mut reader_writer], 100).await,
            Err(FrankenError::BusySnapshot { .. })));
        assert_eq!(vfs.sync_count(), 0);
        let replies = db.commit_batch(&cx, &mut [&mut reader_writer, &mut later_writer], 10).await.unwrap();
        assert_eq!(replies[0].as_ref().unwrap().commit_seq, CommitSeq::new(1));
        assert_eq!(replies[1].as_ref().unwrap().commit_seq, CommitSeq::new(2));
        db.close(&cx).unwrap();
        let (mut recovered, report) = NativePageStore::recover(&cx,
            open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec::default(), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert_eq!(report.markers.len(), 2);
        let mut view = recovered.begin(&cx).unwrap();
        assert_eq!(read(&recovered, &cx, &mut view, 2), Some(vec![2; 512]));
        assert_eq!(read(&recovered, &cx, &mut view, 3), Some(vec![3; 512]));
        recovered.rollback(&mut view).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn combined_page_budget_preserves_pins_and_allows_the_same_group_to_retry() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let limits = NativePageLimits { max_versions: 4, max_retained_page_bytes: 2048,
            ..NativePageLimits::default() };
        let mut db = store(&vfs, &cx, limits);
        let mut seed = db.begin(&cx).unwrap();
        for number in [2, 3] { db.write_page(&cx, &mut seed, page(number), Some(&[1; 512])).unwrap(); }
        db.commit(&cx, &mut seed, 1).await.unwrap();
        let mut old = db.begin(&cx).unwrap();
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
        db.write_page(&cx, &mut b, page(3), Some(&[2; 512])).unwrap();
        db.commit_batch(&cx, &mut [&mut a, &mut b], 2).await.unwrap();
        let mut c = db.begin(&cx).unwrap();
        let mut d = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut c, page(2), Some(&[3; 512])).unwrap();
        db.write_page(&cx, &mut d, page(3), Some(&[3; 512])).unwrap();
        let before = lengths(&vfs, &cx);
        let syncs = vfs.sync_count();
        assert!(matches!(db.commit_batch(&cx, &mut [&mut c, &mut d], 4).await, Err(FrankenError::TooBig)));
        assert_eq!(lengths(&vfs, &cx), before);
        assert_eq!(vfs.sync_count(), syncs);
        assert_eq!(c.state(), NativePageTransactionState::Active);
        assert_eq!(d.state(), NativePageTransactionState::Active);
        assert_eq!(read(&db, &cx, &mut old, 2), Some(vec![1; 512]));
        assert_eq!(read(&db, &cx, &mut old, 3), Some(vec![1; 512]));
        db.rollback(&mut old).unwrap();
        db.commit_batch(&cx, &mut [&mut c, &mut d], 4).await.unwrap();
        assert_eq!(db.committed_tip(), CommitSeq::new(5));
        assert_eq!(db.retained_version_count(), 4);
        assert_eq!(db.retained_page_bytes(), 2048);
        assert_eq!(vfs.sync_count(), syncs + 2);
        db.close(&cx).unwrap();
    });
}

#[test]
fn failed_second_proof_or_cancelled_admission_leaves_all_overlays_retryable() {
    run(async {
        for failure in [1, 2] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let codec = TestCodec::default();
            codec.fail_second_proof.store(failure, Ordering::Relaxed);
            let log = NativeDurabilityLog::create(&cx, open(&vfs, &cx, "objects"),
                open(&vfs, &cx, "markers"), NativeDurabilityLimits::default()).unwrap();
            let mut db = NativePageStore::new(log, codec, 512, NativePageLimits::default()).unwrap();
            let mut a = db.begin(&cx).unwrap();
            let mut b = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
            db.write_page(&cx, &mut b, page(3), Some(&[3; 512])).unwrap();
            assert!(db.commit_batch(&cx, &mut [&mut a, &mut b], 500).await.is_err());
            assert_eq!(a.state(), NativePageTransactionState::Active);
            assert_eq!(b.state(), NativePageTransactionState::Active);
            assert!(!db.needs_recovery());
            let fresh = Cx::new();
            assert_eq!(lengths(&vfs, &fresh), (0, 0));
            assert_eq!(db.retained_version_count(), 0);
            let mut observer = db.begin(&fresh).unwrap();
            assert_eq!(observer.snapshot_db_size(), 0, "failed preparation must not publish empty slots");
            db.rollback(&mut observer).unwrap();
            let replies = db.commit_batch(&fresh, &mut [&mut a, &mut b], 10).await.unwrap();
            assert_eq!(replies[0].as_ref().unwrap().commit_seq, CommitSeq::new(1));
            assert_eq!(replies[0].as_ref().unwrap().commit_time_unix_ns, 10);
            assert_eq!(replies[1].as_ref().unwrap().commit_seq, CommitSeq::new(2));
            db.close(&fresh).unwrap();
        }
    });
}

#[test]
fn foreign_finished_and_unpolled_group_handles_never_publish_a_prefix() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let other_vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut other = store(&other_vfs, &cx, NativePageLimits::default());
        let mut a = db.begin(&cx).unwrap();
        let mut foreign = other.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
        assert!(db.commit_batch(&cx, &mut [&mut a, &mut foreign], 100).await.is_err());
        let mut done = db.begin(&cx).unwrap();
        db.rollback(&mut done).unwrap();
        assert!(db.commit_batch(&cx, &mut [&mut a, &mut done], 100).await.is_err());
        {
            let mut group = [&mut a];
            drop(db.commit_batch(&cx, &mut group, 100));
        }
        assert_eq!(a.state(), NativePageTransactionState::Active);
        assert!(a.is_page_dirty(page(2)));
        assert_eq!(lengths(&vfs, &cx), (0, 0));
        assert_eq!(vfs.sync_count(), 0);
        db.rollback(&mut a).unwrap();
        other.rollback(&mut foreign).unwrap();
        db.close(&cx).unwrap();
        other.close(&cx).unwrap();
    });
}

#[test]
fn both_sync_failures_leave_all_writers_indeterminate_until_actual_log_recovery() {
    run(async {
        for (path, ordinal, recovered_count) in [("objects", 1, 0), ("markers", 2, 2)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut db = store(&vfs, &cx, NativePageLimits::default());
            let mut a = db.begin(&cx).unwrap();
            let mut reader = db.begin(&cx).unwrap();
            let mut b = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
            db.write_page(&cx, &mut b, page(3), Some(&[3; 512])).unwrap();
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(ordinal).build());
            assert!(db.commit_batch(&cx, &mut [&mut a, &mut reader, &mut b], 100).await.is_err());
            assert!(vfs.is_powered_off());
            assert_eq!(db.committed_tip(), CommitSeq::ZERO);
            assert_eq!(db.retained_version_count(), 0);
            assert!(db.needs_recovery());
            for txn in [&mut a, &mut b] {
                assert_eq!(txn.state(), NativePageTransactionState::Indeterminate);
                assert!(db.rollback(txn).is_err());
            }
            assert_eq!(reader.state(), NativePageTransactionState::Active);
            db.rollback(&mut reader).unwrap();
            vfs.power_on();
            db.close(&cx).unwrap();
            let (mut recovered, report) = NativePageStore::recover(&cx,
                open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec::default(), 512,
                NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
            assert_eq!(report.markers.len(), recovered_count);
            assert_eq!(recovered.committed_tip().get(), u64::try_from(recovered_count).unwrap());
            let mut view = recovered.begin(&cx).unwrap();
            assert_eq!(read(&recovered, &cx, &mut view, 2), (recovered_count != 0).then(|| vec![2; 512]));
            assert_eq!(read(&recovered, &cx, &mut view, 3), (recovered_count != 0).then(|| vec![3; 512]));
            recovered.rollback(&mut view).unwrap();
            recovered.close(&cx).unwrap();
        }
    });
}

#[test]
fn torn_group_marker_recovers_only_its_complete_prefix_and_retains_the_tail() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut db = store(&vfs, &cx, NativePageLimits::default());
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
        db.write_page(&cx, &mut b, page(3), Some(&[3; 512])).unwrap();
        vfs.inject_fault(FaultSpec::torn_write("markers")
            .valid_bytes(COMMIT_MARKER_RECORD_V1_SIZE + 17).build());
        assert!(db.commit_batch(&cx, &mut [&mut a, &mut b], 100).await.is_err());
        assert_eq!(vfs.triggered_faults().len(), 1);
        assert_eq!(vfs.sync_count(), 1);
        assert_eq!(a.state(), NativePageTransactionState::Indeterminate);
        assert_eq!(b.state(), NativePageTransactionState::Indeterminate);
        assert_eq!(db.committed_tip(), CommitSeq::ZERO);
        let before = lengths(&vfs, &cx);
        db.close(&cx).unwrap();
        let (mut recovered, report) = NativePageStore::recover(&cx,
            open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec::default(), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(report.markers[0].commit_seq, CommitSeq::new(1));
        assert_eq!(report.marker_tail_bytes, 17);
        assert_eq!(recovered.committed_tip(), CommitSeq::new(1));
        assert_eq!(recovered.retained_version_count(), 1);
        assert_eq!(recovered.retained_page_bytes(), 512);
        assert!(recovered.needs_recovery());
        assert!(recovered.begin(&cx).is_err());
        assert_eq!(lengths(&vfs, &cx), before, "recovery must not truncate evidence");
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn admitted_group_that_fails_storage_preflight_can_close_without_faking_rollback() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let log = NativeDurabilityLog::create(&cx, open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"), NativeDurabilityLimits {
                max_symbol_bytes: 1, ..NativeDurabilityLimits::default()
            }).unwrap();
        let mut db = NativePageStore::new(log, TestCodec::default(), 512, NativePageLimits::default()).unwrap();
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(2), Some(&[2; 512])).unwrap();
        db.write_page(&cx, &mut b, page(3), Some(&[3; 512])).unwrap();
        assert!(matches!(db.commit_batch(&cx, &mut [&mut a, &mut b], 100).await, Err(FrankenError::TooBig)));
        assert!(db.needs_recovery());
        assert_eq!(lengths(&vfs, &cx), (0, 0));
        assert_eq!(vfs.sync_count(), 0);
        db.close(&cx).unwrap(); // No permanently stuck healthy-but-queued driver.
        assert_eq!(a.state(), NativePageTransactionState::Indeterminate);
        assert!(db.rollback(&mut a).is_err());
        let (mut recovered, report) = NativePageStore::recover(&cx,
            open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"), TestCodec::default(), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert!(report.markers.is_empty());
        assert_eq!(recovered.committed_tip(), CommitSeq::ZERO);
        recovered.close(&cx).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn authenticated_file_group_reopens_and_continues_the_same_marker_chain() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;

    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let directory = tempfile::tempdir().unwrap();
        let object_path = directory.path().join("objects");
        let marker_path = directory.path().join("markers");
        let vfs = fsqlite_vfs::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx,
            vfs.open(&cx, Some(&object_path), flags).unwrap().0,
            vfs.open(&cx, Some(&marker_path), flags).unwrap().0,
            NativeDurabilityLimits::default()).unwrap();
        let mut db = NativePageStore::new(log, RaptorQNativeCodec::new(Some([7; 32])),
            512, NativePageLimits::default()).unwrap();
        let mut transactions = Vec::new();
        for number in 2..=5 {
            let mut txn = db.begin(&cx).unwrap();
            db.write_page(&cx, &mut txn, page(number), Some(&[u8::try_from(number).unwrap(); 512])).unwrap();
            transactions.push(txn);
        }
        db.commit_batch(&cx, &mut transactions.iter_mut().collect::<Vec<_>>(), 100).await.unwrap();
        db.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut recovered, report) = NativePageStore::recover(&cx,
            vfs.open(&cx, Some(&object_path), flags).unwrap().0,
            vfs.open(&cx, Some(&marker_path), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])), 512,
            NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap();
        assert_eq!(report.markers.len(), 4);
        for pair in report.markers.windows(2) {
            assert_eq!(pair[1].prev_marker, Some(ObjectId::derive_from_canonical_bytes(&pair[0].to_record_bytes())));
        }
        let mut a = recovered.begin(&cx).unwrap();
        let mut b = recovered.begin(&cx).unwrap();
        assert_eq!(read(&recovered, &cx, &mut a, 2), Some(vec![2; 512]));
        assert_eq!(read(&recovered, &cx, &mut b, 3), Some(vec![3; 512]));
        recovered.write_page(&cx, &mut a, page(2), None).unwrap();
        recovered.write_page(&cx, &mut b, page(3), Some(&[9; 512])).unwrap();
        let replies = recovered.commit_batch(&cx, &mut [&mut a, &mut b], 1).await.unwrap();
        assert_eq!(replies[0].as_ref().unwrap().commit_seq, CommitSeq::new(5));
        assert_eq!(replies[1].as_ref().unwrap().commit_time_unix_ns, 105);
        recovered.close(&cx).unwrap();
    });
}
