//! Native coordinator/log integration. The systematic test codec below tests
//! sequencing and fault handling, not fountain repair or power-loss hardware.
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{CommitSeq, ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags, TxnEpoch, TxnId, TxnToken, reconstruct_systematic_happy_path};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_commit::{CommitResult, CommitSubmission};
use fsqlite_wal::native_commit::durable::{DurableCommitError, DurableWriteCoordinator, NativeCommitCandidate, NativeCommitProof, NativeObjectCodec};
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};

#[derive(Clone, Copy)]
struct SystematicTestCodec;
impl NativeObjectCodec for SystematicTestCodec {
    fn encode(&self, _: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> {
        let t = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(payload),
            Oti { f: u64::from(t), al: 1, t, z: 1, n: 1 },
            0, payload.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let payload = reconstruct_systematic_happy_path(records)
            .map_err(|error| FrankenError::WalCorrupt { detail: error.to_string() })?;
        if ObjectId::derive_from_canonical_bytes(&payload) != id {
            return Err(FrankenError::WalCorrupt { detail: "test object identity mismatch".to_owned() });
        }
        Ok(payload)
    }
}

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn open<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> V::File {
    vfs.open(cx, Some(Path::new(path)), VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn driver<V: Vfs>(vfs: &V, cx: &Cx, limit: usize) -> DurableWriteCoordinator<V::File, V::File, SystematicTestCodec> {
    let log = NativeDurabilityLog::create(cx, open(vfs, cx, "objects"), open(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap();
    DurableWriteCoordinator::new(log, SystematicTestCodec, limit).unwrap()
}
fn payload(page: u32, seed: u32) -> Vec<u8> {
    [page.to_le_bytes(), seed.to_le_bytes()].concat()
}
fn submission(page: u32, seed: u32, begin: u64) -> CommitSubmission {
    let payload = payload(page, seed);
    CommitSubmission {
        capsule_object_id: ObjectId::derive_from_canonical_bytes(&payload),
        capsule_digest: *blake3::hash(&payload).as_bytes(),
        write_set_pages: vec![PageNumber::new(page).unwrap()],
        witness_refs: Vec::new(), edge_ids: Vec::new(), merge_witness_ids: Vec::new(),
        txn_token: TxnToken::new(TxnId::new(u64::from(seed) + 1).unwrap(), TxnEpoch::new(1)),
        begin_seq: CommitSeq::new(begin),
    }
}
async fn stage<S: VfsFile, M: VfsFile>(driver: &mut DurableWriteCoordinator<S, M, SystematicTestCodec>, cx: &Cx, page: u32, seed: u32) {
    driver.stage_symbols(cx, &SystematicTestCodec.encode(cx, &payload(page, seed)).unwrap()).await.unwrap();
}
fn validate(candidates: &[NativeCommitCandidate], objects: &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()> {
    for candidate in candidates {
        let sub = &candidate.proof.submission;
        if candidate.capsule.len() != 8 || sub.write_set_pages.len() != 1
            || candidate.capsule[..4] != sub.write_set_pages[0].get().to_le_bytes()
        {
            return Err(FrankenError::Abort);
        }
        for id in sub.witness_refs.iter().chain(&sub.edge_ids).chain(&sub.merge_witness_ids) {
            assert!(objects.contains_key(id), "validation must receive stored evidence");
        }
    }
    Ok(())
}

#[test]
fn native_proof_binds_all_fields_and_rejects_malformed_wire() {
    let mut sub = submission(3, 5, 4);
    sub.witness_refs = vec![ObjectId::from_bytes([1; 16])];
    sub.edge_ids = vec![ObjectId::from_bytes([2; 16])];
    sub.merge_witness_ids = vec![ObjectId::from_bytes([3; 16])];
    let proof = NativeCommitProof { commit_seq: CommitSeq::new(6), commit_time_unix_ns: 42, submission: sub };
    let bytes = proof.to_bytes().unwrap();
    assert_eq!(bytes.len(), 108 + 4 + 3 * 16);
    assert_eq!(&bytes[..8], b"FNCP\x01\0\0\0");
    assert_eq!(NativeCommitProof::from_bytes(&bytes).unwrap(), proof);
    for end in 0..bytes.len() { assert!(NativeCommitProof::from_bytes(&bytes[..end]).is_err()); }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(NativeCommitProof::from_bytes(&trailing).is_err());
    for (offset, replacement) in [(0, 0), (4, 2)] {
        let mut corrupt = bytes.clone(); corrupt[offset] = replacement;
        assert!(NativeCommitProof::from_bytes(&corrupt).is_err());
    }
    for (start, end) in [(72, 80), (108, 112)] { // zero TxnId / PageNumber
        let mut corrupt = bytes.clone(); corrupt[start..end].fill(0);
        assert!(NativeCommitProof::from_bytes(&corrupt).is_err());
    }
    let mut counts = bytes.clone(); counts[92..96].fill(0xFF);
    assert!(NativeCommitProof::from_bytes(&counts).is_err());
    let mut future = bytes.clone(); future[84..92].copy_from_slice(&6_u64.to_le_bytes());
    assert!(NativeCommitProof::from_bytes(&future).is_err());
}

#[test]
fn queued_writers_need_physical_sync_and_keep_individual_acknowledgements() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut coordinator = driver(&vfs, &cx, 8);
        for seed in 1..=3 {
            stage(&mut coordinator, &cx, seed, seed).await;
            assert_eq!(coordinator.queue(&cx, submission(seed, seed, 0), 100).unwrap(), CommitSeq::new(u64::from(seed)));
        }
        assert_eq!(coordinator.committed_tip(), CommitSeq::ZERO);
        assert_eq!(vfs.sync_count(), 0);
        assert!(coordinator.take_committed(CommitSeq::new(1)).is_none());
        let receipt = coordinator.flush(&cx, validate).await.unwrap().unwrap();
        assert_eq!(receipt.commits, 3);
        assert_eq!(vfs.sync_count(), 2);
        assert_eq!(coordinator.committed_tip(), CommitSeq::new(3));
        assert_eq!(coordinator.take_committed(CommitSeq::new(3)).unwrap().txn_token, submission(3, 3, 0).txn_token);
        assert_eq!(coordinator.pending_count(), 2);
        assert!(coordinator.take_committed(CommitSeq::new(3)).is_none());
        assert!(coordinator.take_committed(CommitSeq::new(1)).is_some());
        assert!(coordinator.take_committed(CommitSeq::new(2)).is_some());
        assert!(coordinator.flush(&cx, |_, _| -> Result<()> { panic!("empty flush validated a nonexistent batch") }).await.unwrap().is_none());
        assert_eq!(vfs.sync_count(), 2);
        coordinator.close(&cx).unwrap();
    });
}

#[test]
fn pending_fcw_future_snapshot_and_capacity_do_not_lose_sequence_numbers() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut coordinator = driver(&vfs, &cx, 2);
        stage(&mut coordinator, &cx, 1, 1).await;
        stage(&mut coordinator, &cx, 2, 2).await;
        coordinator.queue(&cx, submission(1, 1, 0), 100).unwrap();
        assert!(matches!(coordinator.queue(&cx, submission(1, 9, 0), 100), Err(DurableCommitError::Rejected(CommitResult::ConflictFcw { .. }))));
        assert!(coordinator.queue(&cx, submission(2, 2, 1), 100).is_err());
        assert!(coordinator.queue(&cx, submission(1, 1, 0), 100).is_err());
        assert_eq!(coordinator.queue(&cx, submission(2, 2, 0), 100).unwrap(), CommitSeq::new(2));
        assert!(coordinator.queue(&cx, submission(3, 3, 0), 100).is_err());
        assert!(coordinator.close(&cx).is_err());
        coordinator.flush(&cx, validate).await.unwrap();
        assert!(coordinator.queue(&cx, submission(3, 3, 2), 100).is_err(), "uncollected results still count against capacity");
        coordinator.take_committed(CommitSeq::new(1)).unwrap();
        stage(&mut coordinator, &cx, 3, 3).await;
        assert_eq!(coordinator.queue(&cx, submission(3, 3, 2), 50).unwrap(), CommitSeq::new(3));
        coordinator.initiate_shutdown();
        coordinator.flush(&cx, validate).await.unwrap();
        assert_eq!(coordinator.take_committed(CommitSeq::new(3)).unwrap().commit_time_unix_ns, 102);
        coordinator.take_committed(CommitSeq::new(2)).unwrap();
        assert!(matches!(coordinator.queue(&cx, submission(4, 4, 3), 200), Err(DurableCommitError::Rejected(CommitResult::ShuttingDown))));
        coordinator.close(&cx).unwrap();
    });
}

#[test]
fn final_validation_failure_does_not_write_proofs_or_advance_visibility() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut coordinator = driver(&vfs, &cx, 8);
        stage(&mut coordinator, &cx, 1, 1).await;
        coordinator.queue(&cx, submission(1, 1, 0), 100).unwrap();
        let mut reader = open(&vfs, &cx, "objects");
        let before = reader.file_size(&cx).unwrap();
        assert!(coordinator.flush(&cx, |_, _| Err::<(), _>(FrankenError::Abort)).await.is_err());
        assert_eq!(reader.file_size(&cx).unwrap(), before);
        assert_eq!(vfs.sync_count(), 0);
        assert_eq!(coordinator.committed_tip(), CommitSeq::ZERO);
        assert!(!coordinator.needs_recovery());
        assert!(coordinator.take_committed(CommitSeq::new(1)).is_none());
        coordinator.flush(&cx, validate).await.unwrap();
        assert!(coordinator.take_committed(CommitSeq::new(1)).is_some());
        reader.close(&cx).unwrap();
        coordinator.close(&cx).unwrap();
    });
}

#[test]
fn capsule_digest_and_page_summary_are_both_checked_before_publication() {
    run(async {
        for wrong_digest in [true, false] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut coordinator = driver(&vfs, &cx, 8);
            stage(&mut coordinator, &cx, 1, 1).await;
            let mut sub = submission(1, 1, 0);
            if wrong_digest { sub.capsule_digest[0] ^= 1; }
            else { sub.write_set_pages = vec![PageNumber::new(2).unwrap()]; }
            coordinator.queue(&cx, sub, 100).unwrap();
            assert!(coordinator.flush(&cx, validate).await.is_err());
            assert_eq!(vfs.sync_count(), 0);
            assert_eq!(coordinator.committed_tip(), CommitSeq::ZERO);
            assert!(coordinator.take_committed(CommitSeq::new(1)).is_none());
        }
    });
}

#[test]
fn immediate_witness_edge_and_merge_dependencies_must_exist_in_storage() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut coordinator = driver(&vfs, &cx, 8);
        stage(&mut coordinator, &cx, 1, 1).await;
        let evidence = SystematicTestCodec.encode(&cx, b"witness evidence").unwrap();
        let mut sub = submission(1, 1, 0);
        sub.witness_refs = vec![evidence[0].object_id];
        sub.edge_ids = sub.witness_refs.clone();
        sub.merge_witness_ids = sub.witness_refs.clone();
        coordinator.queue(&cx, sub, 100).unwrap();
        assert!(coordinator.flush(&cx, validate).await.is_err());
        assert_eq!(vfs.sync_count(), 0);
        coordinator.stage_symbols(&cx, &evidence).await.unwrap();
        coordinator.flush(&cx, validate).await.unwrap();
        coordinator.take_committed(CommitSeq::new(1)).unwrap();
        coordinator.close(&cx).unwrap();
    });
}

#[test]
fn failed_proof_append_latches_recovery_and_never_completes_the_writer() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut coordinator = driver(&vfs, &cx, 8);
        stage(&mut coordinator, &cx, 1, 1).await;
        coordinator.queue(&cx, submission(1, 1, 0), 100).unwrap();
        vfs.inject_fault(FaultSpec::write_failure("objects").build());
        assert!(coordinator.flush(&cx, validate).await.is_err());
        assert_eq!(vfs.triggered_faults().len(), 1);
        assert_eq!(vfs.sync_count(), 0);
        assert!(coordinator.needs_recovery());
        assert_eq!(coordinator.pending_count(), 1);
        assert!(coordinator.take_committed(CommitSeq::new(1)).is_none());
        assert!(coordinator.flush(&cx, validate).await.is_err());
        assert!(coordinator.queue(&cx, submission(2, 2, 0), 100).is_err());
        coordinator.close(&cx).unwrap();
    });
}

#[test]
fn either_sync_failure_blocks_new_commits_but_preserves_earlier_replies() {
    run(async {
        for (path, advance) in [("objects", 1), ("markers", 2)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut coordinator = driver(&vfs, &cx, 8);
            stage(&mut coordinator, &cx, 1, 1).await;
            coordinator.queue(&cx, submission(1, 1, 0), 100).unwrap();
            coordinator.flush(&cx, validate).await.unwrap();
            stage(&mut coordinator, &cx, 2, 2).await;
            coordinator.queue(&cx, submission(2, 2, 1), 200).unwrap();
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(vfs.sync_count() + advance).build());
            assert!(coordinator.flush(&cx, validate).await.is_err());
            assert!(vfs.is_powered_off());
            assert_eq!(coordinator.committed_tip(), CommitSeq::new(1));
            assert!(coordinator.take_committed(CommitSeq::new(2)).is_none());
            assert_eq!(coordinator.take_committed(CommitSeq::new(1)).unwrap().txn_token, submission(1, 1, 0).txn_token);
            vfs.power_on();
            assert!(coordinator.flush(&cx, validate).await.is_err());
            coordinator.close(&cx).unwrap();
        }
    });
}
