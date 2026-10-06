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

fn validate_recovered(candidate: &NativeCommitCandidate, objects: &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()> {
    validate(std::slice::from_ref(candidate), objects)
}

#[test]
fn recovery_restores_fcw_clocks_and_chain_from_the_stored_proof() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut original = driver(&vfs, &cx, 8);
        stage(&mut original, &cx, 7, 1).await;
        original.queue(&cx, submission(7, 1, 0), 500).unwrap();
        original.flush(&cx, validate).await.unwrap();
        original.close(&cx).unwrap();
        let (mut recovered, report) = DurableWriteCoordinator::recover(
            &cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(), SystematicTestCodec, 8, validate_recovered,
        ).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(recovered.committed_tip(), CommitSeq::new(1));
        assert_eq!(recovered.pending_count(), 0);
        assert!(recovered.take_committed(CommitSeq::new(1)).is_none(), "reopen must not invent outstanding acknowledgements");
        assert!(matches!(recovered.queue(&cx, submission(7, 2, 0), 1),
            Err(DurableCommitError::Rejected(CommitResult::ConflictFcw { .. }))));
        stage(&mut recovered, &cx, 7, 2).await;
        let second = recovered.queue(&cx, submission(7, 2, 1), 1).unwrap();
        assert_eq!(second, CommitSeq::new(2));
        recovered.flush(&cx, validate).await.unwrap();
        assert_eq!(recovered.take_committed(second).unwrap().commit_time_unix_ns, 501);
        recovered.close(&cx).unwrap();
        let (mut again, report) = DurableWriteCoordinator::recover(
            &cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(), SystematicTestCodec, 8, validate_recovered,
        ).await.unwrap();
        assert_eq!(report.markers.len(), 2);
        assert_eq!(report.markers[1].prev_marker,
            Some(ObjectId::derive_from_canonical_bytes(&report.markers[0].to_record_bytes())));
        assert_eq!(again.committed_tip(), CommitSeq::new(2));
        again.close(&cx).unwrap();
    });
}

#[test]
fn driver_recovery_reconciles_an_unacknowledged_marker_sync() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut original = driver(&vfs, &cx, 8);
        stage(&mut original, &cx, 9, 1).await;
        original.queue(&cx, submission(9, 1, 0), 100).unwrap();
        vfs.inject_fault(FaultSpec::power_cut("markers").after_nth_sync(2).build());
        assert!(original.flush(&cx, validate).await.is_err());
        assert!(original.take_committed(CommitSeq::new(1)).is_none());
        vfs.power_on();
        original.close(&cx).unwrap();
        let (mut recovered, report) = DurableWriteCoordinator::recover(
            &cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(), SystematicTestCodec, 8, validate_recovered,
        ).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(recovered.committed_tip(), CommitSeq::new(1));
        assert!(!recovered.needs_recovery());
        assert!(matches!(recovered.queue(&cx, submission(9, 2, 0), 200),
            Err(DurableCommitError::Rejected(CommitResult::ConflictFcw { .. }))));
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn recovery_refuses_marker_proof_substitution_bad_metadata_and_missing_evidence() {
    use fsqlite_types::CommitMarker;
    run(async {
        // Create adversarial but envelope-valid log inputs through the lower
        // storage layer. Its object callback alone cannot bind semantic metadata.
        for case in 0..6 {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut log = NativeDurabilityLog::create(
                &cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"),
                NativeDurabilityLimits::default(),
            ).unwrap();
            let capsule = SystematicTestCodec.encode(&cx, &payload(1, 1)).unwrap();
            let other = SystematicTestCodec.encode(&cx, &payload(2, 2)).unwrap();
            log.append_symbols(&cx, &capsule).await.unwrap();
            log.append_symbols(&cx, &other).await.unwrap();
            let mut proof = NativeCommitProof {
                commit_seq: CommitSeq::new(1), commit_time_unix_ns: 100,
                submission: submission(1, 1, 0),
            };
            match case {
                0 => proof.commit_seq = CommitSeq::new(2),
                1 => proof.commit_time_unix_ns = 101,
                2 => proof.submission = submission(2, 2, 0),
                3 => proof.submission.capsule_digest[0] ^= 1,
                4 => proof.submission.write_set_pages = vec![PageNumber::new(2).unwrap()],
                5 => proof.submission.edge_ids.push(ObjectId::from_bytes([99; 16])),
                _ => unreachable!(),
            }
            let proof_symbols = SystematicTestCodec.encode(&cx, &proof.to_bytes().unwrap()).unwrap();
            log.append_symbols(&cx, &proof_symbols).await.unwrap();
            let marker = CommitMarker::new(CommitSeq::new(1), 100, capsule[0].object_id,
                proof_symbols[0].object_id, None);
            log.publish(&cx, &[marker], |id, records| {
                std::future::ready(SystematicTestCodec.decode(&cx, id, &records).map(|_| ()))
            }).await.unwrap();
            log.close(&cx).unwrap();
            let result = DurableWriteCoordinator::recover(
                &cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"),
                NativeDurabilityLimits::default(), SystematicTestCodec, 8, validate_recovered,
            ).await;
            assert!(result.is_err(), "case {case} incorrectly produced a ready coordinator");
        }
    });
}

#[test]
fn driver_recovery_retains_torn_tail_and_does_not_authorize_new_writes() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut original = driver(&vfs, &cx, 8);
        stage(&mut original, &cx, 1, 1).await;
        original.queue(&cx, submission(1, 1, 0), 100).unwrap();
        original.flush(&cx, validate).await.unwrap();
        original.close(&cx).unwrap();
        let mut markers = open(&vfs, &cx, "markers");
        let end = markers.file_size(&cx).unwrap();
        markers.write(&cx, &[0xAA; 17], end).await.unwrap();
        markers.close(&cx).unwrap();
        let (mut recovered, report) = DurableWriteCoordinator::recover(
            &cx, open(&vfs, &cx, "objects"), open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(), SystematicTestCodec, 8, validate_recovered,
        ).await.unwrap();
        assert_eq!(report.marker_tail_bytes, 17);
        assert_eq!(recovered.committed_tip(), CommitSeq::new(1));
        assert!(recovered.needs_recovery());
        assert!(recovered.queue(&cx, submission(2, 2, 1), 200).is_err());
        assert!(recovered.flush(&cx, validate).await.is_err());
        let mut markers = open(&vfs, &cx, "markers");
        assert_eq!(markers.file_size(&cx).unwrap(), end + 17);
        markers.close(&cx).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn native_raptorq_codec_recovers_exact_ragged_multiblock_payloads() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let codec = RaptorQNativeCodec::default();
        for len in [1, 4097, 65_537, 131_083] {
            let bytes: Vec<u8> = (0..len).map(|index| u8::try_from((index * 17 + 37) % 256).unwrap()).collect();
            let id = RaptorQNativeCodec::object_id(&bytes).unwrap();
            let records = codec.encode(&cx, &bytes).unwrap();
            assert_eq!(codec.decode(&cx, id, &records).unwrap(), bytes);
            assert!(records.iter().any(|r| r.esi & (1 << 31) != 0));
            let erased: Vec<_> = records.iter().filter(|r| r.esi != 0).cloned().collect();
            assert_eq!(records.len(), erased.len() + 1);
            assert_eq!(codec.decode(&cx, id, &erased).unwrap(), bytes, "length={len}");
            let sources_only: Vec<_> = erased.into_iter().filter(|r| r.esi & (1 << 31) == 0).collect();
            assert!(codec.decode(&cx, id, &sources_only).is_err(), "missing source must require a repair");
        }
    });
}

#[test]
fn native_raptorq_codec_refuses_auth_downgrades_and_inconsistent_records() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let authenticated = RaptorQNativeCodec::new(Some([7; 32]));
        let wrong_key = RaptorQNativeCodec::new(Some([8; 32]));
        let unsigned = RaptorQNativeCodec::default();
        let bytes = vec![23; 4097];
        let id = RaptorQNativeCodec::object_id(&bytes).unwrap();
        let records = authenticated.encode(&cx, &bytes).unwrap();
        assert_eq!(authenticated.decode(&cx, id, &records).unwrap(), bytes);
        assert!(wrong_key.decode(&cx, id, &records).is_err());
        assert!(unsigned.decode(&cx, id, &records).is_err());
        let mut downgrade = records.clone();
        for record in &mut downgrade { record.auth_tag = [0; 16]; }
        assert!(authenticated.decode(&cx, id, &downgrade).is_err());
        let plain = unsigned.encode(&cx, &bytes).unwrap();
        assert!(authenticated.decode(&cx, id, &plain).is_err());
        assert!(unsigned.decode(&cx, ObjectId::from_bytes([0; 16]), &plain).is_err());
        let mut malformed = plain.clone();
        malformed[0].symbol_data.pop();
        assert!(unsigned.decode(&cx, id, &malformed).is_err());
        let mut wrong_oti = plain.clone();
        wrong_oti[0].oti.f += 1;
        assert!(unsigned.decode(&cx, id, &wrong_oti).is_err());
        let mut duplicate = plain.clone();
        duplicate.push(SymbolRecord::new(id, plain[0].oti, plain[0].esi, vec![99; 1024], SymbolRecordFlags::empty()));
        assert!(unsigned.decode(&cx, id, &duplicate).is_err());
        assert!(RaptorQNativeCodec::object_id(&[]).is_err());
    });
}

#[cfg(unix)]
#[test]
fn native_file_driver_reopens_authenticated_raptorq_proofs_and_preserves_fcw() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let objects = dir.path().join("objects");
        let markers = dir.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default(),
        ).unwrap();
        let codec = RaptorQNativeCodec::new(Some([7; 32]));
        let capsule = payload(12, 1);
        let encoded = codec.encode(&cx, &capsule).unwrap();
        let mut sub = submission(12, 1, 0);
        sub.capsule_object_id = RaptorQNativeCodec::object_id(&capsule).unwrap();
        let mut original = DurableWriteCoordinator::new(log, codec, 8).unwrap();
        original.stage_symbols(&cx, &encoded).await.unwrap();
        let first = original.queue(&cx, sub, 100).unwrap();
        original.flush(&cx, validate).await.unwrap();
        assert_eq!(original.take_committed(first).unwrap().commit_seq, CommitSeq::new(1));
        original.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut recovered, report) = DurableWriteCoordinator::recover(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default(), RaptorQNativeCodec::new(Some([7; 32])), 8,
            validate_recovered,
        ).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(recovered.committed_tip(), CommitSeq::new(1));
        assert!(matches!(recovered.queue(&cx, submission(12, 2, 0), 200),
            Err(DurableCommitError::Rejected(CommitResult::ConflictFcw { .. }))));
        let second_payload = payload(12, 2);
        let codec = RaptorQNativeCodec::new(Some([7; 32]));
        recovered.stage_symbols(&cx, &codec.encode(&cx, &second_payload).unwrap()).await.unwrap();
        let mut second = submission(12, 2, 1);
        second.capsule_object_id = RaptorQNativeCodec::object_id(&second_payload).unwrap();
        let seq = recovered.queue(&cx, second, 1).unwrap();
        recovered.flush(&cx, validate).await.unwrap();
        assert_eq!(recovered.take_committed(seq).unwrap().commit_time_unix_ns, 101);
        recovered.close(&cx).unwrap();
    });
}
