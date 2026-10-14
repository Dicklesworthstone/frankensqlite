//! Native recovery-copy integration tests. These exercise actual VFS bytes and
//! the existing publication/recovery APIs, not physical power-loss guarantees.
use std::future::{Future, Ready, pending, poll_fn, ready};
use std::path::Path;
use std::task::Poll;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{
    CommitMarker, CommitSeq, ObjectId, Oti, SymbolRecord, SymbolRecordFlags,
    reconstruct_systematic_happy_path,
};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .unwrap()
        .block_on(future);
}

fn file<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> V::File {
    vfs.open(
        cx,
        Some(Path::new(path)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    )
    .unwrap()
    .0
}

fn log<V: Vfs>(vfs: &V, cx: &Cx, prefix: &str) -> NativeDurabilityLog<V::File, V::File> {
    NativeDurabilityLog::create(
        cx,
        file(vfs, cx, &format!("{prefix}-objects")),
        file(vfs, cx, &format!("{prefix}-markers")),
        NativeDurabilityLimits::default(),
    )
    .unwrap()
}

fn symbol(seed: u8) -> SymbolRecord {
    let payload = vec![seed; 4];
    SymbolRecord::new(
        ObjectId::derive_from_canonical_bytes(&payload),
        Oti {
            f: 4,
            al: 1,
            t: 4,
            z: 1,
            n: 1,
        },
        0,
        payload,
        SymbolRecordFlags::SYSTEMATIC_RUN_START,
    )
}

fn verify(id: ObjectId, symbols: Vec<SymbolRecord>) -> Ready<Result<()>> {
    ready((|| {
        let payload = reconstruct_systematic_happy_path(&symbols)
            .map_err(|error| FrankenError::WalCorrupt {
                detail: error.to_string(),
            })?;
        if ObjectId::derive_from_canonical_bytes(&payload) != id {
            return Err(FrankenError::WalCorrupt {
                detail: "wrong test object identity".to_owned(),
            });
        }
        Ok(())
    })())
}

fn marker(seq: u64, previous: Option<ObjectId>, capsule: u8, proof: u8) -> CommitMarker {
    CommitMarker::new(
        CommitSeq::new(seq),
        seq,
        symbol(capsule).object_id,
        symbol(proof).object_id,
        previous,
    )
}

async fn populate<S: VfsFile, M: VfsFile>(cx: &Cx, source: &mut NativeDurabilityLog<S, M>) {
    source
        .append_symbols(cx, &[symbol(1), symbol(2), symbol(3), symbol(4)])
        .await
        .unwrap();
    source
        .publish(cx, &[marker(1, None, 1, 2)], verify)
        .await
        .unwrap();
}

async fn bytes<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> Vec<u8> {
    let mut handle = file(vfs, cx, path);
    let mut result = vec![0; usize::try_from(handle.file_size(cx).unwrap()).unwrap()];
    assert_eq!(handle.read(cx, &mut result, 0).await.unwrap(), result.len());
    handle.close(cx).unwrap();
    result
}

#[test]
fn torn_tail_copy_preserves_originals_and_resumes_the_exact_commit_chain() {
    run(async {
        for (symbol_tail, marker_tail) in [(0, 0), (60, 0), (0, 17), (60, 17)] {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            populate(&cx, &mut source).await;
            source.close(&cx).unwrap();
            let mut objects = file(&vfs, &cx, "source-objects");
            let end = objects.file_size(&cx).unwrap();
            objects
                .write(&cx, &symbol(5).to_bytes()[..symbol_tail], end)
                .await
                .unwrap();
            objects.close(&cx).unwrap();
            let mut markers = file(&vfs, &cx, "source-markers");
            let end = markers.file_size(&cx).unwrap();
            markers
                .write(&cx, &vec![0xAA; marker_tail], end)
                .await
                .unwrap();
            markers.close(&cx).unwrap();
            let original_objects = bytes(&vfs, &cx, "source-objects").await;
            let original_markers = bytes(&vfs, &cx, "source-markers").await;
            let (mut source, report) = NativeDurabilityLog::recover(
                &cx,
                file(&vfs, &cx, "source-objects"),
                file(&vfs, &cx, "source-markers"),
                NativeDurabilityLimits::default(),
                verify,
            )
            .await
            .unwrap();
            assert_eq!(report.symbol_tail_bytes, u64::try_from(symbol_tail).unwrap());
            assert_eq!(report.marker_tail_bytes, u64::try_from(marker_tail).unwrap());
            let mut destination = log(&vfs, &cx, "copy");
            let receipt = source
                .copy_recovered_prefix_to(&cx, &mut destination, verify)
                .await
                .unwrap();
            assert_eq!(receipt.commits, 1);
            assert_eq!(receipt.last_seq, CommitSeq::new(1));
            assert_eq!(receipt.symbol_records, 4, "intact orphans must survive too");
            assert!(!destination.needs_recovery());
            assert_eq!(source.needs_recovery(), symbol_tail != 0 || marker_tail != 0);
            assert_eq!(bytes(&vfs, &cx, "source-objects").await, original_objects);
            assert_eq!(bytes(&vfs, &cx, "source-markers").await, original_markers);
            assert_eq!(
                bytes(&vfs, &cx, "copy-markers").await,
                marker(1, None, 1, 2).to_record_bytes()
            );
            let previous = Some(ObjectId::derive_from_canonical_bytes(
                &marker(1, None, 1, 2).to_record_bytes(),
            ));
            destination
                .publish(&cx, &[marker(2, previous, 3, 4)], verify)
                .await
                .unwrap();
            destination
                .append_symbols(&cx, &[symbol(9)])
                .await
                .unwrap();
            destination.close(&cx).unwrap();
            let (mut reopened, report) = NativeDurabilityLog::recover(
                &cx,
                file(&vfs, &cx, "copy-objects"),
                file(&vfs, &cx, "copy-markers"),
                NativeDurabilityLimits::default(),
                verify,
            )
            .await
            .unwrap();
            assert_eq!(report.markers.len(), 2);
            assert!(!report.append_blocked());
            assert_eq!(report.symbol_records, 5);
            verify(
                symbol(9).object_id,
                reopened.read_object(&cx, symbol(9).object_id).await.unwrap(),
            )
            .await
            .unwrap();
            reopened.close(&cx).unwrap();
            source.close(&cx).unwrap();
        }
    });
}

#[test]
fn multi_batch_copy_keeps_every_marker_and_uses_bounded_publication_groups() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut source = log(&vfs, &cx, "source");
        source
            .append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        let mut chain = Vec::new();
        let mut previous = None;
        for seq in 1..=257 {
            let next = marker(seq, previous, 1, 2);
            previous = Some(ObjectId::derive_from_canonical_bytes(&next.to_record_bytes()));
            chain.push(next);
        }
        source.publish(&cx, &chain, verify).await.unwrap();
        let mut destination = log(&vfs, &cx, "copy");
        let before = vfs.sync_count();
        let receipt = source
            .copy_recovered_prefix_to(&cx, &mut destination, verify)
            .await
            .unwrap();
        assert_eq!(receipt.commits, 257);
        assert_eq!(
            vfs.sync_count() - before,
            6,
            "three ordinary two-barrier groups"
        );
        assert_eq!(
            bytes(&vfs, &cx, "copy-markers").await,
            bytes(&vfs, &cx, "source-markers").await
        );
        assert_eq!(destination.published_tip(), CommitSeq::new(257));
        source.close(&cx).unwrap();
        destination.close(&cx).unwrap();
    });
}

#[test]
fn empty_and_orphan_only_copies_are_synced_without_inventing_a_commit() {
    run(async {
        for staged in [false, true] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut source = log(&vfs, &cx, "source");
            if staged {
                source.append_symbols(&cx, &[symbol(1)]).await.unwrap();
            }
            let mut destination = log(&vfs, &cx, "copy");
            let before = vfs.sync_count();
            let receipt = source
                .copy_recovered_prefix_to(&cx, &mut destination, |_, _| -> Ready<Result<()>> {
                    panic!("no marker referents to verify");
                })
                .await
                .unwrap();
            assert_eq!(receipt.commits, 0);
            assert_eq!(receipt.last_seq, CommitSeq::ZERO);
            assert_eq!(receipt.symbol_records, usize::from(staged));
            assert_eq!(vfs.sync_count() - before, 2);
            assert!(!destination.needs_recovery());
            assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
            source.close(&cx).unwrap();
            destination.close(&cx).unwrap();
        }
    });
}

#[test]
fn preflight_refuses_all_cross_aliases_and_nonempty_destinations_without_mutation() {
    run(async {
        for (objects, markers) in [
            ("source-objects", "new"),
            ("new", "source-objects"),
            ("source-markers", "new"),
            ("new", "source-markers"),
        ] {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            let mut destination = NativeDurabilityLog::create(
                &cx,
                file(&vfs, &cx, objects),
                file(&vfs, &cx, markers),
                NativeDurabilityLimits::default(),
            )
            .unwrap();
            assert!(
                source.copy_recovered_prefix_to(&cx, &mut destination, verify).await.is_err()
            );
            assert!(!source.needs_recovery());
            assert!(!destination.needs_recovery());
            assert!(bytes(&vfs, &cx, objects).await.is_empty());
            assert!(bytes(&vfs, &cx, markers).await.is_empty());
            source.close(&cx).unwrap();
            destination.close(&cx).unwrap();
        }
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut source = log(&vfs, &cx, "source");
        populate(&cx, &mut source).await;
        let mut destination = log(&vfs, &cx, "copy");
        destination.append_symbols(&cx, &[symbol(9)]).await.unwrap();
        let before = bytes(&vfs, &cx, "copy-objects").await;
        assert!(
            source.copy_recovered_prefix_to(&cx, &mut destination, verify).await.is_err()
        );
        assert_eq!(bytes(&vfs, &cx, "copy-objects").await, before);
        assert!(!destination.needs_recovery());
        source.close(&cx).unwrap();
        destination.close(&cx).unwrap();
    });
}

#[test]
fn dropped_verification_future_poisoned_the_destination_between_successful_writes() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut source = log(&vfs, &cx, "source");
        populate(&cx, &mut source).await;
        let mut destination = log(&vfs, &cx, "copy");
        let mut copying = Box::pin(source.copy_recovered_prefix_to(&cx, &mut destination, |_, _| {
            pending::<Result<()>>()
        }));
        poll_fn(|task| {
            assert!(copying.as_mut().poll(task).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(copying);
        assert!(
            destination.needs_recovery(),
            "copy-level owner covers verifier awaits too"
        );
        assert!(
            destination.outstanding_write().is_none(),
            "completed writes are not pending"
        );
        assert!(matches!(
            destination.append_symbols(&cx, &[symbol(9)]).await,
            Err(FrankenError::BusyRecovery)
        ));
        assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
        assert!(!bytes(&vfs, &cx, "copy-objects").await.is_empty());
        assert!(!source.needs_recovery());
        destination.close(&cx).unwrap();
        let (mut recovered, report) = NativeDurabilityLog::recover(
            &cx,
            file(&vfs, &cx, "copy-objects"),
            file(&vfs, &cx, "copy-markers"),
            NativeDurabilityLimits::default(),
            verify,
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 0);
        assert_eq!(report.symbol_records, 4);
        recovered.close(&cx).unwrap();
        source.close(&cx).unwrap();
    });
}

#[test]
fn failed_destination_verification_cannot_publish_or_be_retried_as_success() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut source = log(&vfs, &cx, "source");
        populate(&cx, &mut source).await;
        let original = bytes(&vfs, &cx, "source-objects").await;
        let mut destination = log(&vfs, &cx, "copy");
        let before = vfs.sync_count();
        assert!(matches!(
            source
                .copy_recovered_prefix_to(&cx, &mut destination, |_, _| {
                    ready(Err(FrankenError::Unsupported))
                })
                .await,
            Err(FrankenError::Unsupported)
        ));
        assert!(destination.needs_recovery());
        assert!(
            source.copy_recovered_prefix_to(&cx, &mut destination, verify).await.is_err()
        );
        assert_eq!(vfs.sync_count(), before);
        assert_eq!(destination.published_tip(), CommitSeq::ZERO);
        assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
        assert_eq!(bytes(&vfs, &cx, "source-objects").await, original);
        let mut retry = log(&vfs, &cx, "retry");
        source.copy_recovered_prefix_to(&cx, &mut retry, verify).await.unwrap();
        assert_eq!(retry.published_tip(), CommitSeq::new(1));
        retry.close(&cx).unwrap();
        destination.close(&cx).unwrap();
        source.close(&cx).unwrap();
    });
}

#[test]
fn either_destination_barrier_failure_stops_without_touching_the_source() {
    run(async {
        for (path, index_delta) in [("copy-objects", 0), ("copy-markers", 1)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut source = log(&vfs, &cx, "source");
            populate(&cx, &mut source).await;
            let original_objects = bytes(&vfs, &cx, "source-objects").await;
            let original_markers = bytes(&vfs, &cx, "source-markers").await;
            let mut destination = log(&vfs, &cx, "copy");
            let before = vfs.sync_count();
            vfs.inject_fault(
                FaultSpec::power_cut(path).after_nth_sync(before + index_delta).build(),
            );
            assert!(
                source.copy_recovered_prefix_to(&cx, &mut destination, verify).await.is_err()
            );
            assert!(vfs.is_powered_off());
            assert_eq!(vfs.sync_count(), before + index_delta + 1);
            assert_eq!(vfs.triggered_faults().len(), 1);
            assert!(destination.needs_recovery());
            assert_eq!(destination.published_tip(), CommitSeq::ZERO);
            vfs.power_on();
            assert!(
                source.copy_recovered_prefix_to(&cx, &mut destination, verify).await.is_err()
            );
            assert_eq!(vfs.sync_count(), before + index_delta + 1);
            assert_eq!(bytes(&vfs, &cx, "source-objects").await, original_objects);
            assert_eq!(bytes(&vfs, &cx, "source-markers").await, original_markers);
            let mut retry = log(&vfs, &cx, "retry");
            source.copy_recovered_prefix_to(&cx, &mut retry, verify).await.unwrap();
            assert!(!retry.needs_recovery());
            retry.close(&cx).unwrap();
            destination.close(&cx).unwrap();
            source.close(&cx).unwrap();
        }
    });
}

#[test]
fn repair_rebuilds_real_raptorq_symbols_and_preserves_authenticated_identity() {
    use fsqlite_wal::native_commit::durable::NativeObjectCodec;
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;

    run(async {
        for key in [None, Some([7; 32])] {
            let cx = Cx::new();
            cx.set_native_cx(asupersync::Cx::current().unwrap());
            let vfs = MemoryVfs::new();
            let codec = RaptorQNativeCodec::new(key);
            let payload: Vec<_> = (0..4097).map(|n| u8::try_from(n % 251).unwrap()).collect();
            let proof = vec![0xA5; 1025];
            let payload_id = RaptorQNativeCodec::object_id(&payload).unwrap();
            let proof_id = RaptorQNativeCodec::object_id(&proof).unwrap();
            let payload_records = codec.encode(&cx, &payload).unwrap();
            let proof_records = codec.encode(&cx, &proof).unwrap();
            let missing_esi = payload_records[0].esi;
            assert_eq!(missing_esi & (1 << 31), 0, "erase a source, not a repair");
            assert!(payload_records.iter().any(|record| record.esi & (1 << 31) != 0));
            let symbol_count = payload_records.len() + proof_records.len();
            let mut source = log(&vfs, &cx, "source");
            source.append_symbols(&cx, &payload_records).await.unwrap();
            source.append_symbols(&cx, &proof_records).await.unwrap();
            let first = CommitMarker::new(CommitSeq::new(1), 100, payload_id, proof_id, None);
            source
                .publish(&cx, &[first], |id, records| {
                    ready(codec.decode(&cx, id, &records).map(|_| ()))
                })
                .await
                .unwrap();
            source.close(&cx).unwrap();
            let original = bytes(&vfs, &cx, "source-objects").await;
            let mut objects = file(&vfs, &cx, "source-objects");
            objects.write(&cx, &[original[51] ^ 0x80], 51).await.unwrap();
            objects
                .write(
                    &cx,
                    &payload_records[0].to_bytes()[..60],
                    u64::try_from(original.len()).unwrap(),
                )
                .await
                .unwrap();
            objects.close(&cx).unwrap();
            let damaged = bytes(&vfs, &cx, "source-objects").await;
            let original_markers = bytes(&vfs, &cx, "source-markers").await;
            let (mut source, report) = NativeDurabilityLog::recover(
                &cx,
                file(&vfs, &cx, "source-objects"),
                file(&vfs, &cx, "source-markers"),
                NativeDurabilityLimits::default(),
                |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())),
            )
            .await
            .unwrap();
            assert_eq!(report.erased_symbols, 1);
            assert_eq!(report.symbol_tail_bytes, 60);
            assert!(source.needs_recovery());
            let survivors = source.read_object(&cx, payload_id).await.unwrap();
            assert!(!survivors.iter().any(|record| record.esi == missing_esi));
            assert_eq!(codec.decode(&cx, payload_id, &survivors).unwrap(), payload);
            let mut repaired = log(&vfs, &cx, "repaired");
            let receipt = source
                .repair_recovered_prefix_to(&cx, &mut repaired, &codec)
                .await
                .unwrap();
            assert_eq!(receipt.symbol_records, symbol_count);
            assert!(!repaired.needs_recovery());
            let restored = repaired.read_object(&cx, payload_id).await.unwrap();
            assert!(restored.iter().any(|record| record.esi == missing_esi));
            assert!(restored.iter().all(SymbolRecord::verify_integrity));
            if let Some(key) = key {
                assert!(restored.iter().all(|record| record.verify_auth(&key)));
            }
            assert_eq!(codec.decode(&cx, payload_id, &restored).unwrap(), payload);
            assert_eq!(bytes(&vfs, &cx, "source-objects").await, damaged);
            assert_eq!(bytes(&vfs, &cx, "source-markers").await, original_markers);
            assert_eq!(bytes(&vfs, &cx, "repaired-markers").await, original_markers);
            repaired.close(&cx).unwrap();
            // A second independent source-symbol erasure must be recoverable
            // from the regenerated redundancy, not an in-memory page buffer.
            let copied = bytes(&vfs, &cx, "repaired-objects").await;
            let mut objects = file(&vfs, &cx, "repaired-objects");
            objects.write(&cx, &[copied[51] ^ 0x80], 51).await.unwrap();
            objects.close(&cx).unwrap();
            let (mut reopened, report) = NativeDurabilityLog::recover(
                &cx,
                file(&vfs, &cx, "repaired-objects"),
                file(&vfs, &cx, "repaired-markers"),
                NativeDurabilityLimits::default(),
                |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())),
            )
            .await
            .unwrap();
            assert_eq!(report.erased_symbols, 1);
            assert!(!report.append_blocked());
            let records = reopened.read_object(&cx, payload_id).await.unwrap();
            assert_eq!(codec.decode(&cx, payload_id, &records).unwrap(), payload);
            reopened.close(&cx).unwrap();
            source.close(&cx).unwrap();
        }
    });
}

#[test]
fn repair_refuses_missing_or_wrong_authentication_keys_before_destination_writes() {
    use fsqlite_wal::native_commit::durable::NativeObjectCodec;
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;

    run(async {
        for wrong_key in [None, Some([8; 32])] {
            let cx = Cx::new();
            cx.set_native_cx(asupersync::Cx::current().unwrap());
            let vfs = MemoryVfs::new();
            let codec = RaptorQNativeCodec::new(Some([7; 32]));
            let mut source = log(&vfs, &cx, "source");
            let encoded = codec.encode(&cx, &[0xAB; 4097]).unwrap();
            source.append_symbols(&cx, &encoded).await.unwrap();
            let mut destination = log(&vfs, &cx, "copy");
            let original = bytes(&vfs, &cx, "source-objects").await;
            assert!(
                source
                    .repair_recovered_prefix_to(
                        &cx,
                        &mut destination,
                        &RaptorQNativeCodec::new(wrong_key),
                    )
                    .await
                    .is_err()
            );
            assert!(destination.needs_recovery());
            assert!(bytes(&vfs, &cx, "copy-objects").await.is_empty());
            assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
            assert_eq!(bytes(&vfs, &cx, "source-objects").await, original);
            let mut good = log(&vfs, &cx, "good");
            source.repair_recovered_prefix_to(&cx, &mut good, &codec).await.unwrap();
            assert!(!good.needs_recovery());
            good.close(&cx).unwrap();
            destination.close(&cx).unwrap();
            source.close(&cx).unwrap();
        }
    });
}

// Intentionally broken codecs test the repair boundary, not an alternative
// to RaptorQ. Either corrupt the identity or return different decoded bytes
// while claiming the same identity. Neither may reach destination append.
struct BrokenRepairCodec {
    preserve_identity: bool,
}

impl fsqlite_wal::native_commit::durable::NativeObjectCodec for BrokenRepairCodec {
    fn encode(&self, _: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> {
        let mut changed = payload.to_vec();
        changed[0] ^= 0x80;
        let id = ObjectId::derive_from_canonical_bytes(if self.preserve_identity {
            payload
        } else {
            &changed
        });
        Ok(vec![SymbolRecord::new(
            id,
            Oti { f: 4, al: 1, t: 4, z: 1, n: 1 },
            0,
            changed,
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }

    fn decode(&self, _: &Cx, _: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        reconstruct_systematic_happy_path(records).map_err(|error| FrankenError::WalCorrupt {
            detail: error.to_string(),
        })
    }
}

#[test]
fn repair_rejects_encoder_identity_and_roundtrip_mismatches_before_append() {
    run(async {
        for preserve_identity in [false, true] {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            populate(&cx, &mut source).await;
            let mut destination = log(&vfs, &cx, "copy");
            let error = source
                .repair_recovered_prefix_to(
                    &cx,
                    &mut destination,
                    &BrokenRepairCodec { preserve_identity },
                )
                .await
                .unwrap_err();
            assert!(matches!(error, FrankenError::WalCorrupt { .. }));
            let expected = if preserve_identity { "decoded object" } else { "object identity" };
            assert!(error.to_string().contains(expected));
            assert!(destination.needs_recovery());
            assert!(bytes(&vfs, &cx, "copy-objects").await.is_empty());
            assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
            assert!(!source.needs_recovery());
            destination.close(&cx).unwrap();
            source.close(&cx).unwrap();
        }
    });
}

#[test]
fn repaired_native_page_history_reopens_and_resumes_conflict_checked_transactions() {
    use fsqlite_types::PageNumber;
    use fsqlite_wal::native_commit::durable::NativeObjectCodec;
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore};

    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let vfs = MemoryVfs::new();
        let codec = || RaptorQNativeCodec::new(Some([7; 32]));
        let page = |n| PageNumber::new(n).unwrap();
        let mut db = NativePageStore::new(
            log(&vfs, &cx, "source"), codec(), 512, NativePageLimits::default(),
        )
        .unwrap();
        let mut first = db.begin(&cx).unwrap();
        for (number, fill) in [(1, 0x11), (2, 0x22), (3, 0x33)] {
            db.write_page(&cx, &mut first, page(number), Some(&[fill; 512])).unwrap();
        }
        db.commit(&cx, &mut first, 100).await.unwrap();
        let mut second = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut second, page(1), Some(&[0x44; 512])).unwrap();
        db.write_page(&cx, &mut second, page(2), None).unwrap();
        db.commit(&cx, &mut second, 200).await.unwrap();
        db.close(&cx).unwrap();
        let original = bytes(&vfs, &cx, "source-objects").await;
        let mut objects = file(&vfs, &cx, "source-objects");
        objects.write(&cx, &[original[51] ^ 0x80], 51).await.unwrap();
        objects.write(&cx, b"torn", u64::try_from(original.len()).unwrap()).await.unwrap();
        objects.close(&cx).unwrap();
        let damaged = bytes(&vfs, &cx, "source-objects").await;
        let (mut source, report) = NativeDurabilityLog::recover(
            &cx,
            file(&vfs, &cx, "source-objects"),
            file(&vfs, &cx, "source-markers"),
            NativeDurabilityLimits::default(),
            |id, records| ready(codec().decode(&cx, id, &records).map(|_| ())),
        )
        .await
        .unwrap();
        assert_eq!(report.erased_symbols, 1);
        assert_eq!(report.symbol_tail_bytes, 4);
        let mut repaired = log(&vfs, &cx, "repaired");
        source.repair_recovered_prefix_to(&cx, &mut repaired, &codec()).await.unwrap();
        assert_eq!(repaired.published_tip(), CommitSeq::new(2));
        repaired.close(&cx).unwrap();
        let (mut db, report) = NativePageStore::recover(
            &cx,
            file(&vfs, &cx, "repaired-objects"),
            file(&vfs, &cx, "repaired-markers"),
            codec(),
            512,
            NativeDurabilityLimits::default(),
            NativePageLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.erased_symbols, 0);
        assert!(!report.append_blocked());
        let mut old = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut old, page(1)).unwrap().unwrap().as_ref(), &[0x44; 512]);
        assert!(db.read_page(&cx, &mut old, page(2)).unwrap().is_none());
        assert_eq!(db.read_page(&cx, &mut old, page(3)).unwrap().unwrap().as_ref(), &[0x33; 512]);
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut a, page(1), Some(&[0x55; 512])).unwrap();
        db.write_page(&cx, &mut b, page(1), Some(&[0x66; 512])).unwrap();
        let committed = db.commit(&cx, &mut a, 300).await.unwrap().unwrap();
        assert_eq!(committed.commit_seq, CommitSeq::new(3));
        assert!(matches!(
            db.commit(&cx, &mut b, 400).await,
            Err(FrankenError::BusySnapshot { .. })
        ));
        db.rollback(&mut b).unwrap();
        assert_eq!(db.read_page(&cx, &mut old, page(1)).unwrap().unwrap().as_ref(), &[0x44; 512]);
        db.rollback(&mut old).unwrap();
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut fresh, page(1)).unwrap().unwrap().as_ref(), &[0x55; 512]);
        db.rollback(&mut fresh).unwrap();
        db.close(&cx).unwrap();
        assert_eq!(bytes(&vfs, &cx, "source-objects").await, damaged);
        source.close(&cx).unwrap();
    });
}

#[test]
fn repair_refuses_insufficient_rank_without_fabricating_redundancy() {
    use fsqlite_wal::native_commit::durable::NativeObjectCodec;
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;

    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let vfs = MemoryVfs::new();
        let codec = RaptorQNativeCodec::default();
        let payload = vec![0xAC; 8193];
        let id = RaptorQNativeCodec::object_id(&payload).unwrap();
        let encoded = codec.encode(&cx, &payload).unwrap();
        assert!(encoded.len() > 2);
        let mut source = log(&vfs, &cx, "source");
        source.append_symbols(&cx, &encoded).await.unwrap();
        let mut objects = file(&vfs, &cx, "source-objects");
        let mut offset = 0;
        for (index, record) in encoded.iter().enumerate() {
            let wire = record.to_bytes();
            if index != 0 {
                objects.write(&cx, &[wire[51] ^ 0x80], offset + 51).await.unwrap();
            }
            offset += u64::try_from(wire.len()).unwrap();
        }
        objects.close(&cx).unwrap();
        let survivors = source.read_object(&cx, id).await.unwrap();
        assert_eq!(survivors.len(), 1);
        assert!(codec.decode(&cx, id, &survivors).is_err());
        let before = bytes(&vfs, &cx, "source-objects").await;
        let mut destination = log(&vfs, &cx, "copy");
        assert!(source.repair_recovered_prefix_to(&cx, &mut destination, &codec).await.is_err());
        assert!(destination.needs_recovery());
        assert_eq!(destination.published_tip(), CommitSeq::ZERO);
        assert!(bytes(&vfs, &cx, "copy-objects").await.is_empty());
        assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
        assert_eq!(bytes(&vfs, &cx, "source-objects").await, before);
        destination.close(&cx).unwrap();
        source.close(&cx).unwrap();
    });
}

#[test]
fn repair_enforces_destination_payload_and_symbol_budgets() {
    use fsqlite_wal::native_commit::durable::NativeObjectCodec;
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;

    run(async {
        for limit_payload in [false, true] {
            let cx = Cx::new();
            cx.set_native_cx(asupersync::Cx::current().unwrap());
            let vfs = MemoryVfs::new();
            let codec = RaptorQNativeCodec::default();
            let encoded = codec.encode(&cx, &[0x5A; 4097]).unwrap();
            let mut source = log(&vfs, &cx, "source");
            source.append_symbols(&cx, &encoded).await.unwrap();
            let limits = if limit_payload {
                NativeDurabilityLimits {
                    max_batch_bytes: 2048,
                    ..NativeDurabilityLimits::default()
                }
            } else {
                NativeDurabilityLimits {
                    max_symbol_bytes: 512,
                    ..NativeDurabilityLimits::default()
                }
            };
            let mut destination = NativeDurabilityLog::create(
                &cx, file(&vfs, &cx, "copy-objects"), file(&vfs, &cx, "copy-markers"), limits,
            )
            .unwrap();
            assert!(matches!(
                source.repair_recovered_prefix_to(&cx, &mut destination, &codec).await,
                Err(FrankenError::TooBig)
            ));
            assert!(destination.needs_recovery());
            assert!(bytes(&vfs, &cx, "copy-objects").await.is_empty());
            assert!(bytes(&vfs, &cx, "copy-markers").await.is_empty());
            destination.close(&cx).unwrap();
            source.close(&cx).unwrap();
        }
    });
}
