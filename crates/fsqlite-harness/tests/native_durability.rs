//! Executable VFS publication tests. MemoryVfs/fault injection proves ordering
//! and error handling, not physical power-loss durability or public SQL wiring.
use std::future::{Future, Ready, ready};
use std::path::Path;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{
    COMMIT_MARKER_RECORD_V1_SIZE, CommitMarker, CommitSeq, ObjectId, Oti, SymbolRecord,
    SymbolRecordFlags, reconstruct_systematic_happy_path,
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

#[test]
fn recovery_restores_the_chain_and_keeps_orphans_uncommitted() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        let first = first_marker();
        log.publish(&cx, std::slice::from_ref(&first), verify)
            .await
            .unwrap();
        log.append_symbols(&cx, &[symbol(3), symbol(4)])
            .await
            .unwrap();
        log.close(&cx).unwrap();
        let (mut recovered, report) = NativeDurabilityLog::recover(
            &cx,
            open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(),
            verify,
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(report.symbol_records, 4);
        assert_eq!(report.erased_symbols, 0);
        assert!(!report.append_blocked());
        assert_eq!(recovered.published_tip(), CommitSeq::new(1));
        assert_eq!(vfs.sync_count(), 4); // Publication and recovery each use two.
        let second = CommitMarker::new(
            CommitSeq::new(2),
            101,
            symbol(3).object_id,
            symbol(4).object_id,
            Some(ObjectId::derive_from_canonical_bytes(
                &first.to_record_bytes(),
            )),
        );
        recovered.publish(&cx, &[second], verify).await.unwrap();
        assert_eq!(recovered.published_tip(), CommitSeq::new(2));
        assert_eq!(vfs.sync_count(), 6);
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn recovery_reconciles_a_failed_marker_sync_without_losing_the_commit() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        vfs.inject_fault(FaultSpec::power_cut("markers").after_nth_sync(2).build());
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        vfs.power_on();
        log.close(&cx).unwrap();
        let before = vfs.sync_count();
        let (mut recovered, report) = NativeDurabilityLog::recover(
            &cx,
            open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(),
            verify,
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(recovered.published_tip(), CommitSeq::new(1));
        assert_eq!(vfs.sync_count(), before + 2);
        assert!(!recovered.needs_recovery());
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn recovery_retains_both_torn_tails_and_refuses_to_append_through_them() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        log.publish(&cx, &[first_marker()], verify).await.unwrap();
        log.close(&cx).unwrap();
        let mut objects = open(&vfs, &cx, "objects");
        let object_end = objects.file_size(&cx).unwrap();
        objects
            .write(&cx, &symbol(3).to_bytes()[..60], object_end)
            .await
            .unwrap();
        let mut markers = open(&vfs, &cx, "markers");
        let marker_end = markers.file_size(&cx).unwrap();
        markers.write(&cx, &[0xAA; 17], marker_end).await.unwrap();
        objects.close(&cx).unwrap();
        markers.close(&cx).unwrap();
        let (mut recovered, report) = NativeDurabilityLog::recover(
            &cx,
            open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(),
            verify,
        )
        .await
        .unwrap();
        assert_eq!(report.symbol_tail_bytes, 60);
        assert_eq!(report.marker_tail_bytes, 17);
        assert!(report.append_blocked());
        assert!(recovered.needs_recovery());
        assert_eq!(recovered.published_tip(), CommitSeq::new(1));
        verify_sync(
            symbol(1).object_id,
            &recovered
                .read_object(&cx, symbol(1).object_id)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(recovered.append_symbols(&cx, &[symbol(9)]).await.is_err());
        assert!(recovered.publish(&cx, &[], verify).await.is_err());
        let mut objects = open(&vfs, &cx, "objects");
        let mut markers = open(&vfs, &cx, "markers");
        assert_eq!(objects.file_size(&cx).unwrap(), object_end + 60);
        assert_eq!(markers.file_size(&cx).unwrap(), marker_end + 17);
        objects.close(&cx).unwrap();
        markers.close(&cx).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn recovery_never_skips_a_complete_corrupt_or_discontinuous_marker() {
    run(async {
        for corrupt_version in [true, false] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut log = new_log(&vfs, &cx);
            log.append_symbols(&cx, &[symbol(1), symbol(2)])
                .await
                .unwrap();
            log.close(&cx).unwrap();
            let marker = CommitMarker::new(
                CommitSeq::new(if corrupt_version { 1 } else { 2 }),
                100,
                symbol(1).object_id,
                symbol(2).object_id,
                None,
            );
            let mut bytes = marker.to_record_bytes();
            if corrupt_version {
                bytes[0] = 0xFF;
            }
            let mut file = open(&vfs, &cx, "markers");
            file.write(&cx, &bytes, 0).await.unwrap();
            file.close(&cx).unwrap();
            assert!(
                NativeDurabilityLog::recover(
                    &cx,
                    open(&vfs, &cx, "objects"),
                    open(&vfs, &cx, "markers"),
                    NativeDurabilityLimits::default(),
                    verify,
                )
                .await
                .is_err()
            );
            assert_eq!(vfs.sync_count(), 0);
        }
    });
}

#[test]
fn checksum_erasures_require_a_surviving_verified_object() {
    run(async {
        for has_redundancy in [true, false] {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut log = new_log(&vfs, &cx);
            log.append_symbols(&cx, &[symbol(1), symbol(2)])
                .await
                .unwrap();
            log.publish(&cx, &[first_marker()], verify).await.unwrap();
            if has_redundancy {
                // A duplicate source tests erasure routing, not RaptorQ algebra.
                log.append_symbols(&cx, &[symbol(1)]).await.unwrap();
            }
            log.close(&cx).unwrap();
            let mut objects = open(&vfs, &cx, "objects");
            objects.write(&cx, &[0xFF], 51).await.unwrap();
            objects.close(&cx).unwrap();
            let result = NativeDurabilityLog::recover(
                &cx,
                open(&vfs, &cx, "objects"),
                open(&vfs, &cx, "markers"),
                NativeDurabilityLimits::default(),
                verify,
            )
            .await;
            if has_redundancy {
                let (mut recovered, report) = result.unwrap();
                assert_eq!(report.erased_symbols, 1);
                assert_eq!(report.symbol_records, 3);
                assert_eq!(recovered.published_tip(), CommitSeq::new(1));
                recovered.close(&cx).unwrap();
            } else {
                assert!(result.is_err());
            }
        }
    });
}

#[test]
fn recovery_rejects_payload_identity_spoofing_and_advertised_size_bombs() {
    run(async {
        for oversized in [false, true] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let original = symbol(1);
            let mut record = SymbolRecord::new(
                original.object_id,
                original.oti,
                0,
                vec![9; 4], // Valid envelope, wrong content-addressed identity.
                SymbolRecordFlags::SYSTEMATIC_RUN_START,
            );
            if oversized {
                record.oti.f = u64::MAX;
            }
            let mut objects = open(&vfs, &cx, "objects");
            let mut bytes = record.to_bytes();
            bytes.extend_from_slice(&symbol(2).to_bytes());
            objects.write(&cx, &bytes, 0).await.unwrap();
            objects.close(&cx).unwrap();
            let mut markers = open(&vfs, &cx, "markers");
            markers
                .write(&cx, &first_marker().to_record_bytes(), 0)
                .await
                .unwrap();
            markers.close(&cx).unwrap();
            assert!(
                NativeDurabilityLog::recover(
                    &cx,
                    open(&vfs, &cx, "objects"),
                    open(&vfs, &cx, "markers"),
                    NativeDurabilityLimits::default(),
                    verify,
                )
                .await
                .is_err()
            );
            assert_eq!(vfs.sync_count(), 0);
        }
    });
}

#[test]
fn recovery_must_complete_both_resyncs_before_returning_a_log() {
    run(async {
        for (path, ordinal) in [("objects", 1), ("markers", 2)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            // Stage valid bytes without assuming they were synced.
            let mut objects = open(&vfs, &cx, "objects");
            let mut bytes = symbol(1).to_bytes();
            bytes.extend_from_slice(&symbol(2).to_bytes());
            objects.write(&cx, &bytes, 0).await.unwrap();
            objects.close(&cx).unwrap();
            let mut markers = open(&vfs, &cx, "markers");
            markers
                .write(&cx, &first_marker().to_record_bytes(), 0)
                .await
                .unwrap();
            markers.close(&cx).unwrap();
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(ordinal).build());
            assert!(
                NativeDurabilityLog::recover(
                    &cx,
                    open(&vfs, &cx, "objects"),
                    open(&vfs, &cx, "markers"),
                    NativeDurabilityLimits::default(),
                    verify,
                )
                .await
                .is_err()
            );
            assert!(vfs.is_powered_off());
        }
    });
}

#[cfg(unix)]
#[test]
fn native_file_streams_publish_close_and_recover_through_the_caller_runtime() {
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().expect("caller runtime context"));
        let directory = tempfile::tempdir().unwrap();
        let symbols_path = directory.path().join("objects");
        let markers_path = directory.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let mut log = NativeDurabilityLog::create(
            &cx,
            vfs.open(&cx, Some(&symbols_path), flags).unwrap().0,
            vfs.open(&cx, Some(&markers_path), flags).unwrap().0,
            NativeDurabilityLimits::default(),
        )
        .unwrap();
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        log.publish(&cx, &[first_marker()], verify).await.unwrap();
        log.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut recovered, report) = NativeDurabilityLog::recover(
            &cx,
            vfs.open(&cx, Some(&symbols_path), flags).unwrap().0,
            vfs.open(&cx, Some(&markers_path), flags).unwrap().0,
            NativeDurabilityLimits::default(),
            verify,
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 1);
        assert_eq!(recovered.published_tip(), CommitSeq::new(1));
        assert_eq!(report.symbol_tail_bytes, 0);
        assert_eq!(report.marker_tail_bytes, 0);
        recovered.close(&cx).unwrap();
    });
}

fn open<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> V::File {
    vfs.open(
        cx,
        Some(Path::new(path)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    )
    .expect("open dedicated test stream")
    .0
}

fn new_log<V: Vfs>(vfs: &V, cx: &Cx) -> NativeDurabilityLog<V::File, V::File> {
    NativeDurabilityLog::create(
        cx,
        open(vfs, cx, "objects"),
        open(vfs, cx, "markers"),
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

fn verify_sync(object_id: ObjectId, records: &[SymbolRecord]) -> Result<()> {
    let payload =
        reconstruct_systematic_happy_path(records).map_err(|error| FrankenError::WalCorrupt {
            detail: error.to_string(),
        })?;
    if ObjectId::derive_from_canonical_bytes(&payload) != object_id {
        return Err(FrankenError::WalCorrupt {
            detail: "test object identity mismatch".to_owned(),
        });
    }
    Ok(())
}

fn verify(object_id: ObjectId, records: Vec<SymbolRecord>) -> Ready<Result<()>> {
    ready(verify_sync(object_id, &records))
}

fn first_marker() -> CommitMarker {
    CommitMarker::new(
        CommitSeq::new(1),
        100,
        symbol(1).object_id,
        symbol(2).object_id,
        None,
    )
}

#[test]
fn staged_objects_are_not_commits_and_a_batch_uses_two_syncs() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        let mut markers = Vec::new();
        let mut previous = None;
        for seq in 1_u8..=3 {
            let capsule = symbol(seq * 2 - 1);
            let proof = symbol(seq * 2);
            let marker = CommitMarker::new(
                CommitSeq::new(u64::from(seq)),
                u64::from(seq),
                capsule.object_id,
                proof.object_id,
                previous,
            );
            log.append_symbols(&cx, &[capsule, proof]).await.unwrap();
            previous = Some(ObjectId::derive_from_canonical_bytes(
                &marker.to_record_bytes(),
            ));
            markers.push(marker);
        }
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        assert_eq!(vfs.sync_count(), 0);
        let receipt = log.publish(&cx, &markers, verify).await.unwrap().unwrap();
        assert_eq!(receipt.commits, 3);
        assert_eq!(receipt.last_seq, CommitSeq::new(3));
        assert_eq!(vfs.sync_count(), 2);
        assert_eq!(log.published_tip(), CommitSeq::new(3));
        let mut reader = open(&vfs, &cx, "markers");
        let mut wire = vec![0; COMMIT_MARKER_RECORD_V1_SIZE * 3];
        assert_eq!(reader.read(&cx, &mut wire, 0).await.unwrap(), wire.len());
        for (bytes, expected) in wire
            .chunks_exact(COMMIT_MARKER_RECORD_V1_SIZE)
            .zip(&markers)
        {
            assert_eq!(
                CommitMarker::from_record_bytes(bytes.try_into().unwrap())
                    .unwrap()
                    .to_record_bytes(),
                expected.to_record_bytes(),
            );
        }
        assert!(log.publish(&cx, &[], verify).await.unwrap().is_none());
        assert_eq!(vfs.sync_count(), 2);
        reader.close(&cx).unwrap();
        log.close(&cx).unwrap();
    });
}

#[test]
fn missing_or_invalid_objects_and_bad_chains_never_reach_sync() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1)]).await.unwrap();
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        log.append_symbols(&cx, &[symbol(2)]).await.unwrap();
        let gap = CommitMarker::new(
            CommitSeq::new(2),
            100,
            symbol(1).object_id,
            symbol(2).object_id,
            None,
        );
        assert!(log.publish(&cx, &[gap], verify).await.is_err());
        assert!(
            log.publish(&cx, &[first_marker()], |_, _| ready(Err(
                FrankenError::Unsupported
            )))
            .await
            .is_err()
        );
        assert_eq!(vfs.sync_count(), 0);
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        assert!(!log.needs_recovery());
        log.publish(&cx, &[first_marker()], verify).await.unwrap();
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        assert_eq!(vfs.sync_count(), 2);
        log.close(&cx).unwrap();
    });
}

#[test]
fn first_barrier_failure_writes_no_marker_and_latches_recovery() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        vfs.inject_fault(FaultSpec::power_cut("objects").after_nth_sync(1).build());
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        assert!(vfs.is_powered_off());
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        assert!(log.needs_recovery());
        vfs.power_on();
        let mut reader = open(&vfs, &cx, "markers");
        assert_eq!(reader.file_size(&cx).unwrap(), 0);
        assert!(log.append_symbols(&cx, &[symbol(3)]).await.is_err());
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        reader.close(&cx).unwrap();
        log.close(&cx).unwrap();
    });
}

#[test]
fn second_barrier_failure_does_not_acknowledge_visible_marker_bytes() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        vfs.inject_fault(FaultSpec::power_cut("markers").after_nth_sync(2).build());
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        assert!(vfs.is_powered_off());
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        assert!(log.needs_recovery());
        vfs.power_on();
        let mut reader = open(&vfs, &cx, "markers");
        assert_eq!(
            reader.file_size(&cx).unwrap(),
            u64::try_from(COMMIT_MARKER_RECORD_V1_SIZE).unwrap(),
        );
        reader.close(&cx).unwrap();
        log.close(&cx).unwrap();
    });
}

#[test]
fn torn_marker_write_is_retained_and_never_retried_in_place() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        vfs.inject_fault(FaultSpec::torn_write("markers").valid_bytes(17).build());
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        assert_eq!(vfs.triggered_faults().len(), 1);
        assert_eq!(vfs.sync_count(), 1);
        assert_eq!(log.published_tip(), CommitSeq::ZERO);
        assert!(log.needs_recovery());
        assert!(log.publish(&cx, &[first_marker()], verify).await.is_err());
        let mut reader = open(&vfs, &cx, "markers");
        assert_eq!(reader.file_size(&cx).unwrap(), 17);
        reader.close(&cx).unwrap();
        log.close(&cx).unwrap();
    });
}

#[test]
fn preflight_rejects_aliases_nonempty_files_and_oversized_symbols() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        assert!(
            NativeDurabilityLog::create(
                &cx,
                open(&vfs, &cx, "same"),
                open(&vfs, &cx, "same"),
                NativeDurabilityLimits::default(),
            )
            .is_err()
        );
        let mut occupied = open(&vfs, &cx, "occupied");
        occupied.write(&cx, b"retain me", 0).await.unwrap();
        occupied.close(&cx).unwrap();
        assert!(
            NativeDurabilityLog::create(
                &cx,
                open(&vfs, &cx, "occupied"),
                open(&vfs, &cx, "other"),
                NativeDurabilityLimits::default(),
            )
            .is_err()
        );
        let limits = NativeDurabilityLimits {
            max_symbol_bytes: 3,
            ..NativeDurabilityLimits::default()
        };
        let mut log = NativeDurabilityLog::create(
            &cx,
            open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"),
            limits,
        )
        .unwrap();
        assert!(log.append_symbols(&cx, &[symbol(1)]).await.is_err());
        let mut malformed = symbol(1);
        malformed.oti.t = 2;
        assert!(log.append_symbols(&cx, &[malformed]).await.is_err());
        assert!(!log.needs_recovery());
        log.close(&cx).unwrap();
    });
}

#[test]
fn recovery_decodes_an_erased_source_from_real_asupersync_repair_symbols() {
    use fsqlite_core::raptorq_codec::{AsupersyncCodec, unpack_symbol_key};
    use fsqlite_core::raptorq_integration::{CodecDecodeResult, SymbolCodec};

    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().expect("caller runtime context"));
        let payload: Vec<u8> = (0_u32..4096)
            .map(|index| u8::try_from((index * 17 + 37) % 256).unwrap())
            .collect();
        let capsule_id = ObjectId::derive_from_canonical_bytes(&payload);
        let codec = AsupersyncCodec::default();
        let encoded = codec.encode(&cx, &payload, 512, 2.0).unwrap();
        let k_source = encoded.k_source;
        assert_eq!(k_source, 8);
        assert!(!encoded.repair_symbols.is_empty());
        let oti = Oti {
            f: 4096,
            al: 1,
            t: 512,
            z: 1,
            n: 1,
        };
        let records: Vec<SymbolRecord> = encoded
            .source_symbols
            .into_iter()
            .chain(encoded.repair_symbols)
            .map(|(esi, data)| {
                SymbolRecord::new(capsule_id, oti, esi, data, SymbolRecordFlags::empty())
            })
            .collect();
        let verifier = |object_id, records: Vec<SymbolRecord>| {
            let result = if object_id != capsule_id {
                verify_sync(object_id, &records)
            } else if records.iter().any(|record| record.oti != oti) {
                Err(FrankenError::Unsupported)
            } else {
                let symbols: Vec<_> = records
                    .into_iter()
                    .map(|record| (record.esi, record.symbol_data))
                    .collect();
                match codec.decode(&cx, &symbols, k_source, 512) {
                    Ok(CodecDecodeResult::Success { data, .. }) if data == payload => Ok(()),
                    _ => Err(FrankenError::WalCorrupt {
                        detail: "RaptorQ did not reconstruct the exact committed capsule"
                            .to_owned(),
                    }),
                }
            };
            ready(result)
        };
        let vfs = MemoryVfs::new();
        let mut log = new_log(&vfs, &cx);
        log.append_symbols(&cx, &records).await.unwrap();
        log.append_symbols(&cx, &[symbol(2)]).await.unwrap();
        let marker = CommitMarker::new(
            CommitSeq::new(1),
            100,
            capsule_id,
            symbol(2).object_id,
            None,
        );
        log.publish(&cx, &[marker], &verifier).await.unwrap();
        log.close(&cx).unwrap();
        let mut objects = open(&vfs, &cx, "objects");
        objects.write(&cx, &[0xFF], 51).await.unwrap();
        objects.close(&cx).unwrap();
        let (mut recovered, report) = NativeDurabilityLog::recover(
            &cx,
            open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"),
            NativeDurabilityLimits::default(),
            &verifier,
        )
        .await
        .unwrap();
        assert_eq!(report.erased_symbols, 1);
        assert_eq!(recovered.published_tip(), CommitSeq::new(1));
        let surviving = recovered.read_object(&cx, capsule_id).await.unwrap();
        let sources_only: Vec<_> = surviving
            .into_iter()
            .filter(|record| unpack_symbol_key(record.esi).0.is_source())
            .map(|record| (record.esi, record.symbol_data))
            .collect();
        assert_eq!(sources_only.len(), 7);
        // Negative control: success must depend on the stored repair symbols.
        assert!(matches!(
            codec.decode(&cx, &sources_only, k_source, 512).unwrap(),
            CodecDecodeResult::Failure { .. }
        ));
        recovered.close(&cx).unwrap();
    });
}
