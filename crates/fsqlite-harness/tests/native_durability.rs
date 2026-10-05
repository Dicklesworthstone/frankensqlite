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
        .build()
        .unwrap()
        .block_on(future);
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
    let payload = reconstruct_systematic_happy_path(records).map_err(|error| {
        FrankenError::WalCorrupt {
            detail: error.to_string(),
        }
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
            previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
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
                CommitMarker::from_record_bytes(bytes).unwrap().to_record_bytes(),
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
            log.publish(&cx, &[first_marker()], |_, _| ready(Err(FrankenError::Unsupported)))
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
        log.append_symbols(&cx, &[symbol(1), symbol(2)]).await.unwrap();
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
        log.append_symbols(&cx, &[symbol(1), symbol(2)]).await.unwrap();
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
        log.append_symbols(&cx, &[symbol(1), symbol(2)]).await.unwrap();
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
