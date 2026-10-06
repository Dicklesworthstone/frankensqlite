//! Native transactions through the existing engine dispatcher and VDBE.
//! This is bytecode/storage integration, not public Connection SQL admission.
#![allow(clippy::future_not_send)]

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionKind, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::{
    CommitSeq, ObjectId, Oti, PageData, PageNumber, PageSize, SqliteValue, SymbolRecord,
    SymbolRecordFlags, reconstruct_systematic_happy_path,
};
use fsqlite_vdbe::engine::{ExecOutcome, MemDatabase, VdbeEngine};
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};
use fsqlite_vfs::{MemoryVfs, Vfs};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore};

// A deterministic envelope codec for dispatch/fault tests only. The final
// file-backed test uses the real authenticated RaptorQ encoder and decoder.
struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(bytes),
            Oti {
                f: u64::from(size),
                al: 1,
                t: size,
                z: 1,
                n: 1,
            },
            0,
            bytes.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
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
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .unwrap()
        .block_on(future);
}
fn page(number: u32) -> PageNumber {
    PageNumber::new(number).unwrap()
}
fn file<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(
        cx,
        Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    )
    .unwrap()
    .0
}
fn pager<V: Vfs>(vfs: &V, cx: &Cx) -> NativePager<V::File, V::File, TestCodec> {
    let log = NativeDurabilityLog::create(
        cx,
        file(vfs, cx, "objects"),
        file(vfs, cx, "markers"),
        NativeDurabilityLimits::default(),
    )
    .unwrap();
    NativePager::new(
        NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap(),
    )
    .unwrap()
}
fn empty_root() -> Vec<u8> {
    let mut bytes = vec![0; 512];
    bytes[0] = 0x0D;
    bytes[5..7].copy_from_slice(&512_u16.to_be_bytes());
    bytes
}

fn insert_program(rows: &[(i32, Vec<u8>)]) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let values = builder.alloc_regs(2);
    let rowid = builder.alloc_reg();
    let record = builder.alloc_reg();
    builder.emit_op(Opcode::OpenWrite, 0, 2, 0, P4::Int(2), 0);
    for (key, payload) in rows {
        builder.emit_op(Opcode::Integer, *key, rowid, 0, P4::None, 0);
        builder.emit_op(Opcode::Integer, key * 7, values, 0, P4::None, 0);
        builder.emit_op(
            Opcode::Blob,
            i32::try_from(payload.len()).unwrap(),
            values + 1,
            0,
            P4::Blob(payload.clone()),
            0,
        );
        builder.emit_op(Opcode::MakeRecord, values, 2, record, P4::None, 0);
        builder.emit_op(Opcode::Insert, 0, record, rowid, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.finish().unwrap()
}
fn scan_program() -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let values = builder.alloc_regs(3);
    let done = builder.emit_label();
    builder.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Int(2), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, done, P4::None, 0);
    let start = i32::try_from(builder.current_addr()).unwrap();
    builder.emit_op(Opcode::Rowid, 0, values, 0, P4::None, 0);
    builder.emit_op(Opcode::Column, 0, 0, values + 1, P4::None, 0);
    builder.emit_op(Opcode::Column, 0, 1, values + 2, P4::None, 0);
    builder.emit_op(Opcode::ResultRow, values, 3, 0, P4::None, 0);
    builder.emit_op(Opcode::Next, 0, start, 0, P4::None, 0);
    builder.resolve_label(done);
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.finish().unwrap()
}
fn delete_program(keys: &[i32]) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let rowid = builder.alloc_reg();
    builder.emit_op(Opcode::OpenWrite, 0, 2, 0, P4::Int(2), 0);
    for key in keys {
        let absent = builder.emit_label();
        builder.emit_op(Opcode::Integer, *key, rowid, 0, P4::None, 0);
        builder.emit_jump_to_label(Opcode::SeekRowid, 0, rowid, absent, P4::None, 0);
        builder.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0);
        builder.resolve_label(absent);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.finish().unwrap()
}
fn expected_rows(rows: &[(i32, Vec<u8>)]) -> Vec<Vec<SqliteValue>> {
    rows.iter()
        .map(|(key, bytes)| {
            vec![
                SqliteValue::Integer(i64::from(*key)),
                SqliteValue::Integer(i64::from(*key) * 7),
                SqliteValue::Blob(Arc::from(bytes.as_slice())),
            ]
        })
        .collect()
}
async fn execute(
    cx: &Cx,
    transaction: TransactionKind,
    program: &VdbeProgram,
) -> (TransactionKind, Vec<Vec<SqliteValue>>) {
    assert!(transaction.is_native());
    let mut engine = VdbeEngine::new_with_execution_cx(
        program.register_count(),
        cx,
        PageSize::new(512).unwrap(),
    );
    // Negative control: no shadow rows can supply the expected results.
    engine.set_database(MemDatabase::new());
    engine.set_reject_mem_fallback(true);
    engine.set_transaction(transaction);
    assert!(matches!(
        engine.execute(program).await.unwrap(),
        ExecOutcome::Done
    ));
    let rows = engine.take_results().into_iter().map(Vec::from).collect();
    let transaction = engine
        .take_transaction()
        .expect("release the storage cursors")
        .expect("return the original storage owner");
    assert!(transaction.is_native());
    (transaction, rows)
}

#[test]
fn vdbe_native_inserts_splits_overflow_and_deletes_survive_dispatch_and_reopen() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let pager = pager(&vfs, &cx);
        let mut seed = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        seed.write_page(&cx, page(2), &empty_root()).await.unwrap();
        seed.commit_at(&cx, 1).await.unwrap();
        drop(seed);
        let old: TransactionKind = pager.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let writer: TransactionKind = pager
            .begin(&cx, TransactionMode::Concurrent)
            .unwrap()
            .into();
        let rows: Vec<_> = (1..=80)
            .map(|key| (key, vec![u8::try_from(key).unwrap(); 80]))
            .chain(std::iter::once((200, vec![0xEF; 4097])))
            .collect();
        let (mut writer, _) = execute(&cx, writer, &insert_program(&rows)).await;
        assert_eq!(
            writer.get_page(&cx, page(2)).await.unwrap().as_bytes()[0],
            0x05,
            "bytecode must split the actual native root"
        );
        assert!(writer.pending_commit_pages().unwrap().len() > 10);
        assert_eq!(pager.committed_tip().unwrap(), CommitSeq::new(1));
        let fresh: TransactionKind = pager.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut fresh, uncommitted) = execute(&cx, fresh, &scan_program()).await;
        assert!(
            uncommitted.is_empty(),
            "private bytecode writes must not leak"
        );
        fresh.rollback(&cx).await.unwrap();
        drop(fresh);
        writer.commit(&cx).await.unwrap();
        drop(writer);
        let (mut old, unchanged) = execute(&cx, old, &scan_program()).await;
        assert!(
            unchanged.is_empty(),
            "conversion must not rebind an old snapshot"
        );
        old.rollback(&cx).await.unwrap();
        drop(old);
        pager.close(&cx).unwrap();
        let (store, report) = NativePageStore::recover(
            &cx,
            file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"),
            TestCodec,
            512,
            NativeDurabilityLimits::default(),
            NativePageLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 2);
        let reopened = NativePager::new(store).unwrap();
        let transaction: TransactionKind = reopened
            .begin(&cx, TransactionMode::Concurrent)
            .unwrap()
            .into();
        let (transaction, actual) = execute(&cx, transaction, &scan_program()).await;
        assert_eq!(actual, expected_rows(&rows));
        let (mut transaction, _) = execute(&cx, transaction, &delete_program(&[42, 200])).await;
        transaction.commit(&cx).await.unwrap();
        drop(transaction);
        reopened.close(&cx).unwrap();
        let (store, _) = NativePageStore::recover(
            &cx,
            file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"),
            TestCodec,
            512,
            NativeDurabilityLimits::default(),
            NativePageLimits::default(),
        )
        .await
        .unwrap();
        let again = NativePager::new(store).unwrap();
        let transaction: TransactionKind =
            again.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut transaction, actual) = execute(&cx, transaction, &scan_program()).await;
        let kept: Vec<_> = rows
            .into_iter()
            .filter(|(key, _)| ![42, 200].contains(key))
            .collect();
        assert_eq!(actual, expected_rows(&kept));
        transaction.rollback(&cx).await.unwrap();
        drop(transaction);
        again.close(&cx).unwrap();
    });
}

#[test]
fn conversion_preserves_private_work_savepoints_extent_and_native_acknowledgement() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let pager = pager(&vfs, &cx);
        let mut native = pager.begin(&cx, TransactionMode::Deferred).unwrap();
        native.write_page(&cx, page(10), &[1; 512]).await.unwrap();
        native.savepoint(&cx, "outer").unwrap();
        assert_eq!(native.allocate_page(&cx).await.unwrap(), page(11));
        native.write_page(&cx, page(50), &[5; 512]).await.unwrap();
        let mut transaction: TransactionKind = native.into();
        assert!(transaction.is_native());
        assert_eq!(transaction.snapshot_db_size(), 0);
        assert_eq!(transaction.live_db_size(), 50);
        assert_eq!(transaction.visible_db_size_bound(), 50);
        assert_eq!(
            transaction.live_reserved_pages(),
            vec![page(10), page(11), page(50)]
        );
        assert_eq!(
            transaction.pending_conflict_pages().unwrap(),
            vec![page(10), page(11), page(50)]
        );
        assert!(!transaction.page_one_in_pending_commit_surface().unwrap());
        assert!(
            !transaction
                .allocate_page_requires_page_one_conflict_tracking()
                .unwrap()
        );
        assert!(
            transaction
                .write_page_requires_page_one_conflict_tracking(PageNumber::ONE)
                .unwrap()
        );
        transaction.rollback_to_savepoint(&cx, "outer").unwrap();
        assert_eq!(transaction.live_db_size(), 10);
        assert_eq!(transaction.visible_db_size_bound(), 50);
        assert_eq!(transaction.write_set_page_numbers(), vec![page(10)]);
        transaction
            .write_page_data(&cx, page(10), PageData::from_vec(vec![2; 512]))
            .await
            .unwrap();
        transaction.release_savepoint(&cx, "outer").unwrap();
        assert!(!transaction.commit_and_retain(&cx).await.unwrap());
        assert_eq!(
            transaction.pager_commit_state(),
            PagerCommitState::Committed
        );
        assert_eq!(transaction.live_db_size(), 10);
        assert_eq!(transaction.snapshot_db_size(), 0);
        assert_eq!(
            transaction.published_visible_commit_seq_hint(),
            Some(CommitSeq::ZERO)
        );
        assert!(transaction.live_reserved_pages().is_empty());
        assert!(!transaction.has_pending_writes());
        let TransactionKind::Native(native) = &transaction else {
            panic!("lost native ownership")
        };
        assert_eq!(native.mode(), TransactionMode::Deferred);
        assert_eq!(
            native.acknowledgement().unwrap().commit_seq,
            CommitSeq::new(1)
        );
        assert!(
            transaction
                .write_page(&cx, page(10), &[3; 512])
                .await
                .is_err()
        );
        assert!(transaction.rollback(&cx).await.is_err());
        drop(transaction);
        pager.close(&cx).unwrap();
    });
}

#[test]
fn native_dispatch_preserves_lazy_mutations_readonly_modes_and_cancelled_cleanup() {
    run(async {
        fn assert_send<T: Send>() {}
        assert_send::<TransactionKind>();
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let pager = pager(&vfs, &cx);
        let mut transaction: TransactionKind =
            pager.begin(&cx, TransactionMode::Deferred).unwrap().into();
        drop(transaction.write_page(&cx, page(2), &[1; 512]));
        drop(transaction.allocate_page(&cx));
        assert!(!transaction.is_writer());
        assert!(!transaction.has_pending_writes());
        assert_eq!(transaction.visible_db_size_bound(), 0);
        transaction
            .write_page(&cx, page(2), &[1; 512])
            .await
            .unwrap();
        transaction.savepoint(&cx, "cancel-safe").unwrap();
        transaction
            .write_page(&cx, page(3), &[2; 512])
            .await
            .unwrap();
        let cancelled = Cx::new();
        cancelled.cancel();
        transaction
            .rollback_to_savepoint(&cancelled, "cancel-safe")
            .unwrap();
        assert_eq!(transaction.pending_commit_pages().unwrap(), vec![page(2)]);
        transaction.rollback(&cancelled).await.unwrap();
        drop(transaction);
        let mut readonly: TransactionKind =
            pager.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        assert!(matches!(
            readonly.write_page(&cx, page(2), &[1; 512]).await,
            Err(FrankenError::ReadOnly)
        ));
        assert!(matches!(
            readonly.allocate_page(&cx).await,
            Err(FrankenError::ReadOnly)
        ));
        assert!(matches!(
            readonly.free_page(&cx, page(2)).await,
            Err(FrankenError::ReadOnly)
        ));
        readonly.commit(&cx).await.unwrap();
        let TransactionKind::Native(native) = &readonly else {
            panic!("lost read-only native handle")
        };
        assert!(native.acknowledgement().is_none());
        assert_eq!(native.mode(), TransactionMode::ReadOnly);
        assert_eq!(pager.committed_tip().unwrap(), CommitSeq::ZERO);
        drop(readonly);
        pager.close(&cx).unwrap();
    });
}

#[test]
fn native_dispatch_keeps_read_dependencies_when_writers_touch_disjoint_pages() {
    run(async {
        let cx = Cx::new();
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let pager = pager(&vfs, &cx);
        let mut seed: TransactionKind = pager
            .begin(&cx, TransactionMode::Concurrent)
            .unwrap()
            .into();
        for number in [2, 3] {
            seed.write_page(&cx, page(number), &[1; 512]).await.unwrap();
        }
        seed.commit(&cx).await.unwrap();
        drop(seed);
        let mut first: TransactionKind = pager
            .begin(&cx, TransactionMode::Concurrent)
            .unwrap()
            .into();
        let mut second: TransactionKind = pager
            .begin(&cx, TransactionMode::Concurrent)
            .unwrap()
            .into();
        for transaction in [&mut first, &mut second] {
            for number in [2, 3] {
                assert_eq!(
                    transaction
                        .get_page(&cx, page(number))
                        .await
                        .unwrap()
                        .as_bytes(),
                    &[1; 512]
                );
            }
        }
        first.write_page(&cx, page(2), &[0; 512]).await.unwrap();
        second.write_page(&cx, page(3), &[0; 512]).await.unwrap();
        first.commit(&cx).await.unwrap();
        drop(first);
        let syncs = vfs.sync_count();
        assert!(matches!(
            second.commit(&cx).await,
            Err(FrankenError::BusySnapshot { .. })
        ));
        assert_eq!(second.pager_commit_state(), PagerCommitState::NotCommitted);
        assert_eq!(second.pending_commit_pages().unwrap(), vec![page(3)]);
        assert_eq!(
            vfs.sync_count(),
            syncs,
            "rejected write skew must not reach storage"
        );
        second.rollback(&cx).await.unwrap();
        drop(second);
        pager.close(&cx).unwrap();
    });
}

#[test]
fn native_dispatch_does_not_turn_either_sync_failure_into_a_rollback_verdict() {
    run(async {
        for (path, ordinal, recovered_count) in [("objects", 1, 0), ("markers", 2, 1)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let pager = pager(&vfs, &cx);
            let mut transaction: TransactionKind = pager
                .begin(&cx, TransactionMode::Concurrent)
                .unwrap()
                .into();
            transaction
                .write_page(&cx, page(2), &[7; 512])
                .await
                .unwrap();
            vfs.inject_fault(FaultSpec::power_cut(path).after_nth_sync(ordinal).build());
            assert!(transaction.commit(&cx).await.is_err());
            assert!(vfs.is_powered_off());
            assert_eq!(transaction.pager_commit_state(), PagerCommitState::InDoubt);
            assert!(matches!(
                transaction.settle_commit(&cx).await,
                Err(FrankenError::BusyRecovery)
            ));
            assert!(transaction.rollback(&cx).await.is_err());
            let TransactionKind::Native(native) = &transaction else {
                panic!("lost uncertain native handle")
            };
            assert!(native.acknowledgement().is_none());
            assert!(
                pager.close(&cx).is_err(),
                "live transaction retains its owner"
            );
            vfs.power_on();
            drop(transaction);
            pager.close(&cx).unwrap();
            let (store, report) = NativePageStore::recover(
                &cx,
                file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"),
                TestCodec,
                512,
                NativeDurabilityLimits::default(),
                NativePageLimits::default(),
            )
            .await
            .unwrap();
            assert_eq!(report.markers.len(), recovered_count);
            let reopened = NativePager::new(store).unwrap();
            let mut transaction: TransactionKind = reopened
                .begin(&cx, TransactionMode::ReadOnly)
                .unwrap()
                .into();
            if recovered_count == 0 {
                assert!(transaction.get_page(&cx, page(2)).await.is_err());
            } else {
                assert_eq!(
                    transaction.get_page(&cx, page(2)).await.unwrap().as_bytes(),
                    &[7; 512]
                );
            }
            transaction.rollback(&cx).await.unwrap();
            drop(transaction);
            reopened.close(&cx).unwrap();
        }
    });
}

#[cfg(unix)]
#[test]
fn vdbe_native_dispatch_reopens_authenticated_file_backed_records() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let directory = tempfile::tempdir().unwrap();
        let objects = directory.path().join("objects");
        let markers = directory.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(
            &cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default(),
        )
        .unwrap();
        let pager = NativePager::new(
            NativePageStore::new(
                log,
                RaptorQNativeCodec::new(Some([7; 32])),
                512,
                NativePageLimits::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let mut native = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        native
            .write_page(&cx, page(2), &empty_root())
            .await
            .unwrap();
        let rows = vec![(1, vec![1; 128]), (9, vec![9; 4097])];
        let (mut transaction, _) = execute(&cx, native.into(), &insert_program(&rows)).await;
        transaction.commit(&cx).await.unwrap();
        drop(transaction);
        pager.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (store, report) = NativePageStore::recover(
            &cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])),
            512,
            NativeDurabilityLimits::default(),
            NativePageLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.markers.len(), 1);
        let reopened = NativePager::new(store).unwrap();
        let native = reopened.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let (mut transaction, actual) = execute(&cx, native.into(), &scan_program()).await;
        assert_eq!(actual, expected_rows(&rows));
        assert_eq!(
            transaction.published_visible_commit_seq_hint(),
            Some(CommitSeq::new(1))
        );
        transaction.rollback(&cx).await.unwrap();
        drop(transaction);
        reopened.close(&cx).unwrap();
    });
}
