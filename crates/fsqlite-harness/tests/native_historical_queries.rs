#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::future_not_send)]
//! Native historical pages through the real B-tree/VDBE dispatcher, with
//! MemDatabase fallback disabled. This does not select public Connection mode.

use std::future::{Future, ready};
use std::path::Path;
use std::sync::Arc;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::native::dispatch::{NativeSnapshotPager, NativeSnapshotTransaction};
use fsqlite_pager::{PagerCommitState, TransactionHandle, TransactionKind, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::{
    CommitSeq, ObjectId, Oti, PageData, PageNumber, PageSize, SqliteValue,
    SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path,
};
use fsqlite_vdbe::engine::{ExecOutcome, MemDatabase, VdbeEngine};
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};
use fsqlite_vfs::{MemoryVfs, Vfs};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{MAX_NATIVE_SAVEPOINTS, NativePageLimits, NativePageRecovery, NativePageStore};

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
            .map_err(|error| FrankenError::Internal(error.to_string()))?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id { return Err(FrankenError::Abort); }
        Ok(bytes)
    }
}
type File = <MemoryVfs as Vfs>::File;
type Pager = NativePager<File, File, Codec>;
fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
    vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
fn pager(vfs: &MemoryVfs, cx: &Cx) -> Pager {
    let log = NativeDurabilityLog::create(cx, file(vfs, cx, "objects"), file(vfs, cx, "markers"),
        NativeDurabilityLimits::default()).unwrap();
    NativePager::new(NativePageStore::new(log, Codec, 512, NativePageLimits::default()).unwrap()).unwrap()
}
fn empty_root() -> Vec<u8> {
    let mut bytes = vec![0; 512]; bytes[0] = 0x0D;
    bytes[5..7].copy_from_slice(&512_u16.to_be_bytes()); bytes
}
fn insert_program(rows: &[(i32, Vec<u8>)]) -> VdbeProgram {
    let mut builder = ProgramBuilder::new();
    let values = builder.alloc_regs(2); let rowid = builder.alloc_reg(); let record = builder.alloc_reg();
    builder.emit_op(Opcode::OpenWrite, 0, 2, 0, P4::Int(2), 0);
    for (key, payload) in rows {
        builder.emit_op(Opcode::Integer, *key, rowid, 0, P4::None, 0);
        builder.emit_op(Opcode::Integer, key * 7, values, 0, P4::None, 0);
        builder.emit_op(Opcode::Blob, i32::try_from(payload.len()).unwrap(), values + 1, 0, P4::Blob(payload.clone()), 0);
        builder.emit_op(Opcode::MakeRecord, values, 2, record, P4::None, 0);
        builder.emit_op(Opcode::Insert, 0, record, rowid, P4::None, 0);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0); builder.finish().unwrap()
}
fn scan_program() -> VdbeProgram {
    let mut builder = ProgramBuilder::new(); let values = builder.alloc_regs(3); let done = builder.emit_label();
    builder.emit_op(Opcode::OpenRead, 0, 2, 0, P4::Int(2), 0);
    builder.emit_jump_to_label(Opcode::Rewind, 0, 0, done, P4::None, 0);
    let start = i32::try_from(builder.current_addr()).unwrap();
    builder.emit_op(Opcode::Rowid, 0, values, 0, P4::None, 0);
    builder.emit_op(Opcode::Column, 0, 0, values + 1, P4::None, 0);
    builder.emit_op(Opcode::Column, 0, 1, values + 2, P4::None, 0);
    builder.emit_op(Opcode::ResultRow, values, 3, 0, P4::None, 0);
    builder.emit_op(Opcode::Next, 0, start, 0, P4::None, 0);
    builder.resolve_label(done); builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0); builder.finish().unwrap()
}
fn delete_program(keys: &[i32]) -> VdbeProgram {
    let mut builder = ProgramBuilder::new(); let rowid = builder.alloc_reg();
    builder.emit_op(Opcode::OpenWrite, 0, 2, 0, P4::Int(2), 0);
    for key in keys {
        let absent = builder.emit_label();
        builder.emit_op(Opcode::Integer, *key, rowid, 0, P4::None, 0);
        builder.emit_jump_to_label(Opcode::SeekRowid, 0, rowid, absent, P4::None, 0);
        builder.emit_op(Opcode::Delete, 0, 0, 0, P4::None, 0); builder.resolve_label(absent);
    }
    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0); builder.finish().unwrap()
}
fn expected(rows: &[(i32, Vec<u8>)]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|(key, bytes)| vec![SqliteValue::Integer(i64::from(*key)),
        SqliteValue::Integer(i64::from(*key) * 7), SqliteValue::Blob(Arc::from(bytes.as_slice()))]).collect()
}
async fn execute(cx: &Cx, transaction: TransactionKind, program: &VdbeProgram) -> (TransactionKind, Vec<Vec<SqliteValue>>) {
    assert!(transaction.is_native());
    let mut engine = VdbeEngine::new_with_execution_cx(program.register_count(), cx, PageSize::new(512).unwrap());
    engine.set_database(MemDatabase::new()); engine.set_reject_mem_fallback(true); engine.set_transaction(transaction);
    assert!(matches!(engine.execute(program).await.unwrap(), ExecOutcome::Done));
    let rows = engine.take_results().into_iter().map(Vec::from).collect();
    let transaction = engine.take_transaction().expect("release cursors").expect("retain sealed handle");
    (transaction, rows)
}
async fn replay(owner: Pager, cx: &Cx) -> NativePageRecovery<File, File, Codec> {
    let (mut physical, codec) = owner.try_into_recovery().ok().expect("no outstanding live handles");
    physical.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    NativePageRecovery::with_shared_codec(physical.finish().unwrap(), codec, 512, NativePageLimits::default())
}
async fn image_with_root(cx: &Cx) -> NativeSnapshotPager {
    let vfs = MemoryVfs::new(); let owner = pager(&vfs, cx);
    let mut tx = owner.begin(cx, TransactionMode::Concurrent).unwrap();
    tx.write_page(cx, page(2), &empty_root()).await.unwrap(); tx.commit_at(cx, 100).await.unwrap(); drop(tx);
    let mut replay = replay(owner, cx).await;
    let image = replay.prepare_snapshot(cx, CommitSeq::new(1)).await.unwrap();
    replay.close(cx).unwrap(); NativeSnapshotPager::new(image).unwrap()
}

#[test]
fn historical_vdbe_reads_preserve_split_trees_overflow_and_deleted_rows() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let owner = pager(&vfs, &cx);
        let mut seed = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
        seed.write_page(&cx, page(2), &empty_root()).await.unwrap(); seed.commit_at(&cx, 100).await.unwrap(); drop(seed);
        let rows: Vec<_> = (1..=80).map(|key| (key, vec![u8::try_from(key).unwrap(); 80]))
            .chain(std::iter::once((200, vec![0xEF; 4097]))).collect();
        let writer = owner.begin(&cx, TransactionMode::Concurrent).unwrap().into();
        let (mut writer, _) = execute(&cx, writer, &insert_program(&rows)).await;
        assert_eq!(writer.get_page(&cx, page(2)).await.unwrap().as_bytes()[0], 0x05, "actual interior root required");
        assert!(writer.pending_commit_pages().unwrap().len() > 10, "real overflow/split pages required");
        writer.commit(&cx).await.unwrap(); drop(writer);
        let writer = owner.begin(&cx, TransactionMode::Concurrent).unwrap().into();
        let (mut writer, _) = execute(&cx, writer, &delete_program(&[42, 200])).await;
        writer.commit(&cx).await.unwrap(); drop(writer);
        let mut replay = replay(owner, &cx).await;
        let empty = NativeSnapshotPager::new(replay.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap()).unwrap();
        let old = NativeSnapshotPager::new(replay.prepare_snapshot(&cx, CommitSeq::new(2)).await.unwrap()).unwrap();
        let deleted = NativeSnapshotPager::new(replay.prepare_snapshot(&cx, CommitSeq::new(3)).await.unwrap()).unwrap();
        let (store, _) = replay.finish().unwrap(); let current = NativePager::new(store).unwrap();
        let reader = empty.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, result) = execute(&cx, reader, &scan_program()).await;
        assert!(result.is_empty()); reader.rollback(&cx).await.unwrap();
        let reader = old.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, result) = execute(&cx, reader, &scan_program()).await;
        assert_eq!(result, expected(&rows));
        assert_eq!(reader.published_visible_commit_seq_hint(), Some(CommitSeq::new(2)));
        reader.commit(&cx).await.unwrap();
        let remaining: Vec<_> = rows.iter().filter(|(key, _)| *key != 42 && *key != 200).cloned().collect();
        let reader = deleted.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, result) = execute(&cx, reader, &scan_program()).await;
        assert_eq!(result, expected(&remaining)); reader.rollback(&cx).await.unwrap();
        let writer = current.begin(&cx, TransactionMode::Concurrent).unwrap().into();
        let (mut writer, _) = execute(&cx, writer, &insert_program(&[(300, vec![3; 2049])])).await;
        writer.commit(&cx).await.unwrap(); drop(writer);
        assert_eq!(current.committed_tip().unwrap(), CommitSeq::new(4));
        let reader = current.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, result) = execute(&cx, reader, &scan_program()).await;
        let mut newest = remaining; newest.push((300, vec![3; 2049]));
        assert_eq!(result, expected(&newest)); reader.rollback(&cx).await.unwrap(); drop(reader);
        current.close(&cx).unwrap();
        let reader = old.clone().begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, result) = execute(&cx, reader, &scan_program()).await;
        assert_eq!(result, expected(&rows), "closing and extending live storage cannot change history");
        reader.rollback(&cx).await.unwrap();
    });
}

#[test]
fn historical_dispatch_refuses_all_writes_and_bytecode_cannot_mutate_the_image() {
    run(async {
        let cx = Cx::new(); let owner = image_with_root(&cx).await;
        for mode in [TransactionMode::Deferred, TransactionMode::Concurrent, TransactionMode::Immediate, TransactionMode::Exclusive] {
            assert!(matches!(owner.begin(&cx, mode), Err(FrankenError::ReadOnly)));
        }
        let mut typed = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        assert!(matches!(typed.write_page(&cx, page(2), &[0; 512]).await, Err(FrankenError::ReadOnly)));
        let mut tx: TransactionKind = typed.into();
        assert!(matches!(tx.allocate_page(&cx).await, Err(FrankenError::ReadOnly)));
        assert!(matches!(tx.free_page(&cx, page(2)).await, Err(FrankenError::ReadOnly)));
        assert!(matches!(tx.write_page_data(&cx, page(2), PageData::from_vec(vec![0; 512])).await, Err(FrankenError::ReadOnly)));
        assert!(matches!(tx.restore_staged_page_data(&cx, page(2), PageData::from_vec(vec![0; 512])).await, Err(FrankenError::ReadOnly)));
        assert!(tx.try_take_staged_page_data(page(2)).is_none());
        let mut called = false;
        assert!(!tx.try_mutate_staged_page_data(page(2), &mut |_| { called = true; })); assert!(!called);
        assert!(matches!(tx.allocate_page_requires_page_one_conflict_tracking(), Err(FrankenError::ReadOnly)));
        assert!(matches!(tx.free_page_requires_page_one_conflict_tracking(page(2)), Err(FrankenError::ReadOnly)));
        assert!(matches!(tx.write_page_requires_page_one_conflict_tracking(PageNumber::ONE), Err(FrankenError::ReadOnly)));
        assert_eq!(tx.get_page(&cx, page(2)).await.unwrap().as_bytes(), empty_root());
        assert!(!tx.is_writer()); assert!(!tx.has_pending_writes()); assert!(tx.pending_commit_pages().unwrap().is_empty());
        let program = insert_program(&[(1, vec![1; 64])]);
        let mut engine = VdbeEngine::new_with_execution_cx(program.register_count(), &cx, PageSize::new(512).unwrap());
        engine.set_database(MemDatabase::new()); engine.set_reject_mem_fallback(true); engine.set_transaction(tx);
        assert!(matches!(engine.execute(&program).await, Err(FrankenError::ReadOnly)));
        drop(engine);
        let reader = owner.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, rows) = execute(&cx, reader, &scan_program()).await;
        assert!(rows.is_empty()); assert!(reader.pending_conflict_pages().unwrap().is_empty());
        assert!(!reader.commit_and_retain(&cx).await.unwrap());
        assert_eq!(reader.pager_commit_state(), PagerCommitState::Committed);
        let TransactionKind::Native(native) = &reader else { panic!("historical read lost native dispatch") };
        assert_eq!(native.mode(), TransactionMode::ReadOnly); assert!(native.acknowledgement().is_none());
        assert_eq!(native.snapshot_db_size(), 2); assert_eq!(native.live_db_size(), 2); assert!(native.live_reserved_pages().is_empty());
    });
}

#[test]
fn historical_readers_have_independent_cancel_safe_lifecycles_and_bounded_savepoints() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NativeSnapshotPager>(); assert_send_sync::<NativeSnapshotTransaction>();
    run(async {
        let cx = Cx::new(); let owner = image_with_root(&cx).await;
        let mut a = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        let mut b = owner.begin(&cx, TransactionMode::ReadOnly).unwrap();
        a.savepoint(&cx, "point").unwrap(); a.savepoint(&cx, "point").unwrap();
        a.release_savepoint(&cx, "point").unwrap(); a.rollback_to_savepoint(&cx, "point").unwrap();
        a.release_savepoint(&cx, "point").unwrap(); assert!(a.rollback_to_savepoint(&cx, "point").is_err());
        assert!(matches!(a.savepoint(&cx, &"x".repeat(1025)), Err(FrankenError::TooBig)));
        for _ in 0..MAX_NATIVE_SAVEPOINTS { a.savepoint(&cx, "bounded").unwrap(); }
        assert!(matches!(a.savepoint(&cx, "extra"), Err(FrankenError::TooBig)));
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(owner.begin(&cancelled, TransactionMode::ReadOnly), Err(FrankenError::Interrupt)));
        assert!(matches!(a.get_page(&cancelled, page(2)).await, Err(FrankenError::Interrupt)));
        assert!(matches!(a.commit(&cancelled).await, Err(FrankenError::Interrupt)));
        a.rollback_to_savepoint(&cancelled, "bounded").unwrap(); a.release_savepoint(&cancelled, "bounded").unwrap();
        a.rollback(&cancelled).await.unwrap(); a.rollback(&cancelled).await.unwrap();
        assert!(matches!(a.get_page(&cx, page(2)).await, Err(FrankenError::Abort)));
        assert_eq!(b.get_page(&cx, page(2)).await.unwrap().as_bytes(), empty_root());
        assert!(b.get_page(&cx, page(999)).await.is_err());
        b.commit_at(&cx, u64::MAX).await.unwrap();
        assert_eq!(b.settle_commit(&cancelled).await.unwrap(), PagerCommitState::Committed);
        assert!(b.rollback(&cx).await.is_err()); assert!(b.acknowledgement().is_none());
        assert_eq!(owner.snapshot().commit_seq(), CommitSeq::new(1));
        assert_eq!(owner.snapshot().commit_time_unix_ns(), Some(100));
    });
}

#[cfg(unix)]
#[test]
fn historical_queries_reopen_real_authenticated_native_files() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    use fsqlite_wal::native_durability::NativeRecovery;
    run(async {
        let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
        let directory = tempfile::tempdir().unwrap().keep();
        let objects = directory.join("objects"); let markers = directory.join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx, vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0, NativeDurabilityLimits::default()).unwrap();
        let pager = NativePager::new(NativePageStore::new(log, RaptorQNativeCodec::new(Some([7; 32])),
            512, NativePageLimits::default()).unwrap()).unwrap();
        let mut seed = pager.begin(&cx, TransactionMode::Concurrent).unwrap();
        seed.write_page(&cx, page(2), &empty_root()).await.unwrap();
        let rows = vec![(1, vec![1; 128]), (9, vec![9; 4097])];
        let (mut tx, _) = execute(&cx, seed.into(), &insert_program(&rows)).await;
        tx.commit(&cx).await.unwrap(); drop(tx);
        let tx = pager.begin(&cx, TransactionMode::Concurrent).unwrap().into();
        let (mut tx, _) = execute(&cx, tx, &delete_program(&[9])).await;
        tx.commit(&cx).await.unwrap(); drop(tx); pager.close(&cx).unwrap(); drop(pager);
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let mut physical = NativeRecovery::new(&cx, vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0, NativeDurabilityLimits::default()).unwrap();
        let codec = RaptorQNativeCodec::new(Some([7; 32]));
        physical.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let mut replay = NativePageRecovery::new(physical.finish().unwrap(), codec, 512, NativePageLimits::default());
        let old = NativeSnapshotPager::new(replay.prepare_snapshot(&cx, CommitSeq::new(1)).await.unwrap()).unwrap();
        let latest = NativeSnapshotPager::new(replay.prepare_snapshot(&cx, CommitSeq::new(2)).await.unwrap()).unwrap();
        replay.close(&cx).unwrap(); drop(replay);
        for (image, expected) in [(old, expected(&rows)), (latest, expected(&rows[..1]))] {
            let tx = image.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
            let (mut tx, actual) = execute(&cx, tx, &scan_program()).await;
            assert_eq!(actual, expected); tx.rollback(&cx).await.unwrap();
        }
        // Private fixture is deliberately retained; no production namespace is
        // touched and no file deletion or physical crash certificate is claimed.
    });
}

mod replica_query_tests {
    use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};

    use super::*;

    type History = NativePageRecovery<File, File, Codec>;
    type Rows = Vec<(i32, Vec<u8>)>;

    async fn history(vfs: &MemoryVfs, cx: &Cx) -> (History, Rows) {
        let owner = pager(vfs, cx);
        let mut seed = owner.begin(cx, TransactionMode::Concurrent).unwrap();
        seed.write_page(cx, page(2), &empty_root()).await.unwrap();
        seed.commit_at(cx, 100).await.unwrap();
        drop(seed);
        let rows: Rows = (1..=80).map(|key| (key, vec![u8::try_from(key).unwrap(); 80]))
            .chain(std::iter::once((200, vec![0xEF; 4097]))).collect();
        let writer = owner.begin(cx, TransactionMode::Concurrent).unwrap().into();
        let (mut writer, _) = execute(cx, writer, &insert_program(&rows)).await;
        assert_eq!(writer.get_page(cx, page(2)).await.unwrap().as_bytes()[0], 0x05);
        assert!(writer.pending_commit_pages().unwrap().len() > 10);
        writer.commit(cx).await.unwrap();
        drop(writer);
        let writer = owner.begin(cx, TransactionMode::Concurrent).unwrap().into();
        let (mut writer, _) = execute(cx, writer, &delete_program(&[42, 200])).await;
        writer.commit(cx).await.unwrap();
        drop(writer);
        (replay(owner, cx).await, rows)
    }

    fn replica<V: Vfs>(vfs: &V, cx: &Cx) -> NativeDurabilityLog<V::File, V::File> {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        NativeDurabilityLog::create(cx,
            vfs.open(cx, Some(Path::new("replica-objects")), flags).unwrap().0,
            vfs.open(cx, Some(Path::new("replica-markers")), flags).unwrap().0,
            NativeDurabilityLimits::default()).unwrap()
    }

    fn remaining(rows: &Rows) -> Rows {
        rows.iter().filter(|(key, _)| *key != 42 && *key != 200).cloned().collect()
    }

    async fn query(cx: &Cx, image: &NativeSnapshotPager) -> Vec<Vec<SqliteValue>> {
        let reader = image.begin(cx, TransactionMode::ReadOnly).unwrap().into();
        let (mut reader, rows) = execute(cx, reader, &scan_program()).await;
        assert!(reader.pending_commit_pages().unwrap().is_empty());
        reader.commit(cx).await.unwrap();
        let TransactionKind::Native(reader) = reader else { panic!("replica lost native dispatch") };
        assert!(reader.acknowledgement().is_none(), "a read is not a replicated commit ACK");
        rows
    }

    #[test]
    fn replicated_images_query_real_bytecode_and_keep_readers_stable_during_catchup() {
        run(async {
            let cx = Cx::new();
            let memory = MemoryVfs::new();
            let (mut source, rows) = history(&memory, &cx).await;
            // Move the same namespace into the counter wrapper. Original source
            // handles stay unwrapped; only destination I/O contributes here.
            let vfs = FaultInjectingVfs::new(memory);
            let mut replica = replica(&vfs, &cx);
            let before = vfs.sync_count();
            let (receipt, image) = source.replicate_snapshot_to(&cx, &mut replica, 2)
                .await.unwrap().unwrap();
            assert_eq!(receipt.commits, 2);
            assert_eq!(receipt.first_seq, CommitSeq::new(1));
            assert_eq!(receipt.last_seq, CommitSeq::new(2));
            assert_eq!(vfs.sync_count() - before, 2, "one real barrier pair for two markers");
            let old = NativeSnapshotPager::new(image).unwrap();
            let held: TransactionKind = old.begin(&cx, TransactionMode::ReadOnly).unwrap().into();
            assert_eq!(held.get_page(&cx, page(2)).await.unwrap().as_bytes()[0], 0x05);
            let (receipt, image) = source.replicate_snapshot_to(&cx, &mut replica, 128)
                .await.unwrap().unwrap();
            assert_eq!(receipt.commits, 1);
            assert_eq!(receipt.first_seq, CommitSeq::new(3));
            assert_eq!(image.commit_seq(), receipt.last_seq);
            assert_eq!(vfs.sync_count() - before, 4);
            let latest = NativeSnapshotPager::new(image).unwrap();
            let (mut held, actual) = execute(&cx, held, &scan_program()).await;
            assert_eq!(actual, expected(&rows), "reader cannot switch to later replicated deletions");
            held.rollback(&cx).await.unwrap();
            assert_eq!(query(&cx, &latest).await, expected(&remaining(&rows)));
            assert!(matches!(latest.begin(&cx, TransactionMode::Concurrent), Err(FrankenError::ReadOnly)));
            let mut reader = latest.begin(&cx, TransactionMode::ReadOnly).unwrap();
            assert!(matches!(reader.write_page(&cx, page(2), &empty_root()).await,
                Err(FrankenError::ReadOnly)));
            reader.rollback(&cx).await.unwrap();
            assert!(source.replicate_snapshot_to(&cx, &mut replica, 1).await.unwrap().is_none());
            assert_eq!(vfs.sync_count() - before, 4, "reads and equal-tip checks do not publish");
            source.close(&cx).unwrap();

            // Rebuild solely from destination streams, then query the same rows
            // through the sealed reader with the source already closed.
            let mut physical = replica.into_recovery();
            physical.prepare(&cx, |id, records| ready(Codec.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let mut replay = NativePageRecovery::new(physical.finish().unwrap(), Codec,
                512, NativePageLimits::default());
            let independently_recovered = NativeSnapshotPager::new(
                replay.prepare_snapshot(&cx, CommitSeq::new(3)).await.unwrap()).unwrap();
            replay.close(&cx).unwrap();
            assert_eq!(query(&cx, &independently_recovered).await, expected(&remaining(&rows)));
            assert_eq!(query(&cx, &old).await, expected(&rows));
        });
    }

    #[test]
    fn failed_replica_barriers_never_return_images_and_resume_from_recovered_markers() {
        run(async {
            for (path, index_delta, recovered_tip) in [
                ("replica-objects", 0, 1_u64), ("replica-markers", 1, 2_u64),
            ] {
                let cx = Cx::new();
                let memory = MemoryVfs::new();
                let (mut source, rows) = history(&memory, &cx).await;
                let vfs = FaultInjectingVfs::new(memory);
                let mut replica = replica(&vfs, &cx);
                let (_, old) = source.replicate_snapshot_to(&cx, &mut replica, 1)
                    .await.unwrap().unwrap();
                let old = NativeSnapshotPager::new(old).unwrap();
                assert!(query(&cx, &old).await.is_empty());
                let before = vfs.sync_count();
                vfs.inject_fault(FaultSpec::power_cut(path)
                    .after_nth_sync(before + index_delta).build());
                assert!(source.replicate_snapshot_to(&cx, &mut replica, 1).await.is_err());
                assert!(vfs.is_powered_off());
                assert_eq!(vfs.triggered_faults().len(), 1);
                assert_eq!(vfs.sync_count(), before + index_delta + 1);
                assert!(replica.needs_recovery());
                assert_eq!(replica.published_tip(), CommitSeq::new(1));
                // Already delivered immutable images have no dependency on
                // these file handles or the newly indeterminate destination.
                assert!(query(&cx, &old).await.is_empty());
                vfs.power_on();
                assert!(source.replicate_snapshot_to(&cx, &mut replica, 1).await.is_err());
                assert_eq!(vfs.sync_count(), before + index_delta + 1,
                    "power restoration alone cannot authorize another append");

                // This fixture retains MemoryVfs writes across a sync error.
                // It models explicit barrier failures, not physical power loss.
                let mut physical = replica.into_recovery();
                physical.prepare(&cx, |id, records| ready(Codec.decode(&cx, id, &records).map(|_| ())))
                    .await.unwrap();
                let recovered = physical.finish().unwrap();
                assert_eq!(recovered.report().markers.len(), usize::try_from(recovered_tip).unwrap());
                let (mut replica, _) = recovered.into_log();
                let before = vfs.sync_count();
                let (receipt, image) = source.replicate_snapshot_to(&cx, &mut replica, 128)
                    .await.unwrap().unwrap();
                assert_eq!(receipt.first_seq, CommitSeq::new(recovered_tip + 1));
                assert_eq!(receipt.last_seq, CommitSeq::new(3));
                assert_eq!(receipt.commits, usize::try_from(3 - recovered_tip).unwrap());
                assert_eq!(vfs.sync_count() - before, 2);
                assert!(!replica.needs_recovery());
                let latest = NativeSnapshotPager::new(image).unwrap();
                assert_eq!(query(&cx, &latest).await, expected(&remaining(&rows)));
                assert!(query(&cx, &old).await.is_empty());
                assert!(source.replicate_snapshot_to(&cx, &mut replica, 128).await.unwrap().is_none());
                assert_eq!(vfs.sync_count() - before, 2);
                replica.close(&cx).unwrap();
                source.close(&cx).unwrap();
            }
        });
    }
}
