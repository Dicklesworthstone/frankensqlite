#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::future_not_send)]
//! Portable native messages -> durable destination -> validated bytecode reads.
//! FaultInjectingVfs retains MemoryVfs bytes: this is not power-loss certification.

use std::future::{Future, ready};
use std::path::Path;
use std::sync::Arc;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
use fsqlite_pager::native::NativePager;
use fsqlite_pager::native::dispatch::NativeSnapshotPager;
use fsqlite_pager::{TransactionHandle, TransactionKind, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::{COMMIT_MARKER_RECORD_V1_SIZE, CommitSeq, ObjectId, PageNumber, PageSize, SqliteValue};
use fsqlite_vdbe::engine::{ExecOutcome, MemDatabase, VdbeEngine};
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
use fsqlite_wal::native_durability::{
    NativeDurabilityLimits, NativeDurabilityLog, NativeReplicationCursor,
    NativeReplicationLimits, NativeReplicationPlan,
};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageRecovery, NativePageSnapshot, NativePageStore};

const DATABASE: ObjectId = ObjectId::from_bytes([0x73; 16]);
const TRANSPORT_KEY: [u8; 32] = [0x27; 32];
const OBJECT_KEY: [u8; 32] = [0x4A; 32];
type File = <MemoryVfs as Vfs>::File;
type Log = NativeDurabilityLog<File, File>;

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn context() -> Cx {
    let cx = Cx::new();
    cx.set_native_cx(asupersync::Cx::current().unwrap());
    cx
}
fn file<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::WAL).unwrap().0
}
fn log<V: Vfs>(vfs: &V, cx: &Cx) -> NativeDurabilityLog<V::File, V::File> {
    NativeDurabilityLog::create(cx, file(vfs, cx, "objects"), file(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap()
}
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn empty_root() -> Vec<u8> {
    let mut bytes = vec![0; 512]; bytes[0] = 0x0D;
    bytes[5..7].copy_from_slice(&512_u16.to_be_bytes()); bytes
}
fn insert(rows: &[(i32, Vec<u8>)]) -> VdbeProgram {
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
fn scan() -> VdbeProgram {
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
fn delete(keys: &[i32]) -> VdbeProgram {
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
async fn execute(cx: &Cx, transaction: TransactionKind, program: &VdbeProgram) -> (TransactionKind, Vec<Vec<SqliteValue>>) {
    assert!(transaction.is_native());
    let mut engine = VdbeEngine::new_with_execution_cx(program.register_count(), cx, PageSize::new(512).unwrap());
    engine.set_database(MemDatabase::new());
    engine.set_reject_mem_fallback(true);
    engine.set_transaction(transaction);
    assert!(matches!(engine.execute(program).await.unwrap(), ExecOutcome::Done));
    let rows = engine.take_results().into_iter().map(Vec::from).collect();
    (engine.take_transaction().unwrap().unwrap(), rows)
}
fn expected(rows: &[(i32, Vec<u8>)]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|(key, bytes)| vec![SqliteValue::Integer(i64::from(*key)),
        SqliteValue::Integer(i64::from(*key) * 7), SqliteValue::Blob(Arc::from(bytes.as_slice()))]).collect()
}
async fn query(cx: &Cx, image: NativePageSnapshot) -> Vec<Vec<SqliteValue>> {
    let owner = NativeSnapshotPager::new(image).unwrap();
    assert!(matches!(owner.begin(cx, TransactionMode::Concurrent), Err(FrankenError::ReadOnly)));
    let reader = owner.begin(cx, TransactionMode::ReadOnly).unwrap().into();
    let (mut reader, rows) = execute(cx, reader, &scan()).await;
    assert!(matches!(reader.write_page(cx, page(2), &[0; 512]).await, Err(FrankenError::ReadOnly)));
    reader.rollback(cx).await.unwrap(); rows
}

async fn source(cx: &Cx) -> (MemoryVfs, Log, Arc<RaptorQNativeCodec>, Vec<(i32, Vec<u8>)>) {
    let vfs = MemoryVfs::new();
    let owner = NativePager::new(NativePageStore::new(log(&vfs, cx),
        RaptorQNativeCodec::new(Some(OBJECT_KEY)), 512, NativePageLimits::default()).unwrap()).unwrap();
    let mut seed = owner.begin(cx, TransactionMode::Concurrent).unwrap();
    seed.write_page(cx, page(2), &empty_root()).await.unwrap(); seed.commit_at(cx, 1).await.unwrap(); drop(seed);
    let rows: Vec<_> = (1..=80).map(|key| (key, vec![u8::try_from(key).unwrap(); 80]))
        .chain(std::iter::once((200, vec![0xEF; 4097]))).collect();
    let writer = owner.begin(cx, TransactionMode::Concurrent).unwrap().into();
    let (mut writer, _) = execute(cx, writer, &insert(&rows)).await;
    assert_eq!(writer.get_page(cx, page(2)).await.unwrap().as_bytes()[0], 0x05);
    assert!(writer.pending_commit_pages().unwrap().len() > 10);
    writer.commit(cx).await.unwrap(); drop(writer);
    let writer = owner.begin(cx, TransactionMode::Concurrent).unwrap().into();
    let (mut writer, _) = execute(cx, writer, &delete(&[42, 200])).await;
    writer.commit(cx).await.unwrap(); drop(writer);
    let (mut physical, codec) = owner.try_into_recovery().ok().expect("retire all live handles");
    physical.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    let (log, report) = physical.finish().unwrap().into_log();
    assert_eq!(report.markers.len(), 3);
    (vfs, log, codec, rows)
}

struct Batch {
    plan: Vec<u8>,
    messages: Vec<Vec<u8>>,
    target: NativeReplicationCursor,
}
async fn export(source: &mut Log, cx: &Cx, base: NativeReplicationCursor, count: usize) -> Batch {
    let limits = NativeReplicationLimits::default();
    let plan = source.export_replication_plan(cx, DATABASE, base, count, limits).await.unwrap().unwrap();
    let mut messages = Vec::new();
    // Only this bounded fixture retains every message. Production callers may
    // write each returned message to their transport before requesting the next.
    for id in plan.object_ids() {
        messages.push(source.export_replication_object(cx, &plan, id, &TRANSPORT_KEY, limits).await.unwrap());
    }
    Batch { plan: plan.to_bytes(cx, &TRANSPORT_KEY, limits).unwrap(), messages, target: plan.target() }
}
fn plan(cx: &Cx, batch: &Batch) -> NativeReplicationPlan {
    NativeReplicationPlan::from_bytes(cx, &batch.plan, DATABASE, &TRANSPORT_KEY, NativeReplicationLimits::default()).unwrap()
}
async fn receive<S: VfsFile, M: VfsFile>(
    destination: &mut NativeDurabilityLog<S, M>, cx: &Cx, batch: &Batch, codec: &RaptorQNativeCodec,
) -> Result<Option<fsqlite_wal::native_durability::NativeDurabilityReceipt>> {
    let Some(mut receiver) = destination.receive_replication_plan(cx, plan(cx, batch), NativeReplicationLimits::default()).await? else {
        return Ok(None);
    };
    for message in batch.messages.iter().rev() {
        receiver.stage_object(cx, message, &TRANSPORT_KEY).await?;
        receiver.stage_object(cx, message, &TRANSPORT_KEY).await?;
    }
    receiver.finish(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ()))).await.map(Some)
}
async fn image<S: VfsFile, M: VfsFile>(
    log: NativeDurabilityLog<S, M>, cx: &Cx, codec: Arc<RaptorQNativeCodec>, seq: CommitSeq,
) -> NativePageSnapshot {
    let mut physical = log.into_recovery();
    physical.prepare(cx, |id, records| ready(codec.decode(cx, id, &records).map(|_| ()))).await.unwrap();
    let mut pages = NativePageRecovery::with_shared_codec(physical.finish().unwrap(), codec, 512, NativePageLimits::default());
    let snapshot = pages.prepare_snapshot(cx, seq).await.unwrap();
    pages.close(cx).unwrap();
    snapshot
}
async fn reopen<V: Vfs>(vfs: &V, cx: &Cx, codec: &RaptorQNativeCodec) -> NativeDurabilityLog<V::File, V::File> {
    // This fixture calls reopen only AFTER successful original-handle rewrite
    // recovery, not as a substitute for repairing uncertain writeback.
    NativeDurabilityLog::recover(cx, file(vfs, cx, "objects"), file(vfs, cx, "markers"),
        NativeDurabilityLimits::default(), |id, records| ready(codec.decode(cx, id, &records).map(|_| ())))
        .await.unwrap().0
}

#[test]
fn authenticated_transport_carries_real_native_trees_and_queryable_history_without_source_handles() {
    run(async {
        let cx = context();
        let (_source_vfs, mut source, codec, rows) = source(&cx).await;
        let first = export(&mut source, &cx, NativeReplicationCursor::genesis(), 2).await;
        let second = export(&mut source, &cx, first.target, 2).await;
        source.close(&cx).unwrap(); drop(source);
        let replica_vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut replica = log(&replica_vfs, &cx);
        let before = replica_vfs.sync_count();
        let receipt = receive(&mut replica, &cx, &first, &codec).await.unwrap().unwrap();
        assert_eq!(receipt.commits, 2); assert_eq!(receipt.last_seq, CommitSeq::new(2));
        assert_eq!(replica_vfs.sync_count() - before, 2);
        let old = image(replica, &cx, Arc::clone(&codec), CommitSeq::new(2)).await;
        assert_eq!(query(&cx, old.clone()).await, expected(&rows));
        let mut replica = reopen(&replica_vfs, &cx, &codec).await;
        let before = replica_vfs.sync_count();
        let receipt = receive(&mut replica, &cx, &second, &codec).await.unwrap().unwrap();
        assert_eq!(receipt.first_seq, CommitSeq::new(3)); assert_eq!(receipt.commits, 1);
        assert_eq!(replica_vfs.sync_count() - before, 2);
        let after = replica_vfs.sync_count();
        assert!(receive(&mut replica, &cx, &second, &codec).await.unwrap().is_none());
        assert!(receive(&mut replica, &cx, &first, &codec).await.unwrap().is_none());
        assert_eq!(replica_vfs.sync_count(), after);
        let latest = image(replica, &cx, Arc::clone(&codec), CommitSeq::new(3)).await;
        let remaining: Vec<_> = rows.iter().filter(|(key, _)| *key != 42 && *key != 200).cloned().collect();
        assert_eq!(query(&cx, latest).await, expected(&remaining));
        assert_eq!(query(&cx, old).await, expected(&rows), "later catch-up and all file closes cannot rewrite an old reader");
    });
}

#[test]
fn transported_groups_reconcile_both_barrier_failures_and_a_torn_marker_prefix() {
    run(async {
        let cx = context();
        let (_source_vfs, mut source, codec, rows) = source(&cx).await;
        let batch = export(&mut source, &cx, NativeReplicationCursor::genesis(), 3).await;
        source.close(&cx).unwrap(); drop(source);
        for case in 0..3 {
            let replica_vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let mut replica = log(&replica_vfs, &cx);
            let before = replica_vfs.sync_count();
            let fault = match case {
                0 => FaultSpec::power_cut("objects").after_nth_sync(before).build(),
                1 => FaultSpec::power_cut("markers").after_nth_sync(before + 1).build(),
                2 => FaultSpec::torn_write("markers").valid_bytes(COMMIT_MARKER_RECORD_V1_SIZE).build(),
                _ => unreachable!(),
            };
            replica_vfs.inject_fault(fault);
            assert!(receive(&mut replica, &cx, &batch, &codec).await.is_err());
            assert_eq!(replica_vfs.triggered_faults().len(), 1);
            assert_eq!(replica.published_tip(), CommitSeq::ZERO);
            assert!(replica.needs_recovery());
            if case != 2 { assert!(replica_vfs.is_powered_off()); replica_vfs.power_on(); }
            assert!(receive(&mut replica, &cx, &batch, &codec).await.is_err());
            let mut recovery = replica.into_recovery();
            recovery.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
            let (mut replica, report) = recovery.finish().unwrap().into_log();
            let recovered = match case { 0 => 0, 1 => 3, 2 => 1, _ => unreachable!() };
            assert_eq!(report.markers.len(), recovered);
            assert!(!report.append_blocked(), "the partial write stopped on an exact complete-marker boundary");
            let before = replica_vfs.sync_count();
            let receipt = receive(&mut replica, &cx, &batch, &codec).await.unwrap();
            if recovered == 3 {
                assert!(receipt.is_none()); assert_eq!(replica_vfs.sync_count(), before);
            } else {
                let receipt = receipt.unwrap();
                assert_eq!(receipt.first_seq, CommitSeq::new(u64::try_from(recovered).unwrap() + 1));
                assert_eq!(receipt.commits, 3 - recovered);
                assert_eq!(replica_vfs.sync_count() - before, 2);
            }
            let snapshot = image(replica, &cx, Arc::clone(&codec), CommitSeq::new(3)).await;
            let remaining: Vec<_> = rows.iter().filter(|(key, _)| *key != 42 && *key != 200).cloned().collect();
            assert_eq!(query(&cx, snapshot).await, expected(&remaining));
        }
    });
}

#[test]
fn valid_transport_tags_cannot_replace_native_object_authentication() {
    run(async {
        let cx = context();
        let (_source_vfs, mut source, codec, rows) = source(&cx).await;
        let batch = export(&mut source, &cx, NativeReplicationCursor::genesis(), 2).await;
        source.close(&cx).unwrap(); drop(source);
        let replica_vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let mut replica = log(&replica_vfs, &cx);
        let wrong_codec = RaptorQNativeCodec::new(Some([0xAA; 32]));
        let before = replica_vfs.sync_count();
        assert!(receive(&mut replica, &cx, &batch, &wrong_codec).await.is_err());
        assert_eq!(replica_vfs.sync_count(), before, "object verification must precede both publication barriers");
        assert_eq!(replica.published_tip(), CommitSeq::ZERO); assert!(replica.needs_recovery());
        let mut recovery = replica.into_recovery();
        recovery.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ()))).await.unwrap();
        let (mut replica, report) = recovery.finish().unwrap().into_log();
        assert!(report.markers.is_empty());
        let receipt = receive(&mut replica, &cx, &batch, &codec).await.unwrap().unwrap();
        assert_eq!(receipt.last_seq, CommitSeq::new(2));
        let snapshot = image(replica, &cx, codec, CommitSeq::new(2)).await;
        assert_eq!(query(&cx, snapshot).await, expected(&rows));
    });
}

#[test]
fn transported_survivors_use_real_raptorq_reconstruction_after_source_erasure() {
    run(async {
        let cx = context();
        let (source_vfs, mut source, codec, rows) = source(&cx).await;
        let limits = NativeReplicationLimits::default();
        let selected = source.export_replication_plan(&cx, DATABASE, NativeReplicationCursor::genesis(), 2, limits)
            .await.unwrap().unwrap();
        let mut erased = None;
        for id in selected.object_ids() {
            let records = source.read_object(&cx, id).await.unwrap();
            let decoded = codec.decode(&cx, id, &records).unwrap();
            if decoded.starts_with(b"FNPG") && decoded.len() > 4097 {
                assert!(records.iter().any(|record| record.esi & (1 << 31) != 0));
                let systematic = records.iter().find(|record| record.esi & (1 << 31) == 0).unwrap();
                erased = Some((id, records.len(), systematic.to_bytes(), decoded));
                break;
            }
        }
        let (id, original_count, wire, payload) = erased.expect("a multi-symbol page capsule must exist");
        let mut physical = file(&source_vfs, &cx, "objects");
        let mut bytes = vec![0; usize::try_from(physical.file_size(&cx).unwrap()).unwrap()];
        assert_eq!(physical.read(&cx, &mut bytes, 0).await.unwrap(), bytes.len());
        let offset = bytes.windows(wire.len()).position(|candidate| candidate == wire.as_slice()).unwrap();
        // Preserve framing/OTI; change a real source payload byte so the native
        // log omits this envelope as an erasure, rather than trusting a mock flag.
        physical.write(&cx, &[wire[51] ^ 0x80], u64::try_from(offset + 51).unwrap()).await.unwrap();
        physical.close(&cx).unwrap();
        let survivors = source.read_object(&cx, id).await.unwrap();
        assert_eq!(survivors.len() + 1, original_count);
        assert_eq!(codec.decode(&cx, id, &survivors).unwrap(), payload);
        let batch = export(&mut source, &cx, NativeReplicationCursor::genesis(), 2).await;
        source.close(&cx).unwrap(); drop(source);
        let replica_vfs = MemoryVfs::new(); let mut replica = log(&replica_vfs, &cx);
        receive(&mut replica, &cx, &batch, &codec).await.unwrap().unwrap();
        let stored = replica.read_object(&cx, id).await.unwrap();
        assert_eq!(stored.len(), survivors.len(), "transport cannot secretly restore a missing source envelope");
        assert_eq!(codec.decode(&cx, id, &stored).unwrap(), payload);
        let snapshot = image(replica, &cx, codec, CommitSeq::new(2)).await;
        assert_eq!(query(&cx, snapshot).await, expected(&rows));
    });
}
