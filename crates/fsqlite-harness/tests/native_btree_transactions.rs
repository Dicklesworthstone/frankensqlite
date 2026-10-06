//! Existing B-tree algorithms over native capsules, not public SQL qualification.
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_btree::traits::{BtreeCursorOps, SeekResult};
use fsqlite_core::native_index::btree::{create_native_btree, with_native_btree};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{ObjectId, Oti, PageNumber, SymbolRecord, SymbolRecordFlags, reconstruct_systematic_happy_path};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore, NativePageTransaction};

struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(bytes),
            Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 }, 0,
            bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START)])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records)
            .map_err(|error| FrankenError::WalCorrupt { detail: error.to_string() })?;
        if ObjectId::derive_from_canonical_bytes(&bytes) != id { return Err(FrankenError::Abort); }
        Ok(bytes)
    }
}
fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
}
fn open<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> V::File {
    vfs.open(cx, Some(Path::new(path)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
type Store = NativePageStore<<MemoryVfs as Vfs>::File, <MemoryVfs as Vfs>::File, TestCodec>;
fn store(vfs: &MemoryVfs, cx: &Cx) -> Store {
    let log = NativeDurabilityLog::create(cx, open(vfs, cx, "objects"), open(vfs, cx, "markers"),
        NativeDurabilityLimits::default()).unwrap();
    NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()
}
async fn rows<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, db: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction, root: PageNumber,
) -> Vec<(i64, Vec<u8>)> {
    with_native_btree(cx, db, txn, root, true, async |cursor| {
        let mut rows = Vec::new();
        if cursor.first(cx).await? {
            loop {
                rows.push((cursor.rowid(cx).await?, cursor.payload(cx).await?));
                if !cursor.next(cx).await? { break; }
            }
        }
        Ok(rows)
    }).await.unwrap()
}
fn payload(row: u8) -> Vec<u8> { vec![row; 80] }
// Valid SQLite packed records: two signed-byte INTEGER fields (key, rowid).
fn index_key(key: u8, rowid: u8) -> Vec<u8> { vec![3, 1, 1, key, rowid] }

#[test]
fn table_splits_overflow_and_deletions_recover_from_native_capsules() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        let root = create_native_btree(&cx, &db, &mut txn, true).unwrap();
        let overflow = vec![0xEF; 4097];
        with_native_btree(&cx, &db, &mut txn, root, true, async |cursor| {
            for row in 0_u8..160 { cursor.table_insert(&cx, i64::from(row), &payload(row)).await?; }
            cursor.table_insert(&cx, 200, &overflow).await?;
            Ok(())
        }).await.unwrap();
        // Negative control against a one-page-only or fake memory-tree path.
        let root_image = db.read_page(&cx, &mut txn, root).unwrap().unwrap();
        assert_eq!(root_image[0], 0x05, "table root must have split into an interior page");
        let before = rows(&cx, &db, &mut txn, root).await;
        assert_eq!(before.len(), 161);
        assert_eq!(before.last().unwrap(), &(200, overflow.clone()));
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let (mut reopened, report) = Store::recover(&cx, open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        let mut txn = reopened.begin(&cx).unwrap();
        assert_eq!(rows(&cx, &reopened, &mut txn, root).await, before);
        with_native_btree(&cx, &reopened, &mut txn, root, true, async |cursor| {
            for row in (0_i64..160).step_by(3).chain(std::iter::once(200)) {
                assert_eq!(cursor.table_move_to(&cx, row).await?, SeekResult::Found);
                cursor.delete(&cx).await?;
            }
            Ok(())
        }).await.unwrap();
        let expected: Vec<_> = before.into_iter().filter(|(row, _)| *row != 200 && row % 3 != 0).collect();
        assert_eq!(rows(&cx, &reopened, &mut txn, root).await, expected);
        reopened.commit(&cx, &mut txn, 101).await.unwrap(); reopened.close(&cx).unwrap();
        let (mut again, _) = Store::recover(&cx, open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).await.unwrap();
        let mut view = again.begin(&cx).unwrap();
        assert_eq!(rows(&cx, &again, &mut view, root).await, expected);
        again.rollback(&mut view).unwrap(); again.close(&cx).unwrap();
    });
}

#[test]
fn real_index_keys_split_seek_and_recover_in_order() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        let root = create_native_btree(&cx, &db, &mut txn, false).unwrap();
        let keys: Vec<_> = (0_u8..96).map(|n| index_key(n, 100 - n)).collect();
        with_native_btree(&cx, &db, &mut txn, root, false, async |cursor| {
            for key in keys.iter().rev() { cursor.index_insert(&cx, key).await?; }
            Ok(())
        }).await.unwrap();
        assert_eq!(db.read_page(&cx, &mut txn, root).unwrap().unwrap()[0], 0x02);
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let (mut reopened, _) = Store::recover(&cx, open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).await.unwrap();
        let mut view = reopened.begin(&cx).unwrap();
        let actual = with_native_btree(&cx, &reopened, &mut view, root, false, async |cursor| {
            for key in &keys { assert_eq!(cursor.index_move_to(&cx, key).await?, SeekResult::Found); }
            let mut scanned = Vec::new();
            if cursor.first(&cx).await? {
                loop { scanned.push(cursor.payload(&cx).await?); if !cursor.next(&cx).await? { break; } }
            }
            Ok(scanned)
        }).await.unwrap();
        assert_eq!(actual, keys);
        reopened.rollback(&mut view).unwrap(); reopened.close(&cx).unwrap();
    });
}

#[test]
fn independent_trees_commit_without_conflict_while_old_snapshots_stay_fixed() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut a = db.begin(&cx).unwrap(); let mut b = db.begin(&cx).unwrap();
        let ra = create_native_btree(&cx, &db, &mut a, true).unwrap();
        let rb = create_native_btree(&cx, &db, &mut b, true).unwrap();
        assert_ne!(ra, rb);
        with_native_btree(&cx, &db, &mut a, ra, true, async |c| c.table_insert(&cx, 1, b"A").await).await.unwrap();
        with_native_btree(&cx, &db, &mut b, rb, true, async |c| c.table_insert(&cx, 2, b"B").await).await.unwrap();
        db.commit(&cx, &mut a, 100).await.unwrap(); db.commit(&cx, &mut b, 101).await.unwrap();
        let mut old = db.begin(&cx).unwrap();
        let mut update = db.begin(&cx).unwrap();
        with_native_btree(&cx, &db, &mut update, ra, true, async |c| c.table_insert(&cx, 3, b"new").await).await.unwrap();
        db.commit(&cx, &mut update, 102).await.unwrap();
        assert_eq!(rows(&cx, &db, &mut old, ra).await, vec![(1, b"A".to_vec())]);
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(rows(&cx, &db, &mut fresh, ra).await.len(), 2);
        assert_eq!(rows(&cx, &db, &mut fresh, rb).await, vec![(2, b"B".to_vec())]);
        db.rollback(&mut old).unwrap(); db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn absent_key_search_records_a_real_tree_dependency_for_commit_validation() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut seed = db.begin(&cx).unwrap();
        let ra = create_native_btree(&cx, &db, &mut seed, true).unwrap();
        let rb = create_native_btree(&cx, &db, &mut seed, true).unwrap();
        db.commit(&cx, &mut seed, 100).await.unwrap();
        let mut a = db.begin(&cx).unwrap(); let mut b = db.begin(&cx).unwrap();
        with_native_btree(&cx, &db, &mut a, ra, true, async |c| {
            assert_eq!(c.table_move_to(&cx, 42).await?, SeekResult::NotFound); Ok(())
        }).await.unwrap();
        with_native_btree(&cx, &db, &mut b, ra, true, async |c| c.table_insert(&cx, 42, b"peer").await).await.unwrap();
        db.commit(&cx, &mut b, 101).await.unwrap();
        with_native_btree(&cx, &db, &mut a, rb, true, async |c| c.table_insert(&cx, 1, b"based on absence").await).await.unwrap();
        assert!(matches!(db.commit(&cx, &mut a, 102).await, Err(FrankenError::BusySnapshot { .. })));
        db.rollback(&mut a).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn failed_tree_scope_restores_prior_work_and_preserves_the_original_error() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        let root = create_native_btree(&cx, &db, &mut txn, true).unwrap();
        with_native_btree(&cx, &db, &mut txn, root, true, async |c| c.table_insert(&cx, 1, b"keep").await).await.unwrap();
        let before = db.read_page(&cx, &mut txn, root).unwrap().unwrap();
        let result = with_native_btree(&cx, &db, &mut txn, root, true, async |c| {
            for n in 2_u8..80 { c.table_insert(&cx, i64::from(n), &payload(n)).await?; }
            c.table_insert(&cx, 200, &vec![0xAA; 3000]).await?;
            Err::<(), _>(FrankenError::CheckViolation { name: "original callback failure".to_owned() })
        }).await;
        assert!(matches!(result, Err(FrankenError::CheckViolation { name }) if name == "original callback failure"));
        assert_eq!(db.read_page(&cx, &mut txn, root).unwrap().unwrap(), before);
        assert_eq!(rows(&cx, &db, &mut txn, root).await, vec![(1, b"keep".to_vec())]);
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let (mut reopened, _) = Store::recover(&cx, open(&vfs, &cx, "objects"),
            open(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).await.unwrap();
        let mut view = reopened.begin(&cx).unwrap();
        assert_eq!(rows(&cx, &reopened, &mut view, root).await, vec![(1, b"keep".to_vec())]);
        reopened.rollback(&mut view).unwrap(); reopened.close(&cx).unwrap();
    });
}

struct NoopWake;
impl Wake for NoopWake { fn wake(self: Arc<Self>) {} }

#[test]
fn dropped_future_restores_a_partially_mutated_tree_before_later_commit() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap(); let root = create_native_btree(&cx, &db, &mut txn, true).unwrap();
        let mutated = AtomicBool::new(false);
        {
            let mut operation = Box::pin(with_native_btree(&cx, &db, &mut txn, root, true, async |c| {
                c.table_insert(&cx, 99, &vec![0xEE; 3000]).await?;
                mutated.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(())
            }));
            let waker = Waker::from(Arc::new(NoopWake));
            let mut context = Context::from_waker(&waker);
            assert!(matches!(operation.as_mut().poll(&mut context), Poll::Pending));
            assert!(mutated.load(Ordering::SeqCst), "must abandon after actual page mutations");
            drop(operation);
        }
        assert!(rows(&cx, &db, &mut txn, root).await.is_empty());
        with_native_btree(&cx, &db, &mut txn, root, true, async |c| c.table_insert(&cx, 1, b"survives").await).await.unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let mut view = db.begin(&cx).unwrap();
        assert_eq!(rows(&cx, &db, &mut view, root).await, vec![(1, b"survives".to_vec())]);
        db.rollback(&mut view).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn root_kind_and_missing_roots_fail_without_accepting_mutations() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap(); let root = create_native_btree(&cx, &db, &mut txn, false).unwrap();
        assert!(with_native_btree(&cx, &db, &mut txn, root, true, async |_| Ok(())).await.is_err());
        let missing = PageNumber::new(100).unwrap();
        assert!(with_native_btree(&cx, &db, &mut txn, missing, false, async |_| Ok(())).await.is_err());
        with_native_btree(&cx, &db, &mut txn, root, false, async |c| {
            assert!(!c.first(&cx).await?); Ok(())
        }).await.unwrap();
        db.rollback(&mut txn).unwrap(); db.close(&cx).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn authenticated_native_files_reopen_actual_btree_rows() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
        let directory = tempfile::tempdir().unwrap();
        let objects = directory.path().join("objects"); let markers = directory.path().join("markers");
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx, vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0, NativeDurabilityLimits::default()).unwrap();
        let mut db = NativePageStore::new(log, RaptorQNativeCodec::new(Some([7; 32])), 512,
            NativePageLimits::default()).unwrap();
        let mut txn = db.begin(&cx).unwrap(); let root = create_native_btree(&cx, &db, &mut txn, true).unwrap();
        with_native_btree(&cx, &db, &mut txn, root, true, async |c| {
            for n in 1_u8..24 { c.table_insert(&cx, i64::from(n), &payload(n)).await?; }
            c.table_insert(&cx, 100, &vec![0xA5; 2049]).await?;
            Ok(())
        }).await.unwrap();
        let expected = rows(&cx, &db, &mut txn, root).await;
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut reopened, report) = NativePageStore::recover(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0, vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])), 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        let mut view = reopened.begin(&cx).unwrap();
        assert_eq!(rows(&cx, &reopened, &mut view, root).await, expected);
        reopened.rollback(&mut view).unwrap(); reopened.close(&cx).unwrap();
    });
}
