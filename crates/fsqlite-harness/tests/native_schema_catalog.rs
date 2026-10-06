//! Native schema storage integration, not public Connection/SQL qualification.
use std::future::Future;
use std::path::Path;

use asupersync::runtime::RuntimeBuilder;
use fsqlite_btree::BtreeCursorOps;
use fsqlite_core::native_index::btree::{with_native_btree, catalog::{
    NativeSchemaEntry, NativeSchemaKind, create_native_schema_tree,
    initialize_native_schema, read_native_schema, with_native_schema_tree,
}};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::{ObjectId, Oti, PageNumber, SqliteValue, SymbolRecord,
    SymbolRecordFlags, reconstruct_systematic_happy_path};
use fsqlite_vfs::{MemoryVfs, Vfs, VfsFile};
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
use fsqlite_wal::native_pages::{NativePageLimits, NativePageStore, NativePageTransaction};

// Fault/state cases use deterministic source records. The file-backed test
// uses the real authenticated RaptorQ codec, not this helper.
struct TestCodec;
impl NativeObjectCodec for TestCodec {
    fn encode(&self, _: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> {
        let size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
        Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(payload),
            Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 }, 0,
            payload.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START)])
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
fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }
fn open<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(cx, Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
}
type Store = NativePageStore<<MemoryVfs as Vfs>::File, <MemoryVfs as Vfs>::File, TestCodec>;
fn store(vfs: &MemoryVfs, cx: &Cx) -> Store {
    let log = NativeDurabilityLog::create(cx, open(vfs, cx, "objects"), open(vfs, cx, "markers"),
        NativeDurabilityLimits::default()).unwrap();
    NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()
}
async fn reopen(vfs: &MemoryVfs, cx: &Cx) -> Store {
    Store::recover(cx, open(vfs, cx, "objects"), open(vfs, cx, "markers"), TestCodec,
        512, NativeDurabilityLimits::default(), NativePageLimits::default()).await.unwrap().0
}
async fn table<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, db: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction, name: &str,
) -> NativeSchemaEntry {
    create_native_schema_tree(cx, db, txn, &format!("CREATE TABLE {name}(value TEXT)"),
        async |_, _| Ok(())).await.unwrap()
}
async fn named_rows<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx, db: &NativePageStore<S, M, C>, txn: &mut NativePageTransaction, name: &str,
) -> Vec<(i64, Vec<u8>)> {
    with_native_schema_tree(cx, db, txn, name, NativeSchemaKind::Table, async |cursor| {
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

#[test]
fn named_schema_and_rows_reopen_without_remembered_root_numbers() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        assert!(read_native_schema(&cx, &db, &mut txn).await.is_err());
        assert!(initialize_native_schema(&cx, &db, &mut txn).unwrap());
        assert!(!initialize_native_schema(&cx, &db, &mut txn).unwrap());
        table(&cx, &db, &mut txn, "Items").await;
        with_native_schema_tree(&cx, &db, &mut txn, "items", NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 7, b"persistent row").await).await.unwrap();
        let schema = read_native_schema(&cx, &db, &mut txn).await.unwrap();
        assert_eq!(schema.cookie(), 1);
        assert_eq!(schema.entries().len(), 1);
        assert_eq!(schema.find("ITEMS").unwrap().sql(), "CREATE TABLE Items(value TEXT)");
        let mut marker_file = open(&vfs, &cx, "markers");
        assert_eq!(marker_file.file_size(&cx).unwrap(), 0, "registration is not a fake commit");
        marker_file.close(&cx).unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        drop(db);
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(read_native_schema(&cx, &recovered, &mut txn).await.unwrap(), schema);
        assert_eq!(named_rows(&cx, &recovered, &mut txn, "iTeMs").await,
            vec![(7, b"persistent row".to_vec())]);
        recovered.rollback(&mut txn).unwrap(); recovered.close(&cx).unwrap();
    });
}

#[test]
fn catalog_page_one_splits_without_losing_the_database_header_or_cookie() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        for n in 0..24 { table(&cx, &db, &mut txn, &format!("table_{n}")).await; }
        let p1 = db.read_page(&cx, &mut txn, page(1)).unwrap().unwrap();
        assert_eq!(&p1[..16], b"SQLite format 3\0");
        assert_eq!(p1[100], 0x05, "real catalog must grow into an interior root");
        let header = fsqlite_types::DatabaseHeader::from_bytes(p1[..100].try_into().unwrap()).unwrap();
        assert_eq!(header.schema_cookie, 24);
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        let schema = read_native_schema(&cx, &recovered, &mut txn).await.unwrap();
        assert_eq!(schema.entries().len(), 24);
        for n in 0..24 { assert!(schema.find(&format!("table_{n}")).is_some()); }
        recovered.rollback(&mut txn).unwrap(); recovered.close(&cx).unwrap();
    });
}

#[test]
fn duplicate_names_and_missing_index_owners_do_not_leak_private_schema_edits() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "items").await;
        let before = read_native_schema(&cx, &db, &mut txn).await.unwrap();
        for sql in ["CREATE TABLE ITEMS(value)", "CREATE INDEX items ON items(value)",
            "CREATE INDEX bad ON missing(value)"] {
            assert!(create_native_schema_tree(&cx, &db, &mut txn, sql,
                async |_, _| -> Result<()> { panic!("invalid registration ran the builder") }).await.is_err());
            assert_eq!(read_native_schema(&cx, &db, &mut txn).await.unwrap(), before);
        }
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let mut a = db.begin(&cx).unwrap(); let mut b = db.begin(&cx).unwrap();
        table(&cx, &db, &mut a, "raced").await;
        table(&cx, &db, &mut b, "RACED").await;
        db.commit(&cx, &mut a, 101).await.unwrap();
        assert!(matches!(db.commit(&cx, &mut b, 102).await, Err(FrankenError::BusySnapshot { .. })));
        db.rollback(&mut b).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn catalog_reads_do_not_create_conflicts_between_independent_named_data_writers() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap(); initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "a").await; table(&cx, &db, &mut txn, "b").await;
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let mut a = db.begin(&cx).unwrap(); let mut b = db.begin(&cx).unwrap();
        let mut old = db.begin(&cx).unwrap();
        with_native_schema_tree(&cx, &db, &mut a, "a", NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 1, b"A").await).await.unwrap();
        with_native_schema_tree(&cx, &db, &mut b, "b", NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 2, b"B").await).await.unwrap();
        db.commit(&cx, &mut a, 101).await.unwrap();
        db.commit(&cx, &mut b, 102).await.unwrap();
        assert!(named_rows(&cx, &db, &mut old, "a").await.is_empty());
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(named_rows(&cx, &db, &mut fresh, "b").await, vec![(2, b"B".to_vec())]);
        assert_eq!(read_native_schema(&cx, &db, &mut fresh).await.unwrap().cookie(), 2);
        db.rollback(&mut old).unwrap(); db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    });
}

#[test]
fn populated_index_and_its_catalog_row_recover_at_the_same_commit_boundary() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap(); initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "items").await;
        with_native_schema_tree(&cx, &db, &mut txn, "items", NativeSchemaKind::Table, async |c| {
            for row in 1..8 { c.table_insert(&cx, row, &serialize_record(&[SqliteValue::Integer(8 - row)])).await?; }
            Ok(())
        }).await.unwrap();
        create_native_schema_tree(&cx, &db, &mut txn, "CREATE INDEX by_value ON ITEMS(value)", async |txn, entry| {
            let input = named_rows(&cx, &db, txn, "items").await;
            with_native_btree(&cx, &db, txn, entry.root(), false, async |c| {
                for (rowid, bytes) in &input {
                    let mut values = parse_record(bytes).unwrap(); values.push(SqliteValue::Integer(*rowid));
                    c.index_insert(&cx, &serialize_record(&values)).await?;
                }
                Ok(())
            }).await
        }).await.unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await; let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(read_native_schema(&cx, &recovered, &mut txn).await.unwrap().find("by_value").unwrap().table_name(), "items");
        let keys = with_native_schema_tree(&cx, &recovered, &mut txn, "by_value", NativeSchemaKind::Index, async |c| {
            let mut values = Vec::new();
            if c.first(&cx).await? {
                loop { values.push(parse_record(&c.payload(&cx).await?).unwrap()); if !c.next(&cx).await? { break; } }
            }
            Ok(values)
        }).await.unwrap();
        let expected: Vec<_> = (1..8).map(|n| vec![SqliteValue::Integer(n), SqliteValue::Integer(8 - n)]).collect();
        assert_eq!(keys, expected);
        recovered.rollback(&mut txn).unwrap(); recovered.close(&cx).unwrap();
    });
}

#[test]
fn invalid_persisted_schema_fields_fail_closed_on_named_access_after_recovery() {
    run(async {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap(); initialize_native_schema(&cx, &db, &mut txn).unwrap();
        let entry = table(&cx, &db, &mut txn, "items").await;
        let forged = serialize_record(&[
            SqliteValue::Text("table".into()), SqliteValue::Text("imposter".into()),
            SqliteValue::Text("imposter".into()), SqliteValue::Integer(i64::from(entry.root().get())),
            SqliteValue::Text("CREATE TABLE items(value TEXT)".into()),
        ]);
        with_native_btree(&cx, &db, &mut txn, page(1), true, async |c| c.table_insert(&cx, 1, &forged).await).await.unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await; let mut txn = recovered.begin(&cx).unwrap();
        assert!(read_native_schema(&cx, &recovered, &mut txn).await.is_err());
        assert!(with_native_schema_tree(&cx, &recovered, &mut txn, "imposter", NativeSchemaKind::Table,
            async |_| -> Result<()> { panic!("forged catalog selected a root") }).await.is_err());
        recovered.rollback(&mut txn).unwrap(); recovered.close(&cx).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn authenticated_native_files_recover_schema_and_resolve_rows_by_name() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new(); cx.set_native_cx(asupersync::Cx::current().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let objects = dir.path().join("objects"); let markers = dir.path().join("markers");
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(&cx, vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0, NativeDurabilityLimits::default()).unwrap();
        let mut db = NativePageStore::new(log, RaptorQNativeCodec::new(Some([7; 32])), 512,
            NativePageLimits::default()).unwrap();
        let mut txn = db.begin(&cx).unwrap(); initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "file_table").await;
        with_native_schema_tree(&cx, &db, &mut txn, "file_table", NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 1, b"stored by name").await).await.unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap(); db.close(&cx).unwrap(); drop(db);
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut recovered, report) = NativePageStore::recover(&cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            RaptorQNativeCodec::new(Some([7; 32])), 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).await.unwrap();
        assert_eq!(report.markers.len(), 1);
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(named_rows(&cx, &recovered, &mut txn, "file_table").await,
            vec![(1, b"stored by name".to_vec())]);
        recovered.rollback(&mut txn).unwrap(); recovered.close(&cx).unwrap();
    });
}
