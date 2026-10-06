//! Native schema storage integration, not public Connection/SQL qualification.
// Native catalog records are canonical UTF-8 by definition, so the fixtures
// build and inspect them with the UTF-8 record codec.
#![allow(clippy::disallowed_methods)]
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_btree::BtreeCursorOps;
use fsqlite_core::native_index::btree::{
    catalog::{
        NativeSchemaEntry, NativeSchemaKind, create_native_schema_tree, drop_native_schema_tree,
        initialize_native_schema, read_native_schema, with_native_schema_statement,
        with_native_schema_tree,
    },
    with_native_btree,
};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::record::{parse_record, serialize_record};
use fsqlite_types::{
    ObjectId, Oti, PageNumber, SqliteValue, SymbolRecord, SymbolRecordFlags,
    reconstruct_systematic_happy_path,
};
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
        Ok(vec![SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(payload),
            Oti {
                f: u64::from(size),
                al: 1,
                t: size,
                z: 1,
                n: 1,
            },
            0,
            payload.to_vec(),
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )])
    }
    fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        let bytes = reconstruct_systematic_happy_path(records).map_err(|error| {
            FrankenError::WalCorrupt {
                detail: error.to_string(),
            }
        })?;
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
fn page(n: u32) -> PageNumber {
    PageNumber::new(n).unwrap()
}
fn open<V: Vfs>(vfs: &V, cx: &Cx, name: &str) -> V::File {
    vfs.open(
        cx,
        Some(Path::new(name)),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    )
    .unwrap()
    .0
}
type Store = NativePageStore<<MemoryVfs as Vfs>::File, <MemoryVfs as Vfs>::File, TestCodec>;
fn store(vfs: &MemoryVfs, cx: &Cx) -> Store {
    let log = NativeDurabilityLog::create(
        cx,
        open(vfs, cx, "objects"),
        open(vfs, cx, "markers"),
        NativeDurabilityLimits::default(),
    )
    .unwrap();
    NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()
}
async fn reopen(vfs: &MemoryVfs, cx: &Cx) -> Store {
    Store::recover(
        cx,
        open(vfs, cx, "objects"),
        open(vfs, cx, "markers"),
        TestCodec,
        512,
        NativeDurabilityLimits::default(),
        NativePageLimits::default(),
    )
    .await
    .unwrap()
    .0
}
async fn table<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx,
    db: &NativePageStore<S, M, C>,
    txn: &mut NativePageTransaction,
    name: &str,
) -> NativeSchemaEntry {
    create_native_schema_tree(
        cx,
        db,
        txn,
        &format!("CREATE TABLE {name}(value TEXT)"),
        async |_, _| Ok(()),
    )
    .await
    .unwrap()
}
async fn named_rows<S: VfsFile, M: VfsFile, C: NativeObjectCodec>(
    cx: &Cx,
    db: &NativePageStore<S, M, C>,
    txn: &mut NativePageTransaction,
    name: &str,
) -> Vec<(i64, Vec<u8>)> {
    with_native_schema_tree(cx, db, txn, name, NativeSchemaKind::Table, async |cursor| {
        let mut rows = Vec::new();
        if cursor.first(cx).await? {
            loop {
                rows.push((cursor.rowid(cx).await?, cursor.payload(cx).await?));
                if !cursor.next(cx).await? {
                    break;
                }
            }
        }
        Ok(rows)
    })
    .await
    .unwrap()
}

#[test]
fn named_schema_and_rows_reopen_without_remembered_root_numbers() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        assert!(read_native_schema(&cx, &db, &mut txn).await.is_err());
        assert!(initialize_native_schema(&cx, &db, &mut txn).unwrap());
        assert!(!initialize_native_schema(&cx, &db, &mut txn).unwrap());
        table(&cx, &db, &mut txn, "Items").await;
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "items",
            NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 7, b"persistent row").await,
        )
        .await
        .unwrap();
        let schema = read_native_schema(&cx, &db, &mut txn).await.unwrap();
        assert_eq!(schema.cookie(), 1);
        assert_eq!(schema.entries().len(), 1);
        assert_eq!(
            schema.find("ITEMS").unwrap().sql(),
            "CREATE TABLE Items(value TEXT)"
        );
        let mut marker_file = open(&vfs, &cx, "markers");
        assert_eq!(
            marker_file.file_size(&cx).unwrap(),
            0,
            "registration is not a fake commit"
        );
        marker_file.close(&cx).unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        drop(db);
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(
            read_native_schema(&cx, &recovered, &mut txn).await.unwrap(),
            schema
        );
        assert_eq!(
            named_rows(&cx, &recovered, &mut txn, "iTeMs").await,
            vec![(7, b"persistent row".to_vec())]
        );
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn catalog_page_one_splits_without_losing_the_database_header_or_cookie() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        for n in 0..24 {
            table(&cx, &db, &mut txn, &format!("table_{n}")).await;
        }
        let p1 = db.read_page(&cx, &mut txn, page(1)).unwrap().unwrap();
        assert_eq!(&p1[..16], b"SQLite format 3\0");
        assert_eq!(
            p1[100], 0x05,
            "real catalog must grow into an interior root"
        );
        let header =
            fsqlite_types::DatabaseHeader::from_bytes(p1[..100].try_into().unwrap()).unwrap();
        assert_eq!(header.schema_cookie, 24);
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        let schema = read_native_schema(&cx, &recovered, &mut txn).await.unwrap();
        assert_eq!(schema.entries().len(), 24);
        for n in 0..24 {
            assert!(schema.find(&format!("table_{n}")).is_some());
        }
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn duplicate_names_and_missing_index_owners_do_not_leak_private_schema_edits() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "items").await;
        let before = read_native_schema(&cx, &db, &mut txn).await.unwrap();
        for sql in [
            "CREATE TABLE ITEMS(value)",
            "CREATE INDEX items ON items(value)",
            "CREATE INDEX bad ON missing(value)",
        ] {
            assert!(
                create_native_schema_tree(&cx, &db, &mut txn, sql, async |_, _| -> Result<()> {
                    panic!("invalid registration ran the builder")
                })
                .await
                .is_err()
            );
            assert_eq!(
                read_native_schema(&cx, &db, &mut txn).await.unwrap(),
                before
            );
        }
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        table(&cx, &db, &mut a, "raced").await;
        table(&cx, &db, &mut b, "RACED").await;
        db.commit(&cx, &mut a, 101).await.unwrap();
        assert!(matches!(
            db.commit(&cx, &mut b, 102).await,
            Err(FrankenError::BusySnapshot { .. })
        ));
        db.rollback(&mut b).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn catalog_reads_do_not_create_conflicts_between_independent_named_data_writers() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "a").await;
        table(&cx, &db, &mut txn, "b").await;
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let mut a = db.begin(&cx).unwrap();
        let mut b = db.begin(&cx).unwrap();
        let mut old = db.begin(&cx).unwrap();
        with_native_schema_tree(&cx, &db, &mut a, "a", NativeSchemaKind::Table, async |c| {
            c.table_insert(&cx, 1, b"A").await
        })
        .await
        .unwrap();
        with_native_schema_tree(&cx, &db, &mut b, "b", NativeSchemaKind::Table, async |c| {
            c.table_insert(&cx, 2, b"B").await
        })
        .await
        .unwrap();
        db.commit(&cx, &mut a, 101).await.unwrap();
        db.commit(&cx, &mut b, 102).await.unwrap();
        assert!(named_rows(&cx, &db, &mut old, "a").await.is_empty());
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(
            named_rows(&cx, &db, &mut fresh, "b").await,
            vec![(2, b"B".to_vec())]
        );
        assert_eq!(
            read_native_schema(&cx, &db, &mut fresh)
                .await
                .unwrap()
                .cookie(),
            2
        );
        db.rollback(&mut old).unwrap();
        db.rollback(&mut fresh).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn populated_index_and_its_catalog_row_recover_at_the_same_commit_boundary() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "items").await;
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "items",
            NativeSchemaKind::Table,
            async |c| {
                for row in 1..8 {
                    c.table_insert(
                        &cx,
                        row,
                        &serialize_record(&[SqliteValue::Integer(8 - row)]),
                    )
                    .await?;
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        create_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "CREATE INDEX by_value ON ITEMS(value)",
            async |txn, entry| {
                let input = named_rows(&cx, &db, txn, "items").await;
                with_native_btree(&cx, &db, txn, entry.root(), false, async |c| {
                    for (rowid, bytes) in &input {
                        let mut values = parse_record(bytes).unwrap();
                        values.push(SqliteValue::Integer(*rowid));
                        c.index_insert(&cx, &serialize_record(&values)).await?;
                    }
                    Ok(())
                })
                .await
            },
        )
        .await
        .unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(
            read_native_schema(&cx, &recovered, &mut txn)
                .await
                .unwrap()
                .find("by_value")
                .unwrap()
                .table_name(),
            "items"
        );
        let keys = with_native_schema_tree(
            &cx,
            &recovered,
            &mut txn,
            "by_value",
            NativeSchemaKind::Index,
            async |c| {
                let mut values = Vec::new();
                if c.first(&cx).await? {
                    loop {
                        values.push(parse_record(&c.payload(&cx).await?).unwrap());
                        if !c.next(&cx).await? {
                            break;
                        }
                    }
                }
                Ok(values)
            },
        )
        .await
        .unwrap();
        let expected: Vec<_> = (1..8)
            .map(|n| vec![SqliteValue::Integer(n), SqliteValue::Integer(8 - n)])
            .collect();
        assert_eq!(keys, expected);
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn invalid_persisted_schema_fields_fail_closed_on_named_access_after_recovery() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        let entry = table(&cx, &db, &mut txn, "items").await;
        let forged = serialize_record(&[
            SqliteValue::Text("table".into()),
            SqliteValue::Text("imposter".into()),
            SqliteValue::Text("imposter".into()),
            SqliteValue::Integer(i64::from(entry.root().get())),
            SqliteValue::Text("CREATE TABLE items(value TEXT)".into()),
        ]);
        with_native_btree(&cx, &db, &mut txn, page(1), true, async |c| {
            c.table_insert(&cx, 1, &forged).await
        })
        .await
        .unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        assert!(read_native_schema(&cx, &recovered, &mut txn).await.is_err());
        assert!(
            with_native_schema_tree(
                &cx,
                &recovered,
                &mut txn,
                "imposter",
                NativeSchemaKind::Table,
                async |_| -> Result<()> { panic!("forged catalog selected a root") }
            )
            .await
            .is_err()
        );
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[cfg(unix)]
#[test]
fn authenticated_native_files_recover_schema_and_resolve_rows_by_name() {
    use fsqlite_wal::native_commit::durable::codec::RaptorQNativeCodec;
    run(async {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let vfs = fsqlite_vfs::unix::UnixVfs::new();
        let objects = dir.path().join("objects");
        let markers = dir.path().join("markers");
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let log = NativeDurabilityLog::create(
            &cx,
            vfs.open(&cx, Some(&objects), flags).unwrap().0,
            vfs.open(&cx, Some(&markers), flags).unwrap().0,
            NativeDurabilityLimits::default(),
        )
        .unwrap();
        let mut db = NativePageStore::new(
            log,
            RaptorQNativeCodec::new(Some([7; 32])),
            512,
            NativePageLimits::default(),
        )
        .unwrap();
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "file_table").await;
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "file_table",
            NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 1, b"stored by name").await,
        )
        .await
        .unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        drop(db);
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        let (mut recovered, report) = NativePageStore::recover(
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
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(
            named_rows(&cx, &recovered, &mut txn, "file_table").await,
            vec![(1, b"stored by name".to_vec())]
        );
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn table_drop_retires_indexes_and_old_snapshots_keep_their_original_named_roots() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        let original = table(&cx, &db, &mut txn, "items").await;
        table(&cx, &db, &mut txn, "other").await;
        let index = create_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "CREATE INDEX by_value ON items(value)",
            async |_, _| Ok(()),
        )
        .await
        .unwrap();
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "items",
            NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 1, b"old object").await,
        )
        .await
        .unwrap();
        db.commit(&cx, &mut txn, 100).await.unwrap();
        let mut old = db.begin(&cx).unwrap();
        let mut writer = db.begin(&cx).unwrap();
        with_native_schema_tree(
            &cx,
            &db,
            &mut writer,
            "items",
            NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 2, b"stale write").await,
        )
        .await
        .unwrap();
        let mut ddl = db.begin(&cx).unwrap();
        let removed = drop_native_schema_tree(&cx, &db, &mut ddl, "ITEMS", NativeSchemaKind::Table)
            .await
            .unwrap();
        assert_eq!(removed.len(), 2);
        for root in [original.root(), index.root()] {
            assert!(db.read_page(&cx, &mut ddl, root).unwrap().is_none());
        }
        let after_drop = read_native_schema(&cx, &db, &mut ddl).await.unwrap();
        assert_eq!(after_drop.cookie(), 4);
        assert_eq!(after_drop.entries().len(), 1);
        let replacement = table(&cx, &db, &mut ddl, "items").await;
        assert_ne!(replacement.root(), original.root());
        db.commit(&cx, &mut ddl, 101).await.unwrap();
        assert!(matches!(
            db.commit(&cx, &mut writer, 102).await,
            Err(FrankenError::BusySnapshot { .. })
        ));
        assert_eq!(
            named_rows(&cx, &db, &mut old, "items").await,
            vec![(1, b"old object".to_vec())]
        );
        assert!(
            read_native_schema(&cx, &db, &mut old)
                .await
                .unwrap()
                .find("by_value")
                .is_some()
        );
        db.rollback(&mut writer).unwrap();
        db.rollback(&mut old).unwrap();
        db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        let schema = read_native_schema(&cx, &recovered, &mut txn).await.unwrap();
        assert_eq!(schema.cookie(), 5);
        assert!(schema.find("by_value").is_none());
        assert_eq!(schema.find("items").unwrap().root(), replacement.root());
        assert!(
            named_rows(&cx, &recovered, &mut txn, "items")
                .await
                .is_empty()
        );
        assert!(
            recovered
                .read_page(&cx, &mut txn, original.root())
                .unwrap()
                .is_none()
        );
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn unique_index_failure_rolls_back_the_whole_multitree_statement_not_prior_work() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "items").await;
        create_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "CREATE UNIQUE INDEX uq ON items(value)",
            async |_, _| Ok(()),
        )
        .await
        .unwrap();
        // Prior private work that must survive the rejected statement.
        let kept_record = serialize_record(&[SqliteValue::Integer(42)]);
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "items",
            NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 1, &kept_record).await,
        )
        .await
        .unwrap();
        let key = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(1)]);
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "uq",
            NativeSchemaKind::Index,
            async |c| c.index_insert_unique(&cx, &key, 1, "items.value").await,
        )
        .await
        .unwrap();
        let wrote_table = AtomicBool::new(false);
        let result = with_native_schema_statement(&cx, &db, &mut txn, async |txn| {
            with_native_schema_tree(&cx, &db, txn, "items", NativeSchemaKind::Table, async |c| {
                c.table_insert(&cx, 2, &kept_record).await
            })
            .await?;
            wrote_table.store(true, Ordering::SeqCst);
            let duplicate = serialize_record(&[SqliteValue::Integer(42), SqliteValue::Integer(2)]);
            with_native_schema_tree(&cx, &db, txn, "uq", NativeSchemaKind::Index, async |c| {
                c.index_insert_unique(&cx, &duplicate, 1, "items.value")
                    .await
            })
            .await
        })
        .await;
        assert!(wrote_table.load(Ordering::SeqCst));
        assert!(matches!(result, Err(FrankenError::UniqueViolation { .. })));
        assert_eq!(
            named_rows(&cx, &db, &mut txn, "items").await,
            vec![(1, kept_record.clone())]
        );
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(
            named_rows(&cx, &recovered, &mut txn, "items").await,
            vec![(1, kept_record)]
        );
        let keys = with_native_schema_tree(
            &cx,
            &recovered,
            &mut txn,
            "uq",
            NativeSchemaKind::Index,
            async |c| {
                assert!(c.first(&cx).await?);
                let key = c.payload(&cx).await?;
                assert!(!c.next(&cx).await?);
                Ok(key)
            },
        )
        .await
        .unwrap();
        assert_eq!(keys, key);
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn failed_or_dropped_schema_builders_restore_catalog_roots_cookie_and_prior_rows() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        table(&cx, &db, &mut txn, "keep").await;
        with_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "keep",
            NativeSchemaKind::Table,
            async |c| c.table_insert(&cx, 1, b"prior work").await,
        )
        .await
        .unwrap();
        let before = read_native_schema(&cx, &db, &mut txn).await.unwrap();
        for abandon in [false, true] {
            let allocated = AtomicU32::new(0);
            let mutated = AtomicBool::new(false);
            let mut operation = Box::pin(create_native_schema_tree(
                &cx,
                &db,
                &mut txn,
                "CREATE TABLE rejected(value TEXT)",
                async |txn, entry| {
                    allocated.store(entry.root().get(), Ordering::SeqCst);
                    with_native_btree(&cx, &db, txn, entry.root(), true, async |c| {
                        c.table_insert(&cx, 99, &vec![0xEE; 3000]).await
                    })
                    .await?;
                    mutated.store(true, Ordering::SeqCst);
                    if abandon {
                        std::future::pending::<()>().await;
                    }
                    Err(FrankenError::CheckViolation {
                        name: "original builder error".to_owned(),
                    })
                },
            ));
            if abandon {
                let mut context = Context::from_waker(Waker::noop());
                assert!(matches!(
                    operation.as_mut().poll(&mut context),
                    Poll::Pending
                ));
            } else {
                assert!(
                    matches!(operation.as_mut().await, Err(FrankenError::CheckViolation { name }) if name == "original builder error")
                );
            }
            assert!(
                mutated.load(Ordering::SeqCst),
                "failure must follow actual root/overflow mutations"
            );
            drop(operation);
            assert_eq!(
                read_native_schema(&cx, &db, &mut txn).await.unwrap(),
                before
            );
            assert!(
                db.read_page(&cx, &mut txn, page(allocated.load(Ordering::SeqCst)))
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                named_rows(&cx, &db, &mut txn, "keep").await,
                vec![(1, b"prior work".to_vec())]
            );
        }
        db.commit(&cx, &mut txn, 100).await.unwrap();
        db.close(&cx).unwrap();
        let mut recovered = reopen(&vfs, &cx).await;
        let mut txn = recovered.begin(&cx).unwrap();
        assert_eq!(
            read_native_schema(&cx, &recovered, &mut txn).await.unwrap(),
            before
        );
        recovered.rollback(&mut txn).unwrap();
        recovered.close(&cx).unwrap();
    });
}

#[test]
fn drop_index_keeps_its_table_and_cookie_exhaustion_restores_partial_removal() {
    run(async {
        let cx = Cx::new();
        let vfs = MemoryVfs::new();
        let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        initialize_native_schema(&cx, &db, &mut txn).unwrap();
        let original = table(&cx, &db, &mut txn, "items").await;
        create_native_schema_tree(
            &cx,
            &db,
            &mut txn,
            "CREATE INDEX ix ON items(value)",
            async |_, _| Ok(()),
        )
        .await
        .unwrap();
        assert_eq!(
            drop_native_schema_tree(&cx, &db, &mut txn, "ix", NativeSchemaKind::Index)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            db.read_page(&cx, &mut txn, original.root())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            read_native_schema(&cx, &db, &mut txn)
                .await
                .unwrap()
                .entries()
                .len(),
            1
        );
        let mut p1 = db
            .read_page(&cx, &mut txn, page(1))
            .unwrap()
            .unwrap()
            .to_vec();
        p1[40..44].copy_from_slice(&u32::MAX.to_be_bytes());
        db.write_page(&cx, &mut txn, page(1), Some(&p1)).unwrap();
        let before = read_native_schema(&cx, &db, &mut txn).await.unwrap();
        assert!(matches!(
            drop_native_schema_tree(&cx, &db, &mut txn, "items", NativeSchemaKind::Table).await,
            Err(FrankenError::DatabaseFull)
        ));
        assert_eq!(
            read_native_schema(&cx, &db, &mut txn).await.unwrap(),
            before
        );
        assert_eq!(
            db.read_page(&cx, &mut txn, page(1))
                .unwrap()
                .unwrap()
                .as_ref(),
            p1
        );
        assert!(
            db.read_page(&cx, &mut txn, original.root())
                .unwrap()
                .is_some()
        );
        db.rollback(&mut txn).unwrap();
        db.close(&cx).unwrap();
    });
}

#[test]
fn schema_retirement_recovers_atomically_across_both_sync_failure_boundaries() {
    use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
    run(async {
        for (path, advance, was_committed) in [("objects", 1, false), ("markers", 2, true)] {
            let cx = Cx::new();
            let vfs = FaultInjectingVfs::new(MemoryVfs::new());
            let log = NativeDurabilityLog::create(
                &cx,
                open(&vfs, &cx, "objects"),
                open(&vfs, &cx, "markers"),
                NativeDurabilityLimits::default(),
            )
            .unwrap();
            let mut db =
                NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap();
            let mut txn = db.begin(&cx).unwrap();
            initialize_native_schema(&cx, &db, &mut txn).unwrap();
            let object = table(&cx, &db, &mut txn, "items").await;
            db.commit(&cx, &mut txn, 100).await.unwrap();
            let mut txn = db.begin(&cx).unwrap();
            drop_native_schema_tree(&cx, &db, &mut txn, "items", NativeSchemaKind::Table)
                .await
                .unwrap();
            vfs.inject_fault(
                FaultSpec::power_cut(path)
                    .after_nth_sync(vfs.sync_count() + advance)
                    .build(),
            );
            assert!(db.commit(&cx, &mut txn, 101).await.is_err());
            assert!(db.needs_recovery());
            assert!(
                db.rollback(&mut txn).is_err(),
                "an uncertain schema commit is not rolled back"
            );
            vfs.power_on();
            db.close(&cx).unwrap();
            let (mut recovered, _) = NativePageStore::recover(
                &cx,
                open(&vfs, &cx, "objects"),
                open(&vfs, &cx, "markers"),
                TestCodec,
                512,
                NativeDurabilityLimits::default(),
                NativePageLimits::default(),
            )
            .await
            .unwrap();
            let mut txn = recovered.begin(&cx).unwrap();
            let schema = read_native_schema(&cx, &recovered, &mut txn).await.unwrap();
            assert_eq!(schema.find("items").is_none(), was_committed);
            assert_eq!(schema.cookie(), if was_committed { 2 } else { 1 });
            assert_eq!(
                recovered
                    .read_page(&cx, &mut txn, object.root())
                    .unwrap()
                    .is_none(),
                was_committed
            );
            recovered.rollback(&mut txn).unwrap();
            recovered.close(&cx).unwrap();
        }
    });
}
