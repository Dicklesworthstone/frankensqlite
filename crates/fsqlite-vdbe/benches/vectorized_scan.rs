use std::cell::Cell;
use std::future::Future;
use std::hint::black_box;
use std::rc::Rc;

use asupersync::runtime::{Runtime, RuntimeBuilder};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_btree::{BtCursor, BtreeCursorOps, MemPageStore, PageReader, PageWriter};
use fsqlite_types::record::serialize_record;
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{Cx, PageNumber, WitnessKey};
use fsqlite_vdbe::vectorized::{ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY};
use fsqlite_vdbe::vectorized_scan::VectorizedTableScan;

const PAGE_SIZE: u32 = 4096;

#[derive(Clone)]
struct SharedMemPageIo {
    store: Rc<Cell<Option<MemPageStore>>>,
    stall_write_after_store_take: bool,
}

struct SharedMemPageStoreGuard {
    slot: Rc<Cell<Option<MemPageStore>>>,
    store: Option<MemPageStore>,
}

impl SharedMemPageStoreGuard {
    fn take(slot: Rc<Cell<Option<MemPageStore>>>) -> fsqlite_error::Result<Self> {
        let store = slot.take().ok_or_else(|| {
            fsqlite_error::FrankenError::internal("shared benchmark page store is already in use")
        })?;
        Ok(Self {
            slot,
            store: Some(store),
        })
    }

    fn store(&self) -> fsqlite_error::Result<&MemPageStore> {
        self.store.as_ref().ok_or_else(|| {
            fsqlite_error::FrankenError::internal(
                "shared benchmark page store guard lost ownership",
            )
        })
    }

    fn store_mut(&mut self) -> fsqlite_error::Result<&mut MemPageStore> {
        self.store.as_mut().ok_or_else(|| {
            fsqlite_error::FrankenError::internal(
                "shared benchmark page store guard lost ownership",
            )
        })
    }
}

impl Drop for SharedMemPageStoreGuard {
    fn drop(&mut self) {
        let Some(store) = self.store.take() else {
            return;
        };
        let current = self.slot.take();
        self.slot.set(current.or(Some(store)));
    }
}

impl SharedMemPageIo {
    fn new(page_size: u32, root_page: PageNumber) -> Self {
        Self {
            store: Rc::new(Cell::new(Some(MemPageStore::with_empty_table(
                root_page, page_size,
            )))),
            stall_write_after_store_take: false,
        }
    }
}

// This adapter deliberately uses `Rc` with a current-thread runtime so the
// scan benchmark does not measure cross-thread synchronization overhead.
#[allow(clippy::future_not_send, clippy::manual_async_fn)]
impl PageReader for SharedMemPageIo {
    fn read_page<'a>(
        &'a self,
        cx: &'a Cx,
        page_no: PageNumber,
    ) -> impl Future<Output = fsqlite_error::Result<Vec<u8>>> + 'a {
        async move {
            let store = SharedMemPageStoreGuard::take(Rc::clone(&self.store))?;
            store.store()?.read_page(cx, page_no).await
        }
    }
}

#[allow(clippy::future_not_send, clippy::manual_async_fn)]
impl PageWriter for SharedMemPageIo {
    fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
        data: &'a [u8],
    ) -> impl Future<Output = fsqlite_error::Result<()>> + 'a {
        let store = Rc::clone(&self.store);
        let stall_after_take = self.stall_write_after_store_take;
        async move {
            let mut store = SharedMemPageStoreGuard::take(store)?;
            if stall_after_take {
                std::future::pending::<()>().await;
            }
            store.store_mut()?.write_page(cx, page_no, data).await
        }
    }

    fn allocate_page<'a>(
        &'a mut self,
        cx: &'a Cx,
    ) -> impl Future<Output = fsqlite_error::Result<PageNumber>> + 'a {
        async move {
            let mut store = SharedMemPageStoreGuard::take(Rc::clone(&self.store))?;
            store.store_mut()?.allocate_page(cx).await
        }
    }

    fn free_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page_no: PageNumber,
    ) -> impl Future<Output = fsqlite_error::Result<()>> + 'a {
        async move {
            let mut store = SharedMemPageStoreGuard::take(Rc::clone(&self.store))?;
            store.store_mut()?.free_page(cx, page_no).await
        }
    }

    fn record_write_witness(&mut self, _cx: &Cx, _key: WitnessKey) {}
}

fn verify_shared_mem_page_io_cancellation_restore() {
    use std::task::{Context, Poll, Waker};

    let root_page = PageNumber::new(2).expect("root page should be non-zero");
    let mut io = SharedMemPageIo::new(PAGE_SIZE, root_page);
    io.stall_write_after_store_take = true;
    let store = Rc::clone(&io.store);
    let cx = Cx::new();
    let page = vec![0_u8; PAGE_SIZE as usize];
    let mut pending = Box::pin(PageWriter::write_page(&mut io, &cx, root_page, &page));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
    let in_flight_slot = store.take();
    assert!(
        in_flight_slot.is_none(),
        "polled write future should own the benchmark page store"
    );
    store.set(in_flight_slot);
    drop(pending);
    let restored_slot = store.take();
    assert!(
        restored_slot.is_some(),
        "dropping a pending write future must restore the benchmark page store"
    );
    store.set(restored_slot);
}

#[derive(Clone)]
struct ScanFixture {
    io: SharedMemPageIo,
    root_page: PageNumber,
    specs: Vec<ColumnSpec>,
    payload_bytes: usize,
}

fn specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec::new("id", ColumnVectorType::Int64),
        ColumnSpec::new("score", ColumnVectorType::Float64),
        ColumnSpec::new("name", ColumnVectorType::Text),
        ColumnSpec::new("payload", ColumnVectorType::Binary),
    ]
}

fn row_for_rowid(rowid: i64) -> Vec<SqliteValue> {
    vec![
        SqliteValue::Integer(rowid),
        SqliteValue::Float(rowid as f64 * 0.25),
        SqliteValue::Text(format!("bench-row-{rowid:06}").into()),
        SqliteValue::Blob(
            vec![
                u8::try_from(rowid.rem_euclid(251)).expect("mod value should fit into u8"),
                u8::try_from((rowid * 3).rem_euclid(251)).expect("mod value should fit into u8"),
                u8::try_from((rowid * 11).rem_euclid(251)).expect("mod value should fit into u8"),
                u8::try_from((rowid * 19).rem_euclid(251)).expect("mod value should fit into u8"),
            ]
            .into(),
        ),
    ]
}

fn build_fixture(runtime: &Runtime, row_count: usize) -> ScanFixture {
    runtime.block_on(async {
        let root_page = PageNumber::new(2).expect("root page should be non-zero");
        let io = SharedMemPageIo::new(PAGE_SIZE, root_page);
        let mut writer = BtCursor::new(io.clone(), root_page, PAGE_SIZE, true);
        let cx = Cx::new();
        let mut payload_bytes = 0usize;

        for idx in 0..row_count {
            let rowid = i64::try_from(idx + 1).expect("rowid should fit into i64");
            let row = row_for_rowid(rowid);
            let payload = serialize_record(&row);
            payload_bytes = payload_bytes.saturating_add(payload.len());
            writer
                .table_insert(&cx, rowid, &payload)
                .await
                .expect("table_insert should succeed");
        }

        ScanFixture {
            io,
            root_page,
            specs: specs(),
            payload_bytes,
        }
    })
}

fn bench_vectorized_scan_throughput(c: &mut Criterion) {
    verify_shared_mem_page_io_cancellation_restore();
    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 1)
        .build()
        .expect("vectorized-scan benchmark runtime should build");
    let mut group = c.benchmark_group("vectorized_scan_throughput");

    for row_count in [4_096_usize, 16_384_usize] {
        let fixture = build_fixture(&runtime, row_count);
        let bytes = u64::try_from(fixture.payload_bytes).unwrap_or(u64::MAX);
        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &fixture,
            |b, fixture| {
                b.iter(|| {
                    runtime.block_on(async {
                        let cx = Cx::new();
                        let cursor =
                            BtCursor::new(fixture.io.clone(), fixture.root_page, PAGE_SIZE, true);
                        let mut scan = VectorizedTableScan::try_new(
                            &cx,
                            cursor,
                            fixture.specs.clone(),
                            DEFAULT_BATCH_ROW_CAPACITY,
                        )
                        .expect("scan should initialize");

                        let mut scanned_rows = 0usize;
                        while let Some(batch) =
                            scan.next_batch().await.expect("scan should succeed")
                        {
                            scanned_rows = scanned_rows.saturating_add(batch.stats.rows_scanned);
                        }

                        black_box(scanned_rows);
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_vectorized_scan_throughput);
criterion_main!(benches);
