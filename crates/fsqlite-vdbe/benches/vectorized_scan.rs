use std::cell::RefCell;
use std::rc::Rc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_btree::{BtCursor, BtreeCursorOps, MemPageStore, PageReader, PageWriter};
use fsqlite_types::record::serialize_record;
use fsqlite_types::value::SqliteValue;
use fsqlite_types::{Cx, PageNumber};
use fsqlite_vdbe::vectorized::{ColumnSpec, ColumnVectorType, DEFAULT_BATCH_ROW_CAPACITY};
use fsqlite_vdbe::vectorized_scan::VectorizedTableScan;

const PAGE_SIZE: u32 = 4096;

#[derive(Clone, Debug)]
struct SharedMemPageIo {
    store: Rc<RefCell<MemPageStore>>,
}

impl SharedMemPageIo {
    fn new(page_size: u32, root_page: PageNumber) -> Self {
        Self {
            store: Rc::new(RefCell::new(MemPageStore::with_empty_table(
                root_page, page_size,
            ))),
        }
    }
}

impl PageReader for SharedMemPageIo {
    fn read_page(&self, cx: &Cx, page_no: PageNumber) -> fsqlite_error::Result<Vec<u8>> {
        self.store.borrow().read_page(cx, page_no)
    }
}

impl PageWriter for SharedMemPageIo {
    fn write_page(
        &mut self,
        cx: &Cx,
        page_no: PageNumber,
        data: &[u8],
    ) -> fsqlite_error::Result<()> {
        self.store.borrow_mut().write_page(cx, page_no, data)
    }

    fn allocate_page(&mut self, cx: &Cx) -> fsqlite_error::Result<PageNumber> {
        self.store.borrow_mut().allocate_page(cx)
    }

    fn free_page(&mut self, cx: &Cx, page_no: PageNumber) -> fsqlite_error::Result<()> {
        self.store.borrow_mut().free_page(cx, page_no)
    }
}

#[derive(Clone, Debug)]
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
        SqliteValue::Text(format!("bench-row-{rowid:06}")),
        SqliteValue::Blob(vec![
            u8::try_from(rowid.rem_euclid(251)).expect("mod value should fit into u8"),
            u8::try_from((rowid * 3).rem_euclid(251)).expect("mod value should fit into u8"),
            u8::try_from((rowid * 11).rem_euclid(251)).expect("mod value should fit into u8"),
            u8::try_from((rowid * 19).rem_euclid(251)).expect("mod value should fit into u8"),
        ]),
    ]
}

fn build_fixture(row_count: usize) -> ScanFixture {
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
            .expect("table_insert should succeed");
    }

    ScanFixture {
        io,
        root_page,
        specs: specs(),
        payload_bytes,
    }
}

fn bench_vectorized_scan_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("vectorized_scan_throughput");

    for row_count in [4_096_usize, 16_384_usize] {
        let fixture = build_fixture(row_count);
        let bytes = u64::try_from(fixture.payload_bytes).unwrap_or(u64::MAX);
        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &fixture,
            |b, fixture| {
                b.iter(|| {
                    let cursor =
                        BtCursor::new(fixture.io.clone(), fixture.root_page, PAGE_SIZE, true);
                    let mut scan = VectorizedTableScan::try_new(
                        cursor,
                        fixture.specs.clone(),
                        DEFAULT_BATCH_ROW_CAPACITY,
                    )
                    .expect("scan should initialize");

                    let mut scanned_rows = 0usize;
                    while let Some(batch) = scan.next_batch().expect("scan should succeed") {
                        scanned_rows = scanned_rows.saturating_add(batch.stats.rows_scanned);
                    }

                    criterion::black_box(scanned_rows);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_vectorized_scan_throughput);
criterion_main!(benches);
