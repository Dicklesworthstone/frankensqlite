//! Cell-parsing microbench for the B-tree leaf hot path.
//!
//! `CellRef::parse` and the lightweight `read_table_leaf_rowid_at_offset` /
//! `cell_on_page_size_fast` helpers run once per cell on every leaf scan,
//! seek, and defragmentation pass, so their per-cell cost is multiplied across
//! whole-page traversals. This bench builds synthetic, no-overflow table-leaf
//! and index-leaf pages and measures per-cell parse cost.
//!
//! Like the planner benches in this workspace it is a plain `harness = false`
//! binary that prints deterministic `*_ns_per_op` lines (no criterion), so
//! before/after deltas are read directly from stdout. Run with:
//!
//! ```text
//! CARGO_TARGET_DIR=/data/tmp/cc3-target \
//!   cargo bench -p fsqlite-btree --bench cell_parse_hot_paths
//! ```

use std::env;
use std::hint::black_box;
use std::time::Instant;

use fsqlite_btree::cell::{
    BtreePageType, CellRef, cell_on_page_size_fast, read_table_leaf_rowid_at_offset,
};
use fsqlite_types::serial_type::write_varint;

const USABLE_SIZE: u32 = 4096;
const PAGE_SIZE: usize = 4096;
/// Typical small record payload that stays entirely on-page (no overflow) for
/// both table-leaf and index-leaf max-local thresholds at a 4 KiB page.
const PAYLOAD_BYTES: usize = 16;
const DEFAULT_ITERATIONS: u64 = 2_000_000;
/// Header-area bytes skipped before the first cell. The exact value is
/// irrelevant to per-cell parse cost; it only keeps cells off the page start.
const HEADER_PAD: usize = 12;

/// Build a table-leaf page populated with `payload_size, rowid, payload` cells.
/// Returns the page bytes and the offset of each cell.
fn build_table_leaf_page() -> (Vec<u8>, Vec<usize>) {
    let mut page = vec![0u8; PAGE_SIZE];
    let mut offsets = Vec::new();
    let mut pos = HEADER_PAD;
    let mut rowid: u64 = 1;
    while pos + 10 + PAYLOAD_BYTES <= USABLE_SIZE as usize {
        offsets.push(pos);
        pos += write_varint(&mut page[pos..], PAYLOAD_BYTES as u64);
        // Mix 1-byte and multi-byte rowid varints to exercise the decoder.
        pos += write_varint(&mut page[pos..], rowid.wrapping_mul(2_113));
        pos += PAYLOAD_BYTES;
        rowid += 1;
    }
    (page, offsets)
}

/// Build a leaf-index page populated with `payload_size, payload` cells (index
/// leaf cells carry no rowid; the key lives in the payload).
fn build_index_leaf_page() -> (Vec<u8>, Vec<usize>) {
    let mut page = vec![0u8; PAGE_SIZE];
    let mut offsets = Vec::new();
    let mut pos = HEADER_PAD;
    while pos + 10 + PAYLOAD_BYTES <= USABLE_SIZE as usize {
        offsets.push(pos);
        pos += write_varint(&mut page[pos..], PAYLOAD_BYTES as u64);
        pos += PAYLOAD_BYTES;
    }
    (page, offsets)
}

#[allow(clippy::cast_precision_loss)]
fn ns_per_op(elapsed_ns: f64, ops: u64) -> f64 {
    elapsed_ns / ops as f64
}

fn bench_cellref_parse(
    page: &[u8],
    offsets: &[usize],
    page_type: BtreePageType,
    iterations: u64,
) -> f64 {
    let n = offsets.len();
    let mut acc: u64 = 0;
    let start = Instant::now();
    for i in 0..iterations {
        let off = offsets[(i as usize) % n];
        let cell = CellRef::parse(black_box(page), black_box(off), page_type, USABLE_SIZE)
            .expect("synthetic cell parses");
        acc = acc
            .wrapping_add(cell.payload_offset as u64)
            .wrapping_add(u64::from(cell.local_size));
    }
    black_box(acc);
    ns_per_op(start.elapsed().as_secs_f64() * 1_000_000_000.0, iterations)
}

fn bench_read_rowid(page: &[u8], offsets: &[usize], iterations: u64) -> f64 {
    let n = offsets.len();
    let mut acc: i64 = 0;
    let start = Instant::now();
    for i in 0..iterations {
        let off = offsets[(i as usize) % n];
        let rowid = read_table_leaf_rowid_at_offset(black_box(page), black_box(off))
            .expect("synthetic table-leaf cell has rowid");
        acc = acc.wrapping_add(rowid);
    }
    black_box(acc);
    ns_per_op(start.elapsed().as_secs_f64() * 1_000_000_000.0, iterations)
}

fn bench_on_page_size(
    page: &[u8],
    offsets: &[usize],
    page_type: BtreePageType,
    iterations: u64,
) -> f64 {
    let n = offsets.len();
    let mut acc: usize = 0;
    let start = Instant::now();
    for i in 0..iterations {
        let off = offsets[(i as usize) % n];
        let size = cell_on_page_size_fast(black_box(page), black_box(off), page_type, USABLE_SIZE)
            .expect("synthetic cell has a valid on-page size");
        acc = acc.wrapping_add(size);
    }
    black_box(acc);
    ns_per_op(start.elapsed().as_secs_f64() * 1_000_000_000.0, iterations)
}

fn parse_iterations() -> u64 {
    let mut args = env::args().skip(1);
    let mut iterations = DEFAULT_ITERATIONS;
    while let Some(arg) = args.next() {
        if arg == "--iterations"
            && let Some(value) = args.next()
        {
            match value.parse() {
                Ok(parsed) => iterations = parsed,
                Err(_) => {
                    eprintln!("invalid --iterations value: {value}");
                    std::process::exit(2);
                }
            }
        }
    }
    iterations
}

fn main() {
    let iterations = parse_iterations();
    let (table_page, table_offsets) = build_table_leaf_page();
    let (index_page, index_offsets) = build_index_leaf_page();

    let table_parse = bench_cellref_parse(
        &table_page,
        &table_offsets,
        BtreePageType::LeafTable,
        iterations,
    );
    let index_parse = bench_cellref_parse(
        &index_page,
        &index_offsets,
        BtreePageType::LeafIndex,
        iterations,
    );
    let rowid_read = bench_read_rowid(&table_page, &table_offsets, iterations);
    let table_on_page = bench_on_page_size(
        &table_page,
        &table_offsets,
        BtreePageType::LeafTable,
        iterations,
    );
    let index_on_page = bench_on_page_size(
        &index_page,
        &index_offsets,
        BtreePageType::LeafIndex,
        iterations,
    );

    println!(
        "cell_parse_hot_paths cellref_parse_table_leaf_ns_per_op={table_parse:.2} cells={} iterations={iterations}",
        table_offsets.len()
    );
    println!(
        "cell_parse_hot_paths cellref_parse_index_leaf_ns_per_op={index_parse:.2} cells={} iterations={iterations}",
        index_offsets.len()
    );
    println!(
        "cell_parse_hot_paths read_table_leaf_rowid_ns_per_op={rowid_read:.2} cells={} iterations={iterations}",
        table_offsets.len()
    );
    println!(
        "cell_parse_hot_paths cell_on_page_size_table_leaf_ns_per_op={table_on_page:.2} cells={} iterations={iterations}",
        table_offsets.len()
    );
    println!(
        "cell_parse_hot_paths cell_on_page_size_index_leaf_ns_per_op={index_on_page:.2} cells={} iterations={iterations}",
        index_offsets.len()
    );
}
