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
//!
//! GH#491 also measures the canonical varint writer against its previous
//! loop implementation in the SAME invocation. All nine widths, mixed widths,
//! negative rowids, and the million-row positive prefix are reported, with
//! alternating order and raw paired samples. This is a codec microbenchmark,
//! NOT a profile or end-to-end SQLite comparison. Use benchmark_issue_491.py
//! for the actual INSERT/SELECT and stock-SQLite comparison.

use std::env;
use std::hint::black_box;
use std::time::Instant;

use fsqlite_btree::cell::{
    BtreePageType, CellRef, cell_on_page_size_fast, read_table_leaf_rowid_at_offset,
};
use fsqlite_types::serial_type::{varint_len, write_varint};

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

// Frozen pre-bd70b47 writer: compare the algorithm, not an indirect function
// call or a new allocation. Both writers get the same inline opportunity.
#[inline]
fn previous_write_varint(buf: &mut [u8], value: u64) -> usize {
    let len = varint_len(value);
    if len == 1 {
        buf[0] = value as u8;
    } else if len == 9 {
        let mut v = value >> 8;
        for i in (0..8).rev() {
            buf[i] = (v as u8 & 0x7F) | 0x80;
            v >>= 7;
        }
        buf[8] = value as u8;
    } else {
        let mut v = value;
        for i in (0..len).rev() {
            if i == len - 1 {
                buf[i] = v as u8 & 0x7F;
            } else {
                buf[i] = (v as u8 & 0x7F) | 0x80;
            }
            v >>= 7;
        }
    }
    len
}

fn bench_varint_writer<F>(values: &[u64], iterations: u64, mut write: F) -> f64
where
    F: FnMut(&mut [u8], u64) -> usize,
{
    let mut buf = [0_u8; 9];
    let mut remaining = iterations;
    let start = Instant::now();
    while remaining != 0 {
        let count = remaining.min(values.len() as u64) as usize;
        for &value in &values[..count] {
            let written = write(black_box(&mut buf), black_box(value));
            black_box((&buf, written));
        }
        remaining -= count as u64;
    }
    ns_per_op(start.elapsed().as_secs_f64() * 1_000_000_000.0, iterations)
}

fn compare_varint_writers(label: &str, values: &[u64], iterations: u64) {
    assert!(!values.is_empty());
    // Validate outside the timer, including bytes beyond the encoded value.
    for &value in values {
        let mut previous = [0xCD; 9];
        let mut current = [0xCD; 9];
        let expected = previous_write_varint(&mut previous, value);
        let actual = write_varint(&mut current, value);
        assert_eq!(actual, expected, "{label}: length for {value}");
        assert_eq!(current, previous, "{label}: bytes for {value}");
    }
    let warmup = iterations.min(10_000);
    black_box(bench_varint_writer(values, warmup, previous_write_varint));
    black_box(bench_varint_writer(values, warmup, write_varint));
    let mut previous = [0.0_f64; 5];
    let mut current = [0.0_f64; 5];
    for sample in 0..previous.len() {
        if sample % 2 == 0 {
            previous[sample] = bench_varint_writer(values, iterations, previous_write_varint);
            current[sample] = bench_varint_writer(values, iterations, write_varint);
        } else {
            current[sample] = bench_varint_writer(values, iterations, write_varint);
            previous[sample] = bench_varint_writer(values, iterations, previous_write_varint);
        }
        assert!(previous[sample].is_finite() && previous[sample] > 0.0);
        assert!(current[sample].is_finite() && current[sample] > 0.0);
    }
    println!(
        "varint_write case={label} previous_ns_per_op_samples={previous:?} current_ns_per_op_samples={current:?} iterations={iterations}"
    );
    previous.sort_by(f64::total_cmp);
    current.sort_by(f64::total_cmp);
    println!(
        "varint_write case={label} previous_median_ns_per_op={:.3} current_median_ns_per_op={:.3} current_over_previous={:.6}",
        previous[2], current[2], current[2] / previous[2]
    );
}

fn bench_varint_write_paths(iterations: u64) {
    let mut by_width = Vec::new();
    for width in 1_u32..=9 {
        let minimum = if width == 1 {
            0
        } else {
            1_u64 << (7 * (width - 1))
        };
        let maximum = if width == 9 {
            u64::MAX
        } else {
            (1_u64 << (7 * width)) - 1
        };
        let span = maximum - minimum + 1;
        let mut values = vec![minimum, minimum + 1, maximum - 1, maximum];
        for i in 4_u64..256 {
            let mixed = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(23);
            values.push(minimum + mixed % span);
        }
        assert!(values.iter().all(|&value| varint_len(value) == width as usize));
        compare_varint_writers(&format!("width_{width}"), &values, iterations);
        by_width.push(values);
    }
    let mut mixed = Vec::with_capacity(9 * 256);
    for i in 0..256 {
        for values in &by_width {
            mixed.push(values[i]);
        }
    }
    compare_varint_writers("mixed_widths", &mixed, iterations);
    let negative_rowids = [i64::MIN, i64::MIN + 1, -16_384, -128, -2, -1].map(|v| v as u64);
    compare_varint_writers("negative_rowids", &negative_rowids, iterations);
    let positive_rowids = (1_u64..=1_000_000).collect::<Vec<_>>();
    compare_varint_writers("issue491_positive_rowids", &positive_rowids, iterations);
}

fn parse_iterations() -> u64 {
    let mut args = env::args().skip(1);
    let mut iterations = DEFAULT_ITERATIONS;
    while let Some(arg) = args.next() {
        if arg == "--iterations"
            && let Some(value) = args.next()
        {
            match value.parse() {
                Ok(parsed) if parsed > 0 => iterations = parsed,
                _ => {
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
    bench_varint_write_paths(iterations);
}
