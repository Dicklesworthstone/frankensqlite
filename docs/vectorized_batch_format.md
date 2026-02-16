# Vectorized Batch Format (`bd-14vp7.1`)

This document defines the foundational data-exchange unit for vectorized VDBE execution.

## Goals

- Fixed-size batches (default `1024` rows) for predictable cache behavior.
- Columnar layout for SIMD-friendly scans and expression evaluation.
- Null tracking via compact validity bitmaps (`1 bit / row / column`).
- Selection vectors for active-row filtering without data copies.
- Arrow-compatible buffer contracts for zero-copy interchange.

## Core Types

Implemented in `crates/fsqlite-vdbe/src/vectorized.rs`:

- `Batch`
- `Column`
- `ColumnData`
- `SelectionVector`
- `NullBitmap`
- `ArrowCompatibleBatch`

### Supported column payloads

- Signed integers: `i8`, `i16`, `i32`, `i64`
- Floating point: `f32`, `f64`
- Variable-width binary: `offsets + data`
- Variable-width text: UTF-8 `offsets + data`

## Memory Layout Contract

### Fixed-width columns

Fixed-width columns are stored in `AlignedValues<T>`:

- underlying owner: `Arc<[u8]>`
- typed region starts at an alignment-adjusted byte offset
- alignment target defaults to `32` bytes (`DEFAULT_SIMD_ALIGNMENT_BYTES`)

This allows the format to verify SIMD-oriented alignment requirements (`Batch::verify_alignment`).

### Variable-width columns

Binary/text columns follow Arrow-style split buffers:

- `offsets: Arc<[u32]>` with length `row_count + 1`
- `data: Arc<[u8]>`

The row `i` payload spans `data[offsets[i]..offsets[i + 1]]`.

### Validity bitmap

`NullBitmap` packs validity bits:

- bit `1`: value present
- bit `0`: NULL

## Row-to-Batch Construction

`Batch::from_rows(rows, specs, capacity)` converts row-oriented values (`Vec<Vec<SqliteValue>>`) into columnar buffers.

Rules:

- row width must match schema width
- row count must not exceed batch capacity
- integer downcasts (`i64 -> i8/i16/i32`) are range-checked
- `NULL` values set validity bit to `0` and insert type-appropriate sentinels in data buffers

## Arrow-Compatible Zero-Copy Conversion

`Batch::into_arrow_compatible()` exports an `ArrowCompatibleBatch` without copying column buffers.
`Batch::from_arrow_compatible(...)` reconstructs a batch by reusing the same shared storage.

The interchange shape follows Arrow buffer semantics:

- fixed-width: values + validity
- variable-width: offsets + data + validity

## Selection Vector

`SelectionVector` stores active row indices as `u16` values.

- identity vector (`0..row_count-1`) is produced by default
- compatible with filter pushdown and branchless operator pipelines

## Benchmark

`crates/fsqlite-vdbe/benches/vectorized_batch.rs` measures batch-construction throughput for row counts:

- 64
- 256
- 1024

This benchmark is intended to catch regressions in row-to-column conversion overhead before scan/filter/join operator work begins.

## Vectorized Scan Source (`bd-14vp7.2`)

Implemented in `crates/fsqlite-vdbe/src/vectorized_scan.rs`.

The scan source reads rows from a B-tree cursor and emits `Batch` values:

- sequential leaf-page traversal through `BtCursor`
- early filter pushdown by writing active row indexes into the batch `SelectionVector`
- morsel boundaries as contiguous page ranges (`PageMorsel { start_page, end_page }`)
- explicit best-effort prefetch hints for upcoming pages within the morsel

`ScanBatchStats` provides per-batch observability:

- `rows_scanned`
- `rows_selected`
- `pages_touched`
- `prefetch_hints_issued`

Correctness is validated against row-at-a-time scans, and scan throughput is
tracked by `crates/fsqlite-vdbe/benches/vectorized_scan.rs`.

## Morsel Dispatcher (`bd-14vp7.6`, initial slice)

Implemented in `crates/fsqlite-vdbe/src/vectorized_dispatch.rs`.

Current scope:

- Page-range morsel partitioning: `partition_page_morsels(start, end, pages_per_morsel, numa_nodes)`
- L2-aware auto-tuning helper:
  - `auto_tuned_pages_per_morsel(l2_cache_bytes, page_size_bytes)`
  - `partition_page_morsels_auto_tuned(...)`
- Pipeline task model:
  - `PipelineId`
  - `PipelineKind`
  - `PipelineTask`
  - `build_pipeline_tasks(...)`
- Work-stealing execution using `crossbeam-deque`:
  - per-worker local queues
  - peer stealing when local queue empties
  - NUMA locality hints via `preferred_numa_node` assignment
- Pipeline barriers:
  - `execute_with_barriers(...)` runs pipeline wave `i` to completion before wave `i+1`
- Structured observability:
  - per-morsel span: `morsel_exec` with fields `morsel_size`, `worker_id`, `pipeline_id`, `task_id`
  - scheduling logs at `DEBUG` (`morsel.schedule`, `morsel.execute.start`, `morsel.execute.complete`)
  - pipeline completion logs at `INFO` (`morsel.pipeline.complete`)
  - gauges:
    - `fsqlite_morsel_throughput_rows_per_sec` (task-throughput proxy until row counters are threaded through operators)
    - `fsqlite_morsel_workers_active`

Validation:

- Deterministic unit tests for:
  - partition coverage/no gaps
  - barrier ordering
  - multi-worker utilization
- Micro-benchmark scaffold:
  - `crates/fsqlite-vdbe/benches/vectorized_dispatch.rs`
  - scaling harness for worker counts `1, 2, 4, 8`
