# bd-w4pr1.5: release-by-release regression map (2026-11-10)

Question from the owner: did FrankenSQLite get slower relative to stock SQLite
over its releases, and if so, which release made each workload slower?

## Method

- Shipped Linux CLI of each release (`gh release download`, SHA-256 checked)
  against the system `sqlite3` (3.46.1) on the same host, in the same
  invocation, on tmpfs.
- Each workload ran twice per binary. The cell is the median time divided by
  stock's median time.
- `!` means the answer differed from stock, `T/O` means a timeout, and `ERR`
  means the release could not run the workload.
- Workloads: `scripts/user_shape_scorecard.py` (1M-row file-backed tables
  unless named otherwise).
- Raw results: [`bd-w4pr1.5.json`](bd-w4pr1.5.json).
- Releases covered: v0.1.17, v0.3.0, v0.3.8, v0.3.14, v0.4.4, v0.4.9, v0.4.12
  and v0.4.13. v0.1.14 was dropped after it grew to 13-16 GB on the 1M-row load.
  The other releases were not run.

| workload | stock s | v0.1.17 | v0.3.0 | v0.3.8 | v0.3.14 | v0.4.4 | v0.4.9 | v0.4.12 | v0.4.13 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| bulk_insert_1M_one_txn | 0.428 | T/O | T/O | ERR | T/O | T/O | 7.2x | 7.2x | 11.1x |
| scan_filter_agg | 0.056 | - | - | - | - | - | 7.2x | 7.4x | 6.9x |
| group_by_1000_groups | 0.206 | - | - | - | - | - | 9.4x | 8.8x | 8.3x |
| rowid_lookups_200k | 0.375 | - | - | - | - | - | 9.2x | 15.5x | 11.6x |
| join_1M_to_pk_1000 | 0.198 | - | - | - | - | - | 14.6x | 13.8x | 13.4x |
| order_by_text_limit10 | 0.071 | - | - | - | - | - | 7.8x | 6.6x | 6.7x |
| window_row_number | 0.728 | - | - | - | - | - | 10.5x | 10.8x | 10.1x |
| join_no_index_5k_x_5k | 0.006 | - | - | - | - | - | 956x | 924x | 901x |
| count_star | 0.016 | - | - | - | - | - | 8.4x | 6.0x | 5.2x |
| create_index_int_300k | 0.086 | 284x | 180x | ERR | 328x | T/O | 263x | 87x | 24x |
| update_10pct_300k | 0.047 | 22x | 48x | ERR | 44x | T/O | 123x | 9.6x | 9.2x |
| delete_14pct_300k | 0.033 | 26x | 47x | ERR | 58x | T/O | 177x | 12.6x | 11.8x |
| autocommit_5k_inserts | 0.065 | 140x | 699x | ERR | 278x | T/O | 27x | 27x | 24x |
| txn_1000x10_rows | 0.035 | 37x | 209x | ERR | 116x | T/O | 27x | 27x | 26.5x |
| memory_script_200k | 0.181 | 81x | 113x | 110x | 121x | 108x | 12x | 9.9x | 10.7x |
| multiproc_8x500 | 0.168 | 3.1x! | T/O | ERR | T/O | ERR | 2.0x! | 2.7x! | 9.9x |

`-` marks a cell where the release did not get that far. The 1M-row load timed
out (over 150 s), so the workloads that read that table never ran. v0.3.8's
Linux artifact could not open a file-backed database at all ("file-backed pager
not available on this platform").

## Findings

1. **User-shaped workloads did not get worse over time.** Older releases were
   far slower or broken:
   - The 1M-row bulk load timed out on every release before v0.4.9.
   - Writes ran at 22-700x stock and CREATE INDEX at 180-330x.
   - The multi-process run lost rows (`!`) up to v0.4.12.

   v0.4.13 is the best or near-best release on 14 of the 16 rows. The two
   exceptions are the windows in finding 2. The 900x unindexed join is fixed
   after v0.4.13 (bd-w4pr1.10).
2. **Two windows jumped by more than 1.5x.** These are the B3 bisects
   (bd-w4pr1.6):
   - `bulk_insert_1M_one_txn`: 7.2x on v0.4.12, then 11.1x on v0.4.13
     (+54%). mmap+munmap calls grew from 21k to 31k in the same window.
     Suspect: allocation churn in the 32-row insert replay under musl's
     allocator.
   - `rowid_lookups_200k`: 9.2x on v0.4.9, then 15.5x on v0.4.12, then
     11.6x on v0.4.13.
3. **multiproc_8x500 is not a regression.** v0.4.12's 2.7x lost committed rows;
   v0.4.13 is correct at 9.9x.
4. **The owner's recollection that it "was much faster" matches small
   in-memory statements, not these shapes.** The July comprehensive-bench point
   (b612eb7b) had lower small-statement overhead than now: `read_single` and
   `read_aggregate` per-statement times grew 3.5x and 2.6x since then. One
   cause found so far: the expression parser's `ParseMachine` landed
   2026-07-31. It copies about 15 KB of empty stack frames per parsed
   expression (bd-w4pr1.19). The per-statement file-identity and refresh
   syscalls are another (bd-ih8ak).

## Confound

- v0.1.14 and v0.3.4 Linux assets are dynamically linked glibc builds. The
  others are static musl builds, whose allocator maps and unmaps memory per
  statement.
- A ratio jump between a glibc release and a musl release can therefore come
  from the allocator rather than the engine.
- Attributing an engine regression (B3) needs same-target builds of the
  window's endpoints.
