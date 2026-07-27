# vfs-tax decomposition — bd-trfah / bd-dqdoe mechanism evidence

**Date:** 2026-07-27T02:57–03:24Z · **Host:** csd (64 cores, loadavg 8–21 —
NOT citable for absolute latency; paired same-binary interleaved arms, so the
relative decomposition is attribution-grade) · **Agent:** GreenBirch ·
**Run:** `taskset -c 56-63 vfs-tax`.

## Method

15 rounds, arm order rotated per round, fixed-seed LCG page offsets identical
across arms, 8 MiB file (2048 × 4096B pages, page-cache warm), counting global
allocator for exact allocs/op. The shipped arms drive the real
`UnixVfs`/`UnixFile` with a `Cx` that has **no attached native cx** — the
exact production gate state measured by bd-fo6xw (7520 unix fallbacks, 0
io_uring samples), so this IS the production data path. Harness source:
`harness-main.rs` / `harness-Cargo.toml` (final version, 9 arms).

## Files

| file | crate state | what it shows |
|---|---|---|
| `vfs-tax.json` | pre-change HEAD 648460d3 | baseline decomposition (binary sha256 `fc9120e1…`) |
| `vfs-tax-postfix.json` | inline prototype (withdrawn) | inline fast path collapses the tax (sha256 `95c70697…`) |
| `vfs-tax-final-with-batch.json` | landed batch-hop change | shipped paths + `write/batch16` arm |

## Results (medians of 15 rounds)

Pre-change baseline (`vfs-tax.json`):

| arm | med ns/op | allocs/op | meaning |
|---|---:|---:|---|
| read/shipped | 15,594 | 5.04 | spawn_blocking_io + vec![0;ps] + pread + copy |
| read/inline | 1,164 | 0.01 | counterfactual: same pread loop, inline |
| hop/empty | 14,049 | 4.04 | spawn_blocking_io round-trip, trivial closure |
| alloc+copy | 147 | 1.00 | vec![0;4096] + copy_from_slice only |
| write/shipped | 18,881 | 6.04 | to_vec + hop + pwrite |
| write/inline | 1,676 | 0.01 | counterfactual inline pwrite |
| rt/fresh | 5,416,534 | 116 | fresh RuntimeBuilder + block_on + drop per op |
| rt/reenter | 754 | 11 | block_on(async {}) on persistent runtime |

**Model check:** (shipped − inline) = 14,430 ns/op ≈ (hop + alloc/copy) =
14,196 ns/op → ~90% of the tax is the blocking-pool round-trip, NOT the
allocation/copies. Read tax 13.4x, write tax 11.3x.

Final run with the landed change (`vfs-tax-final-with-batch.json`):

| arm | med ns/op | meaning |
|---|---:|---|
| write/shipped | 14,313 | per-page pooled default (unchanged single-op path) |
| **write/batch16** | **2,893** | **landed `write_page_batch`: ONE pool hop per 16-page group → 4.9x** |
| write/inline | 1,516 | deferred actor-lane ceiling |

## Disposition

- **Landed:** `UnixFile::write_page_batch` override — stage the batch, one
  `spawn_blocking_io` hop pwrites every page. Contract-safe (still pooled).
- **Withdrawn:** unconditional inline `pread`/`pwrite` ≤64 KiB
  (`vfs-tax-postfix.json` documents its effect: shipped read 1,232 ns/op,
  allocs 5.04 → 0.01). Rejected by async-contract review (RusticBasin,
  bd-trfah thread 2026-07-27): raw async `Connection` runs engine futures on
  shared scheduler workers; page I/O may block arbitrarily under
  reclaim/device stalls. Retry only under the actor-lane
  `blocking_io_inline_safe` Cx-marker design or with a scheduler-latency gate
  under induced slow I/O.
- **bd-dqdoe input:** persistent-runtime `block_on` re-entry is 754 ns/op
  (bd-zavyn instrument-drift scale); fresh-runtime-per-op would be 5.4 ms/op
  (ruled out as production behavior); the per-page pool hop on 100%-fallback
  I/O was the dominant engine-side per-I/O mechanism.
