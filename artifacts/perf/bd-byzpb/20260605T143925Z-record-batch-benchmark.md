# bd-byzpb record batch encoder benchmark

Date: 2026-06-05
Worker: `ts2`
Commit under test: local worktree after `5b26b579862141aa74de39cfca75b637e1b8f9b5`

## Command

```bash
timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-bd-byzpb-vdbe-bench-run cargo bench -p fsqlite-vdbe --bench make_record -- record_batch_encoding --warm-up-time 1 --measurement-time 3 --sample-size 10
```

## Scenario

The benchmark compares:

- `scalar_loop`: per-row `serialize_record` followed by concatenation.
- `encode_batch`: the existing two-pass batch encoder writing one contiguous output buffer.

Rows use a mixed six-column shape: integer, text, integer, float, blob, null.
The benchmark setup asserts that `encode_batch` output is byte-identical to the scalar concatenation before measurement.

## Results

| Rows | Scalar mean | `encode_batch` mean | Speedup |
| ---: | ---: | ---: | ---: |
| 16 | 4.0084 us | 3.6639 us | 1.094x |
| 128 | 31.921 us | 28.779 us | 1.109x |
| 1024 | 254.43 us | 230.97 us | 1.101x |

Criterion reported 10 samples per cell. The 128-row scalar/encode cells and the 1024-row scalar/encode cells reported mild outliers, so the result should be treated as a short focused screen rather than a release-grade benchmark matrix.

## Interpretation

This validates a real same-window improvement for the mixed batch-record encoder, but it does not satisfy the bead's `>= 20% faster` acceptance gate. `bd-byzpb` should remain open/in progress until a stronger benchmark or a narrower accepted benchmark contract is available.

No heaptrack allocation artifact was captured for this screen because the primary speed gate did not pass.
