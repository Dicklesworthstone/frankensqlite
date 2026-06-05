# bd-byzpb record batch encoder fast path

Date: 2026-06-05
Worker: `ts2`
Commit under test: local worktree after `991bde1a3a68322c8bcfc405871d6aee23751ec2`

## Scenario

This run measures the production `encode_batch` path against scalar per-row
`serialize_record` plus concatenation for fixed-layout mixed-column rows:

- integer, text, integer, float, blob, null
- text width fixed at 9 bytes (`name_0000` style)
- blob width fixed at 4 bytes
- integer values forced to serial type 6 by using a large base value

The benchmark setup asserts that `encode_batch` output is byte-identical to
the scalar concatenation before measurement.

## Benchmark Command

```bash
timeout 1800 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-bd-byzpb-bench cargo bench -p fsqlite-vdbe --bench make_record -- record_batch_encoding
```

## Benchmark Results

| Rows | Scalar mean | `encode_batch` mean | Speedup | Faster by |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 3.9505 us | 2.2567 us | 1.751x | 42.9% |
| 128 | 31.554 us | 16.609 us | 1.900x | 47.4% |
| 1024 | 255.64 us | 132.88 us | 1.924x | 48.0% |

All measured row counts clear the bead's >=20% faster gate for multi-row
INSERT record encoding.

## Heaptrack Command

```bash
timeout 1800 rch exec -- bash -lc 'set -euo pipefail
cd /data/projects/frankensqlite
json=/tmp/bd-byzpb-fsqlite-types-test2.json
CARGO_TARGET_DIR=/data/tmp/frankensqlite-bd-byzpb-heaptrack cargo test -p fsqlite-types record_batch_mixed_columns_preserves_offsets_and_reuses_buffers --no-run --message-format=json > "$json"
bin=$(jq -r "select(.executable != null) | .executable" "$json" | tail -n 1)
heaptrack --record-only -o /tmp/bd-byzpb-heaptrack2 "$bin" record::tests::record_batch_mixed_columns_preserves_offsets_and_reuses_buffers --exact --nocapture
heaptrack_print --file /tmp/bd-byzpb-heaptrack2.zst --filter-bt-function encode_batch_homogeneous_into --print-allocators 1 --print-peaks 0 --print-temporary 0
'
```

## Heaptrack Result

The exact mixed-column regression test passed. The filtered heaptrack report
for `encode_batch_homogeneous_into` printed no allocator call stacks, which is
the relevant proof that the homogeneous encoder fills the caller-provided
contiguous output buffer and offset table without allocating internally.

The global report still showed normal test harness/setup allocations:

```text
calls to allocation functions: 404
temporary memory allocations: 84
peak heap memory consumption: 159.45K
total memory leaked: 944B
```

Those allocations were outside the filtered encoder stack. The regression test
also asserts that the preallocated payload buffer and offset table capacities
do not grow across 256 repeated encodes.

## Correctness Checks

Focused checks run before this artifact:

```bash
timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-bd-byzpb-types cargo test -p fsqlite-types encode_batch -- --nocapture
timeout 1200 rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-bd-byzpb-types cargo test -p fsqlite-types record_batch_mixed_columns_preserves_offsets_and_reuses_buffers -- --nocapture
```

Both checks passed.
