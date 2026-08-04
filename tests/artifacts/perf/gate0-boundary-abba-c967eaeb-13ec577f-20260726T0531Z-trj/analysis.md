# Gate 0 boundary diagnostic: `c967eaeb` versus `13ec577f`

This is diagnostic evidence, not a release- or README-citable benchmark.
The legacy binary does not provide symmetric work receipts, its engine arms are
not paired within one process, and CPU 2 was pinned but not isolated from its
sibling, IRQs, or other host work.

## Execution

- Host: `threadripperje`, AMD Ryzen Threadripper PRO 5995WX, 128 logical CPUs.
- UTC interval: 2026-07-26 05:31:30 through 05:31:44.
- Affinity: `taskset -c 2`; CPU 2 was under the `performance` governor and has
  sibling CPU 66.
- Initial/final one-minute load average: 4.06 / 4.05 on 128 logical CPUs.
- Invocation: `--quick --filter txn --no-html --json-out <run>.json`.
- Order: 24 fresh processes in six four-process blocks, alternating
  `ABBA, BAAB, ABBA, BAAB, ABBA, BAAB`.
- Pre binary: commit `c967eaeb014988ac0496c3acea24e69d4707fe68`,
  SHA-256
  `f191c209ee46d887bb51ac217ed9a8071624023169c76380963cc83ec513bef5`.
- Post binary: commit `13ec577f14433b19a28ea67cc4400c542a2b4349`,
  SHA-256
  `bcb9ce0f440375d96b26f5ee0d5e0a79e3b8b560632aabfa9c5705d54570c4d4`.
- Every process exited zero and emitted all nine expected transaction rows.
  `order.tsv` binds each report to its SHA-256.

## Analysis

For each engine, scenario, and four-process block, the contrast is the mean
log median of the two post processes minus the mean log median of the two pre
processes. The block-level aggregate gives all nine scenarios equal weight.
The point estimate is the exponential of the mean of the six block contrasts.
The displayed interval exhaustively enumerates the finite, design-stratified
bootstrap distribution: it resamples three blocks with replacement
independently within the three-ABBA and three-BAAB strata, preserving both order
patterns and the multiplicity of all `(3^3)^2 = 729` ordered draws. The
nearest-rank 95% interval sorts the 729 ratios and takes zero-based indices 18
and 710 (`ceil(0.025N) - 1` and `ceil(0.975N) - 1`). The
difference-in-differences subtracts the C SQLite block contrast from the
FrankenSQLite block contrast before aggregation.

| contrast | ratio | diagnostic 95% design-stratified bootstrap interval |
|---|---:|---:|
| FrankenSQLite post / pre | 2.650381 | [2.602754, 2.701227] |
| C SQLite post / pre | 0.994767 | [0.985593, 1.003108] |
| `(FrankenSQLite post/pre) / (C SQLite post/pre)` | 2.664324 | [2.609429, 2.720374] |

All six FrankenSQLite block contrasts were above one:
`2.603740, 2.605136, 2.596626, 2.739866, 2.752716, 2.609288`.
As a small-sample sensitivity check, pairing each adjacent ABBA/BAAB block into
its three complete counterbalancing cycles gives difference-in-differences
ratios `2.588225, 2.710468, 2.695968`.

Per-scenario FrankenSQLite post/pre point ratios:

| scenario | ratio |
|---|---:|
| 100 rows / autocommit | 1.853931 |
| 100 rows / batched (100/txn) | 1.963241 |
| 100 rows / single txn | 1.903171 |
| 1,000 rows / autocommit | 2.282501 |
| 1,000 rows / batched (1,000/txn) | 3.338033 |
| 1,000 rows / single txn | 3.355347 |
| 10,000 rows / autocommit | 2.439847 |
| 10,000 rows / batched (1,000/txn) | 4.182249 |
| 10,000 rows / single txn | 3.571236 |

## Interpretation ceiling

The matrix establishes a large FrankenSQLite-side timing discontinuity across
this buildable boundary while the within-run C control remains stable. It does
not identify a mechanism. `13ec577f` is a merge whose compared interval changes
84 files (`+34,161/-22,499`) across the engine, pager, WAL, VFS, harness, and
benchmark. Attribution requires separately building and measuring the merge
parents and then profiling a narrowed cohort. No individual async bridge,
future allocation, stack-frame, or execution-path mechanism is proven by this
artifact.
