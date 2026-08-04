# Gate 0 three-cohort merge-parent diagnostic

This is diagnostic evidence, not a release- or README-citable benchmark. It
uses the legacy transaction driver, which lacks the Gate 0 symmetric work
receipts, and CPU 2 was pinned but not isolated from its sibling, IRQs, or
unrelated host work.

## Cohorts

| label | commit | role | binary SHA-256 |
|---|---|---|---|
| pre | `c967eaeb014988ac0496c3acea24e69d4707fe68` | repaired first parent | `f191c209ee46d887bb51ac217ed9a8071624023169c76380963cc83ec513bef5` |
| mid | `a0ab400ae2e948af86bae66d6499a1f468bb0875` | last substantive async-branch commit | `3597567bf721475a49365923c7b396db0257ae2fb92d10a323385fe75a8db09c` |
| post | `13ec577f14433b19a28ea67cc4400c542a2b4349` | merge result | `bcb9ce0f440375d96b26f5ee0d5e0a79e3b8b560632aabfa9c5705d54570c4d4` |

The commits after `a0ab400a` on the merged branch change a measurement script,
Beads data, and `.gitignore`; `a0ab400a` is therefore the last substantive
compiled-code parent used here.

## Execution

- Host: `threadripperje`, AMD Ryzen Threadripper PRO 5995WX, 128 logical CPUs.
- UTC interval: 2026-07-26 05:42:16 through 05:42:38.
- Affinity: `taskset -c 2`; CPU 2 was under the `performance` governor and has
  sibling CPU 66.
- Invocation: `--quick --filter txn --no-html --json-out <run>.json`.
- Order: 36 fresh processes in 12 three-process blocks. All six cohort
  permutations were run twice; each cohort occupied each position four times.
- Every process exited zero and emitted all nine expected transaction rows.
  `order.tsv` binds each report to its SHA-256.

## Analysis

For each engine, scenario, and three-process block, the contrast is the log
median of one cohort minus the log median of the comparison cohort. The
block-level aggregate gives all nine scenarios equal weight. The point estimate
is the exponential of the mean of the 12 block contrasts. Displayed intervals
exhaustively enumerate the finite, design-stratified bootstrap distribution:
for each of the six cohort-order permutations, they resample two blocks with
replacement from that permutation's two observed blocks, preserving every
order stratum and the multiplicity of all `(2^2)^6 = 4096` ordered draws. The
nearest-rank 95% interval sorts the 4096 ratios and takes zero-based indices 102
and 3993 (`ceil(0.025N) - 1` and `ceil(0.975N) - 1`).
Difference-in-differences subtracts the C SQLite block contrast before
aggregation.

| contrast | FrankenSQLite | C SQLite | difference-in-differences |
|---|---:|---:|---:|
| async parent / repaired parent | 2.667659 [2.633182, 2.702587] | 0.993471 [0.983246, 1.003802] | 2.685191 [2.646376, 2.724576] |
| merge result / repaired parent | 2.680579 [2.639223, 2.722582] | 0.995215 [0.982470, 1.008126] | 2.693466 [2.658376, 2.729020] |
| merge result / async parent | 1.004843 [0.996293, 1.013466] | 1.001756 [0.991541, 1.012076] | 1.003082 [0.989065, 1.017297] |

As a small-sample sensitivity check, the two complete six-permutation cycles
give difference-in-differences ratios `2.721787` and `2.649088` for async
parent/repaired parent, and `1.006770` and `0.999407` for merge result/async
parent.

Per-scenario FrankenSQLite ratios:

| scenario | async parent / repaired parent | merge result / async parent |
|---|---:|---:|
| 100 rows / autocommit | 1.845207 | 1.001473 |
| 100 rows / batched (100/txn) | 1.937758 | 1.005002 |
| 100 rows / single txn | 1.908361 | 1.006674 |
| 1,000 rows / autocommit | 2.229286 | 0.965248 |
| 1,000 rows / batched (1,000/txn) | 3.320688 | 1.039526 |
| 1,000 rows / single txn | 3.400301 | 1.038928 |
| 10,000 rows / autocommit | 2.528222 | 0.978696 |
| 10,000 rows / batched (1,000/txn) | 4.190814 | 1.025410 |
| 10,000 rows / single txn | 3.759608 | 0.985337 |

## Interpretation ceiling

The merge resolution itself adds no measurable aggregate discontinuity in this
matrix: its post/async-parent difference-in-differences is `1.003082` and the
diagnostic interval spans one. The large transaction slowdown is already
present in the async branch parent relative to the repaired parent.

That narrows the history, but it is not yet an async-mechanism A/B. The two
parents diverge from the unbuildable `31fc4a3b` state and differ across the
engine and benchmark driver. In particular, `a0ab400a` is the commit that makes
the synchronous harness drive async operations through its runtime bridge.
This artifact cannot separate bridge-entry cost from intrinsic engine cost.
The next discriminating experiment is a same-source three-arm comparison of
one runtime entry for the whole workload, one runtime entry per operation, and
a ready-future control, followed by a profile only if the engine residual
remains.
