# MVCC Hot-Path Session Summary — 2026-04-24 (cc_4 / NavyCreek)

Cumulative mvcc commits landed this session, each with its own `#[ignore]`
microbench under `cargo test -p fsqlite-mvcc --lib --release`. All numbers
below come from a single batched run this session:

    cargo test -p fsqlite-mvcc --lib --release -- --ignored --nocapture \
      <bench_name_1> <bench_name_2> ...

The command above is retained as historical provenance; it records the profile
that produced these numbers and must not be silently rewritten. Under current
project policy, `release` is size-optimized, so this report is not current
throughput-release evidence and must not be compared directly with
`release-perf` results. Any new run must use `--profile release-perf` and record
any host-tuning `RUSTFLAGS` in its artifact provenance.

Parallel test execution adds some noise to paired before/after numbers,
but the signs are consistent with each commit's original dedicated run.

## Commits + microbench results

| # | Commit | Area | Bench | Before | After | Δ |
|--:|---|---|---|---:|---:|---:|
| 1 | **06e13def** | waiter-shard mutex skip on post-park | `test_in_process_lock_table_wait_for_holder_change_microbench` | — | 77,922 wakes/sec (n=5) | saves 1 mutex acquire / successful wake (see commit body) |
| 2 | **b17ba9b4** | cache waiter `ThreadId` (land: b17ba9b4 by concurrent agent; pattern from 06e13def) | (shares the wait-for-holder-change harness above) | — | — | — |
| 3 | **40c64b53** | `ActiveEdgeDiscoveryIndex::build` — identity hasher + pre-sized cap | `bench_active_edge_index_build` (16 views × 10 pages) | 43,522 ns/build | 30,115 ns/build (re-run) | **-30.2%** |
| 4 | **78350138** | compound `concurrent_page_read_state` helper for VDBE read path | `bench_read_path_page_state_lookup` (64 staged pages, mixed hit/miss) | 4.5 ns/probe | 3.8 ns/probe | **-15.8%** |
| 5 | **bc4fa6b5** | gate snapshot-read histogram off the resolve fast path | `bench_resolve_visible_version_metric_gate` (256 pages × 1 version) | 116.7 ns/resolve | 35.3 ns/resolve | **-69.8%** |
| 6 | **9521594b** | `SireadTable`-empty short-circuit in `ActiveEdgeDiscoveryIndex::{incoming,outgoing}_candidate_refs` + identity-hashed committed SSI indexes | `bench_incoming_candidate_refs_empty_readers` (16 writer views × 10 pages, zero readers) | 10.7 ns/call | 9.6 ns/call | **-9.3%** |
| 7 | **ec87700b** + **1348b5ac** | branchless `visible()` / `CellDelta::is_visible_to` via `wrapping_sub` | `bench_visible_branchless` (SAMPLES=4096 mix, 80/10/10) | 1.62 ns/call | 1.60 ns/call | **-1.3%** (measurable; compiler was already close) |
| 8 | **f2707d1a** | gate `visibility_ranges` side-index off `publish()` + GC | `bench_publish_visibility_ranges_gate` (32 pages, 20K publishes/trial) | 782 ns/publish | 718 ns/publish | **-8.2%** |
| 9 | **d2156302** | gate CAS-attempts histogram off `publish()` | `bench_publish_cas_metrics_gate` (same shape, toggles CAS gate only) | 777 ns/publish | 722 ns/publish | **-7.1%** |
| 10 | **03c49886** | gate `mvcc_snapshot_established`/`_released` under existing snapshot-metrics flag; fix bc4fa6b5 regression in `test_snapshot_read_span_and_metrics` | `bench_snapshot_established_released_gate` (tight est+rel loop, 4M cycles) | 6.46 ns/cycle | 0.82 ns/cycle | **-87.3%** |

## Pattern recap (what worked, what didn't)

**AtomicBool gate on unconditional diagnostic atomics** was the highest
ROI pattern — bc4fa6b5, f2707d1a, d2156302, 03c49886 all followed the
same shape and saved 1–3 unconditional relaxed `fetch_add`s per call
on a hot path nobody was reading in production. Default off; tests
flip on; readers shaped as `#[inline]` fast path + `#[cold]
#[inline(never)]` slow body.

**Identity hasher on PageNumber-keyed HashMaps** was the next-best
pattern — 40c64b53 on `ActiveEdgeDiscoveryIndex` and the
`committed_readers/writers_by_page*` maps in 9521594b each stopped
paying `RandomState` cost on every lookup.

**Compound HashMap probes** (78350138) beat double lookups when two
accessors shared a key.

**Post-wake mutex elision** (06e13def, b17ba9b4) saved one shard
acquire per successful `wait_for_holder_change`.

**Branchless predicate** (ec87700b) was the smallest win in this set
— the compiler had already lowered the short-circuit `&&` to
near-branchless code on x86.

## Remaining mvcc hot path

Post-commit MT8 profile (`profiling-mt-mvcc-20260424T161631Z/perf_mt8_flat.txt`)
shows no mvcc symbol above 1% self-time. Remaining residual sub-1%:
`notify_all_waiters` (0.29%), `concurrent_page_state` (0.26% —
compound-helper caller not yet wired into every engine site),
`unregister_waiter` (0.14%), `incoming_candidate_refs` (0.12%),
`HandleView::new` (0.10%).

The remaining structural serialization point is `VersionStore::publish`'s
`arena.write()` — `VersionArena::alloc` needs a mutable borrow because
the outer `Vec<Vec<ArenaSlot>>` can reallocate on append. Addressing
that would require a per-thread chunk allocator or a two-phase
alloc-publish split; neither fits a single-session lever budget.

## Test state

All new benches are `#[ignore]`d so they don't run in default CI. The
one existing test this session repaired is
`lifecycle::tests::test_snapshot_read_span_and_metrics` — it was
failing post-bc4fa6b5 because the gate wasn't flipped on; 03c49886
enables it explicitly.
