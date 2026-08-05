# Running the RealDB E2E Suite

This document provides exact commands for running every component of the
FrankenSQLite E2E suite: corpus management, deterministic compatibility
checks, concurrency benchmarks, and corruption/recovery demos.

## Prerequisites

```bash
# Confirm rustup selected the exact dated nightly pinned by this checkout.
# Run these commands from the repository; do not switch to a floating nightly.
rustup show active-toolchain

# Build the E2E binary (release recommended for benchmarks)
cargo build -p fsqlite-e2e --bin realdb-e2e --release

# Verify the binary works
cargo run -p fsqlite-e2e --bin realdb-e2e -- --help
```

## Corpus Safety Rules

1. **Never** run against `/dp/` originals -- always use working copies
2. Ingest fixtures via `sqlite3 ... ".backup ..."` (not `cp`)
3. Treat `sample_sqlite_db_files/golden/` as **immutable**
4. All tests operate on copies in `working/` or temp dirs

See `sample_sqlite_db_files/FIXTURES.md` for the complete ingestion guide.

## 1. Corpus Management

### Scan for Available Databases

```bash
# Discover all SQLite databases under /dp (default root)
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus scan

# Scan a specific directory with depth limit
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus scan --root /dp --max-depth 4

# Only show files with valid SQLite magic headers
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus scan --header-only
```

**Expected output:**

```
[scan] /dp/asupersync/.beads/beads.db
  size: 12,345,678 bytes
  page_size: 4096
  pages: 3015
  ...
```

### Import a Database

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus import \
  --db /dp/asupersync/.beads/beads.db \
  --tag beads
```

This creates a golden copy via `.backup`, computes SHA-256, and updates
`checksums.sha256`.

### Verify Golden Copies

```bash
# Verify all golden copies against recorded checksums
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus verify
```

**Expected output (success):**

```
[verify] asupersync.db: OK (sha256: 8e91ff61...)
[verify] brenner_bot.db: OK (sha256: 83608b0c...)
...
[verify] 20/20 golden copies verified.
```

**Expected output (failure):**

```
[verify] asupersync.db: MISMATCH
  expected: 8e91ff61...
  actual:   a1b2c3d4...
[verify] ERROR: 1 checksum mismatch(es)
```

## 2. Deterministic Compatibility Hash Checks

These tests verify that FrankenSQLite produces bit-identical results to
C SQLite for the same workload.

### Run a Single Workload

```bash
# Run against C SQLite (baseline)
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine sqlite3 \
  --db asupersync \
  --workload commutative_inserts_disjoint_keys \
  --concurrency 1 \
  --pretty

# Run against FrankenSQLite
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine fsqlite \
  --db asupersync \
  --workload commutative_inserts_disjoint_keys \
  --concurrency 1 \
  --pretty
```

### Available Workload Presets

| Preset | Description | Concurrency |
|--------|-------------|-------------|
| `commutative_inserts_disjoint_keys` | Each worker inserts into disjoint key ranges | Multi-worker safe |
| `hot_page_contention` | All workers target the same table/page range | Conflict-heavy |
| `mixed_read_write` | 70% reads, 30% writes across all workers | Realistic mix |
| `deterministic_transform` | Sequential schema creation + data insertion | Single-worker |

### Run with Multiple Concurrency Levels

```bash
# Test at concurrency 1, 2, 4, 8
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine sqlite3 \
  --db frankensqlite \
  --workload commutative_inserts_disjoint_keys \
  --concurrency 1,2,4,8 \
  --repeat 3 \
  --output-jsonl results/sqlite3_commutative.jsonl
```

### Save Results as JSONL

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine fsqlite \
  --db asupersync \
  --workload hot_page_contention \
  --concurrency 4 \
  --mvcc \
  --output-jsonl results/fsqlite_hot_page.jsonl
```

Each JSONL line contains a `RunRecordV1` with:
- `engine`, `workload`, `fixture_id`, `concurrency`
- `wall_time_ms`, `ops_per_sec`, `retries`, `aborts`
- `correctness.raw_sha256`, `correctness.canonical_sha256`
- `methodology` and `environment` metadata

For profiler-focused runs, use:

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine fsqlite \
  --db asupersync \
  --workload hot_page_contention \
  --concurrency 4 \
  --profile-only
```

or the granular flags:

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine fsqlite \
  --db asupersync \
  --workload hot_page_contention \
  --concurrency 4 \
  --skip-integrity-check \
  --skip-environment-metadata
```

These modes keep the JSONL schema stable. The record still uses
`RunRecordV1`. For profiler-safe runs, `run_mode.profile_only_effective`
becomes `true`, `run_mode.run_integrity_check` becomes `false`,
`run_mode.capture_environment_metadata` becomes `false`,
`environment.capture_mode` becomes `suppressed`, and the post-run
`integrity_check_ok` field is omitted. Default runs remain full correctness
runs with captured environment metadata.

## 3. Canonical Scorecard Evidence Pack

Use the dedicated evidence-pack entrypoint when you need the canonical
three-mode benchmark matrix plus one top-level scorecard bundle.

```bash
cargo run --profile release-perf -p fsqlite-e2e --bin realdb-e2e -- evidence-pack
```

For a quicker smoke pass that still covers the full fixture/workload/concurrency
matrix, pin the measurement count explicitly:

```bash
cargo run --profile release-perf -p fsqlite-e2e --bin realdb-e2e -- evidence-pack --repeat 1
```

The output goes under `artifacts/perf/bd-db300.7.7/<run_id>/` by default and
includes:

- `manifest.json` — top-level evidence-pack manifest with rerun command and row artifact links
- `bench/results.jsonl` — unified `BenchmarkSummary` rows across SQLite, single-writer, and MVCC
- `bench/summary.md` — markdown report with direct comparisons and causal scorecards
- `bench/scorecards.json` — machine-readable G1.x causal scorecard report
- `rerun_matrix.sh` — executable rerun script for the exact captured invocation

Each benchmark row still writes its own canonical per-row bundle under the
campaign artifact root with `final_scorecard` retention.

### Track G Certification Note

This evidence pack is the e2e half of the Track G certification bundle, not the
whole bundle by itself. A release may only be called conformant after these e2e
artifacts are paired with the harness-side gate report, verification-contract
outcome, release certificate JSON, and ratchet state described in
[`../../docs/design/certification-gates-ratchets-release-evidence.md`](../../docs/design/certification-gates-ratchets-release-evidence.md).

### Interpreting Results

**SHA-256 match (all three tiers agree):**

```json
{
  "correctness": {
    "raw_sha256_match": true,
    "canonical_sha256_match": true,
    "dump_match": true,
    "integrity_check_ok": true
  }
}
```

Both engines produced identical databases. FrankenSQLite is compatible.

**Raw SHA-256 mismatch but canonical match:**

```json
{
  "correctness": {
    "raw_sha256_match": false,
    "canonical_sha256_match": true,
    "dump_match": true
  }
}
```

Page layout differs (e.g., different WAL checkpoint timing), but logical
content is identical.  This is expected for concurrent workloads.

**Logical dump mismatch:**

```json
{
  "correctness": {
    "raw_sha256_match": false,
    "canonical_sha256_match": false,
    "dump_match": false,
    "notes": "divergence detected at row 42"
  }
}
```

Actual data difference between engines.  This indicates a bug that needs
investigation.  Use the mismatch debugger (see below) to produce a minimal
reproduction.

## 3. Concurrency Benchmarks

### Run the Benchmark Matrix

Track A's canonical many-core Beads campaign is pinned in
`sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json`. That
manifest freezes the fixture source paths and digests, the pinned working-copy
fixtures under `sample_sqlite_db_files/working/beads_bench_20260310/golden/`,
the three benchmark modes (`sqlite_reference`, `fsqlite_mvcc`,
`fsqlite_single_writer`), the fixed seed policy, the retry/backoff contract,
the required placement profiles, the hardware-class identifiers, and the stable
artifact bundle naming template. Future scorecard and verification work should
treat that file as the source of truth instead of inventing ad hoc matrix rows
or artifact names.

```bash
# Run all presets against all golden copies (full matrix)
rch exec -- cargo run -p fsqlite-e2e --bin realdb-e2e --release -- bench

# Run specific database and preset
rch exec -- cargo run -p fsqlite-e2e --bin realdb-e2e --release -- bench \
  --db asupersync \
  --preset commutative_inserts_disjoint_keys

# Control iterations
rch exec -- cargo run -p fsqlite-e2e --bin realdb-e2e --release -- bench \
  --warmup 3 \
  --repeat 10 \
  --output results/bench_report.json
```

### Canonical Beads Campaign

The canonical matrix for `bd-db300.1.2` is tracked in:

```text
sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json
sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.schema.json
```

That manifest freezes the real-fixture campaign dimensions instead of relying on
ad hoc shell history:

- Fixtures: `frankensqlite`, `frankentui`, `frankensearch`
- Workloads: `commutative_inserts_disjoint_keys`, `hot_page_contention`, `mixed_read_write`
- Modes: `sqlite_reference`, `fsqlite_mvcc`, `fsqlite_single_writer`
- Build profile: manifest ID `release_perf` using Cargo profile `release-perf`
- Seed policy: fixed root seed `42`
- Retry policy: harness-instrumented busy retries (`10_000`, `1ms` base, `250ms` cap)
- Placement profiles: `baseline_unpinned`, `recommended_pinned`, `adversarial_cross_node`
  with taxonomy kinds `baseline`, `recommended_pinned`, `adversarial_topology`
- Hardware classes: `linux_x86_64_any`, `linux_x86_64_many_core_numa`
  with identifier fields `os_family`, `cpu_arch`, `topology_class`

Artifact names for canonical runs must include the source revision and the
`.beads/issues.jsonl` hash so results stay mechanically diffable over time, and
they now also carry the build-profile id and explicit run id. The tracked
templates are:

```text
bundle dir: {row_id}__{workload}__c{concurrency}__{fixture_id}__{mode}__{placement_profile_id}__{build_profile_id}__run_{run_id}__rev_{source_revision}__beads_{beads_hash}
bundle key: {row_id}:{fixture_id}:{workload}:c{concurrency}:{mode}:{placement_profile_id}:{build_profile_id}:run_{run_id}:rev_{source_revision}:beads_{beads_hash}
bundle name: {row_id} {fixture_id} {workload} c{concurrency} {mode} {placement_profile_id} {build_profile_id} run {run_id} rev {source_revision} beads {beads_hash}
files: results.jsonl, summary.md, manifest.json, logs/, profiles/
```

Retention classes are part of the contract as well: `quick_run`,
`full_proof`, `failure_bundle`, and `final_scorecard`.

### Placement Profile Execution Contract

The checked-in campaign manifest now carries an exact
`placement_profiles[].execution_contract` for the three canonical profiles.
Full suites select a profile by matching
`matrix_rows[].placement_variants[].placement_profile_id`. Focused reruns and
topology-bundle capture bind the profile mechanically with:

```text
RUN_ID
ARTIFACT_BUNDLE_DIR
ARTIFACT_BUNDLE_RELPATH
PLACEMENT_PROFILE_ID
HARDWARE_CLASS_ID
MANIFEST_JSON
SOURCE_REVISION
BEADS_HASH
```

Every placement-sensitive run must disclose
`placement_profile_id`, `hardware_class_id`, `hardware_signature`,
`cpu_affinity_mask`, `smt_policy_state`, `memory_policy`,
`helper_lane_cpu_set`, and `numa_balancing_state`. If any run drifts from the
declared placement contract, treat it as `not_comparable` rather than folding
it into a clean comparison.

`baseline_unpinned`

- CPU pinning: scheduler default, with no `taskset` or `numactl` overrides.
- SMT and memory policy: host defaults; record them instead of forcing them.
- Helper lane: disclose where housekeeping work landed, but do not relocate it.
- Fixed knobs: `no_taskset_or_numactl_binding`, `report_host_default_smt_policy`, `report_host_default_memory_policy`, `disclose_helper_lane_policy_without_relocation`.
- Optional knobs: exact scheduler-chosen CPU set and extra profiler capture.
- Mandatory for: portable baseline claims and host-default regression checks.
- Avoid for: transferable many-core win claims and cross-node sensitivity claims.

`recommended_pinned`

- CPU pinning: one thread per physical core, all workers kept in one NUMA/LLC locality domain.
- SMT and memory policy: disable sibling reuse inside the primary worker set and bind memory to the same locality domain.
- Helper lane: keep housekeeping on a non-worker CPU in the same locality domain.
- Fixed knobs: `pin_workers_to_one_thread_per_physical_core`, `keep_workers_inside_one_locality_domain`, `bind_memory_to_worker_locality`, `place_helper_lane_on_housekeeping_cpu_in_same_locality`.
- Optional knobs: exact locality-domain choice, exact worker CPU set, and extra profiler capture.
- Mandatory for: transferable many-core win claims and final scorecard primary claims.
- Avoid for: portable baseline claims.

`adversarial_cross_node`

- CPU pinning: split workers across multiple locality domains to expose remote ownership and cache-line movement penalties.
- SMT and memory policy: avoid sibling reuse inside the primary worker set and use a memory policy that matches the cross-domain split instead of pretending locality.
- Helper lane: place housekeeping outside the primary worker domains.
- Fixed knobs: `split_workers_across_locality_domains`, `avoid_smt_sibling_reuse_inside_primary_worker_set`, `match_memory_policy_to_cross_domain_worker_split`, `place_helper_lane_outside_primary_worker_domains`.
- Optional knobs: exact remote-domain pair, exact cross-domain worker split, and extra profiler capture.
- Mandatory for: cross-node sensitivity claims and placement regression guard claims.
- Avoid for: headline speedup claims and portable baseline claims.

### Capture The Hardware Discovery Bundle

`bd-db300.1.6.1` defines a reusable discovery bundle so canonical benchmark runs
carry explicit hardware identity instead of relying on operator memory. The
capture entrypoint is:

```bash
bash scripts/verify_bd_db300_1_6_1_topology_bundle.sh
```

When you already know the artifact bundle directory and run-identity fields,
bind the capture mechanically to the benchmark session:

```bash
RUN_ID=run-20260315T020400Z \
ARTIFACT_BUNDLE_DIR=artifacts/perf/bd-db300.1.2/example_bundle \
ARTIFACT_BUNDLE_RELPATH=artifacts/perf/bd-db300.1.2/example_bundle \
PLACEMENT_PROFILE_ID=recommended_pinned \
HARDWARE_CLASS_ID=linux_x86_64_many_core_numa \
MANIFEST_JSON=artifacts/perf/bd-db300.1.2/example_bundle/manifest.json \
bash scripts/verify_bd_db300_1_6_1_topology_bundle.sh
```

The script emits:

```text
hardware_discovery_bundle.json
hardware_discovery_summary.md
```

The JSON bundle records CPU model and stepping, microcode or firmware where
available, socket or NUMA layout, SMT sibling sets, LLC sharing domains,
boost/EPP/governor state, THP state, relevant scheduler settings, and a stable
`hardware_signature` hash derived from the captured identity fields. Missing or
partially inferred fields are listed explicitly so later scorecards, logging
contracts, and reports do not overstate how complete the machine fingerprint
really is.

For CPU-heavy canonical runs, offload through `rch` and use the manifest as the
source of truth for dimensions and naming:

```bash
rch exec -- cargo run -p fsqlite-e2e --profile release-perf --bin realdb-e2e -- bench
```

### Track H Matched Mode Packs

`bd-db300.8.1.1` needs one reproducible entrypoint that assembles a matched
three-mode pack for the same canonical row instead of leaving operators to
manually stitch together SQLite, MVCC, and forced single-writer outputs.

The checked-in collector is:

```bash
bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

By default it targets the current Track H steering cell
`mixed_read_write_c4`, uses `baseline_unpinned`, collects both
`file_backed` and `memory` storage profiles, and routes every benchmark through
`rch exec`. Narrow the scope explicitly when you want a smaller or more focused
pack:

```bash
ROW_IDS=mixed_read_write_c4 \
FIXTURE_IDS=frankensqlite \
PLACEMENT_PROFILE_IDS=baseline_unpinned \
STORAGE_PROFILE_IDS=file_backed,memory \
WARMUP=0 \
REPEAT=1 \
bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

The output goes under `artifacts/perf/bd-db300.8.1.1/<run_id>/` and includes:

- one pack directory per `row_id` × `fixture_id` × `placement_profile_id` ×
  `storage_profile_id`
- mode-specific `results.jsonl` and `summary.md` artifacts for
  `sqlite_reference`, `fsqlite_mvcc`, and `fsqlite_single_writer`
- a per-pack `manifest.json` plus `summary.md` with shared provenance fields and
  direct deltas between MVCC and forced single-writer behavior
- a run-level `report.json` and `summary.md`

The underlying bench entrypoint now accepts:

- `--placement-profile baseline_unpinned|recommended_pinned|adversarial_cross_node`
- `--storage-profile file_backed|memory`

`baseline_unpinned` packs are directly comparable under scheduler-default
placement. If you request `recommended_pinned` or `adversarial_cross_node`, the
collector records the declared placement profile but does not enforce remote CPU
or memory placement on your behalf; those packs are marked as declared-only
until the operator supplies external placement enforcement.

`memory` storage profiles replay the same canonical OpLog against `:memory:`
databases for all three engines. That is a storage-backend contrast, not a
replacement for external NUMA or CPU pinning, so the placement profile remains
provenance metadata rather than a claim that memory-only runs enforce topology.

For the H1.2 follow-on collection, keep the same steering row and fixture but
override the bead id and placement list explicitly:

```bash
BEAD_ID=bd-db300.8.1.2 \
ROW_IDS=mixed_read_write_c4 \
FIXTURE_IDS=frankensqlite \
PLACEMENT_PROFILE_IDS=recommended_pinned,adversarial_cross_node \
STORAGE_PROFILE_IDS=file_backed,memory \
WARMUP=0 \
REPEAT=1 \
bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

Those non-baseline packs still inherit the declared placement contract from the
campaign manifest, but unless the operator also supplies external CPU and memory
binding, the collector will mark them as declared-only rather than cleanly
comparable topology-enforced runs.

When `BEAD_ID=bd-db300.8.1.2`, the collector also emits:

- `classification.json`
- `classification.md`

Those artifacts live beside the run-level `report.json` and `summary.md`. They
combine the fresh declared-only H1.2 packs with the latest comparable
`bd-db300.8.1.1` baseline report for `frankensqlite`, so H2 gets an explicit
classification instead of only raw pack deltas.

If you already have the pack manifests and only need to regenerate the
classification or run-level report after script updates, reuse the existing
output directory:

```bash
POSTPROCESS_ONLY=1 \
OUTPUT_DIR=artifacts/perf/bd-db300.8.1.2/<run_id> \
BEAD_ID=bd-db300.8.1.2 \
bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

For the H2.3 shared-placement validation rerun, keep the same steering row and
fixture, rerun the declared shared-placement profiles, and let the collector
compare the refreshed packs against the latest `bd-db300.8.1.2` report while it
also proves MVCC-by-default is still intact through the existing default-guard
test:

```bash
BEAD_ID=bd-db300.8.2.3 \
ROW_IDS=mixed_read_write_c4 \
FIXTURE_IDS=frankensqlite \
PLACEMENT_PROFILE_IDS=recommended_pinned,adversarial_cross_node \
STORAGE_PROFILE_IDS=file_backed,memory \
WARMUP=0 \
REPEAT=1 \
bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

When `BEAD_ID=bd-db300.8.2.3`, the collector also emits:

- `validation.json`
- `validation.md`
- `mvcc_default_guard.log`

`validation.json` compares the refreshed `recommended_pinned` and
`adversarial_cross_node` packs against the latest shared-placement follow-on
report from `bd-db300.8.1.2`, with per-profile deltas for single-writer median
throughput, single-writer versus MVCC ratio, and retry deltas. The same run
also executes `cargo test -p fsqlite-e2e --test bd_2yqp6_6_5_concurrent_mode_defaults`
through `rch exec` and records the guard log beside the validation report so
the bead can prove the cleanup did not flip FrankenSQLite's MVCC-by-default
contract.

If you already have the refreshed pack manifests and only need to regenerate
the report plus validation after script updates, reuse the existing output
directory:

```bash
POSTPROCESS_ONLY=1 \
OUTPUT_DIR=artifacts/perf/bd-db300.8.2.3/<run_id> \
BEAD_ID=bd-db300.8.2.3 \
bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

Override `PREVIOUS_SHARED_PLACEMENT_REPORT_JSON` when you need to compare
against a specific earlier H1.2 report instead of the latest discovered one.

For the H3 single-writer role check, keep the same steering row and fixture,
run the collector in the foreground with a timeout, and emit the dedicated
role artifact. The benchmark collector still captures all three modes so the
forced single-writer row is interpreted next to SQLite and MVCC, while the
verify-suite package is generated explicitly in forced single-writer mode:

```bash
timeout 30m env \
  BEAD_ID=bd-db300.8.3 \
  ROW_IDS=mixed_read_write_c4 \
  FIXTURE_IDS=frankensqlite \
  PLACEMENT_PROFILE_IDS=recommended_pinned,adversarial_cross_node \
  STORAGE_PROFILE_IDS=file_backed,memory \
  WARMUP=0 \
  REPEAT=1 \
  bash scripts/verify_bd_db300_8_1_1_matched_artifact_packs.sh
```

When `BEAD_ID=bd-db300.8.3`, the collector also emits:

- `single_writer_role.json`
- `single_writer_role.md`
- `verify-suite/single-writer/suite_package.json`
- `verify-suite/single-writer/logs/verify_suite.jsonl`
- `concurrent_mode_default_guard.txt`

The H3 role artifact records single-writer mode as comparison or fallback
evidence only. It must not be treated as the product default or as a headline
performance mode. The product default remains MVCC concurrent writers
(`concurrent_mode_default=true`); forced single-writer exists for explicit
`PRAGMA fsqlite.concurrent_mode=OFF`, `realdb-e2e bench --no-mvcc`, fallback
diagnostics, and G4 causal attribution. G4 should read the mode bridge as:
SQLite reference measures the external baseline, forced single-writer measures
FrankenSQLite's shared fixed tax without MVCC concurrency, and MVCC measures
the intended concurrent-writer product path.

The H3 verification plan is deliberately small and reproducible:

- Unit guards: `cargo test -p fsqlite-e2e --test bd_2yqp6_6_5_concurrent_mode_defaults`
  and `cargo test -p fsqlite-e2e --test bd_db300_7_1_2_counter_schema_alignment`.
- E2E suite package: `realdb-e2e verify-suite --mode fsqlite_single_writer
  --verification-depth quick --activation-regime low_concurrency_fixed_cost
  --placement-profile baseline_unpinned`.
- Benchmark evidence: matched packs for `sqlite_reference`, `fsqlite_mvcc`,
  and `fsqlite_single_writer` on `mixed_read_write_c4` with the H2 placement
  profiles and both `file_backed` and `memory` storage profiles.
- Logs for follow-on implementation: `events.jsonl`, `report.json`,
  `single_writer_role.json`, `single_writer_role.md`, the verify-suite JSONL
  log, and the source guard proving `concurrent_mode_default` remains true.

### Persistent Phase-Attribution Packs

`bd-db300.1.7.2` uses the persistent Criterion harness, not `realdb-e2e bench`.
The authoritative replay surface is
`crates/fsqlite-e2e/benches/concurrent_write_persistent_bench.rs`, which
exposes the `persistent_concurrent_write_8t` and
`persistent_concurrent_write_16t` groups directly.

Use those exact group ids when filtering. Do not substitute shorthand suffix
filters such as `/8t` or `/16t`; the bench source names the groups with the full
`persistent_concurrent_write_<N>t` prefix.

Keep the authoritative pack capture local-only by default. Do not assume
`rch exec -- cargo bench ... --no-run` or `rch exec -- cargo build --bench ...`
will materialize a usable local `target/.../concurrent_write_persistent_bench-*`
binary; that artifact-sync behavior is not part of the current replay contract.

Use the exact group id with a local `cargo bench` invocation for the measured
pack run:

```bash
FSQLITE_PERSISTENT_PHASE_ATTRIBUTION_DIR="$PACK_ROOT/8t" \
  cargo bench --profile release-perf -p fsqlite-e2e \
    --bench concurrent_write_persistent_bench \
    -- persistent_concurrent_write_8t
```

Keep any `rch` usage advisory-only in operator notes. It is still appropriate
for non-measured compile, check, or test offload elsewhere in the workflow, but
the measured A7.2 replay path and any wrapper script should stay local unless a
separate, explicit local-artifact handoff mechanism is introduced and
documented.

Do not route this pack through `realdb-e2e bench --threads ...`; that CLI does
not own the persistent Criterion harness and its contract is comparative matrix
replay, not the commit-path phase pack.

Replay and comparator contract for authoritative publication:

- Use the same-pack `sqlite3` and `fsqlite_mvcc` rows emitted into each
  `samples.jsonl` as the direct comparator for that thread regime.
- Treat `artifacts/perf/2026-03-23-local/persistent_8t.log` and
  `artifacts/perf/2026-03-23-local/persistent_16t_fsqlite_only.log` as
  historical references only, not as the comparator of record.
- Preserve `provenance.json`, `samples.jsonl`, `bench.log`, a pack-level
  `rerun.sh`, the current commit SHA, the `.beads/issues.jsonl` hash, cargo
  profile, and the matching hardware-discovery bundle.
- `samples.jsonl` currently records every completed Criterion batched iteration.
  That means warmup and measurement iterations are mixed unless the harness or
  wrapper annotates them explicitly. Do not present the pack as authoritative
  without either separating those phases or calling out that limitation in the
  pack summary/manifest.

### Run Library Benchmarks (Unit Tests)

```bash
# Run all benchmark-related tests
cargo test -p fsqlite-e2e --lib benchmark -- --nocapture

# Run bench_summary tests
cargo test -p fsqlite-e2e --lib bench_summary -- --nocapture
```

### Interpreting Benchmark Output

The benchmark summary includes latency and throughput statistics:

```json
{
  "benchmark_id": "sqlite3:commutative_inserts:asupersync:c4",
  "warmup_count": 3,
  "measurement_count": 10,
  "latency": {
    "median_ms": 125.0,
    "p95_ms": 142.0,
    "p99_ms": 155.0,
    "stddev_ms": 8.3
  },
  "throughput": {
    "median_ops_per_sec": 8000.0,
    "peak_ops_per_sec": 8500.0
  }
}
```

**Key metrics:**
- `median_ms` — primary statistic (50th percentile wall time)
- `p95_ms` — tail latency statistic (95th percentile)
- `stddev_ms` — measurement stability (lower = more consistent)
- `median_ops_per_sec` — primary throughput metric

See `crates/fsqlite-e2e/METHODOLOGY.md` for the full methodology.

## 4. Corruption and Recovery Demos

### Run All Corruption Scenarios (Library Tests)

```bash
# C SQLite corruption behavior (baseline)
cargo test -p fsqlite-e2e --lib corruption_demo_sqlite -- --nocapture

# FrankenSQLite recovery demos
cargo test -p fsqlite-e2e --lib fsqlite_recovery_demo -- --nocapture

# Full corruption framework unit tests
cargo test -p fsqlite-e2e --lib corruption -- --nocapture
```

### Run the CLI Corruption Demo

```bash
# Inject bitflip corruption
cargo run -p fsqlite-e2e --bin realdb-e2e -- corrupt \
  --db asupersync \
  --strategy bitflip \
  --count 5 \
  --seed 42

# Zero a page
cargo run -p fsqlite-e2e --bin realdb-e2e -- corrupt \
  --db asupersync \
  --strategy page \
  --page 2 \
  --seed 0

# Zero a byte range
cargo run -p fsqlite-e2e --bin realdb-e2e -- corrupt \
  --db asupersync \
  --strategy zero \
  --offset 0 \
  --length 100
```

See `crates/fsqlite-e2e/CORRUPTION_DEMO_RUNBOOK.md` for the full corruption
and repair runbook with expected output for each scenario.

## 5. Quick Smoke Test

Run a fast verification that the infrastructure works:

```bash
# Library smoke tests (fast, <5 seconds)
cargo test -p fsqlite-e2e --lib smoke -- --nocapture

# Full library test suite (includes all modules)
cargo test -p fsqlite-e2e --lib
```

**Expected output:**

```
test result: ok. 254 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## 6. Artifacts and Output Locations

| Artifact | Location | Format |
|----------|----------|--------|
| Run records | `--output-jsonl <PATH>` | JSONL (one record per line) |
| Benchmark report | `--output <PATH>` | JSON |
| Working copies | `sample_sqlite_db_files/working/` | SQLite `.db` (ephemeral) |
| Golden copies | `sample_sqlite_db_files/golden/` | SQLite `.db` (immutable) |
| Metadata | `sample_sqlite_db_files/metadata/` | JSON per DB |
| Checksums | `sample_sqlite_db_files/checksums.sha256` | sha256sum format |

## Troubleshooting

**"No golden copies found"**
- Run `corpus import` first, or check that `sample_sqlite_db_files/golden/` has `.db` files
- Golden files are git-ignored; they must be ingested locally

**"Checksum mismatch"**
- A golden file was modified after ingestion
- Re-ingest from `/dp/` source: `sqlite3 <source> ".backup '<golden>'"`
- Regenerate checksums: `cd golden && sha256sum *.db | sort -k2 > ../checksums.sha256`

**"integrity_check failed"**
- The source database may have been corrupted before ingestion
- Try re-ingesting; if it persists, the source has problems

**"Engine returned different row counts"**
- Investigate with `--pretty` flag to see per-statement results
- Check if the workload uses non-deterministic operations (avoid RANDOM())
- File a bug with the JSONL output for reproduction
