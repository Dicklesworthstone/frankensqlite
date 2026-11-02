# Issue #531: reviewed correction and native qualification follow-up

## Status

**The optimizer correction is not qualified for production. The two patches
in this directory remain unapplied to engine and benchmark sources on main.
No candidate SQL result, baseline/candidate speedup, or query-memory improvement
has been established by this follow-up.** The existing Python benchmark protocol
passed, and full-scale stock fixtures and prepared jobs were constructed; those
checks do not execute the proposed Rust optimizer or prepared runner.

Work began from `770a812db0d494c46d900c5e5601768599d1bb34` in an isolated clone,
with a separate clean baseline worktree. The saved candidate was applied there.
While review was in progress, main advanced to
`bd569509d80427e11fb40d3654b70015a929c8d5`, an artifacts-only preservation
checkpoint. That commit was fast-forwarded into the working clone, and its
newer candidate changes were reviewed and incorporated alongside this
follow-up's fixes. Relevant committed engine files remain identical between
those two revisions.

The previous [November 1 record](qualification-20261101.md) is historical. Its
full-disk and protective virtual-memory-limit failure is distinct from this
session's SIGKILL failures under shared memory pressure. The preceding patch
remains recoverable from git history.

## Preserved source identities

| Artifact | SHA-256 |
| --- | --- |
| [Complete engine/test candidate](unqualified-engine-candidate.patch) | `9a86244589da2f9af732a55327c2ac1021e0c30a00f87eec6cda72b137267d81` |
| [Prepared benchmark overlay](unqualified-prepared-benchmark.patch) | `f9b7546ab17241ea97e6f9d21b72c07b313454acf6f4036d45104df641a6bb57` |
| Unchanged Cargo.lock | `828b244f97320a944275e708eb28be1032debad72acbb649f10dc03327cca502` |

The engine patch covers the original six source/test files. The benchmark
overlay modifies the existing `prepared_cache_hot_paths` target; it adds no
dependency or Cargo manifest changes. Both patches passed forward applicability
against the clean `770a812` worktree and reverse applicability against the
modified working tree. Patch applicability establishes reproducibility, not
compiler or runtime acceptance.

## Corrections made during this review

1. **Covering GROUP BY eligibility:** the saved optimization compared the
   aggregate name with lowercase `count`, but grouped aggregate parsing stores
   `COUNT`. The comparison now uses the actual representation. Proposed
   bytecode/work guards require the covered path to omit table cursor opens and
   rowid lookups, so a silent failure to activate cannot pass those assertions.
2. **Selective equality with residuals:** an indexed equality can now supply
   grouped input even when other WHERE predicates or aggregate arguments need
   the table. The complete residual predicate still runs. Covering execution
   requires exactly one completely enforced equality conjunct or residual-free
   constant IN membership; repeated index columns cannot falsely prove that
   another predicate was enforced. SUM, FILTER, HAVING, expression keys, and
   residual COUNT controls retain required reads/evaluation.
3. **Raw AST parameter ordering:** grouped index admission declines anonymous
   or named WHERE placeholders that have not been canonicalized. Reordering an
   index probe must not reassign their emission-order slots. Normal Connection
   preparation already canonicalizes bindings; explicit numbered parameters
   retain the indexed route.
4. **Projection aliases and scope:** native scalar-projection admission checks
   dependencies on output aliases in WHERE/ON and LIMIT/OFFSET. WHERE/ON retain
   source-column precedence; LIMIT/OFFSET use their independent scope. Bare
   outer correlations must have exactly one binding across the complete FROM
   scope before a source-specific affinity/index proof is accepted.
5. **Generated columns:** probes that would read a VIRTUAL generated column
   through a raw scalar scan context retain fallback. The proposed regression
   distinguishes a real NULL input whose expression computes a value from a
   missing LEFT JOIN row that must remain null-extended.
6. **Scalar expression evaluation:** the newer saved candidate's recursive
   safety checks were incorporated. Lazy AND/OR, CASE, coalesce/ifnull/iif,
   row-value predicates, nested subqueries, and unsupported modifiers do not
   enter this COUNT fast path, including when nested beneath eager expressions.
   Strict-native controls cover supported eager arithmetic, casts/collations,
   NULL tests, JSON expressions, and malformed-JSON errors.
7. **Prepared invalidation expectations:** the saved COUNT-override test
   expected an old prepared handle to execute after function registration. The
   engine's registry-change contract returns `SchemaChanged`; the test now
   checks that result and verifies freshly prepared and direct executions.
   Additional proposed cases cover collation replacement, TEMP shadowing,
   differing column layouts, rebinding, rollback, and restored main visibility.
8. **Exact benchmark values:** the proposed runner preserves raw TEXT bytes
   using `SmallText::as_bytes_direct()` and stock `ValueRef` materialization.
   It does not decode arbitrary SQLite TEXT through lossy UTF-8. Storage
   classes, float bits, row multiplicity, and required order are compared after
   timing; full owned result materialization remains inside both timed loops.

The ordered two-table lookup join, LIMIT/OFFSET handling, correlated COUNT
cursor scopes, and cursor-derived EQP were independently reviewed. No further
concrete result-correctness defect was found in that source review. That is not
a claim that their new regression tests passed. The ordinary ungrouped
aggregate seek emitter remains unchanged, and concurrent-mode defaults remain
unchanged.

## Remaining expensive shapes

These are source-routing findings, not newly measured performance results.

| Shape | Current candidate behavior / remaining work |
| --- | --- |
| Selective BINARY equality/IN grouping | Matching index runs feed the existing grouping sorter; safe COUNT cases can avoid table lookups. Full native correctness/work checks remain required. |
| Unfiltered ordered GROUP BY | Still lacks general streaming grouping over an ordered index; the existing grouping sorter remains. |
| Non-BINARY grouped predicates | Conservative admission still leaves these cases on the established path. Broader collation-aware seek qualification remains open. |
| Eligible two-table ordered lookup joins | Candidate installs ordered outer traversal or the runtime-bounded top-N sorter. Native NULL/collation/multiplicity/LIMIT tests have not executed. |
| Three-or-more-table joins and general nested-loop joins | `codegen_multi_join_lookup_select` and the general join emitter still open unbounded sorters and scan the driver. The new two-table bound does not extend to them. |
| General correlated expressions | Direct indexed builtin COUNT projections are narrowly admitted. Unsupported/lazy/generated/ambiguous cases retain fallback. |

Broader EQP work tracked by `bd-oi8tv` remains open. VM navigation counts,
callback counts, and zero-table-lookup assertions are work guards, not exact
storage rows-examined measurements.

## Executed verification

| Check | Outcome | Evidence and limit |
| --- | --- | --- |
| Existing #530/#531 Python benchmark protocol tests | 30 passed | [Raw log](followup-20261102/issue531-protocol-tests.log). Validates the existing collector/protocol only. |
| Final workspace formatting gate | Exit 0, Nov 2 00:02:07 start, 239.904 s | [Metadata](followup-20261102/native/candidate-fmt-final.json), [log](followup-20261102/native/candidate-fmt-final.log). `cargo fmt --all --check`; repository exclusions omit core/VDBE, so this alone cannot validate their formatting. |
| Focused pinned rustfmt | Passed | Small changed files formatted directly; new regions of the large files formatted with `--file-lines`. Parsing/formatting only, not type checking. |
| Whitespace and patch applicability | Passed | `git diff --check`, forward and reverse `git apply --check` for both retained patches. |
| Stock SQLite 3.53.2 oracle | Built and identified | [Build provenance](followup-20261102/sqlite-3.53.2/build-provenance.json), [compiler log](followup-20261102/sqlite-3.53.2/build.log). |
| Full CLI fixture | Stock integrity/count checks passed | 50,000 users, 500,000 posts, 500,000 distinct likes; two million insertion attempts. |
| Full synthetic four-per-post fixture | Stock integrity/count checks passed | 50,000 users, 500,000 posts, 2,000,000 distinct likes. |
| Stock bound-versus-literal queries | All 17 matched on each full-scale fixture | [CLI results](followup-20261102/prepared-cli-full-jobs/stock-bound-query-check.json), [synthetic results](followup-20261102/prepared-four-per-post-full-jobs/stock-bound-query-check.json). Stock-only correctness passes, not timing benchmarks. |
| Native engine, proposed Rust benchmark, Clippy, matched performance/RSS | Not qualified | Native build attempts below did not reach changed code. No candidate runtime acceptance. |

The protocol command was:

```sh
python3 -m unittest discover -s scripts/tests -p 'test_benchmark_issue_53[01].py' -v
```

The passing run used a matching process namespace. A sandbox attempt could not
read its child's `/proc/<pid>/status`; no assertion or memory gate was weakened
to hide that mismatch. The suite comprises 14 issue #530 and 16 issue #531 tests.

### Native attempts and resource boundary

The pinned toolchain was verified as `rustc 1.100.0-nightly (908501772
2026-08-30)`, commit `90850177249efe0321573c569aec5d12b257f8d6`, LLVM 23.1.0,
from `nightly-2026-08-31`. One Cargo job, disabled incremental compilation,
and disabled dev/test debug information were used throughout native attempts.

| Attempt, UTC start | Result | Elapsed / maximum child RSS | Raw evidence |
| --- | --- | --- | --- |
| Three required integration targets, Nov 1 23:34:17 | Deliberately stopped with SIGTERM during dependency compilation when shared pressure stalled progress; not a compiler rejection | 384.068 s / 501,928 KiB | [JSON](followup-20261102/native/candidate-tests-initial.json), [log](followup-20261102/native/candidate-tests-initial.log) |
| Grouped VDBE test filter, Nov 1 23:41:46 | Exit 101; unchanged Asupersync rustc killed by SIGKILL | 108.994 s / 2,973,196 KiB | [JSON](followup-20261102/native/candidate-vdbe-grouped.json), [log](followup-20261102/native/candidate-vdbe-grouped.log) |
| Workspace check, Nov 1 23:45:23 | Setup failure: this compiler rejects `mir-strip-debuginfo=all`; corrected for the next attempt | 0.814 s / 31,696 KiB | [Metadata](followup-20261102/native/candidate-workspace-check.json), [captured output](followup-20261102/native/candidate-workspace-check.log.json) |
| Serial workspace check, Nov 1 23:45:55 | Exit 101; unchanged Asupersync rustc killed by SIGKILL | 440.147 s / 3,443,536 KiB | [JSON](followup-20261102/native/candidate-workspace-check-serial.json), [log](followup-20261102/native/candidate-workspace-check-serial.log) |
| Narrow serial VDBE library check, Nov 1 23:55:34 | Exit 101; unchanged Asupersync rustc killed by SIGKILL | 620.798 s / 3,723,244 KiB | [JSON](followup-20261102/native/candidate-vdbe-check-serial.json), [log](followup-20261102/native/candidate-vdbe-check-serial.log) |

The [capture helper](followup-20261102/native/run_native.py) and JSON records
retain exact commands, working directory, environment, lock hash, exit status,
elapsed time, and cgroup counters. `child_max_rss_kib` is
`getrusage(RUSAGE_CHILDREN).ru_maxrss`, the largest child RSS, not simultaneous
process-group or cgroup memory. Cgroup OOM counters are cumulative across
unrelated work and cannot all be attributed to these commands.

The short invalid-flag output is stored in a JSON string container with its
original SHA-256, preserving its terminal blank lines without introducing
artifact whitespace errors. Other compiler logs are retained as plain text.

A controlled stop was planned for the final stalled attempt, but its compiler
process had already exited when checked. No stop signal was sent; its raw log
records SIGKILL independently of that attempted intervention.

The worker shares an **8,589,934,592-byte (8 GiB) memory limit** with other builds,
with usage repeatedly at that ceiling. This session had disk capacity (about
12 GiB free at a late snapshot); the earlier checkpoint's full disk must not
be copied forward as this session's blocker. No files/caches were deleted,
dependencies changed, incompatible compiled artifacts substituted, or test
gates relaxed. No usable alternate RCH runner was available. Clippy and
release/performance builds have not completed; listing their commands is not
evidence of running them successfully.

The serial diagnostic attempts used:

```sh
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export RUSTFLAGS='-Zunstable-options --jobs-frontend=1 --jobs-backend=1 -Zmir-opt-level=0 -Zmir-strip-debuginfo=all-locals'
cargo check --locked --workspace --all-targets
cargo check --locked -p fsqlite-vdbe --lib
```

These flags were a resource-reduction diagnostic, not a performance experiment
configuration. They do not establish that the candidate compiles on a larger
worker; only a completed build can establish that.

## Full-scale oracle fixtures and prepared jobs

The official C SQLite shell reports version 3.53.2 and source ID
`2026-06-03 19:12:13 d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24`.
Its `sqlite3.c` SHA-256 is
`0a409f1633283fa31a9126b11fbfd64a1991c5d30defad07e5745d4667f5e23d`, identical
to the locked libsqlite3-sys 0.38.2 source. Its executable SHA-256 is
`d9d213b02e7e38e6cbb94888317a89cbf2f3fa8a08c7dbf1a9d0243702dfee4e`.
The retained build provenance includes archive URL/hash, compiler arguments,
compile options, and the runtime version. A future rusqlite build must still
record its own live source ID and options; matching source does not prove
matching compiler configuration.

| Fixture | Users / posts / distinct likes | Seed SHA-256 |
| --- | --- | --- |
| Published CLI recipe | 50,000 / 500,000 / 500,000, from 2,000,000 attempted likes inserts | `4aa75af6088ce8d0bf2a0be3224f2fb3b21be67591a33d8b855d551a21a1c6a5` |
| `four-per-post`, explicitly synthetic | 50,000 / 500,000 / 2,000,000 | `2f05531cc79b98e9957d1bf3009de3bc550282f34de665fcc29e4b726a4652e7` |

The CLI seed is 68,055,040 bytes. Its [SQL recipe](followup-20261102/fixture-cli-full/seed.sql),
[provenance](followup-20261102/fixture-cli-full/seed-provenance.json), and
[stock output](followup-20261102/fixture-cli-full/seed.stdout) are retained.
Both fixture recipes come unchanged from `scripts/benchmark_issue_531.py`.
Neither fixture is claimed to reproduce the reporter's unattached prepared
Rust-harness data. Database binaries are reproducible from the retained SQL
and are not committed here.

The synthetic seed is 130,617,344 bytes; its [SQL recipe](followup-20261102/fixture-four-per-post-full/seed.sql),
[provenance](followup-20261102/fixture-four-per-post-full/seed-provenance.json), and
[verification output](followup-20261102/fixture-four-per-post-full/verification.stdout)
are retained as well. Stock creation completed with exit 0 and no stderr, and
the verification reported `50000|500000|2000000` and `ok`. Seed construction
time and seeding RSS are preparation metadata, not query-performance evidence.

The [baseline job](followup-20261102/prepared-cli-full-jobs/baseline-job.json),
[candidate job](followup-20261102/prepared-cli-full-jobs/candidate-job.json), and
[case manifest](followup-20261102/prepared-cli-full-jobs/job-manifest.json) contain
all 14 existing driver cases, an equality GROUP BY with a noncovering residual,
and alternate integer/NULL binding controls: 17 cases in total. The stock-only
bound/literal check preserves quoted full results and ordering/multiplicity.
The jobs request five trials with a minimum two-second window per engine.
Their build provenance deliberately remains `PENDING_BUILD`.

A [second baseline job](followup-20261102/prepared-four-per-post-full-jobs/baseline-job.json),
[candidate job](followup-20261102/prepared-four-per-post-full-jobs/candidate-job.json),
and [manifest](followup-20261102/prepared-four-per-post-full-jobs/job-manifest.json)
provide the same 17 cases for the full synthetic fixture. All 17 stock
bound-versus-literal comparisons passed there too. The two stock checks took
0.233 s and 0.093 s respectively; these totals are collector checks, not
prepared-runner or candidate latency measurements.

### Proposed prepared runner protocol

Apply the identical benchmark-only overlay to baseline and candidate. The
runner prepares outside timing, retains a statement per case, and uses one
current-thread runtime. Both timed loops include binding, execution, complete
owned-row materialization, and dropping later iterations' rows. Typed
comparison and report serialization happen outside timing. Each process runs
one FrankenSQLite revision against live bundled SQLite, alternating engine
order between trials. A checked warmup and the first result of every trial are
verified; the runner does **not** claim to compare every timed iteration.

The implementation checks seed hashes and copies, read-only stock statements,
effective WAL/NORMAL/foreign-key PRAGMAs, and final stock integrity/counts. It
records query plans, binary/job/seed hashes, live SQLite identity/options, raw
samples, and append-only progress events. These are reviewed implementation
properties, not evidence that the uncompiled runner works.

Job provenance is supplied by the caller, not recovered from the executable;
fill and independently validate source revision/patch hashes, source hashes,
Cargo.lock, toolchain, profile, features, flags, command, and actual binary hash
before qualification. The alternate binding cases prepare separate handles for
identical SQL; they check cache contamination, not in-loop rebinding of one
handle. Native integration tests remain necessary for retained-handle mutation,
registry changes, transactions, and correctness boundaries.

## Exact continuation on a provisioned worker

Start clean baseline and candidate checkouts from the recorded base. Apply
`unqualified-engine-candidate.patch` only to the candidate and
`unqualified-prepared-benchmark.patch` to both. Retain patch digests as part of
the source identity while the source changes are uncommitted. Then execute:

```sh
cargo test --locked -p fsqlite-core \
  --test bd_group_by_depth_oracle_probe \
  --test bd_9gl3w_join_limit \
  --test gh432_correlated_exists_index_probe
cargo test --locked -p fsqlite-vdbe --lib codegen::tests::bd_2dgf5_
cargo test --locked -p fsqlite-vdbe --lib codegen::tests::in_list_residual_detection_and_routing
cargo test --locked -p fsqlite-vdbe --lib codegen::tests::grouped_aggregate_limit_validates_and_preserves_numbered_slots
cargo test --locked -p fsqlite-vdbe --lib engine::tests::test_seekge_honors_nocase_collation_for_full_index_keys
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

After correctness acceptance, build each revision with identical compiler,
profile, features, flags, and dependencies. For example, use the repository's
`release-perf` profile for both binaries in each matched experiment:

```sh
cargo build --locked --profile release-perf -p fsqlite-cli
cargo bench --locked --profile release-perf -p fsqlite-core \
  --bench prepared_cache_hot_paths --no-run
```

Resolve actual executable paths from those builds. Update job paths and build
provenance, retain seed hashes, use fresh output directories, and run:

```sh
/absolute/baseline-prepared_cache_hot_paths --issue-531-job /absolute/baseline-job.json
/absolute/candidate-prepared_cache_hot_paths --issue-531-job /absolute/candidate-job.json
```

A two-second trial window is not a per-query timeout. Use an external timeout,
capture its exit status, and retain partial progress when a query cannot finish.
Use separately labeled `read_transaction: true` jobs for explicit read
transactions, with new output directories. The two engines share a process in
this runner, so its RSS cannot attribute memory to one engine.

Run the existing separate-process CLI comparisons as well, with its verified
matched-build manifest:

```sh
python3 scripts/benchmark_issue_531.py \
  --baseline-fsqlite /absolute/baseline-fsqlite \
  --fsqlite /absolute/candidate-fsqlite --sqlite /absolute/sqlite3 \
  --build-manifest /absolute/builds.json \
  --posts 500000 --users 50000 --likes-mode cli \
  --trials 6 --iterations 1 --warmups 1 --timeout 600 \
  --work-dir /absolute/new-cli-timing-directory

python3 scripts/benchmark_issue_531.py \
  --baseline-fsqlite /absolute/baseline-fsqlite \
  --fsqlite /absolute/candidate-fsqlite --sqlite /absolute/sqlite3 \
  --build-manifest /absolute/builds.json \
  --posts 500000 --users 50000 --likes-mode cli \
  --memory --memory-queries 3 --trials 6 --timeout 600 \
  --work-dir /absolute/new-cli-memory-directory
```

Repeat both protocols with the separately labeled `four-per-post` full fixture.
Do not override the memory mode's iterations/warmups defaults. Run Linux memory
collection in a matching process namespace. CLI timing includes parsing,
preparation, and printing; it does not replace the prepared-API experiment.
Optional ratio thresholds are not enabled by default, so a collector's success
status alone is not a performance threshold pass.

Only executed native correctness, workspace gates, and matched full-scale
results can justify integrating the optimizer as a validated implementation
milestone or asserting that issue #531 is fixed.
