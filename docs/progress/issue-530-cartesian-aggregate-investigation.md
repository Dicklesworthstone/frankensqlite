# GH#530: Cartesian aggregate investigation, 2026-10-28

The published FrankenSQLite v0.4.12 binary reproduces the large Cartesian
aggregate penalty. On this worker, Cartesian COUNT and SUM took about 17.8 times
their single-series counterparts. Stock SQLite executed the same SQL in the same
benchmark invocation. Source inspection and a separate memory experiment identify
full intermediate-row materialization as a substantial, avoidable source of work.
They do not establish a complete CPU-cost breakdown.

This change supplies benchmark and investigation evidence. An engine optimization
and Rust regression tests were drafted locally, but compilation could not finish
within the worker's memory budget. The optimization is held for validation: there
are **no current-main/candidate executable measurements, no passing Rust test
claim, and no before-and-after speedup claim**. [Issue #530 remains the tracking
issue](https://github.com/Dicklesworthstone/frankensqlite/issues/530).

## Workloads and timing results

The first four queries are the issue's reproductions. The fifth is a diagnostic
that preserves the COUNT result while bypassing the existing lone-table-function
streaming aggregate admission:

```sql
SELECT count(*) FROM generate_series(1,1000000);
SELECT count(*) FROM generate_series(1,1000)a,generate_series(1,1000)b;
SELECT sum(value) FROM generate_series(1,1000000);
SELECT sum((a.value-1)*1000+b.value) FROM generate_series(1,1000)a,generate_series(1,1000)b;
SELECT count(*) FROM generate_series(1,1000000) HAVING count(*)>=0;
```

COUNT results are `1000000`; SUM results are `500000500000`. Every measured query
and warmup returned the expected scalar. The retained
[timing artifact](../../tests/artifacts/perf/issue-530-20261028/release-stock.json)
was produced by [the reusable CLI harness](../../scripts/benchmark_issue_530.py)
on 2026-10-28, from 20:27:23 to 20:28:26 UTC. Medians are seconds:

| Case | v0.4.12 wall | Stock wall | v0.4.12 / stock | v0.4.12 CPU | Stock CPU |
|---|---:|---:|---:|---:|---:|
| Single COUNT | 0.0960 | 0.0129105 | 7.44x | 0.0958470 | 0.0128475 |
| Cartesian COUNT | 1.7065 | 0.0145780 | 117.06x | 1.7000735 | 0.0145780 |
| Single SUM | 0.1120 | 0.0264805 | 4.23x | 0.1118090 | 0.0264900 |
| Cartesian SUM | 1.9785 | 0.0468235 | 42.25x | 1.9733890 | 0.0468295 |
| Single COUNT with HAVING | 1.1205 | 0.0118895 | 94.24x | 1.1140350 | 0.0118955 |

The release's Cartesian/single ratios are 17.78x for COUNT and 17.67x for SUM;
stock's ratios are 1.13x and 1.77x respectively. HAVING raises the release's
single-series COUNT median by 11.67x without adding a join. The SUM product also
performs arithmetic per input row, so equality of row counts does not imply
identical work. These measurements neither establish an inherent join overhead
nor imply that an optimized FrankenSQLite will match stock's timings.

Each sample uses a fresh process, an explicit empty init file and an in-memory
database, followed by one untimed warmup and one timed SELECT. Six samples per
engine alternate engine order. No builds or profiling ran concurrently with this
timing run. The native `.timer` includes execution and scalar output; it excludes
startup, fixture setup, warmup and shutdown. Shell metadata-preparation boundaries
can differ, so this is a CLI comparison, not an execute-only library comparison.
FrankenSQLite wall times round to 1 ms, with up to 0.5 ms rounding error per
sample; native user+system CPU times retain microsecond precision. The worker is
not a dedicated performance host, and the raw samples show variability.

All six wall-time samples, in repetition order, are retained here as well as in
the JSON. CPU samples and native timer lines are in the JSON.

| Case | v0.4.12 wall samples (seconds) | Stock wall samples (seconds) |
|---|---|---|
| Single COUNT | 0.086, 0.091, 0.100, 0.099, 0.099, 0.093 | 0.012672, 0.012144, 0.011961, 0.013423, 0.015953, 0.013149 |
| Cartesian COUNT | 1.613, 1.807, 1.800, 1.747, 1.666, 1.643 | 0.012780, 0.013315, 0.020909, 0.023292, 0.010875, 0.015841 |
| Single SUM | 0.099, 0.139, 0.108, 0.114, 0.110, 0.119 | 0.024871, 0.028218, 0.040613, 0.026662, 0.026299, 0.022351 |
| Cartesian SUM | 1.905, 1.907, 2.081, 2.022, 2.163, 1.935 | 0.051509, 0.044304, 0.046930, 0.041059, 0.064687, 0.046717 |
| Single COUNT with HAVING | 1.236, 1.149, 1.077, 1.092, 1.211, 1.064 | 0.011466, 0.011426, 0.012189, 0.016121, 0.012406, 0.011590 |

This is a fresh run through the documented harness, replacing the preliminary
ad hoc timing figures. It is a different sample set; the two runs are not pooled,
and earlier, lower timings are not substituted into this report.

## Separate memory experiment

The [memory artifact](../../tests/artifacts/perf/issue-530-20261028/memory.json)
contains three fresh-child `wait4` measurements per engine and query, collected
separately at approximately 20:14 UTC. Each child executes one query without a
warmup. This is **whole child-lifetime maximum RSS**, including fork, pre-exec
Python-parent memory and executable startup. It is not statement-only heap use.
In particular, stock's identical 17,908 KiB readings reflect a measurement floor
and do not determine its query working set.

| Case | v0.4.12 samples (KiB) | v0.4.12 median (MiB) | Stock samples (KiB) |
|---|---|---:|---|
| Single COUNT | 18684, 18748, 18748 | 18.31 | 17908, 17908, 17908 |
| Cartesian COUNT | 1369104, 1368980, 1369108 | 1337.02 | 17908, 17908, 17908 |
| Single SUM | 18748, 18748, 18748 | 18.31 | 17908, 17908, 17908 |
| Cartesian SUM | 1369108, 1369108, 1369104 | 1337.02 | 17908, 17908, 17908 |
| Single COUNT with HAVING | 754580, 754580, 754580 | 736.89 | 17908, 17908, 17908 |

The approximately 1.306 GiB Cartesian peaks and 737 MiB HAVING peak are far above
the startup floor, despite the final result containing one scalar. Together with
the source route below, this makes removing intermediate materialization a
justified first optimization target. RSS does not identify an allocation count,
exclusive ownership, or the percentage of CPU spent allocating and copying.

## Execution route and profiling limits

The relevant path is present in both
[the immutable v0.4.12 source](https://github.com/Dicklesworthstone/frankensqlite/blob/26aa19afcbd5afdb01658a10bb33417ad0b513be/crates/fsqlite-core/src/connection.rs)
and inspected main revision `e3b14bbd33017588dbb0614b98d99ecf27e8a26e`:

1. `try_execute_streaming_table_function_aggregate` folds a qualifying single
   table-function cursor directly into aggregate accumulators. It requires
   `having: None` and rejects a nonempty join list.
2. `execute_group_by_join_select_inner` otherwise removes GROUP BY/HAVING from
   an internal raw-row SELECT, executes that SELECT into `join_rows`, and then
   performs the general aggregation. The single-series HAVING diagnostic enters
   this fallback too, although HAVING itself is evaluated after aggregation.
3. In v0.4.12, `select_join_is_vdbe_eligible` explicitly excludes table-function
   sources. The issue's Cartesian queries therefore use the connection-level
   join executor, not the ordinary-table VDBE join program.
4. `execute_single_join` builds each Cartesian row in a scratch vector and
   clones that vector into the full result. The aggregate subsequently consumes
   the materialized million-row product. This introduces work and memory
   proportional to the intermediate result even for COUNT.

The initial `series_scan.rs` inspection was a useful entry point, but these
results do not establish its virtual-table filter or stepping logic as the
dominant problem. COUNT's penalty exists without a SUM expression, and HAVING
recovers much of the penalty without any Cartesian loop. General materialization
and aggregate routing are the strongest supported targets; the data do not
separate every expression-evaluation, copying and allocation cost.

Plans are retained in the timing JSON. Release EQP for the product reports only
`SCAN a VIRTUAL TABLE INDEX 1:`; full EXPLAIN fails with
`not implemented: non-table FROM source`. This incomplete diagnostic is not a
faithful description of both sources. Stock EQP reports both scans; its full
EXPLAIN contains nested `VFilter`/`VNext` loops around `AggStep` and `AggFinal`.
The harness records plan errors separately and still requires every actual
query to succeed with the expected result.

Separate software `cpu-clock:u` profiles at 499 Hz collected 1,393 samples for
three release Cartesian COUNT executions, 1,413 for thirty release single COUNT
executions, and 1,229 for two hundred stock Cartesian COUNT executions, with zero
lost samples. Profile stdout was checked against the expected scalar for every
execution. The release binary is static and stripped, so its addresses could not
be attributed to FrankenSQLite functions. Hardware cycle/instruction events were
unavailable. These user-CPU, whole-process profiles exclude kernel CPU and do not
establish a resolved FrankenSQLite hotspot or justify assigning CPU percentages
to its allocator, evaluator or virtual-table code. Retained text reports and SQL:
[release Cartesian](../../tests/artifacts/perf/issue-530-20261028/profiles/released-cartesian-count.perf.txt),
[release single](../../tests/artifacts/perf/issue-530-20261028/profiles/released-single-count.perf.txt),
[stock Cartesian](../../tests/artifacts/perf/issue-530-20261028/profiles/stock-cartesian-count.perf.txt).
Stock's flat samples attributed 54.84% to `sqlite3VdbeExec`, 21.97% to `seriesNext`,
10.41% to `countStep`, 6.83% to `sqlite3VtabImportErrmsg` and 5.21% to `seriesEof`.

## Provenance and reproduction

The worker ran Linux 6.18.55, x86_64, under KVM on an AMD EPYC 9V74, with nine
visible vCPUs, an eight-CPU cgroup quota and an 8 GiB memory limit. The release is
the official portable Linux amd64 v0.4.12 asset, source commit
`26aa19afcbd5afdb01658a10bb33417ad0b513be`; it was not rebuilt locally. Stock is
SQLite 3.53.2, source ID
`d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24`,
dated 2026-06-03 19:12:13, built with GCC 13.3.0 using the flags below. Its shell
contains the native upstream `generate_series`, rather than a substitute generator.

SHA-256 identities:

| Artifact | SHA-256 |
|---|---|
| Official v0.4.12 archive | `12dcedd3d8c15bbac7d67ea83a25926b96000cad82e04a224112202f4224242b` |
| Extracted `fsqlite` binary | `71259ce7c788fbf227f58dab7f6696c0c58d62b1637e1e789c86e696653c4543` |
| Official SQLite amalgamation ZIP | `8a310d0a16c7a90cacd4c884e70faa51c902afed2a89f63aaa0126ab83558a32` |
| Locally built `sqlite3` binary | `1977c14d2e533fbde01a69ea8ec79ac03eb5bd233f5d3bc148f0bcb89ab3c5d9` |

Run from the repository root. The local stock executable hash may differ with a
different C compiler or toolchain; the harness records the actual binary used.

```bash
issue530_dir=$(mktemp -d /tmp/issue530-reproduction.XXXXXX)
curl -fL -A 'frankensqlite-agent (+https://github.com/Dicklesworthstone/frankensqlite)' \
  https://github.com/Dicklesworthstone/frankensqlite/releases/download/v0.4.12/fsqlite-0.4.12-linux_amd64.tar.gz \
  -o "$issue530_dir/fsqlite.tar.gz"
printf '%s  %s\n' \
  12dcedd3d8c15bbac7d67ea83a25926b96000cad82e04a224112202f4224242b \
  "$issue530_dir/fsqlite.tar.gz" | sha256sum -c -
tar -xzf "$issue530_dir/fsqlite.tar.gz" -C "$issue530_dir"
curl -fL -A 'frankensqlite-agent (+https://github.com/Dicklesworthstone/frankensqlite)' \
  https://www.sqlite.org/2026/sqlite-amalgamation-3530200.zip \
  -o "$issue530_dir/sqlite.zip"
printf '%s  %s\n' \
  8a310d0a16c7a90cacd4c884e70faa51c902afed2a89f63aaa0126ab83558a32 \
  "$issue530_dir/sqlite.zip" | sha256sum -c -
unzip -q "$issue530_dir/sqlite.zip" -d "$issue530_dir"
cc -O3 -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_ENABLE_FTS5 \
  -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_ENABLE_MATH_FUNCTIONS \
  "$issue530_dir/sqlite-amalgamation-3530200/shell.c" \
  "$issue530_dir/sqlite-amalgamation-3530200/sqlite3.c" \
  -lpthread -ldl -lm -o "$issue530_dir/sqlite3"
python3 scripts/benchmark_issue_530.py \
  --release-fsqlite "$issue530_dir/fsqlite" \
  --release-revision 26aa19afcbd5afdb01658a10bb33417ad0b513be \
  --sqlite "$issue530_dir/sqlite3" \
  --stock-revision d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24 \
  --build-note 'Official portable v0.4.12; stock GCC 13.3.0 -O3, flags above; release/stock only' \
  --case single_count --case cartesian_count --case single_sum --case cartesian_sum \
  --case single_count_having_fallback --repeats 6 --warmups 1 \
  --work-dir "$issue530_dir/logs" --output "$issue530_dir/release-stock.json"
```

Omitting `--case` runs all sixteen cases. Additional controls cover expression
cost, asymmetric and three-way products, WHERE, ordinary tables and JSON table
functions. The harness validates results, retains binary hashes, raw timers,
plans and logs, and writes workload-only `profile-*.sql` files. Unsupported plans
are retained as diagnostics; bad answers, CLI errors or invalid timers fail the
measurement. Existing result files are not overwritten.

For a separate software CPU profile, the following uses the same recording
options as the investigation: `cpu-clock:u`, 499 Hz, DWARF call graphs with the
default stack size. An installed `perf` with these options is required. The
retained Cartesian SQL repeats the exact COUNT three times:

```bash
perf record -e cpu-clock:u -F 499 --call-graph dwarf \
  -o "$issue530_dir/released-cartesian-count.perf.data" -- \
  "$issue530_dir/fsqlite" -batch -init /dev/null :memory: \
  < tests/artifacts/perf/issue-530-20261028/profiles/released-cartesian-count.sql \
  > "$issue530_dir/released-cartesian-count.stdout" \
  2> "$issue530_dir/released-cartesian-count.stderr"
perf report -i "$issue530_dir/released-cartesian-count.perf.data" \
  --stdio --no-children --percent-limit 1 \
  > "$issue530_dir/released-cartesian-count.perf.txt"
```

Use `released-single-count.sql` with `fsqlite` for the thirty-query control and
`stock-cartesian-count.sql` with `sqlite3` for the two-hundred-query stock profile;
their flat reports additionally use `-g none`. Do not mix profiling with the
uninstrumented timing run. The distributed stripped release still cannot provide
resolved FrankenSQLite function attribution.

To reproduce the separate Linux lifetime-RSS experiment after the commands above,
run this collector. Its pre-exec parent-memory floor can differ from this run:

```bash
python3 - "$issue530_dir" <<'PY'
import json, os, runpy, statistics, sys, tempfile
from pathlib import Path

root = Path(sys.argv[1]).resolve()
ns = runpy.run_path('scripts/benchmark_issue_530.py')
logs = Path(tempfile.mkdtemp(prefix='rss-', dir=root))
names = ('single_count', 'cartesian_count', 'single_sum', 'cartesian_sum',
         'single_count_having_fallback')
engines = {'released_0_4_12': root / 'fsqlite', 'stock': root / 'sqlite3'}
report = {'scope': 'wait4 fresh-child lifetime RSS, including fork/pre-exec/startup; no warmup',
          'results': {}}
for name in names:
    sql, expected, setup = ns['CASES'][name]
    query = logs / (name + '.sql')
    query.write_text('\n'.join([*ns['PREFIX'], *setup, sql, '']))
    report['results'][name] = {}
    for engine, binary in engines.items():
        samples = []
        for repetition in range(3):
            stem = logs / f'{name}-{engine}-{repetition}'
            stdout, stderr = Path(str(stem) + '.stdout'), Path(str(stem) + '.stderr')
            with query.open() as inp, stdout.open('w') as out, stderr.open('w') as err:
                pid = os.fork()
                if pid == 0:
                    for fd, stream in enumerate((inp, out, err)):
                        os.dup2(stream.fileno(), fd)
                    os.execv(str(binary), [str(binary), '-batch', '-init', '/dev/null', ':memory:'])
                _, status, usage = os.wait4(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0, stderr.read_text()
            assert not stderr.read_text(), stderr.read_text()
            assert stdout.read_text().strip() == str(expected), stdout.read_text()
            samples.append({'max_rss_kib': usage.ru_maxrss,
                            'user_seconds': usage.ru_utime, 'system_seconds': usage.ru_stime})
        report['results'][name][engine] = {
            'samples': samples,
            'median_max_rss_kib': statistics.median(s['max_rss_kib'] for s in samples)}
with (root / 'memory-reproduced.json').open('x') as out:
    json.dump(report, out, indent=2)
    out.write('\n')
PY
```

## Validation status and next implementation step

The Python runner passed syntax checks and all six
[protocol regression tests](../../scripts/tests/test_benchmark_issue_530.py),
run with `python3 -m unittest discover -s scripts/tests -p 'test_benchmark_issue_530.py' -v`.
These cover argument modes, malformed results/timers and timeout-log preservation.
All sixteen SQL
cases were checked against native stock SQLite without timing; the five cases in
this report then completed the paired release/stock run, with all sixty measured
queries and sixty warmups validated. These checks validate the harness and the
released executables' results, not a new engine implementation.

The local candidate extends existing streaming aggregation to independent,
unfiltered Cartesian products of trusted built-in table functions. It scans each
input once, reuses one combined row and folds that row directly into the existing
accumulators. It does not use a `generate_series` formula or multiply row counts
as a replacement for SQL evaluation. A safe implementation must preserve custom
module/function registration, argument evaluation and error order, empty inputs,
hidden columns/rowids, aggregate overflow and NULL/storage-class behavior.
Predicates, lateral/outer/constrained joins, custom registry behavior and other
unsupported aggregate forms should retain the established fallback until their
semantics are covered. The HAVING query remains a diagnostic fallback control.

Rust regression drafts cover the exact SQL, asymmetric/three-way products,
parameters, empty sources and errors, once-per-source argument evaluation,
custom module/aggregate/collation behavior, predicate short-circuiting, internal
LIMIT/OFFSET and typed grouped-extrema representatives. `git diff --check` passed.
Direct formatting checks of the two edited integration-test files passed with
`rustfmt --edition 2024 --config-path /dev/null --check`; this is not a successful
workspace or whole-engine formatting check. Workspace `cargo fmt --check` found
unrelated existing diffs.
Build/check attempts were stopped while compiling `asupersync`: the final bounded
`cargo check --locked -p fsqlite-core --lib` reached 6,674,664 KiB of compiler
process-group RSS before the guard stopped it under the 8 GiB worker limit.
Consequently the draft has no completed Rust compile, test or Clippy validation.

On a worker able to build both revisions, first run the relevant regressions and
required repository checks, then use the harness's three-engine mode with
identical Rust build flags/features for the baseline and candidate:

```bash
cargo test --locked -p fsqlite-core \
  --test generate_series_constraints --test join_aggregate_lookup_oracle
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
python3 scripts/benchmark_issue_530.py \
  --baseline-fsqlite /path/to/baseline/fsqlite --baseline-revision BASELINE_SHA \
  --fsqlite /path/to/candidate/fsqlite --candidate-revision CANDIDATE_SHA \
  --sqlite /path/to/native/sqlite3 --stock-revision STOCK_SOURCE_ID \
  --build-note 'Record exact common Rust profile, flags, features, compiler and stock build' \
  --repeats 6 --warmups 1 \
  --work-dir /path/to/new/logs --output /path/to/new/paired-result.json
```

Use a symbolized candidate for CPU attribution and repeat the memory experiment.
Until those checks and measurements succeed, the candidate is unverified and
the performance issue remains unresolved.

## Implementation follow-up: preserved candidate, native validation blocked

The streaming engine optimization is **still not active on main**. A complete
implementation and ten new SQL regression tests are now retained as
[candidate.patch](../../tests/artifacts/perf/issue-530-streaming-candidate/candidate.patch),
so the next sufficiently provisioned worker does not have to reconstruct the
uncommitted work. The patch is based on `b4155a06947e936877b2edb699b8e9c448a03283`;
the four affected Rust files are identical in its parent baseline
`eb7e6964bce773e99913f54c28232fcd432303b4`. `git apply --check` against that immutable
baseline passed. Patch SHA-256:
`600c2bc1096b39da2d5611843f588c2b08453eb7a6a3d428ee5f7edbf856e43c`.

The candidate scans each admitted independent table function once in FROM order,
retains those input rows, and enumerates their product through one reusable row
buffer into borrowed-row aggregate accumulators. Retained intermediate storage
is proportional to the sum of source rows and the number of aggregate groups,
rather than the product cardinality. This is an algorithmic property of the
candidate, **not an executed memory or speedup result**. There is no series
formula or multiplication of source cardinalities in place of tuple evaluation.

Admission requires unchanged built-in module factories and verified source
layouts, including hidden columns and rowids. Unsupported predicates, HAVING,
outer/constrained/lateral/custom sources, application functions, non-BINARY
comparison, DISTINCT, attached LIMIT/OFFSET, and projected grouping keys with
MIN/MAX retain the general executor. A structural expression check excludes
evaluation forms whose semantics the streaming lane cannot prove. Every input
is scanned before an empty product is finalized, preserving later scan errors.

The candidate also replaces the older duplicated streaming SUM/AVG states with
the canonical `SumFunc`/`AvgFunc` implementations. The new SQL regressions cover
exact integer prefixes beyond 2^53, mixed REAL/integer overflow, NULLs and
infinities, in addition to the original million-row SQL, product shapes,
parameters, grouping representatives, custom registrations, predicate fallbacks
and internal LIMIT/OFFSET. A custom table-function callback regression detects
incorrect column pruning that would suppress a registry-changing callback.
These new SQL regressions have **not executed**.

### Executed validation and concrete blockers

| Check | Observed outcome |
|---|---|
| Python benchmark protocol suite | 14 passed, including actual child processes, PTY flushing, strict result checks, missing counters, bounded IO, and failed-run evidence |
| Canonical aggregate unit tests | [69 passed, zero failed, one existing performance test ignored](../../tests/artifacts/perf/issue-530-streaming-candidate/candidate-agg-builtins-tests.log); this does not compile or exercise the connection-level change |
| Immutable baseline native CLI build | [SIGKILL while compiling `asupersync`](../../tests/artifacts/perf/issue-530-streaming-candidate/baseline-build-low-memory.log), before engine compilation |
| Candidate `cargo check -p fsqlite-core --lib` | [SIGKILL while checking `asupersync`](../../tests/artifacts/perf/issue-530-streaming-candidate/candidate-core-check.log), before checking the changed connection code |
| Focused function-crate Clippy | [Blocked by an existing `fsqlite-error` lint and an older-toolchain unknown lint](../../tests/artifacts/perf/issue-530-streaming-candidate/candidate-func-clippy.log); no lint allowance was changed |
| Configured workspace formatting | [Existing differences elsewhere](../../tests/artifacts/perf/issue-530-streaming-candidate/workspace-fmt-check.log); the repository configuration excludes the large core/pager/VDBE files |
| Source whitespace and patch application | `git diff --check` and baseline `git apply --check` passed |
| New SQL integration tests, full workspace check/Clippy, baseline/candidate benchmark | **Not completed**; no baseline or candidate native executable was produced |

The worker again has an 8 GiB cgroup limit. Other concurrent work occupies
compiler memory and several GiB of RAM-backed files; the disk also repeatedly
ran out of unreserved space. Only this task's files were moved, reversibly, to
manage space. Other tasks' jobs and files were left untouched. No usable remote
compilation configuration or runner was found. The repository documents Actions
as disabled; inspected historical verification runs remain queued. Actions was
not enabled or reconfigured.

The available toolchain was `nightly-2026-07-06`, rustc
`1.99.0-nightly (3659db0d3 2026-07-05)`, rather than the repository's pinned
`nightly-2026-08-31`. The native attempts preserved default features and locked
dependencies, using the development profile with `CARGO_BUILD_JOBS=1`,
`CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_DEV_CODEGEN_UNITS=256`, `CARGO_PROFILE_DEV_LTO=false`, and
`RUSTFLAGS='-Zthreads=1 -Cembed-bitcode=no'`. The final attempts additionally used
Cargo's `-Zprofile-rustflags` with package-specific asupersync flags
`-Zno-parallel-backend -Zmir-opt-level=0`. Neither those settings nor the narrower
check completed the dependency build. No dependency source, feature set, SQL
assertion, concurrency default, or acceptance gate was weakened.

### Reusable process-memory retention measurement

Commit `b4155a06947e936877b2edb699b8e9c448a03283` adds `--memory` to the existing
benchmark. This is a separate mode: native wall/CPU timing samples remain
uninstrumented. Each fresh child pauses after setup and after each checked
workload result. Linux `/proc/<pid>/status` supplies VmRSS and VmHWM while that
same child waits for more input. Reports distinguish first-query high-water
from RSS retained across subsequent queries, retain raw checkpoints and failures,
and never substitute zero for an unavailable counter. Values include process
state, allocator arenas and engine caches; they are not isolated query heap or
proof of a leak. The first-query VmHWM includes startup/setup but has no
pre-exec Python-parent RSS floor.

The collector passed an actual native SQLite 3.53.2 smoke run over Cartesian
COUNT/SUM, the HAVING diagnostic, ordinary-table COUNT and JSON SUM, checking
three executions of each query. This validates the collector protocol, not a
candidate engine. This worker's sandbox PID namespace does not match its `/proc`
mount, so the initial sandbox memory test correctly failed on missing process
status; the passing protocol and native collector runs used matching namespaces.

A subsequent release/stock retention experiment completed seven cases, three
fresh processes per engine/case and three checked executions per process:
**126 validated workload results**. The [raw report](../../tests/artifacts/perf/issue-530-streaming-candidate/release-stock-memory.json)
contains every checkpoint and both binary hashes; the
[command record](../../tests/artifacts/perf/issue-530-streaming-candidate/release-stock-memory-command.json)
records the collector hash, exact options and resource guard, and the
[log archive](../../tests/artifacts/perf/issue-530-streaming-candidate/release-stock-memory-logs.tar.gz)
retains scripts, outputs and per-process checkpoint files. The guard never fired.
The run used the verified official v0.4.12 binary and native SQLite 3.53.2 built
with GCC 13.3.0 and the investigation's flags. Medians below are **MiB**:

| Case | v0.4.12 first-query HWM | v0.4.12 RSS after query 3 | Stock first-query HWM |
|---|---:|---:|---:|
| Single COUNT | 12.61 | 12.61 | 2.67 |
| Cartesian COUNT | 1331.76 | 16.24 | 2.60 |
| Single SUM | 12.61 | 12.61 | 2.65 |
| Cartesian SUM | 1331.75 | 16.24 | 2.65 |
| Single COUNT with HAVING | 731.44 | 16.33 | 2.65 |
| JSON Cartesian COUNT | 2601.48 | 16.32 | 2.71 |
| JSON Cartesian SUM | 2601.62 | 16.34 | 2.70 |

For every release case, median RSS growth from query 1's completed checkpoint to
query 3's completed checkpoint was zero. The large product peaks are therefore
transient in these repetitions, rather than retained GiB growth. This does not
rule out retention over other workloads or longer runs. The different measurement
scope explains why these figures should not be pooled with the earlier wait4
measurements. JSON's roughly 2.54 GiB peaks also demonstrate that the materializing
path affects table functions beyond generate_series. These are **released versus
stock observations only**, not measurements of main or the preserved candidate.
Fresh wall/CPU comparisons were not rerun while another task was consuming the
shared worker's CPU; the earlier timing evidence remains separate.

On a worker where the locked native build succeeds, apply the retained patch,
run the two integration targets above and the GH#436 streaming-function guard,
then complete the required workspace gates. Build baseline and candidate with
identical settings and retain their source/binary identities. Run the existing
three-engine timing command, followed separately by the retention experiment:

```bash
python3 scripts/benchmark_issue_530.py \
  --baseline-fsqlite /path/to/baseline/fsqlite --baseline-revision BASELINE_SHA \
  --fsqlite /path/to/candidate/fsqlite --candidate-revision CANDIDATE_SHA \
  --sqlite /path/to/native/sqlite3 --stock-revision STOCK_SOURCE_ID \
  --build-note 'Exact matching build settings and toolchains' \
  --memory --memory-queries 5 --repeats 3 \
  --work-dir /path/to/new/memory-logs --output /path/to/new/memory-result.json
```

The retained candidate must not be described as landed, validated, or faster
until the native compile, SQL regressions and paired measurements succeed.
