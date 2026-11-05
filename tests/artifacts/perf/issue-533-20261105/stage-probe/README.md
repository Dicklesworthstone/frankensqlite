# Prepared GH#533 native execution-stage probe — UNEXECUTED

`stage_probe.rs` is one shared source file used verbatim by the baseline and
candidate Cargo targets. It uses only public engine/parser diagnostics and the
existing `futures-lite` executor; it adds no production instrumentation or
`bench-internals`/`test-internals` features. Each manifest requests the default
CLI's effective core features. Direct dependency versions come from the checked
workspace lockfile. This directory is temporary measurement code, not an engine
patch or a release benchmark. The preserved Rust source has not been compiled or
executed in this investigation. The two `preparation.json` files record completed
metadata audits only. Their absolute paths describe the original worker; none is
a required path for reproduction. For portability, the helper discovers Cargo
through `CARGO` or `PATH`. Its build flags reflect the final proposed setup;
the metadata audits do not establish that those builds succeed.

## Portable checkout layout

Copy this directory beside two engine checkouts. The manifests use relative paths:

- `issue533-stage-probe/`: this complete directory, including both arm subdirectories.
- `frankensqlite-baseline/`: the pristine source base chosen for qualification.
- `frankensqlite/`: that same base plus only the candidate changes being qualified.

For example, from an engine repository with the necessary revisions available:

```sh
issue533_work=/absolute/path/with/room/issue533
mkdir -p "$issue533_work"
cp -R tests/artifacts/perf/issue-533-20261105/stage-probe \
  "$issue533_work/issue533-stage-probe"
git worktree add --detach "$issue533_work/frankensqlite-baseline" \
  931a0dd6b31d70d7c5d9fc66c01ce9df9e17b64e
git worktree add --detach "$issue533_work/frankensqlite" \
  931a0dd6b31d70d7c5d9fc66c01ce9df9e17b64e
git -C "$issue533_work/frankensqlite" apply \
  "$(pwd)/tests/artifacts/perf/issue-533-20261105/candidate-unverified.patch"
```

These are future qualification instructions, not commands executed in this
investigation. The archived baseline metadata audit used `dbd27741...`, while the
proposal is based on `931a0dd6...`; the example aligns both bases before testing.
No engine-candidate commit or qualified executable is asserted. Install the
pinned toolchain and its Cranelift component before choosing that backend.
Registry dependencies must already be available locally for the offline commands.

## Prepare without compiling

```sh
python3 "$issue533_work/issue533-stage-probe/probe.py" prepare baseline
python3 "$issue533_work/issue533-stage-probe/probe.py" prepare candidate
```

Preparation runs offline Linux-filtered Cargo metadata and permits Cargo to add
the scratch root package/prune unrelated packages from its copied lock. It then
checks every registry `(name, version, source, checksum)` tuple against that
engine checkout's `Cargo.lock`; any new registry version/checksum fails. It also
rejects unintended `test-internals` or `bench-internals` features. The engine lock
is not modified. `preparation.json` records this audit, enabled core/asupersync
features, engine revision/worktree status, source hash and lock hashes.

## Explicit native build

After the main engine build is complete, build each arm sequentially. The
optional low-memory mode matches the parent investigation's third build:
asupersync remains LLVM with the nightly mostly-unused hint; only fsqlite-core
uses Cranelift. The pinned toolchain must already have the Cranelift component.

```sh
python3 "$issue533_work/issue533-stage-probe/probe.py" build baseline --low-memory
python3 "$issue533_work/issue533-stage-probe/probe.py" build candidate --low-memory
```

The script performs the audit and records the exact environment/command, exit
status and successful binary hash in `build-receipt.json`. Its low-memory Cargo
invocation is equivalent to the following (set the appropriate manifest and
source revision for each arm):

```sh
RUSTUP_TOOLCHAIN=nightly-2026-08-31 \
CARGO_TARGET_DIR="$issue533_work/native-target" \
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 \
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_DEV_CODEGEN_UNITS=256 \
RUSTFLAGS='-Zthreads=1 -Zunstable-options --jobs-backend=1 -Cembed-bitcode=no' \
ISSUE533_SOURCE_REVISION=BASELINE_OR_CANDIDATE_REVISION \
cargo -Zcodegen-backend -Zprofile-hint-mostly-unused \
  --config 'profile.dev.package.asupersync.hint-mostly-unused=true' \
  --config 'profile.dev.package.fsqlite-core.codegen-backend="cranelift"' \
  build --locked --offline \
  --manifest-path "$issue533_work/issue533-stage-probe/baseline/Cargo.toml" \
  -j1
```

Both manifests specify dev opt-level 1, debug disabled, incremental disabled,
256 codegen units, and asupersync opt-level 0. Omitting `--low-memory` retains the
same profile and uses the default LLVM backend without the nightly unused-code
hint. Compare arms only when their build receipts show matching options.

## Run the exact reported queries

Use one case per process so `/proc/self/status`'s cumulative `VmHWM` is an
independent process peak. Defaults are 1,000,000 count rows and 10,000 EXISTS rows.
Each process opens a fresh in-memory connection, then resets/enables diagnostics
only around the query. Opening/closing the connection is excluded from query
wall time and execution-stage counters. Every query must return one integer row
of the expected cardinality or the process exits unsuccessfully.

```sh
"$issue533_work/native-target/debug/issue533-stage-probe-baseline" --case direct_count --label baseline
"$issue533_work/native-target/debug/issue533-stage-probe-baseline" --case cte_count --label baseline
"$issue533_work/native-target/debug/issue533-stage-probe-baseline" --case cte_exists --label baseline
"$issue533_work/native-target/debug/issue533-stage-probe-baseline" --case direct_exists --label baseline
```

Repeat with the candidate binary, capture stdout JSONL, and use the existing
native benchmark harness for uninstrumented timing and stock SQLite comparisons.
`--count-rows` and `--exists-rows` provide smaller diagnostic controls; the output
explicitly says whether the reported cardinality was used. `--case all` is a
convenience, but its memory high-water marks accumulate across cases.

The JSON includes query wall time; parser/parse-cache/rewrite/compile counters;
execution setup, body, schema-refresh and MemDB counters; VDBE opcodes, decoded
values, record heap bytes, result rows and sort counters; B-tree copy/record-decode
snapshots; aggregated fallback decisions and the most recent bounded fallback
events. The complete hot-path snapshot is also retained as a diagnostic string.

These scopes overlap, so **do not sum their durations**. Profiling changes work
and can affect execution-lane selection. These results establish stages and
operation counts for this instrumented dev build; they do not establish release
speedups. RSS includes instrumentation and allocator/process overhead, while
`arena_alloc_bytes` describes only the instrumented arena counters. The public
counters do not provide a disjoint CTE-materialization timer or total heap
allocation count. Missing/zero counters must not be interpreted as proof that a
stage did no work.
