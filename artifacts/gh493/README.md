# GH493 pager-backed CTE candidate — NOT INTEGRATED

This directory contains implementation code and an integration patch generator,
not an active engine fix. `crates/fsqlite-core/src/connection.rs` remains unchanged.
Do not close #493 or report a time/RSS improvement based on this directory.

The reviewed production source is Git blob
`74e4bb5ee503fd40e0104dbbaa5dadabdef90777`, still present at main commit
`79cd4a0a4844f1bcca75e53e30fa1f79d58d64d1`. The generator refuses a different source
blob, missing/duplicate edit anchors, or existing new-file destinations. It only
prints a normal unified diff; it is not a Cargo build hook and never rewrites
source at build time.

## Implementation

`build_candidate_patch.py` carries 30 integration edits; `cte_storage.rs` is the
new connection submodule. Together they propose:

* Explicit statement-owned MemDB roots, allocated from the existing high,
  descending TEMP-storage namespace but never entered in the TEMP name catalog.
  Compiled programs route those exact roots through database 1. Interpreted joins
  admit those rows independently of persistent mirror validity.
* A scoped schema-only WITH policy that suppresses bulk hydration through the
  entire consumer, including attached-child delegation. RAII restores policy on
  success, error, or future drop. Ordinary-open and actual historical-snapshot
  behavior retain their existing policy.
* Schema refresh preservation of live materialized roots, with binding order
  intact, without clearing dirty flags to pretend that an unloaded mirror is
  current. TEMP tables beneath CTE name shadows are captured separately.
* No global time-travel override for bounded mixed CTE/persistent reads. Catalog
  rows inside a bounded WITH receive explicit local roots too, so joining a
  catalog to a real table does not make empty persistent placeholders authoritative.
* Normal child SELECT dispatch when an attached-only consumer provably does not
  reference local CTEs. Existing attached DML conflict, RETURNING, change-count,
  and transaction handlers are retained. Imported roots acquire cleanup ownership
  during installation, not only after all tables have been installed.

`gh493_pager_contract.rs` adds seven integration tests (including a 14-query
stock-SQLite comparison matrix). The submodule adds an eighth test inspecting
actual child hydration and policy restoration. Coverage includes direct/prepared
reads, named and nested CTEs, recursion through persistent tables, UNION semantics,
materialization hints, grouped/window/catalog queries, TEMP/IPK shadows, error
cleanup, own writes, savepoints, WAL reader visibility, attached INSERT/UPDATE/
DELETE with UPSERT and RETURNING, and strict fallback refusal.

These tests become nonignored Rust targets only after integration. They have NOT
been compiled or run against FrankenSQLite. In particular, source review and
reference SQL checks do not establish mixed-cursor correctness, cancellation
safety, transaction preservation, or the absence of regressions.

## Integration and native validation still required

Use a clean checkout of the pinned source, with the repository's nightly Rust
and locked dependencies. First collect the incumbent with the existing isolated
profile on the same machine and build configuration:

```sh
GH493_EXPECT=baseline cargo test --release --locked -p fsqlite-core \
  --no-default-features --features native,ext-json \
  --test gh493_schema_only_cte gh493_isolated_profile -- \
  --ignored --exact --nocapture --test-threads=1

python3 artifacts/gh493/build_candidate_patch.py > /tmp/gh493.patch
git apply --check /tmp/gh493.patch
git apply /tmp/gh493.patch

cargo test --locked -p fsqlite-core --no-default-features --features native,ext-json \
  --test gh493_pager_contract -- --nocapture --test-threads=1
cargo test --locked -p fsqlite-core --no-default-features --features native,ext-json \
  --lib bounded_cte_delegation_preserves_child_policy_and_zero_hydration -- --nocapture
cargo test --locked -p fsqlite-core --no-default-features --features native,ext-json --lib cte
cargo test --locked -p fsqlite-core --no-default-features --features native,ext-json --lib attach
cargo test --locked -p fsqlite-core --no-default-features --features native,ext-json --lib temp

GH493_EXPECT=bounded cargo test --release --locked -p fsqlite-core \
  --no-default-features --features native,ext-json \
  --test gh493_schema_only_cte gh493_isolated_profile -- \
  --ignored --exact --nocapture --test-threads=1
```

Record test counts and failures, not just exit status; a filter matching zero
tests is not evidence. Run formatting, check, and clippy before publishing the
integrated production change. Preserve both profile logs and compiler/build
provenance. The deterministic requirement is zero persistent-row hydration;
RSS/HWM and elapsed time are supplementary measurements, not substitute gates.

## Evidence available in this session

`check_reference.py` exercised the fixture SQL using CPython SQLite 3.46.1.
`reference-results.json` records 14 shape results, local and attached savepoint
rollback, UPSERT/RETURNING, WAL snapshot visibility, TEMP/IPK shadow expectations,
reopen persistence, and both integrity checks. The fixture was 16,842,752 bytes.
This is SQLite-reference evidence only, not FrankenSQLite execution evidence.

Python syntax checking passed. No full-source patch application, Rust compile,
Rust regression run, or native before/after time and memory measurement was
possible in this session. The container has no cargo/rustc and cannot resolve
GitHub; the connector supports direct-main commits but not applying a partial
edit to the 14 MB connection module from a working file. The production source
and the strict fallback inventory have therefore deliberately not been relabeled
as fixed or certified.
