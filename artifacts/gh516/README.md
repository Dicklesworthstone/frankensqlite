# GH516: compiled trigger-aware INSERT row execution

This directory contains an executable **candidate**, not a claim that the live
engine is already optimized. The native gate measures the unchanged engine,
applies the source transformation, runs conformance/lint/format gates, measures
the candidate, and stages a source commit only if every required gate passes.
The workflow never updates a branch. A validated source commit can be promoted
to main with a non-forced, expected-parent Git update after reviewing its receipt.

## Execution change

`apply_candidate.py` extracts the existing INSERT arm from `connection.rs` into
`connection/insert_batch.rs`. It preserves the arm's operations and ordering;
the only change inside that shared kernel is a binding-identity check before
using bytecode compiled at batch entry. Ordinary dispatch calls the same kernel.

For admitted multi-row VALUES statements, `InsertSelectReplayEmitter` owns one
parameterized row statement and one compiled VDBE program. Each row enters the
shared INSERT kernel rather than the public statement pipeline. This removes
per-row AST rewriting, statement setup, program cache lookup, and synthetic SQL
trace events. It does not skip or hoist WHEN evaluation. BEFORE triggers, the
storage write, FK enforcement, and AFTER triggers remain ordered per row.

The enclosing stream still owns the savepoint, final-image FK validation scope,
FK cache lifetime, and affected-row aggregation. The row entry retains conflict
inheritance, write-buffer flushes, failure bookkeeping, and rollback handling.
Schema/function/transaction/encoding identity is checked at row entry **and
after BEFORE triggers**, before bytecode use. A stale identity uses existing
statement/compilation paths instead of stale bytecode.

Admission is deliberately bounded: ordinary VALUES batches with INSERT triggers
and an existing statement savepoint. SELECT streams, UPSERT, RETURNING, explicit
OR FAIL/OR ROLLBACK, virtual tables, and attached/TEMP ownership retain their
established paths. Triggers within admitted statements may still raise IGNORE,
ABORT, FAIL, or ROLLBACK; those outcomes are in the differential corpus.

## Evidence, not assumed performance

`build_scenarios.py` generates 53 scenarios / 326 SQL steps. The native tests run
them against separate FrankenSQLite and bundled stock SQLite connections in
memory and WAL mode, comparing ordered values, storage classes, error codes,
changes, trigger side effects, rollback state, integrity, and reopened data.
A separate test covers numbered parameter reuse; another rejects the synthetic
8-row/9-statement replay behavior for inactive BEFORE and AFTER triggers.

The ignored release experiment measures 8/64/256-row batches, memory and WAL,
no-trigger controls, inactive BEFORE/AFTER triggers, and active AFTER triggers.
Each case has two warmups and seven retained samples. Setup, commit, checks,
and tracing are outside the timed region; inserted and audit counts are checked.
The same fixture and release configuration run before and after the source edit.

Promotion requires at least 1.15x geometric-mean inactive-trigger throughput,
both raw and normalized to no-trigger controls, with no case below 0.90x.
These are acceptance criteria, **not measured results**. Actual results and all
samples appear only in the native run's `performance.json` and logs.

Run from a clean checkout with the pinned Rust toolchain:

```sh
python3 artifacts/gh516/run_native_validation.py --results /tmp/gh516-native
```

The transform is pinned to connection blob
`b3903e679debd22edaa305b77d8f82200ca16d1c` and refuses changed input, duplicate
application, missing anchors, and unexpected existing output files. It must be
reviewed/rebased after another edit to that file. Failed gates never emit a
validated receipt. The workflow verifies SHA-1/SHA-256 of the exact tested
files before creating a staged commit; it never creates a pull request.
