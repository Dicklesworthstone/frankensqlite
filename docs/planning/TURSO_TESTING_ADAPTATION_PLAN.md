# Selective Turso Testing Adaptation Plan

Status: proposed (rev 4 — bead traceability and verification contracts hardened 2026-08-03)

Research date: 2026-08-03

FrankenSQLite baseline: local `main` at the start of this investigation

Turso source baseline: commit
[`19d1952c62b17012cba6392e14581b48db05ec1e`](https://github.com/tursodatabase/turso/commit/19d1952c62b17012cba6392e14581b48db05ec1e)
(2026-08-03)

Design comparison source: [May 9, 2026 X post](https://x.com/doodlestein/status/2052922541367291929)

## 1. Decision

Selectively adapt Turso's test-system ideas. Do not port the `testing/`
directory, vendor its runners, or treat its expected results as authoritative
without independent review.

The highest-value additions are:

1. A test-only, typed, capability-aware SQL generator that feeds the existing
   FrankenSQLite differential, replay, minimization, and corpus pipelines.
2. Transaction-history capture plus a serializability oracle for deterministic
   concurrent-writer campaigns. Turso's Elle history work is useful as a
   transport and workload reference, but its snapshot-isolation acceptance
   model is too weak for FrankenSQLite's SSI contract.
3. A small external-oracle lane using SQLancer first, with SQLRight evaluated
   separately, to add generator and oracle independence.
4. Coverage accounting that joins generator capabilities to the existing
   supported-surface and feature ledgers.

The following should not be adopted as-is:

- Turso's Tokio-based `sqltest` runner or any Tokio dependency.
- Turso MVCC assumptions, file formats, lock rules, transaction modes, or
  snapshot-isolation acceptance thresholds.
- A second failure-bundle, replay, seed, corpus, or minimization subsystem.
- Broad copying of Turso test cases without scope, provenance, and value
  classification.
- Product-specific JavaScript, Go, PostgreSQL, cloud, or Turso extension tests
  unless the corresponding FrankenSQLite surface is explicitly in scope.

This is an accretive program only if each adopted piece proves that it finds
new defect classes or materially improves reduction/replay. Test-count growth
is not a success metric.

## 2. Research Basis

### 2.1 Sources read

The assessment used the complete FrankenSQLite `AGENTS.md` and `README.md`, the
current architecture and test infrastructure, the canonical parity contracts,
CI workflows, the X comparison post, and Turso's current `testing/` tree pinned
to the commit above.

Primary Turso sources include:

- [`testing/`](https://github.com/tursodatabase/turso/tree/19d1952c62b17012cba6392e14581b48db05ec1e/testing)
- [`testing/simulator/README.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/simulator/README.md)
- [`testing/simulator/COVERAGE.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/simulator/COVERAGE.md)
- [`testing/concurrent-simulator/README.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/concurrent-simulator/README.md)
- [`testing/concurrent-simulator/properties.rs`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/concurrent-simulator/properties.rs)
- [`testing/concurrent-simulator/elle.rs`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/concurrent-simulator/elle.rs)
- [`testing/concurrent-simulator/yield_injection.rs`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/concurrent-simulator/yield_injection.rs)
- [`testing/differential-oracle/sql_gen`](https://github.com/tursodatabase/turso/tree/19d1952c62b17012cba6392e14581b48db05ec1e/testing/differential-oracle/sql_gen)
- [`testing/sqltest/docs`](https://github.com/tursodatabase/turso/tree/19d1952c62b17012cba6392e14581b48db05ec1e/testing/sqltest/docs)
- [`testing/sqlancer/README.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/sqlancer/README.md)
- [`testing/sqlright/README.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/sqlright/README.md)
- [`testing/antithesis/README.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/testing/antithesis/README.md)
- [`LICENSE.md`](https://github.com/tursodatabase/turso/blob/19d1952c62b17012cba6392e14581b48db05ec1e/LICENSE.md)

The pinned `testing/` tree contains 373 entries across differential generation,
deterministic simulation, concurrent simulation, a declarative SQL runner,
SQLancer, SQLRight, Antithesis, stress tools, CLI/system/conformance tests, and
support code. The count is an inventory fact, not an adoption target.

### 2.2 Time-sensitive interpretation

The X post is a design comparison from 2026-05-09. It is useful for the
project-level distinction between FrankenSQLite's page-level SSI/default
concurrent-writer goal and Turso's then-described logical MVCC/snapshot
isolation work. Turso has changed since that post. Current source is the
authority for what Turso's tests do now; FrankenSQLite's checked-in contracts
are the authority for what FrankenSQLite must prove.

The post also discusses broader semantic intent merging. Current FrankenSQLite
documentation says the safe merge ladder is dormant/test-only and the live path
aborts/retries on same-page drift. Concurrency tests must assert that live
behavior until a separately reviewed implementation and contract change enables
more of the ladder; they must not turn a design aspiration into a current claim.

This plan therefore borrows test methods, not architectural conclusions.

### 2.3 Independent verification record (2026-08-03 review)

A second-agent review re-verified the load-bearing claims in this plan:

- The pinned Turso commit exists (committed 2026-08-03T11:43:52Z) and its
  `testing/` subtree contains exactly 373 entries (GitHub tree API,
  non-truncated listing).
- `testing/sqltest/Cargo.toml` at that commit declares
  `tokio = { workspace = true, features = ["full"] }`; the Tokio exclusion is
  fact-based, not speculative.
- Turso's `LICENSE.md` at that commit is MIT.
- Whopper's documented Elle workflow checks
  `--consistency-models snapshot-isolation`, confirming its acceptance model
  is weaker than FrankenSQLite's SSI contract.
- All four concurrent-mode defaults named in AGENTS.md are `true` on current
  `main`, and no tokio-family crate appears in `Cargo.lock` or any workspace
  `Cargo.toml`. `crates/fsqlite-harness/tests/no_tokio_enforcement.rs` already
  enforces the dependency policy against `cargo metadata`, so the §11 control
  extends an existing gate rather than inventing one.
- The §3.1 infrastructure inventory was checked against the tree; corrections
  from that check are folded into §3.1, §3.2, §5.2, §5.4, §5.5, and §5.7.
- The only existing concurrency beads this plan consumes are `bd-2lt76` and
  `bd-28z4i.5`. A review recap also named `bd-3d5y3` and `bd-p4dcv`, but neither
  is referenced by this plan or its child beads; those names are not plan
  inputs.

## 3. FrankenSQLite Baseline

This is not a greenfield test effort. Existing infrastructure already covers
most of the mechanics that a naive port would duplicate.

### 3.1 Existing assets to extend

| Capability | Existing owner | Consequence for this plan |
|---|---|---|
| Differential execution against C SQLite | `fsqlite-harness::differential_v2` (in-process rusqlite executors) plus `fsqlite-harness::oracle` (external `sqlite3` CLI) and `fsqlite-e2e::comparison` backends | New generators must emit the existing envelope; both reference paths stay available. |
| Seeded workload generation | `fsqlite-e2e::workload` (`WorkloadGenerator`) and the six `oplog` presets | Reuse seed derivation and operation logs. |
| Corpus normalization and coverage | `fsqlite-harness::corpus_ingest` plus `fixture_root_contract` (hash-locked roots) | Imported or generated cases become corpus entries. |
| Replay and failure bundles | `replay_harness`, `replay_triage`, `failure_bundle` (present in both harness and e2e), `fsqlite-e2e::mismatch_artifacts` | Extend bundle schemas; do not fork them. |
| Mismatch minimization | `mismatch_minimizer`; `fsqlite-e2e::comparison::reduce_*` repro reducers | Add structured reducers behind the existing interfaces. |
| Metamorphic testing | `metamorphic` (a registry of exactly 8 rewrite transforms) | Typed generation complements, rather than replaces, transformations. Beyond these rewrites, the only generative SQL today is a self-described scaffold proptest over single-table SELECT (`crates/fsqlite-core/tests/differential_proptest.rs`). |
| Adversarial campaigns | `adversarial_search` | This module attacks the verification gates themselves (mutation-based counterexample search), not SQL; external generators are additional campaign producers, not a replacement for it. |
| Deterministic runtime and faults | `FsLab`/`LabRuntime`, `FaultVfs`, `fault_profiles` | Use asupersync scheduling and VFS faults. Caveat: LabRuntime/DPOR currently drive small hand-written MVCC models, not `fsqlite::Connection`; see §5.4. |
| Coverage-guided fuzzing | `fuzz/` workspace: 5 libfuzzer targets (lexer, SQL parser, expression parser, record round-trip, xor-merge guard), no checked-in corpus | The SQLRight evaluation (§5.6) must beat this baseline; no engine-level (VDBE/btree/pager/WAL) fuzz target exists today. |
| Parity scope | `docs/canonical_parity_contract.md` and the `docs/contracts/*.toml` contracts | Generation is scope-driven and fail-closed. See §5.1 for the canonical-path warning. |
| CI target accounting | current verification workflows plus `fsqlite-harness::lane_selector` (CI lane selection) | New lanes must be discoverable, budgeted, and artifact-producing. |

The current test-realism inventory reports a very large suite spanning unit,
memory-backed, file-backed, end-to-end, and property tests. Its numeric totals
must be refreshed before this campaign uses them as a baseline, and the
recount must separate engine-behavior tests from tracker-metadata tests. As of
this review, 68 of 236 `fsqlite-harness` integration test files exercise
`issues.jsonl`-shaped tracker metadata rather than engine behavior; 64 of those
reference the literal `.beads/issues.jsonl` path, while four use temporary
tracker fixtures. The real differential mass sits in
`crates/fsqlite-e2e/tests/` (214 of 269 files use rusqlite) and
`crates/fsqlite/tests/`. The counts are reproducible with:

```bash
find crates/fsqlite-harness/tests -maxdepth 1 -type f -name '*.rs' | wc -l
rg -l 'issues\.jsonl' crates/fsqlite-harness/tests --glob '*.rs' | wc -l
rg -l '\.beads/issues\.jsonl' crates/fsqlite-harness/tests --glob '*.rs' | wc -l
find crates/fsqlite-e2e/tests -maxdepth 1 -type f -name '*.rs' | wc -l
rg -l 'rusqlite' crates/fsqlite-e2e/tests --glob '*.rs' | wc -l
```

The key point is already verified from the tree: hundreds of integration
targets and mature harness modules exist, so one-file-per-import expansion
would make CI worse.

### 3.2 Architecture-sensitive test requirements

Every new test must declare the execution lane it intends to exercise.
`Connection` currently dispatches work among pager-backed/direct-VDBE and
selected compatibility/fallback paths. A result can be correct while missing
the storage, planner, VDBE, WAL, or MVCC code the test claims to validate.

Substantial machinery for this already exists and must be the foundation, not
duplicated:

- `fsqlite-core::Connection` exposes `PRAGMA fsqlite.backend_kind`,
  `PRAGMA fsqlite.backend_mode`, and `PRAGMA fsqlite.parity_cert_strict`, and
  computes a `backend_identity()` string such as `unix:parity_cert_strict` or
  `memory:fallback_allowed`.
- Strict parity-cert mode (`set_strict_mem_fallback_rejection`) turns any
  in-memory compatibility fallback into a hard error via
  `log_mem_execution_fallback`; non-strict mode logs structured
  `fsqlite.fallback_decision` tracing events carrying `statement_kind`,
  `decision_reason`, `decision_outcome`, and `fallback_boundary`.
- `fsqlite-e2e::fsqlite_executor::configure_connection` already fails closed
  when a file-backed run resolves to the memory backend or is not strict, and
  records a `StorageWiringReport` on every `EngineRunReport`.
- `docs/contracts/fallback_boundary_inventory.toml` is the audited registry of
  every fallback boundary, with runtime `decision_reason` strings
  contract-tested against it.

What is missing — and what bead `.2` delivers — is the finer-grained lane
vocabulary below (planner/VDBE/MVCC/recovery participation, beyond today's
memory-vs-pager and strict-mode distinction) and the propagation of observed
lane evidence into failure bundles and coverage reports. Terminology note:
"lane" in `fsqlite-harness::lane_selector` means CI lane selection, and
`fsqlite-wal` has WAL "lanes"; this plan's term is always *execution-lane
evidence* to avoid collision.

Required lane evidence:

- `sql_result_only`: semantic SQL comparison may use the normal public path.
- `pager_backed_required`: the test fails if it falls back to `MemDatabase` or
  another compatibility-only lane.
- `planner_required`: the test proves planner participation.
- `vdbe_required`: the test proves VDBE bytecode execution.
- `mvcc_required`: concurrent mode remains enabled and page-level MVCC is
  exercised.
- `recovery_required`: the scenario crosses real file/WAL close, crash, and
  reopen boundaries.

The lane identifier and fallback reason belong in failure bundles and coverage
reports. Storage and concurrency campaigns must fail closed when their required
lane was not observed.

### 3.3 Non-negotiable invariants

1. `BEGIN` continues to promote to concurrent mode by default.
2. No SQLite-style global writer serialization is introduced.
3. Async work uses asupersync with explicit `Cx`; Tokio and its ecosystem are
   forbidden even in new first-party test tooling.
4. The current target is SQLite 3.52.0 and encoding 1 (UTF-8).
5. Excluded or partial features remain excluded or partial unless a separate
   implementation decision changes the canonical contract.
6. Test adapters may not silently normalize away type, error, order, or
   transaction-outcome differences.
7. A failed/aborted transaction is part of the concurrency history, not noise.

## 4. Turso Portfolio Portability Matrix

| Turso area | Distinct value | Existing overlap | Decision | Priority |
|---|---|---|---|---|
| `differential-oracle/sql_gen` | Independent typed AST, hard capabilities, weighted policy, generation trace, proptest strategies | Existing differential runner and string/workload generation, but no equivalently broad typed SQL generator was found | Adapt concepts into the existing harness; keep a test-owned AST independent of production AST | P0 |
| `simulator` | Stateful interaction plans, model properties, double-check mode, plan shrink, bugbase, I/O profiles | Strong overlap in `FsLab`, corpus, replay, minimizer, FaultVfs | Adapt stateful-plan and hierarchical reduction ideas only | P0/P1 |
| `concurrent-simulator` (Whopper) | Operation histories, deterministic yield selection, multiprocess restarts, inline invariants, Elle export | Existing seeded concurrent workloads, crash tests, LabRuntime work, pending DPOR bead | Adapt history schema and oracle integration; coordinate with existing DPOR work | P0/P1 |
| Elle integration | Independent anomaly analysis | No named Elle integration found | Pilot serializability checking; never use snapshot isolation as the acceptance model | P0 |
| `sqltest` DSL | Readable cases, isolated DBs, setup reuse, comparison modes, capability annotations, snapshot support | JSON conformance fixtures, Rust integration tests, snapshots, target accounting | Run a bounded format pilot; do not port the Tokio runner | P1, gated |
| SQLancer | Independent query synthesis and metamorphic/query-partitioning oracles | No integration found | Add a pinned, time-bounded external campaign after a provider spike | P1 |
| SQLRight | Coverage-guided mutation plus NoREC/TLP/index oracles | No integration found; high toolchain cost | Separate feasibility and value gate after SQLancer | P2 |
| Antithesis | Deterministic external fault exploration and multiverse debugging | Strong local deterministic/fault infrastructure; service access required | Optional feasibility spike only; local campaign must remain authoritative | P2 |
| `unreliable-libc` | Faults below the VFS abstraction | FaultVfs already covers deterministic logical I/O faults | Unix-native VFS gap experiment, not a default lane | P2 |
| Rust stress/Shuttle | Scheduling perturbation and long-running mixed workloads | asupersync LabRuntime is mandatory and better aligned | Mine workload shapes; reject Shuttle/Tokio runtime code | P1 ideas only |
| CLI tests | End-user shell behavior | FrankenSQLite CLI parity is partial and already tracked | Import only confirmed gaps into the existing CLI harness | P2 |
| system/TCL/large DB fixtures | Broad legacy and file-format cases | SQLite corpus/TCL accounting and generated DB tests already exist | Deduplicate; import only missing, in-scope cases | P2 |
| JS conformance and Go stress | Binding/product coverage | Different product surface | Defer until those APIs are declared targets | Not now |
| Turso extensions and MVCC format tests | Turso-specific behavior | Architectural mismatch | Reject | Never |

## 5. What To Borrow, Precisely

### 5.1 Typed SQL generation

Build a harness-owned generator, not a production parser helper. Using a
separate test AST preserves oracle independence and allows syntactically valid,
schema-aware generation without coupling expected behavior to production AST
logic.

The generator contract should include:

- A deterministic seed and stable derivation algorithm.
- Schema state updated only after an operation is accepted by both engines.
- Hard capabilities derived from canonical support state.
- Weighted profiles for read-only, DML, DDL, transaction-heavy, expression,
  planner, VDBE, and MVCC workloads.
- Maximum AST depth, statement count, row count, value size, and execution
  budget.
- A generation trace listing every selected construct and origin path.
- Feature IDs from `docs/contracts/supported_surface_matrix.toml`,
  `docs/contracts/feature_universe_ledger.toml`, and
  `docs/contracts/parity_taxonomy.toml`. Warning: stale divergent duplicates
  of these files (and of `sqlite_version_contract.toml` and
  `corpus_manifest.toml`) exist at the repository root. The Phase-0 executable
  inventory found no live repository-root consumer: the apparent joins in
  `fixture_root_contract.rs:627` and `feature_coverage_dashboard.rs:776` are
  `cfg(test)` temporary-workspace fixtures, and the remaining bare-name
  references are report metadata or links resolved relative to the canonical
  `docs/contracts/` directory. Authority is nevertheless distributed:
  `canonical_parity_contract.rs` declares the `docs/contracts/` paths for the
  SQLite-version, supported-surface, and feature-universe contracts;
  `fixture_root_contract.rs::DEFAULT_FIXTURE_ROOT_MANIFEST_PATH` declares the
  corpus manifest; and `parity_taxonomy_test.rs` plus
  `scripts/verify_parity_taxonomy.sh` enforce the taxonomy path. The two
  `corpus_manifest.toml` copies already disagree on `content_hash`. Phase 0
  must consolidate this authority, make every root duplicate inert, reject any
  future live root-path read, and add a drift guard before capability mapping
  is allowed to start. A decision record or follow-up link alone does not
  satisfy that gate.
- Required execution lane and forbidden fallbacks.
- Strict result/error/transaction comparison policy.

Do not copy Turso's capability table directly. Generate FrankenSQLite's table
from its canonical contracts or validate a checked-in table against them.
`supported`, `partial`, and `excluded` are distinct states:

- `supported`: eligible for required differential pass/fail coverage.
- `partial`: eligible only in a named profile with explicit expected gaps.
- `excluded`: generator rejects it unless a feature-development campaign opts
  in under a separate bead.

The first profile should deliberately be narrow: core tables, indexes, DML,
expressions, joins, aggregates, subqueries/compound SELECT where declared, and
transactions. DDL churn, partial extensions, window functions, maintenance
PRAGMAs, and recovery sequences enter later profiles.

The existing single-table SELECT proptest scaffold
(`crates/fsqlite-core/tests/differential_proptest.rs`, frankensqlite#86) is
prior art for exactly this idea at miniature scale; the typed generator should
absorb or supersede it rather than leave two partial generators in the tree.
Its comparator's known weakness — treating any both-engines-error pair as
agreement — is precisely what §3.3 invariant 6 forbids here.

### 5.2 Structured SQL reduction

The existing minimizer removes workload statements. Extend it with reducers
that understand generated structure:

1. Remove transactions or statements while preserving setup dependencies.
2. Remove clauses, joins, projections, predicates, order terms, and indexes.
3. Replace expressions with children or typed literals.
4. Reduce table/column cardinality and value sizes.
5. Revalidate the exact SQL result/error mismatch signature after every
   reduction.

The existing failure bundle remains canonical (`failure_bundle` in both
`fsqlite-harness` and `fsqlite-e2e`, plus `fsqlite-e2e::mismatch_artifacts`).
Add original and minimized AST traces, generator profile hash, lane evidence,
environment, and upstream provenance. The `replay_triage` workflow (artifact
manifest → first divergence → replay → minimize → operator report) is the
operator-facing integration point for new reducers. A minimized case must
replay through the public verifier, not only an in-memory reducer callback.
History, schedule, worker, yield, and crash reduction belong to §5.4 because
they require the typed history schema and deterministic runtime adapter; they
must not make this SQL reducer wait for the concurrency stack.

### 5.3 Stateful deterministic simulation

Adapt Turso's interaction-plan concept as a producer for FrankenSQLite's
existing operation log. Each plan maintains independent model state and emits
preconditions, operation, expected state transition, and postconditions.

Initial properties:

- successful committed rows do not disappear absent a modeled delete;
- rollback and savepoint rollback leave no unmodeled effects;
- uniqueness, foreign-key, and index state agree with table state;
- reopen preserves committed state and discards uncommitted state;
- `integrity_check` remains clean where supported;
- differential result and error categories agree under the existing strict
  comparator;
- required execution lane was observed.

FrankenSQLite should not inherit Turso simulator behavior that treats two
arbitrary errors as equivalent. Error-category comparison must be explicit and
unknown pairs must be mismatches requiring classification.

### 5.4 Concurrent histories and serializability

Use a small, typed transaction history independent of tracing text:

- run ID, seed, schedule ID, process/connection/transaction IDs;
- invocation and completion logical times;
- begin mode actually selected;
- reads with observed values or versions;
- writes with keys/pages when observable;
- commit, rollback, cancellation, retry, and conflict outcomes;
- crash/restart/checkpoint events;
- required/observed execution lane;
- final logical state and integrity evidence.

The first oracle should target histories that are cheap to check independently:
read/write registers, append-only lists, bank transfers, unique allocation, and
write-skew patterns. Acceptance is serializability/SSI plus the documented
first-committer-wins behavior, not snapshot isolation.

An Elle adapter is useful only if a pinned checker can evaluate the intended
serializable model and its handling of aborted/unknown transactions is verified
against golden histories. Otherwise, build or reuse a small internal
serialization-graph checker and optionally export Elle EDN as a secondary
diagnostic.

Seed material for that checker already exists and is currently duplicated:
`crates/fsqlite-e2e/tests/bd_2yqp6_6_1_ssi_serialization_anomaly_differential.rs`
carries a hand-rolled `detect_cycle`/`dfs_cycle` conflict-graph checker,
`bd_3plop_5_ssi_serialization_correctness.rs` asserts conflict-graph
acyclicity independently, and `fsqlite-harness::tla` has a model-level
`simulate_ssi_execution`. The history oracle should unify these into one
shared library component instead of adding a fourth copy.

Coordinate schedule exploration with existing beads `bd-2lt76` and
`bd-28z4i.5`. This campaign owns SQL-level history semantics and workload
oracles; those beads own LabRuntime determinism and DPOR machinery. No new
runtime or scheduler should be created here. Two DPOR engines already exist —
asupersync's `DporExplorer` (used by `mvcc_alien_verification.rs`) and the
harness-native trace-monoid DPOR in `fsqlite-harness::tla` — so this campaign
must consume one of them, never add a third.

LabRuntime does not currently schedule the production `fsqlite::Connection`
engine; today it drives small hand-written MVCC models. A narrow child of
`bd-2lt76`, `bd-2lt76.1`, therefore owns the missing test-only bridge that
drives the real pager-backed `Connection` path with an externally supplied
`Cx`, deterministic yield choices, and lane evidence. Turso bead `.8` blocks
on that bridge. Observation-only OS-thread histories remain useful inputs to
the `.7` oracle, but they cannot close `.8` or support deterministic-replay
claims.

Once the history schema, oracle, and production-engine scheduling bridge are
available, a separate history reducer removes transactions, operations,
workers, schedule events, yield choices, and generic crash points while
preserving the exact serializability/crash witness. It replays every candidate
through the public history verifier and canonical failure bundle. Phase 4 may
extend the generic crash-point reducer with process-specific kill/restart
events, but it must not invent another reducer or artifact format.

### 5.5 Multiprocess and fault campaigns

After in-process histories are stable, run the same operation schema across
multiple processes using real files. The base harness already exists:
`crates/fsqlite-e2e/src/bin/swarm_multiprocess.rs` (the canonical
multi-process source of truth per `docs/concurrency-contract.md`) spawns real
child processes against one WAL database and already checks
read-your-own-write, cross-process visibility, WAL shape, and C-SQLite
cross-check invariants with versioned report schemas. This phase extends that
harness with the typed history schema; it does not build a parallel one. Note
also that the C-SQLite-side process executor (`fsqlite-e2e::executor`)
documents its `deterministic`/`barrier` concurrency modes as currently
degrading to `free`, so reference-side schedules are not deterministic today
and history comparison must not assume they are. Start with bounded cases:

- two processes with disjoint-page writes;
- two processes with same-page conflicts;
- process death before/after WAL publication and commit acknowledgement;
- checkpoint racing readers and writers;
- reopen after killed writer;
- cancellation at reserved asupersync yield points.

Fault schedules must be seed-derived and replayable. Prefer FaultVfs and
LabRuntime hooks. A native syscall or allocator-fault lane requires a separate
design review because the workspace forbids unsafe code except in explicitly
allowed boundary crates and because platform-only failure modes can destabilize
merge CI.

### 5.6 External generator diversity

SQLancer is the first external candidate because it provides an independent
generator and well-known SQLite oracles (TLP, NoREC, PQS) without becoming a
Rust runtime dependency. It is a Java tool whose SQLite support runs
in-process over JDBC, so "add a provider" concretely means implementing a
SQLancer database-provider that reaches FrankenSQLite across a process or ABI
boundary — candidate routes are driving the `fsqlite` CLI, a thin socket/pipe
shim, or the optional `fsqlite-c-api` behind a SQLite-compatible JDBC driver
configured to load a replacement native library. Selecting and de-risking that
route is the point of the spike. The provider must:

- invoke FrankenSQLite through a stable CLI or C API boundary;
- pin the SQLancer revision and container/toolchain digest;
- declare supported constructs and expected errors from canonical contracts;
- preserve every statement, seed, oracle, engine build SHA, and timeout;
- deduplicate crashes and semantic mismatches into existing bundles;
- produce one-command local replay;
- distinguish unsupported input, harness error, timeout, crash, and semantic
  mismatch.

SQLRight is evaluated later because AFL/LLVM instrumentation, Linux-only setup,
patch maintenance, and corpus management are materially more expensive. It is
accepted only if a fixed-budget trial finds unique coverage or defects beyond
the native generator and SQLancer.

### 5.7 Declarative case-format pilot

Turso's `.sqltest` format has useful ergonomics: named setup blocks, isolated
databases, exact/error/pattern/unordered results, capability requirements,
backend selection, and plan snapshots. Its implementation depends on Tokio and
duplicates substantial FrankenSQLite infrastructure.

The pilot should therefore test the need before implementing a new parser.
Note that SLT machinery already exists end to end — `fsqlite-harness::oracle`
parses SLT (`parse_slt`, `skipif`/`onlyif`, `rowsort`/`valuesort`) and
`corpus_ingest::ingest_slt_files` ingests it — but the checked-in SLT corpus
is a single ~25-line smoke file (`conformance/slt/smoke/basic.slt`). Growing
that corpus through the existing parser is the zero-new-syntax baseline any
new DSL must beat.

1. Select 20-50 representative existing FrankenSQLite cases that are currently
   verbose or duplicated.
2. Express them in the least new syntax possible, first considering the
   existing JSON conformance format and SLT conventions.
3. Compare reviewability, line count, diagnostics, execution time, target
   accounting, and ability to express lane requirements.
4. Proceed only if the format removes meaningful maintenance cost and remains
   asupersync-native.

No mass conversion is part of this plan. Snapshot output must mask unstable
addresses/IDs and must not normalize meaningful opcode or plan changes.

## 6. Provenance And Intake Policy

Turso is MIT-licensed at the pinned source commit. That permits adaptation but
requires preservation of the copyright and permission notice for copied or
substantial derived portions.

Every imported or derived case must record:

- source repository and pinned commit;
- source path and, where useful, source test name;
- classification: `concept_only`, `translated`, `substantial_derivative`, or
  `verbatim_fixture`;
- applicable upstream license;
- FrankenSQLite surface/feature IDs;
- architectural adaptations made;
- reason the case is non-duplicative;
- reviewer and review date;
- expected update policy.

Default to concept-level reimplementation. If substantial Turso code or fixture
content is copied, add the required notice through a separately reviewed
licensing change. Never mechanically translate the directory.

The intake triage for each candidate is:

1. Is the behavior in FrankenSQLite's declared supported or named partial
   surface?
2. Does an existing test already cover the same defect class and execution
   lane?
3. Is the expected result valid for page-level SSI and default concurrent mode?
4. Can the case use the existing harness and failure bundle?
5. Does it exercise real code rather than a compatibility fallback?
6. Is provenance complete?
7. Is its runtime stable enough for the intended CI tier?

Only candidates answering all required questions proceed.

## 7. Delivery Phases

### Phase 0: Baseline and governance

Deliverables:

- Recompute current test inventory and CI runtime/resource baseline,
  separating engine-behavior tests from tracker-metadata compliance tests
  (§3.1).
- Produce a machine-readable overlap map from Turso areas to FrankenSQLite
  owners, contracts, and existing tests.
- Define provenance schema and licensing decision record.
- Define the lane-evidence vocabulary as an extension of the existing
  parity-cert/fallback machinery (§3.2: `backend_identity`, strict-mode
  rejection, `fsqlite.fallback_decision` events,
  `fallback_boundary_inventory.toml`) and demonstrate the finer lanes the
  dispatcher must additionally expose.
- Inventory the distributed contract-path authorities and hand the exact
  root-path reference list, including the reason each non-live reference is a
  temporary fixture or metadata value, to a dedicated canonicalization task.
- Consolidate the canonical contract path set under `docs/contracts/`, remove
  every live root-path read, and resolve or quarantine the divergent root-level
  duplicates of
  `supported_surface_matrix.toml`, `feature_universe_ledger.toml`,
  `parity_taxonomy.toml`, `sqlite_version_contract.toml`, and
  `corpus_manifest.toml` (§5.1). Add an executable drift guard. File deletion
  requires explicit human approval per AGENTS.md; without that approval, the
  canonicalization bead remains open until the root copies are made inert and
  mechanically prevented from drifting.
- Record explicit non-goals and owners.

Exit gate:

- Every proposed later deliverable has a non-duplicative owner and measurable
  baseline.
- No source copying has occurred.
- Concurrency work is coordinated with `bd-2lt76` and `bd-28z4i.5`.
- Canonical contract authority is executable rather than documentary: all
  consumers resolve `docs/contracts/`, no live fallback reads a root duplicate,
  and a drift test fails closed. A decision record or linked follow-up alone is
  insufficient.

### Phase 1: Typed differential generator pilot

Deliverables:

- Independent test AST for a narrow supported core.
- Capability/profile mapping validated against canonical contracts.
- Deterministic generation trace and coverage report.
- Adapter into `differential_v2` and corpus intake.
- Strict error/result comparator use.
- Unit tests for generation validity, determinism, caps, and contract drift.

Pilot matrix:

- 100 fixed seeds per profile in presubmit smoke.
- A larger bounded nightly seed set.
- In-memory semantic lane and temporary-file pager-backed lane.
- Ordered and unordered result cases selected from SQL semantics, never by
  blanket sorting.

Exit gate:

- At least 99 percent of generated supported-core cases parse and execute on
  both engines; the remainder is fully classified and does not silently skip.
- Same seed/profile/schema produces byte-identical SQL and trace artifacts.
- At least one demonstrated coverage gap or unique defect/reproducer, or a
  documented stop decision if the generator adds no value.

### Phase 2: Structured SQL reduction and corpus promotion

Deliverables:

- AST/schema/value-aware reducers behind the current minimizer contract.
- Exact SQL result/error signature preservation.
- Bundle schema extensions and stable replay commands.
- Promotion policy from random failure to reviewed regression corpus.
- Regression tests with deliberately reducible synthetic failures.

Exit gate:

- A representative mismatch corpus is reduced materially without signature
  drift.
- Original and minimized cases replay from a clean process.
- Existing bundles remain readable or receive a direct schema migration with
  no compatibility shim.

### Phase 2A: Stateful deterministic operation plans

Deliverables:

- A typed stateful interaction-plan layer extending the existing
  `fsqlite-e2e::workload` and `fsqlite-e2e::oplog` ownership.
- Independent model preconditions, expected transitions, and postconditions
  for bounded supported-core DDL, DML, transactions, savepoints, rollback,
  close/reopen, and integrity checks.
- Strict public-path differential execution using the Phase-1 adapter and the
  Phase-2 SQL reducer, canonical failure bundle, replay, and corpus promotion.
- Unit/property, integration, and fixed-seed E2E coverage for model
  transitions, deterministic generation, error classification, lane evidence,
  reduction, clean-process replay, cancellation, and exhausted budgets.

Exit gate:

- Committed rows do not disappear without a modeled delete; rollback and
  savepoint rollback have no unmodeled effects; table/index/uniqueness/foreign
  key state agrees where supported; and reopen preserves committed state while
  discarding uncommitted state.
- Same seed/profile/schema produces byte-identical plan, SQL, trace, and stable
  artifact metadata from a clean process.
- A deliberately reducible state mismatch preserves its exact result/error/
  model-state signature and required lane through minimization and public
  replay.
- No parallel runner, operation-log format, model coupled to production
  internals, seed scheme, corpus, minimizer, replay path, or failure bundle is
  introduced.

### Phase 3: SQL-level concurrency oracle and deterministic replay

Deliverables:

- Typed operation/history schema.
- Independent model for register, list-append, bank, allocation, and write-skew
  workloads.
- Serializability/SSI checker and first-committer-wins assertions.
- Production-`Connection` LabRuntime bridge owned by `bd-2lt76.1`, preserving
  explicit `Cx`, real pager/MVCC/WAL lane evidence, and default concurrent mode.
- LabRuntime schedule adapter that consumes that bridge and the existing DPOR
  engines.
- History/schedule/worker/crash reducer with exact witness preservation.
- Golden valid and invalid history fixtures.

Exit gate:

- The oracle rejects known G1/G2/write-skew/lost-update fixtures and accepts
  known serializable fixtures.
- Aborted, cancelled, timed-out, and indeterminate transactions have explicit
  semantics.
- The production `Connection`, not only a hand-written model, is scheduled by
  LabRuntime; observation-only OS-thread histories do not satisfy this gate.
- Failures emit deterministic seed, schedule, original and minimized history,
  serialization witness, lane evidence, and replay command.
- Concurrent mode is asserted true at setup and after reopen.

### Phase 4: Real-file and multiprocess campaigns

Deliverables:

- Same history protocol across process boundaries.
- Real-file/WAL restart and kill-point campaigns.
- Checkpoint and crash matrices with bounded schedules.
- File integrity plus logical-state oracles after reopen.

Exit gate:

- Disjoint-page writers demonstrate concurrent progress rather than serialized
  admission.
- Same-page conflicts match documented retry/abort behavior.
- Acknowledged commits survive; unacknowledged outcomes are classified rather
  than guessed.
- All failures replay from captured artifacts.

### Phase 5: External oracle lane

Deliverables:

- SQLancer provider/container spike, then a time-bounded nightly campaign if
  accepted.
- Intake bridge from external logs to canonical failure bundles.
- Unique-finding and coverage comparison against native campaigns.
- Separate SQLRight feasibility report and go/no-go decision.

Exit gate:

- External-tool revision and environment are pinned.
- No expected-error list can expand without a linked feature/contract reason.
- A four-week trial reports unique defects, unique coverage, false-positive
  rate, runtime, and maintenance burden.
- SQLRight proceeds only if the result justifies its toolchain cost.

### Phase 6: Optional ergonomics and hosted testing

Deliverables:

- Declarative case-format pilot decision.
- Antithesis access/cost/value feasibility report.
- Native VFS syscall-fault gap report.
- CLI/system corpus gap report.

Exit gate:

- Each optional item has an evidence-based adopt/defer/reject decision.
- None is required for the core campaign to remain reproducible locally.

## 8. CI And Resource Policy

Use three tiers:

| Tier | Trigger | Budget | Contents |
|---|---|---|---|
| Presubmit | every relevant change | target under 10 minutes for this campaign | fixed seeds, generator contracts, reducer fixtures, small exhaustive histories |
| Nightly | scheduled | bounded per lane, initially 30-60 minutes | expanded seeds, multiprocess schedules, SQLancer |
| Campaign | manual/release | explicit operator budget | large DPOR, SQLRight, Antithesis, long fault and stress runs |

Rules:

- Add a small number of runner targets, not hundreds of Rust integration files.
- Shard by stable seed ranges and profiles.
- Emit counts for generated, executed, unsupported, invalid, timed out, skipped,
  mismatched, crashed, reduced, and promoted cases.
- A skipped/unsupported count increase fails contract drift validation unless
  approved with a bead.
- A timeout is not a semantic pass.
- Budget exhaustion reports incomplete exploration, never proof.
- Coverage is reported by feature, construct, execution lane, fault kind, and
  concurrency workload, not just lines.
- Numeric performance claims remain outside this plan unless measured by a
  named benchmark artifact under the README performance-claim rules.

## 9. Observability And Artifacts

Every failure must preserve:

- engine SHA and dirty-state indicator;
- Turso/external source revision where applicable;
- generator and profile version/hash;
- seed and derived seed path;
- original and minimized SQL/AST;
- schema and setup;
- result/error classification from both oracles;
- transaction history and schedule;
- required and observed execution lanes;
- fault/crash/yield schedule;
- database, WAL, and checksum artifacts when relevant;
- one-command local replay;
- minimizer result and signature.

Structured tracing should include `run_id`, `trace_id`, `scenario_id`, `seed`,
`profile`, `feature_ids`, `lane_required`, `lane_observed`, `worker`, `txn_id`,
`schedule_step`, `oracle`, and `outcome`. Avoid per-operation INFO logs in
large campaigns; detailed events belong in bounded artifacts or DEBUG traces.

## 10. Test Strategy For The Test Infrastructure

The new harness code itself needs tests.

### Unit

- capability mapping for supported/partial/excluded features;
- deterministic generation and seed splitting;
- schema-state transitions;
- AST printer/parser round trip on the supported subset;
- budget enforcement and depth limits;
- comparison rules for NULL, integers/reals, blobs, text, order, and errors;
- every SQL reducer preserves syntax and setup dependencies;
- every history reducer preserves the serializability/crash witness and public
  replay contract;
- history serialization and stable ordering;
- serializability oracle golden accept/reject cases;
- provenance validation and contract-drift failure.

### Integration

- known-good and deliberately divergent fake backends for comparator tests;
- public FrankenSQLite and C SQLite differential execution;
- forced fallback proving a pager-required case fails closed;
- generated mismatch through bundle, minimization, replay, and promotion;
- cancellation at generation, execution, reduction, and artifact-write stages;
- corrupt/truncated artifacts fail clearly.

### End to end

- fixed-seed typed SQL campaign;
- fixed invalid serializability history;
- small LabRuntime concurrent campaign;
- real-file crash/reopen campaign;
- external-provider smoke using a pinned image when that phase is accepted.

## 11. Risks And Controls

| Risk | Control |
|---|---|
| Test volume increases without new information | Value gates use unique defects, coverage, reduction, and replay, not count. |
| Common-mode oracle bug | Test AST/model stays independent of production AST; retain C SQLite and external oracles. |
| Fallback masks storage defects | Required-lane evidence fails closed. |
| Turso semantics weaken FrankenSQLite guarantees | Canonical FrankenSQLite contracts win; serializability replaces SI acceptance. |
| Tokio enters dev graph | Existing `no_tokio_enforcement.rs` cargo-metadata gate plus cargo-tree audit; implement with asupersync. |
| Imported unsupported tests create skip debt | Contract-driven capability map and skip-count drift gate. |
| Flaky randomized tests | Fixed seed derivation, deterministic schedules, bounded time, replay artifact. |
| Minimizer changes failure identity | Exact signature revalidation after every reduction. |
| Divergent root contract copies silently redefine scope | Hard-gated authority consolidation, no live root-path reads, and an executable drift guard before profile mapping. |
| Observation-only histories are mislabeled deterministic | Production `Connection`/LabRuntime bridge is a blocking prerequisite; observation-only evidence cannot close the deterministic integration bead. |
| External tools become release blockers | Start nightly/advisory; promote only after stability and ownership evidence. |
| License/provenance is lost | Required intake metadata and review before substantial copying. |
| CI target explosion | Corpus-driven runners and target accounting rather than one target per case. |

## 12. Explicit Non-Goals

- Replacing SQLite's upstream TCL corpus or FrankenSQLite's native harness.
- Claiming full SQLite compatibility from imported test counts.
- Matching Turso's storage format, MVCC implementation, cloud protocol, or
  product bindings.
- Making snapshot isolation an acceptable FrankenSQLite concurrency result.
- Changing concurrent mode defaults or adding connection/file writer locks.
- Adding Tokio, Shuttle, or a second async runtime for tests.
- Expanding excluded features by accident through generator support.
- Rewriting current Rust tests into a new DSL en masse.
- Making Antithesis or any hosted service necessary for local reproduction.

## 13. Stop/Go Metrics

Each phase ends with a written keep/defer/reject decision.

Keep a component when it demonstrates at least one of:

- a unique confirmed defect class;
- materially new feature/lane/fault coverage;
- a substantially smaller reproducer with preserved signature;
- deterministic reproduction of a previously non-reproducible failure;
- a measurable maintenance reduction in the DSL pilot.

Reject or defer when:

- findings duplicate existing campaigns;
- invalid/unsupported generation remains above the agreed threshold;
- failures cannot replay deterministically;
- false positives dominate triage;
- required execution lanes cannot be proven;
- runtime or dependency cost exceeds the lane budget;
- the work pressures the project toward serialized writers, weaker isolation,
  Tokio, or unsupported parity claims.

## 14. Recommended Order

1. Baseline, provenance, overlap map, execution-lane evidence, and hard-gated
   contract-authority consolidation.
2. Narrow typed differential generator.
3. Structured SQL reducer integration.
4. Stateful deterministic operation plans over the existing workload/oplog,
   differential, reduction, and replay ownership.
5. SQL-level serializability histories plus the production-`Connection`
   LabRuntime bridge.
6. History/schedule reduction, then real-file/multiprocess schedules.
7. SQLancer trial.
8. Coverage-ledger and CI promotion decision (recommended after the SQLancer
   trial but not blocked by it — the tracker edge is non-blocking).
9. Optional SQLRight, declarative DSL, Antithesis, and syscall-fault decisions.

This order maximizes independent correctness signal early while containing
dependency, CI, and maintenance costs.

## 15. Definition Of Done

The program is complete when:

- every Turso testing area has an evidence-backed adopt/defer/reject record;
- adopted work is integrated into existing FrankenSQLite harness ownership;
- generated cases are scope-aware, deterministic, minimized, and replayable;
- stateful operation plans use an independent model, exercise rollback/reopen
  invariants, and flow through the canonical operation-log, differential,
  reduction, bundle, replay, and corpus ownership;
- SQL-level concurrent histories are checked against serializability/SSI;
- pager/MVCC/recovery claims include execution-lane evidence;
- every scope-defining contract consumer resolves the canonical
  `docs/contracts/` authority and the duplicate/drift guard passes;
- external campaigns, if retained, have pinned provenance and bounded CI lanes;
- the canonical ledgers expose generated and imported coverage without skip
  inflation;
- concurrent writer mode remains true by default everywhere;
- no Tokio dependency is introduced;
- all implementation beads meet their stated unit, integration, E2E, logging,
  and artifact acceptance criteria.

## 16. Beads Map

Epic: `bd-turso-test-adaptation-zu081`

| Phase | Bead | Deliverable |
|---|---|---|
| 0 | `bd-turso-test-adaptation-zu081.1` | Baseline, overlap map, and provenance policy |
| 0 | `bd-turso-test-adaptation-zu081.18` | Canonical contract-authority consolidation and drift guard |
| 0 | `bd-turso-test-adaptation-zu081.2` | Fail-closed execution-lane evidence |
| 1 | `bd-turso-test-adaptation-zu081.3` | Independent typed SQL generator core |
| 1 | `bd-turso-test-adaptation-zu081.4` | Contract-derived profiles and coverage |
| 1 | `bd-turso-test-adaptation-zu081.5` | Differential, corpus, and replay adapters |
| 2 | `bd-turso-test-adaptation-zu081.6` | SQL AST/schema/value reduction |
| 2A | `bd-turso-test-adaptation-zu081.20` | Stateful deterministic operation-plan/model campaign |
| 3 | `bd-turso-test-adaptation-zu081.7` | SSI/serializability history oracle |
| 3 prerequisite | `bd-2lt76.1` | Production `Connection`/LabRuntime scheduling bridge |
| 3 | `bd-turso-test-adaptation-zu081.8` | LabRuntime/DPOR history integration |
| 3 | `bd-turso-test-adaptation-zu081.19` | History/schedule/worker/crash reduction |
| 4 | `bd-turso-test-adaptation-zu081.9` | Real-file multiprocess crash/recovery |
| 5 | `bd-turso-test-adaptation-zu081.10` | SQLancer provider spike |
| 5 | `bd-turso-test-adaptation-zu081.11` | Bounded SQLancer nightly trial |
| 5 | `bd-turso-test-adaptation-zu081.12` | SQLRight feasibility decision |
| 6 | `bd-turso-test-adaptation-zu081.13` | Declarative case-format pilot |
| 6 | `bd-turso-test-adaptation-zu081.14` | Antithesis feasibility decision |
| 6 | `bd-turso-test-adaptation-zu081.15` | Allocator/syscall fault-gap decision |
| 6 | `bd-turso-test-adaptation-zu081.16` | CLI/system/fixed-database gap audit |
| cross-phase | `bd-turso-test-adaptation-zu081.17` | CI, coverage, and phase-promotion gates |

The epic is P0 because it contains the current top triage pick. Cross-epic
`bd-2lt76.1` is a child of `bd-2lt76`, not of this epic. The Turso epic
therefore has 20 children, and every child carries both a
`## Acceptance` section in its description and the structured
`acceptance_criteria` field, so tooling that reads the structured field sees
the same criteria as human readers.

The plan-to-bead coverage ledger is:

| Plan requirement | Primary owner | Required supporting boundary |
|---|---|---|
| §3.1 baseline and existing-owner inventory | `.1` | Current repository discovery and pinned Turso inventory |
| §3.2 execution-lane evidence | `.2` | Existing parity-cert/fallback machinery and `.1` baseline |
| §5.1 typed SQL generation | `.3`, `.4` | `.18` canonical contract authority |
| §5.2 SQL reduction and canonical replay | `.6` | `.5` public differential/corpus/bundle adapter |
| §5.3 stateful deterministic simulation | `.20` | `.3`-`.6` typed SQL, profile, adapter, and reducer chain |
| §5.4 typed histories and SSI oracle | `.7` | `.1`, `.2` |
| §5.4 production deterministic scheduling | `.8` | `.7` and cross-epic `bd-2lt76.1`; `bd-28z4i.5` is related |
| §5.4 history/schedule reduction | `.19` | `.7`, `.8` |
| §5.5 multiprocess crash/recovery | `.9` | `.19`, extended through `swarm_multiprocess.rs` |
| §5.6 SQLancer provider and trial | `.10`, `.11` | `.18` contracts, `.2` lane evidence, `.6` canonical reduction |
| §5.6 SQLRight decision | `.12` | `.11` equal-budget baseline |
| §5.7 declarative case-format pilot | `.13` | `.18` contracts, `.2` lanes, existing SLT/JSON baseline |
| §6 provenance and licensing | `.1` | Every importing/adapting child consumes the intake record |
| §7-§9 phase gates, CI, coverage, artifacts | `.17` | `.5`, `.6`, `.8`, `.19`, `.9`, `.20`; adopted optional lanes only |
| §11-§13 risk controls and stop/go decisions | Owning child plus `.17` | Epic closure requires all 20 child decisions |
| §12 excluded/product-specific families | `.1`, `.16` | Explicit reject/defer records, never dormant code |

The complete blocking-edge set (`task <- blockers`), kept exactly in sync
with the tracker:

```text
.2  <- .1            .3  <- .1            .18 <- .1
.4  <- .3, .18       .5  <- .2, .3, .4    .6  <- .5
.20 <- .6
.7  <- .1, .2        bd-2lt76.1 <- bd-2jpu6.5
.8  <- .2, .7, bd-2lt76.1                 .19 <- .7, .8
.9  <- .19           .10 <- .1, .2, .18   .11 <- .6, .10
.12 <- .11           .13 <- .1, .2, .18   .14 <- .7, .9
.15 <- .1, .9        .16 <- .1
.17 <- .5, .6, .8, .9, .19, .20
```

Reading order of the spine: `.1 -> .18 -> .4` is the contract gate; the SQL
generator/reducer chain is `.3 -> .4 -> .5 -> .6`; the concurrency chain joins
`.7` with cross-epic bridge `bd-2lt76.1` at `.8`, then continues through
`.19 -> .9`; the stateful-model campaign is `.6 -> .20`; the external-oracle
chain is `.18 -> .10 -> .11 -> .12`.

`.17` (CI, coverage, and promotion gates) hard-blocks on all retained native
campaign inputs, including `.9` multiprocess/recovery and `.20` stateful
operation plans. It deliberately does **not** block on
the external or optional lanes: its edges to `.10`, `.11`, `.12`, `.13`,
`.14`, `.15`, and `.16` are non-blocking `related` links, so a deferred or
rejected external tool can never stall CI promotion of the native lanes. Only
adopted lanes gate CI; epic-level closure (all 20 children) is what enforces
that every Turso testing area ends with an adopt/defer/reject record.

The LabRuntime integration (`.8`) hard-blocks on narrow bridge `bd-2lt76.1`
and retains a non-blocking `related` edge to `bd-28z4i.5`. This makes production
determinism real without duplicating or waiting for unrelated DPOR scope.
Observation-only histories are `.7` inputs, not `.8` completion evidence.
Optional beads `.13` through `.16` hang off Phase 0 governance
(`.13 <- .1, .2, .18`; `.16 <- .1`) or the concurrency campaigns whose results they
assess (`.14 <- .7, .9`; `.15 <- .1, .9`); none of them blocks the core
delivery spine.
